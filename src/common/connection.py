import logging
import threading
from contextvars import ContextVar
from typing import Any, Dict, Optional, Tuple, Union

from redis import Redis
from redis.backoff import NoBackoff
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry
from redis.sentinel import Sentinel

from src.common.config_loader import ClusterConfig, get_config
from src.version import __version__

_logger = logging.getLogger(__name__)

# Cluster selected by the tool call being served; set by the tool registry for each call.
current_cluster: ContextVar[Optional[str]] = ContextVar(
    "redis_mcp_cluster", default=None
)

# A tool call has 15 s (see src.common.registry). With these timeouts and the retry policy below, a slow or
# unreachable standalone/sentinel cluster produces its own error within that time instead of the generic tool
# timeout. `options` can override the timeouts per cluster.
SOCKET_CONNECT_TIMEOUT_SECONDS = 5
SOCKET_TIMEOUT_SECONDS = 10
# Per cluster (per node for Redis Cluster): room for the concurrent tool calls the registry allows per cluster
# plus open pub/sub subscriptions.
MAX_CONNECTIONS = 16


class ClusterSelectionError(ValueError):
    """No cluster selected, or an unknown cluster name."""


class RedisConnectionManager:
    """One client per (cluster, decode_responses), created on first use and then reused.

    Each cluster has its own lock, so a cluster whose client is slow to create (e.g. a Redis Cluster that
    does not answer CLUSTER SLOTS) does not hold up the others. A failed creation is not cached; the next
    call tries again.
    """

    _clients: Dict[Tuple[str, bool], Union[Redis, RedisCluster]] = {}
    _locks: Dict[Tuple[str, bool], threading.Lock] = {}
    _guard = threading.Lock()

    @classmethod
    def get_connection(
        cls, decode_responses: bool = True, cluster: Optional[str] = None
    ) -> Redis:
        config = cls.cluster_config(cluster)
        key = (config.name, decode_responses)
        client = cls._clients.get(key)
        if client is not None:
            return client
        with cls._guard:
            lock = cls._locks.setdefault(key, threading.Lock())
        with lock:
            client = cls._clients.get(key)
            if client is None:
                try:
                    client = create_client(config, decode_responses)
                except Exception as e:
                    _logger.error(
                        "Failed to create the Redis client for cluster %s: %s",
                        config.name,
                        e,
                    )
                    raise
                cls._clients[key] = client
        return client

    @staticmethod
    def cluster_config(cluster: Optional[str] = None) -> ClusterConfig:
        """Config of the named cluster, else of the call's cluster, else of the only configured cluster."""
        config = get_config()
        name = cluster or current_cluster.get()
        if name is None:
            if len(config.clusters) == 1:
                return config.clusters[0]
            raise ClusterSelectionError(
                f"Missing parameter: cluster. Valid values: {config.cluster_names()}"
            )
        found = config.cluster(name)
        if found is None:
            raise ClusterSelectionError(
                f"Unknown cluster: {name}. Valid values: {config.cluster_names()}"
            )
        return found

    @classmethod
    def reset(cls) -> None:
        """Closes and drops every client; the next call creates new ones."""
        with cls._guard:
            clients = list(cls._clients.values())
            cls._clients.clear()
            cls._locks.clear()
        for client in clients:
            try:
                client.close()
            # best effort while discarding clients
            except Exception:  # nosec B110
                pass


def client_kwargs(config: ClusterConfig, decode_responses: bool) -> Dict[str, Any]:
    """Connection arguments shared by all modes: defaults, auth, TLS, then the cluster's options."""
    kwargs: Dict[str, Any] = {
        "decode_responses": decode_responses,
        "lib_name": f"redis-py(mcp-server_v{__version__})",
        "socket_connect_timeout": SOCKET_CONNECT_TIMEOUT_SECONDS,
        "socket_timeout": SOCKET_TIMEOUT_SECONDS,
    }
    if config.entraid:
        from src.common.entraid_auth import (
            EntraIDAuthenticationError,
            create_credential_provider,
        )

        provider = create_credential_provider()
        if provider is None:
            # never fall back to an unauthenticated connection
            raise EntraIDAuthenticationError(
                f"cluster {config.name} sets entraid: true, but no Entra ID auth flow is configured"
            )
        kwargs["credential_provider"] = provider
    else:
        if config.username:
            kwargs["username"] = config.username
        if config.password:
            kwargs["password"] = config.password
    if config.tls is not None:
        kwargs.update(config.tls.client_kwargs())
    kwargs.update(config.options)
    return kwargs


def _retry() -> Retry:
    """One immediate retry when a connection drops (e.g. after a Redis restart), none on timeouts: a command
    that timed out may have run, and sending a write again would apply it twice. redis-py's default (3 retries
    with backoff, timeouts included) could also outlast the tool's time limit."""
    return Retry(NoBackoff(), 1, supported_errors=(RedisConnectionError,))


def create_client(
    config: ClusterConfig, decode_responses: bool = True
) -> Union[Redis, RedisCluster]:
    kwargs = client_kwargs(config, decode_responses)
    kwargs.setdefault("max_connections", MAX_CONNECTIONS)
    addresses = config.addresses()
    if config.mode == "cluster":
        # RedisCluster retries every attempt on connection errors and timeouts alike (with a topology refresh),
        # so it gets a single attempt; the next call uses the refreshed topology.
        return RedisCluster(
            startup_nodes=[ClusterNode(host, port) for host, port in addresses],
            retry=Retry(NoBackoff(), 0),
            **kwargs,
        )
    if config.mode == "sentinel":
        sentinel_kwargs: Dict[str, Any] = {
            "socket_connect_timeout": kwargs["socket_connect_timeout"],
            "socket_timeout": kwargs["socket_timeout"],
            "retry": _retry(),
        }
        if config.tls is not None:
            sentinel_kwargs.update(config.tls.client_kwargs())
        if config.sentinel_password:
            sentinel_kwargs["password"] = config.sentinel_password
        sentinel = Sentinel(addresses, sentinel_kwargs=sentinel_kwargs)
        return sentinel.master_for(
            config.master_name, db=config.db, retry=_retry(), **kwargs
        )
    host, port = addresses[0]
    return Redis(host=host, port=port, db=config.db, retry=_retry(), **kwargs)
