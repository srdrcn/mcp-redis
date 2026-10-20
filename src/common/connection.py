import logging
import threading
import time
from contextvars import ContextVar
from typing import Any, Dict, Optional, Tuple, Union

from redis import Redis
from redis.backoff import NoBackoff
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry
from redis.sentinel import MasterNotFoundError, Sentinel

from src.common.config_loader import ClusterConfig, get_config
from src.version import __version__

_logger = logging.getLogger(__name__)

# Cluster selected by the tool call being served; set by the tool registry for each call.
current_cluster: ContextVar[Optional[str]] = ContextVar(
    "redis_mcp_cluster", default=None
)

# A tool call has 15 s (see src.common.registry). With these timeouts and no client retries, a slow or
# unreachable standalone/sentinel cluster produces its own error within that time instead of the generic tool
# timeout. `options` can override the timeouts per cluster.
SOCKET_CONNECT_TIMEOUT_SECONDS = 5
SOCKET_TIMEOUT_SECONDS = 10
# Each Sentinel is asked in turn for the master; a short limit per Sentinel keeps the lookup within the budget.
SENTINEL_TIMEOUT_SECONDS = 2
# Per cluster (per node for Redis Cluster): above the concurrent tool calls the registry allows per cluster.
MAX_CONNECTIONS = 16
# Pub/sub subscriptions hold their connection, so they get a client of their own and never take the
# connections the tool calls need.
MAX_PUBSUB_CONNECTIONS = 50
# A failed client creation is reported to the callers waiting for it instead of being repeated by each one.
FAILED_CREATION_SECONDS = 5


class ClusterSelectionError(ValueError):
    """No cluster selected, or an unknown cluster name."""


ClientKey = Tuple[str, bool, bool]


class RedisConnectionManager:
    """One client per (cluster, decode_responses, pub/sub), created on first use and then reused.

    Each cluster has its own lock, so a cluster whose client is slow to create (e.g. a Redis Cluster that
    does not answer CLUSTER SLOTS) does not hold up the others. A failed creation is not kept: callers that
    were waiting for it get the same error, and a call more than FAILED_CREATION_SECONDS later tries again.
    """

    _clients: Dict[ClientKey, Union[Redis, RedisCluster]] = {}
    _locks: Dict[ClientKey, threading.Lock] = {}
    _failures: Dict[ClientKey, Tuple[float, Exception]] = {}
    _guard = threading.Lock()

    @classmethod
    def get_connection(
        cls,
        decode_responses: bool = True,
        cluster: Optional[str] = None,
        pubsub: bool = False,
    ) -> Redis:
        config = cls.cluster_config(cluster)
        key = (config.name, decode_responses, pubsub)
        client = cls._clients.get(key)
        if client is not None:
            return client
        with cls._guard:
            lock = cls._locks.setdefault(key, threading.Lock())
        with lock:
            client = cls._clients.get(key)
            if client is None:
                failure = cls._failures.get(key)
                if (
                    failure is not None
                    and time.monotonic() - failure[0] < FAILED_CREATION_SECONDS
                ):
                    # without the traceback of earlier raises, which would grow with every waiter
                    raise failure[1].with_traceback(None)
                try:
                    client = create_client(
                        config,
                        decode_responses,
                        max_connections=MAX_PUBSUB_CONNECTIONS if pubsub else None,
                    )
                except Exception as e:
                    cls._failures[key] = (time.monotonic(), e)
                    _logger.error(
                        "Failed to create the Redis client for cluster %s: %s",
                        config.name,
                        e,
                    )
                    raise
                cls._failures.pop(key, None)
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
            cls._failures.clear()
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
        "lib_name": f"redis-py(redis-mcp_v{__version__})",
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


def _no_retry() -> Retry:
    """A single attempt per command. redis-py's default retries 3 times with backoff, timeouts included: a
    command that timed out (or whose connection dropped) may already have run, so a write could be applied
    twice, and the retries could outlast the tool's time limit. Stale pooled connections are still replaced:
    the pool checks a connection before handing it out."""
    return Retry(NoBackoff(), 0)


class _Sentinel(Sentinel):
    """Sentinel whose "no master found" error names each Sentinel by host:port. redis-py puts the repr of the
    Sentinel clients into that message, and the repr lists the connection arguments, the password included."""

    def discover_master(self, service_name):
        collected_errors = []
        for sentinel_no, sentinel in enumerate(self.sentinels):
            try:
                masters = sentinel.sentinel_masters()
            except (RedisConnectionError, RedisTimeoutError) as e:
                kwargs = sentinel.connection_pool.connection_kwargs
                collected_errors.append(
                    f"{kwargs.get('host')}:{kwargs.get('port')} - {type(e).__name__}: {e}"
                )
                continue
            state = masters.get(service_name)
            if state and self.check_master_state(state, service_name):
                # same as redis-py: ask this Sentinel first next time
                self.sentinels[0], self.sentinels[sentinel_no] = (
                    sentinel,
                    self.sentinels[0],
                )
                ip = (
                    self._force_master_ip
                    if self._force_master_ip is not None
                    else state["ip"]
                )
                return ip, state["port"]
        error_info = f" : {', '.join(collected_errors)}" if collected_errors else ""
        raise MasterNotFoundError(f"No master found for {service_name!r}{error_info}")


def create_client(
    config: ClusterConfig,
    decode_responses: bool = True,
    max_connections: Optional[int] = None,
) -> Union[Redis, RedisCluster]:
    kwargs = client_kwargs(config, decode_responses)
    if max_connections is not None:
        kwargs["max_connections"] = max_connections
    else:
        kwargs.setdefault("max_connections", MAX_CONNECTIONS)
    addresses = config.addresses()
    if config.mode == "cluster":
        # a single attempt; a failure still refreshes the topology for the next call
        return RedisCluster(
            startup_nodes=[ClusterNode(host, port) for host, port in addresses],
            retry=_no_retry(),
            **kwargs,
        )
    if config.mode == "sentinel":
        sentinel_kwargs: Dict[str, Any] = {
            "socket_connect_timeout": min(
                kwargs["socket_connect_timeout"], SENTINEL_TIMEOUT_SECONDS
            ),
            "socket_timeout": min(kwargs["socket_timeout"], SENTINEL_TIMEOUT_SECONDS),
            "retry": _no_retry(),
        }
        if config.tls is not None:
            sentinel_kwargs.update(config.tls.client_kwargs())
        if config.sentinel_password:
            sentinel_kwargs["password"] = config.sentinel_password
        sentinel = _Sentinel(addresses, sentinel_kwargs=sentinel_kwargs)
        return sentinel.master_for(
            config.master_name, db=config.db, retry=_no_retry(), **kwargs
        )
    host, port = addresses[0]
    return Redis(host=host, port=port, db=config.db, retry=_no_retry(), **kwargs)
