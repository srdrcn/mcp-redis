"""
Unit tests for src/common/connection.py
"""

import logging
import socket
import threading
import uuid
from unittest.mock import Mock, patch

import pytest
import redis
from redis import Redis
from redis.backoff import NoBackoff
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import AuthenticationError, ConnectionError
from redis.retry import Retry

from src.common.config_loader import parse_config, resolve_passwords, set_config
from src.common.connection import (
    MAX_CONNECTIONS,
    SOCKET_CONNECT_TIMEOUT_SECONDS,
    SOCKET_TIMEOUT_SECONDS,
    ClusterSelectionError,
    RedisConnectionManager,
    client_kwargs,
    create_client,
    current_cluster,
)
from src.common.registry import TOOL_TIMEOUT_SECONDS
from src.version import __version__

LOGGER_NAME = "src.common.connection"
LIB_NAME = f"redis-py(mcp-server_v{__version__})"

# Arguments every client gets unless the cluster's options override them.
DEFAULTS = {
    "decode_responses": True,
    "lib_name": LIB_NAME,
    "socket_connect_timeout": 5,
    "socket_timeout": 10,
    "max_connections": 10,
}

# Waits on the threading Events below; generous, but bounded so a broken lock cannot hang the run.
WAIT_SECONDS = 5


def cluster(name="a", **fields):
    data = {"name": name, "hosts": f"{name}.example:6379"}
    data.update(fields)
    return data


@pytest.fixture
def use_config(tmp_path, monkeypatch):
    """Activates a config built from cluster maps; `passwords` become files in a temporary passwordsDir."""

    def _use(*clusters, passwords=None, **root):
        for key, value in (passwords or {}).items():
            (tmp_path / key).write_text(value + "\n", encoding="utf-8")
        config = parse_config(
            {"passwordsDir": str(tmp_path), "clusters": list(clusters), **root}
        )
        # only the files above may supply passwords, not <name>_pass variables of the host environment
        for c in config.clusters:
            monkeypatch.delenv(f"{c.name}_pass", raising=False)
            monkeypatch.delenv(f"{c.name}_sentinel_pass", raising=False)
        resolve_passwords(config)
        set_config(config)
        return config

    return _use


@pytest.fixture
def redis_cls():
    """Patched redis.Redis; every call returns a new mock client."""
    with patch("src.common.connection.Redis") as cls:
        cls.side_effect = lambda *args, **kwargs: Mock(spec=Redis)
        yield cls


@pytest.fixture
def cluster_cls():
    with patch("src.common.connection.RedisCluster") as cls:
        cls.side_effect = lambda *args, **kwargs: Mock(spec=RedisCluster)
        yield cls


@pytest.fixture
def sentinel_cls():
    with patch("src.common.connection.Sentinel") as cls:
        yield cls


@pytest.fixture
def credential_provider():
    """Patched Entra ID credential provider factory (imported by connection.py on use)."""
    provider = object()
    with patch(
        "src.common.entraid_auth.create_credential_provider", return_value=provider
    ) as factory:
        factory.provider = provider
        yield factory


class TestTimeouts:
    def test_defaults(self):
        assert SOCKET_CONNECT_TIMEOUT_SECONDS == 5
        assert SOCKET_TIMEOUT_SECONDS == 10
        assert MAX_CONNECTIONS == 10

    def test_client_timeouts_stay_below_the_tool_timeout(self):
        """A slow cluster must fail with its own error, not the generic tool timeout."""
        assert SOCKET_CONNECT_TIMEOUT_SECONDS < TOOL_TIMEOUT_SECONDS
        assert SOCKET_TIMEOUT_SECONDS < TOOL_TIMEOUT_SECONDS

    @pytest.mark.xfail(
        strict=True,
        reason="redis-py's default Retry (3 retries, exponential backoff) repeats the 5 s connect timeout:"
        " an unreachable standalone cluster takes 20-34 s to fail, longer than the 15 s tool timeout",
    )
    def test_unreachable_cluster_fails_before_the_tool_timeout(self, use_config):
        """A host that never answers (firewalled, blackholed) must fail with the client's own TimeoutError
        within the tool timeout. Time is simulated: each connect attempt costs its connect timeout and each
        backoff its sleep, nothing really waits."""
        use_config(cluster("blackhole", hosts="192.0.2.1:6379"))
        attempts = []
        sleeps = []

        def connect_times_out(connection):
            attempts.append(connection.socket_connect_timeout)
            raise socket.timeout("timed out")

        with (
            patch.object(redis.connection.Connection, "_connect", connect_times_out),
            patch("redis.retry.sleep", side_effect=sleeps.append),
        ):
            client = RedisConnectionManager.get_connection()
            with pytest.raises(redis.exceptions.TimeoutError):
                client.ping()

        total = sum(attempts) + sum(sleeps)
        assert total < TOOL_TIMEOUT_SECONDS, (
            f"{len(attempts)} connect attempts of {SOCKET_CONNECT_TIMEOUT_SECONDS} s"
            f" and {sum(sleeps):.1f} s of backoff: {total:.1f} s"
        )


class TestStandaloneClient:
    def test_exact_kwargs(self, use_config, redis_cls):
        use_config(cluster("cbtstrds", hosts="redis.example:6390", db=2))

        RedisConnectionManager.get_connection()

        redis_cls.assert_called_once_with(
            host="redis.example", port=6390, db=2, **DEFAULTS
        )

    def test_returns_the_created_client(self, use_config, redis_cls):
        use_config(cluster())
        created = Mock(spec=Redis)
        redis_cls.side_effect = None
        redis_cls.return_value = created

        assert RedisConnectionManager.get_connection() is created

    def test_decode_responses_false(self, use_config, redis_cls):
        use_config(cluster(hosts="h:6379"))

        RedisConnectionManager.get_connection(decode_responses=False)

        redis_cls.assert_called_once_with(
            host="h", port=6379, db=0, **{**DEFAULTS, "decode_responses": False}
        )

    def test_lib_name_carries_the_version(self, use_config, redis_cls):
        use_config(cluster())

        with patch("src.common.connection.__version__", "1.0.0"):
            RedisConnectionManager.get_connection()

        assert redis_cls.call_args.kwargs["lib_name"] == "redis-py(mcp-server_v1.0.0)"

    def test_ipv6_host(self, use_config, redis_cls):
        use_config(cluster(hosts="[::1]:6379"))

        RedisConnectionManager.get_connection()

        assert redis_cls.call_args.kwargs["host"] == "::1"
        assert redis_cls.call_args.kwargs["port"] == 6379

    def test_username_and_resolved_password(self, use_config, redis_cls):
        use_config(
            cluster("cbtstrds", hosts="h:6379", username="mcp-reader"),
            passwords={"cbtstrds_pass": "12345"},
        )

        RedisConnectionManager.get_connection()

        redis_cls.assert_called_once_with(
            host="h",
            port=6379,
            db=0,
            username="mcp-reader",
            password="12345",
            **DEFAULTS,
        )

    def test_password_from_environment(self, redis_cls, tmp_path, monkeypatch):
        # no <name>_pass file in the passwords directory, so the variable is used
        config = parse_config(
            {"passwordsDir": str(tmp_path), "clusters": [cluster("envpw")]}
        )
        monkeypatch.setenv("envpw_pass", "from-env")
        resolve_passwords(config)
        set_config(config)

        RedisConnectionManager.get_connection()

        assert redis_cls.call_args.kwargs["password"] == "from-env"
        assert "username" not in redis_cls.call_args.kwargs

    def test_no_auth_without_password_or_username(self, use_config, redis_cls):
        use_config(cluster("nopw"))

        RedisConnectionManager.get_connection()

        kwargs = redis_cls.call_args.kwargs
        assert "password" not in kwargs
        assert "username" not in kwargs
        assert "credential_provider" not in kwargs

    def test_tls_map_becomes_ssl_kwargs(self, use_config, redis_cls):
        use_config(
            cluster(
                hosts="h:6380",
                tls={
                    "caFile": "/etc/redis-mcp/tls/ca.crt",
                    "caPath": "/etc/ssl/certs",
                    "certFile": "/etc/redis-mcp/tls/client.crt",
                    "keyFile": "/etc/redis-mcp/tls/client.key",
                    "certReqs": "optional",
                    "checkHostname": False,
                },
            )
        )

        RedisConnectionManager.get_connection()

        redis_cls.assert_called_once_with(
            host="h",
            port=6380,
            db=0,
            ssl=True,
            ssl_cert_reqs="optional",
            ssl_ca_certs="/etc/redis-mcp/tls/ca.crt",
            ssl_ca_path="/etc/ssl/certs",
            ssl_certfile="/etc/redis-mcp/tls/client.crt",
            ssl_keyfile="/etc/redis-mcp/tls/client.key",
            ssl_check_hostname=False,
            **DEFAULTS,
        )

    def test_tls_true_uses_the_defaults(self, use_config, redis_cls):
        use_config(cluster(hosts="h:6380", tls=True))

        RedisConnectionManager.get_connection()

        redis_cls.assert_called_once_with(
            host="h", port=6380, db=0, ssl=True, ssl_cert_reqs="required", **DEFAULTS
        )

    def test_tls_false_means_no_ssl(self, use_config, redis_cls):
        use_config(cluster(tls=False))

        RedisConnectionManager.get_connection()

        assert not any(k.startswith("ssl") for k in redis_cls.call_args.kwargs)

    def test_options_override_defaults_and_add_kwargs(self, use_config, redis_cls):
        use_config(
            cluster(
                hosts="h:6379",
                options={
                    "socket_timeout": 2.5,
                    "max_connections": 50,
                    "health_check_interval": "30",
                    "retry_on_timeout": True,
                },
            )
        )

        RedisConnectionManager.get_connection()

        redis_cls.assert_called_once_with(
            host="h",
            port=6379,
            db=0,
            decode_responses=True,
            lib_name=LIB_NAME,
            socket_connect_timeout=5,
            socket_timeout=2.5,
            max_connections=50,
            health_check_interval=30,
            retry_on_timeout=True,
        )

    def test_options_override_tls_kwargs(self, use_config, redis_cls):
        use_config(cluster(tls=True, options={"ssl_cert_reqs": "none"}))

        RedisConnectionManager.get_connection()

        assert redis_cls.call_args.kwargs["ssl_cert_reqs"] == "none"


class TestRedisClusterClient:
    def test_startup_nodes_and_no_db(self, use_config, cluster_cls, redis_cls):
        use_config(
            cluster("cbprdrds", mode="cluster", hosts=["h1:6379", "h2:6380"]),
            passwords={"cbprdrds_pass": "pw"},
        )

        client = RedisConnectionManager.get_connection()

        cluster_cls.assert_called_once_with(
            startup_nodes=[ClusterNode("h1", 6379), ClusterNode("h2", 6380)],
            password="pw",
            **DEFAULTS,
        )
        kwargs = cluster_cls.call_args.kwargs
        assert "db" not in kwargs
        assert "host" not in kwargs
        assert "port" not in kwargs
        assert [(n.host, n.port) for n in kwargs["startup_nodes"]] == [
            ("h1", 6379),
            ("h2", 6380),
        ]
        redis_cls.assert_not_called()
        assert isinstance(client, RedisCluster)

    def test_tls_username_and_options(self, use_config, cluster_cls):
        use_config(
            cluster(
                "c1",
                mode="cluster",
                hosts="h1:6379",
                username="app",
                tls={"caFile": "/ca.crt"},
                options={"max_connections": 32, "read_from_replicas": True},
            ),
            passwords={"c1_pass": "pw"},
        )

        RedisConnectionManager.get_connection(decode_responses=False)

        cluster_cls.assert_called_once_with(
            startup_nodes=[ClusterNode("h1", 6379)],
            decode_responses=False,
            lib_name=LIB_NAME,
            socket_connect_timeout=5,
            socket_timeout=10,
            max_connections=32,
            read_from_replicas=True,
            username="app",
            password="pw",
            ssl=True,
            ssl_cert_reqs="required",
            ssl_ca_certs="/ca.crt",
        )


class TestSentinelClient:
    def test_sentinel_and_master_for(self, use_config, sentinel_cls, redis_cls):
        use_config(
            cluster(
                "x",
                mode="sentinel",
                hosts="s1:26379,s2:26380",
                masterName="mymaster",
                db=3,
                username="app",
                tls={"caFile": "/etc/redis-mcp/tls/ca.crt"},
            ),
            passwords={"x_pass": "pw", "x_sentinel_pass": "spw"},
        )

        client = RedisConnectionManager.get_connection()

        tls = {
            "ssl": True,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": "/etc/redis-mcp/tls/ca.crt",
        }
        sentinel_cls.assert_called_once_with(
            [("s1", 26379), ("s2", 26380)],
            sentinel_kwargs={
                "socket_connect_timeout": 5,
                "socket_timeout": 10,
                "password": "spw",
                **tls,
            },
        )
        sentinel = sentinel_cls.return_value
        sentinel.master_for.assert_called_once_with(
            "mymaster", db=3, username="app", password="pw", **tls, **DEFAULTS
        )
        assert client is sentinel.master_for.return_value
        redis_cls.assert_not_called()

    def test_without_tls_and_sentinel_password(self, use_config, sentinel_cls):
        use_config(
            cluster("y", mode="sentinel", hosts="s1:26379", masterName="m"),
            passwords={"y_pass": "pw"},
        )

        RedisConnectionManager.get_connection(decode_responses=False)

        sentinel_cls.assert_called_once_with(
            [("s1", 26379)],
            sentinel_kwargs={"socket_connect_timeout": 5, "socket_timeout": 10},
        )
        sentinel_cls.return_value.master_for.assert_called_once_with(
            "m",
            db=0,
            password="pw",
            **{**DEFAULTS, "decode_responses": False},
        )

    def test_options_timeouts_reach_the_sentinels(self, use_config, sentinel_cls):
        use_config(
            cluster(
                "z",
                mode="sentinel",
                hosts="s1:26379",
                masterName="m",
                options={"socket_timeout": 3, "socket_connect_timeout": 1},
            )
        )

        RedisConnectionManager.get_connection()

        assert sentinel_cls.call_args.kwargs["sentinel_kwargs"] == {
            "socket_connect_timeout": 1,
            "socket_timeout": 3,
        }
        kwargs = sentinel_cls.return_value.master_for.call_args.kwargs
        assert kwargs["socket_timeout"] == 3
        assert kwargs["socket_connect_timeout"] == 1


class TestEntraIdClient:
    def test_credential_provider_instead_of_username_and_password(
        self, use_config, redis_cls, credential_provider
    ):
        # a password file for an entraid cluster is ignored
        use_config(
            cluster("ent", hosts="h:6379", entraid=True),
            passwords={"ent_pass": "ignored"},
        )

        RedisConnectionManager.get_connection()

        credential_provider.assert_called_once_with()
        redis_cls.assert_called_once_with(
            host="h",
            port=6379,
            db=0,
            credential_provider=credential_provider.provider,
            **DEFAULTS,
        )

    def test_cluster_mode(self, use_config, cluster_cls, credential_provider):
        use_config(cluster("ent", mode="cluster", hosts="h1:6379", entraid=True))

        RedisConnectionManager.get_connection()

        cluster_cls.assert_called_once_with(
            startup_nodes=[ClusterNode("h1", 6379)],
            credential_provider=credential_provider.provider,
            **DEFAULTS,
        )

    def test_not_used_without_entraid(self, use_config, redis_cls, credential_provider):
        use_config(cluster("plain"), passwords={"plain_pass": "pw"})

        RedisConnectionManager.get_connection()

        credential_provider.assert_not_called()
        assert "credential_provider" not in redis_cls.call_args.kwargs

    def test_provider_error_is_raised_and_not_cached(
        self, use_config, redis_cls, credential_provider
    ):
        from src.common.entraid_auth import EntraIDAuthenticationError

        use_config(cluster("ent", entraid=True))
        credential_provider.side_effect = [
            EntraIDAuthenticationError("bad tenant"),
            credential_provider.provider,
        ]

        with pytest.raises(EntraIDAuthenticationError, match="bad tenant"):
            RedisConnectionManager.get_connection()
        redis_cls.assert_not_called()

        RedisConnectionManager.get_connection()
        assert credential_provider.call_count == 2
        redis_cls.assert_called_once()

    @pytest.mark.xfail(
        strict=True,
        reason="entraid: true without an Entra ID auth flow (REDIS_ENTRAID_AUTH_FLOW / --entraid-auth-flow):"
        " create_credential_provider() returns None and the client connects without any authentication",
    )
    def test_entraid_without_auth_flow_does_not_connect_unauthenticated(
        self, use_config, redis_cls
    ):
        with patch.dict("src.common.config.ENTRAID_CFG", {"auth_flow": None}):
            try:
                use_config(
                    cluster("ent", entraid=True), passwords={"ent_pass": "ignored"}
                )
                RedisConnectionManager.get_connection()
            except Exception:  # refusing the cluster (at startup or on use) is correct
                return

        assert redis_cls.call_args.kwargs["credential_provider"] is not None


class TestClientKwargsHelpers:
    def test_client_kwargs_does_not_mutate_options(self, use_config):
        config = use_config(cluster(options={"socket_timeout": 1}))
        c = config.clusters[0]

        kwargs = client_kwargs(c, True)
        kwargs["socket_timeout"] = 99

        assert c.options == {"socket_timeout": 1}
        assert client_kwargs(c, True)["socket_timeout"] == 1

    def test_create_client_bypasses_the_cache(self, use_config, redis_cls):
        config = use_config(cluster())

        first = create_client(config.clusters[0])
        second = create_client(config.clusters[0])

        assert first is not second
        assert redis_cls.call_count == 2
        assert RedisConnectionManager._clients == {}


class TestRealClientConstruction:
    """The real redis-py classes accept the arguments (these clients connect lazily, so no server is needed)."""

    def test_standalone_with_tls_auth_and_options(self, use_config):
        use_config(
            cluster(
                "a",
                hosts="h:6380",
                db=1,
                username="u",
                tls={"caFile": "/ca.crt", "certReqs": "optional"},
                options={"health_check_interval": 30, "max_connections": 7},
            ),
            passwords={"a_pass": "pw"},
        )

        client = RedisConnectionManager.get_connection()

        assert type(client) is Redis
        pool = client.connection_pool
        assert pool.connection_class is redis.connection.SSLConnection
        assert pool.max_connections == 7
        expected = {
            "host": "h",
            "port": 6380,
            "db": 1,
            "username": "u",
            "password": "pw",
            "decode_responses": True,
            "lib_name": LIB_NAME,
            "socket_connect_timeout": 5,
            "socket_timeout": 10,
            "health_check_interval": 30,
            "ssl_ca_certs": "/ca.crt",
            "ssl_cert_reqs": "optional",
        }
        kwargs = pool.connection_kwargs
        assert {k: kwargs.get(k) for k in expected} == expected

    def test_sentinel_master_and_sentinels(self, use_config):
        use_config(
            cluster(
                "x",
                mode="sentinel",
                hosts="s1:26379,s2:26380",
                masterName="mymaster",
                db=2,
                tls=True,
            ),
            passwords={"x_pass": "pw", "x_sentinel_pass": "spw"},
        )

        client = RedisConnectionManager.get_connection(decode_responses=False)

        assert type(client) is Redis
        pool = client.connection_pool
        assert isinstance(pool, redis.sentinel.SentinelConnectionPool)
        assert pool.service_name == "mymaster"
        assert pool.connection_class is redis.sentinel.SentinelManagedSSLConnection
        assert pool.max_connections == 10
        assert pool.connection_kwargs["db"] == 2
        assert pool.connection_kwargs["password"] == "pw"
        assert pool.connection_kwargs["decode_responses"] is False
        sentinels = pool.sentinel_manager.sentinels
        assert [
            (
                s.connection_pool.connection_kwargs["host"],
                s.connection_pool.connection_kwargs["port"],
            )
            for s in sentinels
        ] == [("s1", 26379), ("s2", 26380)]
        for s in sentinels:
            assert s.connection_pool.connection_class is redis.connection.SSLConnection
            kwargs = s.connection_pool.connection_kwargs
            assert kwargs["password"] == "spw"
            assert kwargs["socket_connect_timeout"] == 5
            assert kwargs["socket_timeout"] == 10


class TestClientCache:
    def test_one_client_per_cluster_is_reused(self, use_config, redis_cls):
        use_config(cluster("a"))

        first = RedisConnectionManager.get_connection()
        second = RedisConnectionManager.get_connection()
        named = RedisConnectionManager.get_connection(cluster="a")

        assert first is second is named
        redis_cls.assert_called_once()

    def test_decode_responses_false_gets_its_own_client(self, use_config, redis_cls):
        """Regression: get_connection(decode_responses=False) used to return the cached decoding client."""
        use_config(cluster("a"))

        text = RedisConnectionManager.get_connection()
        raw = RedisConnectionManager.get_connection(decode_responses=False)

        assert raw is not text
        assert [c.kwargs["decode_responses"] for c in redis_cls.call_args_list] == [
            True,
            False,
        ]
        # both are cached under their own key
        assert RedisConnectionManager.get_connection(decode_responses=False) is raw
        assert RedisConnectionManager.get_connection() is text
        assert redis_cls.call_count == 2

    def test_decode_responses_false_first(self, use_config, redis_cls):
        use_config(cluster("a"))

        raw = RedisConnectionManager.get_connection(decode_responses=False)
        text = RedisConnectionManager.get_connection()

        assert raw is not text
        assert redis_cls.call_args.kwargs["decode_responses"] is True

    def test_each_cluster_has_its_own_client(self, use_config, redis_cls):
        use_config(cluster("a"), cluster("b"))

        a = RedisConnectionManager.get_connection(cluster="a")
        b = RedisConnectionManager.get_connection(cluster="b")

        assert a is not b
        assert [c.kwargs["host"] for c in redis_cls.call_args_list] == [
            "a.example",
            "b.example",
        ]
        assert RedisConnectionManager.get_connection(cluster="a") is a
        assert RedisConnectionManager.get_connection(cluster="b") is b
        assert redis_cls.call_count == 2

    def test_failed_creation_is_not_cached(self, use_config, redis_cls, caplog):
        use_config(cluster("a"))
        created = Mock(spec=Redis)
        redis_cls.side_effect = [ConnectionError("Connection refused"), created]

        with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
            with pytest.raises(ConnectionError, match="Connection refused"):
                RedisConnectionManager.get_connection()
        assert "Failed to create the Redis client for cluster a" in caplog.text
        assert "Connection refused" in caplog.text

        assert RedisConnectionManager.get_connection() is created
        assert RedisConnectionManager.get_connection() is created
        assert redis_cls.call_count == 2

    def test_failed_cluster_creation_is_not_cached(self, use_config, cluster_cls):
        use_config(cluster("c", mode="cluster", hosts="h1:7000"))
        created = Mock(spec=RedisCluster)
        cluster_cls.side_effect = [
            redis.exceptions.RedisClusterException("no slots"),
            created,
        ]

        with pytest.raises(redis.exceptions.RedisClusterException):
            RedisConnectionManager.get_connection()

        assert RedisConnectionManager.get_connection() is created
        assert cluster_cls.call_count == 2

    def test_failure_of_one_cluster_does_not_affect_another(
        self, use_config, redis_cls
    ):
        use_config(cluster("bad"), cluster("good"))

        def create(**kwargs):
            if kwargs["host"] == "bad.example":
                raise ConnectionError("down")
            return Mock(spec=Redis)

        redis_cls.side_effect = create

        with pytest.raises(ConnectionError):
            RedisConnectionManager.get_connection(cluster="bad")
        good = RedisConnectionManager.get_connection(cluster="good")

        assert RedisConnectionManager.get_connection(cluster="good") is good

    def test_reset_closes_all_clients(self, use_config, redis_cls, cluster_cls):
        use_config(cluster("a"), cluster("b", mode="cluster", hosts="h1:7000"))
        clients = [
            RedisConnectionManager.get_connection(cluster="a"),
            RedisConnectionManager.get_connection(cluster="a", decode_responses=False),
            RedisConnectionManager.get_connection(cluster="b"),
        ]

        RedisConnectionManager.reset()

        for client in clients:
            client.close.assert_called_once_with()
        assert RedisConnectionManager._clients == {}
        assert RedisConnectionManager._locks == {}

    def test_reset_creates_new_clients_afterwards(self, use_config, redis_cls):
        use_config(cluster("a"))
        before = RedisConnectionManager.get_connection()

        RedisConnectionManager.reset()
        after = RedisConnectionManager.get_connection()

        assert after is not before
        assert redis_cls.call_count == 2

    def test_reset_ignores_close_errors(self, use_config, redis_cls):
        use_config(cluster("a"), cluster("b"))
        a = RedisConnectionManager.get_connection(cluster="a")
        b = RedisConnectionManager.get_connection(cluster="b")
        a.close.side_effect = ConnectionError("already gone")

        RedisConnectionManager.reset()

        a.close.assert_called_once_with()
        b.close.assert_called_once_with()
        assert RedisConnectionManager._clients == {}

    def test_reset_without_clients(self):
        RedisConnectionManager.reset()

        assert RedisConnectionManager._clients == {}


class TestClusterSelection:
    def test_only_cluster_is_the_default(self, use_config):
        use_config(cluster("only"))

        assert RedisConnectionManager.cluster_config().name == "only"

    def test_explicit_name(self, use_config):
        use_config(cluster("a"), cluster("b"))

        assert RedisConnectionManager.cluster_config("b").name == "b"

    def test_context_var(self, use_config, redis_cls):
        use_config(cluster("a"), cluster("b"))
        token = current_cluster.set("b")
        try:
            assert RedisConnectionManager.cluster_config().name == "b"
            RedisConnectionManager.get_connection()
        finally:
            current_cluster.reset(token)

        assert redis_cls.call_args.kwargs["host"] == "b.example"

    def test_explicit_name_wins_over_context_var(self, use_config, redis_cls):
        use_config(cluster("a"), cluster("b"))
        token = current_cluster.set("b")
        try:
            assert RedisConnectionManager.cluster_config("a").name == "a"
            RedisConnectionManager.get_connection(cluster="a")
        finally:
            current_cluster.reset(token)

        assert redis_cls.call_args.kwargs["host"] == "a.example"

    def test_context_var_wins_over_the_only_cluster(self, use_config):
        use_config(cluster("only"))
        token = current_cluster.set("other")
        try:
            with pytest.raises(ClusterSelectionError, match="Unknown cluster: other"):
                RedisConnectionManager.cluster_config()
        finally:
            current_cluster.reset(token)

    def test_context_var_default_is_none(self):
        assert current_cluster.get() is None

    def test_missing_selection_with_several_clusters(self, use_config, redis_cls):
        use_config(cluster("cbtstrds"), cluster("cbprdrds"))

        with pytest.raises(ClusterSelectionError) as exc:
            RedisConnectionManager.get_connection()

        message = str(exc.value)
        assert message.startswith("Missing parameter: cluster.")
        assert "cbtstrds" in message
        assert "cbprdrds" in message
        assert message == (
            "Missing parameter: cluster. Valid values: ['cbtstrds', 'cbprdrds']"
        )
        redis_cls.assert_not_called()

    def test_unknown_cluster(self, use_config, redis_cls):
        use_config(cluster("a"), cluster("b"))

        with pytest.raises(ClusterSelectionError) as exc:
            RedisConnectionManager.get_connection(cluster="nope")

        assert str(exc.value) == "Unknown cluster: nope. Valid values: ['a', 'b']"
        redis_cls.assert_not_called()

    def test_unknown_cluster_with_a_single_cluster(self, use_config):
        use_config(cluster("a"))

        with pytest.raises(ClusterSelectionError, match="Unknown cluster: b"):
            RedisConnectionManager.get_connection(cluster="b")

    def test_selection_error_is_a_value_error(self):
        assert issubclass(ClusterSelectionError, ValueError)

    def test_legacy_config_without_set_config(self, redis_cls):
        """Without a config file the REDIS_* settings describe the single 'default' cluster."""
        legacy = {
            "host": "legacy.example",
            "port": 6390,
            "db": 4,
            "username": "legacy-user",
            "password": "legacy-pw",
            "ssl": False,
            "cluster_mode": False,
        }
        with (
            patch.dict("src.common.config.REDIS_CFG", legacy),
            patch("src.common.config.is_entraid_auth_enabled", return_value=False),
        ):
            assert RedisConnectionManager.cluster_config().name == "default"
            RedisConnectionManager.get_connection()

        redis_cls.assert_called_once_with(
            host="legacy.example",
            port=6390,
            db=4,
            username="legacy-user",
            password="legacy-pw",
            **DEFAULTS,
        )


class TestConcurrency:
    def test_slow_cluster_does_not_block_another_cluster(self, use_config, redis_cls):
        use_config(cluster("slow"), cluster("fast"))
        started = threading.Event()
        release = threading.Event()

        def create(**kwargs):
            if kwargs["host"] == "slow.example":
                started.set()
                release.wait(WAIT_SECONDS * 2)
            return Mock(spec=Redis)

        redis_cls.side_effect = create
        results = {}

        def connect(name):
            results[name] = RedisConnectionManager.get_connection(cluster=name)

        slow = threading.Thread(target=connect, args=("slow",), daemon=True)
        fast = threading.Thread(target=connect, args=("fast",), daemon=True)
        slow.start()
        try:
            assert started.wait(WAIT_SECONDS), "the slow creation never started"
            fast.start()
            fast.join(WAIT_SECONDS)

            assert not fast.is_alive(), "get_connection('fast') waited for 'slow'"
            assert "fast" in results
            assert "slow" not in results
        finally:
            release.set()
            slow.join(WAIT_SECONDS)
            fast.join(WAIT_SECONDS)

        assert not slow.is_alive()
        assert "slow" in results

    def test_slow_creation_does_not_block_the_other_decode_mode(
        self, use_config, redis_cls
    ):
        use_config(cluster("a"))
        started = threading.Event()
        release = threading.Event()

        def create(**kwargs):
            if kwargs["decode_responses"]:
                started.set()
                release.wait(WAIT_SECONDS * 2)
            return Mock(spec=Redis)

        redis_cls.side_effect = create
        results = {}

        def connect(decode):
            results[decode] = RedisConnectionManager.get_connection(
                decode_responses=decode
            )

        slow = threading.Thread(target=connect, args=(True,), daemon=True)
        fast = threading.Thread(target=connect, args=(False,), daemon=True)
        slow.start()
        try:
            assert started.wait(WAIT_SECONDS)
            fast.start()
            fast.join(WAIT_SECONDS)

            assert not fast.is_alive()
            assert False in results
        finally:
            release.set()
            slow.join(WAIT_SECONDS)
            fast.join(WAIT_SECONDS)

        assert not slow.is_alive()
        assert results[True] is not results[False]

    def test_concurrent_calls_for_one_cluster_create_one_client(
        self, use_config, redis_cls
    ):
        use_config(cluster("a"))
        started = threading.Event()
        release = threading.Event()

        def create(**kwargs):
            started.set()
            release.wait(WAIT_SECONDS * 2)
            return Mock(spec=Redis)

        redis_cls.side_effect = create
        results = []

        def connect():
            results.append(RedisConnectionManager.get_connection(cluster="a"))

        threads = [threading.Thread(target=connect, daemon=True) for _ in range(4)]
        threads[0].start()
        try:
            assert started.wait(WAIT_SECONDS)
            for t in threads[1:]:
                t.start()
            # the others wait for the cluster's lock instead of creating their own client
            threads[1].join(0.2)
            assert threads[1].is_alive()
        finally:
            release.set()
            for t in threads:
                t.join(WAIT_SECONDS)

        assert not any(t.is_alive() for t in threads)
        assert len(results) == 4
        assert all(r is results[0] for r in results)
        redis_cls.assert_called_once()


STANDALONE_PORT = 16379
PASSWORD_PORT = 16380
PASSWORD = "s3cret"  # nosec B105 - password of the local test server
CLUSTER_PORTS = (17000, 17001, 17002)


def _responds(port, password=None, cluster_mode=False):
    client = Redis(
        host="127.0.0.1",
        port=port,
        password=password,
        socket_connect_timeout=0.5,
        socket_timeout=0.5,
        # one attempt: without a server the probe must not spend seconds in backoff
        retry=Retry(NoBackoff(), 0),
    )
    try:
        if not client.ping():
            return False
        return not cluster_mode or client.cluster("INFO").get("cluster_state") == "ok"
    except redis.RedisError:
        return False
    finally:
        client.close()


@pytest.fixture
def standalone_port():
    if not _responds(STANDALONE_PORT):
        pytest.skip(f"no Redis on 127.0.0.1:{STANDALONE_PORT}")
    return STANDALONE_PORT


@pytest.fixture
def cluster_port():
    if not _responds(CLUSTER_PORTS[0], cluster_mode=True):
        pytest.skip(f"no Redis Cluster on 127.0.0.1:{CLUSTER_PORTS[0]}")
    return CLUSTER_PORTS[0]


@pytest.fixture
def password_port():
    if not _responds(PASSWORD_PORT, password=PASSWORD):
        pytest.skip(f"no Redis with requirepass on 127.0.0.1:{PASSWORD_PORT}")
    return PASSWORD_PORT


@pytest.fixture
def unique_key():
    """A unique key prefix; the keys written under it are deleted afterwards."""
    prefix = f"redis-mcp-test:{uuid.uuid4().hex}"
    used = []

    def _key(client, suffix=""):
        key = f"{prefix}:{suffix}"
        used.append((client, key))
        return key

    yield _key
    for client, key in used:
        try:
            client.delete(key)
        except redis.RedisError:
            pass


@pytest.mark.integration
class TestRealClients:
    def test_standalone_set_get(self, standalone_port, unique_key):
        set_config(
            parse_config(
                {
                    "clusters": [
                        {"name": "local", "hosts": f"127.0.0.1:{standalone_port}"}
                    ]
                }
            )
        )

        client = RedisConnectionManager.get_connection()
        key = unique_key(client, "s")
        assert client.set(key, "value") is True
        assert client.get(key) == "value"

        raw = RedisConnectionManager.get_connection(decode_responses=False)
        assert raw is not client
        assert raw.get(key) == b"value"
        assert type(client) is Redis

    def test_cluster_set_get(self, cluster_port, unique_key):
        set_config(
            parse_config(
                {
                    "clusters": [
                        {
                            "name": "localcluster",
                            "mode": "cluster",
                            "hosts": [f"127.0.0.1:{p}" for p in CLUSTER_PORTS],
                        }
                    ]
                }
            )
        )

        client = RedisConnectionManager.get_connection()
        assert isinstance(client, RedisCluster)
        # keys spread over the slots, so more than one primary is used
        for i in range(10):
            key = unique_key(client, f"c{i}")
            assert client.set(key, f"v{i}") is True
            assert client.get(key) == f"v{i}"

        raw = RedisConnectionManager.get_connection(decode_responses=False)
        assert raw.get(unique_key(client, "c0")) == b"v0"

    def test_two_clusters_selected_by_name_and_context(
        self, standalone_port, cluster_port, unique_key
    ):
        set_config(
            parse_config(
                {
                    "clusters": [
                        {"name": "single", "hosts": f"127.0.0.1:{standalone_port}"},
                        {
                            "name": "multi",
                            "mode": "cluster",
                            "hosts": f"127.0.0.1:{cluster_port}",
                        },
                    ]
                }
            )
        )

        single = RedisConnectionManager.get_connection(cluster="single")
        token = current_cluster.set("multi")
        try:
            multi = RedisConnectionManager.get_connection()
        finally:
            current_cluster.reset(token)

        assert type(single) is Redis
        assert isinstance(multi, RedisCluster)
        key = unique_key(single, "shared")
        unique_key(multi, "shared")
        single.set(key, "on-single")
        multi.set(key, "on-multi")
        assert single.get(key) == "on-single"
        assert multi.get(key) == "on-multi"

        with pytest.raises(ClusterSelectionError):
            RedisConnectionManager.get_connection()

    def test_password_from_passwords_dir(
        self, password_port, tmp_path, monkeypatch, unique_key
    ):
        monkeypatch.delenv("authed_pass", raising=False)
        (tmp_path / "authed_pass").write_text(PASSWORD + "\n", encoding="utf-8")
        config = parse_config(
            {
                "passwordsDir": str(tmp_path),
                "clusters": [{"name": "authed", "hosts": f"127.0.0.1:{password_port}"}],
            }
        )
        resolve_passwords(config)
        set_config(config)

        client = RedisConnectionManager.get_connection()
        key = unique_key(client, "p")
        client.set(key, "secret-value")
        assert client.get(key) == "secret-value"

    def test_missing_password_fails_on_use(self, password_port, monkeypatch):
        monkeypatch.delenv("noauth_pass", raising=False)
        set_config(
            parse_config(
                {
                    "clusters": [
                        {"name": "noauth", "hosts": f"127.0.0.1:{password_port}"}
                    ]
                }
            )
        )

        client = RedisConnectionManager.get_connection()
        # redis-py retries the AuthenticationError (a ConnectionError) with backoff; skip the real waiting
        with patch("redis.retry.sleep"):
            with pytest.raises(AuthenticationError):
                client.ping()
