"""
Tests of src/common/connection.py: the client arguments per mode (with mocked redis-py classes), the retry policy
over real sockets (a fake RESP server), and real clients and tool calls against redis-server processes started
for this module (see tests/redis_servers.py; skipped where redis-server / redis-cli are not installed).
"""

import logging
import secrets
import socket
import threading
import time
import uuid
from typing import Callable, List, Optional
from unittest.mock import Mock, patch

import pytest
import redis
from redis import Redis
from redis.backoff import NoBackoff
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import AuthenticationError, ConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from src.common import registry
from src.common.config_loader import (
    ConfigError,
    parse_config,
    resolve_passwords,
    set_config,
)
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
from src.common.entraid_auth import EntraIDAuthenticationError
from src.common.registry import MAX_CONCURRENT_CALLS_PER_CLUSTER, TOOL_TIMEOUT_SECONDS
from src.common.server import build_server, load_tools
from src.version import __version__
from tests import redis_servers

LOGGER_NAME = "src.common.connection"
LIB_NAME = f"redis-py(mcp-server_v{__version__})"

# Arguments every client gets unless the cluster's options override them.
DEFAULTS = {
    "decode_responses": True,
    "lib_name": LIB_NAME,
    "socket_connect_timeout": 5,
    "socket_timeout": 10,
    "max_connections": 16,
}

# Retry policy of standalone and sentinel clients (and of the sentinels themselves). Retry compares backoff,
# retries and supported errors, so these work in assert_called_once_with; assert_command_retry checks the details.
COMMAND_RETRY = Retry(NoBackoff(), 1, supported_errors=(ConnectionError,))
# RedisCluster: a single attempt
CLUSTER_RETRY = Retry(NoBackoff(), 0)

# Waits on the threading Events below; generous, but bounded so a broken lock cannot hang the run.
WAIT_SECONDS = 5


def assert_command_retry(retry):
    """One immediate retry when the connection drops, none on a timeout: a command that timed out may have run,
    and sending a write again would apply it twice."""
    assert type(retry) is Retry
    assert type(retry._backoff) is NoBackoff
    assert retry.get_retries() == 1
    assert set(retry._supported_errors) == {ConnectionError}
    assert RedisTimeoutError not in retry._supported_errors
    assert socket.timeout not in retry._supported_errors


def assert_cluster_retry(retry):
    assert type(retry) is Retry
    assert type(retry._backoff) is NoBackoff
    assert retry.get_retries() == 0


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
    """Patched Entra ID credential provider factory (imported by connection.py on use), with an auth flow
    configured (entraid clusters need one)."""
    provider = object()
    with (
        patch.dict("src.common.config.ENTRAID_CFG", {"auth_flow": "managed_identity"}),
        patch(
            "src.common.entraid_auth.create_credential_provider", return_value=provider
        ) as factory,
    ):
        factory.provider = provider
        yield factory


# FakeRedisServer: reply that closes the connection instead of answering
DROP = b"<drop>"
PONG = b"+PONG\r\n"


def _read_command(reader) -> Optional[List[str]]:
    """One RESP command (an array of bulk strings, or an inline command); None at the end of the stream."""
    line = reader.readline()
    if not line:
        return None
    if not line.startswith(b"*"):
        return line.decode().split()
    args = []
    for _ in range(int(line[1:])):
        length = int(reader.readline()[1:])
        args.append(reader.read(length + 2)[:-2].decode())
    return args


class FakeRedisServer:
    """A RESP server on a free local port, so the retry behaviour can be tested with real sockets everywhere
    (no redis-server needed). It answers the connection handshake (CLIENT SETINFO, ...) with +OK and every
    other command as ``respond(n)`` says, n being the command's number (1-based): the reply, None to never
    answer (a hung server), or DROP to close the connection (a restarted server)."""

    HANDSHAKE = {"CLIENT", "SELECT", "AUTH", "HELLO"}

    def __init__(self, respond: Callable[[int], Optional[bytes]]):
        self.respond = respond
        self.commands: List[List[str]] = []
        self.connections = 0
        self._lock = threading.Lock()
        self._sockets: List[socket.socket] = []
        self._stopped = threading.Event()
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(0.05)
        self.port = self._listener.getsockname()[1]
        self._acceptor = threading.Thread(target=self._accept, daemon=True)
        self._acceptor.start()

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def _accept(self) -> None:
        while not self._stopped.is_set():
            try:
                conn, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            conn.settimeout(None)
            with self._lock:
                self.connections += 1
                self._sockets.append(conn)
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        reader = conn.makefile("rb")
        try:
            while True:
                command = _read_command(reader)
                if command is None:
                    return
                if command[0].upper() in self.HANDSHAKE:
                    conn.sendall(b"+OK\r\n")
                    continue
                with self._lock:
                    self.commands.append(command)
                    number = len(self.commands)
                reply = self.respond(number)
                if reply == DROP:
                    conn.shutdown(socket.SHUT_RDWR)
                    return
                if reply is not None:
                    conn.sendall(reply)
        except (OSError, ValueError):  # closed by close()
            return
        finally:
            reader.close()
            conn.close()

    def close(self) -> None:
        self._stopped.set()
        self._acceptor.join(5)
        self._listener.close()
        with self._lock:
            sockets = list(self._sockets)
        for conn in sockets:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()


@pytest.fixture
def fake_server():
    """Starts FakeRedisServer(respond); all of them are closed after the test."""
    servers = []

    def _start(respond):
        server = FakeRedisServer(respond)
        servers.append(server)
        return server

    yield _start
    for server in servers:
        server.close()


class TestTimeouts:
    def test_defaults(self):
        assert SOCKET_CONNECT_TIMEOUT_SECONDS == 5
        assert SOCKET_TIMEOUT_SECONDS == 10
        assert MAX_CONNECTIONS == 16

    def test_client_timeouts_stay_below_the_tool_timeout(self):
        """A slow cluster must fail with its own error, not the generic tool timeout."""
        assert SOCKET_CONNECT_TIMEOUT_SECONDS < TOOL_TIMEOUT_SECONDS
        assert SOCKET_TIMEOUT_SECONDS < TOOL_TIMEOUT_SECONDS

    def test_pool_has_room_for_the_concurrent_tool_calls(self):
        """Every worker the registry runs for a cluster gets a connection, with room left for pub/sub."""
        assert MAX_CONNECTIONS > MAX_CONCURRENT_CALLS_PER_CLUSTER

    def test_unreachable_cluster_fails_before_the_tool_timeout(self, use_config):
        """Regression: redis-py's default Retry (3 retries, exponential backoff) repeated the 5 s connect
        timeout, so an unreachable standalone cluster took 20-34 s to fail, longer than the 15 s tool timeout.

        A host that never answers (firewalled, blackholed) must fail with the client's own TimeoutError after a
        single connect attempt. Time is simulated (no blackhole address is reliable everywhere): each connect
        attempt costs its connect timeout and each backoff its sleep, nothing really waits."""
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
            with pytest.raises(RedisTimeoutError, match="Timeout connecting"):
                client.ping()

        assert attempts == [SOCKET_CONNECT_TIMEOUT_SECONDS]
        assert sleeps == []
        total = sum(attempts) + sum(sleeps)
        assert total < TOOL_TIMEOUT_SECONDS, (
            f"{len(attempts)} connect attempts of {SOCKET_CONNECT_TIMEOUT_SECONDS} s"
            f" and {sum(sleeps):.1f} s of backoff: {total:.1f} s"
        )

    def test_hung_server_fails_after_one_socket_timeout(self, use_config, fake_server):
        """A server that accepts connections but stops answering (overloaded, stuck): the command is sent once
        and fails with the client's own TimeoutError after one socket timeout. redis-py's default Retry sent it
        again on new connections (4 times, 40 s with the default 10 s socket timeout)."""
        server = fake_server(lambda n: PONG if n == 1 else None)
        timeout = 0.3
        use_config(
            cluster(
                "hung",
                hosts=server.address,
                options={"socket_timeout": timeout, "socket_connect_timeout": timeout},
            )
        )
        client = RedisConnectionManager.get_connection()
        assert client.ping() is True  # the pool now holds a connected connection

        started = time.monotonic()
        with pytest.raises(RedisTimeoutError, match="Timeout reading"):
            client.ping()
        elapsed = time.monotonic() - started

        assert [c[0] for c in server.commands] == ["PING", "PING"]
        assert server.connections == 1
        assert elapsed >= timeout
        # with the default socket timeout the single attempt ends before the tool timeout
        attempts = len(server.commands) - 1
        assert attempts * SOCKET_TIMEOUT_SECONDS < TOOL_TIMEOUT_SECONDS

    def test_dropped_connection_is_retried_once(self, use_config, fake_server):
        """A connection the server closed (e.g. after a Redis restart) is replaced and the command sent again."""
        server = fake_server(lambda n: DROP if n == 2 else PONG)
        use_config(cluster("restarted", hosts=server.address))
        client = RedisConnectionManager.get_connection()
        assert client.ping() is True

        assert client.ping() is True

        assert [c[0] for c in server.commands] == ["PING", "PING", "PING"]
        assert server.connections == 2

    def test_connection_dropped_again_fails(self, use_config, fake_server):
        server = fake_server(lambda n: PONG if n == 1 else DROP)
        use_config(cluster("flapping", hosts=server.address))
        client = RedisConnectionManager.get_connection()
        assert client.ping() is True

        with pytest.raises(ConnectionError):
            client.ping()

        # the first attempt and one retry
        assert [c[0] for c in server.commands] == ["PING", "PING", "PING"]
        assert server.connections == 2


class TestStandaloneClient:
    def test_exact_kwargs(self, use_config, redis_cls):
        use_config(cluster("cbtstrds", hosts="redis.example:6390", db=2))

        RedisConnectionManager.get_connection()

        redis_cls.assert_called_once_with(
            host="redis.example", port=6390, db=2, retry=COMMAND_RETRY, **DEFAULTS
        )
        assert_command_retry(redis_cls.call_args.kwargs["retry"])

    def test_each_client_gets_its_own_retry(self, use_config, redis_cls):
        """Retry objects are mutable (update_supported_errors); clients must not share one."""
        use_config(cluster("a"), cluster("b"))

        RedisConnectionManager.get_connection(cluster="a")
        RedisConnectionManager.get_connection(cluster="b")
        RedisConnectionManager.get_connection(cluster="a", decode_responses=False)

        retries = [c.kwargs["retry"] for c in redis_cls.call_args_list]
        assert len({id(r) for r in retries}) == 3
        for retry in retries:
            assert_command_retry(retry)

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
            host="h",
            port=6379,
            db=0,
            retry=COMMAND_RETRY,
            **{**DEFAULTS, "decode_responses": False},
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
            retry=COMMAND_RETRY,
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
            retry=COMMAND_RETRY,
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
            host="h",
            port=6380,
            db=0,
            retry=COMMAND_RETRY,
            ssl=True,
            ssl_cert_reqs="required",
            **DEFAULTS,
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
                    "socket_connect_timeout": 1,
                    "max_connections": 50,
                    "health_check_interval": "30",
                    "client_name": "mcp",
                    "socket_keepalive": True,
                },
            )
        )

        RedisConnectionManager.get_connection()

        redis_cls.assert_called_once_with(
            host="h",
            port=6379,
            db=0,
            retry=COMMAND_RETRY,
            decode_responses=True,
            lib_name=LIB_NAME,
            socket_connect_timeout=1,
            socket_timeout=2.5,
            max_connections=50,
            health_check_interval=30,
            client_name="mcp",
            socket_keepalive=True,
        )

    @pytest.mark.parametrize(
        "option",
        [
            "retry",
            "retry_on_timeout",
            "retry_on_error",
            "ssl_cert_reqs",
            "decode_responses",
            "password",
            "credential_provider",
            "host",
            "connection_pool",
        ],
    )
    def test_options_cannot_replace_what_the_server_manages(
        self, use_config, redis_cls, option
    ):
        """The retry policy (no resend after a timeout), TLS, decoding, secrets and the address are set by the
        server or by the cluster's own keys; `options` cannot change them."""
        with pytest.raises(ConfigError, match=f"options {option} not supported"):
            use_config(cluster(tls=True, options={option: "x"}))

        redis_cls.assert_not_called()


class TestRedisClusterClient:
    def test_startup_nodes_and_no_db(self, use_config, cluster_cls, redis_cls):
        use_config(
            cluster("cbprdrds", mode="cluster", hosts=["h1:6379", "h2:6380"]),
            passwords={"cbprdrds_pass": "pw"},
        )

        client = RedisConnectionManager.get_connection()

        cluster_cls.assert_called_once_with(
            startup_nodes=[ClusterNode("h1", 6379), ClusterNode("h2", 6380)],
            retry=CLUSTER_RETRY,
            password="pw",
            **DEFAULTS,
        )
        assert_cluster_retry(cluster_cls.call_args.kwargs["retry"])
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
            retry=CLUSTER_RETRY,
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
                "retry": COMMAND_RETRY,
                "password": "spw",
                **tls,
            },
        )
        assert_command_retry(sentinel_cls.call_args.kwargs["sentinel_kwargs"]["retry"])
        sentinel = sentinel_cls.return_value
        sentinel.master_for.assert_called_once_with(
            "mymaster",
            db=3,
            retry=COMMAND_RETRY,
            username="app",
            password="pw",
            **tls,
            **DEFAULTS,
        )
        assert_command_retry(sentinel.master_for.call_args.kwargs["retry"])
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
            sentinel_kwargs={
                "socket_connect_timeout": 5,
                "socket_timeout": 10,
                "retry": COMMAND_RETRY,
            },
        )
        sentinel_cls.return_value.master_for.assert_called_once_with(
            "m",
            db=0,
            retry=COMMAND_RETRY,
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
            "retry": COMMAND_RETRY,
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
            retry=COMMAND_RETRY,
            credential_provider=credential_provider.provider,
            **DEFAULTS,
        )

    def test_cluster_mode(self, use_config, cluster_cls, credential_provider):
        use_config(cluster("ent", mode="cluster", hosts="h1:6379", entraid=True))

        RedisConnectionManager.get_connection()

        cluster_cls.assert_called_once_with(
            startup_nodes=[ClusterNode("h1", 6379)],
            retry=CLUSTER_RETRY,
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

    def test_entraid_without_auth_flow_is_rejected_at_startup(
        self, use_config, redis_cls
    ):
        """Without REDIS_ENTRAID_AUTH_FLOW / --entraid-auth-flow an entraid cluster would have no credentials."""
        with patch.dict("src.common.config.ENTRAID_CFG", {"auth_flow": None}):
            with pytest.raises(
                ConfigError,
                match="clusters ent set entraid: true, but no Entra ID auth flow is configured",
            ):
                use_config(
                    cluster("ent", entraid=True),
                    cluster("plain"),
                    passwords={"ent_pass": "ignored"},
                )

        redis_cls.assert_not_called()

    @pytest.mark.parametrize(
        "fields",
        [{}, {"mode": "cluster"}, {"mode": "sentinel", "masterName": "m"}],
        ids=["standalone", "cluster", "sentinel"],
    )
    def test_entraid_without_auth_flow_does_not_connect_unauthenticated(
        self, redis_cls, cluster_cls, sentinel_cls, fields
    ):
        """Regression: create_credential_provider() returns None without an auth flow, and the client was
        created without any authentication. A config that skipped the startup check must still not connect."""
        set_config(parse_config({"clusters": [cluster("ent", entraid=True, **fields)]}))

        with patch.dict("src.common.config.ENTRAID_CFG", {"auth_flow": None}):
            with pytest.raises(
                EntraIDAuthenticationError,
                match="cluster ent sets entraid: true, but no Entra ID auth flow is configured",
            ):
                RedisConnectionManager.get_connection()

        redis_cls.assert_not_called()
        cluster_cls.assert_not_called()
        sentinel_cls.assert_not_called()
        assert RedisConnectionManager._clients == {}


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
        # redis-py copies the Retry into each connection without adding TimeoutError (no retry_on_timeout)
        assert_command_retry(kwargs["retry"])
        assert_command_retry(pool.make_connection().retry)

    def test_standalone_defaults(self, use_config):
        use_config(cluster("a", hosts="h:6379"))

        client = RedisConnectionManager.get_connection()

        pool = client.connection_pool
        assert pool.max_connections == MAX_CONNECTIONS
        assert pool.connection_kwargs["socket_connect_timeout"] == 5
        assert pool.connection_kwargs["socket_timeout"] == 10
        assert_command_retry(pool.make_connection().retry)

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
        assert pool.max_connections == MAX_CONNECTIONS
        assert_command_retry(pool.connection_kwargs["retry"])
        assert_command_retry(pool.make_connection().retry)
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
            assert_command_retry(kwargs["retry"])


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
            retry=COMMAND_RETRY,
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


# Real servers, started for this module on free ports (skipped where the Redis binaries are not installed).
PASSWORD = "pw-" + secrets.token_hex(12)


@pytest.fixture(scope="module")
def standalone_server(tmp_path_factory):
    redis_servers.skip_unless_installed()
    with redis_servers.running(tmp_path_factory.mktemp("standalone")) as server:
        yield server


@pytest.fixture(scope="module")
def password_server(tmp_path_factory):
    redis_servers.skip_unless_installed()
    with redis_servers.running(
        tmp_path_factory.mktemp("requirepass"), password=PASSWORD
    ) as server:
        yield server


@pytest.fixture(scope="module")
def redis_cluster(tmp_path_factory):
    """Three primaries joined with redis-cli --cluster create."""
    redis_servers.skip_unless_installed(cluster=True)
    with redis_servers.running_cluster(tmp_path_factory.mktemp("cluster")) as nodes:
        yield nodes


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
    for client in {id(c): c for c, _ in used}.values():
        client.close()


@pytest.mark.integration
class TestRealClients:
    def test_standalone_set_get(self, standalone_server, unique_key):
        set_config(
            parse_config(
                {"clusters": [{"name": "local", "hosts": standalone_server.address}]}
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

    def test_cluster_set_get(self, redis_cluster, unique_key):
        set_config(
            parse_config(
                {
                    "clusters": [
                        {
                            "name": "localcluster",
                            "mode": "cluster",
                            "hosts": redis_cluster.addresses,
                        }
                    ]
                }
            )
        )

        client = RedisConnectionManager.get_connection()
        assert isinstance(client, RedisCluster)
        assert_cluster_retry(client.retry)
        keys = [unique_key(client, f"c{i}") for i in range(10)]
        for i, key in enumerate(keys):
            assert client.set(key, f"v{i}") is True
            assert client.get(key) == f"v{i}"
        # the keys spread over the slots, so more than one primary was used
        assert len({redis_cluster.node_for_key(key).port for key in keys}) > 1

        raw = RedisConnectionManager.get_connection(decode_responses=False)
        assert raw.get(unique_key(client, "c0")) == b"v0"

    def test_two_clusters_selected_by_name_and_context(
        self, standalone_server, redis_cluster, unique_key
    ):
        set_config(
            parse_config(
                {
                    "clusters": [
                        {"name": "single", "hosts": standalone_server.address},
                        {
                            "name": "multi",
                            "mode": "cluster",
                            "hosts": redis_cluster.addresses[0],
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
        self, password_server, tmp_path, monkeypatch, unique_key
    ):
        monkeypatch.delenv("authed_pass", raising=False)
        (tmp_path / "authed_pass").write_text(PASSWORD + "\n", encoding="utf-8")
        config = parse_config(
            {
                "passwordsDir": str(tmp_path),
                "clusters": [{"name": "authed", "hosts": password_server.address}],
            }
        )
        resolve_passwords(config)
        set_config(config)

        client = RedisConnectionManager.get_connection()
        key = unique_key(client, "p")
        client.set(key, "secret-value")
        assert client.get(key) == "secret-value"

    def test_missing_password_fails_on_use(self, password_server, monkeypatch):
        monkeypatch.delenv("noauth_pass", raising=False)
        set_config(
            parse_config(
                {"clusters": [{"name": "noauth", "hosts": password_server.address}]}
            )
        )

        client = RedisConnectionManager.get_connection()
        # no backoff to wait for: the retry policy retries a dropped connection once, immediately
        with pytest.raises(AuthenticationError):
            client.ping()


async def _call_tool(mcp, tool: str, **arguments) -> str:
    """The text a client gets back from the tool."""
    result = await mcp.call_tool(tool, arguments)
    content = result[0] if isinstance(result, tuple) else result
    return content[0].text


@pytest.fixture
def src_tools(monkeypatch):
    """Only the tools of src/tools, even if another test left a synthetic one in the registry."""
    load_tools()
    monkeypatch.setattr(
        registry,
        "_SPECS",
        {
            name: spec
            for name, spec in registry._SPECS.items()
            if spec.fn.__module__.startswith("src.tools.")
        },
    )


def _list_tool_server(cluster_config: dict):
    """build_server for one writable cluster with the data tools, the config active as at startup."""
    config = parse_config({"toolsets": ["core", "data"], "clusters": [cluster_config]})
    set_config(config)
    return build_server(config)


# Short enough to fail while the server sleeps (DEBUG SLEEP below), long enough for a loaded CI runner.
WRITE_SOCKET_TIMEOUT = 1
SLEEP_SECONDS = 2


def _list_after_wakeup(server: redis_servers.RedisServer, key: str) -> List[str]:
    """The list's elements once it exists: the timed-out push runs only when the server wakes up."""
    with server.client() as client:
        deadline = time.monotonic() + redis_servers.STARTUP_TIMEOUT
        while client.llen(key) == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        return client.lrange(key, 0, -1)


@pytest.mark.integration
class TestNoDoubleWrite:
    """Regression: redis-py's default Retry resends a command after a socket timeout. When the server was only
    slow, the first command still runs, so a timed-out write was applied twice (LPUSH: two elements)."""

    @pytest.mark.asyncio
    async def test_timed_out_lpush_is_not_sent_again(
        self, standalone_server, unique_key, src_tools
    ):
        mcp = _list_tool_server(
            {
                "name": "local",
                "hosts": standalone_server.address,
                "options": {"socket_timeout": WRITE_SOCKET_TIMEOUT},
            }
        )
        key = unique_key(standalone_server.client(), "list")
        # creates the cluster's client and leaves a connected connection in its pool, so the push below is
        # sent at once (a new connection would already time out in its handshake)
        assert await _call_tool(mcp, "llen", name=key) == "0"

        with redis_servers.debug_sleep(standalone_server, SLEEP_SECONDS):
            started = time.monotonic()
            result = await _call_tool(mcp, "lpush", name=key, value="once")
            elapsed = time.monotonic() - started

        # the timed-out push ran when the server woke up, exactly once (twice with redis-py's default Retry)
        elements = _list_after_wakeup(standalone_server, key)
        assert len(elements) == 1, elements
        assert elements == ["once"]
        # the client's own timeout error (not the tool timeout), no success after a resend
        assert "Timeout" in result, result
        assert result.startswith(f"Error pushing value to list '{key}'")
        assert "did not finish within" not in result
        assert WRITE_SOCKET_TIMEOUT <= elapsed < TOOL_TIMEOUT_SECONDS

    @pytest.mark.asyncio
    async def test_timed_out_lpush_is_not_sent_again_in_cluster_mode(
        self, redis_cluster, unique_key, src_tools
    ):
        mcp = _list_tool_server(
            {
                "name": "rc",
                "mode": "cluster",
                "hosts": redis_cluster.addresses,
                "options": {"socket_timeout": WRITE_SOCKET_TIMEOUT},
            }
        )
        key = unique_key(redis_cluster.client(), "list")
        node = redis_cluster.node_for_key(key)
        assert await _call_tool(mcp, "llen", name=key) == "0"

        with redis_servers.debug_sleep(node, SLEEP_SECONDS):
            started = time.monotonic()
            result = await _call_tool(mcp, "lpush", name=key, value="once")
            elapsed = time.monotonic() - started

        elements = _list_after_wakeup(node, key)
        assert len(elements) == 1, elements
        assert elements == ["once"]
        assert "Timeout" in result, result
        assert result.startswith(f"Error pushing value to list '{key}'")
        # one socket timeout, then the topology refresh (the other primaries answer at once)
        assert WRITE_SOCKET_TIMEOUT <= elapsed < TOOL_TIMEOUT_SECONDS
