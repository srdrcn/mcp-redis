"""
Unit tests for src/tools/clusters.py
"""

import json
import threading
import time
from unittest.mock import Mock, patch

import pytest
import redis
from redis.cluster import RedisCluster
from redis.exceptions import AuthenticationError, ConnectionError

import src.tools.clusters as clusters_module
from src.common import registry
from src.common.config_loader import parse_config, resolve_passwords, set_config
from src.common.connection import current_cluster
from src.tools.clusters import CHECK_BUDGET_SECONDS, clusters_list

PASSWORD = "pw-never-shown-7f3a"
SENTINEL_PASSWORD = "sentinel-pw-never-shown-91c2"


def _use_config(tmp_path, clusters, passwords=None, **root):
    """Activates a config built from YAML-style (camelCase) keys. ``passwords`` maps a secret file name
    (``<name>_pass``) to its content, as a mounted Secret would provide it."""
    passwords_dir = tmp_path / "passwords"
    passwords_dir.mkdir(exist_ok=True)
    for file_name, value in (passwords or {}).items():
        (passwords_dir / file_name).write_text(value, encoding="utf-8")
    config = parse_config(
        {"passwordsDir": str(passwords_dir), "clusters": clusters, **root}
    )
    resolve_passwords(config)
    set_config(config)
    return config


def _redis_mock(version="7.2.4", role="master", spec=redis.Redis):
    client = Mock(spec=spec)
    client.ping.return_value = True
    client.info.side_effect = lambda section: {
        "server": {"redis_version": version, "redis_mode": "standalone"},
        "replication": {"role": role, "connected_slaves": 0},
    }[section]
    return client


def _cluster_mock(primaries=3, version="7.2.4"):
    client = _redis_mock(version=version, spec=RedisCluster)
    client.get_primaries.return_value = [Mock() for _ in range(primaries)]
    return client


def _by_name(rows):
    return {row["name"]: row for row in rows}


@pytest.fixture
def connections():
    """Patches the probe's get_connection; map a cluster name to a client, or to an exception to raise."""
    clients = {}

    def get_connection(decode_responses=True, cluster=None):
        client = clients[cluster]
        if isinstance(client, BaseException):
            raise client
        return client

    with patch(
        "src.tools.clusters.RedisConnectionManager.get_connection",
        side_effect=get_connection,
    ) as mock_get_connection:
        mock_get_connection.clients = clients
        yield mock_get_connection


class TestDescribe:
    """The configuration part of each row (check=False)."""

    @pytest.mark.asyncio
    async def test_standalone_row(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "tst", "hosts": "redis-tst:6379", "db": 2}])

        rows = await clusters_list(check=False)

        assert rows == [
            {
                "name": "tst",
                "mode": "standalone",
                "hosts": "redis-tst:6379",
                "readOnly": False,
                "modules": [],
                "tls": False,
                "auth": "none",
                "db": 2,
            }
        ]
        connections.assert_not_called()

    @pytest.mark.asyncio
    async def test_cluster_mode_row_has_no_db(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [{"name": "prd", "mode": "cluster", "hosts": ["h1:6379", "h2:6379"]}],
        )

        (row,) = await clusters_list(check=False)

        assert row["mode"] == "cluster"
        assert row["hosts"] == "h1:6379,h2:6379"
        assert "db" not in row
        assert "masterName" not in row

    @pytest.mark.asyncio
    async def test_sentinel_row_has_master_name_and_db(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [
                {
                    "name": "sen",
                    "mode": "sentinel",
                    "hosts": "s1:26379,s2:26379",
                    "masterName": "mymaster",
                    "db": 3,
                }
            ],
        )

        (row,) = await clusters_list(check=False)

        assert row["mode"] == "sentinel"
        assert row["hosts"] == "s1:26379,s2:26379"
        assert row["masterName"] == "mymaster"
        assert row["db"] == 3

    @pytest.mark.asyncio
    async def test_non_sentinel_rows_have_no_master_name(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [
                {"name": "a", "hosts": "a:6379"},
                {"name": "b", "mode": "cluster", "hosts": "b:6379"},
            ],
        )

        rows = await clusters_list(check=False)

        assert all("masterName" not in row for row in rows)

    @pytest.mark.asyncio
    async def test_username_only_when_set(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [
                {"name": "with-user", "hosts": "a:6379", "username": "mcp-reader"},
                {"name": "without-user", "hosts": "b:6379"},
            ],
        )

        rows = _by_name(await clusters_list(check=False))

        assert rows["with-user"]["username"] == "mcp-reader"
        assert "username" not in rows["without-user"]

    @pytest.mark.asyncio
    async def test_auth(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [
                {"name": "pw", "hosts": "a:6379"},
                {"name": "open", "hosts": "b:6379"},
                {"name": "entra", "hosts": "c:6379", "entraid": True},
                # a stray password file does not turn an Entra ID cluster into a password one
                {"name": "entra-pw", "hosts": "d:6379", "entraid": True},
            ],
            passwords={"pw_pass": PASSWORD, "entra-pw_pass": PASSWORD},
        )

        rows = _by_name(await clusters_list(check=False))

        assert rows["pw"]["auth"] == "password"
        assert rows["open"]["auth"] == "none"
        assert rows["entra"]["auth"] == "entraid"
        assert rows["entra-pw"]["auth"] == "entraid"

    @pytest.mark.asyncio
    async def test_password_from_environment_counts_as_password(
        self, tmp_path, connections, monkeypatch
    ):
        monkeypatch.setenv("envpw_pass", PASSWORD)
        _use_config(tmp_path, [{"name": "envpw", "hosts": "a:6379"}])

        (row,) = await clusters_list(check=False)

        assert row["auth"] == "password"

    @pytest.mark.asyncio
    async def test_read_only_is_the_effective_value(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [
                {"name": "inherits", "hosts": "a:6379"},
                {"name": "writable", "hosts": "b:6379", "readOnly": False},
            ],
            readOnly=True,
        )

        rows = _by_name(await clusters_list(check=False))

        assert rows["inherits"]["readOnly"] is True
        assert rows["writable"]["readOnly"] is False

    @pytest.mark.asyncio
    async def test_cluster_read_only_overrides_writable_default(
        self, tmp_path, connections
    ):
        _use_config(
            tmp_path,
            [
                {"name": "inherits", "hosts": "a:6379"},
                {"name": "locked", "hosts": "b:6379", "readOnly": True},
            ],
        )

        rows = _by_name(await clusters_list(check=False))

        assert rows["inherits"]["readOnly"] is False
        assert rows["locked"]["readOnly"] is True

    @pytest.mark.asyncio
    async def test_tls_and_modules(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [
                {
                    "name": "secure",
                    "hosts": "a:6380",
                    "tls": {"caFile": "/etc/redis-mcp/tls/ca.crt"},
                    "modules": ["json", "search"],
                },
                {"name": "flag", "hosts": "b:6380", "tls": True},
                {"name": "plain", "hosts": "c:6379", "tls": False},
            ],
        )

        rows = _by_name(await clusters_list(check=False))

        assert rows["secure"]["tls"] is True
        assert rows["secure"]["modules"] == ["json", "search"]
        assert rows["flag"]["tls"] is True
        assert rows["plain"]["tls"] is False
        assert rows["plain"]["modules"] == []

    @pytest.mark.asyncio
    async def test_rows_keep_config_order(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [
                {"name": "zeta", "hosts": "z:6379"},
                {"name": "alpha", "hosts": "a:6379"},
                {"name": "mid", "hosts": "m:6379"},
            ],
        )

        rows = await clusters_list(check=False)

        assert [row["name"] for row in rows] == ["zeta", "alpha", "mid"]

    @pytest.mark.asyncio
    async def test_modules_are_a_copy(self, tmp_path, connections):
        config = _use_config(
            tmp_path, [{"name": "a", "hosts": "a:6379", "modules": ["json"]}]
        )

        (row,) = await clusters_list(check=False)
        row["modules"].append("search")

        assert config.clusters[0].modules == ["json"]


class TestClusterSelection:
    @pytest.mark.asyncio
    async def test_all_clusters_without_current_cluster(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [{"name": "a", "hosts": "a:6379"}, {"name": "b", "hosts": "b:6379"}],
        )

        rows = await clusters_list(check=False)

        assert [row["name"] for row in rows] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_only_current_cluster(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [{"name": "a", "hosts": "a:6379"}, {"name": "b", "hosts": "b:6379"}],
        )
        connections.clients["b"] = _redis_mock()

        token = current_cluster.set("b")
        try:
            rows = await clusters_list()
        finally:
            current_cluster.reset(token)

        assert [row["name"] for row in rows] == ["b"]
        assert rows[0]["status"] == "ok"
        connections.assert_called_once_with(cluster="b")

    @pytest.mark.asyncio
    async def test_unknown_current_cluster_lists_nothing(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])

        token = current_cluster.set("missing")
        try:
            rows = await clusters_list()
        finally:
            current_cluster.reset(token)

        assert rows == []
        connections.assert_not_called()

    @pytest.mark.asyncio
    async def test_cluster_parameter_through_the_registry(self, tmp_path, connections):
        """The optional ``cluster`` parameter the registry adds selects the row."""
        _use_config(
            tmp_path,
            [{"name": "a", "hosts": "a:6379"}, {"name": "b", "hosts": "b:6379"}],
        )
        tool = registry.wrap(registry._SPECS["clusters_list"], ["a", "b"])

        one = await tool(cluster="b", check=False)
        every = await tool(check=False)

        assert [row["name"] for row in one] == ["b"]
        assert [row["name"] for row in every] == ["a", "b"]

    def test_declared_as_core_read_tool_with_optional_cluster(self):
        spec = registry._SPECS["clusters_list"]

        assert spec.fn is clusters_list
        assert spec.toolset == "core"
        assert spec.kind is registry.ToolKind.READ
        assert spec.cluster == "optional"


class TestProbe:
    @pytest.mark.asyncio
    async def test_check_false_skips_probing(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])

        (row,) = await clusters_list(check=False)

        connections.assert_not_called()
        for key in ("status", "latencyMs", "redisVersion", "role", "error"):
            assert key not in row

    @pytest.mark.asyncio
    async def test_check_is_the_default(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        connections.clients["a"] = _redis_mock()

        (row,) = await clusters_list()

        assert row["status"] == "ok"
        connections.assert_called_once_with(cluster="a")

    @pytest.mark.asyncio
    async def test_ok_fields_for_standalone(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        client = _redis_mock(version="7.4.1", role="slave")
        connections.clients["a"] = client

        (row,) = await clusters_list(check=True)

        assert row["status"] == "ok"
        assert isinstance(row["latencyMs"], int)
        assert row["latencyMs"] >= 0
        assert row["redisVersion"] == "7.4.1"
        assert row["role"] == "slave"
        assert "primaries" not in row
        assert "error" not in row
        client.ping.assert_called_once_with()
        client.info.assert_any_call("server")
        client.info.assert_any_call("replication")
        # the configuration fields stay next to the probe result
        assert row["name"] == "a"
        assert row["db"] == 0

    @pytest.mark.asyncio
    async def test_latency_covers_connect_and_ping(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        client = _redis_mock()
        client.ping.side_effect = lambda: time.sleep(0.05) or True
        connections.clients["a"] = client

        (row,) = await clusters_list()

        assert row["latencyMs"] >= 40

    @pytest.mark.asyncio
    async def test_primaries_for_redis_cluster(self, tmp_path, connections):
        _use_config(
            tmp_path,
            [{"name": "prd", "mode": "cluster", "hosts": "h1:6379,h2:6379"}],
        )
        connections.clients["prd"] = _cluster_mock(primaries=3)

        (row,) = await clusters_list()

        assert row["status"] == "ok"
        assert row["primaries"] == 3
        assert row["role"] == "master"

    @pytest.mark.asyncio
    async def test_missing_info_fields_are_none(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        client = Mock(spec=redis.Redis)
        client.info.return_value = {}
        connections.clients["a"] = client

        (row,) = await clusters_list()

        assert row["status"] == "ok"
        assert row["redisVersion"] is None
        assert row["role"] is None

    @pytest.mark.asyncio
    async def test_connection_error_reports_exception_type(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        connections.clients["a"] = ConnectionError("Connection refused")

        (row,) = await clusters_list()

        assert row["status"] == "error"
        assert row["error"] == "ConnectionError: Connection refused"
        assert "latencyMs" not in row
        assert "redisVersion" not in row

    @pytest.mark.asyncio
    async def test_ping_error_reports_exception_type(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        client = _redis_mock()
        client.ping.side_effect = AuthenticationError("invalid password")
        connections.clients["a"] = client

        (row,) = await clusters_list()

        assert row["status"] == "error"
        assert row["error"] == "AuthenticationError: invalid password"

    @pytest.mark.asyncio
    async def test_info_error_reports_exception_type(self, tmp_path, connections):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        client = _redis_mock()
        client.info.side_effect = redis.exceptions.NoPermissionError(
            "this user has no permissions to run the 'info' command"
        )
        connections.clients["a"] = client

        (row,) = await clusters_list()

        assert row["status"] == "error"
        assert row["error"].startswith("NoPermissionError: ")

    @pytest.mark.asyncio
    async def test_any_exception_while_creating_the_client_is_reported(
        self, tmp_path, connections
    ):
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        connections.clients["a"] = ValueError("Invalid host")

        (row,) = await clusters_list()

        assert row["status"] == "error"
        assert row["error"] == "ValueError: Invalid host"

    @pytest.mark.asyncio
    async def test_one_failing_cluster_does_not_affect_the_others(
        self, tmp_path, connections
    ):
        _use_config(
            tmp_path,
            [
                {"name": "up", "hosts": "a:6379"},
                {"name": "down", "hosts": "b:6379"},
                {"name": "also-up", "mode": "cluster", "hosts": "c:6379"},
            ],
        )
        connections.clients.update(
            {
                "up": _redis_mock(version="7.0.15"),
                "down": ConnectionError("Error 111 connecting to b:6379"),
                "also-up": _cluster_mock(primaries=2),
            }
        )

        rows = _by_name(await clusters_list())

        assert rows["up"]["status"] == "ok"
        assert rows["up"]["redisVersion"] == "7.0.15"
        assert rows["down"]["status"] == "error"
        assert rows["down"]["error"].startswith("ConnectionError: ")
        assert rows["also-up"]["status"] == "ok"
        assert rows["also-up"]["primaries"] == 2

    @pytest.mark.asyncio
    async def test_clusters_are_probed_in_parallel(self, tmp_path, connections):
        """Each probe waits for the other: sequential probes would break the barrier."""
        _use_config(
            tmp_path,
            [{"name": "a", "hosts": "a:6379"}, {"name": "b", "hosts": "b:6379"}],
        )
        barrier = threading.Barrier(2, timeout=5)
        for name in ("a", "b"):
            client = _redis_mock()
            client.ping.side_effect = lambda: barrier.wait() is not None
            connections.clients[name] = client

        rows = await clusters_list()

        assert [row["status"] for row in rows] == ["ok", "ok"]

    @pytest.mark.asyncio
    async def test_each_probe_names_its_cluster(self, tmp_path, connections):
        """Each probe asks for its cluster by name instead of relying on current_cluster."""
        _use_config(
            tmp_path,
            [{"name": "a", "hosts": "a:6379"}, {"name": "b", "hosts": "b:6379"}],
        )
        connections.clients.update({"a": _redis_mock(), "b": _redis_mock()})

        await clusters_list()

        assert sorted(c.kwargs["cluster"] for c in connections.call_args_list) == [
            "a",
            "b",
        ]


class TestTimeout:
    def test_budget_leaves_time_before_the_tool_timeout(self):
        assert 0 < CHECK_BUDGET_SECONDS < registry.TOOL_TIMEOUT_SECONDS

    @pytest.mark.asyncio
    async def test_hanging_probe_reports_timeout(
        self, tmp_path, connections, monkeypatch
    ):
        monkeypatch.setattr(clusters_module, "CHECK_BUDGET_SECONDS", 0.3)
        _use_config(
            tmp_path,
            [
                {"name": "hangs", "hosts": "a:6379"},
                {"name": "fast", "hosts": "b:6379"},
            ],
        )
        release = threading.Event()
        hanging = _redis_mock()
        hanging.ping.side_effect = lambda: release.wait(10)
        connections.clients.update({"hangs": hanging, "fast": _redis_mock()})

        start = time.monotonic()
        try:
            rows = _by_name(await clusters_list())
            elapsed = time.monotonic() - start
        finally:
            release.set()

        assert rows["hangs"]["status"] == "timeout"
        assert (
            rows["hangs"]["error"]
            == "The connection check did not finish within the time limit"
        )
        assert "latencyMs" not in rows["hangs"]
        assert rows["fast"]["status"] == "ok"
        assert 0.25 <= elapsed < 2

    @pytest.mark.asyncio
    async def test_budget_is_shared_by_all_probes(
        self, tmp_path, connections, monkeypatch
    ):
        """Two hanging clusters still answer within one budget, not one budget each."""
        monkeypatch.setattr(clusters_module, "CHECK_BUDGET_SECONDS", 0.3)
        _use_config(
            tmp_path,
            [
                {"name": "h1", "hosts": "a:6379"},
                {"name": "h2", "hosts": "b:6379"},
                {"name": "h3", "hosts": "c:6379"},
            ],
        )
        release = threading.Event()
        for name in ("h1", "h2", "h3"):
            client = _redis_mock()
            client.ping.side_effect = lambda: release.wait(10)
            connections.clients[name] = client

        start = time.monotonic()
        try:
            rows = await clusters_list()
            elapsed = time.monotonic() - start
        finally:
            release.set()

        assert [row["status"] for row in rows] == ["timeout"] * 3
        assert elapsed < 0.3 * 2

    @pytest.mark.asyncio
    async def test_hanging_connection_creation_reports_timeout(
        self, tmp_path, connections, monkeypatch
    ):
        monkeypatch.setattr(clusters_module, "CHECK_BUDGET_SECONDS", 0.2)
        _use_config(tmp_path, [{"name": "a", "hosts": "a:6379"}])
        release = threading.Event()

        def slow_get_connection(decode_responses=True, cluster=None):
            release.wait(10)
            return _redis_mock()

        connections.side_effect = slow_get_connection

        try:
            (row,) = await clusters_list()
        finally:
            release.set()

        assert row["status"] == "timeout"


class TestSecrets:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("check", [True, False])
    async def test_passwords_never_appear(self, tmp_path, connections, check):
        _use_config(
            tmp_path,
            [
                {"name": "pw", "hosts": "a:6379", "username": "mcp-reader"},
                {
                    "name": "sen",
                    "mode": "sentinel",
                    "hosts": "s1:26379",
                    "masterName": "mymaster",
                },
                {"name": "down", "hosts": "c:6379"},
            ],
            passwords={
                "pw_pass": PASSWORD,
                "sen_pass": PASSWORD,
                "sen_sentinel_pass": SENTINEL_PASSWORD,
                "down_pass": PASSWORD,
            },
        )
        connections.clients.update(
            {
                "pw": _redis_mock(),
                "sen": _redis_mock(),
                "down": ConnectionError("Connection refused"),
            }
        )

        rows = await clusters_list(check=check)
        output = json.dumps(rows)

        assert PASSWORD not in output
        assert SENTINEL_PASSWORD not in output
        for row in rows:
            assert "password" not in {key.lower() for key in row}
        assert _by_name(rows)["pw"]["auth"] == "password"
        assert _by_name(rows)["sen"]["auth"] == "password"

    @pytest.mark.asyncio
    async def test_options_are_not_listed(self, tmp_path, connections):
        """redis-py option overrides may carry connection details; the row does not echo them."""
        _use_config(
            tmp_path,
            [
                {
                    "name": "a",
                    "hosts": "a:6379",
                    "options": {"client_name": "secret-client-name"},
                }
            ],
        )

        (row,) = await clusters_list(check=False)

        assert "options" not in row
        assert "secret-client-name" not in json.dumps(row)
