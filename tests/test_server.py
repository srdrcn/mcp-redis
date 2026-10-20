"""
Unit tests for src/common/server.py
"""

import inspect
import pkgutil
import sys
import warnings
from unittest.mock import Mock, patch

import pytest
from mcp.server.fastmcp import FastMCP
from starlette.exceptions import StarletteDeprecationWarning

with warnings.catch_warnings():
    # starlette 1.x prefers httpx2 for its TestClient; httpx works the same for these tests
    warnings.simplefilter("ignore", StarletteDeprecationWarning)
    from starlette.testclient import TestClient

import src.tools
from src.common import registry
from src.common.config_loader import (
    ConfigError,
    ServerConfig,
    from_legacy,
    parse_config,
    set_config,
)
from src.common.connection import current_cluster
from src.common.server import (
    DEFAULT_HTTP_PORT,
    HEALTH_PATH,
    INSTRUCTIONS,
    MCP_PATH,
    SERVER_NAME,
    build_server,
    load_tools,
)

MCP_HEADERS = {"Accept": "application/json, text/event-stream"}
ONLY_CLUSTER_DESCRIPTION = "Cluster name (default: the only configured cluster)"
ALL_CLUSTERS_DESCRIPTION = "Cluster name (default: all clusters)"
LEGACY_REDIS_CFG = {"host": "localhost", "port": 6379, "db": 0}


def _config(**root) -> ServerConfig:
    data = {"clusters": [{"name": "a", "hosts": "a:6379"}]}
    data.update(root)
    return parse_config(data)


def _single_cluster(kind: str):
    """A config with one cluster and that cluster's name: from a YAML-style config or the legacy env/CLI."""
    if kind == "legacy":
        return from_legacy(LEGACY_REDIS_CFG), "default"
    return (
        _config(
            toolsets=list(registry.TOOLSETS),
            clusters=[{"name": "a", "hosts": "a:6379", "modules": ["json", "search"]}],
        ),
        "a",
    )


def _two_clusters() -> ServerConfig:
    return _config(
        toolsets=list(registry.TOOLSETS),
        readOnly=True,
        clusters=[
            {"name": "rw", "hosts": "rw:6379", "readOnly": False},
            {"name": "ro", "hosts": "ro:6379", "modules": ["json", "search"]},
        ],
    )


def _call_tool(client, name, arguments):
    """tools/call over the stateless HTTP endpoint; returns the JSON-RPC result."""
    response = client.post(
        MCP_PATH,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
        headers=MCP_HEADERS,
    )
    assert response.status_code == 200
    return response.json()["result"]


@pytest.fixture(autouse=True)
def fresh_executors(monkeypatch):
    """Tool calls get new worker pools in each test; they are shut down afterwards."""
    executors = {}
    monkeypatch.setattr(registry, "_executors", executors)
    yield executors
    for executor in executors.values():
        executor.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def created_clients():
    """The real RedisConnectionManager with create_client mocked: records the call's cluster (as the tool's
    worker sees it) and the cluster the manager resolved."""
    client = Mock()
    client.get.return_value = "value"
    client.created_for = []

    def create_client(cluster_config, decode_responses=True, max_connections=None):
        client.created_for.append((current_cluster.get(), cluster_config.name))
        return client

    with patch("src.common.connection.create_client", side_effect=create_client):
        yield client


class TestConstants:
    def test_server_name(self):
        assert SERVER_NAME == "redis-mcp"

    def test_paths_and_port(self):
        assert MCP_PATH == "/mcp"
        assert HEALTH_PATH == "/healthz"
        assert DEFAULT_HTTP_PORT == 8080

    def test_instructions(self):
        assert "cluster" in INSTRUCTIONS
        assert "clusters_list" in INSTRUCTIONS
        assert "WRITE:" in INSTRUCTIONS
        assert f"{registry.TOOL_TIMEOUT_SECONDS} s" in INSTRUCTIONS


class TestLoadTools:
    def test_imports_every_tools_module(self):
        load_tools()

        for module in pkgutil.iter_modules(src.tools.__path__):
            assert f"src.tools.{module.name}" in sys.modules

    def test_is_idempotent(self):
        load_tools()
        before = registry.specs()

        load_tools()

        assert registry.specs() == before


class TestBuildServer:
    def test_returns_named_fastmcp(self):
        mcp = build_server(_config())

        assert isinstance(mcp, FastMCP)
        assert mcp.name == SERVER_NAME

    def test_initialize_reports_the_project_version(self):
        """FastMCP takes no version; initialize must report redis-mcp's, not the mcp package's."""
        from src.version import __version__

        options = build_server(_config())._mcp_server.create_initialization_options()

        assert (options.server_name, options.server_version) == (
            SERVER_NAME,
            __version__,
        )

    def test_instructions_mention_cluster(self):
        mcp = build_server(_config())

        assert mcp.instructions == INSTRUCTIONS
        assert "cluster" in mcp.instructions

    def test_streamable_http_settings(self):
        mcp = build_server(_config())

        assert mcp.settings.stateless_http is True
        assert mcp.settings.json_response is True
        assert mcp.settings.streamable_http_path == MCP_PATH

    def test_port_from_config(self):
        assert build_server(_config(port=9090)).settings.port == 9090

    @pytest.mark.parametrize("port", [None, 0])
    def test_default_port(self, port):
        assert build_server(_config(port=port)).settings.port == DEFAULT_HTTP_PORT

    def test_host(self):
        assert build_server(_config()).settings.host == "0.0.0.0"  # nosec B104
        assert build_server(_config(), host="127.0.0.1").settings.host == "127.0.0.1"

    def test_dependencies(self):
        mcp = build_server(_config())

        assert mcp.dependencies == [
            "redis",
            "python-dotenv",
            "numpy",
            "aiohttp",
            "pyyaml",
        ]

    @pytest.mark.asyncio
    async def test_registers_the_selected_tools(self):
        config = _config(
            toolsets=["core", "data"],
            clusters=[
                {"name": "a", "hosts": "a:6379", "readOnly": True},
                {"name": "b", "hosts": "b:6379"},
            ],
        )

        mcp = build_server(config)
        listed = await mcp.list_tools()

        selected = registry.select(config)
        assert [t.name for t in listed] == [spec.name for spec, _ in selected]
        by_name = {t.name: t for t in listed}
        for spec, _ in selected:
            tool = by_name[spec.name]
            assert tool.description == spec.description
            assert tool.annotations == registry.annotations(spec)

    @pytest.mark.asyncio
    async def test_servers_are_independent(self):
        writable = build_server(_config())
        read_only = build_server(_config(readOnly=True))

        writable_names = {t.name for t in await writable.list_tools()}
        read_only_names = {t.name for t in await read_only.list_tools()}

        assert "delete" in writable_names
        assert "delete" not in read_only_names
        assert read_only_names < writable_names

    @pytest.mark.asyncio
    async def test_no_tool_without_enabled_toolset(self):
        mcp = build_server(_config(toolsets=["json"]))

        # no cluster lists the json module
        assert await mcp.list_tools() == []

    def test_unknown_toolset_fails(self):
        with pytest.raises(ConfigError, match="nope"):
            build_server(_config(toolsets=["nope"]))


class TestClusterParameter:
    """With one configured cluster (legacy env/CLI mode or a one-cluster config) the ``cluster`` parameter is
    optional, so calls written before multi-cluster support keep working; with more it is required."""

    @staticmethod
    async def _listed(config):
        specs = {spec.name: spec for spec, _ in registry.select(config)}
        tools = await build_server(config).list_tools()
        assert {t.name for t in tools} == set(specs)
        return [(specs[t.name], t.inputSchema) for t in tools]

    @pytest.mark.parametrize("kind", ["yaml", "legacy"])
    @pytest.mark.asyncio
    async def test_single_cluster_makes_cluster_optional(self, kind):
        config, name = _single_cluster(kind)

        listed = await self._listed(config)

        assert {spec.cluster for spec, _ in listed} == {"required", "optional", "none"}
        for spec, schema in listed:
            properties = schema["properties"]
            if spec.cluster == "none":
                assert "cluster" not in properties, spec.name
                continue
            assert list(properties)[-1] == "cluster", spec.name
            assert "cluster" not in schema.get("required", []), spec.name
            cluster = properties["cluster"]
            assert cluster["default"] is None
            assert {"type": "string", "enum": [name]} in cluster["anyOf"]
            assert {"type": "null"} in cluster["anyOf"]
            assert cluster["description"] == (
                ONLY_CLUSTER_DESCRIPTION
                if spec.cluster == "required"
                else ALL_CLUSTERS_DESCRIPTION
            ), spec.name

    @pytest.mark.asyncio
    async def test_two_clusters_require_cluster(self):
        listed = await self._listed(_two_clusters())

        by_name = {spec.name: schema for spec, schema in listed}
        for spec, schema in listed:
            if spec.cluster != "required":
                continue
            assert list(schema["properties"])[0] == "cluster", spec.name
            assert "cluster" in schema["required"], spec.name
            assert schema["properties"]["cluster"]["description"] == "Cluster name"
        # eligible on one cluster only, but two are configured: still required
        assert by_name["set"]["properties"]["cluster"]["enum"] == ["rw"]
        assert by_name["json_get"]["properties"]["cluster"]["enum"] == ["ro"]
        assert "cluster" in by_name["set"]["required"]
        assert "cluster" in by_name["json_get"]["required"]
        clusters_list = by_name["clusters_list"]["properties"]["cluster"]
        assert clusters_list["description"] == ALL_CLUSTERS_DESCRIPTION
        assert "cluster" not in by_name["clusters_list"].get("required", [])

    @pytest.mark.parametrize(
        "kind, single_cluster",
        [("yaml", True), ("legacy", True), ("two-clusters", False)],
    )
    def test_passes_single_cluster_to_wrap(self, kind, single_cluster):
        config = _two_clusters() if kind == "two-clusters" else _single_cluster(kind)[0]

        with patch("src.common.registry.wrap", side_effect=registry.wrap) as mock_wrap:
            build_server(config)

        assert mock_wrap.call_count == len(registry.select(config))
        signature = inspect.signature(registry.wrap)
        for wrap_call in mock_wrap.call_args_list:
            bound = signature.bind(*wrap_call.args, **wrap_call.kwargs)
            bound.apply_defaults()
            assert bound.arguments["single_cluster"] is single_cluster


class TestHttpApp:
    def test_healthz(self):
        mcp = build_server(_config())

        with TestClient(mcp.streamable_http_app()) as client:
            response = client.get(HEALTH_PATH)

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        assert response.json() == {"status": "UP"}

    def test_healthz_does_not_touch_redis(self):
        mcp = build_server(_config())

        with patch(
            "src.common.connection.RedisConnectionManager.get_connection",
            side_effect=ConnectionError("cluster down"),
        ) as mock_get_connection:
            with TestClient(mcp.streamable_http_app()) as client:
                response = client.get(HEALTH_PATH)

        assert response.json() == {"status": "UP"}
        mock_get_connection.assert_not_called()

    def test_healthz_only_answers_get(self):
        mcp = build_server(_config())

        with TestClient(mcp.streamable_http_app()) as client:
            response = client.post(HEALTH_PATH)

        assert response.status_code == 405

    def test_mcp_endpoint_is_stateless_json(self):
        mcp = build_server(_config(toolsets=["core"]))

        with TestClient(mcp.streamable_http_app()) as client:
            # no initialize first: in stateless mode every request stands alone
            response = client.post(
                MCP_PATH,
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                headers=MCP_HEADERS,
            )

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/json")
        assert "mcp-session-id" not in response.headers
        names = [t["name"] for t in response.json()["result"]["tools"]]
        assert "clusters_list" in names
        assert "get" not in names

    @pytest.mark.parametrize("kind", ["yaml", "legacy"])
    def test_single_cluster_call_without_cluster(
        self, kind, created_clients, fresh_executors
    ):
        config, name = _single_cluster(kind)
        set_config(config)
        mcp = build_server(config)

        with TestClient(mcp.streamable_http_app()) as client:
            result = _call_tool(client, "get", {"key": "k"})
            named = _call_tool(client, "get", {"key": "k", "cluster": name})

        assert result["isError"] is False
        assert result["content"][0]["text"] == "value"
        assert named["isError"] is False
        assert created_clients.get.call_count == 2
        # no cluster selected for the first call; the connection manager uses the only one, and both calls
        # share its client and its worker pool
        assert created_clients.created_for == [(None, name)]
        assert set(fresh_executors) == {name}

    def test_two_clusters_call_without_cluster_is_rejected(self, created_clients):
        config = _two_clusters()
        set_config(config)
        mcp = build_server(config)

        with TestClient(mcp.streamable_http_app()) as client:
            result = _call_tool(client, "get", {"key": "k"})

        assert result["isError"] is True
        assert "cluster" in result["content"][0]["text"]
        assert "Field required" in result["content"][0]["text"]
        assert created_clients.created_for == []
