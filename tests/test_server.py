"""
Unit tests for src/common/server.py
"""

import pkgutil
import sys
import warnings
from unittest.mock import patch

import pytest
from mcp.server.fastmcp import FastMCP
from starlette.exceptions import StarletteDeprecationWarning

with warnings.catch_warnings():
    # starlette 1.x prefers httpx2 for its TestClient; httpx works the same for these tests
    warnings.simplefilter("ignore", StarletteDeprecationWarning)
    from starlette.testclient import TestClient

import src.tools
from src.common import registry
from src.common.config_loader import ConfigError, ServerConfig, parse_config
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


def _config(**root) -> ServerConfig:
    data = {"clusters": [{"name": "a", "hosts": "a:6379"}]}
    data.update(root)
    return parse_config(data)


class TestConstants:
    def test_server_name(self):
        assert SERVER_NAME == "Redis MCP Server"

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
