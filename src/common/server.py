import importlib
import pkgutil

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from src.common import registry
from src.common.config_loader import ServerConfig
from src.version import __version__

SERVER_NAME = "redis-mcp"
MCP_PATH = "/mcp"
HEALTH_PATH = "/healthz"
DEFAULT_HTTP_PORT = 8080


def instructions(status_tool: bool = True) -> str:
    """The server instructions; they name clusters_list only while it is listed (``status_tool``)."""
    status = "; clusters_list shows each cluster's status" if status_tool else ""
    return (
        "MCP server for Redis clusters. Every tool that touches Redis takes a `cluster` parameter (valid names "
        f"are in the schema enum{status}). Tools whose description starts "
        'with "WRITE:" change data. Each tool call runs for at most '
        f"{registry.TOOL_TIMEOUT_SECONDS} s."
    )


INSTRUCTIONS = instructions()


def load_tools():
    import src.tools as tools_pkg

    for _, module_name, _ in pkgutil.iter_modules(tools_pkg.__path__):
        importlib.import_module(f"src.tools.{module_name}")


def build_server(
    config: ServerConfig,
    # a server in a container listens on all interfaces
    host: str = "0.0.0.0",  # nosec B104
) -> FastMCP:
    """FastMCP server with the tools the config enables (see src.common.registry)."""
    load_tools()
    selected = registry.select(config)
    status_tool = any(spec.name == registry.STATUS_TOOL for spec, _ in selected)
    mcp = FastMCP(
        SERVER_NAME,
        instructions=instructions(status_tool),
        dependencies=["redis", "python-dotenv", "numpy", "aiohttp", "pyyaml"],
        host=host,
        port=config.port or DEFAULT_HTTP_PORT,
        streamable_http_path=MCP_PATH,
        # every HTTP request stands alone: no MCP session to lose when the pod restarts
        stateless_http=True,
        json_response=True,
    )
    # FastMCP takes no version; without this, initialize reports the mcp package's version
    mcp._mcp_server.version = __version__
    single_cluster = len(config.clusters) == 1
    for spec, clusters in selected:
        mcp.add_tool(
            registry.wrap(spec, clusters, single_cluster, status_tool),
            name=spec.name,
            description=spec.description,
            annotations=registry.annotations(spec),
        )

    @mcp.custom_route(HEALTH_PATH, methods=["GET"])
    async def health(request: Request) -> JSONResponse:
        # does not depend on Redis: an outage of a cluster must not restart the pod
        return JSONResponse({"status": "UP"})

    return mcp
