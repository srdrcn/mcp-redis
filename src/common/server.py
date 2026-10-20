import importlib
import pkgutil

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from src.common import registry
from src.common.config_loader import ServerConfig

SERVER_NAME = "Redis MCP Server"
MCP_PATH = "/mcp"
HEALTH_PATH = "/healthz"
DEFAULT_HTTP_PORT = 8080

INSTRUCTIONS = (
    "MCP server for Redis clusters. Every tool that touches Redis takes a `cluster` parameter (valid names "
    "are in the schema enum; clusters_list shows each cluster's status). Tools whose description starts "
    'with "WRITE:" change data. Each tool call runs for at most '
    f"{registry.TOOL_TIMEOUT_SECONDS} s."
)


def load_tools():
    import src.tools as tools_pkg

    for _, module_name, _ in pkgutil.iter_modules(tools_pkg.__path__):
        importlib.import_module(f"src.tools.{module_name}")


def build_server(
    config: ServerConfig,
    host: str = "0.0.0.0",  # nosec B104 - a server in a container listens on all interfaces
) -> FastMCP:
    """FastMCP server with the tools the config enables (see src.common.registry)."""
    load_tools()
    mcp = FastMCP(
        SERVER_NAME,
        instructions=INSTRUCTIONS,
        dependencies=["redis", "python-dotenv", "numpy", "aiohttp", "pyyaml"],
        host=host,
        port=config.port or DEFAULT_HTTP_PORT,
        streamable_http_path=MCP_PATH,
        # every HTTP request stands alone: no MCP session to lose when the pod restarts
        stateless_http=True,
        json_response=True,
    )
    for spec, clusters in registry.select(config):
        mcp.add_tool(
            registry.wrap(spec, clusters),
            name=spec.name,
            description=spec.description,
            annotations=registry.annotations(spec),
        )

    @mcp.custom_route(HEALTH_PATH, methods=["GET"])
    async def health(request: Request) -> JSONResponse:
        # does not depend on Redis: an outage of a cluster must not restart the pod
        return JSONResponse({"status": "UP"})

    return mcp
