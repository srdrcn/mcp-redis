"""Streamable HTTP transport: API key check, browser request refusal and the uvicorn runner."""

import hmac
import json
import logging
from typing import Any, Awaitable, Callable, Dict, MutableMapping, Optional

import uvicorn
from mcp.server.fastmcp import FastMCP

from src.common.config_loader import RECOMMENDED_API_KEY_LENGTH, ServerConfig
from src.common.server import MCP_PATH

_logger = logging.getLogger(__name__)

API_KEY_HEADER = "x-authorization"

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class ApiKeyMiddleware:
    """Guards ``/mcp``:

    - a request with an ``Origin`` header (sent by a web page) gets 403, which stops browser CSRF and DNS
      rebinding; MCP clients such as Claude Code send no Origin,
    - if an API key is set, the ``X-Authorization`` header must match it exactly, otherwise 401.

    Other paths (the health endpoint) pass through.
    """

    def __init__(self, app: ASGIApp, api_key: Optional[str]):
        self.app = app
        self.api_key = api_key.encode("utf-8") if api_key else None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not _guarded(scope.get("path", "")):
            await self.app(scope, receive, send)
            return
        headers = _headers(scope)
        if "origin" in headers:
            await _reject(
                send,
                403,
                "forbidden",
                "Requests with an Origin header (from a web page) are not accepted",
            )
            return
        if self.api_key is not None:
            provided = headers.get(API_KEY_HEADER)
            if provided is None or not hmac.compare_digest(
                provided.encode("latin-1"), self.api_key
            ):
                client = scope.get("client")
                _logger.warning(
                    "unauthorized request remoteAddr=%s headerPresent=%s",
                    client[0] if client else None,
                    provided is not None,
                    extra={
                        "remoteAddr": client[0] if client else None,
                        "headerPresent": provided is not None,
                    },
                )
                await _reject(
                    send,
                    401,
                    "unauthorized",
                    "X-Authorization header is missing or the API key does not match",
                )
                return
        await self.app(scope, receive, send)


def _guarded(path: str) -> bool:
    return path == MCP_PATH or path.startswith(MCP_PATH + "/")


def _headers(scope: Scope) -> Dict[str, str]:
    return {
        name.decode("latin-1").lower(): value.decode("latin-1")
        for name, value in scope.get("headers", [])
    }


async def _reject(send: Send, status: int, error: str, message: str) -> None:
    body = json.dumps({"error": error, "message": message}).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


def http_app(mcp: FastMCP, config: ServerConfig) -> ASGIApp:
    if config.api_key_enabled:
        _logger.info(
            "API key authentication enabled for %s (header X-Authorization)", MCP_PATH
        )
        if len(config.api_key or "") < RECOMMENDED_API_KEY_LENGTH:
            _logger.warning(
                "apiKey is shorter than %d characters; use a long random key (e.g. openssl rand -hex 32)",
                RECOMMENDED_API_KEY_LENGTH,
            )
    else:
        _logger.warning(
            "API key authentication DISABLED (apiKey is empty): anyone who can reach %s can call the tools",
            MCP_PATH,
        )
    return ApiKeyMiddleware(mcp.streamable_http_app(), config.api_key)


def run_http(mcp: FastMCP, config: ServerConfig) -> None:
    uvicorn.run(
        http_app(mcp, config),
        host=mcp.settings.host,
        port=mcp.settings.port,
        log_config=None,
        access_log=False,
    )
