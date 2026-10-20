import json
import logging
import os
import sys
from datetime import datetime, timezone


def _log_level_setting():
    return os.getenv("REDIS_MCP_LOG_LEVEL") or os.getenv("MCP_REDIS_LOG_LEVEL")


def resolve_log_level() -> int:
    """Resolve desired log level from REDIS_MCP_LOG_LEVEL (or the older name MCP_REDIS_LOG_LEVEL).

    Accepts numeric strings or standard level names (DEBUG, INFO, WARNING,
    ERROR, CRITICAL, NOTSET) including aliases WARN and FATAL. Defaults to WARNING.
    """
    name = _log_level_setting()
    if name:
        s = name.strip()
        try:
            return int(s)
        except ValueError:
            pass
        level = getattr(logging, s.upper(), None)
        if isinstance(level, int):
            return level
    return logging.WARNING


def configure_logging() -> int:
    """Configure logging based on environment.

    - Default level WARNING
    - REDIS_MCP_LOG_LEVEL to override

    Returns the resolved log level. Idempotent.
    """

    level = resolve_log_level()
    root = logging.getLogger()

    # Always set the root logger level
    root.setLevel(level)

    # Only lower overly-restrictive handler thresholds to avoid host filtering.
    # - Leave NOTSET (0) alone so it defers to logger/root levels
    # - Do not raise handler thresholds (respect host-configured verbosity)
    for h in root.handlers:
        try:
            cur = getattr(h, "level", None)
            if isinstance(cur, int) and cur != logging.NOTSET and cur > level:
                h.setLevel(level)
        except Exception:
            # Log at DEBUG to avoid noisy stderr while still providing diagnostics.
            logging.getLogger(__name__).debug(
                "Failed to adjust handler level for handler %r", h, exc_info=True
            )

    # Only add our own stderr handler if there are NO handlers at all.
    # Many hosts (pytest, uv, VS Code) install a console handler already.
    if not root.handlers:
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(level)
        sh.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        root.addHandler(sh)

    # Route warnings.warn(...) through logging
    logging.captureWarnings(True)

    return level


# Attributes every LogRecord has; anything else on a record came from `extra=` and goes into the JSON line
# (uvicorn's color_message duplicates the message with terminal colors).
_RECORD_ATTRIBUTES = set(vars(logging.makeLogRecord({}))) | {
    "message",
    "asctime",
    "color_message",
}


class JsonFormatter(logging.Formatter):
    """One JSON object per line: timestamp, level, logger, message, the `extra=` fields and any exception."""

    def format(self, record: logging.LogRecord) -> str:
        event = {
            "@timestamp": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _RECORD_ATTRIBUTES and not key.startswith("_"):
                event[key] = value
        if record.exc_info:
            event["exception"] = self.formatException(record.exc_info)
        return json.dumps(event, default=str)


def configure_json_logging(stream=None) -> int:
    """JSON logs for the HTTP transport (to stdout by default), at INFO unless REDIS_MCP_LOG_LEVEL says
    otherwise, replacing any handlers already installed. In stdio mode stdout carries JSON-RPC, so this is
    not used there."""
    level = resolve_log_level() if _log_level_setting() else logging.INFO
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(handler)
    root.setLevel(level)
    # the MCP SDK logs every request at INFO; the server logs one line per tool call instead
    logging.getLogger("mcp").setLevel(max(level, logging.WARNING))
    logging.captureWarnings(True)
    return level
