import io
import json
import logging
import sys
import time
from pathlib import PurePosixPath
from unittest.mock import patch

import pytest

from src.common.logging_utils import (
    JsonFormatter,
    configure_json_logging,
    configure_logging,
    resolve_log_level,
)


@pytest.fixture()
def preserve_logging():
    """Snapshot and restore the root logger state to avoid cross-test interference."""
    root = logging.getLogger()
    mcp_logger = logging.getLogger("mcp")
    saved_level = root.level
    saved_mcp_level = mcp_logger.level
    saved_handlers = list(root.handlers)
    saved_handler_levels = [h.level for h in saved_handlers]
    try:
        yield
    finally:
        # Remove any handlers added during the test
        for h in list(root.handlers):
            try:
                root.removeHandler(h)
            except Exception:
                pass
        # Restore original handlers and their levels
        for h, lvl in zip(saved_handlers, saved_handler_levels):
            try:
                root.addHandler(h)
                h.setLevel(lvl)
            except Exception:
                pass
        # Restore original root level and the mcp logger level set by configure_json_logging
        root.setLevel(saved_level)
        mcp_logger.setLevel(saved_mcp_level)
        # Best-effort: disable warnings capture enabled by configure_logging
        try:
            logging.captureWarnings(False)
        except Exception:
            pass


@pytest.fixture(autouse=True)
def no_log_level_env(monkeypatch):
    monkeypatch.delenv("REDIS_MCP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)


def test_resolve_log_level_default_warning(monkeypatch):
    assert resolve_log_level() == logging.WARNING


def test_resolve_log_level_new_name_wins_over_the_old_one(monkeypatch):
    monkeypatch.setenv("REDIS_MCP_LOG_LEVEL", "debug")
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "error")
    assert resolve_log_level() == logging.DEBUG


def test_configure_json_logging_honours_the_new_name(monkeypatch, preserve_logging):
    monkeypatch.setenv("REDIS_MCP_LOG_LEVEL", "WARNING")
    assert configure_json_logging(io.StringIO()) == logging.WARNING


def test_resolve_log_level_parses_name_and_alias(monkeypatch):
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "info")
    assert resolve_log_level() == logging.INFO
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "WARN")
    assert resolve_log_level() == logging.WARNING
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "fatal")
    assert resolve_log_level() == logging.CRITICAL


def test_resolve_log_level_parses_numeric(monkeypatch):
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "10")
    assert resolve_log_level() == 10
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "+20")
    assert resolve_log_level() == 20


def test_configure_logging_adds_stderr_handler_when_none(monkeypatch, preserve_logging):
    # Ensure no handlers exist before configuring
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "INFO")
    level = configure_logging()

    assert level == logging.INFO
    assert len(root.handlers) == 1, (
        "Should add exactly one stderr handler when none exist"
    )
    handler = root.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    # StreamHandler exposes the underlying stream attribute
    assert getattr(handler, "stream", None) is sys.stderr
    assert handler.level == logging.INFO
    assert root.level == logging.INFO


def test_configure_logging_only_lowers_restrictive_handlers(
    monkeypatch, preserve_logging
):
    root = logging.getLogger()
    # Start from a clean handler set
    for h in list(root.handlers):
        root.removeHandler(h)

    # Add two handlers: one restrictive WARNING, one permissive NOTSET
    h_warning = logging.StreamHandler(sys.stderr)
    h_warning.setLevel(logging.WARNING)
    root.addHandler(h_warning)

    h_notset = logging.StreamHandler(sys.stderr)
    h_notset.setLevel(logging.NOTSET)
    root.addHandler(h_notset)

    # Request DEBUG
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "DEBUG")
    configure_logging()

    # The WARNING handler should be lowered to DEBUG; NOTSET should remain NOTSET
    assert h_warning.level == logging.DEBUG
    assert h_notset.level == logging.NOTSET


def test_configure_logging_does_not_raise_handler_threshold(
    monkeypatch, preserve_logging
):
    root = logging.getLogger()
    # Clean handlers
    for h in list(root.handlers):
        root.removeHandler(h)

    # Add a handler at WARNING and then set env to ERROR
    h_warning = logging.StreamHandler(sys.stderr)
    h_warning.setLevel(logging.WARNING)
    root.addHandler(h_warning)

    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "ERROR")
    configure_logging()

    # Handler should remain at WARNING (30), not be raised to ERROR (40)
    assert h_warning.level == logging.WARNING
    # Root level should reflect ERROR
    assert root.level == logging.ERROR


def test_configure_logging_does_not_add_handler_if_exists(
    monkeypatch, preserve_logging
):
    root = logging.getLogger()
    # Start with one existing handler
    for h in list(root.handlers):
        root.removeHandler(h)
    existing = logging.StreamHandler(sys.stderr)
    root.addHandler(existing)

    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "INFO")
    configure_logging()

    # Should not add another handler
    assert len(root.handlers) == 1
    assert root.handlers[0] is existing


def make_record(
    msg="hello %s",
    args=("world",),
    level=logging.INFO,
    name="src.example",
    exc_info=None,
    extra=None,
    created=None,
):
    record = logging.getLogger(name).makeRecord(
        name, level, "example.py", 42, msg, args, exc_info, extra=extra
    )
    if created is not None:
        record.created = created
    return record


def format_event(record) -> dict:
    line = JsonFormatter().format(record)
    assert "\n" not in line
    return json.loads(line)


class TestJsonFormatter:
    def test_core_fields(self):
        event = format_event(make_record())

        assert set(event) == {"@timestamp", "level", "logger", "message"}
        assert event["level"] == "INFO"
        assert event["logger"] == "src.example"
        assert event["message"] == "hello world"

    @pytest.mark.parametrize(
        "level, name",
        [
            (logging.DEBUG, "DEBUG"),
            (logging.WARNING, "WARNING"),
            (logging.ERROR, "ERROR"),
            (logging.CRITICAL, "CRITICAL"),
        ],
    )
    def test_level_name(self, level, name):
        assert format_event(make_record(level=level))["level"] == name

    @pytest.mark.parametrize(
        "created, expected",
        [
            (0.5, "1970-01-01T00:00:00.500Z"),
            (1700000000.25, "2023-11-14T22:13:20.250Z"),
        ],
    )
    def test_timestamp_is_utc_milliseconds_with_z(self, created, expected):
        assert format_event(make_record(created=created))["@timestamp"] == expected

    @pytest.mark.skipif(not hasattr(time, "tzset"), reason="needs time.tzset")
    def test_timestamp_ignores_local_time_zone(self, monkeypatch):
        monkeypatch.setenv("TZ", "Asia/Tokyo")
        time.tzset()
        try:
            local_hour = time.localtime(0).tm_hour
            stamp = format_event(make_record(created=0.5))["@timestamp"]
        finally:
            monkeypatch.undo()
            time.tzset()

        assert local_hour == 9
        assert stamp == "1970-01-01T00:00:00.500Z"

    def test_extra_fields_keep_their_types(self):
        event = format_event(
            make_record(
                msg="tool call",
                args=(),
                extra={
                    "tool": "get",
                    "cluster": "cbtstrds",
                    "durationMs": 12,
                    "headerPresent": False,
                    "error": None,
                },
            )
        )

        assert event["tool"] == "get"
        assert event["cluster"] == "cbtstrds"
        assert event["durationMs"] == 12
        assert event["headerPresent"] is False
        assert event["error"] is None

    def test_non_serializable_extra_uses_str(self):
        # PurePosixPath: str() of a native Path would use backslashes on Windows
        event = format_event(
            make_record(extra={"path": PurePosixPath("/etc/redis-mcp")})
        )

        assert event["path"] == "/etc/redis-mcp"

    def test_color_message_dropped(self):
        # uvicorn passes the message again with terminal colors
        record = make_record(extra={"color_message": "\x1b[1mhello\x1b[0m"})

        event = format_event(record)

        assert "color_message" not in event
        assert event["message"] == "hello world"

    def test_private_attributes_dropped(self):
        event = format_event(make_record(extra={"_internal": 1}))

        assert "_internal" not in event

    def test_standard_record_attributes_not_included(self):
        event = format_event(make_record())

        for attribute in ("args", "msg", "pathname", "lineno", "levelno", "thread"):
            assert attribute not in event

    def test_exception_text(self):
        try:
            raise ValueError("boom")
        except ValueError:
            record = make_record(
                msg="failed", args=(), level=logging.ERROR, exc_info=sys.exc_info()
            )

        event = format_event(record)

        assert event["message"] == "failed"
        assert event["exception"].startswith("Traceback (most recent call last)")
        assert "ValueError: boom" in event["exception"]

    def test_no_exception_key_without_exc_info(self):
        assert "exception" not in format_event(make_record())

    def test_multi_line_message_stays_one_line(self):
        event = format_event(make_record(msg="line1\nline2", args=()))

        assert event["message"] == "line1\nline2"

    def test_non_ascii_message(self):
        event = format_event(make_record(msg="küme %s", args=("çalışıyor",)))

        assert event["message"] == "küme çalışıyor"

    def test_through_a_logger(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(JsonFormatter())
        logger = logging.getLogger("tests.json_formatter")
        logger.addHandler(handler)
        logger.propagate = False
        try:
            logger.warning("first %d", 1, extra={"remoteAddr": "10.0.0.1"})
            logger.warning("second")
        finally:
            logger.removeHandler(handler)
            logger.propagate = True

        lines = stream.getvalue().splitlines()
        assert len(lines) == 2
        first, second = (json.loads(line) for line in lines)
        assert first["message"] == "first 1"
        assert first["remoteAddr"] == "10.0.0.1"
        assert second["message"] == "second"
        assert "remoteAddr" not in second


class TestConfigureJsonLogging:
    def test_replaces_existing_handlers(self, monkeypatch, preserve_logging):
        monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)
        root = logging.getLogger()
        existing = [logging.StreamHandler(sys.stderr), logging.NullHandler()]
        for h in existing:
            root.addHandler(h)
        stream = io.StringIO()

        configure_json_logging(stream)

        assert len(root.handlers) == 1
        handler = root.handlers[0]
        assert handler not in existing
        assert isinstance(handler, logging.StreamHandler)
        assert isinstance(handler.formatter, JsonFormatter)
        assert handler.stream is stream

    def test_is_idempotent(self, monkeypatch, preserve_logging):
        monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)

        configure_json_logging(io.StringIO())
        configure_json_logging(io.StringIO())

        assert len(logging.getLogger().handlers) == 1

    def test_defaults_to_stdout(self, monkeypatch, preserve_logging):
        monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)

        configure_json_logging()

        assert logging.getLogger().handlers[0].stream is sys.stdout

    def test_defaults_to_info(self, monkeypatch, preserve_logging):
        monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)

        level = configure_json_logging(io.StringIO())

        assert level == logging.INFO
        assert logging.getLogger().level == logging.INFO

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("DEBUG", logging.DEBUG),
            ("warning", logging.WARNING),
            ("ERROR", logging.ERROR),
            ("10", logging.DEBUG),
        ],
    )
    def test_honours_env_level(self, monkeypatch, preserve_logging, value, expected):
        monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", value)

        level = configure_json_logging(io.StringIO())

        assert level == expected
        assert logging.getLogger().level == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, logging.WARNING),
            ("DEBUG", logging.WARNING),
            ("INFO", logging.WARNING),
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
        ],
    )
    def test_mcp_logger_at_least_warning(
        self, monkeypatch, preserve_logging, value, expected
    ):
        if value is None:
            monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)
        else:
            monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", value)

        configure_json_logging(io.StringIO())

        assert logging.getLogger("mcp").level == expected

    def test_writes_json_lines(self, monkeypatch, preserve_logging):
        monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)
        stream = io.StringIO()
        configure_json_logging(stream)

        logger = logging.getLogger("src.common.registry")
        logger.debug("not shown")
        logger.info("tool call", extra={"tool": "get", "durationMs": 3})
        logging.getLogger("mcp.server.lowlevel.server").info("Processing request")
        logging.getLogger("mcp.server.lowlevel.server").warning("mcp warning")

        events = [json.loads(line) for line in stream.getvalue().splitlines()]
        assert [e["message"] for e in events] == ["tool call", "mcp warning"]
        assert events[0]["logger"] == "src.common.registry"
        assert events[0]["tool"] == "get"
        assert events[0]["durationMs"] == 3
        assert events[1]["level"] == "WARNING"

    def test_captures_warnings(self, monkeypatch, preserve_logging):
        monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)

        with patch("src.common.logging_utils.logging.captureWarnings") as capture:
            configure_json_logging(io.StringIO())

        capture.assert_called_once_with(True)
