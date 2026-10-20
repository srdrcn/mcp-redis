"""
Unit tests for src/common/http.py
"""

import hmac
import json
import logging
import warnings
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from starlette.exceptions import StarletteDeprecationWarning

with warnings.catch_warnings():
    # starlette 1.x prefers httpx2 for its TestClient; httpx works the same for these tests
    warnings.simplefilter("ignore", StarletteDeprecationWarning)
    from starlette.testclient import TestClient

from src.common.config_loader import ServerConfig, from_legacy, parse_config
from src.common.http import API_KEY_HEADER, ApiKeyMiddleware, http_app, run_http
from src.common.server import HEALTH_PATH, MCP_PATH, build_server
from src.main import apply_api_key, cli

API_KEY = "0123456789abcdef0123456789abcdef"
HTTP_LOGGER = "src.common.http"

MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}
TOOLS_LIST = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}


def _config(**root) -> ServerConfig:
    data = {"clusters": [{"name": "a", "hosts": "a:6379"}]}
    data.update(root)
    return parse_config(data)


def _client(config: ServerConfig) -> TestClient:
    return TestClient(http_app(build_server(config), config))


def _headers(**extra) -> dict:
    headers = dict(MCP_HEADERS)
    headers.update(extra)
    return headers


def _unauthorized_records(caplog):
    return [
        r
        for r in caplog.records
        if r.name == HTTP_LOGGER and r.getMessage().startswith("unauthorized request")
    ]


class _App:
    """Inner ASGI app that records the scopes it is called with and answers 200."""

    def __init__(self):
        self.scopes = []

    async def __call__(self, scope, receive, send):
        self.scopes.append(scope)
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})


async def _call(middleware, scope):
    sent = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await middleware(scope, receive, send)
    return sent


def _scope(path=MCP_PATH, headers=(), client=("10.1.2.3", 51234), scope_type="http"):
    scope = {
        "type": scope_type,
        "path": path,
        "method": "POST",
        "headers": list(headers),
    }
    if client is not None:
        scope["client"] = client
    return scope


def _status(sent):
    return next(m["status"] for m in sent if m["type"] == "http.response.start")


def _body(sent):
    return b"".join(
        m.get("body", b"") for m in sent if m["type"] == "http.response.body"
    )


class TestApiKeyEnforced:
    def test_healthz_is_open_without_key(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.get(HEALTH_PATH)

        assert response.status_code == 200
        assert response.json() == {"status": "UP"}

    def test_healthz_accepts_origin_header(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.get(HEALTH_PATH, headers={"Origin": "https://evil.test"})

        assert response.status_code == 200

    def test_missing_key_is_401_json(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post(MCP_PATH, json=TOOLS_LIST, headers=_headers())

        assert response.status_code == 401
        assert response.headers["content-type"] == "application/json"
        body = response.json()
        assert body["error"] == "unauthorized"
        assert "X-Authorization" in body["message"]
        assert int(response.headers["content-length"]) == len(response.content)

    def test_wrong_key_is_401(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": "not-the-key"}),
            )

        assert response.status_code == 401
        assert response.json()["error"] == "unauthorized"

    @pytest.mark.parametrize(
        "provided",
        [
            API_KEY.upper(),
            API_KEY[:-1],
            API_KEY + "0",
            "Bearer " + API_KEY,
            "",
        ],
        ids=["case", "prefix", "longer", "bearer", "empty"],
    )
    def test_key_must_match_exactly(self, provided):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": provided}),
            )

        assert response.status_code == 401

    def test_right_key_passes_and_lists_tools(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": API_KEY}),
            )

        assert response.status_code == 200
        names = [t["name"] for t in response.json()["result"]["tools"]]
        assert "clusters_list" in names

    def test_header_name_is_case_insensitive(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"x-authorization": API_KEY}),
            )

        assert response.status_code == 200

    def test_origin_header_is_403_even_with_right_key(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(
                    **{"X-Authorization": API_KEY, "Origin": "http://localhost:3000"}
                ),
            )

        assert response.status_code == 403
        assert response.headers["content-type"] == "application/json"
        body = response.json()
        assert body["error"] == "forbidden"
        assert "Origin" in body["message"]

    def test_origin_check_comes_before_key_check(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post(
                MCP_PATH, json=TOOLS_LIST, headers=_headers(Origin="null")
            )

        assert response.status_code == 403

    def test_get_on_mcp_is_guarded(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.get(MCP_PATH, headers={"Accept": "text/event-stream"})

        assert response.status_code == 401

    @pytest.mark.parametrize("path", [MCP_PATH + "/", MCP_PATH + "/messages/x"])
    def test_mcp_subpaths_are_guarded(self, path):
        with _client(_config(apiKey=API_KEY)) as client:
            missing = client.post(path, json=TOOLS_LIST, headers=_headers())
            origin = client.post(
                path,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": API_KEY, "Origin": "x"}),
            )

        assert missing.status_code == 401
        assert origin.status_code == 403

    def test_subpath_with_right_key_reaches_the_app(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post(
                MCP_PATH + "/unknown",
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": API_KEY}),
            )

        assert response.status_code not in (401, 403)

    def test_path_that_only_starts_with_mcp_is_not_guarded(self):
        with _client(_config(apiKey=API_KEY)) as client:
            response = client.post("/mcpx", json=TOOLS_LIST, headers=_headers())

        assert response.status_code == 404


class TestApiKeyDisabled:
    @pytest.mark.parametrize("api_key", [None, ""], ids=["unset", "empty"])
    def test_no_key_check(self, api_key):
        root = {} if api_key is None else {"apiKey": api_key}

        with _client(_config(**root)) as client:
            response = client.post(MCP_PATH, json=TOOLS_LIST, headers=_headers())

        assert response.status_code == 200
        assert "result" in response.json()

    def test_any_key_header_is_ignored(self):
        with _client(_config(apiKey="")) as client:
            response = client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": "whatever"}),
            )

        assert response.status_code == 200

    def test_origin_is_still_refused(self):
        with _client(_config(apiKey="")) as client:
            response = client.post(
                MCP_PATH, json=TOOLS_LIST, headers=_headers(Origin="https://a.test")
            )

        assert response.status_code == 403

    def test_warning_logged(self, caplog):
        caplog.set_level(logging.INFO, logger=HTTP_LOGGER)
        config = _config(apiKey="")

        http_app(build_server(config), config)

        warnings_ = [
            r
            for r in caplog.records
            if r.name == HTTP_LOGGER and r.levelno == logging.WARNING
        ]
        assert len(warnings_) == 1
        assert "DISABLED" in warnings_[0].getMessage()
        assert MCP_PATH in warnings_[0].getMessage()


class TestHttpAppLogging:
    def test_enabled_key_logs_info(self, caplog):
        caplog.set_level(logging.INFO, logger=HTTP_LOGGER)
        config = _config(apiKey=API_KEY)

        http_app(build_server(config), config)

        records = [r for r in caplog.records if r.name == HTTP_LOGGER]
        assert [r.levelno for r in records] == [logging.INFO]
        assert "API key authentication enabled" in records[0].getMessage()
        assert API_KEY not in caplog.text

    def test_short_key_warns_but_is_enforced(self, caplog):
        caplog.set_level(logging.INFO, logger=HTTP_LOGGER)
        config = _config(apiKey="short-key")

        app = http_app(build_server(config), config)

        warnings_ = [
            r
            for r in caplog.records
            if r.name == HTTP_LOGGER and r.levelno == logging.WARNING
        ]
        assert len(warnings_) == 1
        assert "shorter than 16 characters" in warnings_[0].getMessage()
        assert "short-key" not in caplog.text
        with TestClient(app) as client:
            missing = client.post(MCP_PATH, json=TOOLS_LIST, headers=_headers())
            right = client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": "short-key"}),
            )
        assert missing.status_code == 401
        assert right.status_code == 200

    def test_key_of_recommended_length_does_not_warn(self, caplog):
        caplog.set_level(logging.INFO, logger=HTTP_LOGGER)
        config = _config(apiKey="k" * 16)

        http_app(build_server(config), config)

        assert not [
            r
            for r in caplog.records
            if r.name == HTTP_LOGGER and r.levelno >= logging.WARNING
        ]

    def test_returns_middleware_around_streamable_http_app(self):
        config = _config(apiKey=API_KEY)

        app = http_app(build_server(config), config)

        assert isinstance(app, ApiKeyMiddleware)
        assert app.api_key == API_KEY.encode("utf-8")


class TestUnauthorizedLog:
    def test_missing_header_logged_without_key(self, caplog):
        caplog.set_level(logging.WARNING, logger=HTTP_LOGGER)

        with _client(_config(apiKey=API_KEY)) as client:
            client.post(MCP_PATH, json=TOOLS_LIST, headers=_headers())

        records = _unauthorized_records(caplog)
        assert len(records) == 1
        record = records[0]
        assert record.levelno == logging.WARNING
        assert record.remoteAddr == "testclient"
        assert record.headerPresent is False
        assert API_KEY not in record.getMessage()
        assert API_KEY not in str(vars(record))

    def test_wrong_header_logged_without_either_key(self, caplog):
        caplog.set_level(logging.WARNING, logger=HTTP_LOGGER)
        wrong = "wrong-key-value-123456"

        with _client(_config(apiKey=API_KEY)) as client:
            client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": wrong}),
            )

        records = _unauthorized_records(caplog)
        assert len(records) == 1
        record = records[0]
        assert record.headerPresent is True
        assert record.remoteAddr == "testclient"
        for secret in (API_KEY, wrong):
            assert secret not in record.getMessage()
            assert secret not in str(vars(record))
            assert secret not in caplog.text

    def test_successful_request_not_logged(self, caplog):
        caplog.set_level(logging.WARNING, logger=HTTP_LOGGER)

        with _client(_config(apiKey=API_KEY)) as client:
            client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": API_KEY}),
            )

        assert _unauthorized_records(caplog) == []

    def test_origin_rejection_not_logged_as_unauthorized(self, caplog):
        caplog.set_level(logging.WARNING, logger=HTTP_LOGGER)

        with _client(_config(apiKey=API_KEY)) as client:
            client.post(MCP_PATH, json=TOOLS_LIST, headers=_headers(Origin="x"))

        assert _unauthorized_records(caplog) == []


class TestApiKeyMiddleware:
    def test_stores_key_as_utf8_bytes(self):
        assert ApiKeyMiddleware(_App(), "abc").api_key == b"abc"

    @pytest.mark.parametrize("api_key", [None, ""])
    def test_no_key(self, api_key):
        assert ApiKeyMiddleware(_App(), api_key).api_key is None

    def test_header_constant(self):
        assert API_KEY_HEADER == "x-authorization"

    async def test_constant_time_compare(self):
        app = _App()
        middleware = ApiKeyMiddleware(app, API_KEY)

        with patch(
            "src.common.http.hmac.compare_digest", wraps=hmac.compare_digest
        ) as compare:
            sent = await _call(
                middleware, _scope(headers=[(b"x-authorization", b"wrong")])
            )

        assert _status(sent) == 401
        compare.assert_called_once_with(b"wrong", API_KEY.encode("utf-8"))
        assert app.scopes == []

    async def test_constant_time_compare_on_match(self):
        app = _App()
        middleware = ApiKeyMiddleware(app, API_KEY)

        with patch(
            "src.common.http.hmac.compare_digest", wraps=hmac.compare_digest
        ) as compare:
            sent = await _call(
                middleware,
                _scope(headers=[(b"x-authorization", API_KEY.encode("ascii"))]),
            )

        assert _status(sent) == 200
        compare.assert_called_once()
        assert len(app.scopes) == 1

    async def test_missing_header_skips_compare(self):
        middleware = ApiKeyMiddleware(_App(), API_KEY)

        with patch("src.common.http.hmac.compare_digest") as compare:
            sent = await _call(middleware, _scope())

        assert _status(sent) == 401
        compare.assert_not_called()

    async def test_non_ascii_key_matches_utf8_header(self):
        key = "schlüssel-ключ-0123456789"
        app = _App()
        middleware = ApiKeyMiddleware(app, key)

        sent = await _call(
            middleware, _scope(headers=[(b"x-authorization", key.encode("utf-8"))])
        )

        assert _status(sent) == 200

    async def test_non_utf8_header_bytes_are_401_not_an_error(self):
        middleware = ApiKeyMiddleware(_App(), API_KEY)

        sent = await _call(
            middleware, _scope(headers=[(b"x-authorization", b"\xff\xfe\x00")])
        )

        assert _status(sent) == 401

    async def test_reject_body_and_headers(self):
        middleware = ApiKeyMiddleware(_App(), API_KEY)

        sent = await _call(middleware, _scope())

        start = sent[0]
        assert start["type"] == "http.response.start"
        headers = dict(start["headers"])
        assert headers[b"content-type"] == b"application/json"
        body = _body(sent)
        assert int(headers[b"content-length"]) == len(body)
        assert json.loads(body)["error"] == "unauthorized"

    async def test_origin_header_any_case(self):
        app = _App()
        middleware = ApiKeyMiddleware(app, None)

        sent = await _call(middleware, _scope(headers=[(b"ORIGIN", b"https://a.b")]))

        assert _status(sent) == 403
        assert json.loads(_body(sent))["error"] == "forbidden"
        assert app.scopes == []

    async def test_log_without_client_address(self, caplog):
        caplog.set_level(logging.WARNING, logger=HTTP_LOGGER)
        middleware = ApiKeyMiddleware(_App(), API_KEY)

        await _call(middleware, _scope(client=None))

        record = _unauthorized_records(caplog)[0]
        assert record.remoteAddr is None
        assert record.headerPresent is False

    async def test_log_has_client_address(self, caplog):
        caplog.set_level(logging.WARNING, logger=HTTP_LOGGER)
        middleware = ApiKeyMiddleware(_App(), API_KEY)

        await _call(middleware, _scope(headers=[(b"x-authorization", b"nope")]))

        record = _unauthorized_records(caplog)[0]
        assert record.remoteAddr == "10.1.2.3"
        assert record.headerPresent is True
        assert "nope" not in record.getMessage()

    @pytest.mark.parametrize("scope_type", ["lifespan", "websocket"])
    async def test_non_http_scopes_pass_through(self, scope_type):
        app = _App()
        middleware = ApiKeyMiddleware(app, API_KEY)

        await _call(
            middleware, _scope(scope_type=scope_type, headers=[(b"origin", b"x")])
        )

        assert [s["type"] for s in app.scopes] == [scope_type]

    async def test_other_paths_pass_through(self):
        app = _App()
        middleware = ApiKeyMiddleware(app, API_KEY)

        sent = await _call(
            middleware, _scope(path=HEALTH_PATH, headers=[(b"origin", b"x")])
        )

        assert _status(sent) == 200
        assert len(app.scopes) == 1

    async def test_scope_without_path_passes_through(self):
        app = _App()
        middleware = ApiKeyMiddleware(app, API_KEY)
        scope = _scope()
        del scope["path"]

        await _call(middleware, scope)

        assert len(app.scopes) == 1


class TestRunHttp:
    def test_runs_uvicorn_with_guarded_app(self):
        config = _config(port="9123", apiKey=API_KEY)
        mcp = build_server(config)

        with patch("src.common.http.uvicorn.run") as mock_run:
            run_http(mcp, config)

        mock_run.assert_called_once()
        args, kwargs = mock_run.call_args
        assert isinstance(args[0], ApiKeyMiddleware)
        assert args[0].api_key == API_KEY.encode("utf-8")
        assert kwargs["host"] == mcp.settings.host
        assert kwargs["port"] == 9123
        assert kwargs["log_config"] is None
        assert kwargs["access_log"] is False


def _legacy_config(api_key=None) -> ServerConfig:
    """The single ``default`` cluster of the env/CLI mode, with --api-key / REDIS_MCP_API_KEY applied."""
    config = from_legacy({"host": "127.0.0.1", "port": 6379})
    apply_api_key(config, api_key)
    return config


def _post(client, api_key=None, message=TOOLS_LIST):
    headers = (
        _headers() if api_key is None else _headers(**{"X-Authorization": api_key})
    )
    return client.post(MCP_PATH, json=message, headers=headers)


class TestLegacyModeApiKey:
    """The legacy single-cluster mode has no config file: --api-key / REDIS_MCP_API_KEY is how its HTTP
    transport gets an API key, and it is enforced exactly like apiKey."""

    def test_missing_and_wrong_key_are_401(self):
        with _client(_legacy_config(API_KEY)) as client:
            missing = _post(client)
            wrong = _post(client, "not-the-key")

        assert missing.status_code == 401
        assert missing.json()["error"] == "unauthorized"
        assert wrong.status_code == 401

    def test_right_key_lists_tools_with_optional_cluster(self):
        with _client(_legacy_config(API_KEY)) as client:
            response = _post(client, API_KEY)

        assert response.status_code == 200
        tools = {t["name"]: t for t in response.json()["result"]["tools"]}
        schema = tools["get"]["inputSchema"]
        cluster = schema["properties"]["cluster"]
        # one cluster: calls written before multi-cluster support (without cluster) keep working
        assert "cluster" not in schema.get("required", [])
        assert cluster["default"] is None
        assert {"type": "string", "enum": ["default"]} in cluster["anyOf"]

    def test_origin_is_403_with_right_key(self):
        with _client(_legacy_config(API_KEY)) as client:
            response = client.post(
                MCP_PATH,
                json=TOOLS_LIST,
                headers=_headers(**{"X-Authorization": API_KEY, "Origin": "x"}),
            )

        assert response.status_code == 403

    @pytest.mark.parametrize("api_key", [None, ""], ids=["unset", "empty"])
    def test_without_key_is_open(self, api_key):
        with _client(_legacy_config(api_key)) as client:
            response = _post(client)

        assert response.status_code == 200


class TestCliHttpApiKey:
    """The CLI end to end up to uvicorn: the app it would serve guards /mcp with the --api-key /
    REDIS_MCP_API_KEY value, in the legacy mode and over a config file's apiKey."""

    @pytest.fixture(autouse=True)
    def isolated_cli(self, monkeypatch):
        for name in ("REDIS_MCP_CONFIG", "REDIS_MCP_CONFIG_DIR", "REDIS_MCP_API_KEY"):
            monkeypatch.delenv(name, raising=False)
        # the CLI writes the legacy settings into the shared REDIS_CFG and sets up logging for HTTP
        with (
            patch.dict("src.common.config.REDIS_CFG"),
            patch("src.main.configure_json_logging"),
        ):
            yield

    @staticmethod
    def served_app(args, env=None):
        """Runs the CLI and returns the ASGI app and config it hands to uvicorn."""
        with patch("src.common.http.uvicorn.run") as mock_run:
            result = CliRunner().invoke(cli, args, env=env)

        assert result.exit_code == 0, result.output
        mock_run.assert_called_once()
        app = mock_run.call_args.args[0]
        assert isinstance(app, ApiKeyMiddleware)
        return app, mock_run.call_args.kwargs

    @pytest.mark.parametrize(
        "args, env",
        [
            (["--transport", "http", "--api-key", API_KEY], None),
            (["--transport", "http"], {"REDIS_MCP_API_KEY": API_KEY}),
            (["--http-port", "9123", "--api-key", API_KEY], None),
        ],
        ids=["option", "env", "http-port"],
    )
    def test_legacy_mode_enforces_key(self, args, env):
        app, kwargs = self.served_app(args, env)

        with TestClient(app) as client:
            missing = _post(client)
            wrong = _post(client, API_KEY + "x")
            right = _post(client, API_KEY)
            health = client.get(HEALTH_PATH)

        assert kwargs["port"] == (9123 if "--http-port" in args else 8080)
        assert missing.status_code == 401
        assert wrong.status_code == 401
        assert right.status_code == 200
        names = [t["name"] for t in right.json()["result"]["tools"]]
        assert "get" in names
        assert health.status_code == 200

    def test_legacy_mode_without_key_is_open(self, caplog):
        caplog.set_level(logging.WARNING, logger=HTTP_LOGGER)

        app, _ = self.served_app(["--transport", "http"])

        with TestClient(app) as client:
            response = _post(client)

        assert response.status_code == 200
        assert any("DISABLED" in r.getMessage() for r in caplog.records)

    def test_env_key_replaces_config_key(self, tmp_path):
        other = "fedcba9876543210fedcba9876543210"
        path = tmp_path / "redis-mcp.yaml"
        path.write_text(
            f"apiKey: {API_KEY}\npasswordsDir: {tmp_path}\n"
            "clusters:\n  - name: a\n    hosts: a:6379\n",
            encoding="utf-8",
        )

        app, _ = self.served_app(
            ["--config", str(path), "--transport", "http"],
            env={"REDIS_MCP_API_KEY": other, "a_pass": None},
        )

        with TestClient(app) as client:
            config_key = _post(client, API_KEY)
            env_key = _post(client, other)

        assert config_key.status_code == 401
        assert env_key.status_code == 200
