"""
Integration tests of the multi-cluster mode with real Redis servers.

Two throwaway redis-server processes are started on free ports (one with --requirepass), a redis-mcp.yaml and a
passwords directory are written, and the MCP server is driven like a client would: over stdio JSON-RPC
(``python -m src.main`` with REDIS_MCP_CONFIG) and over Streamable HTTP (the CLI with --config and --http-port).
The tests are skipped when the redis-server binary is not installed.
"""

import json
import os
import queue
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pytest
import redis
import yaml

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).parent.parent
REDIS_SERVER = shutil.which("redis-server")

PASSWORD = "it-" + secrets.token_hex(12)
API_KEY = secrets.token_hex(24)

# alpha: the password-protected server; beta and gamma: the open server (db 0 and db 1).
WRITABLE = ["alpha", "beta"]
ALL_CLUSTERS = ["alpha", "beta", "gamma"]
DESTRUCTIVE_TOOLS = [
    "delete",
    "hdel",
    "lpop",
    "rpop",
    "lrem",
    "srem",
    "zrem",
    "xdel",
    "xgroup_destroy",
]

STARTUP_TIMEOUT = 10
RESPONSE_TIMEOUT = 15
MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}


@dataclass
class RedisServer:
    port: int
    password: Optional[str]
    process: subprocess.Popen

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def client(self, db: int = 0) -> redis.Redis:
        return redis.Redis(
            host="127.0.0.1",
            port=self.port,
            db=db,
            password=self.password,
            decode_responses=True,
            socket_timeout=5,
        )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _stop(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _start_redis(directory: Path, password: Optional[str]) -> RedisServer:
    """A redis-server without persistence on a free port; retried if another process takes the port first."""
    directory.mkdir(parents=True, exist_ok=True)
    last_error = None
    for _ in range(3):
        port = _free_port()
        args = [
            REDIS_SERVER,
            "--port",
            str(port),
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            str(directory),
            "--daemonize",
            "no",
        ]
        if password:
            args += ["--requirepass", password]
        process = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        server = RedisServer(port=port, password=password, process=process)
        deadline = time.monotonic() + STARTUP_TIMEOUT
        while time.monotonic() < deadline and process.poll() is None:
            try:
                if server.client().ping():
                    return server
            except redis.RedisError as e:
                last_error = e
            time.sleep(0.05)
        _stop(process)
    raise RuntimeError(f"redis-server did not start: {last_error}")


@pytest.fixture(scope="module")
def redis_servers(tmp_path_factory):
    """(protected, open): two redis-server processes for the whole module, stopped at the end."""
    if REDIS_SERVER is None:
        pytest.skip("redis-server binary not found")
    root = tmp_path_factory.mktemp("redis-servers")
    started: List[RedisServer] = []
    try:
        started.append(_start_redis(root / "protected", PASSWORD))
        started.append(_start_redis(root / "open", None))
        yield started[0], started[1]
    finally:
        for server in started:
            _stop(server.process)


@pytest.fixture(autouse=True)
def empty_databases(redis_servers):
    for server in redis_servers:
        server.client().flushall()


def _write_config(
    tmp_path: Path,
    redis_servers,
    alpha_password: Optional[str] = PASSWORD,
) -> Path:
    """redis-mcp.yaml and its passwords directory: alpha (protected, writable), beta (open, writable) and gamma
    (open server, db 1, read-only through the global readOnly)."""
    protected, open_server = redis_servers
    passwords = tmp_path / "passwords"
    passwords.mkdir(exist_ok=True)
    if alpha_password is not None:
        # like a mounted Secret made with `oc create secret generic ... --from-env-file=passwords.env`
        (passwords / "alpha_pass").write_text(alpha_password, encoding="utf-8")
    config = {
        "apiKey": API_KEY,
        "readOnly": True,
        "disableDestructive": True,
        "toolsets": ["core", "data"],
        "passwordsDir": str(passwords),
        "clusters": [
            {
                "name": "alpha",
                "mode": "standalone",
                "hosts": protected.address,
                "readOnly": False,
            },
            {"name": "beta", "hosts": open_server.address, "readOnly": False},
            {"name": "gamma", "hosts": open_server.address, "db": 1},
        ],
    }
    path = tmp_path / "redis-mcp.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


@pytest.fixture
def config_file(tmp_path, redis_servers) -> Path:
    return _write_config(tmp_path, redis_servers)


def _server_env(**extra: str) -> Dict[str, str]:
    """The test's environment without settings that would change which config or passwords the server uses."""
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in ("REDIS_MCP_CONFIG", "REDIS_MCP_CONFIG_DIR")
        and not key.endswith("_pass")
    }
    env.update(extra)
    return env


def _log_tail(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[-4000:]
    except OSError:
        return ""


class StdioClient:
    """Minimal MCP client over the server's stdin/stdout (one JSON-RPC message per line)."""

    def __init__(self, process: subprocess.Popen, log_path: Path):
        self.process = process
        self.log_path = log_path
        self._lines: "queue.Queue[Optional[str]]" = queue.Queue()
        self._next_id = 0
        self._reader = threading.Thread(target=self._pump, daemon=True)
        self._reader.start()

    def _pump(self) -> None:
        for line in self.process.stdout:
            self._lines.put(line)
        self._lines.put(None)

    def close(self) -> None:
        """Stops the server and closes the pipes once the reader has seen the end of stdout."""
        _stop(self.process)
        self._reader.join(timeout=5)
        for pipe in (self.process.stdin, self.process.stdout):
            try:
                pipe.close()
            except (OSError, ValueError):
                pass

    def _fail(self, message: str):
        pytest.fail(f"{message}\n--- server log ---\n{_log_tail(self.log_path)}")

    def send(self, message: Dict[str, Any]) -> None:
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def request(self, method: str, params: Optional[Dict[str, Any]] = None):
        self._next_id += 1
        request_id = self._next_id
        message: Dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        self.send(message)
        deadline = time.monotonic() + RESPONSE_TIMEOUT
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._fail(f"No response to {method} within {RESPONSE_TIMEOUT} s")
            try:
                line = self._lines.get(timeout=remaining)
            except queue.Empty:
                continue
            if line is None:
                self._fail(f"The server exited while waiting for {method}")
            response = json.loads(line)
            if response.get("id") == request_id:
                return response

    def initialize(self) -> Dict[str, Any]:
        response = self.request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "multi-cluster-test", "version": "1.0.0"},
            },
        )
        if "result" not in response:
            self._fail(f"initialize failed: {response}")
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return response["result"]

    def tools(self) -> Dict[str, Dict[str, Any]]:
        response = self.request("tools/list")
        return {tool["name"]: tool for tool in response["result"]["tools"]}

    def call(self, name: str, **arguments: Any) -> Dict[str, Any]:
        response = self.request("tools/call", {"name": name, "arguments": arguments})
        if "result" not in response:
            self._fail(f"tools/call {name} failed: {response}")
        return response["result"]


def _text(result: Dict[str, Any]) -> str:
    return "".join(c["text"] for c in result["content"] if c["type"] == "text")


def _rows(result: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """clusters_list rows by cluster name."""
    assert not result.get("isError"), _text(result)
    return {row["name"]: row for row in result["structuredContent"]["result"]}


@contextmanager
def _stdio_server(tmp_path: Path, config: Path):
    log_path = tmp_path / "stdio-server.log"
    with open(log_path, "w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "src.main"],
            cwd=PROJECT_ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=_server_env(REDIS_MCP_CONFIG=str(config)),
        )
        client = StdioClient(process, log_path)
        try:
            client.initialize()
            yield client
        finally:
            client.close()


@pytest.fixture
def stdio(tmp_path, config_file):
    with _stdio_server(tmp_path, config_file) as client:
        yield client


class TestStdio:
    def test_tools_list_cluster_enums(self, stdio):
        tools = stdio.tools()

        get_schema = tools["get"]["inputSchema"]
        assert get_schema["properties"]["cluster"]["enum"] == ALL_CLUSTERS
        assert "cluster" in get_schema["required"]

        # write tools only on the clusters with readOnly: false
        for name in ("set", "hset", "lpush", "sadd", "zadd", "xadd", "expire"):
            schema = tools[name]["inputSchema"]
            assert schema["properties"]["cluster"]["enum"] == WRITABLE, name
            assert tools[name]["description"].startswith("WRITE:"), name
            assert tools[name]["annotations"]["readOnlyHint"] is False, name

        # clusters_list takes an optional cluster
        clusters_schema = tools["clusters_list"]["inputSchema"]
        assert "cluster" not in clusters_schema.get("required", [])
        assert tools["clusters_list"]["annotations"]["readOnlyHint"] is True

    def test_tools_list_honours_disable_destructive_and_toolsets(self, stdio):
        tools = stdio.tools()

        for name in DESTRUCTIVE_TOOLS:
            assert name not in tools, f"destructive tool {name} is listed"
        # only the core and data toolsets
        for name in (
            "json_get",
            "json_set",
            "get_indexes",
            "vector_search_hash",
            "publish",
            "subscribe",
            "search_redis_documents",
        ):
            assert name not in tools, f"{name} is listed outside its toolset"
        for name in ("clusters_list", "info", "dbsize", "scan_keys", "get", "hget"):
            assert name in tools

    def test_set_and_get_are_isolated_per_cluster(self, stdio, redis_servers):
        protected, open_server = redis_servers
        key = "mcp:it:isolation"

        for cluster in WRITABLE:
            result = stdio.call(
                "set", cluster=cluster, key=key, value=f"from-{cluster}"
            )
            assert not result.get("isError"), _text(result)
            assert _text(result) == f"Successfully set {key}"

        assert _text(stdio.call("get", cluster="alpha", key=key)) == "from-alpha"
        assert _text(stdio.call("get", cluster="beta", key=key)) == "from-beta"
        # gamma is db 1 of beta's server
        assert (
            _text(stdio.call("get", cluster="gamma", key=key))
            == f"Key {key} does not exist"
        )

        # what really landed in Redis
        assert protected.client().get(key) == "from-alpha"
        assert open_server.client(db=0).get(key) == "from-beta"
        assert open_server.client(db=1).get(key) is None

    def test_write_on_read_only_cluster_is_rejected(self, stdio, redis_servers):
        _, open_server = redis_servers

        result = stdio.call("set", cluster="gamma", key="mcp:it:ro", value="nope")

        assert result.get("isError") is True
        assert "gamma" in _text(result)
        assert open_server.client(db=1).get("mcp:it:ro") is None

    def test_unknown_and_missing_cluster_are_rejected(self, stdio):
        unknown = stdio.call("get", cluster="delta", key="k")
        missing = stdio.call("get", key="k")

        assert unknown.get("isError") is True
        assert missing.get("isError") is True

    def test_read_tools_see_their_own_cluster(self, stdio, redis_servers):
        protected, open_server = redis_servers
        protected.client().set("only:alpha", "1")
        open_server.client(db=1).mset({"only:gamma:1": "1", "only:gamma:2": "2"})

        assert _text(stdio.call("dbsize", cluster="alpha")) == "1"
        assert _text(stdio.call("dbsize", cluster="beta")) == "0"
        assert _text(stdio.call("dbsize", cluster="gamma")) == "2"

    def test_clusters_list(self, stdio, redis_servers):
        protected, _ = redis_servers
        version = protected.client().info("server")["redis_version"]

        result = stdio.call("clusters_list")
        rows = _rows(result)

        assert list(rows) == ALL_CLUSTERS
        for name, row in rows.items():
            assert row["status"] == "ok", row
            assert row["redisVersion"] == version
            assert row["role"] == "master"
            assert isinstance(row["latencyMs"], int)
            assert row["mode"] == "standalone"
        assert rows["alpha"]["auth"] == "password"
        assert rows["beta"]["auth"] == "none"
        assert rows["gamma"]["auth"] == "none"
        assert rows["alpha"]["readOnly"] is False
        assert rows["beta"]["readOnly"] is False
        assert rows["gamma"]["readOnly"] is True
        assert rows["gamma"]["db"] == 1
        assert rows["alpha"]["hosts"] == protected.address
        assert PASSWORD not in json.dumps(result)

    def test_clusters_list_one_cluster_without_check(self, stdio):
        rows = _rows(stdio.call("clusters_list", cluster="alpha", check=False))

        assert list(rows) == ["alpha"]
        assert rows["alpha"]["auth"] == "password"
        assert "status" not in rows["alpha"]


class TestStdioWrongPassword:
    def test_bad_password_only_breaks_its_own_cluster(self, tmp_path, redis_servers):
        config = _write_config(tmp_path, redis_servers, alpha_password="wrong")

        with _stdio_server(tmp_path, config) as client:
            result = client.call("clusters_list")
            rows = _rows(result)
            alpha_get = _text(client.call("get", cluster="alpha", key="k"))
            beta_set = client.call("set", cluster="beta", key="k", value="v")

        assert rows["alpha"]["status"] == "error"
        assert "AuthenticationError" in rows["alpha"]["error"]
        assert rows["beta"]["status"] == "ok"
        assert rows["gamma"]["status"] == "ok"
        assert "wrong" not in json.dumps(result)
        assert alpha_get.startswith("Error retrieving key k")
        assert not beta_set.get("isError"), _text(beta_set)


@contextmanager
def _http_server(tmp_path: Path, config: Path):
    """The CLI entry point with --config and --http-port; yields the base URL once /healthz answers."""
    port = _free_port()
    log_path = tmp_path / "http-server.log"
    with open(log_path, "w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from src.main import cli; cli()",
                "--config",
                str(config),
                "--http-port",
                str(port),
            ],
            cwd=PROJECT_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=_server_env(),
        )
        try:
            base_url = f"http://127.0.0.1:{port}"
            deadline = time.monotonic() + STARTUP_TIMEOUT
            with httpx.Client(trust_env=False, timeout=2) as http:
                while True:
                    if process.poll() is not None:
                        pytest.fail(
                            f"The HTTP server exited ({process.returncode})\n{_log_tail(log_path)}"
                        )
                    try:
                        if http.get(base_url + "/healthz").status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    if time.monotonic() > deadline:
                        pytest.fail(
                            f"/healthz did not answer within {STARTUP_TIMEOUT} s\n{_log_tail(log_path)}"
                        )
                    time.sleep(0.1)
            yield base_url
        finally:
            _stop(process)


def _rpc(method: str, params: Optional[Dict[str, Any]] = None, request_id: int = 1):
    message: Dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    return message


class TestHttp:
    def test_api_key_and_tool_call(self, tmp_path, config_file, redis_servers):
        protected, _ = redis_servers
        call_set = _rpc(
            "tools/call",
            {
                "name": "set",
                "arguments": {"cluster": "alpha", "key": "mcp:it:http", "value": "v1"},
            },
        )

        with _http_server(tmp_path, config_file) as base_url:
            with httpx.Client(
                base_url=base_url, trust_env=False, timeout=RESPONSE_TIMEOUT
            ) as http:
                health = http.get("/healthz")
                without_key = http.post("/mcp", json=call_set, headers=MCP_HEADERS)
                wrong_key = http.post(
                    "/mcp",
                    json=call_set,
                    headers={**MCP_HEADERS, "X-Authorization": API_KEY + "x"},
                )
                with_origin = http.post(
                    "/mcp",
                    json=call_set,
                    headers={
                        **MCP_HEADERS,
                        "X-Authorization": API_KEY,
                        "Origin": "https://evil.example",
                    },
                )
                authorized = {**MCP_HEADERS, "X-Authorization": API_KEY}
                initialize = http.post(
                    "/mcp",
                    json=_rpc(
                        "initialize",
                        {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "clientInfo": {"name": "http-test", "version": "1.0.0"},
                        },
                    ),
                    headers=authorized,
                )
                set_response = http.post(
                    "/mcp", json={**call_set, "id": 2}, headers=authorized
                )
                get_response = http.post(
                    "/mcp",
                    json=_rpc(
                        "tools/call",
                        {
                            "name": "get",
                            "arguments": {"cluster": "alpha", "key": "mcp:it:http"},
                        },
                        request_id=3,
                    ),
                    headers=authorized,
                )
                list_response = http.post(
                    "/mcp", json=_rpc("tools/list", request_id=4), headers=authorized
                )

        assert health.status_code == 200
        assert health.json() == {"status": "UP"}

        assert without_key.status_code == 401
        assert without_key.json()["error"] == "unauthorized"
        assert wrong_key.status_code == 401
        assert with_origin.status_code == 403

        assert initialize.status_code == 200, initialize.text
        assert initialize.json()["result"]["serverInfo"]["name"] == "Redis MCP Server"

        assert set_response.status_code == 200, set_response.text
        set_result = set_response.json()["result"]
        assert not set_result.get("isError"), set_result
        assert _text(set_result) == "Successfully set mcp:it:http"

        assert get_response.status_code == 200, get_response.text
        assert _text(get_response.json()["result"]) == "v1"
        assert protected.client().get("mcp:it:http") == "v1"

        tools = {t["name"]: t for t in list_response.json()["result"]["tools"]}
        assert tools["set"]["inputSchema"]["properties"]["cluster"]["enum"] == WRITABLE
        assert "delete" not in tools

    def test_rejected_request_does_not_run_the_tool(
        self, tmp_path, config_file, redis_servers
    ):
        protected, _ = redis_servers
        call_set = _rpc(
            "tools/call",
            {
                "name": "set",
                "arguments": {"cluster": "alpha", "key": "mcp:it:denied", "value": "x"},
            },
        )

        with _http_server(tmp_path, config_file) as base_url:
            with httpx.Client(
                base_url=base_url, trust_env=False, timeout=RESPONSE_TIMEOUT
            ) as http:
                denied = http.post("/mcp", json=call_set, headers=MCP_HEADERS)

        assert denied.status_code == 401
        assert protected.client().get("mcp:it:denied") is None
