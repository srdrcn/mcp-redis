"""
Integration tests of the multi-cluster mode with real Redis servers.

Throwaway redis-server processes are started on free ports (tests/redis_servers.py): two standalone servers (one
with --requirepass) and, where redis-cli is installed, a Redis Cluster of three primaries. A redis-mcp.yaml and a
passwords directory are written, and the MCP server is driven like a client would: over stdio JSON-RPC
(``python -m src.main`` with REDIS_MCP_CONFIG) and over Streamable HTTP (the CLI with --config and --http-port,
or the single-cluster --url mode with --transport http and REDIS_MCP_API_KEY).
The tests are skipped when the redis-server binary (for the Redis Cluster also redis-cli) is not installed.
"""

import json
import os
import queue
import secrets
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
import pytest
import yaml

from tests import redis_servers
from tests.redis_servers import RedisClusterNodes, RedisServer

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).parent.parent

PASSWORD = "it-" + secrets.token_hex(12)
API_KEY = secrets.token_hex(24)

# alpha: the password-protected server; beta and gamma: the open server (db 0 and db 1).
WRITABLE = ["alpha", "beta"]
ALL_CLUSTERS = ["alpha", "beta", "gamma"]
# the Redis Cluster (mode: cluster, three primaries), writable; only in the TestRedisCluster config
REDIS_CLUSTER = "omega"
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


@pytest.fixture(scope="module")
def standalone(tmp_path_factory):
    """(protected, open): two redis-server processes for the whole module, stopped at the end."""
    redis_servers.skip_unless_installed()
    root = tmp_path_factory.mktemp("redis-servers")
    with redis_servers.running(root / "protected", PASSWORD) as protected:
        with redis_servers.running(root / "open") as open_server:
            yield protected, open_server


@pytest.fixture(scope="module")
def redis_cluster(tmp_path_factory):
    """A Redis Cluster of three primaries (created with redis-cli) for the whole module."""
    redis_servers.skip_unless_installed(cluster=True)
    with redis_servers.running_cluster(
        tmp_path_factory.mktemp("redis-cluster")
    ) as cluster:
        yield cluster


def _flushall(server: RedisServer) -> None:
    client = server.client()
    try:
        client.flushall()
    finally:
        client.close()


@pytest.fixture(autouse=True)
def empty_databases(standalone):
    for server in standalone:
        _flushall(server)


@pytest.fixture
def empty_cluster(redis_cluster) -> RedisClusterNodes:
    for node in redis_cluster.nodes:
        _flushall(node)
    return redis_cluster


def _value(server: RedisServer, key: str, db: int = 0) -> Optional[str]:
    """The key's value read straight from the server (on a cluster node: a key of a slot the node serves)."""
    client = server.client(db=db)
    try:
        return client.get(key)
    finally:
        client.close()


def _dbsize(server: RedisServer, db: int = 0) -> int:
    client = server.client(db=db)
    try:
        return client.dbsize()
    finally:
        client.close()


def _write(tmp_path: Path, config: Dict[str, Any]) -> Path:
    path = tmp_path / "redis-mcp.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def _passwords_dir(tmp_path: Path, alpha_password: Optional[str]) -> Path:
    passwords = tmp_path / "passwords"
    passwords.mkdir(exist_ok=True)
    if alpha_password is not None:
        # like a mounted Secret made with `oc create secret generic ... --from-env-file=passwords.env`
        (passwords / "alpha_pass").write_text(alpha_password, encoding="utf-8")
    return passwords


def _write_config(
    tmp_path: Path,
    standalone,
    alpha_password: Optional[str] = PASSWORD,
    cluster: Optional[RedisClusterNodes] = None,
) -> Path:
    """redis-mcp.yaml and its passwords directory: alpha (protected, writable), beta (open, writable) and gamma
    (open server, db 1, read-only through the global readOnly); with ``cluster`` also omega, the Redis Cluster
    (writable)."""
    protected, open_server = standalone
    clusters = [
        {
            "name": "alpha",
            "mode": "standalone",
            "hosts": protected.address,
            "readOnly": False,
        },
        {"name": "beta", "hosts": open_server.address, "readOnly": False},
        {"name": "gamma", "hosts": open_server.address, "db": 1},
    ]
    if cluster is not None:
        clusters.append(
            {
                "name": REDIS_CLUSTER,
                "mode": "cluster",
                "hosts": cluster.addresses,
                "readOnly": False,
            }
        )
    return _write(
        tmp_path,
        {
            "apiKey": API_KEY,
            "readOnly": True,
            "disableDestructive": True,
            "toolsets": ["core", "data"],
            "passwordsDir": str(_passwords_dir(tmp_path, alpha_password)),
            "clusters": clusters,
        },
    )


@pytest.fixture
def config_file(tmp_path, standalone) -> Path:
    return _write_config(tmp_path, standalone)


def _server_env(**extra: str) -> Dict[str, str]:
    """The test's environment without settings that would change which config, Redis, API key or passwords
    the server uses (REDIS_MCP_CONFIG, REDIS_MCP_API_KEY, the legacy REDIS_HOST/REDIS_PWD/..., <name>_pass)."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("REDIS_") and not key.endswith("_pass")
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
        redis_servers.stop(self.process)
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

    def call(self, tool: str, /, **arguments: Any) -> Dict[str, Any]:
        response = self.request("tools/call", {"name": tool, "arguments": arguments})
        if "result" not in response:
            self._fail(f"tools/call {tool} failed: {response}")
        return response["result"]


def _text(result: Dict[str, Any]) -> str:
    return "".join(c["text"] for c in result["content"] if c["type"] == "text")


def _rows(result: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """clusters_list rows by cluster name."""
    assert not result.get("isError"), _text(result)
    return {row["name"]: row for row in result["structuredContent"]["result"]}


def _cluster_enum(schema: Dict[str, Any]) -> Any:
    """The cluster names a tool accepts: the enum of a required cluster parameter, or of the non-null branch of
    an optional one (single-cluster configs)."""
    cluster = schema["properties"]["cluster"]
    if "enum" in cluster:
        return cluster["enum"]
    return next(branch["enum"] for branch in cluster["anyOf"] if "enum" in branch)


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
        # several clusters: the cluster parameter is required
        assert "cluster" in get_schema["required"]

        # write tools only on the clusters with readOnly: false
        for name in ("set", "hset", "lpush", "sadd", "zadd", "xadd", "expire"):
            schema = tools[name]["inputSchema"]
            assert schema["properties"]["cluster"]["enum"] == WRITABLE, name
            assert "cluster" in schema["required"], name
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

    def test_set_and_get_are_isolated_per_cluster(self, stdio, standalone):
        protected, open_server = standalone
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
        assert _value(protected, key) == "from-alpha"
        assert _value(open_server, key) == "from-beta"
        assert _value(open_server, key, db=1) is None

    def test_write_on_read_only_cluster_is_rejected(self, stdio, standalone):
        _, open_server = standalone

        result = stdio.call("set", cluster="gamma", key="mcp:it:ro", value="nope")

        assert result.get("isError") is True
        assert "gamma" in _text(result)
        assert _value(open_server, "mcp:it:ro", db=1) is None

    def test_unknown_and_missing_cluster_are_rejected(self, stdio):
        unknown = stdio.call("get", cluster="delta", key="k")
        missing = stdio.call("get", key="k")

        assert unknown.get("isError") is True
        assert missing.get("isError") is True

    def test_read_tools_see_their_own_cluster(self, stdio, standalone):
        protected, open_server = standalone
        protected.client().set("only:alpha", "1")
        open_server.client(db=1).mset({"only:gamma:1": "1", "only:gamma:2": "2"})

        assert _text(stdio.call("dbsize", cluster="alpha")) == "1"
        assert _text(stdio.call("dbsize", cluster="beta")) == "0"
        assert _text(stdio.call("dbsize", cluster="gamma")) == "2"

    def test_clusters_list(self, stdio, standalone):
        protected, _ = standalone
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
    def test_bad_password_only_breaks_its_own_cluster(self, tmp_path, standalone):
        config = _write_config(tmp_path, standalone, alpha_password="wrong")

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


class TestStdioSingleCluster:
    """A config with one cluster: the cluster parameter is optional, like in the legacy env/CLI mode."""

    def test_calls_without_cluster_use_the_only_cluster(self, tmp_path, standalone):
        protected, open_server = standalone
        config = _write(
            tmp_path,
            {
                "toolsets": ["core", "data"],
                "passwordsDir": str(_passwords_dir(tmp_path, PASSWORD)),
                "clusters": [
                    {"name": "alpha", "hosts": protected.address, "readOnly": False}
                ],
            },
        )
        key = "mcp:it:single"

        with _stdio_server(tmp_path, config) as client:
            tools = client.tools()
            set_result = client.call("set", key=key, value="v1")
            get_result = client.call("get", key=key)
            named = client.call("get", cluster="alpha", key=key)
            unknown = client.call("get", cluster="beta", key=key)

        schema = tools["get"]["inputSchema"]
        assert "cluster" not in schema.get("required", [])
        assert _cluster_enum(schema) == ["alpha"]
        assert not set_result.get("isError"), _text(set_result)
        assert _text(get_result) == "v1"
        assert _text(named) == "v1"
        assert unknown.get("isError") is True
        assert _value(protected, key) == "v1"
        assert _value(open_server, key) is None


def _keys_on_every_node(
    cluster: RedisClusterNodes, prefix: str
) -> Dict[str, RedisServer]:
    """One key per node of the Redis Cluster, mapped to the node that serves its hash slot."""
    keys: Dict[str, RedisServer] = {}
    for i in range(1000):
        key = f"{prefix}:{i}"
        node = cluster.node_for_key(key)
        if all(node.port != taken.port for taken in keys.values()):
            keys[key] = node
            if len(keys) == len(cluster.nodes):
                return keys
    raise AssertionError(f"no keys found for every node of {cluster.addresses}")


@pytest.fixture
def cluster_config(tmp_path, standalone, empty_cluster) -> Path:
    return _write_config(tmp_path, standalone, cluster=empty_cluster)


@pytest.fixture
def cluster_stdio(tmp_path, cluster_config):
    with _stdio_server(tmp_path, cluster_config) as client:
        yield client


class TestRedisCluster:
    """A Redis Cluster (mode: cluster, three primaries) next to the standalone clusters of the same server."""

    def test_tools_list_includes_the_redis_cluster(self, cluster_stdio):
        tools = cluster_stdio.tools()

        assert tools["get"]["inputSchema"]["properties"]["cluster"]["enum"] == [
            *ALL_CLUSTERS,
            REDIS_CLUSTER,
        ]
        assert tools["set"]["inputSchema"]["properties"]["cluster"]["enum"] == [
            *WRITABLE,
            REDIS_CLUSTER,
        ]

    def test_keys_reach_the_node_of_their_slot(
        self, cluster_stdio, empty_cluster, standalone
    ):
        keys = _keys_on_every_node(empty_cluster, "mcp:it:slot")

        set_results = {
            key: cluster_stdio.call(
                "set", cluster=REDIS_CLUSTER, key=key, value=f"v-{key}"
            )
            for key in keys
        }
        get_results = {
            key: _text(cluster_stdio.call("get", cluster=REDIS_CLUSTER, key=key))
            for key in keys
        }

        for key, node in keys.items():
            assert not set_results[key].get("isError"), _text(set_results[key])
            assert get_results[key] == f"v-{key}"
            # stored on the primary that serves the key's slot, one key per node
            assert _value(node, key) == f"v-{key}"
            assert _dbsize(node) == 1
        # and not on the standalone servers
        for server in standalone:
            assert _dbsize(server) == 0

    def test_hash_on_the_redis_cluster(self, cluster_stdio, empty_cluster):
        hset = cluster_stdio.call(
            "hset", cluster=REDIS_CLUSTER, name="mcp:it:hash", key="f", value="1"
        )
        hget = cluster_stdio.call(
            "hget", cluster=REDIS_CLUSTER, name="mcp:it:hash", key="f"
        )

        assert not hset.get("isError"), _text(hset)
        assert _text(hget) == "1"
        client = empty_cluster.client()
        try:
            assert client.hget("mcp:it:hash", "f") == "1"
        finally:
            client.close()

    def test_clusters_list(self, cluster_stdio, empty_cluster):
        rows = _rows(cluster_stdio.call("clusters_list"))

        assert list(rows) == [*ALL_CLUSTERS, REDIS_CLUSTER]
        for name, row in rows.items():
            assert row["status"] == "ok", row
        row = rows[REDIS_CLUSTER]
        assert row["mode"] == "cluster"
        assert row["hosts"] == ",".join(empty_cluster.addresses)
        assert row["primaries"] == len(empty_cluster.nodes) == 3
        assert row["role"] == "master"
        assert row["auth"] == "none"
        assert row["readOnly"] is False
        assert "db" not in row
        assert "primaries" not in rows["alpha"]

    def test_cluster_parameter_still_required(self, cluster_stdio):
        missing = cluster_stdio.call("get", key="k")

        assert missing.get("isError") is True

    @pytest.mark.xfail(
        strict=True,
        reason="src bug: dbsize on a Redis Cluster counts one node only (redis-py sends DBSIZE to the default"
        " node), so it reports a fraction of the keys",
    )
    def test_dbsize_counts_the_keys_of_every_node(self, cluster_stdio, empty_cluster):
        keys = _keys_on_every_node(empty_cluster, "mcp:it:dbsize")
        client = empty_cluster.client()
        try:
            for key in keys:
                client.set(key, "1")
        finally:
            client.close()

        result = cluster_stdio.call("dbsize", cluster=REDIS_CLUSTER)

        assert _text(result) == str(len(keys))

    @pytest.mark.xfail(
        strict=True,
        reason="src bug: scan_all_keys on a Redis Cluster fails (RedisCluster.scan returns a cursor per node,"
        " which the loop passes back as the cursor: \"Invalid input of type: 'dict'\")",
    )
    def test_scan_all_keys_sees_every_node(self, cluster_stdio, empty_cluster):
        keys = _keys_on_every_node(empty_cluster, "mcp:it:scan")
        client = empty_cluster.client()
        try:
            for key in keys:
                client.set(key, "1")
        finally:
            client.close()

        result = cluster_stdio.call(
            "scan_all_keys", cluster=REDIS_CLUSTER, pattern="mcp:it:scan:*"
        )

        assert not result.get("isError"), _text(result)
        assert sorted(result["structuredContent"]["result"]) == sorted(keys)


@contextmanager
def _http_server(tmp_path: Path, *args: str, **env: str):
    """The CLI entry point with the given arguments plus --http-port; yields the base URL once /healthz
    answers. ``env`` is added to the server's environment."""
    port = redis_servers.free_port()
    log_path = tmp_path / "http-server.log"
    with open(log_path, "w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from src.main import cli; cli()",
                *args,
                "--http-port",
                str(port),
            ],
            cwd=PROJECT_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=_server_env(**env),
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
            redis_servers.stop(process)


def _rpc(method: str, params: Optional[Dict[str, Any]] = None, request_id: int = 1):
    message: Dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    return message


def _initialize(request_id: int = 1) -> Dict[str, Any]:
    return _rpc(
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "http-test", "version": "1.0.0"},
        },
        request_id=request_id,
    )


def _tool_call(name: str, arguments: Dict[str, Any], request_id: int = 1):
    return _rpc(
        "tools/call", {"name": name, "arguments": arguments}, request_id=request_id
    )


class TestHttp:
    def test_api_key_and_tool_call(self, tmp_path, config_file, standalone):
        protected, _ = standalone
        call_set = _tool_call(
            "set", {"cluster": "alpha", "key": "mcp:it:http", "value": "v1"}
        )

        with _http_server(tmp_path, "--config", str(config_file)) as base_url:
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
                initialize = http.post("/mcp", json=_initialize(), headers=authorized)
                set_response = http.post(
                    "/mcp", json={**call_set, "id": 2}, headers=authorized
                )
                get_response = http.post(
                    "/mcp",
                    json=_tool_call(
                        "get", {"cluster": "alpha", "key": "mcp:it:http"}, 3
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
        assert _value(protected, "mcp:it:http") == "v1"

        tools = {t["name"]: t for t in list_response.json()["result"]["tools"]}
        assert tools["set"]["inputSchema"]["properties"]["cluster"]["enum"] == WRITABLE
        assert "delete" not in tools

    def test_rejected_request_does_not_run_the_tool(
        self, tmp_path, config_file, standalone
    ):
        protected, _ = standalone
        call_set = _tool_call(
            "set", {"cluster": "alpha", "key": "mcp:it:denied", "value": "x"}
        )

        with _http_server(tmp_path, "--config", str(config_file)) as base_url:
            with httpx.Client(
                base_url=base_url, trust_env=False, timeout=RESPONSE_TIMEOUT
            ) as http:
                denied = http.post("/mcp", json=call_set, headers=MCP_HEADERS)

        assert denied.status_code == 401
        assert _value(protected, "mcp:it:denied") is None

    def test_api_key_env_replaces_config_key(self, tmp_path, config_file):
        """REDIS_MCP_API_KEY (e.g. from a Secret) wins over the apiKey of the config file."""
        env_key = secrets.token_hex(24)

        with _http_server(
            tmp_path, "--config", str(config_file), REDIS_MCP_API_KEY=env_key
        ) as base_url:
            with httpx.Client(
                base_url=base_url, trust_env=False, timeout=RESPONSE_TIMEOUT
            ) as http:
                config_key = http.post(
                    "/mcp",
                    json=_rpc("tools/list"),
                    headers={**MCP_HEADERS, "X-Authorization": API_KEY},
                )
                with_env_key = http.post(
                    "/mcp",
                    json=_rpc("tools/list"),
                    headers={**MCP_HEADERS, "X-Authorization": env_key},
                )

        assert config_key.status_code == 401
        assert with_env_key.status_code == 200, with_env_key.text


class TestLegacyHttp:
    """The single-cluster mode without a config file (--url) over HTTP: REDIS_MCP_API_KEY guards /mcp, and
    tool calls need no cluster argument."""

    def test_api_key_env_and_calls_without_cluster(self, tmp_path, standalone):
        _, open_server = standalone
        call_set = _tool_call("set", {"key": "mcp:it:legacy", "value": "v1"})
        denied_set = _tool_call("set", {"key": "mcp:it:legacy:denied", "value": "x"})

        with _http_server(
            tmp_path,
            "--url",
            f"redis://{open_server.address}/0",
            "--transport",
            "http",
            REDIS_MCP_API_KEY=API_KEY,
        ) as base_url:
            with httpx.Client(
                base_url=base_url, trust_env=False, timeout=RESPONSE_TIMEOUT
            ) as http:
                without_key = http.post("/mcp", json=denied_set, headers=MCP_HEADERS)
                authorized = {**MCP_HEADERS, "X-Authorization": API_KEY}
                initialize = http.post("/mcp", json=_initialize(), headers=authorized)
                set_response = http.post(
                    "/mcp", json={**call_set, "id": 2}, headers=authorized
                )
                get_response = http.post(
                    "/mcp",
                    json=_tool_call("get", {"key": "mcp:it:legacy"}, 3),
                    headers=authorized,
                )
                clusters_response = http.post(
                    "/mcp",
                    json=_tool_call("clusters_list", {}, 4),
                    headers=authorized,
                )
                list_response = http.post(
                    "/mcp", json=_rpc("tools/list", request_id=5), headers=authorized
                )

        assert without_key.status_code == 401
        assert without_key.json()["error"] == "unauthorized"
        assert _value(open_server, "mcp:it:legacy:denied") is None

        assert initialize.status_code == 200, initialize.text
        assert set_response.status_code == 200, set_response.text
        set_result = set_response.json()["result"]
        assert not set_result.get("isError"), set_result
        assert _text(get_response.json()["result"]) == "v1"
        assert _value(open_server, "mcp:it:legacy") == "v1"

        rows = _rows(clusters_response.json()["result"])
        assert list(rows) == ["default"]
        assert rows["default"]["status"] == "ok", rows["default"]
        assert rows["default"]["hosts"] == open_server.address

        tools = {t["name"]: t for t in list_response.json()["result"]["tools"]}
        schema = tools["set"]["inputSchema"]
        assert "cluster" not in schema.get("required", [])
        assert _cluster_enum(schema) == ["default"]
        # the legacy mode lists every toolset
        assert "delete" in tools
        assert "json_get" in tools
