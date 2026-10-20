"""
Integration tests for redis-mcp.

These tests actually start the MCP server process and verify it can handle real requests.
"""

import json
import subprocess
import sys
import time
import os
import uuid
from pathlib import Path

import pytest
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

# What the legacy single-cluster mode describes as its only cluster, named "default".
LEGACY_CLUSTER = "default"


def _redis_client() -> redis.Redis:
    """A client of the Redis the server process connects to (REDIS_HOST/REDIS_PORT, default localhost:6379)."""
    env = _legacy_env()
    return redis.Redis(
        host=env["REDIS_HOST"],
        port=int(env["REDIS_PORT"]),
        db=int(env.get("REDIS_DB") or 0),
        username=env.get("REDIS_USERNAME") or None,
        password=env.get("REDIS_PWD") or None,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=5,
        # one attempt: where no Redis runs (Windows/macOS CI) the check must fail fast
        retry=Retry(NoBackoff(), 0),
    )


def _redis_available():
    """Check if Redis is available for testing."""
    try:
        client = _redis_client()
        try:
            return client.ping()
        finally:
            client.close()
    except Exception:
        return False


def _create_server_process(project_root):
    """Create a server process with proper encoding for cross-platform compatibility."""
    return subprocess.Popen(
        [sys.executable, "-m", "src.main"],
        cwd=project_root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",  # Replace invalid characters instead of failing
        env=_legacy_env(),
    )


def _legacy_env():
    """The environment of the legacy (single-cluster) mode: no multi-cluster config from the caller's env."""
    env = {"REDIS_HOST": "localhost", "REDIS_PORT": "6379", **dict(os.environ)}
    env.pop("REDIS_MCP_CONFIG", None)
    env.pop("REDIS_MCP_CONFIG_DIR", None)
    return env


@pytest.mark.integration
class TestMCPServerIntegration:
    """Integration tests that start the actual MCP server."""

    @pytest.fixture
    def server_process(self):
        """Start the MCP server process for testing."""
        # Get the project root directory
        project_root = Path(__file__).parent.parent

        # Start the server process with proper encoding for cross-platform compatibility
        process = _create_server_process(project_root)

        # Give the server a moment to start
        time.sleep(1)

        yield process

        # Clean up
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        for pipe in (process.stdin, process.stdout, process.stderr):
            pipe.close()

    def test_server_starts_successfully(self, server_process):
        """Test that the MCP server starts without crashing."""
        # Check if process is still running
        assert server_process.poll() is None, "Server process should be running"

        # Check for startup message in stderr
        # Note: MCP servers typically output startup info to stderr
        time.sleep(0.5)  # Give time for startup message

        # The server should still be running
        assert server_process.poll() is None

    def test_server_handles_unicode_on_windows(self, server_process):
        """Test that the server handles Unicode properly on Windows."""
        # This test specifically addresses the Windows Unicode decode error
        # Check if process is still running
        assert server_process.poll() is None, "Server process should be running"

        # Try to read any available output without blocking
        # This should not cause a UnicodeDecodeError on Windows
        try:
            # Use a short timeout to avoid blocking
            import select
            import sys

            if sys.platform == "win32":
                # On Windows, we can't use select, so just check if process is alive
                time.sleep(0.1)
                assert server_process.poll() is None
            else:
                # On Unix-like systems, we can use select
                ready, _, _ = select.select([server_process.stdout], [], [], 0.1)
                # If there's output available, try to read it
                if ready:
                    try:
                        server_process.stdout.read(1)  # Read just one character
                        # If we get here, Unicode handling is working
                        assert True
                    except UnicodeDecodeError:
                        pytest.fail("Unicode decode error occurred")

        except Exception:
            # If any other error occurs, that's fine - we're just testing Unicode handling
            pass

        # Main assertion: process should still be running
        assert server_process.poll() is None

    def test_server_responds_to_initialize_request(self, server_process):
        """Test that the server responds to MCP initialize request."""
        # MCP initialize request
        initialize_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0.0"},
            },
        }

        # Send the request
        request_json = json.dumps(initialize_request) + "\n"
        server_process.stdin.write(request_json)
        server_process.stdin.flush()

        # Read the response
        response_line = server_process.stdout.readline()
        assert response_line.strip(), "Server should respond to initialize request"

        # Parse the response
        try:
            response = json.loads(response_line)
            assert response.get("jsonrpc") == "2.0"
            assert response.get("id") == 1
            assert "result" in response
        except json.JSONDecodeError:
            pytest.fail(f"Invalid JSON response: {response_line}")

    def test_server_lists_tools(self, server_process):
        """Test that the server can list available tools."""
        # First initialize
        initialize_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0.0"},
            },
        }

        server_process.stdin.write(json.dumps(initialize_request) + "\n")
        server_process.stdin.flush()
        server_process.stdout.readline()  # Read initialize response

        # Send initialized notification
        initialized_notification = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }
        server_process.stdin.write(json.dumps(initialized_notification) + "\n")
        server_process.stdin.flush()

        # Request tools list
        tools_request = {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}

        server_process.stdin.write(json.dumps(tools_request) + "\n")
        server_process.stdin.flush()

        # Read the response
        response_line = server_process.stdout.readline()
        response = json.loads(response_line)

        assert response.get("jsonrpc") == "2.0"
        assert response.get("id") == 2
        assert "result" in response
        assert "tools" in response["result"]

        # Verify we have some Redis tools
        tools = response["result"]["tools"]
        tool_names = [tool["name"] for tool in tools]

        # Should have basic Redis operations
        expected_tools = [
            "clusters_list",
            "hset",
            "hget",
            "hdel",
            "hgetall",
            "hexists",
            "hybrid_search",
            "set_vector_in_hash",
            "get_vector_from_hash",
            "json_set",
            "json_get",
            "json_del",
            "lpush",
            "rpush",
            "lpop",
            "rpop",
            "lrange",
            "llen",
            "delete",
            "type",
            "expire",
            "rename",
            "scan_keys",
            "scan_all_keys",
            "search_redis_documents",
            "publish",
            "psubscribe",
            "read_messages",
            "subscribe",
            "unsubscribe",
            "get_indexes",
            "get_index_info",
            "get_indexed_keys_number",
            "create_vector_index_hash",
            "vector_search_hash",
            "dbsize",
            "info",
            "client_list",
            "sadd",
            "srem",
            "smembers",
            "zadd",
            "zrange",
            "zrem",
            "xadd",
            "xgroup_create",
            "xgroup_destroy",
            "xreadgroup",
            "xack",
            "xrange",
            "xdel",
            "set",
            "get",
            "lrem",
        ]
        for tool in tool_names:
            assert tool in expected_tools, (
                f"Expected tool '{tool}' not found in {tool_names}"
            )

    def test_server_tool_count_and_names(self, server_process):
        """Test that the server registers the correct number of tools with expected names."""
        # Initialize the server
        self._initialize_server(server_process)

        # Request tools list
        tools_request = {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}

        server_process.stdin.write(json.dumps(tools_request) + "\n")
        server_process.stdin.flush()

        # Read the response
        response_line = server_process.stdout.readline()
        response = json.loads(response_line)

        assert response.get("jsonrpc") == "2.0"
        assert response.get("id") == 3
        assert "result" in response
        assert "tools" in response["result"]

        tools = response["result"]["tools"]
        tool_names = [tool["name"] for tool in tools]

        # Expected tool count (the @redis_tool declarations in src/tools; legacy mode lists every toolset)
        expected_tool_count = 54
        assert len(tools) == expected_tool_count, (
            f"Expected {expected_tool_count} tools, but got {len(tools)}"
        )

        # Expected tool names (alphabetically sorted for easier verification)
        expected_tools = [
            "client_list",
            "clusters_list",
            "create_vector_index_hash",
            "dbsize",
            "delete",
            "expire",
            "get",
            "get_index_info",
            "get_indexed_keys_number",
            "get_indexes",
            "get_vector_from_hash",
            "hdel",
            "hexists",
            "hget",
            "hgetall",
            "hset",
            "hybrid_search",
            "info",
            "json_del",
            "json_get",
            "json_set",
            "llen",
            "lrem",
            "lpop",
            "lpush",
            "lrange",
            "publish",
            "psubscribe",
            "read_messages",
            "rename",
            "rpop",
            "rpush",
            "sadd",
            "scan_all_keys",
            "scan_keys",
            "search_redis_documents",
            "set",
            "set_vector_in_hash",
            "smembers",
            "srem",
            "subscribe",
            "type",
            "unsubscribe",
            "vector_search_hash",
            "xack",
            "xadd",
            "xdel",
            "xgroup_create",
            "xgroup_destroy",
            "xrange",
            "xreadgroup",
            "zadd",
            "zrange",
            "zrem",
        ]

        # Verify all expected tools are present
        missing_tools = set(expected_tools) - set(tool_names)
        extra_tools = set(tool_names) - set(expected_tools)

        assert not missing_tools, f"Missing expected tools: {sorted(missing_tools)}"
        assert not extra_tools, f"Found unexpected tools: {sorted(extra_tools)}"

        # Verify tool categories are represented
        tool_categories = {
            "clusters": ["clusters_list"],
            "string": ["get", "set"],
            "hash": ["hget", "hset", "hgetall", "hdel", "hexists"],
            "list": ["lpush", "rpush", "lpop", "rpop", "lrange", "llen"],
            "set": ["sadd", "srem", "smembers"],
            "sorted_set": ["zadd", "zrem", "zrange"],
            "stream": [
                "xadd",
                "xdel",
                "xgroup_create",
                "xgroup_destroy",
                "xrange",
                "xreadgroup",
                "xack",
            ],
            "json": ["json_get", "json_set", "json_del"],
            "pub_sub": [
                "publish",
                "psubscribe",
                "read_messages",
                "subscribe",
                "unsubscribe",
            ],
            "server_mgmt": ["dbsize", "info", "client_list"],
            "misc": [
                "delete",
                "expire",
                "rename",
                "type",
                "scan_keys",
                "scan_all_keys",
            ],
            "vector_search": [
                "create_vector_index_hash",
                "vector_search_hash",
                "hybrid_search",
                "get_indexes",
                "get_index_info",
                "set_vector_in_hash",
                "get_vector_from_hash",
                "get_indexed_keys_number",
            ],
        }

        for category, category_tools in tool_categories.items():
            for tool in category_tools:
                assert tool in tool_names, (
                    f"Tool '{tool}' from category '{category}' not found in registered tools"
                )

    def test_legacy_mode_has_a_single_default_cluster(self, server_process):
        """Without a config file the env/CLI settings form one cluster named 'default'. With a single cluster
        the cluster parameter is optional, so clients written before multi-cluster support keep working."""
        self._initialize_server(server_process)

        tools = {
            tool["name"]: tool
            for tool in self._request(server_process, 4, "tools/list")["tools"]
        }

        get_schema = tools["get"]["inputSchema"]
        cluster = get_schema["properties"]["cluster"]
        assert {"type": "string", "enum": [LEGACY_CLUSTER]} in cluster["anyOf"]
        assert {"type": "null"} in cluster["anyOf"]
        assert cluster["default"] is None
        assert (
            cluster["description"]
            == "Cluster name (default: the only configured cluster)"
        )
        assert get_schema["required"] == ["key"]

        for name, tool in tools.items():
            schema = tool["inputSchema"]
            if "cluster" not in schema["properties"]:
                continue
            # optional everywhere and appended after the tool's own parameters
            assert "cluster" not in schema.get("required", []), name
            assert list(schema["properties"])[-1] == "cluster", name

        set_schema = tools["set"]["inputSchema"]
        assert list(set_schema["properties"]) == [
            "key",
            "value",
            "expiration",
            "cluster",
        ]
        assert set_schema["required"] == ["key", "value"]

        clusters_list_schema = tools["clusters_list"]["inputSchema"]
        assert "cluster" not in clusters_list_schema.get("required", [])
        assert (
            clusters_list_schema["properties"]["cluster"]["description"]
            == "Cluster name (default: all clusters)"
        )

        docs_schema = tools["search_redis_documents"]["inputSchema"]
        assert "cluster" not in docs_schema["properties"]

    @pytest.fixture
    def redis_client(self):
        """The Redis the legacy server uses; the test is skipped when it does not answer."""
        if not _redis_available():
            env = _legacy_env()
            pytest.skip(
                f"no Redis at {env['REDIS_HOST']}:{env['REDIS_PORT']} (REDIS_HOST/REDIS_PORT)"
            )
        client = _redis_client()
        yield client
        client.close()

    def test_legacy_tools_work_without_cluster_argument(
        self, redis_client, server_process
    ):
        """Tool calls of single-cluster clients (no cluster argument) reach the one configured Redis."""
        key = f"mcp:it:legacy:{uuid.uuid4().hex}"
        self._initialize_server(server_process)

        try:
            set_result = self._call_tool(
                server_process,
                10,
                "set",
                {"key": key, "value": "legacy-value", "expiration": 300},
            )
            stored = redis_client.get(key)
            ttl = redis_client.ttl(key)
            get_result = self._call_tool(server_process, 11, "get", {"key": key})
            named = self._call_tool(
                server_process, 12, "get", {"key": key, "cluster": LEGACY_CLUSTER}
            )
            unknown = self._call_tool(
                server_process, 13, "get", {"key": key, "cluster": "other"}
            )
            clusters = self._call_tool(server_process, 14, "clusters_list", {})
        finally:
            redis_client.delete(key)

        assert not set_result.get("isError"), set_result
        assert (
            _text(set_result) == f"Successfully set {key} with expiration 300 seconds"
        )
        assert stored == "legacy-value"
        assert 0 < ttl <= 300

        assert not get_result.get("isError"), get_result
        assert _text(get_result) == "legacy-value"
        assert _text(named) == "legacy-value"
        assert unknown.get("isError") is True

        rows = clusters["structuredContent"]["result"]
        assert [row["name"] for row in rows] == [LEGACY_CLUSTER]
        assert rows[0]["status"] == "ok", rows[0]

    def test_legacy_pubsub_first_read_after_subscribe_gets_the_message(
        self, redis_client, server_process
    ):
        """The legacy mode lists the pubsub toolset; the first read_messages after subscribe returns the
        published message (it gets None for the skipped subscribe confirmation first and keeps reading)."""
        channel = f"mcp:it:legacy:channel:{uuid.uuid4().hex}"
        self._initialize_server(server_process)

        subscription = self._call_tool(
            server_process, 20, "subscribe", {"channel": channel}
        )
        subscription_id = subscription["structuredContent"]["result"]["subscription_id"]
        try:
            published = self._call_tool(
                server_process, 21, "publish", {"channel": channel, "message": "hi"}
            )
            first_read = self._call_tool(
                server_process,
                22,
                "read_messages",
                {
                    "subscription_id": subscription_id,
                    "timeout_ms": 5000,
                    "max_messages": 1,
                },
            )
        finally:
            self._call_tool(
                server_process, 23, "unsubscribe", {"subscription_id": subscription_id}
            )

        assert not published.get("isError"), published
        messages = first_read["structuredContent"]["result"]["messages"]
        assert [(m["channel"], m["data"]) for m in messages] == [(channel, "hi")]

    def _request(self, server_process, request_id, method, params=None):
        """Sends a request and returns the result of the response with its id."""
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        server_process.stdin.write(json.dumps(message) + "\n")
        server_process.stdin.flush()
        while True:
            line = server_process.stdout.readline()
            if not line:
                pytest.fail(f"The server exited while waiting for {method}")
            response = json.loads(line)
            if response.get("id") == request_id:
                assert "result" in response, response
                return response["result"]

    def _call_tool(self, server_process, request_id, name, arguments):
        return self._request(
            server_process,
            request_id,
            "tools/call",
            {"name": name, "arguments": arguments},
        )

    def _initialize_server(self, server_process):
        """Helper to initialize the MCP server."""
        # Send initialize request
        initialize_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0.0"},
            },
        }

        server_process.stdin.write(json.dumps(initialize_request) + "\n")
        server_process.stdin.flush()
        server_process.stdout.readline()  # Read response

        # Send initialized notification
        initialized_notification = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }
        server_process.stdin.write(json.dumps(initialized_notification) + "\n")
        server_process.stdin.flush()


def _text(result):
    return "".join(c["text"] for c in result["content"] if c["type"] == "text")
