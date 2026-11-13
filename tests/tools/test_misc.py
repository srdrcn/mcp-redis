import random
from unittest.mock import Mock, patch

import pytest
from redis import Redis
from redis.cluster import RedisCluster
from redis.exceptions import MovedError, RedisError

import src.tools.misc as misc


@pytest.fixture
def mock_redis_cluster():
    """get_connection returns a RedisCluster mock (a cluster in mode: cluster)."""
    with patch(
        "src.common.connection.RedisConnectionManager.get_connection"
    ) as mock_get_conn:
        mock_cluster = Mock(spec=RedisCluster)
        mock_get_conn.return_value = mock_cluster
        yield mock_cluster


@pytest.mark.asyncio
async def test_scan_all_keys_follows_the_cursor(mock_redis_connection_manager):
    """A standalone/sentinel client: SCAN until the cursor is back at 0."""
    mock_redis = mock_redis_connection_manager
    mock_redis.scan.side_effect = [(17, [b"user:1", "user:2"]), (0, [b"user:3"])]

    result = await misc.scan_all_keys("user:*", batch_size=50)

    assert result == ["user:1", "user:2", "user:3"]
    assert [c.kwargs for c in mock_redis.scan.call_args_list] == [
        {"cursor": 0, "match": "user:*", "count": 50},
        {"cursor": 17, "match": "user:*", "count": 50},
    ]
    mock_redis.scan_iter.assert_not_called()


@pytest.mark.asyncio
async def test_scan_all_keys_on_a_redis_cluster_uses_scan_iter(mock_redis_cluster):
    """RedisCluster.scan returns a cursor per node, which cannot be passed back as the cursor; scan_iter
    walks every primary."""
    mock_redis_cluster.scan_iter.return_value = iter([b"user:1", "user:2", b"user:3"])

    result = await misc.scan_all_keys("user:*", batch_size=50)

    assert result == ["user:1", "user:2", "user:3"]
    mock_redis_cluster.scan_iter.assert_called_once_with(match="user:*", count=50)
    mock_redis_cluster.scan.assert_not_called()


@pytest.mark.asyncio
async def test_scan_all_keys_on_a_redis_cluster_defaults(mock_redis_cluster):
    mock_redis_cluster.scan_iter.return_value = iter([])

    assert await misc.scan_all_keys() == []
    mock_redis_cluster.scan_iter.assert_called_once_with(match="*", count=100)


@pytest.mark.asyncio
async def test_scan_all_keys_on_a_redis_cluster_error(mock_redis_cluster):
    def failing_scan(**kwargs):
        yield b"user:1"
        raise RedisError("CLUSTERDOWN")

    mock_redis_cluster.scan_iter.side_effect = failing_scan

    result = await misc.scan_all_keys("user:*")

    assert result == "Error scanning all keys with pattern 'user:*': CLUSTERDOWN"


@pytest.mark.asyncio
async def test_scan_all_keys_error(mock_redis_connection_manager):
    mock_redis_connection_manager.scan.side_effect = RedisError("Connection failed")

    result = await misc.scan_all_keys("user:*")

    assert result == "Error scanning all keys with pattern 'user:*': Connection failed"


def test_redis_cluster_is_not_a_redis():
    """The cluster check of scan_all_keys/dbsize: a standalone client mock must not pass for a cluster."""
    assert not isinstance(Mock(spec=Redis), RedisCluster)
    assert isinstance(Mock(spec=RedisCluster), RedisCluster)


@pytest.mark.asyncio
async def test_search_redis_documents_url_not_configured(monkeypatch):
    """Return a clear error if MCP_DOCS_SEARCH_URL is not set for search_redis_documents."""
    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "")

    result = await misc.search_redis_documents("What is Redis?")

    assert isinstance(result, dict)
    assert (
        result["error"] == "MCP_DOCS_SEARCH_URL environment variable is not configured"
    )


@pytest.mark.asyncio
async def test_search_redis_documents_empty_question(monkeypatch):
    """Reject empty/whitespace-only questions for search_redis_documents."""
    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")

    result = await misc.search_redis_documents("   ")

    assert isinstance(result, dict)
    assert result["error"] == "Question parameter cannot be empty"


@pytest.mark.asyncio
async def test_search_redis_documents_success_json_response(monkeypatch):
    """Return parsed JSON when the docs API responds with JSON for search_redis_documents."""

    class DummyResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"results": [{"title": "Redis Intro"}]}

    class DummySession:
        def __init__(self, *_, **__):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *_, **__):  # pragma: no cover - trivial wrapper
            return DummyResponse()

    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")
    monkeypatch.setattr(misc.aiohttp, "ClientSession", DummySession)

    result = await misc.search_redis_documents("What is a Redis stream?")

    assert isinstance(result, dict)
    assert result["results"][0]["title"] == "Redis Intro"


@pytest.mark.asyncio
async def test_search_redis_documents_non_json_response(monkeypatch):
    """If the response is not JSON, surface the text content in an error from search_redis_documents."""

    class DummyContentTypeError(Exception):
        pass

    class DummyResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            raise DummyContentTypeError("Not JSON")

        async def text(self):
            return "<html>not json</html>"

    class DummySession:
        def __init__(self, *_, **__):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *_, **__):  # pragma: no cover - trivial wrapper
            return DummyResponse()

    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")
    # Patch aiohttp.ContentTypeError to our dummy so the except block matches
    monkeypatch.setattr(misc.aiohttp, "ContentTypeError", DummyContentTypeError)
    monkeypatch.setattr(misc.aiohttp, "ClientSession", DummySession)

    result = await misc.search_redis_documents("Explain Redis JSON")

    assert isinstance(result, dict)
    assert "Non-JSON response" in result["error"]
    assert "not json" in result["error"]


@pytest.mark.asyncio
async def test_search_redis_documents_http_client_error(monkeypatch):
    """HTTP client errors from search_redis_documents are caught and returned in an error dict."""

    class DummyClientError(Exception):
        pass

    class ErrorResponse:
        async def __aenter__(self):
            raise DummyClientError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        def __init__(self, *_, **__):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *_, **__):  # pragma: no cover - trivial wrapper
            return ErrorResponse()

    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")
    monkeypatch.setattr(misc.aiohttp, "ClientError", DummyClientError)
    monkeypatch.setattr(misc.aiohttp, "ClientSession", DummySession)

    result = await misc.search_redis_documents("What is Redis?")

    assert isinstance(result, dict)
    assert result["error"] == "HTTP client error: boom"


@pytest.mark.asyncio
async def test_search_redis_documents_http_error_status_with_json(monkeypatch):
    """HTTP >=400 with JSON payload should return structured error details."""

    class DummyResponse:
        status = 503

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"error": "upstream unavailable"}

    class DummySession:
        def __init__(self, *_, **__):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *_, **__):
            return DummyResponse()

    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")
    monkeypatch.setattr(misc.aiohttp, "ClientSession", DummySession)

    result = await misc.search_redis_documents("What is Redis?")

    assert isinstance(result, dict)
    assert result["error"] == "Docs search request failed with status 503"
    assert result["details"] == {"error": "upstream unavailable"}


@pytest.mark.asyncio
async def test_search_redis_documents_timeout(monkeypatch):
    """Timeouts should return a clear bounded-latency error."""

    class TimeoutResponse:
        async def __aenter__(self):
            raise TimeoutError("timed out")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        def __init__(self, *_, **__):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *_, **__):
            return TimeoutResponse()

    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")
    monkeypatch.setattr(misc.aiohttp, "ClientSession", DummySession)

    result = await misc.search_redis_documents("What is Redis?")

    assert isinstance(result, dict)
    assert result["error"] == (
        f"Docs search request timed out after {misc.DOCS_SEARCH_TIMEOUT_SECONDS} seconds"
    )


def _cluster_node(name, role="primary"):
    host, _, port = name.rpartition(":")
    node = Mock()
    node.name, node.host, node.port, node.server_type = name, host, int(port), role
    return node


P1, P2, R1 = "10.0.0.1:6500", "10.0.0.2:6500", "10.0.0.3:6501"
NODE_LIST = (
    "Its nodes, as host:port: 10.0.0.1:6500 (primary; replicas: 10.0.0.3:6501), 10.0.0.2:6500 (primary)."
    " Ask the user which node to scan."
)


def _nodes(cluster):
    """Makes the Redis Cluster mock one of two primaries, P1 and P2, and R1, a replica of P1. Returns the
    client mock of each node by its address; ``get_connection`` hands out the cluster mock itself."""
    nodes = {
        P1: _cluster_node(P1),
        P2: _cluster_node(P2),
        R1: _cluster_node(R1, "replica"),
    }
    clients = {name: Mock(spec=Redis) for name in nodes}
    first, second = [nodes[P1], nodes[R1]], [nodes[P2]]
    cluster.get_nodes.return_value = list(nodes.values())
    cluster.get_primaries.return_value = [nodes[P2], nodes[P1]]
    # one list of nodes for all the slots of a range, as redis-py keeps them
    cluster.nodes_manager = Mock(slots_cache={0: first, 1: first, 2: second})
    cluster.get_redis_connection.side_effect = lambda node: clients[node.name]
    return clients


@pytest.mark.asyncio
@pytest.mark.parametrize("node", [None, "", "  "])
async def test_scan_keys_on_a_redis_cluster_needs_a_node(mock_redis_cluster, node):
    """A scan of a Redis Cluster runs on the one node the caller names, never on all of them."""
    clients = _nodes(mock_redis_cluster)

    result = await misc.scan_keys("user:*", node=node)

    assert result == (
        "Error: cluster default is a Redis Cluster: node is required, the one node to scan. "
        + NODE_LIST
    )
    for client in clients.values():
        client.scan.assert_not_called()
    mock_redis_cluster.scan.assert_not_called()
    mock_redis_cluster.scan_iter.assert_not_called()
    # the list is read again from the cluster, not taken from what the client knew
    mock_redis_cluster.nodes_manager.initialize.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("node", [P1, R1, f" {P2} ", "10.0.0.2:6500"])
async def test_scan_keys_on_a_redis_cluster_scans_the_named_node(
    mock_redis_cluster, node
):
    """A primary or a replica: the SCAN goes to that node's own client, and to no other."""
    clients = _nodes(mock_redis_cluster)
    name = node.strip()
    clients[name].scan.return_value = (12, [b"user:1", "user:2"])

    result = await misc.scan_keys("user:*", count=50, node=node)

    assert result == {
        "cursor": 12,
        "keys": ["user:1", "user:2"],
        "total_scanned": 2,
        "scan_complete": False,
        "node": name,
    }
    clients[name].scan.assert_called_once_with(cursor=0, match="user:*", count=50)
    for other, client in clients.items():
        if other != name:
            client.scan.assert_not_called()
    mock_redis_cluster.scan.assert_not_called()
    mock_redis_cluster.nodes_manager.initialize.assert_not_called()


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_pages_with_the_cursor(mock_redis_cluster):
    """The cursor is the node's own SCAN cursor: a number, as on any other server."""
    clients = _nodes(mock_redis_cluster)
    clients[P1].scan.side_effect = [(12, ["a:1"]), (0, ["a:2"])]

    first = await misc.scan_keys("a:*", count=10, node=P1)
    last = await misc.scan_keys("a:*", count=10, node=P1, cursor=str(first["cursor"]))

    assert (first["cursor"], first["scan_complete"]) == (12, False)
    assert last == {
        "cursor": 0,
        "keys": ["a:2"],
        "total_scanned": 1,
        "scan_complete": True,
        "node": P1,
    }
    assert [c.kwargs["cursor"] for c in clients[P1].scan.call_args_list] == [0, 12]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "node",
    ["10.0.0.9:6500", "10.0.0.1:6501", "10.0.0.1", ":6500", "10.0.0.1:abc", "n1"],
)
async def test_scan_keys_on_a_redis_cluster_rejects_a_node_it_does_not_have(
    mock_redis_cluster, monkeypatch, node
):
    clients = _nodes(mock_redis_cluster)
    monkeypatch.setattr(
        misc.socket, "getaddrinfo", Mock(side_effect=OSError("unknown"))
    )

    result = await misc.scan_keys("*", node=node)

    assert result == (
        f"Error: node {node} is not a node of the Redis Cluster default. " + NODE_LIST
    )
    for client in clients.values():
        client.scan.assert_not_called()
    mock_redis_cluster.nodes_manager.initialize.assert_called_once_with()


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_finds_a_node_the_client_did_not_know(
    mock_redis_cluster,
):
    """A node that joined, or a replica that had replicated nothing, since the client read the nodes."""
    clients = _nodes(mock_redis_cluster)
    known = mock_redis_cluster.get_nodes.return_value
    mock_redis_cluster.get_nodes.side_effect = [
        [n for n in known if n.name != R1],
        known,
    ]
    clients[R1].scan.return_value = (0, [])

    result = await misc.scan_keys("*", node=R1)

    assert (result["node"], result["scan_complete"]) == (R1, True)
    mock_redis_cluster.nodes_manager.initialize.assert_called_once_with()


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_accepts_a_host_name(
    mock_redis_cluster, monkeypatch
):
    """The cluster names its nodes by IP address; a host name that resolves to one of them names that node."""
    clients = _nodes(mock_redis_cluster)
    resolve = Mock(return_value=[(2, 1, 6, "", ("10.0.0.3", 0))])
    monkeypatch.setattr(misc.socket, "getaddrinfo", resolve)
    clients[R1].scan.return_value = (0, ["k"])

    result = await misc.scan_keys("*", node="CBPRDRDS03.example.com:6501")

    assert result["node"] == R1
    assert resolve.call_args.args[0] == "cbprdrds03.example.com"
    clients[R1].scan.assert_called_once()


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_rejects_a_host_name_of_two_nodes(
    mock_redis_cluster, monkeypatch
):
    """P1 and P2 share the port: a name that resolves to both addresses names neither."""
    clients = _nodes(mock_redis_cluster)
    monkeypatch.setattr(
        misc.socket,
        "getaddrinfo",
        Mock(
            return_value=[
                (2, 1, 6, "", ("10.0.0.1", 0)),
                (2, 1, 6, "", ("10.0.0.2", 0)),
            ]
        ),
    )

    result = await misc.scan_keys("*", node="redis.example.com:6500")

    assert result.startswith(
        "Error: node redis.example.com:6500 is not a node of the Redis Cluster default."
    )
    for client in clients.values():
        client.scan.assert_not_called()


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_error(mock_redis_cluster):
    clients = _nodes(mock_redis_cluster)
    clients[P1].scan.side_effect = RedisError("node down")

    result = await misc.scan_keys("user:*", node=P1)

    assert result == "Error scanning keys with pattern 'user:*': node down"


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_rejects_an_invalid_cursor(
    mock_redis_cluster,
):
    clients = _nodes(mock_redis_cluster)

    result = await misc.scan_keys("*", node=P1, cursor="10.0.0.1:6500=12")

    assert result == (
        "Error: invalid cursor: pass the cursor returned by the previous scan_keys call unchanged, or 0 to"
        " start"
    )
    clients[P1].scan.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["scan_keys", "big_keys"])
async def test_a_node_is_rejected_on_a_cluster_that_is_not_a_redis_cluster(
    mock_redis_connection_manager, tool
):
    """A standalone or Sentinel cluster has one server; naming another must not scan this one instead."""
    result = await getattr(misc, tool)("app:*", node="10.0.0.1:6500")

    assert result == (
        "Error: cluster default is not a Redis Cluster (mode standalone): it has one server, so leave node"
        " out"
    )
    mock_redis_connection_manager.scan.assert_not_called()


@pytest.mark.asyncio
async def test_scan_keys_accepts_a_cursor_given_as_text(mock_redis_connection_manager):
    mock_redis_connection_manager.scan.return_value = (0, [])

    result = await misc.scan_keys("user:*", cursor="1234")

    assert result["scan_complete"] is True
    mock_redis_connection_manager.scan.assert_called_once_with(
        cursor=1234, match="user:*", count=100
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cursor", ["n1:7000=12", "abc", "-1", "", "1" * 21, "1" * 5000]
)
async def test_scan_keys_rejects_a_cursor_that_is_not_a_number(
    mock_redis_connection_manager, cursor
):
    result = await misc.scan_keys("*", cursor=cursor)

    assert result.startswith("Error: invalid cursor")
    mock_redis_connection_manager.scan.assert_not_called()


def test_scan_descriptions_name_no_other_tool():
    """Either scan tool may be excluded, so neither description points to the other."""
    assert "scan_all_keys" not in misc.scan_keys.__doc__
    assert "scan_keys" not in misc.scan_all_keys.__doc__


class _Pipeline:
    """A pipeline of a fake server that holds ``keys``: name -> (type, bytes, length, encoding). ``bytes`` is
    a number, or {samples: number} for a key whose size depends on the samples; ``bytes`` and ``length`` may
    be an exception the command fails with; the encoding is "hashtable" when left out. READONLY is answered
    with True, or with ``keys["READONLY"]``. Records each queued command in ``commands`` and each executed
    pipeline's number of commands in ``batches``."""

    def __init__(self, keys, commands, batches):
        self._keys = keys
        self._commands = commands
        self._batches = batches
        self._replies = []

    def _queue(self, command, key, reply, **options):
        self._commands.append((command, key, options) if options else (command, key))
        self._replies.append(reply)

    def type(self, key):
        self._queue("type", key, self._keys[key][0] if key in self._keys else "none")

    def memory_usage(self, key, samples=None):
        size = self._keys[key][1] if key in self._keys else None
        if isinstance(size, dict):
            size = size[samples]
        self._queue("memory_usage", key, size, samples=samples)

    def object(self, infotype, key):
        encoding = None
        if key in self._keys:
            encoding = (*self._keys[key], "hashtable")[3]
        self._queue(f"object {infotype}", key, encoding)

    def ttl(self, key):
        self._queue("ttl", key, -1 if key in self._keys else -2)

    def execute_command(self, command):
        assert command == "READONLY"
        self._commands.append((command,))
        self._replies.append(self._keys.get(command, True))

    def __getattr__(self, command):
        if command not in {"strlen", "hlen", "llen", "scard", "zcard", "xlen"}:
            raise AttributeError(command)
        return lambda key: self._queue(
            command, key, self._keys[key][2] if key in self._keys else 0
        )

    def execute(self, raise_on_error=True):
        self._batches.append(len(self._replies))
        for reply in self._replies:
            if raise_on_error and isinstance(reply, Exception):
                raise reply
        return self._replies


def _holding(client, keys):
    """Makes the client mock hold ``keys`` (see _Pipeline); returns the list its pipelines' commands go to,
    with the sizes of the executed pipelines in its ``batches``."""

    class Commands(list):
        batches = []

    commands = Commands()
    commands.batches = []

    def pipeline(transaction=True):
        assert transaction is False
        return _Pipeline(keys, commands, commands.batches)

    client.pipeline.side_effect = pipeline
    return commands


def _clock(monkeypatch, *times):
    """big_keys reads these times: one when it starts and one after each round of SCAN steps."""
    ticks = iter(times)
    monkeypatch.setattr(misc, "time", Mock(monotonic=lambda: next(ticks)))


@pytest.mark.asyncio
async def test_memory_usage_of_a_hash(mock_redis_connection_manager):
    commands = _holding(
        mock_redis_connection_manager, {"h": ("hash", 4096, 3, "listpack")}
    )

    result = await misc.memory_usage("h")

    assert result == {
        "key": "h",
        "type": "hash",
        "bytes": 4096,
        "length": 3,
        "lengthUnit": "fields",
        "encoding": "listpack",
        "ttl": -1,
    }
    # the value is never read
    assert commands == [
        ("type", "h"),
        ("ttl", "h"),
        ("memory_usage", "h", {"samples": 1000}),
        ("object ENCODING", "h"),
        ("hlen", "h"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key_type, command, unit",
    [
        ("string", "strlen", "bytes"),
        ("hash", "hlen", "fields"),
        ("list", "llen", "items"),
        ("set", "scard", "members"),
        ("zset", "zcard", "members"),
        ("stream", "xlen", "entries"),
    ],
)
async def test_memory_usage_counts_each_type_with_its_own_command(
    mock_redis_connection_manager, key_type, command, unit
):
    commands = _holding(mock_redis_connection_manager, {"k": (key_type, 100, 7)})

    result = await misc.memory_usage("k")

    assert (result["type"], result["length"], result["lengthUnit"]) == (
        key_type,
        7,
        unit,
    )
    assert commands[-1] == (command, "k")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key_type, length, encoding, samples, estimated",
    [
        # more elements than samples: their size is scaled to the length
        ("hash", 1001, "hashtable", 1000, True),
        ("zset", 5000, "skiplist", 1000, True),
        ("list", 5000, "quicklist", 1000, True),
        ("set", 50, "hashtable", 49, True),
        ("stream", 2000, "stream", 1000, True),
        # every element is measured
        ("hash", 1000, "hashtable", 1000, False),
        ("set", 50, "hashtable", 50, False),
        ("hash", 5000, "hashtable", 5000, False),
        # one block of memory, measured whole whatever the samples
        ("hash", 400, "listpack", 5, False),
        ("set", 400, "intset", 5, False),
        ("zset", 100, "ziplist", 5, False),
        # a string is not sampled
        ("string", 5_000_000, "raw", 5, False),
    ],
)
async def test_memory_usage_marks_an_estimate(
    mock_redis_connection_manager, key_type, length, encoding, samples, estimated
):
    _holding(mock_redis_connection_manager, {"k": (key_type, 900, length, encoding)})

    result = await misc.memory_usage("k", samples=samples)

    assert result.get("estimated", False) is estimated
    assert list(result)[:4] == (
        ["key", "type", "bytes", "estimated"]
        if estimated
        else ["key", "type", "bytes", "length"]
    )


@pytest.mark.asyncio
async def test_memory_usage_of_a_module_type_has_no_length(
    mock_redis_connection_manager,
):
    commands = _holding(
        mock_redis_connection_manager, {"doc": ("ReJSON-RL", 900, None, "raw")}
    )

    result = await misc.memory_usage("doc")

    assert result == {
        "key": "doc",
        "type": "ReJSON-RL",
        "bytes": 900,
        "encoding": "raw",
        "ttl": -1,
    }
    assert len(commands) == 4


@pytest.mark.asyncio
async def test_memory_usage_of_a_missing_key(mock_redis_connection_manager):
    commands = _holding(mock_redis_connection_manager, {})

    assert await misc.memory_usage("nope") == {"key": "nope", "type": "none"}
    assert len(commands) == 4


@pytest.mark.asyncio
async def test_memory_usage_passes_the_samples(mock_redis_connection_manager):
    commands = _holding(mock_redis_connection_manager, {"l": ("list", 100, 9)})

    await misc.memory_usage("l", samples=misc.MAX_SAMPLES)

    assert ("memory_usage", "l", {"samples": 10000}) in commands


@pytest.mark.asyncio
@pytest.mark.parametrize("samples", [0, -1, misc.MAX_SAMPLES + 1])
async def test_memory_usage_rejects_samples_out_of_range(
    mock_redis_connection_manager, samples
):
    """0 would make MEMORY USAGE walk every element of the key."""
    result = await misc.memory_usage("k", samples=samples)

    assert result == {"error": "samples must be between 1 and 10000"}
    mock_redis_connection_manager.pipeline.assert_not_called()


@pytest.mark.asyncio
async def test_memory_usage_on_a_key_that_changed_type_has_no_length(
    mock_redis_connection_manager,
):
    wrong_type = RedisError(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    )
    _holding(mock_redis_connection_manager, {"k": ("hash", 100, wrong_type)})

    result = await misc.memory_usage("k")

    assert result["bytes"] == 100
    assert not {"length", "lengthUnit", "estimated"} & set(result)


@pytest.mark.asyncio
async def test_memory_usage_reports_a_length_command_that_fails(
    mock_redis_connection_manager,
):
    denied = RedisError("NOPERM this user has no permissions to run the 'hlen' command")
    _holding(mock_redis_connection_manager, {"k": ("hash", 100, denied)})

    assert await misc.memory_usage("k") == {"error": str(denied)}


@pytest.mark.asyncio
async def test_memory_usage_reports_a_memory_usage_that_fails(
    mock_redis_connection_manager,
):
    denied = RedisError(
        "NOPERM this user has no permissions to run the 'memory|usage' command"
    )
    _holding(mock_redis_connection_manager, {"k": ("hash", denied, 3)})

    assert await misc.memory_usage("k") == {"error": str(denied)}


@pytest.mark.asyncio
async def test_memory_usage_error(mock_redis_connection_manager):
    mock_redis_connection_manager.pipeline.side_effect = RedisError("Connection failed")

    assert await misc.memory_usage("k") == {"error": "Connection failed"}


@pytest.mark.asyncio
async def test_memory_usage_on_a_redis_cluster(mock_redis_cluster):
    """The cluster client's pipeline sends each command to the node of the key."""
    _holding(mock_redis_cluster, {"s": ("set", 500, 4)})

    result = await misc.memory_usage("s")

    assert (result["bytes"], result["length"], result["lengthUnit"]) == (
        500,
        4,
        "members",
    )


KEYS = {
    b"cache:1": (b"string", 300, 250, b"raw"),
    b"session:1": (b"hash", 5000, 3, b"listpack"),
    b"queue": (b"list", 90000, 1200, b"quicklist"),
    b"session:2": (b"hash", 7000, 4, b"listpack"),
}
QUEUE = {
    "key": "queue",
    "type": "list",
    "bytes": 90000,
    "estimated": True,
    "length": 1200,
    "lengthUnit": "items",
    "encoding": "quicklist",
}
SESSION_1 = {
    "key": "session:1",
    "type": "hash",
    "bytes": 5000,
    "length": 3,
    "lengthUnit": "fields",
    "encoding": "listpack",
}
SESSION_2 = {**SESSION_1, "key": "session:2", "bytes": 7000, "length": 4}
CACHE_1 = {
    "key": "cache:1",
    "type": "string",
    "bytes": 300,
    "length": 250,
    "lengthUnit": "bytes",
    "encoding": "raw",
}


@pytest.mark.asyncio
async def test_big_keys_scans_until_the_scan_is_complete(mock_redis_connection_manager):
    mock_redis = mock_redis_connection_manager
    commands = _holding(mock_redis, KEYS)
    mock_redis.scan.side_effect = [
        (17, [b"cache:1", b"session:1"]),
        (0, [b"queue", b"session:2"]),
    ]
    mock_redis.dbsize.return_value = 40

    result = await misc.big_keys("app:*")

    assert result == {
        "cursor": 0,
        "scanComplete": True,
        "keysScanned": 4,
        "dbsize": 40,
        "keys": [QUEUE, SESSION_2, SESSION_1, CACHE_1],
        "byType": {
            "list": {"keys": 1, "bytes": 90000},
            "hash": {"keys": 2, "bytes": 12000},
            "string": {"keys": 1, "bytes": 300},
        },
    }
    assert list(result["byType"]) == ["list", "hash", "string"]
    assert [c.kwargs for c in mock_redis.scan.call_args_list] == [
        {"cursor": 0, "match": "app:*", "count": misc.BIG_KEYS_SCAN_COUNT},
        {"cursor": 17, "match": "app:*", "count": misc.BIG_KEYS_SCAN_COUNT},
    ]
    # every key is measured with the 5 samples of the scan
    assert commands[:8] == [
        ("type", b"cache:1"),
        ("memory_usage", b"cache:1", {"samples": 5}),
        ("type", b"session:1"),
        ("memory_usage", b"session:1", {"samples": 5}),
        ("type", b"queue"),
        ("memory_usage", b"queue", {"samples": 5}),
        ("type", b"session:2"),
        ("memory_usage", b"session:2", {"samples": 5}),
    ]
    # the listed keys again, with more; no value is read
    assert commands[8:11] == [
        ("memory_usage", b"queue", {"samples": 1000}),
        ("object ENCODING", b"queue"),
        ("llen", b"queue"),
    ]
    assert {c[0] for c in commands} == {
        "type",
        "memory_usage",
        "object ENCODING",
        "strlen",
        "hlen",
        "llen",
    }


@pytest.mark.asyncio
async def test_big_keys_measures_the_listed_keys_again(mock_redis_connection_manager):
    """The 5 samples of the scan rank the keys; the listed ones come back with the size 1000 samples give."""
    mock_redis = mock_redis_connection_manager
    keys = {
        # the scan caught one of its few large fields: far too large
        b"lucky": (b"hash", {5: 20_000_000, 1000: 1_200_000}, 1000, b"hashtable"),
        # and none of this one's
        b"unlucky": (b"hash", {5: 400_000, 1000: 3_000_000}, 800, b"hashtable"),
        b"steady": (b"hash", {5: 2_000_000, 1000: 2_100_000}, 4000, b"hashtable"),
        b"small": (b"string", 100, 50, b"raw"),
    }
    commands = _holding(mock_redis, keys)
    mock_redis.scan.return_value = (0, list(keys))
    mock_redis.dbsize.return_value = 4

    result = await misc.big_keys("app:*", top=3)

    assert [(k["key"], k["bytes"]) for k in result["keys"]] == [
        ("unlucky", 3_000_000),
        ("steady", 2_100_000),
        ("lucky", 1_200_000),
    ]
    # only the key with more elements than samples is still an estimate
    assert [k.get("estimated", False) for k in result["keys"]] == [False, True, False]
    # the totals are those of the scan
    assert result["byType"] == {
        "hash": {"keys": 3, "bytes": 22_400_000},
        "string": {"keys": 1, "bytes": 100},
    }
    assert ("memory_usage", b"small", {"samples": 1000}) not in commands


@pytest.mark.asyncio
async def test_big_keys_measures_the_listed_keys_ten_at_a_time(
    mock_redis_connection_manager,
):
    """A pipeline runs without the server serving other clients, so the larger measurements are split."""
    mock_redis = mock_redis_connection_manager
    keys = {b"k%d" % i: (b"hash", 1000 + i, 2000, b"hashtable") for i in range(25)}
    commands = _holding(mock_redis, keys)
    mock_redis.scan.return_value = (0, list(keys))
    mock_redis.dbsize.return_value = 25

    result = await misc.big_keys("app:*", top=25)

    assert len(result["keys"]) == 25
    # the scan step (TYPE and MEMORY USAGE of 25 keys), then 10, 10 and 5 keys with three commands each
    assert commands.batches == [50, 30, 30, 15]


@pytest.mark.asyncio
async def test_big_keys_returns_the_cursor_when_its_time_is_up(
    mock_redis_connection_manager, monkeypatch
):
    mock_redis = mock_redis_connection_manager
    _holding(mock_redis, KEYS)
    mock_redis.scan.side_effect = [
        (17, [b"cache:1"]),
        (33, [b"session:1"]),
        (0, [b"queue"]),
    ]
    mock_redis.dbsize.return_value = 40
    # started at 100; the first step ends in time, the second after the 5 s
    _clock(monkeypatch, 100.0, 104.9, 105.0)

    first = await misc.big_keys("app:*")

    assert first["cursor"] == 33
    assert first["scanComplete"] is False
    assert first["keysScanned"] == 2
    assert [k["key"] for k in first["keys"]] == ["session:1", "cache:1"]
    assert mock_redis.scan.call_count == 2

    _clock(monkeypatch, 200.0, 200.1)
    last = await misc.big_keys("app:*", cursor=first["cursor"])

    assert (last["cursor"], last["scanComplete"], last["keysScanned"]) == (0, True, 1)
    assert mock_redis.scan.call_args.kwargs["cursor"] == 33


@pytest.mark.asyncio
async def test_big_keys_top_and_min_bytes(mock_redis_connection_manager):
    mock_redis = mock_redis_connection_manager
    _holding(mock_redis, KEYS)
    mock_redis.scan.return_value = (0, list(KEYS))
    mock_redis.dbsize.return_value = 4

    top = await misc.big_keys("app:*", top=2)
    large = await misc.big_keys("app:*", min_bytes=7000)
    none = await misc.big_keys("app:*", min_bytes=90001)

    assert [k["key"] for k in top["keys"]] == ["queue", "session:2"]
    assert [k["key"] for k in large["keys"]] == ["queue", "session:2"]
    assert none["keys"] == []
    # the totals cover every key that was measured
    assert top["keysScanned"] == large["keysScanned"] == none["keysScanned"] == 4
    assert top["byType"] == large["byType"] == none["byType"]
    assert none["byType"]["hash"] == {"keys": 2, "bytes": 12000}


@pytest.mark.asyncio
async def test_big_keys_min_bytes_applies_to_the_size_it_lists(
    mock_redis_connection_manager,
):
    mock_redis = mock_redis_connection_manager
    keys = {
        b"shrinks": (b"hash", {5: 9000, 1000: 4000}, 900, b"hashtable"),
        b"stays": (b"hash", {5: 9000, 1000: 8000}, 900, b"hashtable"),
    }
    _holding(mock_redis, keys)
    mock_redis.scan.return_value = (0, list(keys))
    mock_redis.dbsize.return_value = 2

    result = await misc.big_keys("app:*", min_bytes=5000)

    assert [(k["key"], k["bytes"]) for k in result["keys"]] == [("stays", 8000)]


@pytest.mark.asyncio
async def test_big_keys_passes_the_pattern(mock_redis_connection_manager):
    mock_redis = mock_redis_connection_manager
    _holding(mock_redis, KEYS)
    mock_redis.scan.return_value = (0, [b"session:1"])
    mock_redis.dbsize.return_value = 4

    result = await misc.big_keys("session:*")

    assert result["keysScanned"] == 1
    assert result["dbsize"] == 4
    assert mock_redis.scan.call_args.kwargs["match"] == "session:*"


@pytest.mark.asyncio
async def test_big_keys_skips_a_key_that_is_gone(mock_redis_connection_manager):
    """A key that expired after SCAN returned it has no memory usage."""
    mock_redis = mock_redis_connection_manager
    _holding(mock_redis, KEYS)
    mock_redis.scan.return_value = (0, [b"expired", b"cache:1"])
    mock_redis.dbsize.return_value = 1

    result = await misc.big_keys("app:*")

    assert result["keysScanned"] == 1
    assert result["keys"] == [CACHE_1]
    assert result["byType"] == {"string": {"keys": 1, "bytes": 300}}


@pytest.mark.asyncio
async def test_big_keys_leaves_out_a_key_that_is_gone_when_measured_again(
    mock_redis_connection_manager,
):
    mock_redis = mock_redis_connection_manager
    keys = {
        b"stays": (b"string", 300, 250, b"raw"),
        b"expires": (b"hash", {5: 9000, 1000: None}, 0, None),
    }
    _holding(mock_redis, keys)
    mock_redis.scan.return_value = (0, list(keys))
    mock_redis.dbsize.return_value = 1

    result = await misc.big_keys("app:*")

    assert [k["key"] for k in result["keys"]] == ["stays"]
    # it was there when the scan measured it
    assert result["keysScanned"] == 2


@pytest.mark.asyncio
async def test_big_keys_lists_a_key_once(mock_redis_connection_manager):
    """SCAN may return a key more than once while the keyspace changes."""
    mock_redis = mock_redis_connection_manager
    _holding(mock_redis, KEYS)
    mock_redis.scan.side_effect = [(9, [b"queue", b"cache:1"]), (0, [b"queue"])]
    mock_redis.dbsize.return_value = 2

    result = await misc.big_keys("app:*")

    assert [k["key"] for k in result["keys"]] == ["queue", "cache:1"]


@pytest.mark.asyncio
async def test_big_keys_on_an_empty_database(mock_redis_connection_manager):
    mock_redis = mock_redis_connection_manager
    commands = _holding(mock_redis, {})
    mock_redis.scan.return_value = (0, [])
    mock_redis.dbsize.return_value = 0

    assert await misc.big_keys("app:*") == {
        "cursor": 0,
        "scanComplete": True,
        "keysScanned": 0,
        "dbsize": 0,
        "keys": [],
        "byType": {},
    }
    assert commands == []
    assert commands.batches == []


@pytest.mark.asyncio
async def test_big_keys_marks_a_key_name_that_is_not_utf8(
    mock_redis_connection_manager,
):
    mock_redis = mock_redis_connection_manager
    _holding(
        mock_redis,
        {
            b"odd:\x8e": (b"string", 80, 1, b"embstr"),
            b"plain": (b"string", 70, 1, b"embstr"),
        },
    )
    mock_redis.scan.return_value = (0, [b"odd:\x8e", b"plain"])
    mock_redis.dbsize.return_value = 2

    result = await misc.big_keys("app:*")

    plain = {
        "key": "plain",
        "type": "string",
        "bytes": 70,
        "length": 1,
        "lengthUnit": "bytes",
        "encoding": "embstr",
    }
    assert result["keys"] == [
        {**plain, "key": "odd:\\x8e", "bytes": 80, "nonUtf8Key": True},
        plain,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments, error",
    [
        ({"top": 0}, "Error: top must be between 1 and 100"),
        ({"top": 101}, "Error: top must be between 1 and 100"),
        ({"min_bytes": -1}, "Error: min_bytes must not be negative"),
    ],
)
async def test_big_keys_rejects_values_out_of_range(
    mock_redis_connection_manager, arguments, error
):
    assert await misc.big_keys("app:*", **arguments) == error
    mock_redis_connection_manager.scan.assert_not_called()


NO_WHOLE_PREFIX = [
    "*",
    "",
    "s*",
    "sess*",
    "session*",
    "*session",
    "*:123",
    "?ession:*",
    "[st]ession:*",
]
WHOLE_PREFIX = [
    "session:*",
    "s:*",
    "user:42*",
    "user:42:*",
    "cache:*:html",
    "Florya_Session_*",
    "rate-limit*",
    "one-key",
    "onekey",
    "\\*starred*",
    "a\\:b*",
]


@pytest.mark.parametrize("pattern", NO_WHOLE_PREFIX)
def test_a_pattern_without_a_whole_prefix(pattern):
    """Nothing before the first glob character, or only the start of a name: it could match most keys."""
    assert misc._has_whole_prefix(pattern) is False


@pytest.mark.parametrize("pattern", WHOLE_PREFIX)
def test_a_pattern_with_a_whole_prefix(pattern):
    """A part of the name up to a separator comes before the first glob character, or the pattern names
    one key."""
    assert misc._has_whole_prefix(pattern) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("pattern", NO_WHOLE_PREFIX)
async def test_big_keys_rejects_a_pattern_without_a_whole_prefix(
    mock_redis_connection_manager, pattern
):
    result = await misc.big_keys(pattern)

    assert result == (
        "Error: pattern must start with a whole key prefix, up to a separator, for example session:*"
        f" (it is {pattern!r}). Ask the user which prefix to measure."
    )
    mock_redis_connection_manager.scan.assert_not_called()
    mock_redis_connection_manager.pipeline.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("pattern", WHOLE_PREFIX)
async def test_big_keys_accepts_a_pattern_with_a_whole_prefix(
    mock_redis_connection_manager, pattern
):
    mock_redis = mock_redis_connection_manager
    _holding(mock_redis, {})
    mock_redis.scan.return_value = (0, [])
    mock_redis.dbsize.return_value = 0

    result = await misc.big_keys(pattern)

    assert result["scanComplete"] is True
    assert mock_redis.scan.call_args.kwargs["match"] == pattern


def test_big_keys_has_no_default_pattern():
    """The schema marks pattern as required, so a call cannot leave it out."""
    import inspect

    parameters = inspect.signature(misc.big_keys).parameters

    assert parameters["pattern"].default is inspect.Parameter.empty
    assert "A WHOLE KEY PREFIX IS REQUIRED" in misc.big_keys.__doc__
    assert "ask them for it before calling" in misc.big_keys.__doc__


@pytest.mark.asyncio
async def test_big_keys_accepts_a_cursor_given_as_text(mock_redis_connection_manager):
    mock_redis = mock_redis_connection_manager
    _holding(mock_redis, {})
    mock_redis.scan.return_value = (0, [])
    mock_redis.dbsize.return_value = 0

    result = await misc.big_keys("app:*", cursor="1234")

    assert result["scanComplete"] is True
    assert mock_redis.scan.call_args.kwargs["cursor"] == 1234


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", ["n1:7000=12", "abc", "-1", "", "1" * 21])
async def test_big_keys_rejects_a_cursor_that_is_not_a_number(
    mock_redis_connection_manager, cursor
):
    result = await misc.big_keys("app:*", cursor=cursor)

    assert result == (
        "Error: invalid cursor: pass the cursor returned by the previous big_keys call unchanged, or 0 to"
        " start"
    )
    mock_redis_connection_manager.scan.assert_not_called()


@pytest.mark.asyncio
async def test_big_keys_error(mock_redis_connection_manager):
    mock_redis_connection_manager.scan.side_effect = RedisError("Connection failed")

    result = await misc.big_keys("user:*")

    assert result == "Error measuring keys with pattern 'user:*': Connection failed"


@pytest.mark.asyncio
async def test_big_keys_reports_a_command_the_user_may_not_run(
    mock_redis_connection_manager,
):
    """Without the right to run MEMORY USAGE the call fails; it does not report an empty database."""
    mock_redis = mock_redis_connection_manager
    denied = RedisError(
        "NOPERM this user has no permissions to run the 'memory|usage' command"
    )
    mock_redis.scan.return_value = (0, [b"cache:1"])
    mock_redis.pipeline.return_value.execute.side_effect = denied

    result = await misc.big_keys("app:*")

    assert result == f"Error measuring keys with pattern 'app:*': {denied}"


@pytest.mark.asyncio
async def test_big_keys_on_a_redis_cluster_needs_a_node(mock_redis_cluster):
    clients = _nodes(mock_redis_cluster)

    result = await misc.big_keys("app:*")

    assert result == (
        "Error: cluster default is a Redis Cluster: node is required, the one node to scan. "
        + NODE_LIST
    )
    for client in clients.values():
        client.scan.assert_not_called()
        client.pipeline.assert_not_called()
    mock_redis_cluster.pipeline.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("node", [P1, R1])
async def test_big_keys_on_a_redis_cluster_measures_the_named_node(
    mock_redis_cluster, node
):
    """Every command goes to the node's own client. Each pipeline starts with READONLY, without which a
    replica would redirect the commands on its keys to its primary."""
    clients = _nodes(mock_redis_cluster)
    commands = _holding(clients[node], KEYS)
    clients[node].scan.side_effect = [(17, [b"cache:1", b"session:1"]), (0, [b"queue"])]
    clients[node].dbsize.return_value = 7

    result = await misc.big_keys("app:*", node=node)

    assert result == {
        "cursor": 0,
        "scanComplete": True,
        "keysScanned": 3,
        # the keys of that node
        "dbsize": 7,
        "node": node,
        "keys": [QUEUE, SESSION_1, CACHE_1],
        "byType": {
            "list": {"keys": 1, "bytes": 90000},
            "hash": {"keys": 1, "bytes": 5000},
            "string": {"keys": 1, "bytes": 300},
        },
    }
    assert [c.kwargs["cursor"] for c in clients[node].scan.call_args_list] == [0, 17]
    # two scan steps and one pipeline for the three listed keys
    assert commands.batches == [5, 3, 10]
    assert commands[0] == commands[5] == commands[8] == ("READONLY",)
    assert commands.count(("READONLY",)) == 3
    for other, client in clients.items():
        if other != node:
            client.scan.assert_not_called()
            client.pipeline.assert_not_called()
    mock_redis_cluster.scan.assert_not_called()
    mock_redis_cluster.pipeline.assert_not_called()
    mock_redis_cluster.dbsize.assert_not_called()


@pytest.mark.asyncio
async def test_big_keys_on_a_redis_cluster_pages_with_the_cursor(
    mock_redis_cluster, monkeypatch
):
    # one SCAN step per call
    monkeypatch.setattr(misc, "BIG_KEYS_SECONDS", 0)
    clients = _nodes(mock_redis_cluster)
    _holding(clients[P2], KEYS)
    clients[P2].scan.side_effect = [(12, [b"cache:1"]), (0, [b"queue"])]
    clients[P2].dbsize.return_value = 2

    first = await misc.big_keys("app:*", node=P2)
    last = await misc.big_keys("app:*", node=P2, cursor=first["cursor"])

    assert (first["cursor"], first["scanComplete"], first["keys"]) == (
        12,
        False,
        [CACHE_1],
    )
    assert (last["cursor"], last["scanComplete"], last["keys"]) == (0, True, [QUEUE])
    assert [c.kwargs["cursor"] for c in clients[P2].scan.call_args_list] == [0, 12]


@pytest.mark.asyncio
async def test_big_keys_does_not_send_readonly_outside_a_redis_cluster(
    mock_redis_connection_manager,
):
    mock_redis = mock_redis_connection_manager
    commands = _holding(mock_redis, KEYS)
    mock_redis.scan.return_value = (0, [b"cache:1"])
    mock_redis.dbsize.return_value = 1

    result = await misc.big_keys("app:*")

    assert "node" not in result
    assert ("READONLY",) not in commands


@pytest.mark.asyncio
async def test_big_keys_on_a_redis_cluster_skips_a_key_whose_slot_has_moved(
    mock_redis_cluster,
):
    """The node redirects a key it no longer holds: the key is another node's now, not an error."""
    clients = _nodes(mock_redis_cluster)
    moved = MovedError("866 10.0.0.2:6500")
    keys = {
        b"moved": (b"hash", moved, 3, b"listpack"),
        b"moves-later": (b"hash", {5: 9000, 1000: moved}, 3, b"listpack"),
        b"stays": (b"string", 300, 250, b"raw"),
    }
    _holding(clients[P1], keys)
    clients[P1].scan.return_value = (0, list(keys))
    clients[P1].dbsize.return_value = 1

    result = await misc.big_keys("app:*", node=P1)

    assert [k["key"] for k in result["keys"]] == ["stays"]
    # the key that moved before the scan measured it is not counted
    assert result["keysScanned"] == 2


@pytest.mark.asyncio
async def test_big_keys_on_a_redis_cluster_reports_a_readonly_that_fails(
    mock_redis_cluster,
):
    """Without READONLY a replica would redirect every key, and the scan would report an empty node."""
    clients = _nodes(mock_redis_cluster)
    denied = RedisError(
        "NOPERM this user has no permissions to run the 'readonly' command"
    )
    _holding(clients[R1], {**KEYS, "READONLY": denied})
    clients[R1].scan.return_value = (0, [b"cache:1"])

    result = await misc.big_keys("app:*", node=R1)

    assert result == f"Error measuring keys with pattern 'app:*': {denied}"


@pytest.mark.asyncio
async def test_big_keys_on_a_redis_cluster_rejects_a_node_it_does_not_have(
    mock_redis_cluster,
):
    clients = _nodes(mock_redis_cluster)

    result = await misc.big_keys("app:*", node="10.0.0.9:6500")

    assert result == (
        "Error: node 10.0.0.9:6500 is not a node of the Redis Cluster default. "
        + NODE_LIST
    )
    for client in clients.values():
        client.scan.assert_not_called()


def test_scan_descriptions_require_the_node_and_the_users_choice():
    for tool in (misc.scan_keys, misc.big_keys, misc.key_prefixes):
        assert "SCAN ONLY WHERE THE USER ASKED" in tool.__doc__
        assert "do not try the clusters or the nodes one after another" in tool.__doc__
        assert (
            "node: REQUIRED on a Redis Cluster: the one node to scan, as host:port"
            in (tool.__doc__)
        )


def test_size_descriptions_name_no_other_tool():
    """Any of the scan and size tools may be excluded, so none of the size tools' descriptions points to
    another of them, and the scan tools' descriptions point to neither size tool."""
    tools = {
        "memory_usage": misc.memory_usage,
        "big_keys": misc.big_keys,
        "scan_keys": misc.scan_keys,
        "scan_all_keys": misc.scan_all_keys,
    }
    for name in ("memory_usage", "big_keys"):
        for other, tool in tools.items():
            if other != name:
                assert other not in tools[name].__doc__, (name, other)
                assert name not in tool.__doc__, (other, name)


def test_size_descriptions_state_the_limits():
    assert f"default {misc.DEFAULT_SAMPLES}, at most {misc.MAX_SAMPLES}" in (
        misc.memory_usage.__doc__
    )
    assert f"scans for about {misc.BIG_KEYS_SECONDS} s" in misc.big_keys.__doc__
    assert f"default 10, at most {misc.MAX_BIG_KEYS}" in misc.big_keys.__doc__
    assert f"a quick estimate ({misc.SCAN_SAMPLES} elements" in misc.big_keys.__doc__
    assert f"measured again with {misc.DEFAULT_SAMPLES} elements" in (
        misc.big_keys.__doc__
    )
    assert f"more than {misc.DEFAULT_SAMPLES}\n" in misc.big_keys.__doc__


@pytest.mark.asyncio
@pytest.mark.parametrize("pattern", ["*", "s*", "sess*", "*:123"])
async def test_scan_keys_without_a_whole_prefix_returns_a_sample(
    mock_redis_connection_manager, pattern
):
    """One page of names, and no cursor to walk the keyspace with."""
    mock_redis_connection_manager.scan.return_value = (17, [b"session:1", b"user:2"])

    result = await misc.scan_keys(pattern, count=1000)

    assert result == {
        "keys": ["session:1", "user:2"],
        "total_scanned": 2,
        "scan_complete": False,
        "sample": True,
    }
    mock_redis_connection_manager.scan.assert_called_once_with(
        cursor=0, match=pattern, count=1000
    )


@pytest.mark.asyncio
async def test_scan_keys_default_pattern_is_a_sample(mock_redis_connection_manager):
    mock_redis_connection_manager.scan.return_value = (5, [b"k"])

    result = await misc.scan_keys()

    assert result["sample"] is True and "cursor" not in result


@pytest.mark.asyncio
async def test_scan_keys_sample_of_a_small_database_is_the_whole_scan(
    mock_redis_connection_manager,
):
    """SCAN came back to 0 in one step: every key was seen, so there is nothing to continue."""
    mock_redis_connection_manager.scan.return_value = (0, [b"a", b"b"])

    result = await misc.scan_keys("*")

    assert result == {
        "cursor": 0,
        "keys": ["a", "b"],
        "total_scanned": 2,
        "scan_complete": True,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("pattern", ["*", "s*", "*:123"])
@pytest.mark.parametrize("cursor", [17, "17"])
async def test_scan_keys_without_a_whole_prefix_cannot_be_continued(
    mock_redis_connection_manager, pattern, cursor
):
    result = await misc.scan_keys(pattern, cursor=cursor)

    assert result == (
        f"Error: pattern {pattern!r} does not start with a whole key prefix, so it returns one sample page"
        " and cannot be continued. To page, use a pattern that starts with a key prefix up to a separator,"
        " for example session:*. Ask the user which prefix to scan."
    )
    mock_redis_connection_manager.scan.assert_not_called()


@pytest.mark.asyncio
async def test_scan_keys_with_a_whole_prefix_pages(mock_redis_connection_manager):
    mock_redis = mock_redis_connection_manager
    mock_redis.scan.side_effect = [(17, [b"session:1"]), (0, [b"session:2"])]

    first = await misc.scan_keys("session:*")
    last = await misc.scan_keys("session:*", cursor=first["cursor"])

    assert (first["cursor"], first["scan_complete"]) == (17, False)
    assert "sample" not in first
    assert (last["cursor"], last["scan_complete"]) == (0, True)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, -5, misc.MAX_SCAN_COUNT + 1, 100_000])
async def test_scan_keys_rejects_a_count_out_of_range(
    mock_redis_connection_manager, count
):
    """One SCAN command with a large COUNT keeps the server from answering anyone else until it is done."""
    result = await misc.scan_keys("session:*", count=count)

    assert result == "Error: count must be between 1 and 1000"
    mock_redis_connection_manager.scan.assert_not_called()


@pytest.mark.asyncio
async def test_scan_keys_sample_on_a_redis_cluster_names_the_node(mock_redis_cluster):
    clients = _nodes(mock_redis_cluster)
    clients[R1].scan.return_value = (9, [b"session:1"])

    result = await misc.scan_keys("*", node=R1)

    assert result == {
        "keys": ["session:1"],
        "total_scanned": 1,
        "scan_complete": False,
        "sample": True,
        "node": R1,
    }


def _patterns(*keys):
    names = misc._KeyNames()
    for key in keys:
        names.add(key)
    return names.patterns()


def test_key_names_with_an_id_become_one_pattern():
    """A part with a digit is variable: UUIDs, numbers, dates."""
    assert _patterns(
        b"spring:session:sessions:f43a430b-4af3-43cb-a545-b19327d0f414",
        b"spring:session:sessions:ce7eb517-8711-4d18-a5ff-8fa085bf0c94",
        b"spring:session:sessions:expires:ce7eb517-8711-4d18-a5ff-8fa085bf0c94",
        b"customerSolutionsWithEligibility::30962005140",
        b"customerSolutionsWithEligibility::0680684141",
        b"customerSolutionsWithEligibility::55846550592",
        b"Florya_Session_3f2a9c",
        b"report:2026-11-13",
    ) == [
        ("customerSolutionsWithEligibility::*", 3),
        ("spring:session:sessions:*", 2),
        ("Florya_Session_*", 1),
        ("report:*", 1),
        ("spring:session:sessions:expires:*", 1),
    ]


def test_key_names_keep_the_fixed_parts_after_an_id():
    assert _patterns(
        "user:1:profile", "user:2:profile", "user:2:settings", "user:17"
    ) == [("user:*:profile", 2), ("user:*", 1), ("user:*:settings", 1)]


def test_key_names_without_a_variable_part_stay_as_they_are():
    assert _patterns(b"config", b"maintenance_mode", b"feature:flags") == [
        ("config", 1),
        ("feature:flags", 1),
        ("maintenance_mode", 1),
    ]


def test_key_names_are_split_at_every_separator():
    assert _patterns(b"a.b/c|d_e:1", b"a.b/c|d_e:2", b"{user1000}.following") == [
        ("a.b/c|d_e:*", 2),
        ("*.following", 1),
    ]


def test_key_names_escape_their_glob_characters():
    """The pattern of a name with a glob character matches that name, not what the character would."""
    patterns = dict(_patterns(b"we*ird[x]:a", b"what?:1", b"back\\slash"))

    assert patterns == {"we\\*ird\\[x\\]:a": 1, "what\\?:*": 1, "back\\\\slash": 1}
    assert all(misc._has_whole_prefix(pattern) for pattern in patterns)


def test_key_names_that_are_not_utf8():
    assert _patterns(b"odd:\x8e", b"odd:\xff\xfe") == [("odd:*", 2)]


def test_key_names_with_very_many_parts_end_in_one_variable_part():
    deep = ":".join("abcdefghijklmnopqrst")

    assert _patterns(deep, deep + ":more") == [("a:b:c:d:e:f:g:h:i:j:k:l:*", 2)]


def test_many_different_names_at_one_place_are_a_variable_part():
    """Ids without a digit (user names, e-mail addresses): past 200 of them the place is variable."""
    words = [f"{a}{b}{c}" for a in "abcdefgh" for b in "abcdefgh" for c in "abcdefgh"]
    assert len(words) == 512

    patterns = _patterns(*(f"otp:{word}@example.com" for word in words))

    assert patterns == [("otp:*.com", 512)]


def test_a_name_with_many_keys_among_many_rare_ones_stays():
    """spring:session:sessions:<id> and spring:session:sessions:expires:<id>, with ids without a digit: half
    of the keys under sessions: are under expires:, which stays a name while the ids merge."""
    ids = [f"{a}{b}{c}" for a in "abcdefgh" for b in "abcdefgh" for c in "abcdefgh"]
    keys = []
    for value in ids:
        keys.append(f"sessions:{value}")
        keys.append(f"sessions:expires:{value}")

    assert _patterns(*keys) == [("sessions:*", 512), ("sessions:expires:*", 512)]


def test_few_different_names_at_one_place_stay_names():
    kinds = ["gold", "silver", "youth"]

    patterns = _patterns(*(f"campaign:{kinds[i % 3]}:{i}" for i in range(300)))

    assert patterns == [
        ("campaign:gold:*", 100),
        ("campaign:silver:*", 100),
        ("campaign:youth:*", 100),
    ]


def test_every_key_is_counted_once_whatever_is_merged():
    words = [f"{a}{b}{c}" for a in "abcdefgh" for b in "abcdefgh" for c in "abcdefgh"]
    keys = [f"{word}:x" for word in words] + [f"{word}" for word in words]
    keys += [f"user:{word}:profile" for word in words] + ["config"] * 3
    # as SCAN returns them: in no order of their names
    random.Random(1).shuffle(keys)

    patterns = _patterns(*keys)

    assert sum(count for _, count in patterns) == len(keys)
    assert dict(patterns)["user:*:profile"] == 512


@pytest.mark.asyncio
async def test_key_prefixes_groups_one_sample(mock_redis_connection_manager):
    mock_redis = mock_redis_connection_manager
    mock_redis.scan.return_value = (
        17,
        [b"session:1", b"session:2", b"user:1:profile", b"session:3", b"config"],
    )
    mock_redis.dbsize.return_value = 820_000

    result = await misc.key_prefixes()

    assert result == {
        "sampledKeys": 5,
        "dbsize": 820_000,
        "scanComplete": False,
        "prefixes": [
            {"pattern": "session:*", "keys": 3},
            {"pattern": "config", "keys": 1},
            {"pattern": "user:*:profile", "keys": 1},
        ],
    }
    # one SCAN step and nothing else: no pattern, no command on a key, no value, no second step
    mock_redis.scan.assert_called_once_with(cursor=0, count=1000)
    mock_redis.pipeline.assert_not_called()


@pytest.mark.asyncio
async def test_key_prefixes_of_a_small_database_are_exact(
    mock_redis_connection_manager,
):
    """The one step came back to 0: the sample is every key."""
    mock_redis = mock_redis_connection_manager
    mock_redis.scan.return_value = (0, [b"session:1", b"session:2"])
    mock_redis.dbsize.return_value = 2

    result = await misc.key_prefixes()

    assert (result["sampledKeys"], result["dbsize"], result["scanComplete"]) == (
        2,
        2,
        True,
    )


def test_key_prefixes_takes_no_cursor():
    """Nothing to continue with: a call cannot walk the keyspace."""
    import inspect

    assert list(inspect.signature(misc.key_prefixes).parameters) == ["node", "top"]


@pytest.mark.asyncio
async def test_key_prefixes_sums_the_patterns_top_leaves_out(
    mock_redis_connection_manager,
):
    mock_redis = mock_redis_connection_manager
    keys = [b"a:1", b"a:2", b"a:3", b"b:1", b"b:2", b"c:1", b"d:1"]
    mock_redis.scan.return_value = (0, keys)
    mock_redis.dbsize.return_value = len(keys)

    result = await misc.key_prefixes(top=2)

    assert result["prefixes"] == [
        {"pattern": "a:*", "keys": 3},
        {"pattern": "b:*", "keys": 2},
    ]
    assert result["otherPrefixes"] == {"patterns": 2, "keys": 2}


@pytest.mark.asyncio
async def test_key_prefixes_on_an_empty_database(mock_redis_connection_manager):
    mock_redis_connection_manager.scan.return_value = (0, [])
    mock_redis_connection_manager.dbsize.return_value = 0

    assert await misc.key_prefixes() == {
        "sampledKeys": 0,
        "dbsize": 0,
        "scanComplete": True,
        "prefixes": [],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("top", [0, misc.MAX_KEY_PREFIXES + 1])
async def test_key_prefixes_rejects_a_top_out_of_range(
    mock_redis_connection_manager, top
):
    assert await misc.key_prefixes(top=top) == "Error: top must be between 1 and 200"
    mock_redis_connection_manager.scan.assert_not_called()


@pytest.mark.asyncio
async def test_key_prefixes_error(mock_redis_connection_manager):
    mock_redis_connection_manager.scan.side_effect = RedisError("Connection failed")

    assert await misc.key_prefixes() == (
        "Error listing the key prefixes: Connection failed"
    )


@pytest.mark.asyncio
async def test_key_prefixes_on_a_redis_cluster_needs_a_node(mock_redis_cluster):
    clients = _nodes(mock_redis_cluster)

    result = await misc.key_prefixes()

    assert result == (
        "Error: cluster default is a Redis Cluster: node is required, the one node to scan. "
        + NODE_LIST
    )
    for client in clients.values():
        client.scan.assert_not_called()


@pytest.mark.asyncio
async def test_key_prefixes_on_a_redis_cluster_scans_the_named_node(mock_redis_cluster):
    clients = _nodes(mock_redis_cluster)
    clients[R1].scan.return_value = (40, [b"session:1", b"session:2"])
    clients[R1].dbsize.return_value = 270_000

    result = await misc.key_prefixes(node=R1)

    assert result == {
        "sampledKeys": 2,
        "dbsize": 270_000,
        "scanComplete": False,
        "node": R1,
        "prefixes": [{"pattern": "session:*", "keys": 2}],
    }
    clients[R1].scan.assert_called_once_with(cursor=0, count=1000)
    for other, client in clients.items():
        if other != R1:
            client.scan.assert_not_called()
    mock_redis_cluster.scan.assert_not_called()


def test_key_prefixes_description_states_its_limits():
    doc = misc.key_prefixes.__doc__
    assert f"a sample of about {misc.MAX_SCAN_COUNT} keys" in doc
    assert "IT IS A SAMPLE, NOT A COUNT" in doc
    assert f"default 50, at most {misc.MAX_KEY_PREFIXES}" in doc
    assert f"more than {misc.MAX_NAMES_OF_A_PART} different parts" in doc
    assert "(: _ / . |)" in doc and set(misc.NAME_SEPARATORS) == set(":_/.|")
    assert f"at most {misc.MAX_SCAN_COUNT}" in misc.scan_keys.__doc__
    # any of the scan tools may be excluded, so it names none of the others
    for other in ("scan_keys", "scan_all_keys", "big_keys", "memory_usage"):
        assert other not in doc
    for tool in (misc.scan_keys, misc.big_keys, misc.memory_usage, misc.scan_all_keys):
        assert "key_prefixes" not in tool.__doc__
