from unittest.mock import Mock, patch

import pytest
from redis import Redis
from redis.cluster import RedisCluster
from redis.exceptions import RedisError

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


def _primaries(cluster, steps):
    """Primaries n1:7000 and n2:7001 (listed out of order); ``steps`` maps a node name to the (cursor, keys) its
    SCAN steps return, in order."""
    nodes = {}
    for name in ("n2:7001", "n1:7000"):
        node = Mock()
        node.name = name
        nodes[name] = node
    cluster.get_primaries.return_value = list(nodes.values())
    remaining = {name: list(results) for name, results in steps.items()}

    def scan(cursor, match, count, target_nodes):
        next_cursor, keys = remaining[target_nodes.name].pop(0)
        return {target_nodes.name: next_cursor}, keys

    cluster.scan.side_effect = scan
    return nodes


def _scan_calls(cluster):
    return [
        (c.kwargs["target_nodes"].name, c.kwargs["cursor"], c.kwargs["count"])
        for c in cluster.scan.call_args_list
    ]


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_reports_completion(mock_redis_cluster):
    """One SCAN step on every primary; the scan is complete when every node is back at 0."""
    _primaries(
        mock_redis_cluster,
        {"n1:7000": [(0, [b"user:1"])], "n2:7001": [(0, ["user:2"])]},
    )

    result = await misc.scan_keys("user:*", count=50)

    assert result == {
        "cursor": 0,
        "keys": ["user:1", "user:2"],
        "total_scanned": 2,
        "scan_complete": True,
    }
    assert _scan_calls(mock_redis_cluster) == [("n1:7000", 0, 50), ("n2:7001", 0, 50)]
    assert all(
        c.kwargs["match"] == "user:*" for c in mock_redis_cluster.scan.call_args_list
    )


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_pages_with_the_cursor(mock_redis_cluster):
    """The cursor holds the position of every primary that has not finished; passing it back continues
    those primaries only."""
    _primaries(
        mock_redis_cluster,
        {
            "n1:7000": [(12, ["a:1"]), (40, ["a:2"]), (0, ["a:3"])],
            "n2:7001": [(0, ["b:1"])],
        },
    )

    first = await misc.scan_keys("*", count=10)
    second = await misc.scan_keys("*", count=10, cursor=first["cursor"])
    last = await misc.scan_keys("*", count=10, cursor=second["cursor"])

    assert first == {
        "cursor": "n1:7000=12",
        "keys": ["a:1", "b:1"],
        "total_scanned": 2,
        "scan_complete": False,
    }
    assert second["cursor"] == "n1:7000=40"
    assert second["keys"] == ["a:2"]
    assert last == {
        "cursor": 0,
        "keys": ["a:3"],
        "total_scanned": 1,
        "scan_complete": True,
    }
    assert _scan_calls(mock_redis_cluster) == [
        ("n1:7000", 0, 10),
        ("n2:7001", 0, 10),
        ("n1:7000", 12, 10),
        ("n1:7000", 40, 10),
    ]


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_continues_every_listed_node(
    mock_redis_cluster,
):
    _primaries(
        mock_redis_cluster,
        {"n1:7000": [(0, ["a"])], "n2:7001": [(9, ["b"])]},
    )

    result = await misc.scan_keys("*", cursor=" n2:7001=5 , n1:7000=3 ")

    assert result["cursor"] == "n2:7001=9"
    assert result["keys"] == ["b", "a"]
    assert _scan_calls(mock_redis_cluster) == [("n2:7001", 5, 100), ("n1:7000", 3, 100)]


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", [0, "0", " 0 "])
async def test_scan_keys_on_a_redis_cluster_starts_at_zero(mock_redis_cluster, cursor):
    _primaries(
        mock_redis_cluster,
        {"n1:7000": [(0, [])], "n2:7001": [(0, [])]},
    )

    result = await misc.scan_keys("*", cursor=cursor)

    assert result["scan_complete"] is True
    assert [name for name, _, _ in _scan_calls(mock_redis_cluster)] == [
        "n1:7000",
        "n2:7001",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cursor",
    [
        12,
        "12",
        "",
        "n1:7000",
        "n1:7000=",
        "=12",
        "n1:7000=abc",
        "n1:7000=-1",
        "n1:7000=1,n1:7000=2",
        "n1:7000=1,",
        "n1:7000=" + "1" * 21,
    ],
)
async def test_scan_keys_on_a_redis_cluster_rejects_an_invalid_cursor(
    mock_redis_cluster, cursor
):
    _primaries(mock_redis_cluster, {})

    result = await misc.scan_keys("*", cursor=cursor)

    assert result == (
        "Error: invalid cursor: pass the cursor returned by the previous scan_keys call unchanged, or 0 to"
        " start"
    )
    mock_redis_cluster.scan.assert_not_called()


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_rejects_a_node_that_is_not_a_primary(
    mock_redis_cluster,
):
    _primaries(mock_redis_cluster, {})

    result = await misc.scan_keys("*", cursor="n1:7000=3,n9:7009=4")

    assert result == (
        "Error: node n9:7009 of the cursor is not a primary of this Redis Cluster (a failover or resharding"
        " since the scan started?); start again with cursor 0"
    )
    mock_redis_cluster.scan.assert_not_called()


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_error(mock_redis_cluster):
    _primaries(mock_redis_cluster, {})
    mock_redis_cluster.scan.side_effect = RedisError("node down")

    result = await misc.scan_keys("user:*")

    assert result == "Error scanning keys with pattern 'user:*': node down"


@pytest.mark.asyncio
async def test_scan_keys_accepts_a_cursor_given_as_text(mock_redis_connection_manager):
    mock_redis_connection_manager.scan.return_value = (0, [])

    result = await misc.scan_keys("*", cursor="1234")

    assert result["scan_complete"] is True
    mock_redis_connection_manager.scan.assert_called_once_with(
        cursor=1234, match="*", count=100
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
    a number, or {samples: number} for a key whose size depends on the samples; ``length`` may be an exception
    its length command fails with; the encoding is "hashtable" when left out. Records each queued command in
    ``commands`` and each executed pipeline's number of commands in ``batches``."""

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pattern", ["*", "", "*session", "*:session:*", "?ession:*", "[st]ession:*"]
)
async def test_big_keys_rejects_a_pattern_without_a_prefix(
    mock_redis_connection_manager, pattern
):
    """A pattern that starts with a glob character has no prefix: it could match every key."""
    result = await misc.big_keys(pattern)

    assert result == (
        f"Error: pattern must start with a key prefix, for example session:* (it is {pattern!r})."
        " Ask the user which prefix to measure."
    )
    mock_redis_connection_manager.scan.assert_not_called()
    mock_redis_connection_manager.pipeline.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pattern",
    ["session:*", "s*", "user:42:*", "cache:*:html", "one-key", "\\*starred*"],
)
async def test_big_keys_accepts_a_pattern_that_starts_with_a_prefix(
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
    assert "A KEY PREFIX IS REQUIRED" in misc.big_keys.__doc__
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
async def test_big_keys_on_a_redis_cluster_pages_with_the_cursor(
    mock_redis_cluster, monkeypatch
):
    """One SCAN step on every primary that has not finished per round; the cursor holds their positions."""
    # one round per call
    monkeypatch.setattr(misc, "BIG_KEYS_SECONDS", 0)
    _primaries(
        mock_redis_cluster,
        {
            "n1:7000": [(12, [b"cache:1"]), (0, [b"queue"])],
            "n2:7001": [(0, [b"session:1", b"session:2"])],
        },
    )
    _holding(mock_redis_cluster, KEYS)
    mock_redis_cluster.dbsize.return_value = 4

    first = await misc.big_keys("app:*")
    last = await misc.big_keys("app:*", cursor=first["cursor"])

    assert first["cursor"] == "n1:7000=12"
    assert first["scanComplete"] is False
    assert first["keysScanned"] == 3
    assert first["keys"] == [SESSION_2, SESSION_1, CACHE_1]
    assert (last["cursor"], last["scanComplete"]) == (0, True)
    assert last["keys"] == [QUEUE]
    assert _scan_calls(mock_redis_cluster) == [
        ("n1:7000", 0, misc.BIG_KEYS_SCAN_COUNT),
        ("n2:7001", 0, misc.BIG_KEYS_SCAN_COUNT),
        ("n1:7000", 12, misc.BIG_KEYS_SCAN_COUNT),
    ]
    # the keys of every primary are counted
    assert first["dbsize"] == 4
    mock_redis_cluster.dbsize.assert_called_with(target_nodes=RedisCluster.PRIMARIES)


@pytest.mark.asyncio
async def test_big_keys_on_a_redis_cluster_scans_every_primary_until_done(
    mock_redis_cluster,
):
    _primaries(
        mock_redis_cluster,
        {
            "n1:7000": [(12, [b"cache:1"]), (40, []), (0, [b"queue"])],
            "n2:7001": [(0, [b"session:1"])],
        },
    )
    _holding(mock_redis_cluster, KEYS)
    mock_redis_cluster.dbsize.return_value = 3

    result = await misc.big_keys("app:*")

    assert (result["cursor"], result["scanComplete"], result["keysScanned"]) == (
        0,
        True,
        3,
    )
    assert [k["key"] for k in result["keys"]] == ["queue", "session:1", "cache:1"]
    assert [name for name, _, _ in _scan_calls(mock_redis_cluster)] == [
        "n1:7000",
        "n2:7001",
        "n1:7000",
        "n1:7000",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cursor, error",
    [
        (
            12,
            "Error: invalid cursor: pass the cursor returned by the previous big_keys call unchanged, or 0 to"
            " start",
        ),
        (
            "n1:7000=3,n9:7009=4",
            "Error: node n9:7009 of the cursor is not a primary of this Redis Cluster (a failover or resharding"
            " since the scan started?); start again with cursor 0",
        ),
    ],
)
async def test_big_keys_on_a_redis_cluster_rejects_an_invalid_cursor(
    mock_redis_cluster, cursor, error
):
    _primaries(mock_redis_cluster, {})

    assert await misc.big_keys("app:*", cursor=cursor) == error
    mock_redis_cluster.scan.assert_not_called()


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
