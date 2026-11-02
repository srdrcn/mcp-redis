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
