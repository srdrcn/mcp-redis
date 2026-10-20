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


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_reports_completion(mock_redis_cluster):
    """RedisCluster.scan returns one cursor per node: the batch is complete when every node is at 0."""
    mock_redis_cluster.scan.return_value = (
        {"n1:7000": 0, "n2:7001": 0},
        [b"user:1", "user:2"],
    )

    result = await misc.scan_keys("user:*", count=50)

    assert result == {
        "cursor": 0,
        "keys": ["user:1", "user:2"],
        "total_scanned": 2,
        "scan_complete": True,
    }
    mock_redis_cluster.scan.assert_called_once_with(cursor=0, match="user:*", count=50)


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_points_to_scan_all_keys(mock_redis_cluster):
    mock_redis_cluster.scan.return_value = ({"n1:7000": 12, "n2:7001": 0}, ["user:1"])

    result = await misc.scan_keys("user:*")

    assert result["scan_complete"] is False
    assert result["cursor"] == 0
    assert result["keys"] == ["user:1"]
    assert "scan_all_keys" in result["note"]


@pytest.mark.asyncio
async def test_scan_keys_on_a_redis_cluster_rejects_a_cursor(mock_redis_cluster):
    result = await misc.scan_keys("user:*", cursor=12)

    assert result.startswith(
        "Error: scan_keys cannot continue a cursor on a Redis Cluster"
    )
    assert "scan_all_keys" in result
    mock_redis_cluster.scan.assert_not_called()
