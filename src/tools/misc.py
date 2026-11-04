import asyncio
import re
from typing import Any, Dict, Union, List, Tuple
import aiohttp

from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import RedisError

from src.common.connection import RedisConnectionManager
from src.common.registry import ToolKind, redis_tool
from src.common.values import text
from src.common.config import MCP_DOCS_SEARCH_URL
from src.version import __version__

DOCS_SEARCH_TIMEOUT_SECONDS = 10


@redis_tool("core", ToolKind.DESTRUCTIVE)
async def delete(key: str) -> str:
    """Delete a Redis key.

    Args:
        key (str): The key to delete.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        result = r.delete(key)
        return f"Successfully deleted {key}" if result else f"Key {key} not found"
    except RedisError as e:
        return f"Error deleting key {key}: {str(e)}"


@redis_tool("core")
async def type(key: str) -> Dict[str, Any]:
    """Returns the string representation of the type of the value stored at key

    Args:
        key (str): The key to check.

    Returns:
        str: The type of key, or none when key doesn't exist
    """
    try:
        r = RedisConnectionManager.get_connection()
        key_type = r.type(key)
        info = {"key": key, "type": key_type, "ttl": r.ttl(key)}

        return info
    except RedisError as e:
        return {"error": str(e)}


@redis_tool("core", ToolKind.WRITE)
async def expire(name: str, expire_seconds: int) -> str:
    """Set an expiration time for a Redis key.

    Args:
        name: The Redis key.
        expire_seconds: Time in seconds after which the key should expire.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        success = r.expire(name, expire_seconds)
        return (
            f"Expiration set to {expire_seconds} seconds for '{name}'."
            if success
            else f"Key '{name}' does not exist."
        )
    except RedisError as e:
        return f"Error setting expiration for key '{name}': {str(e)}"


@redis_tool("core", ToolKind.WRITE)
async def rename(old_key: str, new_key: str) -> Dict[str, Any]:
    """
    Renames a Redis key from old_key to new_key.

    Args:
        old_key (str): The current name of the Redis key to rename.
        new_key (str): The new name to assign to the key.

    Returns:
        Dict[str, Any]: A dictionary containing the result of the operation.
            On success: {"status": "success", "message": "..."}
            On error: {"error": "..."}
    """
    try:
        r = RedisConnectionManager.get_connection()

        # Check if the old key exists
        if not r.exists(old_key):
            return {"error": f"Key '{old_key}' does not exist."}

        # Rename the key
        r.rename(old_key, new_key)
        return {
            "status": "success",
            "message": f"Renamed key '{old_key}' to '{new_key}'",
        }

    except RedisError as e:
        return {"error": str(e)}


@redis_tool("core")
async def scan_keys(
    pattern: str = "*", count: int = 100, cursor: Union[int, str] = 0
) -> Union[str, Dict[str, Any]]:
    """
    Scan keys in the Redis database using the SCAN command (non-blocking, production-safe).

    ⚠️  IMPORTANT: This returns PARTIAL results from one iteration. Call it again with the returned cursor
    until cursor becomes 0 (scan_complete is true) to see every matching key.

    The SCAN command iterates through the keyspace in small chunks, making it safe to use
    on large databases without blocking other operations.

    Args:
        pattern: Pattern to match keys against (default is "*" for all keys).
                Common patterns: "user:*", "cache:*", "*:123", etc.
        count: Hint for the number of keys to return per iteration (default 100).
               Redis may return more or fewer keys than this hint.
        cursor: 0 to start from the beginning. To continue scanning, pass the cursor value returned
                from the previous call unchanged.

    Returns:
        A dictionary containing:
        - 'cursor': Next cursor position (0 means scan is complete)
        - 'keys': List of keys found in this iteration (PARTIAL RESULTS)
        - 'total_scanned': Number of keys returned in this batch
        - 'scan_complete': Boolean indicating if scan is finished
        Or an error message if something goes wrong.

    Example usage:
        First call: scan_keys("user:*") -> returns cursor=1234, keys=[...], scan_complete=False
        Next call: scan_keys("user:*", cursor=1234) -> continues from where it left off
        Final call: returns cursor=0, scan_complete=True when done

    On a Redis Cluster each call runs one SCAN step on every primary that has not finished (count applies to
    each of them), and the cursor is text that holds the position on each of those primaries, e.g.
    "10.0.0.1:6379=1536,10.0.0.2:6379=96": pass it back as it is.

    A key name that is not UTF-8 is shown with \\xNN for those bytes and also listed in 'nonUtf8Keys'; the
    other tools cannot read such a key by name.
    """
    try:
        r = RedisConnectionManager.get_connection(decode_responses=False)
        if isinstance(r, RedisCluster):
            return _scan_cluster_step(r, pattern, count, cursor)
        cursor, keys = r.scan(
            cursor=_standalone_cursor(cursor), match=pattern, count=count
        )
        return _scan_page(cursor, keys, cursor == 0)
    except _CursorError as e:
        return f"Error: {e}"
    except RedisError as e:
        return f"Error scanning keys with pattern '{pattern}': {str(e)}"


def _scan_page(
    cursor: Union[int, str], keys: List[Any], complete: bool
) -> Dict[str, Any]:
    names = [text(key) for key in keys]
    page: Dict[str, Any] = {
        "cursor": cursor,
        "keys": names,
        "total_scanned": len(names),
        "scan_complete": complete,
    }
    not_utf8 = [name for key, name in zip(keys, names) if not _is_utf8(key)]
    if not_utf8:
        page["nonUtf8Keys"] = not_utf8
    return page


def _is_utf8(key: Any) -> bool:
    if not isinstance(key, bytes):
        return True
    try:
        key.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


class _CursorError(ValueError):
    pass


# a SCAN cursor is an unsigned 64-bit number
_CURSOR_NUMBER = re.compile(r"[0-9]{1,20}")
_INVALID_CURSOR = "invalid cursor: pass the cursor returned by the previous scan_keys call unchanged, or 0 to start"


def _standalone_cursor(cursor: Union[int, str]) -> int:
    if isinstance(cursor, int):
        return cursor
    if _CURSOR_NUMBER.fullmatch(cursor.strip()):
        return int(cursor)
    raise _CursorError(_INVALID_CURSOR)


def _cluster_positions(
    r: RedisCluster, cursor: Union[int, str]
) -> Dict[str, Tuple[ClusterNode, int]]:
    """The primaries still to be scanned with their SCAN cursors, by node name. The cursor of a Redis Cluster
    scan is ``name=position`` for each of them, comma-separated; a primary that finished is left out."""
    primaries = {
        node.name: node for node in sorted(r.get_primaries(), key=lambda n: n.name)
    }
    if isinstance(cursor, int) or _CURSOR_NUMBER.fullmatch(cursor.strip()):
        # a number other than 0 is the cursor of a standalone server, not of a Redis Cluster
        if int(cursor) != 0:
            raise _CursorError(_INVALID_CURSOR)
        return {name: (node, 0) for name, node in primaries.items()}
    positions: Dict[str, Tuple[ClusterNode, int]] = {}
    for part in cursor.split(","):
        name, _, position = part.strip().rpartition("=")
        if not name or not _CURSOR_NUMBER.fullmatch(position) or name in positions:
            raise _CursorError(_INVALID_CURSOR)
        if name not in primaries:
            raise _CursorError(
                f"node {name} of the cursor is not a primary of this Redis Cluster (a failover or"
                " resharding since the scan started?); start again with cursor 0"
            )
        positions[name] = (primaries[name], int(position))
    return positions


def _scan_cluster_step(
    r: RedisCluster, pattern: str, count: int, cursor: Union[int, str]
) -> Dict[str, Any]:
    positions = _cluster_positions(r, cursor)
    keys: List[Any] = []
    remaining: Dict[str, int] = {}
    for name, (node, position) in positions.items():
        cursors, batch = r.scan(
            cursor=position, match=pattern, count=count, target_nodes=node
        )
        keys.extend(batch)
        if cursors[name]:
            remaining[name] = cursors[name]
    return _scan_page(
        ",".join(f"{name}={c}" for name, c in remaining.items()) or 0,
        keys,
        not remaining,
    )


@redis_tool("core")
async def scan_all_keys(
    pattern: str = "*", batch_size: int = 100
) -> Union[str, List[str]]:
    """
    Scan and return ALL keys matching a pattern using multiple SCAN iterations.

    This function automatically handles the SCAN cursor iteration to collect all matching keys.
    It's safer than KEYS * for large databases but will still collect all results in memory.

    ⚠️  WARNING: With very large datasets (millions of keys), this may consume significant memory and
    exceed the time limit of a tool call: narrow the pattern.

    A key name that is not UTF-8 is shown with \\xNN for those bytes; the other tools cannot read such a key
    by name.

    Args:
        pattern: Pattern to match keys against (default is "*" for all keys).
        batch_size: Number of keys to scan per iteration (default 100).

    Returns:
        A list of all keys matching the pattern or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(decode_responses=False)
        if isinstance(r, RedisCluster):
            # RedisCluster.scan returns one cursor per node; scan_iter walks every primary
            return [text(key) for key in r.scan_iter(match=pattern, count=batch_size)]
        all_keys = []
        cursor = 0

        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=batch_size)
            all_keys.extend(text(key) for key in keys)

            # Break when scan is complete (cursor returns to 0)
            if cursor == 0:
                break

        return all_keys
    except RedisError as e:
        return f"Error scanning all keys with pattern '{pattern}': {str(e)}"


@redis_tool("docs", cluster="none")
async def search_redis_documents(
    question: str,
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Search Redis documentation and knowledge base to learn about Redis concepts and use cases.

    This tool exposes updated and curated documentation, and must be invoked every time the user wants to learn more in areas including:

    **Common Use Cases:**
    - Session Management: User session storage and management
    - Caching: Application-level and database query caching
    - Rate Limiting: API throttling and request limiting
    - Leaderboards: Gaming and ranking systems
    - Semantic Search: AI-powered similarity search
    - Agentic Workflows: AI agent state and memory management
    - RAG (Retrieval-Augmented Generation): Vector storage for AI applications
    - Real-time Analytics: Counters, metrics, and time-series data
    - Message Queues: Task queues and job processing
    - Geospatial: Location-based queries and proximity search

    Args:
        question: The question about Redis concepts, data structures, features, or use cases

    Returns:
        Union[List[Dict[str, Any]], Dict[str, Any]]: A list of documentation results from the API, or a dict with an error message.
    """
    if not MCP_DOCS_SEARCH_URL:
        return {"error": "MCP_DOCS_SEARCH_URL environment variable is not configured"}

    if not question.strip():
        return {"error": "Question parameter cannot be empty"}

    try:
        headers = {
            "Accept": "application/json",
            "User-Agent": f"redis-mcp/{__version__}",
        }
        timeout = aiohttp.ClientTimeout(total=DOCS_SEARCH_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                url=MCP_DOCS_SEARCH_URL, params={"q": question}, headers=headers
            ) as response:
                if response.status >= 400:
                    try:
                        error_payload = await response.json()
                        return {
                            "error": f"Docs search request failed with status {response.status}",
                            "details": error_payload,
                        }
                    except aiohttp.ContentTypeError:
                        text_content = await response.text()
                        return {
                            "error": f"Docs search request failed with status {response.status}: {text_content}"
                        }

                # Try to parse JSON response
                try:
                    result = await response.json()
                    return result
                except aiohttp.ContentTypeError:
                    # If not JSON, return text content
                    text_content = await response.text()
                    return {"error": f"Non-JSON response: {text_content}"}

    except (asyncio.TimeoutError, TimeoutError):
        return {
            "error": f"Docs search request timed out after {DOCS_SEARCH_TIMEOUT_SECONDS} seconds"
        }
    except aiohttp.ClientError as e:
        return {"error": f"HTTP client error: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error calling docs API: {str(e)}"}
