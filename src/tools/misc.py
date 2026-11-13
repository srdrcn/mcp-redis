import asyncio
import heapq
import re
import socket
import time
from typing import Any, Dict, Optional, Union, List, Tuple
import aiohttp

from redis import Redis
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import AskError, RedisError

from src.common.connection import RedisConnectionManager
from src.common.registry import ToolKind, redis_tool
from src.common.values import text
from src.common.config import MCP_DOCS_SEARCH_URL
from src.version import __version__

DOCS_SEARCH_TIMEOUT_SECONDS = 10

# MEMORY USAGE measures this many elements of a collection and scales the result to its length, so the size of
# a key with more elements is an estimate. For one key, and for the keys big_keys lists:
DEFAULT_SAMPLES = 1000
# it runs in O(samples): no more than this, and never 0, which measures every element
MAX_SAMPLES = 10_000
# for every key big_keys scans (the Redis default)
SCAN_SAMPLES = 5
# keys measured with DEFAULT_SAMPLES in one pipeline, which the server runs without serving other clients
SIZES_PER_PIPELINE = 10
# big_keys returns its cursor once it has scanned this long; a tool call has 15 s (see src.common.registry)
BIG_KEYS_SECONDS = 5
# keys of one SCAN step, measured with one pipeline
BIG_KEYS_SCAN_COUNT = 1000
MAX_BIG_KEYS = 100

# the command that counts what a type holds, and what it counts
_LENGTHS = {
    "string": ("strlen", "bytes"),
    "hash": ("hlen", "fields"),
    "list": ("llen", "items"),
    "set": ("scard", "members"),
    "zset": ("zcard", "members"),
    "stream": ("xlen", "entries"),
}
# a collection stored as one block of memory, which MEMORY USAGE measures whole
_COMPACT_ENCODINGS = {"listpack", "listpackex", "ziplist", "zipmap", "intset"}


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
    pattern: str = "*",
    count: int = 100,
    cursor: Union[int, str] = 0,
    node: Optional[str] = None,
) -> Union[str, Dict[str, Any]]:
    """
    Scan keys in the Redis database using the SCAN command (non-blocking, production-safe).

    ⚠️  IMPORTANT: This returns PARTIAL results from one iteration. Call it again with the returned cursor
    until cursor becomes 0 (scan_complete is true) to see every matching key.

    ⚠️  SCAN ONLY WHERE THE USER ASKED: the cluster they named and, on a Redis Cluster, the node they named.
    If either is missing, ask the user; do not try the clusters or the nodes one after another.

    The SCAN command iterates through the keyspace in small chunks, making it safe to use
    on large databases without blocking other operations.

    Args:
        pattern: Pattern to match keys against (default is "*" for all keys).
                Common patterns: "user:*", "cache:*", "*:123", etc.
        count: Hint for the number of keys to return per iteration (default 100).
               Redis may return more or fewer keys than this hint.
        cursor: 0 to start from the beginning. To continue scanning, pass the cursor value returned
                from the previous call unchanged, with the same node.
        node: REQUIRED on a Redis Cluster: the one node to scan, as host:port. The keys of a Redis Cluster
              are spread over its primaries, and a replica holds the keys of its primary. A call without it
              is rejected with the list of the cluster's nodes. Leave it out on any other cluster.

    Returns:
        A dictionary containing:
        - 'cursor': Next cursor position (0 means scan is complete)
        - 'keys': List of keys found in this iteration (PARTIAL RESULTS)
        - 'total_scanned': Number of keys returned in this batch
        - 'scan_complete': Boolean indicating if scan is finished
        - 'node': The node that was scanned (on a Redis Cluster)
        Or an error message if something goes wrong.

    Example usage:
        First call: scan_keys("user:*") -> returns cursor=1234, keys=[...], scan_complete=False
        Next call: scan_keys("user:*", cursor=1234) -> continues from where it left off
        Final call: returns cursor=0, scan_complete=True when done

    A key name that is not UTF-8 is shown with \\xNN for those bytes and also listed in 'nonUtf8Keys'; the
    other tools cannot read such a key by name.
    """
    try:
        server, scanned_node = _scan_server(node)
        cursor, keys = server.scan(
            cursor=_cursor_number(cursor, "scan_keys"), match=pattern, count=count
        )
        page = _scan_page(cursor, keys, cursor == 0)
        if scanned_node:
            page["node"] = scanned_node
        return page
    except (_CursorError, _NodeError) as e:
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


def _cursor_number(cursor: Union[int, str], tool: str) -> int:
    """The SCAN cursor a call was given; ``tool`` is the scanning tool, named in the error."""
    if isinstance(cursor, int):
        return cursor
    if _CURSOR_NUMBER.fullmatch(cursor.strip()):
        return int(cursor)
    raise _CursorError(
        f"invalid cursor: pass the cursor returned by the previous {tool} call unchanged, or 0 to start"
    )


class _NodeError(ValueError):
    pass


def _scan_server(node: Optional[str]) -> Tuple[Redis, Optional[str]]:
    """The one server a scan runs on, as a client that returns bytes: the server of a standalone or Sentinel
    cluster or, on a Redis Cluster, the node ``node`` names (host:port), with that node's address. A scan of a
    Redis Cluster never spreads over its nodes: the caller says which one."""
    config = RedisConnectionManager.cluster_config()
    r = RedisConnectionManager.get_connection(decode_responses=False)
    named = (node or "").strip()
    if not isinstance(r, RedisCluster):
        if named:
            raise _NodeError(
                f"cluster {config.name} is not a Redis Cluster (mode {config.mode}): it has one server, so"
                " leave node out"
            )
        return r, None
    found = _cluster_node(r, named) if named else None
    if found is None:
        # the cluster's nodes may have changed since the client last read them
        r.nodes_manager.initialize()
        found = _cluster_node(r, named) if named else None
    if found is None:
        problem = (
            f"node {named} is not a node of the Redis Cluster {config.name}"
            if named
            else f"cluster {config.name} is a Redis Cluster: node is required, the one node to scan"
        )
        raise _NodeError(
            f"{problem}. Its nodes, as host:port: {_node_list(r)}. Ask the user which node to scan."
        )
    return r.get_redis_connection(found), found.name


def _cluster_node(r: RedisCluster, named: str) -> Optional[ClusterNode]:
    """The node of the Redis Cluster at ``named`` (host:port): by the address the cluster gives it, else by a
    host name that resolves to that address (a cluster mostly names its nodes by IP address)."""
    host, _, port = named.rpartition(":")
    host = host.strip("[]").lower()
    if not host or not port.isdigit():
        return None
    nodes = [n for n in r.get_nodes() if n.port == int(port)]
    for candidate in nodes:
        if candidate.host.lower() == host:
            return candidate
    if not nodes:
        return None
    try:
        addresses = {
            info[4][0]
            for info in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        }
    except OSError:
        return None
    matches = [n for n in nodes if n.host in addresses]
    return matches[0] if len(matches) == 1 else None


def _node_list(r: RedisCluster) -> str:
    """The nodes of the Redis Cluster: each primary with its replicas."""
    replicas: Dict[str, List[str]] = {
        n.name: [] for n in sorted(r.get_primaries(), key=lambda n: n.name)
    }
    seen = set()
    # one list of nodes (the primary first) for all the slots of a range
    for nodes in r.nodes_manager.slots_cache.values():
        if id(nodes) not in seen:
            seen.add(id(nodes))
            followers = replicas.setdefault(nodes[0].name, [])
            followers.extend(n.name for n in nodes[1:] if n.name not in followers)
    return ", ".join(
        f"{primary} (primary; replicas: {', '.join(sorted(names))})"
        if names
        else f"{primary} (primary)"
        for primary, names in replicas.items()
    )


def _pipeline(server: Any, node: bool) -> Any:
    """A pipeline whose commands run as they are sent. On a node of a Redis Cluster it starts with READONLY, so
    that a replica answers commands on its keys instead of redirecting them to its primary."""
    pipe = server.pipeline(transaction=False)
    if node:
        pipe.execute_command("READONLY")
    return pipe


def _replies(pipe: Any, node: bool) -> List[Any]:
    """The replies of a _pipeline, a failed command's error in its place."""
    replies = pipe.execute(raise_on_error=False)
    if not node:
        return replies
    if isinstance(replies[0], Exception):
        raise replies[0]
    return replies[1:]


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


@redis_tool("core")
async def memory_usage(key: str, samples: int = DEFAULT_SAMPLES) -> Dict[str, Any]:
    """How large a key is, without reading its value: the bytes it takes in memory (MEMORY USAGE), its type,
    length, encoding and TTL. Use it before reading a value that may be large.

    Args:
        key: The key to measure.
        samples: How many elements of a hash, list, set, sorted set or stream are measured to work out
                 'bytes' (default 1000, at most 10000). A key with more elements than that gets an estimate,
                 marked 'estimated': the size of the measured elements, scaled to the length.

    Returns:
        A dictionary containing:
        - 'key' and 'type'; for a key that does not exist the type is 'none' and nothing else is set
        - 'bytes': Memory the key and its value take, overhead included
        - 'estimated': true when 'bytes' is an estimate. It can be far off when a few elements are much
          larger than the others; with samples equal to the length it is exact
        - 'length' and 'lengthUnit': bytes of a string, fields of a hash, items of a list, members of a set or
          sorted set, entries of a stream (left out for any other type)
        - 'encoding': How Redis stores the value (OBJECT ENCODING)
        - 'ttl': Seconds until the key expires, -1 for never
        Or {"error": ...} if something goes wrong.
    """
    if not 1 <= samples <= MAX_SAMPLES:
        return {"error": f"samples must be between 1 and {MAX_SAMPLES}"}
    try:
        r = RedisConnectionManager.get_connection()
        pipe = r.pipeline(transaction=False)
        pipe.type(key)
        pipe.ttl(key)
        key_type, ttl = pipe.execute()
        size = _sizes(r, [(key, key_type)], samples)[0]
        if size is None:
            return {"key": key, "type": "none"}
        return {"key": key, "type": key_type, **size, "ttl": ttl}
    except RedisError as e:
        return {"error": str(e)}


def _sizes(
    r: Union[Redis, RedisCluster],
    keys: List[Tuple[Any, str]],
    samples: int,
    node: bool = False,
) -> List[Optional[Dict[str, Any]]]:
    """For each (key, type): 'bytes' (MEMORY USAGE with ``samples``), 'estimated' if they are scaled from the
    samples, 'length' with 'lengthUnit' (left out for a type without a length command, such as a module's, and
    for a key that holds another type by now) and 'encoding'. None for a key that is gone. ``node``: ``r`` is
    a client of one node of a Redis Cluster (see _pipeline)."""
    sizes: List[Optional[Dict[str, Any]]] = []
    for start in range(0, len(keys), SIZES_PER_PIPELINE):
        batch = keys[start : start + SIZES_PER_PIPELINE]
        pipe = _pipeline(r, node)
        for key, key_type in batch:
            pipe.memory_usage(key, samples=samples)
            pipe.object("ENCODING", key)
            if key_type in _LENGTHS:
                getattr(pipe, _LENGTHS[key_type][0])(key)
        replies = iter(_replies(pipe, node))
        for key, key_type in batch:
            size, encoding = next(replies), next(replies)
            length = next(replies) if key_type in _LENGTHS else None
            if any(isinstance(reply, AskError) for reply in (size, encoding, length)):
                # a redirect: the key's slot has moved to another node
                sizes.append(None)
                continue
            for reply in (size, encoding):
                if isinstance(reply, Exception):
                    raise reply
            if isinstance(length, Exception):
                if not str(length).startswith("WRONGTYPE"):
                    raise length
                length = None
            if size is None:
                sizes.append(None)
                continue
            encoding = text(encoding) if encoding is not None else None
            described: Dict[str, Any] = {"bytes": size}
            if length is not None:
                if (
                    key_type != "string"
                    and length > samples
                    and encoding not in _COMPACT_ENCODINGS
                ):
                    described["estimated"] = True
                described["length"] = length
                described["lengthUnit"] = _LENGTHS[key_type][1]
            described["encoding"] = encoding
            sizes.append(described)
    return sizes


@redis_tool("core")
async def big_keys(
    pattern: str,
    node: Optional[str] = None,
    cursor: Union[int, str] = 0,
    top: int = 10,
    min_bytes: int = 0,
) -> Union[str, Dict[str, Any]]:
    """
    Find the keys that take the most memory among the keys with a prefix: SCAN the keyspace and measure every
    key that matches the pattern with MEMORY USAGE, without reading values (non-blocking, as redis-cli --memkeys
    does).

    ⚠️  A KEY PREFIX IS REQUIRED: pattern must start with one, e.g. "session:*". If the user has not named the
    prefix to measure, ask them for it before calling. Do not guess a prefix, and do not try a pattern that
    matches every key: a pattern that starts with *, ? or [ is rejected.

    ⚠️  SCAN ONLY WHERE THE USER ASKED: the cluster they named and, on a Redis Cluster, the node they named.
    If either is missing, ask the user; do not try the clusters or the nodes one after another.

    ⚠️  IMPORTANT: One call scans for about 5 s and returns PARTIAL results: the largest keys among those it
    scanned, and a cursor. Call it again with the returned cursor until cursor becomes 0 (scanComplete is true),
    and keep the largest keys of every call. SCAN walks the whole keyspace whatever the pattern (the prefix
    saves measuring the other keys), so on a database with many millions of keys a scan still takes several
    calls.

    The keys are ranked by a quick estimate (5 elements of a hash, list, set, sorted set or stream, scaled to
    its length), so a key whose size lies in a few very large elements among many small ones can be ranked too
    low and missed. The keys that are listed are measured again with 1000 elements.

    Args:
        pattern: Pattern of the keys to measure. Required; it starts with the literal characters of a key
                 prefix and may go on with glob characters: "session:*", "user:42:*", "cache:*:html".
        node: REQUIRED on a Redis Cluster: the one node to scan, as host:port. The keys of a Redis Cluster are
              spread over its primaries, and a replica holds the keys of its primary, so scanning the replica
              keeps the load off the primary. A call without it is rejected with the list of the cluster's
              nodes. Leave it out on any other cluster.
        cursor: 0 to start from the beginning. To continue scanning, pass the cursor value returned from the
                previous call unchanged, with the same node.
        top: How many of the largest keys to return (default 10, at most 100).
        min_bytes: Leave out keys that take fewer bytes than this (default 0).

    Returns:
        A dictionary containing:
        - 'cursor': Next cursor position (0 means scan is complete)
        - 'scanComplete': Boolean indicating if scan is finished
        - 'keysScanned': Number of keys this call measured
        - 'dbsize': Number of keys on the scanned server, whatever the pattern
        - 'node': The node that was scanned (on a Redis Cluster)
        - 'keys': The largest keys this call measured, largest first: 'key', 'type', 'bytes' (memory the key
          and its value take), 'estimated' (true when 'bytes' is an estimate: the key has more than 1000
          elements), 'length' with 'lengthUnit' (bytes of a string, fields of a hash, items of a list, members
          of a set or sorted set, entries of a stream) and 'encoding'
        - 'byType': For each type, the number of keys this call measured and their bytes in total, from the
          quick estimates
        Or an error message if something goes wrong.

    A key name that is not UTF-8 is shown with \\xNN for those bytes and marked 'nonUtf8Key'; the other tools
    cannot read such a key by name.
    """
    if not pattern or pattern[0] in "*?[":
        return (
            f"Error: pattern must start with a key prefix, for example session:* (it is {pattern!r})."
            " Ask the user which prefix to measure."
        )
    if not 1 <= top <= MAX_BIG_KEYS:
        return f"Error: top must be between 1 and {MAX_BIG_KEYS}"
    if min_bytes < 0:
        return "Error: min_bytes must not be negative"
    try:
        server, scanned_node = _scan_server(node)
        on_node = scanned_node is not None
        position = _cursor_number(cursor, "big_keys")
        deadline = time.monotonic() + BIG_KEYS_SECONDS
        scanned = 0
        by_type: Dict[str, Dict[str, int]] = {}
        largest: Dict[Any, Tuple[int, str]] = {}
        while True:
            position, keys = server.scan(
                cursor=position, match=pattern, count=BIG_KEYS_SCAN_COUNT
            )
            for key, key_type, size in _measure(server, keys, on_node):
                scanned += 1
                totals = by_type.setdefault(key_type, {"keys": 0, "bytes": 0})
                totals["keys"] += 1
                totals["bytes"] += size
                if size >= min_bytes:
                    largest[key] = (size, key_type)
            largest = dict(
                heapq.nlargest(top, largest.items(), key=lambda item: item[1][0])
            )
            if not position or time.monotonic() >= deadline:
                break
        # the quick estimate ranked them; now measure the few that are listed with more samples
        listed = [(key, key_type) for key, (_, key_type) in largest.items()]
        sizes = _sizes(server, listed, DEFAULT_SAMPLES, on_node)
        entries = []
        for (key, key_type), size in zip(listed, sizes):
            # None: gone since it was scanned
            if size is None or size["bytes"] < min_bytes:
                continue
            entry = {"key": text(key), "type": key_type, **size}
            if not _is_utf8(key):
                entry["nonUtf8Key"] = True
            entries.append(entry)
        entries.sort(key=lambda entry: entry["bytes"], reverse=True)
        result: Dict[str, Any] = {
            "cursor": position,
            "scanComplete": not position,
            "keysScanned": scanned,
            "dbsize": server.dbsize(),
        }
        if on_node:
            result["node"] = scanned_node
        result["keys"] = entries
        result["byType"] = dict(
            sorted(by_type.items(), key=lambda item: item[1]["bytes"], reverse=True)
        )
        return result
    except (_CursorError, _NodeError) as e:
        return f"Error: {e}"
    except RedisError as e:
        return f"Error measuring keys with pattern '{pattern}': {str(e)}"


def _measure(server: Redis, keys: List[Any], node: bool) -> List[Tuple[Any, str, int]]:
    """(key, type, bytes) of the keys that are still on the server; ``node``: see _pipeline."""
    if not keys:
        return []
    pipe = _pipeline(server, node)
    for key in keys:
        pipe.type(key)
        pipe.memory_usage(key, samples=SCAN_SAMPLES)
    replies = _replies(pipe, node)
    measured = []
    for key, key_type, size in zip(keys, replies[::2], replies[1::2]):
        # a redirect: the key's slot has moved to another node since SCAN returned it
        if isinstance(key_type, AskError) or isinstance(size, AskError):
            continue
        for reply in (key_type, size):
            if isinstance(reply, Exception):
                raise reply
        # None: expired or deleted since SCAN returned it
        if size is not None:
            measured.append((key, text(key_type), size))
    return measured


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
