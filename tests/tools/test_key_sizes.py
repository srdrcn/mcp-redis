"""memory_usage and big_keys on a real redis-server and a real Redis Cluster, called through FastMCP (so the
output schema of each tool must accept what it returns)."""

import asyncio
import shlex
from pathlib import Path

import pytest
from mcp.server.fastmcp.exceptions import ToolError

import src.tools.misc as misc
from src.common.config_loader import parse_config, resolve_passwords, set_config
from src.common.server import build_server
from tests import redis_servers

README = Path(__file__).resolve().parents[2] / "README.md"

SMALL_KEYS = 60
# small:*, big:string, big:hash, list, set, zset, stream, expiring, uneven and the key with the odd name
ALL_KEYS = SMALL_KEYS + 9


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    redis_servers.skip_unless_installed()
    with redis_servers.running(tmp_path_factory.mktemp("sizes")) as redis_server:
        client = redis_server.client(decode_responses=False)
        try:
            pipe = client.pipeline(transaction=False)
            for i in range(SMALL_KEYS):
                pipe.set(f"small:{i}", "x" * 10)
            pipe.set("big:string", "z" * 300_000)
            pipe.hset("big:hash", mapping={f"f{i}": "v" * 20 for i in range(2000)})
            pipe.rpush("list", *["item"] * 7)
            pipe.sadd("set", *range(40))
            pipe.zadd("zset", {f"m{i}": i for i in range(30)})
            pipe.xadd("stream", {"a": "1"})
            pipe.xadd("stream", {"a": "2"})
            pipe.setex("expiring", 500, "soon")
            # 10 of its 1000 fields hold 100 KB each: the first 5 seldom include one
            pipe.hset("uneven", mapping={f"f{i}": "s" for i in range(990)})
            pipe.hset("uneven", mapping={f"L{i}": "L" * 100_000 for i in range(10)})
            pipe.set(b"odd:\x8e", b"q" * 50_000)
            pipe.execute()
        finally:
            client.close()
        yield redis_server


def _server(clusters):
    config = parse_config({"toolsets": ["core"], "clusters": clusters})
    resolve_passwords(config)
    set_config(config)
    return build_server(config)


@pytest.fixture
def mcp(server):
    return _server([{"name": "local", "hosts": server.address}])


def _call(mcp, tool, /, **arguments):
    content, structured = asyncio.run(mcp.call_tool(tool, arguments))
    return structured["result"]


def test_memory_usage_of_a_string(mcp):
    result = _call(mcp, "memory_usage", key="big:string")

    assert result["bytes"] >= 300_000
    del result["bytes"]
    assert result == {
        "key": "big:string",
        "type": "string",
        "length": 300_000,
        "lengthUnit": "bytes",
        "encoding": "raw",
        "ttl": -1,
    }


@pytest.mark.parametrize(
    "key, key_type, length, unit, estimated",
    [
        # 2000 fields in a hash table: more than the 1000 that are measured
        ("big:hash", "hash", 2000, "fields", True),
        ("list", "list", 7, "items", False),
        ("set", "set", 40, "members", False),
        ("zset", "zset", 30, "members", False),
        ("stream", "stream", 2, "entries", False),
    ],
)
def test_memory_usage_of_a_collection(mcp, key, key_type, length, unit, estimated):
    result = _call(mcp, "memory_usage", key=key)

    assert (result["type"], result["length"], result["lengthUnit"]) == (
        key_type,
        length,
        unit,
    )
    assert result.get("estimated", False) is estimated
    assert result["bytes"] > 0
    assert result["encoding"]
    assert result["ttl"] == -1


def test_memory_usage_of_a_key_that_expires(mcp):
    result = _call(mcp, "memory_usage", key="expiring")

    assert 0 < result["ttl"] <= 500
    assert (result["length"], result["lengthUnit"]) == (4, "bytes")


def test_memory_usage_of_a_missing_key(mcp):
    assert _call(mcp, "memory_usage", key="nope") == {"key": "nope", "type": "none"}


def test_memory_usage_measures_up_to_1000_elements(mcp):
    """Every field of the uneven hash is measured, so its size is that of all of them."""
    first = _call(mcp, "memory_usage", key="uneven")
    second = _call(mcp, "memory_usage", key="uneven")

    assert first == second
    assert 1_000_000 < first["bytes"] < 1_300_000
    assert first["length"] == 1000
    assert "estimated" not in first


def test_memory_usage_with_fewer_samples_than_elements_is_an_estimate(mcp):
    """MEMORY USAGE scales the size of the first elements it comes to: with 5 samples the uneven hash is some
    KB when none of its 10 large fields is among them, and 20 MB or more when one is."""
    few = _call(mcp, "memory_usage", key="uneven", samples=5)
    even = _call(mcp, "memory_usage", key="big:hash", samples=5)
    whole = _call(mcp, "memory_usage", key="big:hash", samples=2000)

    assert few["estimated"] is True
    assert not 1_000_000 < few["bytes"] < 1_300_000
    # fields of about the same size: the estimate is close
    assert even["estimated"] is True
    assert "estimated" not in whole
    assert abs(even["bytes"] - whole["bytes"]) < whole["bytes"] * 0.1


def test_memory_usage_rejects_samples_out_of_range(mcp):
    assert _call(mcp, "memory_usage", key="big:hash", samples=0) == {
        "error": "samples must be between 1 and 10000"
    }


def test_big_keys_lists_the_largest_first(mcp):
    result = _call(mcp, "big_keys", pattern="big:*")

    string, big_hash = result["keys"]
    assert (string["key"], string["type"]) == ("big:string", "string")
    assert (string["length"], string["lengthUnit"]) == (300_000, "bytes")
    assert "estimated" not in string
    assert (big_hash["key"], big_hash["type"]) == ("big:hash", "hash")
    assert (big_hash["length"], big_hash["lengthUnit"]) == (2000, "fields")
    assert (big_hash["encoding"], big_hash["estimated"]) == ("hashtable", True)
    assert string["bytes"] > big_hash["bytes"] > 2000 * 20
    assert (result["cursor"], result["scanComplete"]) == (0, True)
    assert result["keysScanned"] == 2
    assert result["dbsize"] == ALL_KEYS
    assert list(result["byType"]) == ["string", "hash"]
    assert result["byType"]["string"] == {"keys": 1, "bytes": string["bytes"]}
    assert result["byType"]["hash"]["keys"] == 1


def test_big_keys_measures_a_listed_key_again(mcp):
    """The scan measures 5 fields of the uneven hash, which makes it some KB or 20 MB and more (the total of
    its type); the key that is listed has the size of its 1000 fields."""
    result = _call(mcp, "big_keys", pattern="uneven")

    (entry,) = result["keys"]
    assert 1_000_000 < entry["bytes"] < 1_300_000
    assert "estimated" not in entry
    assert entry == {
        k: v for k, v in _call(mcp, "memory_usage", key="uneven").items() if k != "ttl"
    }
    assert not 1_000_000 < result["byType"]["hash"]["bytes"] < 1_300_000


def test_big_keys_agrees_with_memory_usage(mcp):
    (entry,) = _call(mcp, "big_keys", pattern="big:string")["keys"]
    single = _call(mcp, "memory_usage", key="big:string")

    assert entry == {k: single[k] for k in entry}


def test_big_keys_measures_every_key_with_the_prefix(mcp):
    result = _call(mcp, "big_keys", pattern="small:*", top=100)

    assert result["scanComplete"] is True
    assert result["keysScanned"] == SMALL_KEYS == len(result["keys"])
    assert result["dbsize"] == ALL_KEYS
    sizes = [entry["bytes"] for entry in result["keys"]]
    assert sizes == sorted(sizes, reverse=True)
    # strings are measured exactly, so the sizes of the scan add up to those of the list
    assert result["byType"] == {"string": {"keys": SMALL_KEYS, "bytes": sum(sizes)}}


def test_big_keys_requires_a_whole_prefix(mcp):
    """pattern has no default, and a pattern that could match most keys is turned down."""
    (tool,) = [t for t in asyncio.run(mcp.list_tools()) if t.name == "big_keys"]
    assert tool.inputSchema["required"] == ["pattern"]
    assert "default" not in tool.inputSchema["properties"]["pattern"]

    with pytest.raises(ToolError, match="pattern"):
        asyncio.run(mcp.call_tool("big_keys", {}))
    for pattern in ("*", "s*", "big*", "*:string", "?ig:*", "[b]ig:*", ""):
        assert _call(mcp, "big_keys", pattern=pattern) == (
            "Error: pattern must start with a whole key prefix, up to a separator, for example session:*"
            f" (it is {pattern!r}). Ask the user which prefix to measure."
        )


def test_big_keys_top_and_min_bytes(mcp):
    top = _call(mcp, "big_keys", pattern="big:*", top=1)
    large = _call(mcp, "big_keys", pattern="big:*", min_bytes=200_000)
    small = _call(mcp, "big_keys", pattern="small:*", top=3)

    assert [entry["key"] for entry in top["keys"]] == ["big:string"]
    # also matches big:hash, which is smaller
    assert [entry["key"] for entry in large["keys"]] == ["big:string"]
    assert large["keysScanned"] == 2
    assert small["keysScanned"] == SMALL_KEYS
    assert len(small["keys"]) == 3
    assert small["byType"]["string"]["keys"] == SMALL_KEYS


def test_big_keys_marks_a_key_name_that_is_not_utf8(mcp):
    (entry,) = _call(mcp, "big_keys", pattern="odd:*")["keys"]

    assert entry["bytes"] >= 50_000
    del entry["bytes"]
    assert entry == {
        "key": "odd:\\x8e",
        "type": "string",
        "length": 50_000,
        "lengthUnit": "bytes",
        "encoding": "raw",
        "nonUtf8Key": True,
    }


def test_big_keys_rejects_an_invalid_cursor(mcp):
    assert _call(mcp, "big_keys", pattern="big:*", cursor="abc") == (
        "Error: invalid cursor: pass the cursor returned by the previous big_keys call unchanged, or 0 to"
        " start"
    )


def _all_pages(mcp, **arguments):
    """Calls big_keys with the cursor it returns until the scan is complete; the pages."""
    pages, cursor = [], 0
    for _ in range(500):
        page = _call(mcp, "big_keys", cursor=cursor, **arguments)
        assert isinstance(page, dict), page
        pages.append(page)
        cursor = page["cursor"]
        if page["scanComplete"]:
            return pages
    raise AssertionError("big_keys did not finish")


@pytest.fixture
def short_calls(monkeypatch):
    """A call ends after one round of small SCAN steps, as when its time is up."""
    monkeypatch.setattr(misc, "BIG_KEYS_SECONDS", 0)
    monkeypatch.setattr(misc, "BIG_KEYS_SCAN_COUNT", 10)


def test_big_keys_pages_with_the_cursor(mcp, short_calls):
    pages = _all_pages(mcp, pattern="small:*", top=2)

    assert len(pages) > 2
    assert all(isinstance(page["cursor"], int) for page in pages)
    assert all(page["cursor"] != 0 for page in pages[:-1])
    assert pages[-1]["cursor"] == 0
    assert all(len(page["keys"]) <= 2 for page in pages)
    # every key with the prefix is measured once: the keyspace does not change during the scan
    assert sum(page["keysScanned"] for page in pages) == SMALL_KEYS
    listed = [entry["key"] for page in pages for entry in page["keys"]]
    assert listed and all(key.startswith("small:") for key in listed)


def _readme_acl_rule(user):
    """The ``ACL SETUSER <user> ...`` command of the README, as its arguments."""
    (line,) = [
        line
        for line in README.read_text(encoding="utf-8").splitlines()
        if line.startswith(f"ACL SETUSER {user} ")
    ]
    return shlex.split(line)


@pytest.fixture
def acl_users(server, monkeypatch):
    """The README's mcp-reader, and a user that may not run MEMORY USAGE; both with the password "pw"."""
    if redis_servers.server_version() < (7, 0):
        pytest.skip("the ACL rules of the README are checked on Redis 7.0 and later")
    reader = [
        ">pw" if rule == ">REPLACE_ME" else rule
        for rule in _readme_acl_rule("mcp-reader")
    ]
    assert ">pw" in reader
    client = server.client()
    try:
        client.execute_command(*reader)
        client.execute_command(
            "ACL", "SETUSER", "no-memory", "on", ">pw", "~*", "+@read", "-memory"
        )
    finally:
        client.close()
    monkeypatch.setenv("reader_pass", "pw")
    monkeypatch.setenv("limited_pass", "pw")
    return _server(
        [
            {"name": "reader", "hosts": server.address, "username": "mcp-reader"},
            {"name": "limited", "hosts": server.address, "username": "no-memory"},
        ]
    )


def test_the_readme_acl_user_can_run_both_tools(acl_users):
    single = _call(acl_users, "memory_usage", cluster="reader", key="big:hash")
    scan = _call(acl_users, "big_keys", cluster="reader", pattern="big:*")

    assert (single["length"], single["encoding"]) == (2000, "hashtable")
    assert [entry["key"] for entry in scan["keys"]] == ["big:string", "big:hash"]
    assert scan["dbsize"] == ALL_KEYS


def test_a_user_without_memory_usage_gets_the_error(acl_users):
    """Not an empty result: the call names the command the user may not run."""
    single = _call(acl_users, "memory_usage", cluster="limited", key="big:hash")
    scan = _call(acl_users, "big_keys", cluster="limited", pattern="big:*")

    assert list(single) == ["error"]
    assert "memory|usage" in single["error"]
    assert scan.startswith("Error measuring keys with pattern 'big:*': ")
    assert "memory|usage" in scan


CLUSTER_KEYS = 90


@pytest.fixture(scope="module")
def redis_cluster(tmp_path_factory):
    """Three primaries with a replica each."""
    redis_servers.skip_unless_installed(cluster=True)
    with redis_servers.running_cluster(
        tmp_path_factory.mktemp("sizes-rc"), replicas=1
    ) as nodes:
        client = nodes.client()
        try:
            for i in range(CLUSTER_KEYS - 2):
                client.set(f"small:{i}", "x" * 10)
            client.set("big:string", "z" * 300_000)
            client.hset("big:hash", mapping={f"f{i}": "v" * 20 for i in range(2000)})
        finally:
            client.close()
        nodes.wait_for_replicas()
        yield nodes


@pytest.fixture
def cluster_mcp(redis_cluster):
    # two of the six nodes as startup nodes: the client discovers the others
    return _server(
        [{"name": "rc", "mode": "cluster", "hosts": redis_cluster.addresses[:2]}]
    )


@pytest.fixture
def primaries(redis_cluster):
    return [node.address for node in redis_cluster.roles()["master"]]


@pytest.fixture
def replicas(redis_cluster):
    return [node.address for node in redis_cluster.roles()["slave"]]


def _keys_of(node_address):
    """The keys a node holds, read from the node itself (a replica answers KEYS without READONLY)."""
    client = redis_servers._client(int(node_address.rpartition(":")[2]), None)
    try:
        return sorted(client.keys("*"))
    finally:
        client.close()


def test_memory_usage_on_a_redis_cluster(cluster_mcp, redis_cluster):
    """One key needs no node: it is measured on the node of its slot."""
    keys = ["big:string", "big:hash", *(f"small:{i}" for i in range(20))]
    assert len({redis_cluster.node_for_key(key).port for key in keys}) == 3

    results = {key: _call(cluster_mcp, "memory_usage", key=key) for key in keys}

    assert results["big:string"]["length"] == 300_000
    assert results["big:hash"]["lengthUnit"] == "fields"
    assert all(results[f"small:{i}"]["length"] == 10 for i in range(20))
    assert _call(cluster_mcp, "memory_usage", key="nope") == {
        "key": "nope",
        "type": "none",
    }


@pytest.mark.parametrize("tool", ["scan_keys", "big_keys"])
def test_a_scan_of_a_redis_cluster_needs_a_node(cluster_mcp, primaries, replicas, tool):
    """Without a node nothing is scanned, and the error lists every node with its role."""
    result = _call(cluster_mcp, tool, pattern="small:*")

    assert result.startswith(
        "Error: cluster rc is a Redis Cluster: node is required, the one node to scan. Its nodes, as"
        " host:port: "
    )
    assert result.endswith(". Ask the user which node to scan.")
    for primary in primaries:
        assert f"{primary} (primary; replicas: " in result
    assert all(replica in result for replica in replicas)
    # node is optional in the schema: the other modes take none
    (listed,) = [t for t in asyncio.run(cluster_mcp.list_tools()) if t.name == tool]
    assert "node" not in listed.inputSchema.get("required", [])


@pytest.mark.parametrize("tool", ["scan_keys", "big_keys"])
def test_a_scan_rejects_a_node_the_redis_cluster_does_not_have(cluster_mcp, tool):
    result = _call(cluster_mcp, tool, pattern="small:*", node="127.0.0.1:1")

    assert result.startswith(
        "Error: node 127.0.0.1:1 is not a node of the Redis Cluster rc. Its nodes, as host:port: "
    )


@pytest.mark.parametrize("tool", ["scan_keys", "big_keys"])
def test_a_scan_rejects_a_node_on_a_standalone_cluster(mcp, server, tool):
    result = _call(mcp, tool, pattern="small:*", node=server.address)

    assert result == (
        "Error: cluster local is not a Redis Cluster (mode standalone): it has one server, so leave node out"
    )


def test_big_keys_on_a_redis_cluster_measures_only_the_named_node(
    cluster_mcp, primaries
):
    """Each primary holds its own share of the keys; together they hold all of them."""
    measured = {}
    for primary in primaries:
        result = _call(
            cluster_mcp, "big_keys", pattern="small:*", node=primary, top=100
        )
        own = [key for key in _keys_of(primary) if key.startswith("small:")]

        assert (result["cursor"], result["scanComplete"]) == (0, True)
        assert result["node"] == primary
        assert sorted(entry["key"] for entry in result["keys"]) == own
        assert result["keysScanned"] == len(own) > 0
        assert result["dbsize"] == len(_keys_of(primary))
        measured[primary] = set(own)

    assert sum(len(keys) for keys in measured.values()) == CLUSTER_KEYS - 2
    assert len(set.union(*measured.values())) == CLUSTER_KEYS - 2


def test_big_keys_on_a_replica_measures_the_keys_of_its_primary(
    cluster_mcp, primaries, replicas
):
    """The replica answers the commands on its keys (READONLY) instead of redirecting them to the primary."""
    primary_of = {tuple(_keys_of(primary)): primary for primary in primaries}
    for replica in replicas:
        primary = primary_of[tuple(_keys_of(replica))]

        on_replica = _call(
            cluster_mcp, "big_keys", pattern="small:*", node=replica, top=100
        )
        on_primary = _call(
            cluster_mcp, "big_keys", pattern="small:*", node=primary, top=100
        )

        assert on_replica["node"] == replica
        assert on_replica["scanComplete"] is True
        assert on_replica["keysScanned"] == on_primary["keysScanned"] > 0
        assert sorted(e["key"] for e in on_replica["keys"]) == sorted(
            e["key"] for e in on_primary["keys"]
        )
        assert on_replica["dbsize"] == on_primary["dbsize"]

    big = [
        entry
        for replica in replicas
        for entry in _call(cluster_mcp, "big_keys", pattern="big:*", node=replica)[
            "keys"
        ]
    ]
    assert sorted((e["key"], e["length"]) for e in big) == [
        ("big:hash", 2000),
        ("big:string", 300_000),
    ]


def test_the_readme_acl_user_can_scan_a_replica(redis_cluster, replicas, monkeypatch):
    """mcp-reader may send READONLY (+@connection), so a replica answers its TYPE and MEMORY USAGE."""
    if redis_servers.server_version() < (7, 0):
        pytest.skip("the ACL rules of the README are checked on Redis 7.0 and later")
    rule = [
        ">pw" if part == ">REPLACE_ME" else part
        for part in _readme_acl_rule("mcp-reader")
    ]
    # ACL users are per node
    for node in redis_cluster.nodes:
        client = node.client()
        try:
            client.execute_command(*rule)
        finally:
            client.close()
    monkeypatch.setenv("rc-reader_pass", "pw")
    mcp = _server(
        [
            {
                "name": "rc-reader",
                "mode": "cluster",
                "hosts": redis_cluster.addresses,
                "username": "mcp-reader",
            }
        ]
    )

    scan = _call(mcp, "big_keys", pattern="small:*", node=replicas[0], top=100)
    page = _call(mcp, "scan_keys", pattern="small:*", node=replicas[0], count=1000)

    own = [key for key in _keys_of(replicas[0]) if key.startswith("small:")]
    assert scan["node"] == page["node"] == replicas[0]
    assert sorted(entry["key"] for entry in scan["keys"]) == own
    assert own and scan["keysScanned"] == len(own)


def test_scans_accept_a_host_name_for_a_node(cluster_mcp, primaries):
    """The cluster names its nodes by IP address (127.0.0.1); localhost resolves to it."""
    port = primaries[0].rpartition(":")[2]

    page = _call(cluster_mcp, "scan_keys", pattern="small:*", node=f"localhost:{port}")
    sizes = _call(cluster_mcp, "big_keys", pattern="small:*", node=f"LOCALHOST:{port}")

    assert page["node"] == sizes["node"] == primaries[0]


def test_scan_keys_on_a_redis_cluster_pages_through_one_node(cluster_mcp, replicas):
    replica = replicas[0]
    seen, cursor = [], 0
    for _ in range(500):
        page = _call(
            cluster_mcp,
            "scan_keys",
            pattern="small:*",
            count=5,
            cursor=cursor,
            node=replica,
        )
        assert isinstance(page, dict), page
        assert page["node"] == replica
        seen.extend(page["keys"])
        cursor = page["cursor"]
        assert isinstance(cursor, int)
        if page["scan_complete"]:
            break
    else:
        raise AssertionError("scan_keys did not finish")

    assert sorted(seen) == [k for k in _keys_of(replica) if k.startswith("small:")]
    assert 0 < len(seen) < CLUSTER_KEYS


def test_scan_keys_without_a_whole_prefix_is_one_sample_page(mcp):
    """Key names to look at, and no way to walk the keyspace with them."""
    sample = _call(mcp, "scan_keys", pattern="*", count=10)
    again = _call(mcp, "scan_keys", pattern="s*", count=10)

    assert sample["sample"] is True and again["sample"] is True
    assert "cursor" not in sample
    assert sample["scan_complete"] is False
    assert 0 < len(sample["keys"]) < ALL_KEYS
    for cursor in (1, 8, "24"):
        assert _call(mcp, "scan_keys", pattern="*", count=10, cursor=cursor) == (
            "Error: pattern '*' does not start with a whole key prefix, so it returns one sample page and"
            " cannot be continued. To page, use a pattern that starts with a key prefix up to a separator,"
            " for example session:*. Ask the user which prefix to scan."
        )
    assert _call(mcp, "scan_keys", pattern="small:*", count=1001) == (
        "Error: count must be between 1 and 1000"
    )


def test_scan_keys_with_a_whole_prefix_pages_to_the_end(mcp):
    seen, cursor = [], 0
    for _ in range(500):
        page = _call(mcp, "scan_keys", pattern="small:*", count=10, cursor=cursor)
        assert "sample" not in page
        seen.extend(page["keys"])
        cursor = page["cursor"]
        if page["scan_complete"]:
            break
    else:
        raise AssertionError("scan_keys did not finish")

    assert sorted(seen) == sorted(f"small:{i}" for i in range(SMALL_KEYS))


def test_key_prefixes_of_a_server(mcp):
    """Fewer keys than one step holds: the sample is every key, and no key name comes back."""
    result = _call(mcp, "key_prefixes")

    assert result == {
        "sampledKeys": ALL_KEYS,
        "dbsize": ALL_KEYS,
        "scanComplete": True,
        "prefixes": [
            {"pattern": "small:*", "keys": SMALL_KEYS},
            {"pattern": "big:hash", "keys": 1},
            {"pattern": "big:string", "keys": 1},
            {"pattern": "expiring", "keys": 1},
            {"pattern": "list", "keys": 1},
            # the key whose name is not UTF-8
            {"pattern": "odd:*", "keys": 1},
            {"pattern": "set", "keys": 1},
            {"pattern": "stream", "keys": 1},
            {"pattern": "uneven", "keys": 1},
            {"pattern": "zset", "keys": 1},
        ],
    }
    top = _call(mcp, "key_prefixes", top=1)
    assert top["prefixes"] == [{"pattern": "small:*", "keys": SMALL_KEYS}]
    assert top["otherPrefixes"] == {"patterns": 9, "keys": 9}


def test_key_prefixes_can_be_scanned(mcp):
    """A pattern it returns is a pattern the scans page with."""
    (pattern,) = [
        entry["pattern"]
        for entry in _call(mcp, "key_prefixes")["prefixes"]
        if entry["keys"] == SMALL_KEYS
    ]

    page = _call(mcp, "scan_keys", pattern=pattern, count=1000)
    sizes = _call(mcp, "big_keys", pattern=pattern, top=100)

    assert len(page["keys"]) == sizes["keysScanned"] == SMALL_KEYS


def test_key_prefixes_takes_one_sample_of_a_large_database(tmp_path):
    """Far more keys than one step holds: one SCAN and about 1000 names, whatever the size."""
    redis_servers.skip_unless_installed()
    with redis_servers.running(tmp_path) as large:
        client = large.client()
        try:
            pipe = client.pipeline(transaction=False)
            for i in range(30_000):
                pipe.set(f"session:{i}", "1")
                if i % 3 == 0:
                    pipe.set(f"user:{i}:profile", "1")
            pipe.execute()
            client.config_resetstat()
            mcp = _server([{"name": "large", "hosts": large.address}])

            result = _call(mcp, "key_prefixes")
            again = _call(mcp, "key_prefixes")

            scans = client.info("commandstats")["cmdstat_scan"]["calls"]
        finally:
            client.close()

    assert result == again
    assert scans == 2
    assert result["dbsize"] == 40_000
    assert result["scanComplete"] is False
    assert 800 < result["sampledKeys"] < 1300
    found = {entry["pattern"]: entry["keys"] for entry in result["prefixes"]}
    assert set(found) == {"session:*", "user:*:profile"}
    assert sum(found.values()) == result["sampledKeys"]
    # three keys in four are sessions
    assert 0.65 < found["session:*"] / result["sampledKeys"] < 0.85


def test_key_prefixes_on_a_redis_cluster_samples_the_named_node(
    cluster_mcp, primaries, replicas
):
    without_node = _call(cluster_mcp, "key_prefixes")
    assert without_node.startswith(
        "Error: cluster rc is a Redis Cluster: node is required, the one node to scan."
    )

    for node in (primaries[0], replicas[0]):
        result = _call(cluster_mcp, "key_prefixes", node=node)
        own = _keys_of(node)

        assert result["node"] == node
        assert result["sampledKeys"] == result["dbsize"] == len(own)
        assert result["scanComplete"] is True
        found = {entry["pattern"]: entry["keys"] for entry in result["prefixes"]}
        assert found["small:*"] == len([k for k in own if k.startswith("small:")])
        assert sum(found.values()) == len(own)


def test_big_keys_on_a_redis_cluster_pages_with_the_cursor(
    cluster_mcp, primaries, short_calls
):
    primary = primaries[0]
    own = [key for key in _keys_of(primary) if key.startswith("small:")]

    pages = _all_pages(cluster_mcp, pattern="small:*", node=primary, top=1)

    assert len(pages) > 1
    assert all(isinstance(page["cursor"], int) for page in pages)
    assert pages[-1]["cursor"] == 0
    assert all(page["node"] == primary for page in pages)
    assert sum(page["keysScanned"] for page in pages) == len(own)
    listed = [entry["key"] for page in pages for entry in page["keys"]]
    assert listed and set(listed) <= set(own)
