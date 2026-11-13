"""memory_usage and big_keys on a real redis-server and a real Redis Cluster, called through FastMCP (so the
output schema of each tool must accept what it returns)."""

import asyncio
import shlex
from pathlib import Path

import pytest

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
    "key, key_type, length, unit",
    [
        ("big:hash", "hash", 2000, "fields"),
        ("list", "list", 7, "items"),
        ("set", "set", 40, "members"),
        ("zset", "zset", 30, "members"),
        ("stream", "stream", 2, "entries"),
    ],
)
def test_memory_usage_of_a_collection(mcp, key, key_type, length, unit):
    result = _call(mcp, "memory_usage", key=key)

    assert (result["type"], result["length"], result["lengthUnit"]) == (
        key_type,
        length,
        unit,
    )
    assert result["bytes"] > 0
    assert result["encoding"]
    assert result["ttl"] == -1


def test_memory_usage_of_a_key_that_expires(mcp):
    result = _call(mcp, "memory_usage", key="expiring")

    assert 0 < result["ttl"] <= 500
    assert (result["length"], result["lengthUnit"]) == (4, "bytes")


def test_memory_usage_of_a_missing_key(mcp):
    assert _call(mcp, "memory_usage", key="nope") == {"key": "nope", "type": "none"}


def test_memory_usage_with_samples_for_every_element_is_exact(mcp):
    """MEMORY USAGE scales the size of the first elements it comes to: with 5 samples the uneven hash is some
    KB when none of its 10 large fields is among them, and about 20 MB when one is. With a sample for every
    field it is the size of all of them."""
    first = _call(mcp, "memory_usage", key="uneven", samples=1000)
    second = _call(mcp, "memory_usage", key="uneven", samples=1000)

    assert first == second
    assert 1_000_000 < first["bytes"] < 1_300_000
    assert first["length"] == 1000


def test_memory_usage_rejects_samples_out_of_range(mcp):
    assert _call(mcp, "memory_usage", key="big:hash", samples=0) == {
        "error": "samples must be between 1 and 1000"
    }


def test_big_keys_lists_the_largest_first(mcp):
    result = _call(mcp, "big_keys", pattern="big:*")

    string, big_hash = result["keys"]
    assert (string["key"], string["type"]) == ("big:string", "string")
    assert (string["length"], string["lengthUnit"]) == (300_000, "bytes")
    assert (big_hash["key"], big_hash["type"]) == ("big:hash", "hash")
    assert (big_hash["length"], big_hash["lengthUnit"]) == (2000, "fields")
    assert string["bytes"] > big_hash["bytes"] > 2000 * 20
    assert (result["cursor"], result["scanComplete"]) == (0, True)
    assert result["keysScanned"] == 2
    assert result["dbsize"] == ALL_KEYS
    assert result["byType"] == {
        "string": {"keys": 1, "bytes": string["bytes"]},
        "hash": {"keys": 1, "bytes": big_hash["bytes"]},
    }


def test_big_keys_agrees_with_memory_usage(mcp):
    (entry,) = _call(mcp, "big_keys", pattern="big:string")["keys"]
    single = _call(mcp, "memory_usage", key="big:string")

    assert entry == {k: single[k] for k in entry}


def test_big_keys_measures_the_whole_database(mcp):
    result = _call(mcp, "big_keys", top=100)

    assert result["scanComplete"] is True
    assert result["keysScanned"] == result["dbsize"] == ALL_KEYS == len(result["keys"])
    assert {t: totals["keys"] for t, totals in result["byType"].items()} == {
        "string": SMALL_KEYS + 3,
        "hash": 2,
        "list": 1,
        "set": 1,
        "zset": 1,
        "stream": 1,
    }
    sizes = [entry["bytes"] for entry in result["keys"]]
    assert sizes == sorted(sizes, reverse=True)
    assert sum(sizes) == sum(totals["bytes"] for totals in result["byType"].values())
    assert list(result["byType"].values()) == sorted(
        result["byType"].values(), key=lambda totals: totals["bytes"], reverse=True
    )


def test_big_keys_top_and_min_bytes(mcp):
    top = _call(mcp, "big_keys", pattern="big:*", top=1)
    large = _call(mcp, "big_keys", pattern="*i*", min_bytes=200_000)
    small = _call(mcp, "big_keys", pattern="small:*", top=3)

    assert [entry["key"] for entry in top["keys"]] == ["big:string"]
    # also matches big:hash, list and expiring, which are smaller
    assert [entry["key"] for entry in large["keys"]] == ["big:string"]
    assert large["keysScanned"] == 4
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
        "nonUtf8Key": True,
    }


def test_big_keys_rejects_an_invalid_cursor(mcp):
    assert _call(mcp, "big_keys", cursor="abc") == (
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
    pages = _all_pages(mcp, top=2)

    assert len(pages) > 2
    assert all(isinstance(page["cursor"], int) for page in pages)
    assert all(page["cursor"] != 0 for page in pages[:-1])
    assert pages[-1]["cursor"] == 0
    assert all(len(page["keys"]) <= 2 for page in pages)
    # every key is measured once: the keyspace does not change during the scan
    assert sum(page["keysScanned"] for page in pages) == ALL_KEYS
    largest = max(
        (entry for page in pages for entry in page["keys"]),
        key=lambda entry: entry["bytes"] if entry["key"] != "uneven" else 0,
    )
    assert largest["key"] == "big:string"


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
    redis_servers.skip_unless_installed(cluster=True)
    with redis_servers.running_cluster(tmp_path_factory.mktemp("sizes-rc")) as nodes:
        client = nodes.client()
        try:
            for i in range(CLUSTER_KEYS - 2):
                client.set(f"small:{i}", "x" * 10)
            client.set("big:string", "z" * 300_000)
            client.hset("big:hash", mapping={f"f{i}": "v" * 20 for i in range(2000)})
        finally:
            client.close()
        yield nodes


@pytest.fixture
def cluster_mcp(redis_cluster):
    return _server(
        [{"name": "rc", "mode": "cluster", "hosts": redis_cluster.addresses}]
    )


def test_memory_usage_on_a_redis_cluster(cluster_mcp, redis_cluster):
    """Each key is measured on the node of its slot."""
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


def test_big_keys_on_a_redis_cluster_measures_every_primary(cluster_mcp):
    result = _call(cluster_mcp, "big_keys", top=2)

    assert (result["cursor"], result["scanComplete"]) == (0, True)
    assert result["keysScanned"] == result["dbsize"] == CLUSTER_KEYS
    assert [(e["key"], e["length"]) for e in result["keys"]] == [
        ("big:string", 300_000),
        ("big:hash", 2000),
    ]
    assert result["byType"]["string"]["keys"] == CLUSTER_KEYS - 1


def test_big_keys_on_a_redis_cluster_pages_with_the_cursor(cluster_mcp, short_calls):
    pages = _all_pages(cluster_mcp, top=1)

    assert len(pages) > 1
    # text that holds the position of every primary that has not finished
    assert all(
        isinstance(page["cursor"], str) and "=" in page["cursor"] for page in pages[:-1]
    )
    assert pages[-1]["cursor"] == 0
    assert sum(page["keysScanned"] for page in pages) == CLUSTER_KEYS
    assert max(
        (entry for page in pages for entry in page["keys"]),
        key=lambda entry: entry["bytes"],
    )["key"] == ("big:string")
