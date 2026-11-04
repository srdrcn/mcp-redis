"""The value tools on a real redis-server holding binary values, called through FastMCP (so the output schema
of each tool must accept the descriptions)."""

import asyncio
import gzip
import json

import pytest

from src.common.config_loader import parse_config, set_config
from src.common.server import build_server
from tests import redis_servers
from tests.test_values import SESSION_ID, aspnet_session

BLOB = b"\x00\x8e\xffabc"
# 600 KB each when unpacked: one call may unpack 1 MB in total, so only the first of two
PACKED = gzip.compress(b"x" * 600 * 1024)


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    redis_servers.skip_unless_installed()
    with redis_servers.running(tmp_path_factory.mktemp("binary")) as redis_server:
        client = redis_server.client(decode_responses=False)
        try:
            client.hset(
                "Florya_Session_x",
                mapping={
                    "absexp": "-1",
                    "sldexp": "18000000000",
                    "data": aspnet_session(
                        ("UserName", "ayşe".encode()),
                        ("LoginCount", (5).to_bytes(4, "big")),
                    ),
                },
            )
            client.set("blob", BLOB)
            client.set("text", "çağrı")
            client.rpush("list", "a", BLOB)
            client.sadd("set", BLOB)
            client.zadd("zset", {BLOB: 1.5})
            client.xadd("stream", {"payload": BLOB})
            client.rpush("packed-list", PACKED, PACKED)
            client.hset("packed-hash", mapping={"a": PACKED, "b": PACKED})
        finally:
            client.close()
        yield redis_server


@pytest.fixture
def mcp(server):
    config = parse_config(
        {
            "toolsets": ["data"],
            "clusters": [{"name": "local", "hosts": server.address}],
        }
    )
    set_config(config)
    return build_server(config)


def _call(mcp, tool, /, **arguments):
    content, structured = asyncio.run(mcp.call_tool(tool, arguments))
    return structured["result"]


def test_hget_unpacks_an_aspnet_session(mcp):
    result = _call(mcp, "hget", name="Florya_Session_x", key="data")

    assert result["format"] == "aspnet-core-session"
    assert result["sessionId"] == str(SESSION_ID)
    assert result["entries"] == {"UserName": "ayşe", "LoginCount": {"int32": 5}}


def test_hget_text_field(mcp):
    assert _call(mcp, "hget", name="Florya_Session_x", key="sldexp") == "18000000000"


def test_hget_missing_field(mcp):
    assert (
        _call(mcp, "hget", name="Florya_Session_x", key="nope")
        == "Field 'nope' not found in hash 'Florya_Session_x'."
    )


def test_hgetall(mcp):
    result = _call(mcp, "hgetall", name="Florya_Session_x")

    assert result["absexp"] == "-1"
    assert result["data"]["format"] == "aspnet-core-session"


def test_get_binary_and_text(mcp):
    blob = _call(mcp, "get", key="blob")

    assert blob["binary"] is True
    assert blob["head"] == BLOB.hex(" ")
    assert _call(mcp, "get", key="text") == "çağrı"


def test_lrange(mcp):
    values = json.loads(_call(mcp, "lrange", name="list", start=0, stop=-1))

    assert values[0] == "a"
    assert values[1]["head"] == BLOB.hex(" ")


def test_smembers(mcp):
    (member,) = _call(mcp, "smembers", name="set")

    assert member["head"] == BLOB.hex(" ")


def test_zrange_and_xrange(mcp):
    assert "'binary': True" in _call(
        mcp, "zrange", key="zset", start=0, end=-1, with_scores=True
    )
    assert "1.5" in _call(mcp, "zrange", key="zset", start=0, end=-1, with_scores=True)
    entries = _call(mcp, "xrange", key="stream", count=10)
    assert "'payload': {'binary': True" in entries


def test_one_call_unpacks_at_most_1_mb(mcp):
    first, second = json.loads(
        _call(mcp, "lrange", name="packed-list", start=0, stop=-1)
    )

    assert len(first["unpacked"]) == 600 * 1024
    assert "unpacked" not in second
    assert second["note"].startswith("not unpacked")

    fields = _call(mcp, "hgetall", name="packed-hash")
    assert sum("unpacked" in v for v in fields.values()) == 1
