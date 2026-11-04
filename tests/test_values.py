"""src.common.values: values that are not UTF-8 text are described instead of failing to decode."""

import gzip
import uuid

import pytest

from src.common import values
from src.common.values import readable, text

SESSION_ID = uuid.UUID("8e81c174-8094-fb59-2eb9-58438479aea4")


def _entry(key: str, value: bytes) -> bytes:
    key_bytes = key.encode()
    return (
        len(key_bytes).to_bytes(2, "big")
        + key_bytes
        + len(value).to_bytes(4, "big")
        + value
    )


def aspnet_session(*entries, revision: int = 2, count: int = None) -> bytes:
    """An ASP.NET Core DistributedSession payload with the given (key, bytes) entries."""
    return (
        bytes([revision])
        + (len(entries) if count is None else count).to_bytes(3, "big")
        + SESSION_ID.bytes_le
        + b"".join(_entry(k, v) for k, v in entries)
    )


class TestText:
    def test_utf8_is_returned_as_text(self):
        assert readable("çağrı: ok".encode()) == "çağrı: ok"

    def test_text_from_a_decoding_client_is_returned_as_it_is(self):
        assert readable("already text") == "already text"
        assert text("field") == "field"

    def test_empty(self):
        assert readable(b"") == ""

    def test_keys_and_ids_keep_every_byte(self):
        assert text(b"user:\x8e1") == "user:\\x8e1"


class TestBinary:
    def test_unknown_binary_is_described(self):
        value = b"\x00\x8eAB\xff"

        assert readable(value) == {
            "binary": True,
            "length": 5,
            "text": "\x00\\x8eAB\\xff",
            "head": "00 8e 41 42 ff",
        }

    def test_long_values_are_cut(self):
        value = b"\xff" + b"a" * (values.PREVIEW_BYTES + 10)

        result = readable(value)

        assert result["length"] == values.PREVIEW_BYTES + 11
        assert result["truncated"] is True
        assert result["text"] == "\\xff" + "a" * (values.PREVIEW_BYTES - 1)
        assert result["head"] == ("ff " + "61 " * (values.HEAD_BYTES - 1)).strip()

    def test_java_serialization_is_named(self):
        value = b"\xac\xed\x00\x05sr\x00\x11java.util.HashMap"

        result = readable(value)

        assert result["format"] == "java-serialization"
        assert "java.util.HashMap" in result["text"]

    def test_gzip_text_is_unpacked(self):
        value = gzip.compress('{"user": "ayşe"}'.encode())

        assert readable(value) == {
            "binary": True,
            "length": len(value),
            "format": "gzip",
            "unpacked": '{"user": "ayşe"}',
        }

    def test_gzip_binary_is_unpacked_and_described(self):
        result = readable(gzip.compress(b"\xff\xfe"))

        assert result["format"] == "gzip"
        assert result["unpacked"]["head"] == "ff fe"

    def test_gzip_over_the_limit_is_not_unpacked(self, monkeypatch):
        monkeypatch.setattr(values, "MAX_GUNZIP_BYTES", 10)
        value = gzip.compress(b"x" * 100)

        result = readable(value)

        assert "format" not in result
        assert result["head"].startswith("1f 8b")

    def test_gzip_followed_by_more_data_is_not_unpacked(self):
        result = readable(gzip.compress(b"x") + b"\xff more")

        assert "format" not in result

    def test_broken_gzip_is_described(self):
        result = readable(b"\x1f\x8b\x00\x00garbage\xff")

        assert "format" not in result
        assert result["head"].startswith("1f 8b")


class TestAspNetSession:
    def test_entries_are_unpacked(self):
        value = aspnet_session(
            ("UserName", "ayşe".encode()),
            ("Profile", b'{"role": "admin"}'),
            ("LoginCount", (5).to_bytes(4, "big")),
            ("Negative", (-2).to_bytes(4, "big", signed=True)),
            ("Blob", b"\x00\x8e\x01\x02\x03"),
            ("Empty", b""),
        )

        result = readable(value)

        assert result["binary"] is True
        assert result["length"] == len(value)
        assert result["format"] == "aspnet-core-session"
        assert result["sessionId"] == str(SESSION_ID)
        entries = result["entries"]
        assert entries["UserName"] == "ayşe"
        assert entries["Profile"] == '{"role": "admin"}'
        assert entries["LoginCount"] == {"int32": 5}
        assert entries["Negative"] == {"int32": -2}
        assert entries["Blob"]["head"] == "00 8e 01 02 03"
        assert entries["Empty"] == ""

    def test_an_empty_session(self):
        result = readable(aspnet_session())

        assert result["format"] == "aspnet-core-session"
        assert result["entries"] == {}

    @pytest.mark.parametrize(
        "value",
        [
            aspnet_session(("a", b"1")) + b"\x00",  # trailing bytes
            aspnet_session(("a", b"1"), revision=3),  # unknown revision
            aspnet_session(("a", b"1"), count=2),  # fewer entries than counted
            aspnet_session(("a", b"1"), ("a", b"2")),  # the same key twice
            aspnet_session(("a", b"1"))[:-1],  # cut off
            aspnet_session()[:-1],  # shorter than the header
        ],
    )
    def test_anything_else_is_not_a_session(self, value):
        result = readable(value)

        assert not (isinstance(result, dict) and result.get("format"))

    def test_text_that_starts_like_a_session_stays_text(self):
        assert readable(b"\x02 plain text") == "\x02 plain text"
