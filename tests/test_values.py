"""src.common.values: values that are not UTF-8 text are described instead of failing to decode."""

import gzip
import uuid

import pytest

from src.common import values
from src.common.values import Unpacking, readable, text

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

    def test_a_binary_value_is_shown_whole(self):
        """e.g. a 31 KB .NET BinaryFormatter session object: every byte, not a preview."""
        value = b"\x00\x01\xff" + b"customer data " * 2500

        result = readable(value)

        assert result["text"] == "\x00\x01\\xff" + "customer data " * 2500
        assert "truncated" not in result

    def test_a_large_binary_value_is_not_cut(self):
        value = b"\xff" + b"a" * (2 * 1024 * 1024)

        result = readable(value)

        assert result["length"] == len(value)
        assert result["text"] == "\\xff" + "a" * (2 * 1024 * 1024)
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

    NOT_UNPACKED = (
        "not unpacked: not valid gzip, or over the 1024 KB one call may unpack"
    )

    def test_gzip_over_the_limit_is_not_unpacked(self, monkeypatch):
        monkeypatch.setattr(values, "MAX_GUNZIP_BYTES", 10)
        value = gzip.compress(b"x" * 100)

        result = readable(value)

        assert result["format"] == "gzip"
        assert "unpacked" not in result
        assert result["note"].startswith("not unpacked")
        assert result["head"].startswith("1f 8b")

    def test_gzip_followed_by_more_data_is_not_unpacked(self):
        result = readable(gzip.compress(b"x") + b"\xff more")

        assert result["note"] == self.NOT_UNPACKED
        assert "unpacked" not in result

    def test_broken_gzip_is_described(self):
        result = readable(b"\x1f\x8b\x00\x00garbage\xff")

        assert result["format"] == "gzip"
        assert result["note"] == self.NOT_UNPACKED
        assert result["head"].startswith("1f 8b")

    def test_one_call_unpacks_at_most_the_limit_in_total(self, monkeypatch):
        """Many small gzip values must not add up past the limit: a list or a hash shares one Unpacking."""
        monkeypatch.setattr(values, "MAX_GUNZIP_BYTES", 150)
        unpacking = Unpacking()
        value = gzip.compress(b"x" * 100)

        first = readable(value, unpacking)
        second = readable(value, unpacking)

        assert first["unpacked"] == "x" * 100
        assert "unpacked" not in second
        assert second["note"].startswith("not unpacked")
        assert unpacking.left == 50
        # a call of its own starts with the whole limit again
        assert readable(value)["unpacked"] == "x" * 100

    def test_gzip_inside_gzip_is_unpacked_once(self):
        inner = gzip.compress(b"\xff payload")
        result = readable(gzip.compress(inner))

        assert result["unpacked"]["format"] == "gzip"
        assert "unpacked" not in result["unpacked"]
        assert "note" not in result["unpacked"]


class TestAspNetSession:
    def test_entries_are_unpacked(self):
        value = aspnet_session(
            ("UserName", "ayşe".encode()),
            ("Profile", b'{"role": "admin"}'),
            ("LoginCount", (5).to_bytes(4, "big")),
            ("Negative", (-2).to_bytes(4, "big", signed=True)),
            ("LastSeen", (1730822480).to_bytes(4, "big")),
            ("Year", b"2024"),
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
        # 4 bytes may be SetInt32 or a 4-byte SetString: both readings when the bytes are text
        assert entries["LastSeen"] == {"int32": 1730822480, "text": "g*AP"}
        assert entries["Year"] == {"int32": 842019380, "text": "2024"}
        assert entries["Blob"]["head"] == "00 8e 01 02 03"
        assert entries["Empty"] == ""

    @pytest.mark.parametrize(
        "value",
        [
            "Fiyat:\u00a01.234\u00a0TL",  # no-break space
            "👨\u200d👩\u200d👧",  # zero-width joiner
            "\ufeffmerhaba",  # byte order mark
            "東京\u3000駅",  # ideographic space
            '{"cart": "' + "ürün\u00a0" * 2000 + '"}',  # long, with no-break spaces
        ],
    )
    def test_any_utf8_entry_is_text(self, value):
        result = readable(aspnet_session(("Entry", value.encode())))

        assert result["entries"]["Entry"] == value

    def test_entries_share_the_unpack_limit(self, monkeypatch):
        monkeypatch.setattr(values, "MAX_GUNZIP_BYTES", 150)
        packed = gzip.compress(b"x" * 100)

        entries = readable(aspnet_session(("a", packed), ("b", packed)))["entries"]

        assert entries["a"]["unpacked"] == "x" * 100
        assert "unpacked" not in entries["b"]

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
