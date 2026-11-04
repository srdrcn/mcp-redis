"""Redis values for tool output. The value tools read bytes (decode_responses=False) and pass each value
through :func:`readable`: a UTF-8 value is returned as text, any other value as a description an LLM can read
instead of a decode error. Known binary formats are recognised by their content:

- ASP.NET Core session payloads (the ``data`` field of the hash Microsoft.Extensions.Caching.StackExchangeRedis
  writes, next to ``absexp`` and ``sldexp``) are unpacked into their entries,
- gzip is unpacked, up to MAX_GUNZIP_BYTES per tool call (see :class:`Unpacking`),
- Java serialization is named; its class and field names show in the text preview.
"""

import unicodedata
import uuid
import zlib
from typing import Any, Dict, Optional, Union

# bytes of a binary value shown as text, with \\xNN for the bytes that are not UTF-8
PREVIEW_BYTES = 4096
# leading bytes shown in hex, to tell the format
HEAD_BYTES = 32
# gzip unpacked by one tool call, over all the values it returns
MAX_GUNZIP_BYTES = 1024 * 1024

Readable = Union[str, Dict[str, Any]]

_JAVA_SERIALIZATION = b"\xac\xed\x00\x05"
_GZIP = b"\x1f\x8b"
# ASP.NET Core DistributedSession: 1 byte revision, 3 bytes entry count, 16 bytes session id
_ASPNET_REVISION = 2
_ASPNET_HEADER = 20


class Unpacking:
    """What one tool call may still unpack: MAX_GUNZIP_BYTES over all its values, so a call that returns many
    small gzip values (a list, a hash, a session's entries) cannot expand them past the pod's memory. A tool
    that returns several values passes one instance to every :func:`readable` call."""

    def __init__(self) -> None:
        self.left = MAX_GUNZIP_BYTES


def readable(
    value: Union[bytes, str], unpacking: Optional[Unpacking] = None
) -> Readable:
    """The value as text if it is UTF-8 (and not an ASP.NET Core session payload), else its description. Text
    (from a client that decodes) is returned as it is."""
    if isinstance(value, str):
        return value
    if unpacking is None:
        unpacking = Unpacking()
    session = _aspnet_session(value, unpacking)
    if session is not None:
        return {
            "binary": True,
            "length": len(value),
            "format": "aspnet-core-session",
            **session,
        }
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return _binary(value, unpacking)


def text(value: Union[bytes, str]) -> str:
    """A key, field or id: UTF-8 text, with \\xNN for bytes that are not UTF-8."""
    if isinstance(value, str):
        return value
    return value.decode("utf-8", "backslashreplace")


def _binary(value: bytes, unpacking: Optional[Unpacking]) -> Dict[str, Any]:
    """The description of bytes that are not UTF-8; ``unpacking`` is None inside an unpacked value."""
    result: Dict[str, Any] = {"binary": True, "length": len(value)}
    if value.startswith(_JAVA_SERIALIZATION):
        result["format"] = "java-serialization"
    elif value.startswith(_GZIP):
        result["format"] = "gzip"
        unpacked = (
            _gunzip(value, unpacking.left)
            if unpacking is not None and unpacking.left > 0
            else None
        )
        if unpacked is not None:
            unpacking.left -= len(unpacked)
            try:
                result["unpacked"] = unpacked.decode("utf-8")
            except UnicodeDecodeError:
                result["unpacked"] = _binary(unpacked, None)
            return result
        if unpacking is not None:
            result["note"] = (
                "not unpacked: not valid gzip, or over the"
                f" {MAX_GUNZIP_BYTES // 1024} KB one call may unpack"
            )
    result["text"] = text(value[:PREVIEW_BYTES])
    if len(value) > PREVIEW_BYTES:
        result["truncated"] = True
    result["head"] = value[:HEAD_BYTES].hex(" ")
    return result


def _gunzip(value: bytes, limit: int) -> Optional[bytes]:
    """The unpacked bytes, or None if they are not gzip or larger than ``limit`` (> 0)."""
    unpacker = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        unpacked = unpacker.decompress(value, limit)
    except zlib.error:
        return None
    # larger than the limit, cut off, or followed by more data
    if unpacker.unconsumed_tail or not unpacker.eof or unpacker.unused_data:
        return None
    return unpacked


def _aspnet_session(value: bytes, unpacking: Unpacking) -> Optional[Dict[str, Any]]:
    """The entries of an ASP.NET Core session payload (Microsoft.AspNetCore.Session, revision 2): after the
    header, per entry a 2-byte key length, the UTF-8 key, a 4-byte value length and the value (lengths
    big-endian). None unless the bytes are exactly that."""
    if len(value) < _ASPNET_HEADER or value[0] != _ASPNET_REVISION:
        return None
    count = int.from_bytes(value[1:4], "big")
    position = _ASPNET_HEADER
    spans: Dict[str, slice] = {}
    for _ in range(count):
        if position + 2 > len(value):
            return None
        key_length = int.from_bytes(value[position : position + 2], "big")
        position += 2
        if position + key_length + 4 > len(value):
            return None
        try:
            key = value[position : position + key_length].decode("utf-8")
        except UnicodeDecodeError:
            return None
        position += key_length
        value_length = int.from_bytes(value[position : position + 4], "big")
        position += 4
        if position + value_length > len(value) or key in spans:
            return None
        spans[key] = slice(position, position + value_length)
        position += value_length
    if position != len(value):
        return None
    # the entries are read only once the whole payload is known to be a session
    return {
        # .NET writes a Guid with its first three fields little-endian
        "sessionId": str(uuid.UUID(bytes_le=bytes(value[4:_ASPNET_HEADER]))),
        "entries": {
            key: _session_value(value[span], unpacking) for key, span in spans.items()
        },
    }


def _session_value(value: bytes, unpacking: Unpacking) -> Readable:
    """A session entry. The payload does not say how an entry was written: UTF-8 is text (SetString), and a
    4-byte entry may also be a number (SetInt32, big-endian), so it gets both readings."""
    try:
        decoded: Optional[str] = value.decode("utf-8")
    except UnicodeDecodeError:
        decoded = None
    if len(value) == 4:
        readings: Dict[str, Any] = {"int32": int.from_bytes(value, "big", signed=True)}
        if decoded is not None and not _has_control_characters(decoded):
            readings["text"] = decoded
        return readings
    if decoded is not None:
        return decoded
    return _binary(value, unpacking)


def _has_control_characters(value: str) -> bool:
    return any(unicodedata.category(c) == "Cc" and c not in "\t\r\n" for c in value)
