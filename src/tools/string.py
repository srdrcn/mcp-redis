import json
from typing import Any, Dict, Optional, Union

from redis.exceptions import RedisError
from redis import Redis

from src.common.connection import RedisConnectionManager
from src.common.registry import ToolKind, redis_tool
from src.common.values import readable


@redis_tool("data", ToolKind.WRITE)
async def set(
    key: str,
    value: Union[str, bytes, int, float, dict],
    expiration: Optional[int] = None,
) -> str:
    """Set a Redis string value with an optional expiration time.

    Args:
        key (str): The key to set.
        value (str, bytes, int, float, dict): The value to store.
        expiration (int, optional): Expiration time in seconds.

    Returns:
        str: Confirmation message or an error message.
    """
    if isinstance(value, bytes):
        encoded_value = value
    elif isinstance(value, dict):
        encoded_value = json.dumps(value)
    else:
        encoded_value = str(value)

    if isinstance(encoded_value, str):
        encoded_value = encoded_value.encode("utf-8")

    try:
        r: Redis = RedisConnectionManager.get_connection()
        if expiration:
            r.setex(key, expiration, encoded_value)
        else:
            r.set(key, encoded_value)

        return f"Successfully set {key}" + (
            f" with expiration {expiration} seconds" if expiration else ""
        )
    except RedisError as e:
        return f"Error setting key {key}: {str(e)}"


@redis_tool("data")
async def get(key: str) -> Union[str, Dict[str, Any]]:
    """Get a Redis string value.

    A value that is not UTF-8 text comes back as a description instead: binary, length, format, the whole value
    as text with \\xNN for the other bytes, and the leading bytes in hex. An ASP.NET Core session payload (the
    data field of a cache hash, next to absexp and sldexp) is unpacked into its entries, and gzip is unpacked.

    Args:
        key (str): The key to retrieve.

    Returns:
        The stored value or an error message.
    """
    try:
        r: Redis = RedisConnectionManager.get_connection(decode_responses=False)
        value = r.get(key)

        if value is None:
            return f"Key {key} does not exist"

        return readable(value)
    except RedisError as e:
        return f"Error retrieving key {key}: {str(e)}"
