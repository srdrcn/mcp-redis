import contextvars
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Dict, List

from redis.cluster import RedisCluster

from src.common.config_loader import ClusterConfig, get_config
from src.common.connection import RedisConnectionManager, current_cluster
from src.common.registry import TOOL_TIMEOUT_SECONDS, redis_tool

# The connection checks share the tool's time limit, with a margin for building the answer.
CHECK_BUDGET_SECONDS = TOOL_TIMEOUT_SECONDS - 2


@redis_tool("core", cluster="optional")
async def clusters_list(check: bool = True) -> List[Dict[str, Any]]:
    """List the configured Redis clusters (mode, hosts, readOnly, modules, auth) and, unless check is false,
    whether each one answers: status, latency, Redis version and role. Start here when a cluster misbehaves.

    Args:
        check: Also connect to each cluster and report its status (default true).

    Returns:
        One entry per cluster (only the given cluster if the cluster parameter is set).
    """
    config = get_config()
    only = current_cluster.get()
    selected = [c for c in config.clusters if only is None or c.name == only]
    rows = [_describe(c, config.is_read_only(c)) for c in selected]
    if not check or not selected:
        return rows

    pool = ThreadPoolExecutor(
        max_workers=min(16, len(selected)), thread_name_prefix="clusters-list"
    )
    try:
        futures = [
            pool.submit(contextvars.copy_context().run, _probe, c.name)
            for c in selected
        ]
        deadline = time.monotonic() + CHECK_BUDGET_SECONDS
        for row, future in zip(rows, futures):
            try:
                row.update(future.result(max(0.0, deadline - time.monotonic())))
            except FutureTimeoutError:
                row.update(
                    {
                        "status": "timeout",
                        "error": "The connection check did not finish within the time limit",
                    }
                )
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return rows


def _describe(c: ClusterConfig, read_only: bool) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "name": c.name,
        "mode": c.mode,
        "hosts": c.hosts,
        "readOnly": read_only,
        "modules": list(c.modules),
        "tls": c.tls is not None,
        "auth": "entraid" if c.entraid else ("password" if c.password else "none"),
    }
    if c.mode != "cluster":
        row["db"] = c.db
    if c.mode == "sentinel":
        row["masterName"] = c.master_name
    if c.username:
        row["username"] = c.username
    return row


def _probe(name: str) -> Dict[str, Any]:
    start = time.monotonic()
    try:
        r = RedisConnectionManager.get_connection(cluster=name)
        r.ping()
        latency_ms = int((time.monotonic() - start) * 1000)
        server = r.info("server")
        replication = r.info("replication")
        result: Dict[str, Any] = {
            "status": "ok",
            "latencyMs": latency_ms,
            "redisVersion": server.get("redis_version"),
            "role": replication.get("role"),
        }
        if isinstance(r, RedisCluster):
            result["primaries"] = len(r.get_primaries())
        return result
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}
