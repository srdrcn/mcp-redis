import contextvars
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Dict, List, Union

from redis.cluster import RedisCluster
from redis.exceptions import RedisError

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


@redis_tool("core")
async def cluster_nodes() -> Union[str, Dict[str, Any]]:
    """Show the nodes of a cluster and their state.

    Redis Cluster (mode: cluster): the CLUSTER INFO summary (state, slots assigned/ok/pfail/fail, known nodes,
    size) and every node from CLUSTER NODES: address, node id, role (primary or replica), the primary a replica
    follows, its slot ranges, extra flags (pfail, fail, handshake, noaddr, nofailover) and whether its link is
    connected. Primaries are listed in slot order, each followed by its replicas.

    Standalone and Sentinel: the replication view of the node the client uses (INFO replication): its role,
    its primary and link status if it is a replica, and its connected replicas with state, offset and lag.

    Returns:
        The node overview, or an error message.
    """
    config = RedisConnectionManager.cluster_config()
    try:
        r = RedisConnectionManager.get_connection()
        if isinstance(r, RedisCluster):
            return _cluster_view(config.name, r)
        return _replication_view(config, r.info("replication"))
    except RedisError as e:
        return f"Error reading the nodes of cluster {config.name}: {e}"


_ROLE_FLAGS = {"master": "primary", "slave": "replica"}
_HIDDEN_FLAGS = {"master", "slave", "myself", "noflags"}


def _cluster_view(name: str, r: RedisCluster) -> Dict[str, Any]:
    info = r.cluster_info()
    raw = r.cluster_nodes()
    address_of = {node["node_id"]: address for address, node in raw.items()}
    nodes = []
    for address, node in raw.items():
        flags = node.get("flags", "").split(",")
        role = next((_ROLE_FLAGS[f] for f in flags if f in _ROLE_FLAGS), "unknown")
        entry: Dict[str, Any] = {
            "address": address,
            "id": node["node_id"],
            "role": role,
        }
        if node.get("hostname"):
            entry["hostname"] = node["hostname"]
        if role == "replica":
            primary_id = node.get("master_id")
            entry["primary"] = address_of.get(primary_id, primary_id)
        slots = []
        for slot_range in node.get("slots", []):
            start, end = slot_range[0], slot_range[-1]
            slots.append(start if start == end else f"{start}-{end}")
        if slots:
            entry["slots"] = slots
        extra = [
            ("pfail" if f == "fail?" else f)
            for f in flags
            if f and f not in _HIDDEN_FLAGS
        ]
        if extra:
            entry["flags"] = extra
        entry["connected"] = bool(node.get("connected"))
        nodes.append(entry)

    primaries = sorted(
        (n for n in nodes if n["role"] == "primary"),
        key=lambda n: int(str(n.get("slots", ["99999"])[0]).split("-")[0]),
    )
    ordered = []
    for primary in primaries:
        ordered.append(primary)
        ordered.extend(n for n in nodes if n.get("primary") == primary["address"])
    ordered.extend(n for n in nodes if n not in ordered)

    def number(key: str) -> Any:
        value = info.get(key)
        try:
            return int(value)
        except (TypeError, ValueError):
            return value

    return {
        "cluster": name,
        "mode": "cluster",
        "state": info.get("cluster_state"),
        "slotsAssigned": number("cluster_slots_assigned"),
        "slotsOk": number("cluster_slots_ok"),
        "slotsPfail": number("cluster_slots_pfail"),
        "slotsFail": number("cluster_slots_fail"),
        "knownNodes": number("cluster_known_nodes"),
        "size": number("cluster_size"),
        "nodes": ordered,
    }


def _replication_view(config: ClusterConfig, info: Dict[str, Any]) -> Dict[str, Any]:
    role = info.get("role")
    view: Dict[str, Any] = {
        "cluster": config.name,
        "mode": config.mode,
        "role": {"master": "primary", "slave": "replica"}.get(role, role),
    }
    if role == "slave":
        view["primary"] = f"{info.get('master_host')}:{info.get('master_port')}"
        view["linkStatus"] = info.get("master_link_status")
    replicas = []
    for i in range(int(info.get("connected_slaves") or 0)):
        replica = info.get(f"slave{i}")
        if isinstance(replica, dict):
            replicas.append(
                {
                    "address": f"{replica.get('ip')}:{replica.get('port')}",
                    "state": replica.get("state"),
                    "offset": replica.get("offset"),
                    "lag": replica.get("lag"),
                }
            )
    view["replicas"] = replicas
    return view
