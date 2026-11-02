"""Sentinel tools, listed only for clusters in mode sentinel.

They ask the cluster's Sentinels directly and never the master, so they work without the master's password (the
Sentinels' own password is ``<name>_sentinel_pass``) and show what the Sentinels see: the master, its replicas,
the other Sentinels, quorum and TILT. Every Sentinel gets SENTINEL_TIMEOUT_SECONDS per socket operation; one
that does not answer is reported, not fatal.
"""

import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Callable, Dict, List, Optional, Tuple

from redis import Redis
from redis.exceptions import RedisError, ResponseError

from src.common.config_loader import ClusterConfig, get_config
from src.common.connection import (
    SENTINEL_TIMEOUT_SECONDS,
    RedisConnectionManager,
    sentinel_client_kwargs,
)
from src.common.registry import redis_tool

# Time for one round of Sentinels asked in parallel (the configured ones, then the ones they report): a connect
# and a few replies. sentinel_status runs two rounds and stays within the tool's time limit.
ROUND_SECONDS = 2 * SENTINEL_TIMEOUT_SECONDS + 1
# At most this many Sentinels learned from SENTINEL SENTINELS are asked in addition to the configured ones.
MAX_DISCOVERED_SENTINELS = 8

Address = Tuple[str, int]


def _pairs(reply: List[Any]) -> Dict[str, Any]:
    """A SENTINEL reply (a flat field/value list) as a dict."""
    items = iter(reply)
    return dict(zip(items, items))


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _address(host: Any, port: Any) -> str:
    return f"{host}:{port}"


def _ask_all(
    config: ClusterConfig,
    addresses: List[Address],
    ask: Callable[[Redis], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Runs ``ask`` on a client of every Sentinel at once and returns one result per address, in order:
    ``{"address", "status": "ok", **ask(...)}``, or ``status`` error/timeout with the error."""
    if not addresses:
        return []

    def run(address: Address) -> Dict[str, Any]:
        client = Redis(
            host=address[0],
            port=address[1],
            decode_responses=True,
            **sentinel_client_kwargs(config),
        )
        try:
            return {"status": "ok", **ask(client)}
        except RedisError as e:
            return {"status": "error", "error": f"{type(e).__name__}: {e}"}
        finally:
            client.close()

    pool = ThreadPoolExecutor(
        max_workers=len(addresses), thread_name_prefix="sentinel-check"
    )
    try:
        futures = [pool.submit(run, address) for address in addresses]
        deadline = time.monotonic() + ROUND_SECONDS
        results = []
        for address, future in zip(addresses, futures):
            try:
                result = future.result(max(0.0, deadline - time.monotonic()))
            except FutureTimeoutError:
                result = {
                    "status": "timeout",
                    "error": f"no answer within {ROUND_SECONDS} s",
                }
            results.append({"address": _address(*address), **result})
        return results
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def _first_answer(
    config: ClusterConfig, ask: Callable[[Redis], Dict[str, Any]]
) -> Dict[str, Any]:
    """The answer of the first configured Sentinel (in config order) that answers, with the errors of the ones
    before it; or all the errors if none answers."""
    results = _ask_all(config, config.addresses(), ask)
    errors = []
    for result in results:
        if result["status"] == "ok":
            answer = {"sentinel": result.pop("address"), **result}
            answer.pop("status")
            if errors:
                answer["unansweredSentinels"] = errors
            return answer
        errors.append(
            {
                "address": result["address"],
                "status": result["status"],
                "error": result["error"],
            }
        )
    return {"error": "no Sentinel answered", "unansweredSentinels": errors}


def _sentinel_view(master_name: str) -> Callable[[Redis], Dict[str, Any]]:
    def ask(client: Redis) -> Dict[str, Any]:
        master = _pairs(client.execute_command("SENTINEL", "MASTER", master_name))
        peers = [
            _pairs(p)
            for p in client.execute_command("SENTINEL", "SENTINELS", master_name)
        ]
        try:
            quorum_check = client.execute_command("SENTINEL", "CKQUORUM", master_name)
        except ResponseError as e:
            # NOQUORUM / NOAUTH ... come back as errors
            quorum_check = str(e)
        try:
            sentinel_id = client.execute_command("SENTINEL", "MYID")
        except ResponseError:
            sentinel_id = None  # before Redis 6.2
        info = client.info("sentinel")
        view: Dict[str, Any] = {
            "id": sentinel_id,
            "master": _address(master.get("ip"), master.get("port")),
            "flags": master.get("flags"),
            "quorum": _int(master.get("quorum")),
            "ckquorum": quorum_check,
            "otherSentinels": _int(master.get("num-other-sentinels")),
            "replicas": _int(master.get("num-slaves")),
            "configEpoch": _int(master.get("config-epoch")),
            "msSinceLastOkPing": _int(master.get("last-ok-ping-reply")),
            "downAfterMs": _int(master.get("down-after-milliseconds")),
            "tilt": str(info.get("sentinel_tilt", "0")) == "1",
            "_peers": peers,
        }
        if master.get("failover-state"):
            view["failoverState"] = master["failover-state"]
        return view

    return ask


def _discovered(
    rows: List[Dict[str, Any]], known: List[Address]
) -> Tuple[List[Address], Dict[str, str], int]:
    """The Sentinels the answering ones report (SENTINEL SENTINELS) that are not already known, by id or by
    address; the flags they are reported with; and how many were left out over MAX_DISCOVERED_SENTINELS."""
    known_ids = {row.get("id") for row in rows if row.get("id")}
    known_addresses = {_address(*a) for a in known}
    found: List[Address] = []
    flags: Dict[str, str] = {}
    for row in rows:
        for peer in row.get("_peers", []):
            address = _address(peer.get("ip"), peer.get("port"))
            if peer.get("runid") in known_ids or address in known_addresses:
                continue
            port = _int(peer.get("port"))
            if not peer.get("ip") or port is None:
                continue
            known_addresses.add(address)
            if peer.get("runid"):
                known_ids.add(peer["runid"])
            found.append((peer["ip"], port))
            flags[address] = peer.get("flags", "")
    return (
        found[:MAX_DISCOVERED_SENTINELS],
        flags,
        max(0, len(found) - MAX_DISCOVERED_SENTINELS),
    )


def _warnings(rows: List[Dict[str, Any]], masters: Dict[str, List[str]]) -> List[str]:
    warnings = []
    answered = [r for r in rows if r["status"] == "ok"]
    if not answered:
        return ["no Sentinel answered"]
    if len(masters) > 1:
        views = "; ".join(f"{m} ({', '.join(s)})" for m, s in masters.items())
        warnings.append(f"the Sentinels disagree on the master: {views}")
    for row in rows:
        address = row["address"]
        if row["status"] != "ok":
            warnings.append(f"{address} did not answer ({row['status']})")
            continue
        if row["tilt"]:
            warnings.append(
                f"{address} is in TILT mode and does not act until it leaves it"
            )
        extra = set((row.get("flags") or "").split(",")) - {"master", ""}
        if extra:
            warnings.append(f"{address} flags the master {','.join(sorted(extra))}")
        if not str(row.get("ckquorum", "")).startswith("OK"):
            warnings.append(f"{address}: {row.get('ckquorum')}")
    return warnings


@redis_tool("core", mode="sentinel")
async def sentinel_status() -> Dict[str, Any]:
    """Ask the Sentinels of a Sentinel cluster (not the master) how they see the master.

    Every configured Sentinel is asked, and so are the other Sentinels they report (source "discovered"). For
    each: the master address it names, the master's flags (s_down, o_down, ...), quorum and the quorum check
    (SENTINEL CKQUORUM), the number of other Sentinels and replicas it knows, the config epoch (it grows with
    every failover), the milliseconds since the master last answered its ping and whether the Sentinel is in
    TILT mode. "agreement" tells whether every
    Sentinel that answered names the same master, and "warnings" lists what needs attention. Needs no master
    password.

    Returns:
        The master as the Sentinels see it, one entry per Sentinel and the warnings.
    """
    config = RedisConnectionManager.cluster_config()
    configured = config.addresses()
    ask = _sentinel_view(config.master_name)
    rows = [
        {"source": "configured", **row} for row in _ask_all(config, configured, ask)
    ]
    found, peer_flags, left_out = _discovered(rows, configured)
    for row in _ask_all(config, found, ask):
        rows.append(
            {
                "source": "discovered",
                "reportedFlags": peer_flags.get(row["address"], ""),
                **row,
            }
        )
    masters: Dict[str, List[str]] = {}
    for row in rows:
        row.pop("_peers", None)
        if row["status"] == "ok":
            masters.setdefault(row["master"], []).append(row["address"])
    warnings = _warnings(rows, masters)
    if left_out:
        warnings.append(
            f"{left_out} more Sentinels were reported and not asked (at most {MAX_DISCOVERED_SENTINELS})"
        )
    # each row: address and source first
    ordered = [
        {"address": r.pop("address"), "source": r.pop("source"), **r} for r in rows
    ]
    return {
        "cluster": config.name,
        "masterName": config.master_name,
        "master": next(iter(masters)) if len(masters) == 1 else None,
        "agreement": len(masters) == 1,
        "sentinels": ordered,
        "warnings": warnings,
    }


@redis_tool("core", mode="sentinel")
async def sentinel_replicas() -> Dict[str, Any]:
    """List the replicas of a Sentinel cluster's master as its Sentinels see them (SENTINEL REPLICAS), without
    connecting to the master or the replicas.

    For each replica: address, flags (s_down, disconnected, ...), the replication link to the master
    (master-link-status and the master it follows), replication offset, replica priority (0: never promoted)
    and the milliseconds since it last answered a ping. The first configured Sentinel that answers is asked.

    Returns:
        The Sentinel asked and the replicas, or the errors of the Sentinels that did not answer.
    """
    config = RedisConnectionManager.cluster_config()
    master_name = config.master_name

    def ask(client: Redis) -> Dict[str, Any]:
        replicas = []
        for reply in client.execute_command("SENTINEL", "REPLICAS", master_name):
            r = _pairs(reply)
            replicas.append(
                {
                    "address": _address(r.get("ip"), r.get("port")),
                    "flags": r.get("flags"),
                    "masterLinkStatus": r.get("master-link-status"),
                    "master": _address(r.get("master-host"), r.get("master-port")),
                    "offset": _int(r.get("slave-repl-offset")),
                    "priority": _int(r.get("slave-priority")),
                    "msSinceLastOkPing": _int(r.get("last-ok-ping-reply")),
                }
            )
        return {"replicas": replicas}

    return {
        "cluster": config.name,
        "masterName": master_name,
        **_first_answer(config, ask),
    }


@redis_tool("core", mode="sentinel")
async def sentinel_masters() -> Dict[str, Any]:
    """List every master the Sentinels of a Sentinel cluster monitor (SENTINEL MASTERS), not only this
    cluster's. A Sentinel often monitors several masters: this shows their names (the masterName to configure),
    addresses, flags, quorum and the replicas and Sentinels each has, and which configured clusters use them.
    The first configured Sentinel that answers is asked.

    Returns:
        The Sentinel asked and its masters, or the errors of the Sentinels that did not answer.
    """
    config = RedisConnectionManager.cluster_config()
    sentinel_addresses = {_address(*a) for a in config.addresses()}
    # sentinel clusters that share a Sentinel with this one, by master name
    configured: Dict[str, List[str]] = {}
    for c in get_config().clusters:
        if c.mode == "sentinel" and sentinel_addresses & {
            _address(*a) for a in c.addresses()
        }:
            configured.setdefault(c.master_name, []).append(c.name)

    def ask(client: Redis) -> Dict[str, Any]:
        masters = []
        for reply in client.execute_command("SENTINEL", "MASTERS"):
            m = _pairs(reply)
            masters.append(
                {
                    "name": m.get("name"),
                    "address": _address(m.get("ip"), m.get("port")),
                    "flags": m.get("flags"),
                    "quorum": _int(m.get("quorum")),
                    "replicas": _int(m.get("num-slaves")),
                    "otherSentinels": _int(m.get("num-other-sentinels")),
                    "configuredAs": configured.get(m.get("name"), []),
                }
            )
        return {"masters": masters}

    return {"cluster": config.name, **_first_answer(config, ask)}
