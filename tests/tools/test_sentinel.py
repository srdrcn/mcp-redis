"""Sentinel tools: unit tests with fake Sentinels, and integration tests with real redis-server Sentinels."""

import asyncio
import time
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

import src.tools.sentinel as sentinel_tools
from src.common import registry
from src.common.config_loader import parse_config, resolve_passwords, set_config
from src.common.connection import current_cluster
from src.common.server import load_tools
from tests import redis_servers

MASTER = "androidpos"


def _flat(fields: Dict[str, Any]) -> List[Any]:
    return [item for pair in fields.items() for item in pair]


class FakeSentinel:
    """Answers the SENTINEL commands and INFO sentinel the tools send, from plain data."""

    def __init__(
        self,
        id: Optional[str] = None,
        master: str = "10.0.0.1:6379",
        flags: str = "master",
        quorum: int = 2,
        ckquorum: str = "OK 3 usable Sentinels. Quorum and failover authorization can be reached",
        peers: Optional[List[Dict[str, Any]]] = None,
        tilt: bool = False,
        replicas: Optional[List[Dict[str, Any]]] = None,
        masters: Optional[List[Dict[str, Any]]] = None,
        error: Optional[Exception] = None,
        delay: float = 0,
        failover_state: Optional[str] = None,
    ):
        self.id = id
        self.master = master
        self.flags = flags
        self.quorum = quorum
        self.ckquorum = ckquorum
        self.peers = peers or []
        self.tilt = tilt
        self.replicas = replicas or []
        self.masters = masters or []
        self.error = error
        self.delay = delay
        self.failover_state = failover_state
        self.closed = False

    def execute_command(self, *args):
        if self.delay:
            time.sleep(self.delay)
        if self.error:
            raise self.error
        command = args[1]
        if command == "MASTER":
            ip, port = self.master.rsplit(":", 1)
            fields = {
                "name": args[2],
                "ip": ip,
                "port": port,
                "flags": self.flags,
                "quorum": str(self.quorum),
                "num-other-sentinels": str(len(self.peers)),
                "num-slaves": str(len(self.replicas)),
                "config-epoch": "4",
                "last-ok-ping-reply": "120",
                "down-after-milliseconds": "5000",
            }
            if self.failover_state:
                fields["failover-state"] = self.failover_state
            return _flat(fields)
        if command == "SENTINELS":
            return [_flat(p) for p in self.peers]
        if command == "CKQUORUM":
            if not self.ckquorum.startswith("OK"):
                raise ResponseError(self.ckquorum)
            return self.ckquorum
        if command == "MYID":
            if self.id is None:
                raise ResponseError("ERR unknown subcommand 'MYID'")
            return self.id
        if command == "REPLICAS":
            return [_flat(r) for r in self.replicas]
        if command == "MASTERS":
            return [_flat(m) for m in self.masters]
        raise AssertionError(f"unexpected command {args}")

    def info(self, section):
        assert section == "sentinel"
        return {"sentinel_masters": 1, "sentinel_tilt": 1 if self.tilt else 0}

    def close(self):
        self.closed = True


@pytest.fixture
def fakes():
    """Fake Sentinels by "host:port"; the Redis clients the tools create are taken from here."""
    sentinels: Dict[str, FakeSentinel] = {}
    created: List[Dict[str, Any]] = []

    def factory(host, port, **kwargs):
        created.append({"host": host, "port": port, **kwargs})
        address = f"{host}:{port}"
        if address not in sentinels:
            raise AssertionError(f"no fake Sentinel at {address}")
        return sentinels[address]

    with patch.object(sentinel_tools, "Redis", side_effect=factory):
        sentinels["created"] = created  # type: ignore[assignment]
        yield sentinels


def _use(*clusters, passwords=None, tmp_path=None, **root):
    data = {"clusters": list(clusters), **root}
    if tmp_path is not None:
        data["passwordsDir"] = str(tmp_path)
        for key, value in (passwords or {}).items():
            (tmp_path / key).write_text(value, encoding="utf-8")
    config = parse_config(data)
    resolve_passwords(config)
    set_config(config)
    return config


def _sentinel_cluster(name="s", hosts="s1:26379", master=MASTER, **fields):
    return {
        "name": name,
        "mode": "sentinel",
        "hosts": hosts,
        "masterName": master,
        **fields,
    }


def _call(tool, cluster="s"):
    token = current_cluster.set(cluster)
    try:
        return asyncio.run(tool())
    finally:
        current_cluster.reset(token)


def _peer(ip, port, runid, flags="sentinel"):
    return {
        "name": f"{ip}:{port}",
        "ip": ip,
        "port": str(port),
        "runid": runid,
        "flags": flags,
    }


class TestSentinelStatus:
    def test_asks_the_configured_and_the_discovered_sentinels(self, fakes):
        _use(_sentinel_cluster(hosts="s1:26379"))
        fakes["s1:26379"] = FakeSentinel(
            id="id1",
            peers=[_peer("10.0.0.2", 26379, "id2"), _peer("10.0.0.3", 26379, "id3")],
        )
        fakes["10.0.0.2:26379"] = FakeSentinel(
            id="id2", peers=[_peer("s1", 26379, "id1")]
        )
        fakes["10.0.0.3:26379"] = FakeSentinel(id="id3")

        result = _call(sentinel_tools.sentinel_status)

        assert result["cluster"] == "s"
        assert result["masterName"] == MASTER
        assert result["master"] == "10.0.0.1:6379"
        assert result["agreement"] is True
        assert result["warnings"] == []
        rows = result["sentinels"]
        assert [(r["address"], r["source"]) for r in rows] == [
            ("s1:26379", "configured"),
            ("10.0.0.2:26379", "discovered"),
            ("10.0.0.3:26379", "discovered"),
        ]
        first = rows[0]
        assert list(first)[:3] == ["address", "source", "status"]
        assert first["status"] == "ok"
        assert first["id"] == "id1"
        assert first["flags"] == "master"
        assert first["quorum"] == 2
        assert first["ckquorum"].startswith("OK 3 usable Sentinels")
        assert first["otherSentinels"] == 2
        assert first["configEpoch"] == 4
        assert first["msSinceLastOkPing"] == 120
        assert first["downAfterMs"] == 5000
        assert first["tilt"] is False
        assert "_peers" not in first
        assert rows[1]["reportedFlags"] == "sentinel"
        assert all(s.closed for k, s in fakes.items() if k != "created")

    def test_a_configured_sentinel_found_again_by_id_is_not_asked_twice(self, fakes):
        # s1 and s2 are configured by host name; they report each other by IP
        _use(_sentinel_cluster(hosts="s1:26379,s2:26379"))
        fakes["s1:26379"] = FakeSentinel(
            id="id1", peers=[_peer("10.0.0.2", 26379, "id2")]
        )
        fakes["s2:26379"] = FakeSentinel(
            id="id2", peers=[_peer("10.0.0.1", 26379, "id1")]
        )

        result = _call(sentinel_tools.sentinel_status)

        assert [r["address"] for r in result["sentinels"]] == ["s1:26379", "s2:26379"]

    def test_without_myid_a_sentinel_is_known_by_its_address(self, fakes):
        _use(_sentinel_cluster(hosts="10.0.0.1:26379"))
        fakes["10.0.0.1:26379"] = FakeSentinel(
            id=None, peers=[_peer("10.0.0.2", 26379, "id2")]
        )
        fakes["10.0.0.2:26379"] = FakeSentinel(
            id=None, peers=[_peer("10.0.0.1", 26379, "id1")]
        )

        result = _call(sentinel_tools.sentinel_status)

        assert [r["address"] for r in result["sentinels"]] == [
            "10.0.0.1:26379",
            "10.0.0.2:26379",
        ]
        assert result["sentinels"][0]["id"] is None

    def test_disagreement_tilt_flags_and_quorum_are_warnings(self, fakes):
        _use(_sentinel_cluster(hosts="s1:26379,s2:26379,s3:26379"))
        fakes["s1:26379"] = FakeSentinel(id="id1")
        fakes["s2:26379"] = FakeSentinel(id="id2", master="10.0.0.9:6379", tilt=True)
        fakes["s3:26379"] = FakeSentinel(
            id="id3",
            flags="master,s_down,o_down",
            ckquorum="NOQUORUM 1 usable Sentinels. Not enough available Sentinels to reach the specified quorum for this master",
            failover_state="wait_start",
        )

        result = _call(sentinel_tools.sentinel_status)

        assert result["agreement"] is False
        assert result["master"] is None
        assert result["sentinels"][2]["failoverState"] == "wait_start"
        assert result["warnings"] == [
            "the Sentinels disagree on the master: 10.0.0.1:6379 (s1:26379, s3:26379); 10.0.0.9:6379 (s2:26379)",
            "s2:26379 is in TILT mode and does not act until it leaves it",
            "s3:26379 flags the master o_down,s_down",
            "s3:26379: NOQUORUM 1 usable Sentinels. Not enough available Sentinels to reach the specified quorum"
            " for this master",
        ]

    def test_a_sentinel_that_fails_is_reported(self, fakes):
        _use(_sentinel_cluster(hosts="s1:26379,s2:26379"))
        fakes["s1:26379"] = FakeSentinel(
            error=RedisConnectionError("Error 111 connecting to s1:26379.")
        )
        fakes["s2:26379"] = FakeSentinel(id="id2")

        result = _call(sentinel_tools.sentinel_status)

        assert result["sentinels"][0] == {
            "address": "s1:26379",
            "source": "configured",
            "status": "error",
            "error": "ConnectionError: Error 111 connecting to s1:26379.",
        }
        assert result["agreement"] is True
        assert result["warnings"] == [
            "s1:26379: ConnectionError: Error 111 connecting to s1:26379."
        ]

    def test_any_failure_of_one_sentinel_is_its_own_row(self, fakes):
        _use(_sentinel_cluster(hosts="s1:26379,s2:26379"))
        fakes["s1:26379"] = FakeSentinel(error=UnicodeError("label too long"))
        fakes["s2:26379"] = FakeSentinel(id="id2")

        result = _call(sentinel_tools.sentinel_status)

        assert result["sentinels"][0]["error"] == "UnicodeError: label too long"
        assert result["master"] == "10.0.0.1:6379"

    def test_unknown_master_name(self, fakes):
        _use(_sentinel_cluster(hosts="s1:26379", master="nope"))
        fakes["s1:26379"] = FakeSentinel(
            error=ResponseError("No such master with that name")
        )

        result = _call(sentinel_tools.sentinel_status)

        assert (
            result["sentinels"][0]["error"]
            == "ResponseError: No such master with that name"
        )
        assert result["master"] is None
        assert result["agreement"] is False
        assert result["warnings"] == [
            "no Sentinel reported the master's state",
            "s1:26379: ResponseError: No such master with that name",
        ]

    def test_a_sentinel_that_does_not_answer_in_time(self, fakes, monkeypatch):
        monkeypatch.setattr(sentinel_tools, "ROUND_SECONDS", 0.2)
        _use(_sentinel_cluster(hosts="s1:26379,s2:26379"))
        fakes["s1:26379"] = FakeSentinel(id="id1", delay=1)
        fakes["s2:26379"] = FakeSentinel(id="id2")

        start = time.monotonic()
        result = _call(sentinel_tools.sentinel_status)

        assert time.monotonic() - start < 1
        assert result["sentinels"][0]["status"] == "timeout"
        assert result["sentinels"][0]["error"] == "no answer within 0.2 s"
        assert result["sentinels"][1]["status"] == "ok"
        assert result["warnings"] == ["s1:26379: no answer within 0.2 s"]

    def test_discovered_sentinels_are_capped(self, fakes, monkeypatch):
        monkeypatch.setattr(sentinel_tools, "MAX_DISCOVERED_SENTINELS", 1)
        _use(_sentinel_cluster(hosts="s1:26379"))
        fakes["s1:26379"] = FakeSentinel(
            id="id1",
            peers=[_peer("10.0.0.2", 26379, "id2"), _peer("10.0.0.3", 26379, "id3")],
        )
        fakes["10.0.0.2:26379"] = FakeSentinel(id="id2")

        result = _call(sentinel_tools.sentinel_status)

        assert [r["address"] for r in result["sentinels"]] == [
            "s1:26379",
            "10.0.0.2:26379",
        ]
        assert result["notAsked"] == [
            {"address": "10.0.0.3:26379", "reportedFlags": "sentinel"}
        ]
        assert result["warnings"] == [
            "1 more reported Sentinels were not asked (at most 1): see notAsked"
        ]

    def test_healthy_peers_are_asked_before_the_ones_reported_down(
        self, fakes, monkeypatch
    ):
        """A Sentinel keeps every peer it has seen: replaced hosts stay listed as s_down."""
        monkeypatch.setattr(sentinel_tools, "MAX_DISCOVERED_SENTINELS", 2)
        _use(_sentinel_cluster(hosts="s1:26379"))
        fakes["s1:26379"] = FakeSentinel(
            id="id1",
            peers=[
                _peer("10.0.0.7", 26379, "old1", flags="s_down,sentinel,disconnected"),
                _peer("10.0.0.8", 26379, "old2", flags="s_down,sentinel"),
                _peer("10.0.0.2", 26379, "id2"),
                _peer("10.0.0.3", 26379, "id3"),
            ],
        )
        fakes["10.0.0.2:26379"] = FakeSentinel(id="id2")
        fakes["10.0.0.3:26379"] = FakeSentinel(id="id3")

        result = _call(sentinel_tools.sentinel_status)

        assert [r["address"] for r in result["sentinels"]] == [
            "s1:26379",
            "10.0.0.2:26379",
            "10.0.0.3:26379",
        ]
        assert result["notAsked"] == [
            {
                "address": "10.0.0.7:26379",
                "reportedFlags": "s_down,sentinel,disconnected",
            },
            {"address": "10.0.0.8:26379", "reportedFlags": "s_down,sentinel"},
        ]

    def test_a_reused_address_is_left_out_of_the_agreement(self, fakes):
        _use(_sentinel_cluster(hosts="s1:26379"))
        fakes["s1:26379"] = FakeSentinel(
            id="id1", peers=[_peer("10.0.0.2", 26379, "id2")]
        )
        # another Sentinel now listens at the reported address and names another master
        fakes["10.0.0.2:26379"] = FakeSentinel(id="other", master="10.9.9.9:6379")

        result = _call(sentinel_tools.sentinel_status)

        row = result["sentinels"][1]
        assert (row["id"], row["reportedId"]) == ("other", "id2")
        assert result["agreement"] is True
        assert result["master"] == "10.0.0.1:6379"
        assert result["warnings"] == [
            "10.0.0.2:26379 answers as Sentinel other but was reported as id2: the address now belongs to"
            " another Sentinel, left out of the agreement"
        ]

    def test_connects_with_the_sentinel_password_and_short_timeouts(
        self, fakes, tmp_path
    ):
        _use(
            _sentinel_cluster(hosts="s1:26379", username="mcp"),
            passwords={"s_pass": "master-secret", "s_sentinel_pass": "sentinel-secret"},
            tmp_path=tmp_path,
        )
        fakes["s1:26379"] = FakeSentinel(id="id1")

        _call(sentinel_tools.sentinel_status)

        (created,) = fakes["created"]
        assert created["password"] == "sentinel-secret"
        assert "username" not in created
        assert created["socket_connect_timeout"] == 2
        assert created["socket_timeout"] == 2
        assert created["decode_responses"] is True


class TestSentinelReplicas:
    def test_lists_the_replicas_of_the_first_sentinel_that_answers(self, fakes):
        _use(_sentinel_cluster(hosts="s1:26379,s2:26379"))
        fakes["s1:26379"] = FakeSentinel(error=RedisConnectionError("refused"))
        fakes["s2:26379"] = FakeSentinel(
            replicas=[
                {
                    "ip": "10.0.0.2",
                    "port": "6379",
                    "flags": "slave",
                    "master-link-status": "ok",
                    "master-host": "10.0.0.1",
                    "master-port": "6379",
                    "slave-repl-offset": "1234",
                    "slave-priority": "100",
                    "last-ok-ping-reply": "300",
                }
            ]
        )

        result = _call(sentinel_tools.sentinel_replicas)

        assert result == {
            "cluster": "s",
            "masterName": MASTER,
            "sentinel": "s2:26379",
            "replicas": [
                {
                    "address": "10.0.0.2:6379",
                    "flags": "slave",
                    "masterLinkStatus": "ok",
                    "master": "10.0.0.1:6379",
                    "offset": 1234,
                    "priority": 100,
                    "msSinceLastOkPing": 300,
                }
            ],
            "unansweredSentinels": [
                {
                    "address": "s1:26379",
                    "status": "error",
                    "error": "ConnectionError: refused",
                }
            ],
        }

    def test_no_sentinel_answers(self, fakes):
        _use(_sentinel_cluster(hosts="s1:26379"))
        fakes["s1:26379"] = FakeSentinel(error=RedisConnectionError("refused"))

        result = _call(sentinel_tools.sentinel_replicas)

        assert result == {
            "cluster": "s",
            "masterName": MASTER,
            "error": "no Sentinel answered",
            "unansweredSentinels": [
                {
                    "address": "s1:26379",
                    "status": "error",
                    "error": "ConnectionError: refused",
                }
            ],
        }


class TestSentinelMasters:
    def test_lists_every_master_and_the_clusters_that_use_it(self, fakes):
        _use(
            _sentinel_cluster(name="s", hosts="s1:26379", master="vpos"),
            _sentinel_cluster(name="s-limit", hosts="s1:26379", master="limit"),
            # another Sentinel group with a master of the same name
            _sentinel_cluster(name="elsewhere", hosts="s9:26379", master="vpos"),
            {"name": "plain", "hosts": "p:6379"},
        )
        fakes["s1:26379"] = FakeSentinel(
            masters=[
                {
                    "name": "vpos",
                    "ip": "10.0.0.1",
                    "port": "6381",
                    "flags": "master",
                    "quorum": "2",
                    "num-slaves": "2",
                    "num-other-sentinels": "2",
                },
                {
                    "name": "limit",
                    "ip": "10.0.0.1",
                    "port": "6380",
                    "flags": "master",
                    "quorum": "2",
                    "num-slaves": "1",
                    "num-other-sentinels": "2",
                },
                {
                    "name": "unused",
                    "ip": "10.0.0.5",
                    "port": "6390",
                    "flags": "master,s_down",
                    "quorum": "2",
                    "num-slaves": "0",
                    "num-other-sentinels": "2",
                },
            ]
        )

        result = _call(sentinel_tools.sentinel_masters)

        assert result["cluster"] == "s"
        assert result["sentinel"] == "s1:26379"
        assert result["masters"] == [
            {
                "name": "vpos",
                "address": "10.0.0.1:6381",
                "flags": "master",
                "quorum": 2,
                "replicas": 2,
                "otherSentinels": 2,
                "configuredAs": ["s"],
            },
            {
                "name": "limit",
                "address": "10.0.0.1:6380",
                "flags": "master",
                "quorum": 2,
                "replicas": 1,
                "otherSentinels": 2,
                "configuredAs": ["s-limit"],
            },
            {
                "name": "unused",
                "address": "10.0.0.5:6390",
                "flags": "master,s_down",
                "quorum": 2,
                "replicas": 0,
                "otherSentinels": 2,
                "configuredAs": [],
            },
        ]


class TestConfiguredAs:
    MASTERS = [
        {
            "name": "vpos",
            "ip": "10.0.0.1",
            "port": "6381",
            "flags": "master",
            "quorum": "2",
            "num-slaves": "1",
            "num-other-sentinels": "2",
        },
    ]

    def test_another_member_of_the_group_is_matched_by_its_id(self, fakes):
        _use(
            _sentinel_cluster(name="s", hosts="s1:26379", master="other"),
            # the same group through another of its Sentinels, named by host name
            _sentinel_cluster(name="by-name", hosts="s2.example:26379", master="vpos"),
            # a Sentinel of another group with a master of the same name
            _sentinel_cluster(name="elsewhere", hosts="s9:26379", master="vpos"),
            # its Sentinel does not answer
            _sentinel_cluster(name="down", hosts="s8:26379", master="vpos"),
        )
        fakes["s1:26379"] = FakeSentinel(
            id="id1", peers=[_peer("10.0.0.2", 26379, "id2")], masters=self.MASTERS
        )
        fakes["s2.example:26379"] = FakeSentinel(id="id2")
        fakes["s9:26379"] = FakeSentinel(id="id9")
        fakes["s8:26379"] = FakeSentinel(error=RedisConnectionError("refused"))

        result = _call(sentinel_tools.sentinel_masters)

        assert result["masters"][0]["configuredAs"] == ["by-name"]
        assert result["unverifiedClusters"] == [
            {"cluster": "down", "error": "ConnectionError: refused"}
        ]

    def test_without_myid_a_cluster_cannot_be_verified(self, fakes):
        _use(
            _sentinel_cluster(name="s", hosts="s1:26379", master="other"),
            _sentinel_cluster(name="old", hosts="s2:26379", master="vpos"),
        )
        fakes["s1:26379"] = FakeSentinel(id=None, masters=self.MASTERS)
        fakes["s2:26379"] = FakeSentinel(id=None)

        result = _call(sentinel_tools.sentinel_masters)

        assert result["masters"][0]["configuredAs"] == []
        assert result["unverifiedClusters"] == [
            {"cluster": "old", "error": "SENTINEL MYID is not supported"}
        ]


class TestRegistration:
    NAMES = {"sentinel_status", "sentinel_replicas", "sentinel_masters"}

    def test_core_read_tools_for_sentinel_clusters_only(self):
        load_tools()
        specs = {s.name: s for s in registry.specs() if s.name in self.NAMES}

        assert set(specs) == self.NAMES
        for spec in specs.values():
            assert spec.toolset == "core"
            assert spec.kind is registry.ToolKind.READ
            assert spec.mode == "sentinel"
            assert spec.cluster == "required"

    def test_listed_with_the_sentinel_clusters_in_the_enum(self):
        load_tools()
        config = parse_config(
            {
                "clusters": [
                    {"name": "plain", "hosts": "p:6379"},
                    _sentinel_cluster(name="s1"),
                    {"name": "rc", "mode": "cluster", "hosts": "r:7000"},
                    _sentinel_cluster(name="s2"),
                ]
            }
        )

        selected = {spec.name: clusters for spec, clusters in registry.select(config)}

        for name in self.NAMES:
            assert selected[name] == ["s1", "s2"]

    def test_not_listed_without_a_sentinel_cluster(self):
        load_tools()
        config = parse_config({"clusters": [{"name": "plain", "hosts": "p:6379"}]})

        names = {spec.name for spec, _ in registry.select(config)}

        assert not names & self.NAMES


# --- real Sentinels -------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sentinel_setup(tmp_path_factory):
    """A master (with a password), one replica and two Sentinels (quorum 2) that know each other, plus a third
    Sentinel with a password of its own monitoring the same master under another name."""
    redis_servers.skip_unless_installed()
    root = tmp_path_factory.mktemp("sentinels")
    started = []
    try:
        master = redis_servers.start(root / "master", password="master-pw")
        started.append(master)
        replica = redis_servers.start(root / "replica", password="master-pw")
        started.append(replica)
        replica_client = replica.client()
        try:
            replica_client.config_set("masterauth", "master-pw")
            replica_client.replicaof(redis_servers.HOST, master.port)
        finally:
            replica_client.close()
        sentinels = []
        for i in range(2):
            sentinels.append(
                redis_servers.start_sentinel(root / f"s{i}", master, MASTER, quorum=2)
            )
            started.append(sentinels[-1])
        guarded = redis_servers.start_sentinel(
            root / "guarded", master, "guarded", password="sentinel-pw"
        )
        started.append(guarded)
        redis_servers.wait_for_sentinels(sentinels, MASTER)
        yield master, replica, sentinels, guarded
    finally:
        for process in reversed(started):
            process.stop()


@pytest.fixture
def real_cluster(sentinel_setup, tmp_path):
    master, replica, sentinels, guarded = sentinel_setup

    def use(hosts=None, master_name=MASTER, passwords=None, name="s"):
        return _use(
            _sentinel_cluster(
                name=name, hosts=hosts or sentinels[0].address, master=master_name
            ),
            passwords=passwords,
            tmp_path=tmp_path,
        )

    return use


class TestWithRealSentinels:
    def test_status_finds_the_other_sentinel(self, sentinel_setup, real_cluster):
        master, _, sentinels, _ = sentinel_setup
        real_cluster()

        result = _call(sentinel_tools.sentinel_status)

        assert result["master"] == master.address
        assert result["agreement"] is True
        assert result["warnings"] == []
        rows = result["sentinels"]
        assert [(r["address"], r["source"]) for r in rows] == [
            (sentinels[0].address, "configured"),
            (sentinels[1].address, "discovered"),
        ]
        for row in rows:
            assert row["status"] == "ok"
            assert row["quorum"] == 2
            assert row["ckquorum"].startswith("OK 2 usable Sentinels")
            assert row["otherSentinels"] == 1
            assert row["flags"] == "master"
            assert row["tilt"] is False
            if redis_servers.server_version() >= (6, 2):
                assert len(row["id"]) == 40
            else:  # no SENTINEL MYID
                assert row["id"] is None

    def test_status_reports_a_sentinel_that_is_down(self, sentinel_setup, real_cluster):
        master, _, sentinels, _ = sentinel_setup
        closed = f"{redis_servers.HOST}:{redis_servers.free_port()}"
        real_cluster(hosts=f"{closed},{sentinels[0].address}")

        result = _call(sentinel_tools.sentinel_status)

        down = result["sentinels"][0]
        assert (down["address"], down["status"]) == (closed, "error")
        assert down["error"].startswith("ConnectionError: ")
        assert result["master"] == master.address
        (warning,) = result["warnings"]
        assert warning.startswith(f"{closed}: ConnectionError: ")

    def test_unknown_master_name(self, real_cluster, sentinel_setup):
        real_cluster(master_name="nope")

        result = _call(sentinel_tools.sentinel_status)

        assert result["warnings"][0] == "no Sentinel reported the master's state"
        assert "No such master" in result["sentinels"][0]["error"]

    def test_replicas(self, sentinel_setup, real_cluster):
        master, replica, sentinels, _ = sentinel_setup
        real_cluster()

        deadline = time.monotonic() + 15
        while True:
            result = _call(sentinel_tools.sentinel_replicas)
            if result.get("replicas") or time.monotonic() > deadline:
                break
            time.sleep(0.5)

        assert result["sentinel"] == sentinels[0].address
        (row,) = result["replicas"]
        assert row["address"] == replica.address
        assert row["master"] == master.address
        assert row["priority"] == 100

    def test_masters(self, sentinel_setup, real_cluster):
        master, _, sentinels, _ = sentinel_setup
        real_cluster()

        result = _call(sentinel_tools.sentinel_masters)

        assert result["sentinel"] == sentinels[0].address
        (row,) = result["masters"]
        assert row["name"] == MASTER
        assert row["address"] == master.address
        assert row["configuredAs"] == ["s"]

    def test_masters_matches_a_cluster_by_sentinel_id(self, sentinel_setup, tmp_path):
        """A cluster that names the other Sentinel by host name (not an address a Sentinel reports) still uses
        the same master: SENTINEL MYID tells."""
        if redis_servers.server_version() < (6, 2):
            pytest.skip("SENTINEL MYID needs Redis 6.2")
        _, _, sentinels, _ = sentinel_setup
        _use(
            _sentinel_cluster(name="s", hosts=sentinels[0].address),
            _sentinel_cluster(name="t", hosts=f"localhost:{sentinels[1].port}"),
            tmp_path=tmp_path,
        )

        result = _call(sentinel_tools.sentinel_masters)

        assert result["masters"][0]["configuredAs"] == ["s", "t"]
        assert "unverifiedClusters" not in result

    def test_sentinel_password(self, sentinel_setup, real_cluster):
        master, _, _, guarded = sentinel_setup
        real_cluster(
            hosts=guarded.address,
            master_name="guarded",
            passwords={"s_sentinel_pass": "sentinel-pw"},
        )

        result = _call(sentinel_tools.sentinel_status)

        assert result["master"] == master.address
        assert result["sentinels"][0]["status"] == "ok"

    def test_sentinel_password_missing(self, sentinel_setup, real_cluster):
        _, _, _, guarded = sentinel_setup
        real_cluster(hosts=guarded.address, master_name="guarded")

        result = _call(sentinel_tools.sentinel_status)

        row = result["sentinels"][0]
        assert row["status"] == "error"
        assert "Authentication required" in row["error"]

    def test_needs_no_master_password(self, sentinel_setup, real_cluster):
        """The tools talk to the Sentinels only: no <name>_pass for the master is configured here."""
        master, _, _, _ = sentinel_setup
        config = real_cluster()

        assert config.clusters[0].password is None
        assert _call(sentinel_tools.sentinel_status)["master"] == master.address
