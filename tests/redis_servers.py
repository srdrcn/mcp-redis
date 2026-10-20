"""
Throwaway redis-server processes for integration tests.

Every server listens on 127.0.0.1 on a free port, keeps no data on disk (``--save ''``, ``--appendonly no``)
and allows the DEBUG command (``--enable-debug-command yes`` on Redis 7+, where it is off by default), so a
test can make it hang with :func:`debug_sleep`. Use the context managers, which stop every process they
started, also when the setup or the test fails::

    with redis_servers.running(directory, password="pw") as server:
        server.client().ping()

    with redis_servers.running_cluster(directory) as nodes:  # 3 primaries, created with redis-cli
        nodes.client().set("k", "v")

Call :func:`skip_unless_installed` first (in the fixture) to skip, instead of fail, where redis-server or
redis-cli is not installed (e.g. CI runners without Redis binaries).
"""

import atexit
import re
import shutil
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator, List, Optional, Set, Tuple

import pytest
import redis
from redis.backoff import NoBackoff
from redis.cluster import ClusterNode, RedisCluster
from redis.retry import Retry

REDIS_SERVER = shutil.which("redis-server")
REDIS_CLI = shutil.which("redis-cli")

HOST = "127.0.0.1"
STARTUP_TIMEOUT = 10
# Redis Cluster nodes talk to each other on port + 10000 (the cluster bus).
CLUSTER_BUS_OFFSET = 10000
CLUSTER_SLOTS = 16384

# Every process started here, so none outlives the test run even if a fixture's teardown never runs.
_processes: Set[subprocess.Popen] = set()
_processes_lock = threading.Lock()


def skip_unless_installed(cluster: bool = False) -> None:
    """Skips the calling test or fixture when redis-server (and for a cluster, redis-cli) is not installed."""
    if REDIS_SERVER is None:
        pytest.skip("redis-server binary not found")
    if cluster and REDIS_CLI is None:
        pytest.skip("redis-cli binary not found (needed to create a Redis Cluster)")


@lru_cache(maxsize=None)
def server_version() -> Tuple[int, int]:
    """(major, minor) of the installed redis-server; (0, 0) if the output is not understood."""
    if REDIS_SERVER is None:
        return (0, 0)
    output = subprocess.run(
        [REDIS_SERVER, "--version"],
        capture_output=True,
        text=True,
        timeout=STARTUP_TIMEOUT,
        check=False,
    ).stdout
    match = re.search(r"v=(\d+)\.(\d+)", output)
    return (int(match.group(1)), int(match.group(2))) if match else (0, 0)


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((HOST, port))
        except OSError:
            return False
    return True


def free_port(cluster_bus: bool = False) -> int:
    """A port nobody listens on; with ``cluster_bus`` also port + 10000 is free."""
    for _ in range(100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((HOST, 0))
            port = s.getsockname()[1]
        if not cluster_bus:
            return port
        bus = port + CLUSTER_BUS_OFFSET
        if bus <= 65535 and _port_is_free(bus):
            return port
    raise RuntimeError("no free port pair for a Redis Cluster node")


def stop(process: subprocess.Popen) -> None:
    """Terminates the process (kills it if it does not exit) and waits for it."""
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    finally:
        with _processes_lock:
            _processes.discard(process)


@atexit.register
def _stop_all() -> None:
    with _processes_lock:
        leftovers = list(_processes)
    for process in leftovers:
        stop(process)


def _client(port: int, password: Optional[str], **kwargs: Any) -> redis.Redis:
    options = {
        "host": HOST,
        "port": port,
        "password": password,
        "decode_responses": True,
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
        # one attempt: a probe of a server that is not up yet must not spend seconds in backoff
        "retry": Retry(NoBackoff(), 0),
    }
    options.update(kwargs)
    return redis.Redis(**options)


@dataclass
class RedisServer:
    port: int
    password: Optional[str]
    process: subprocess.Popen
    directory: Path

    @property
    def host(self) -> str:
        return HOST

    @property
    def address(self) -> str:
        return f"{HOST}:{self.port}"

    def client(self, **kwargs: Any) -> redis.Redis:
        """A plain client of this server (decoding responses, no retries); kwargs override the defaults."""
        return _client(self.port, self.password, **kwargs)

    def log_tail(self, lines: int = 20) -> str:
        try:
            text = (self.directory / "redis.log").read_text(errors="replace")
        except OSError:
            return ""
        return "\n".join(text.splitlines()[-lines:])

    def stop(self) -> None:
        stop(self.process)


def start(
    directory: Path, password: Optional[str] = None, cluster_enabled: bool = False
) -> RedisServer:
    """Starts a redis-server and waits until it answers PING; retried on another port if the port was taken
    in the meantime. Stop it with :func:`stop` (or use :func:`running`)."""
    if REDIS_SERVER is None:
        raise RuntimeError("redis-server binary not found")
    directory.mkdir(parents=True, exist_ok=True)
    last_error = None
    for _ in range(3):
        port = free_port(cluster_bus=cluster_enabled)
        args = [
            REDIS_SERVER,
            "--port",
            str(port),
            "--bind",
            HOST,
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            str(directory),
            "--logfile",
            str(directory / "redis.log"),
            "--daemonize",
            "no",
        ]
        if server_version() >= (7, 0):
            args += ["--enable-debug-command", "yes"]
        if password:
            args += ["--requirepass", password]
        if cluster_enabled:
            args += [
                "--cluster-enabled",
                "yes",
                "--cluster-config-file",
                str(directory / "nodes.conf"),
                "--cluster-node-timeout",
                "5000",
            ]
        process = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with _processes_lock:
            _processes.add(process)
        server = RedisServer(port, password, process, directory)
        try:
            deadline = time.monotonic() + STARTUP_TIMEOUT
            while time.monotonic() < deadline and process.poll() is None:
                probe = server.client(socket_connect_timeout=0.5, socket_timeout=0.5)
                try:
                    if probe.ping():
                        return server
                except redis.RedisError as e:
                    last_error = e
                finally:
                    probe.close()
                time.sleep(0.05)
        except BaseException:
            stop(process)
            raise
        last_error = f"{last_error}; log:\n{server.log_tail()}"
        stop(process)
    raise RuntimeError(f"redis-server did not start: {last_error}")


@contextmanager
def running(directory: Path, password: Optional[str] = None) -> Iterator[RedisServer]:
    """A standalone redis-server for the duration of the block."""
    server = start(directory, password)
    try:
        yield server
    finally:
        server.stop()


@dataclass
class RedisClusterNodes:
    """The primaries of a Redis Cluster (no replicas)."""

    nodes: List[RedisServer] = field(default_factory=list)

    @property
    def addresses(self) -> List[str]:
        return [n.address for n in self.nodes]

    @property
    def ports(self) -> List[int]:
        return [n.port for n in self.nodes]

    def client(self, **kwargs: Any) -> RedisCluster:
        options = {
            "decode_responses": True,
            "socket_connect_timeout": 5,
            "socket_timeout": 5,
        }
        options.update(kwargs)
        return RedisCluster(
            startup_nodes=[ClusterNode(HOST, n.port) for n in self.nodes], **options
        )

    def node_for_key(self, key: str) -> RedisServer:
        """The node serving the key's hash slot."""
        probe = self.nodes[0].client()
        try:
            slot = probe.cluster("KEYSLOT", key)
            for start_slot, end_slot, primary, *_ in probe.execute_command(
                "CLUSTER SLOTS"
            ):
                if start_slot <= slot <= end_slot:
                    port = int(primary[1])
                    return next(n for n in self.nodes if n.port == port)
        finally:
            probe.close()
        raise LookupError(f"no node serves the slot of {key}")

    def stop(self) -> None:
        for node in self.nodes:
            node.stop()


def _cluster_is_ready(nodes: List[RedisServer]) -> bool:
    for node in nodes:
        probe = node.client(socket_connect_timeout=0.5, socket_timeout=0.5)
        try:
            info = probe.cluster("INFO")
        except redis.RedisError:
            return False
        finally:
            probe.close()
        if (
            info.get("cluster_state") != "ok"
            or int(info.get("cluster_slots_ok", 0)) != CLUSTER_SLOTS
            or int(info.get("cluster_known_nodes", 0)) != len(nodes)
        ):
            return False
    return True


def start_cluster(directory: Path, size: int = 3) -> RedisClusterNodes:
    """Starts ``size`` cluster-enabled servers, joins them into a cluster with ``redis-cli --cluster create``
    and waits until every node reports cluster_state:ok. Stop it with ``.stop()`` (or use
    :func:`running_cluster`)."""
    if REDIS_CLI is None:
        raise RuntimeError("redis-cli binary not found")
    cluster = RedisClusterNodes()
    try:
        for i in range(size):
            cluster.nodes.append(start(directory / f"node{i}", cluster_enabled=True))
        created = subprocess.run(
            [REDIS_CLI, "--cluster", "create", *cluster.addresses, "--cluster-yes"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if created.returncode != 0:
            raise RuntimeError(
                f"redis-cli --cluster create failed: {created.stdout}{created.stderr}"
            )
        deadline = time.monotonic() + STARTUP_TIMEOUT * 3
        while not _cluster_is_ready(cluster.nodes):
            if time.monotonic() > deadline:
                raise RuntimeError("the Redis Cluster did not reach cluster_state:ok")
            time.sleep(0.1)
        return cluster
    except BaseException:
        cluster.stop()
        raise


@contextmanager
def running_cluster(directory: Path, size: int = 3) -> Iterator[RedisClusterNodes]:
    """A Redis Cluster of ``size`` primaries for the duration of the block."""
    cluster = start_cluster(directory, size)
    try:
        yield cluster
    finally:
        cluster.stop()


def _wait_until_busy(
    server: RedisServer, sleeper: threading.Thread, deadline: float
) -> bool:
    """True once the server stops answering (an inline PING on a connection of its own gets no reply); False
    if the sleeper ended or the deadline passed first."""
    with socket.create_connection((HOST, server.port), timeout=1) as probe:
        probe.settimeout(0.2)
        while time.monotonic() < deadline and sleeper.is_alive():
            # any reply, even -NOAUTH on a server with a password, means it still processes commands
            probe.sendall(b"PING\r\n")
            try:
                if not probe.recv(1024):
                    raise RuntimeError("the server closed the probe connection")
            except socket.timeout:
                return True
            time.sleep(0.01)
    return False


@contextmanager
def debug_sleep(server: RedisServer, seconds: float) -> Iterator[None]:
    """Makes the server hang: DEBUG SLEEP runs on a connection of its own, and the block starts once the
    server has stopped answering. On exit it waits until the sleep is over (the server answers again)."""
    sleeper = server.client(socket_timeout=seconds + STARTUP_TIMEOUT)
    errors: List[BaseException] = []

    def sleep() -> None:
        try:
            sleeper.execute_command("DEBUG", "SLEEP", seconds)
        except BaseException as e:  # reported below
            errors.append(e)

    thread = threading.Thread(target=sleep, name="redis-debug-sleep", daemon=True)
    thread.start()
    try:
        if not _wait_until_busy(server, thread, time.monotonic() + STARTUP_TIMEOUT):
            raise RuntimeError(
                f"DEBUG SLEEP did not make the server hang: {errors or 'no error'}"
            )
        yield
    finally:
        thread.join(seconds + STARTUP_TIMEOUT)
        sleeper.close()
    if thread.is_alive():
        raise RuntimeError("DEBUG SLEEP did not finish")
    if errors:
        raise RuntimeError(f"DEBUG SLEEP failed: {errors[0]}") from errors[0]
