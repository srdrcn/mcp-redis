"""
Unit tests for src/common/registry.py
"""

import asyncio
import json
import logging
import threading
from unittest.mock import Mock, patch

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from src.common import registry
from src.common.config_loader import (
    ConfigError,
    ServerConfig,
    from_legacy,
    parse_config,
    set_config,
)
from src.common.connection import SOCKET_TIMEOUT_SECONDS, current_cluster
from src.common.registry import (
    TOOLSETS,
    ToolKind,
    ToolSpec,
    annotations,
    call,
    eligible_clusters,
    enabled_toolsets,
    reason_unavailable,
    redis_tool,
    select,
    wrap,
)
from src.common.server import build_server, load_tools

# Every tool of src/tools, by toolset. A tool moving to another toolset or kind must show up here.
CORE_READ = {
    "clusters_list",
    "type",
    "scan_keys",
    "scan_all_keys",
    "dbsize",
    "info",
    "client_list",
}
DATA_READ = {
    "hget",
    "hgetall",
    "hexists",
    "get_vector_from_hash",
    "lrange",
    "llen",
    "smembers",
    "zrange",
    "xrange",
    "get",
}
JSON_READ = {"json_get"}
DESTRUCTIVE_TOOLS = {
    "delete",
    "hdel",
    "json_del",
    "lpop",
    "rpop",
    "lrem",
    "srem",
    "zrem",
    "xdel",
    "xgroup_destroy",
}
NO_CLUSTER_TOOLS = {"search_redis_documents", "read_messages", "unsubscribe"}
REAL_TOOL_COUNT = 54


def _config(**root) -> ServerConfig:
    """A config from YAML-style (camelCase) keys; one cluster named ``a`` unless clusters are given."""
    data = {"clusters": [{"name": "a", "hosts": "a:6379"}]}
    data.update(root)
    return parse_config(data)


def _cluster(name: str, **fields):
    return {"name": name, "hosts": f"{name}:6379", **fields}


def _declare(
    name: str,
    toolset: str = "core",
    kind: ToolKind = ToolKind.READ,
    requires=None,
    cluster="required",
    doc="Read something.",
):
    """Declares a synthetic tool ``name(key: str, count: int = 10)`` in the (isolated) registry."""

    def fn(key: str, count: int = 10) -> str:
        return f"{key}:{count}"

    fn.__name__ = name
    fn.__qualname__ = name
    fn.__doc__ = doc
    return redis_tool(toolset, kind, requires=requires, cluster=cluster)(fn)


def _selected(config: ServerConfig):
    return [(spec.name, clusters) for spec, clusters in select(config)]


def _spec(fn, name="probe", kind=ToolKind.READ, cluster="required") -> ToolSpec:
    return ToolSpec(
        name=name,
        toolset="core",
        kind=kind,
        fn=fn,
        description="Probe.",
        cluster=cluster,
    )


def _tool_call_records(caplog):
    return [
        r
        for r in caplog.records
        if r.name == "src.common.registry" and r.getMessage().startswith("tool call")
    ]


async def _listed(spec: ToolSpec, clusters):
    """The tool as an MCP client sees it after FastMCP registered ``wrap(spec, clusters)``."""
    server = FastMCP("test")
    server.add_tool(
        wrap(spec, clusters),
        name=spec.name,
        description=spec.description,
        annotations=annotations(spec),
    )
    (tool,) = await server.list_tools()
    return tool


@pytest.fixture
def isolated_registry(monkeypatch):
    """An empty registry for synthetic tools; the real tools are imported first so they stay registered."""
    load_tools()
    monkeypatch.setattr(registry, "_SPECS", {})
    return registry._SPECS


@pytest.fixture
def real_tools(monkeypatch):
    """Only the tools of src/tools, even if another test left a synthetic one in the registry."""
    load_tools()
    monkeypatch.setattr(
        registry,
        "_SPECS",
        {
            name: spec
            for name, spec in registry._SPECS.items()
            if spec.fn.__module__.startswith("src.tools.")
        },
    )
    return registry.specs()


class TestToolKind:
    def test_writes(self):
        assert ToolKind.READ.writes is False
        assert ToolKind.WRITE.writes is True
        assert ToolKind.DESTRUCTIVE.writes is True

    def test_toolset_names(self):
        assert list(TOOLSETS) == ["core", "data", "json", "search", "pubsub", "docs"]

    def test_tool_timeout_is_above_the_client_socket_timeout(self):
        # a slow cluster must produce its own error before the generic tool timeout
        assert registry.TOOL_TIMEOUT_SECONDS == 15
        assert SOCKET_TIMEOUT_SECONDS < registry.TOOL_TIMEOUT_SECONDS


class TestRedisTool:
    def test_records_spec_and_returns_function_unchanged(self, isolated_registry):
        def lookup(key: str) -> str:
            """Look a key up."""
            return key

        decorated = redis_tool("data", requires="json", cluster="optional")(lookup)

        assert decorated is lookup
        spec = isolated_registry["lookup"]
        assert spec == ToolSpec(
            name="lookup",
            toolset="data",
            kind=ToolKind.READ,
            fn=lookup,
            description="Look a key up.",
            requires="json",
            cluster="optional",
        )
        assert registry.specs() == [spec]

    def test_defaults(self, isolated_registry):
        _declare("plain")

        spec = isolated_registry["plain"]
        assert spec.kind is ToolKind.READ
        assert spec.requires is None
        assert spec.cluster == "required"

    @pytest.mark.parametrize("kind", [ToolKind.WRITE, ToolKind.DESTRUCTIVE])
    def test_write_tools_get_write_prefix(self, isolated_registry, kind):
        _declare("changer", kind=kind, doc="Change something.")

        assert isolated_registry["changer"].description == "WRITE: Change something."

    def test_read_tools_keep_their_description(self, isolated_registry):
        _declare("reader", doc="Read something.")

        assert isolated_registry["reader"].description == "Read something."

    def test_existing_write_prefix_is_not_doubled(self, isolated_registry):
        _declare("changer", kind=ToolKind.WRITE, doc="WRITE: Change something.")

        assert isolated_registry["changer"].description == "WRITE: Change something."

    def test_missing_docstring(self, isolated_registry):
        _declare("bare", doc=None)
        _declare("bare_writer", kind=ToolKind.WRITE, doc=None)

        assert isolated_registry["bare"].description == ""
        assert isolated_registry["bare_writer"].description.startswith("WRITE:")

    def test_docstring_is_dedented(self, isolated_registry):
        def indented():
            """First line.

            Args:
                nothing
            """

        redis_tool("core")(indented)

        assert (
            isolated_registry["indented"].description
            == "First line.\n\nArgs:\n    nothing"
        )

    def test_unknown_toolset_is_rejected(self, isolated_registry):
        with pytest.raises(ValueError, match="Unknown toolset: nope"):
            redis_tool("nope")
        assert isolated_registry == {}

    def test_duplicate_name_from_another_module_is_rejected(self, isolated_registry):
        first = _declare("twin")

        def twin():
            """Other twin."""

        twin.__module__ = "somewhere.else"
        with pytest.raises(ValueError, match="Duplicate tool name: twin"):
            redis_tool("core")(twin)
        assert isolated_registry["twin"].fn is first

    def test_redeclaring_from_the_same_module_replaces_the_spec(
        self, isolated_registry
    ):
        # importlib.reload of a tools module declares its tools again
        _declare("again", doc="Old.")
        newer = _declare("again", doc="New.")

        assert len(isolated_registry) == 1
        assert isolated_registry["again"].fn is newer
        assert isolated_registry["again"].description == "New."


class TestReasonUnavailable:
    def test_eligible(self, isolated_registry):
        _declare("reader")
        config = _config()

        assert (
            reason_unavailable(config, isolated_registry["reader"], config.clusters[0])
            is None
        )

    def test_missing_module(self, isolated_registry):
        _declare("doc_reader", requires="json")
        config = _config(clusters=[_cluster("a", modules=["search"])])

        assert (
            reason_unavailable(
                config, isolated_registry["doc_reader"], config.clusters[0]
            )
            == "cluster a does not list the json module in modules"
        )

    @pytest.mark.parametrize("kind", [ToolKind.WRITE, ToolKind.DESTRUCTIVE])
    def test_read_only_cluster(self, isolated_registry, kind):
        _declare("changer", kind=kind)
        config = _config(readOnly=True)

        assert (
            reason_unavailable(config, isolated_registry["changer"], config.clusters[0])
            == "cluster a is readOnly; write tools are disabled"
        )

    def test_read_tool_on_read_only_cluster(self, isolated_registry):
        _declare("reader")
        config = _config(readOnly=True, disableDestructive=True)

        assert (
            reason_unavailable(config, isolated_registry["reader"], config.clusters[0])
            is None
        )

    def test_destructive_disabled(self, isolated_registry):
        _declare("remover", kind=ToolKind.DESTRUCTIVE)
        _declare("writer", kind=ToolKind.WRITE)
        config = _config(disableDestructive=True)
        cluster = config.clusters[0]

        assert (
            reason_unavailable(config, isolated_registry["remover"], cluster)
            == "disableDestructive is on; delete/pop/remove tools are disabled"
        )
        assert reason_unavailable(config, isolated_registry["writer"], cluster) is None

    def test_module_is_reported_before_read_only(self, isolated_registry):
        _declare("doc_writer", kind=ToolKind.DESTRUCTIVE, requires="json")
        config = _config(readOnly=True, disableDestructive=True)

        assert "json module" in reason_unavailable(
            config, isolated_registry["doc_writer"], config.clusters[0]
        )

    def test_eligible_clusters_keep_config_order(self, isolated_registry):
        _declare("writer", kind=ToolKind.WRITE)
        config = _config(
            clusters=[
                _cluster("z"),
                _cluster("ro", readOnly=True),
                _cluster("b"),
            ]
        )

        assert eligible_clusters(config, isolated_registry["writer"]) == ["z", "b"]


class TestSelect:
    @pytest.fixture
    def tools(self, isolated_registry):
        _declare("c_read")
        _declare("c_write", kind=ToolKind.WRITE)
        _declare("c_delete", kind=ToolKind.DESTRUCTIVE)
        _declare("c_optional", cluster="optional")
        _declare("d_read", toolset="data")
        _declare("j_read", toolset="json", requires="json")
        _declare("s_read", toolset="search", requires="search")
        _declare("docs_search", toolset="docs", cluster="none")
        return isolated_registry

    def test_only_enabled_toolsets(self, tools):
        assert [name for name, _ in _selected(_config())] == [
            "c_read",
            "c_write",
            "c_delete",
            "c_optional",
        ]

    def test_toolsets_order(self, tools):
        names = [name for name, _ in _selected(_config(toolsets=["data", "core"]))]

        assert names == ["d_read", "c_read", "c_write", "c_delete", "c_optional"]

    def test_unknown_toolset(self, tools):
        config = _config(toolsets=["core", "nope"])

        with pytest.raises(ConfigError) as info:
            select(config)
        message = str(info.value)
        unknown, _, valid = message.partition("valid")
        assert "Unknown toolset" in unknown
        assert "nope" in unknown
        assert "core" not in unknown
        for name in TOOLSETS:
            assert f"'{name}'" in valid

    def test_enabled_toolsets(self):
        assert enabled_toolsets(_config()) == ["core"]
        assert enabled_toolsets(_config(toolsets=["pubsub", "core"])) == [
            "pubsub",
            "core",
        ]

    def test_enabled_toolsets_in_legacy_mode_are_all(self, redis_config):
        legacy = from_legacy(redis_config)

        assert legacy.toolsets == ["core"]
        assert enabled_toolsets(legacy) == list(TOOLSETS)

    def test_global_read_only_hides_write_tools(self, tools):
        selected = dict(_selected(_config(readOnly=True)))

        assert set(selected) == {"c_read", "c_optional"}
        assert selected["c_read"] == ["a"]

    def test_cluster_read_only_false_overrides_global(self, tools):
        config = _config(
            readOnly=True,
            clusters=[_cluster("a"), _cluster("b", readOnly=False)],
        )

        selected = dict(_selected(config))

        assert selected["c_read"] == ["a", "b"]
        assert selected["c_write"] == ["b"]
        assert selected["c_delete"] == ["b"]

    def test_cluster_read_only_true_overrides_global(self, tools):
        config = _config(
            readOnly=False,
            clusters=[_cluster("a", readOnly=True), _cluster("b")],
        )

        selected = dict(_selected(config))

        assert selected["c_read"] == ["a", "b"]
        assert selected["c_write"] == ["b"]
        assert selected["c_delete"] == ["b"]

    def test_disable_destructive(self, tools):
        selected = dict(_selected(_config(disableDestructive=True)))

        assert "c_delete" not in selected
        assert selected["c_write"] == ["a"]

    def test_modules(self, tools):
        config = _config(
            toolsets=["json", "search"],
            clusters=[
                _cluster("plain"),
                _cluster("docs", modules=["json"]),
                _cluster("both", modules=["json", "search"]),
            ],
        )

        assert _selected(config) == [
            ("j_read", ["docs", "both"]),
            ("s_read", ["both"]),
        ]

    def test_tool_without_eligible_cluster_is_omitted(self, tools):
        config = _config(toolsets=["core", "json", "search"])

        names = [name for name, _ in _selected(config)]

        assert "j_read" not in names
        assert "s_read" not in names
        assert "c_read" in names

    def test_cluster_none_tools_are_listed_regardless(self, tools):
        config = _config(
            toolsets=["docs", "json"],
            readOnly=True,
            disableDestructive=True,
        )

        assert _selected(config) == [("docs_search", [])]

    def test_optional_cluster_tools_are_filtered_like_required_ones(
        self, isolated_registry
    ):
        _declare("o_write", kind=ToolKind.WRITE, cluster="optional")
        config = _config(clusters=[_cluster("a", readOnly=True), _cluster("b")])

        assert _selected(config) == [("o_write", ["b"])]
        assert _selected(_config(readOnly=True)) == []


class TestRealTools:
    def test_tool_count(self, real_tools):
        assert len(real_tools) == REAL_TOOL_COUNT

    def test_write_prefix_matches_kind(self, real_tools):
        for spec in real_tools:
            assert spec.description.startswith("WRITE:") == spec.kind.writes, spec.name

    def test_cluster_parameter_kinds(self, real_tools):
        by_cluster = {}
        for spec in real_tools:
            by_cluster.setdefault(spec.cluster, set()).add(spec.name)

        assert by_cluster["none"] == NO_CLUSTER_TOOLS
        assert by_cluster["optional"] == {"clusters_list"}

    def test_destructive_tools(self, real_tools):
        assert {
            s.name for s in real_tools if s.kind is ToolKind.DESTRUCTIVE
        } == DESTRUCTIVE_TOOLS

    def test_legacy_config_lists_every_tool(self, real_tools, redis_config):
        selected = select(from_legacy(redis_config))

        assert len(selected) == REAL_TOOL_COUNT
        assert {spec.name for spec, _ in selected} == {s.name for s in real_tools}
        for spec, clusters in selected:
            assert clusters == ([] if spec.cluster == "none" else ["default"])

    def test_representative_read_only_config(self, real_tools):
        config = _config(
            readOnly=True,
            disableDestructive=True,
            toolsets=["core", "data", "json"],
            clusters=[_cluster("cbtstrds", modules=["json"])],
        )

        selected = select(config)
        toolsets = [spec.toolset for spec, _ in selected]
        names = [spec.name for spec, _ in selected]

        assert toolsets == ["core"] * 7 + ["data"] * 10 + ["json"]
        assert set(names[:7]) == CORE_READ
        assert set(names[7:17]) == DATA_READ
        assert names[17:] == ["json_get"]
        assert all(clusters == ["cbtstrds"] for _, clusters in selected)

    def test_writable_config_without_destructive_tools(self, real_tools):
        config = _config(
            disableDestructive=True,
            toolsets=list(TOOLSETS),
            clusters=[_cluster("a", modules=["json", "search"])],
        )

        names = {spec.name for spec, _ in select(config)}

        assert names == {s.name for s in real_tools} - DESTRUCTIVE_TOOLS


class TestAnnotations:
    @pytest.mark.parametrize(
        "kind, read_only, destructive",
        [
            (ToolKind.READ, True, False),
            (ToolKind.WRITE, False, False),
            (ToolKind.DESTRUCTIVE, False, True),
        ],
    )
    def test_hints(self, kind, read_only, destructive):
        hints = annotations(_spec(lambda: None, kind=kind))

        assert hints.readOnlyHint is read_only
        assert hints.destructiveHint is destructive


class TestWrap:
    @staticmethod
    def _tool_spec(cluster="required", kind=ToolKind.READ) -> ToolSpec:
        def lookup(key: str, count: int = 10) -> str:
            """Look a key up."""
            return f"{key}:{count}"

        return ToolSpec(
            name="lookup",
            toolset="core",
            kind=kind,
            fn=lookup,
            description="Look a key up.",
            cluster=cluster,
        )

    def test_wrapper_metadata(self):
        spec = self._tool_spec()

        wrapped = wrap(spec, ["a"])

        assert wrapped.__name__ == "lookup"
        assert wrapped.__doc__ == "Look a key up."
        assert asyncio.iscoroutinefunction(wrapped)

    @pytest.mark.asyncio
    async def test_required_cluster_is_first_and_required(self):
        tool = await _listed(self._tool_spec(), ["a", "b"])
        schema = tool.inputSchema

        assert list(schema["properties"]) == ["cluster", "key", "count"]
        assert schema["required"] == ["cluster", "key"]
        assert schema["properties"]["cluster"]["type"] == "string"
        assert schema["properties"]["cluster"]["enum"] == ["a", "b"]
        assert schema["properties"]["cluster"]["description"] == "Cluster name"
        assert schema["properties"]["count"]["default"] == 10

    @pytest.mark.asyncio
    async def test_single_cluster_is_an_enum_not_a_const(self):
        tool = await _listed(self._tool_spec(), ["only"])

        cluster = tool.inputSchema["properties"]["cluster"]
        assert cluster["enum"] == ["only"]
        assert "const" not in json.dumps(tool.inputSchema)

    @pytest.mark.asyncio
    async def test_optional_cluster_is_last_and_nullable(self):
        tool = await _listed(self._tool_spec(cluster="optional"), ["a", "b"])
        schema = tool.inputSchema

        assert list(schema["properties"]) == ["key", "count", "cluster"]
        assert schema["required"] == ["key"]
        cluster = schema["properties"]["cluster"]
        assert cluster["default"] is None
        assert {"type": "string", "enum": ["a", "b"]} in cluster["anyOf"]
        assert {"type": "null"} in cluster["anyOf"]

    @pytest.mark.asyncio
    async def test_optional_single_cluster_is_an_enum(self):
        tool = await _listed(self._tool_spec(cluster="optional"), ["only"])

        assert "const" not in json.dumps(tool.inputSchema)
        assert {"type": "string", "enum": ["only"]} in tool.inputSchema["properties"][
            "cluster"
        ]["anyOf"]

    @pytest.mark.asyncio
    async def test_no_cluster_parameter(self):
        tool = await _listed(self._tool_spec(cluster="none"), [])
        schema = tool.inputSchema

        assert list(schema["properties"]) == ["key", "count"]
        assert schema["required"] == ["key"]

    @pytest.mark.parametrize(
        "kind, read_only, destructive",
        [
            (ToolKind.READ, True, False),
            (ToolKind.WRITE, False, False),
            (ToolKind.DESTRUCTIVE, False, True),
        ],
    )
    @pytest.mark.asyncio
    async def test_listed_annotations(self, kind, read_only, destructive):
        tool = await _listed(self._tool_spec(kind=kind), ["a"])

        assert tool.annotations.readOnlyHint is read_only
        assert tool.annotations.destructiveHint is destructive

    @pytest.mark.asyncio
    async def test_wrapper_passes_the_cluster_to_call(self):
        spec = self._tool_spec()
        wrapped = wrap(spec, ["a"])

        with patch("src.common.registry.call", return_value="done") as mock_call:
            result = await wrapped(cluster="a", key="k", count=3)

        assert result == "done"
        mock_call.assert_called_once_with(spec, "a", {"key": "k", "count": 3})

    @pytest.mark.asyncio
    async def test_wrapper_without_cluster_passes_none(self):
        spec = self._tool_spec(cluster="none")
        wrapped = wrap(spec, [])

        with patch("src.common.registry.call", return_value="done") as mock_call:
            await wrapped(key="k", count=3)

        mock_call.assert_called_once_with(spec, None, {"key": "k", "count": 3})


class TestCall:
    @pytest.mark.asyncio
    async def test_sync_tool_sees_cluster_in_worker_thread(self):
        def probe(key):
            return current_cluster.get(), threading.get_ident(), key

        cluster, thread, key = await call(_spec(probe), "a", {"key": "k"})

        assert cluster == "a"
        assert key == "k"
        assert thread != threading.get_ident()

    @pytest.mark.asyncio
    async def test_async_tool_sees_cluster_in_worker_thread(self):
        async def probe(key):
            await asyncio.sleep(0)
            return current_cluster.get(), threading.get_ident(), key

        cluster, thread, key = await call(_spec(probe), "b", {"key": "k"})

        assert cluster == "b"
        assert key == "k"
        assert thread != threading.get_ident()

    @pytest.mark.asyncio
    async def test_returns_the_tool_result(self):
        async def probe(a, b):
            return {"sum": a + b}

        assert await call(_spec(probe), "a", {"a": 1, "b": 2}) == {"sum": 3}

    @pytest.mark.asyncio
    async def test_no_cluster(self):
        def probe():
            return current_cluster.get()

        assert await call(_spec(probe, cluster="none"), None, {}) is None

    @pytest.mark.asyncio
    async def test_cluster_is_reset_afterwards(self):
        def probe():
            return current_cluster.get()

        assert current_cluster.get() is None
        assert await call(_spec(probe), "a", {}) == "a"
        assert current_cluster.get() is None

        token = current_cluster.set("outer")
        try:
            assert await call(_spec(probe), "a", {}) == "a"
            assert current_cluster.get() == "outer"
        finally:
            current_cluster.reset(token)

    @pytest.mark.asyncio
    async def test_cluster_is_reset_after_an_error(self):
        def probe():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await call(_spec(probe), "a", {})
        assert current_cluster.get() is None

    @pytest.mark.asyncio
    async def test_error_is_raised_unchanged(self):
        error = ValueError("bad input")

        async def probe():
            raise error

        with pytest.raises(ValueError) as info:
            await call(_spec(probe), "a", {})
        assert info.value is error

    @pytest.mark.asyncio
    async def test_timeout(self, monkeypatch, caplog):
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 0.05)
        release = threading.Event()
        started = threading.Event()

        def probe():
            started.set()
            release.wait(5)
            return "late"

        caplog.set_level(logging.INFO, logger="src.common.registry")
        try:
            with pytest.raises(ToolError) as info:
                await call(_spec(probe), "a", {})
        finally:
            release.set()

        assert started.is_set()
        message = str(info.value)
        assert "did not finish within 0.05 s (timeout)" in message
        assert current_cluster.get() is None
        (record,) = _tool_call_records(caplog)
        assert record.outcome == "error"
        assert record.error == message
        assert record.durationMs >= 40

    @pytest.mark.asyncio
    async def test_async_tool_timeout(self, monkeypatch):
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 0.05)
        release = threading.Event()

        async def probe():
            while not release.is_set():
                await asyncio.sleep(0.01)

        try:
            with pytest.raises(ToolError, match="timeout"):
                await call(_spec(probe), "a", {})
        finally:
            release.set()

    @pytest.mark.asyncio
    async def test_logs_one_record_for_a_successful_call(self, caplog):
        caplog.set_level(logging.INFO, logger="src.common.registry")

        await call(_spec(lambda: "ok", name="lookup"), "a", {})

        (record,) = _tool_call_records(caplog)
        assert record.levelno == logging.INFO
        assert record.tool == "lookup"
        assert record.cluster == "a"
        assert isinstance(record.durationMs, int)
        assert record.durationMs >= 0
        assert record.outcome == "ok"
        assert record.error is None
        assert "tool=lookup" in record.getMessage()
        assert "cluster=a" in record.getMessage()
        assert "outcome=ok" in record.getMessage()

    @pytest.mark.asyncio
    async def test_logs_one_record_for_a_failed_call(self, caplog):
        def probe():
            raise RuntimeError("connection refused")

        caplog.set_level(logging.INFO, logger="src.common.registry")

        with pytest.raises(RuntimeError):
            await call(_spec(probe, name="lookup"), "b", {})

        (record,) = _tool_call_records(caplog)
        assert record.tool == "lookup"
        assert record.cluster == "b"
        assert record.outcome == "error"
        assert record.error == "connection refused"
        assert "outcome=error" in record.getMessage()

    @pytest.mark.asyncio
    async def test_log_record_without_cluster(self, caplog):
        caplog.set_level(logging.INFO, logger="src.common.registry")

        await call(_spec(lambda: None, cluster="none"), None, {})

        (record,) = _tool_call_records(caplog)
        assert record.cluster is None


class TestEndToEnd:
    """build_server(config) and the MCP calls a client makes, with a mocked Redis client."""

    @pytest.fixture
    def config(self):
        config = _config(
            readOnly=True,
            toolsets=["core", "data", "json"],
            clusters=[
                _cluster("rw", readOnly=False, modules=["json"]),
                _cluster("ro"),
            ],
        )
        set_config(config)
        return config

    @pytest.fixture
    def redis_client(self):
        """get_connection returns a mock and records the cluster each call ran for."""
        client = Mock()
        client.seen_clusters = []

        def get_connection(*args, **kwargs):
            client.seen_clusters.append(current_cluster.get())
            return client

        with patch(
            "src.common.connection.RedisConnectionManager.get_connection",
            side_effect=get_connection,
        ) as mock_get_connection:
            client.get_connection = mock_get_connection
            yield client

    @pytest.mark.asyncio
    async def test_representative_config_lists_exact_tools(self, real_tools):
        config = _config(
            readOnly=True,
            disableDestructive=True,
            toolsets=["core", "data", "json"],
            clusters=[_cluster("cbtstrds", modules=["json"])],
        )
        mcp = build_server(config)

        tools = await mcp.list_tools()

        assert {t.name for t in tools} == CORE_READ | DATA_READ | JSON_READ
        assert len(tools) == len(CORE_READ | DATA_READ | JSON_READ)
        for tool in tools:
            assert tool.annotations.readOnlyHint is True
            assert not tool.description.startswith("WRITE:")
            cluster = tool.inputSchema["properties"]["cluster"]
            enum = cluster["enum"] if "enum" in cluster else cluster["anyOf"][0]["enum"]
            assert enum == ["cbtstrds"]

    @pytest.mark.asyncio
    async def test_cluster_enums_follow_eligibility(self, real_tools, config):
        mcp = build_server(config)

        tools = {t.name: t for t in await mcp.list_tools()}

        def enum(name):
            return tools[name].inputSchema["properties"]["cluster"]["enum"]

        assert enum("get") == ["rw", "ro"]
        assert enum("set") == ["rw"]
        assert enum("delete") == ["rw"]
        assert enum("json_get") == ["rw"]
        assert tools["set"].description.startswith("WRITE:")
        assert tools["delete"].annotations.destructiveHint is True
        assert "search_redis_documents" not in tools

    @pytest.mark.asyncio
    async def test_call_runs_on_the_given_cluster(
        self, real_tools, config, redis_client, caplog
    ):
        redis_client.get.return_value = "value"
        mcp = build_server(config)
        caplog.set_level(logging.INFO, logger="src.common.registry")

        content, structured = await mcp.call_tool("get", {"cluster": "ro", "key": "k"})

        assert content[0].text == "value"
        assert structured == {"result": "value"}
        redis_client.get.assert_called_once_with("k")
        assert redis_client.seen_clusters == ["ro"]
        assert current_cluster.get() is None
        (record,) = _tool_call_records(caplog)
        assert (record.tool, record.cluster, record.outcome) == ("get", "ro", "ok")

    @pytest.mark.asyncio
    async def test_write_call_on_writable_cluster(
        self, real_tools, config, redis_client
    ):
        mcp = build_server(config)

        content, _ = await mcp.call_tool(
            "set", {"cluster": "rw", "key": "k", "value": "v"}
        )

        assert "Successfully set k" in content[0].text
        redis_client.set.assert_called_once_with("k", b"v")
        assert redis_client.seen_clusters == ["rw"]

    @pytest.mark.parametrize(
        "name, arguments",
        [
            ("get", {"cluster": "nope", "key": "k"}),
            # configured, but readOnly: not in the enum of a write tool
            ("set", {"cluster": "ro", "key": "k", "value": "v"}),
            # configured, but without the json module
            ("json_get", {"cluster": "ro", "name": "doc"}),
        ],
    )
    @pytest.mark.asyncio
    async def test_cluster_outside_the_enum_is_rejected(
        self, real_tools, config, redis_client, name, arguments
    ):
        mcp = build_server(config)

        with pytest.raises(ToolError, match="cluster"):
            await mcp.call_tool(name, arguments)

        redis_client.get_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_required_cluster_is_rejected(
        self, real_tools, config, redis_client
    ):
        mcp = build_server(config)

        with pytest.raises(ToolError, match="Field required"):
            await mcp.call_tool("get", {"key": "k"})

        redis_client.get_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_unlisted_tool_is_unknown(self, real_tools, config):
        mcp = build_server(config)

        with pytest.raises(ToolError, match="Unknown tool"):
            await mcp.call_tool("search_redis_documents", {"question": "x"})

    @pytest.mark.asyncio
    async def test_optional_cluster(self, real_tools, config, redis_client):
        mcp = build_server(config)

        _, every = await mcp.call_tool("clusters_list", {"check": False})
        _, one = await mcp.call_tool("clusters_list", {"check": False, "cluster": "ro"})

        assert [row["name"] for row in every["result"]] == ["rw", "ro"]
        assert [row["name"] for row in one["result"]] == ["ro"]
        assert one["result"][0]["readOnly"] is True
        redis_client.get_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_tool_timeout(self, real_tools, config, redis_client, monkeypatch):
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 0.05)
        release = threading.Event()
        redis_client.get.side_effect = lambda key: release.wait(5) and "late"
        mcp = build_server(config)

        try:
            with pytest.raises(ToolError, match=r"did not finish within 0\.05 s"):
                await mcp.call_tool("get", {"cluster": "rw", "key": "k"})
        finally:
            release.set()
