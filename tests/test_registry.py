"""
Unit tests for src/common/registry.py
"""

import asyncio
import contextvars
import inspect
import json
import logging
import threading
import time
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
from src.common.connection import (
    SOCKET_TIMEOUT_SECONDS,
    ClusterSelectionError,
    current_cluster,
)
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
    "cluster_nodes",
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
REAL_TOOL_COUNT = 55
ONLY_CLUSTER_DESCRIPTION = "Cluster name (default: the only configured cluster)"
ALL_CLUSTERS_DESCRIPTION = "Cluster name (default: all clusters)"

# set by a caller; a tool call must see it in its worker thread
_REQUEST_ID: contextvars.ContextVar = contextvars.ContextVar(
    "test_request_id", default=None
)


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


def _timeout_message(seconds) -> str:
    return (
        f"Tool did not finish within {seconds} s (timeout). The cluster may be slow or unreachable"
        " (check clusters_list), or the request too large (narrow the pattern or count)."
    )


def _blocking(release: threading.Event, result="released"):
    """A synthetic tool that waits for ``release``; its ``started`` event is set once it runs."""
    started = threading.Event()

    def blocked():
        started.set()
        release.wait(10)
        return result

    blocked.started = started
    return blocked


async def _wait(event: threading.Event, timeout: float = 5) -> bool:
    """Waits for a threading.Event without blocking the event loop."""
    deadline = time.monotonic() + timeout
    while not event.is_set():
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.005)
    return True


async def _listed(spec: ToolSpec, clusters, single_cluster: bool = False):
    """The tool as an MCP client sees it after FastMCP registered ``wrap(spec, clusters, single_cluster)``."""
    server = FastMCP("test")
    server.add_tool(
        wrap(spec, clusters, single_cluster),
        name=spec.name,
        description=spec.description,
        annotations=annotations(spec),
    )
    (tool,) = await server.list_tools()
    return tool


@pytest.fixture(autouse=True)
def fresh_executors(monkeypatch):
    """No worker pool is shared between tests: a monkeypatched MAX_CONCURRENT_CALLS_PER_CLUSTER applies to the
    pools a test creates, and a worker an earlier test left busy is never reused. The pools are shut down
    afterwards (after the ``release`` fixture let blocked tools return)."""
    executors = {}
    monkeypatch.setattr(registry, "_executors", executors)
    yield executors
    for executor in executors.values():
        executor.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def release():
    """Lets the tools of :func:`_blocking` return; always set in teardown, so no worker stays blocked."""
    event = threading.Event()
    yield event
    event.set()


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

        core = len(CORE_READ)
        data = len(DATA_READ)
        assert toolsets == ["core"] * core + ["data"] * data + ["json"]
        assert set(names[:core]) == CORE_READ
        assert set(names[core : core + data]) == DATA_READ
        assert names[core + data :] == ["json_get"]
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
        assert cluster["description"] == ALL_CLUSTERS_DESCRIPTION
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

    def test_single_cluster_is_off_by_default(self):
        assert inspect.signature(wrap).parameters["single_cluster"].default is False

        signature = inspect.signature(wrap(self._tool_spec(), ["only"]))

        assert list(signature.parameters) == ["cluster", "key", "count"]
        assert signature.parameters["cluster"].default is inspect.Parameter.empty

    @pytest.mark.asyncio
    async def test_single_cluster_makes_the_cluster_optional_and_last(self):
        # calls written before multi-cluster support (no cluster argument) keep working
        tool = await _listed(self._tool_spec(), ["only"], single_cluster=True)
        schema = tool.inputSchema

        assert list(schema["properties"]) == ["key", "count", "cluster"]
        assert schema["required"] == ["key"]
        cluster = schema["properties"]["cluster"]
        assert cluster["default"] is None
        assert cluster["description"] == ONLY_CLUSTER_DESCRIPTION
        assert {"type": "string", "enum": ["only"]} in cluster["anyOf"]
        assert {"type": "null"} in cluster["anyOf"]
        assert "const" not in json.dumps(schema)

    @pytest.mark.asyncio
    async def test_single_cluster_keeps_the_optional_cluster_description(self):
        tool = await _listed(
            self._tool_spec(cluster="optional"), ["only"], single_cluster=True
        )
        schema = tool.inputSchema

        assert list(schema["properties"]) == ["key", "count", "cluster"]
        assert schema["required"] == ["key"]
        assert schema["properties"]["cluster"]["description"] == (
            ALL_CLUSTERS_DESCRIPTION
        )

    @pytest.mark.asyncio
    async def test_single_cluster_adds_nothing_to_clusterless_tools(self):
        tool = await _listed(self._tool_spec(cluster="none"), [], single_cluster=True)

        assert list(tool.inputSchema["properties"]) == ["key", "count"]

    @pytest.mark.parametrize("kind", list(ToolKind))
    @pytest.mark.asyncio
    async def test_single_cluster_for_every_kind(self, kind):
        tool = await _listed(self._tool_spec(kind=kind), ["only"], single_cluster=True)

        assert "cluster" not in tool.inputSchema["required"]
        assert tool.annotations == annotations(self._tool_spec(kind=kind))

    @pytest.mark.asyncio
    async def test_single_cluster_call_without_cluster_passes_none(self):
        spec = self._tool_spec()
        server = FastMCP("test")
        server.add_tool(wrap(spec, ["only"], single_cluster=True), name=spec.name)

        with patch("src.common.registry.call", return_value="done") as mock_call:
            await server.call_tool("lookup", {"key": "k"})
            await server.call_tool("lookup", {"key": "k", "cluster": "only"})

        # no cluster selected, but both calls run in the only cluster's worker pool
        assert [(c.args, c.kwargs) for c in mock_call.call_args_list] == [
            ((spec, None, {"key": "k", "count": 10}), {"pool": "only"}),
            ((spec, "only", {"key": "k", "count": 10}), {"pool": "only"}),
        ]

    @pytest.mark.asyncio
    async def test_single_cluster_optional_tool_without_cluster_has_no_pool(self):
        # "all clusters" (e.g. clusters_list) is not a call on the only cluster
        spec = self._tool_spec(cluster="optional")
        wrapped = wrap(spec, ["only"], single_cluster=True)

        with patch("src.common.registry.call", return_value="done") as mock_call:
            await wrapped(key="k", count=3)
            await wrapped(key="k", count=3, cluster="only")

        assert [(c.args, c.kwargs) for c in mock_call.call_args_list] == [
            ((spec, None, {"key": "k", "count": 3}), {"pool": None}),
            ((spec, "only", {"key": "k", "count": 3}), {"pool": "only"}),
        ]

    @pytest.mark.asyncio
    async def test_single_cluster_still_rejects_other_names(self):
        spec = self._tool_spec()
        server = FastMCP("test")
        server.add_tool(wrap(spec, ["only"], single_cluster=True), name=spec.name)

        with patch("src.common.registry.call", return_value="done") as mock_call:
            with pytest.raises(ToolError, match="cluster"):
                await server.call_tool("lookup", {"key": "k", "cluster": "other"})

        mock_call.assert_not_called()

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
        mock_call.assert_called_once_with(spec, "a", {"key": "k", "count": 3}, pool="a")

    @pytest.mark.asyncio
    async def test_wrapper_with_several_clusters_uses_the_named_cluster_pool(self):
        spec = self._tool_spec()
        wrapped = wrap(spec, ["a", "b"])

        with patch("src.common.registry.call", return_value="done") as mock_call:
            await wrapped(cluster="b", key="k")

        mock_call.assert_called_once_with(spec, "b", {"key": "k"}, pool="b")

    @pytest.mark.asyncio
    async def test_wrapper_without_cluster_passes_none(self):
        spec = self._tool_spec(cluster="none")
        wrapped = wrap(spec, [])

        with patch("src.common.registry.call", return_value="done") as mock_call:
            await wrapped(key="k", count=3)

        mock_call.assert_called_once_with(
            spec, None, {"key": "k", "count": 3}, pool=None
        )

    @pytest.mark.asyncio
    async def test_clusterless_tool_in_single_cluster_mode_has_no_pool(self):
        spec = self._tool_spec(cluster="none")
        wrapped = wrap(spec, [], single_cluster=True)

        with patch("src.common.registry.call", return_value="done") as mock_call:
            await wrapped(key="k")

        mock_call.assert_called_once_with(spec, None, {"key": "k"}, pool=None)


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

    @pytest.mark.parametrize("asynchronous", [False, True])
    @pytest.mark.asyncio
    async def test_caller_context_is_visible_in_the_worker(self, asynchronous):
        def probe():
            return _REQUEST_ID.get(), current_cluster.get(), threading.get_ident()

        async def async_probe():
            return probe()

        token = _REQUEST_ID.set("request-1")
        try:
            request_id, cluster, thread = await call(
                _spec(async_probe if asynchronous else probe), "a", {}
            )
        finally:
            _REQUEST_ID.reset(token)

        assert (request_id, cluster) == ("request-1", "a")
        assert thread != threading.get_ident()

    @pytest.mark.asyncio
    async def test_worker_changes_do_not_leak(self, monkeypatch):
        # one worker, so both calls run on the same thread; each runs in its own copy of the caller's context
        monkeypatch.setattr(registry, "MAX_CONCURRENT_CALLS_PER_CLUSTER", 1)

        def first():
            _REQUEST_ID.set("set by the first call")
            current_cluster.set("changed")
            return threading.get_ident()

        def second():
            return threading.get_ident(), _REQUEST_ID.get(), current_cluster.get()

        first_thread = await call(_spec(first), "a", {})
        second_thread, request_id, cluster = await call(_spec(second), "a", {})

        assert second_thread == first_thread
        assert request_id is None
        assert cluster == "a"
        assert _REQUEST_ID.get() is None
        assert current_cluster.get() is None

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
    async def test_timeout(self, monkeypatch, caplog, release):
        # long enough for the worker to start the tool even on a slow CI machine
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 0.5)
        blocked = _blocking(release)
        caplog.set_level(logging.INFO, logger="src.common.registry")

        with pytest.raises(ToolError) as info:
            await call(_spec(blocked), "a", {})

        assert blocked.started.is_set()
        message = str(info.value)
        assert message == _timeout_message(0.5)
        # no exception chained to the ToolError (it would only show the internal TimeoutError)
        assert info.value.__cause__ is None
        assert info.value.__suppress_context__ is True
        assert current_cluster.get() is None
        (record,) = _tool_call_records(caplog)
        assert record.outcome == "error"
        assert record.error == message
        # coarse clocks (Windows) may let the timer fire a little early
        assert record.durationMs >= 400

    @pytest.mark.asyncio
    async def test_async_tool_timeout(self, monkeypatch, release):
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 0.05)

        async def probe():
            while not release.is_set():
                await asyncio.sleep(0.01)

        with pytest.raises(ToolError) as info:
            await call(_spec(probe), "a", {})
        assert str(info.value) == _timeout_message(0.05)

    @pytest.mark.asyncio
    async def test_cancelled_call_is_logged_as_cancelled(self, caplog, release):
        # e.g. the MCP client cancels the request or disconnects
        blocked = _blocking(release)
        caplog.set_level(logging.INFO, logger="src.common.registry")
        task = asyncio.create_task(call(_spec(blocked, name="blocked"), "a", {}))
        assert await _wait(blocked.started)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        (record,) = _tool_call_records(caplog)
        assert (record.tool, record.cluster) == ("blocked", "a")
        assert record.outcome == "cancelled"
        assert record.error == "cancelled by the client"
        assert "outcome=cancelled" in record.getMessage()
        assert current_cluster.get() is None

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


class TestWorkerPools:
    """Tool calls run in a thread pool per cluster, so a hanging cluster ties up only its own workers."""

    def test_pool_size(self):
        assert registry.MAX_CONCURRENT_CALLS_PER_CLUSTER == 8

    def test_one_pool_per_cluster(self):
        a = registry._executor("a")

        assert registry._executor("a") is a
        assert registry._executor("b") is not a
        assert a._max_workers == registry.MAX_CONCURRENT_CALLS_PER_CLUSTER

    def test_calls_without_cluster_share_one_pool(self):
        clusterless = registry._executor(None)

        assert registry._executor(None) is clusterless
        assert registry._executor("") is clusterless
        assert clusterless is not registry._executor("a")
        assert set(registry._executors) == {"", "a"}

    def test_pool_size_applies_to_new_pools(self, monkeypatch):
        monkeypatch.setattr(registry, "MAX_CONCURRENT_CALLS_PER_CLUSTER", 2)

        assert registry._executor("a")._max_workers == 2

    def test_concurrent_first_calls_create_one_pool(self):
        barrier = threading.Barrier(16)
        pools = []

        def first_call():
            barrier.wait(5)
            pools.append(registry._executor("a"))

        threads = [threading.Thread(target=first_call) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)

        assert len(pools) == 16
        assert all(pool is pools[0] for pool in pools)
        assert list(registry._executors) == ["a"]

    @pytest.mark.asyncio
    async def test_calls_run_in_the_pool_of_their_cluster(self):
        def probe():
            return threading.current_thread().name

        names = {
            cluster: await call(_spec(probe, cluster=kind), cluster, {})
            for cluster, kind in (("a", "required"), ("b", "required"), (None, "none"))
        }

        assert names["a"].startswith("tool-a_")
        assert names["b"].startswith("tool-b_")
        assert names[None].startswith("tool--_")

    @pytest.mark.parametrize("workers", [1, 3])
    @pytest.mark.parametrize("other", ["b", None])
    @pytest.mark.asyncio
    async def test_busy_cluster_does_not_delay_other_calls(
        self, monkeypatch, release, workers, other
    ):
        monkeypatch.setattr(registry, "MAX_CONCURRENT_CALLS_PER_CLUSTER", workers)
        # were the call queued behind cluster a, it would fail at this limit
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 5)
        blocked = [_blocking(release, result=i) for i in range(workers)]
        busy = [asyncio.create_task(call(_spec(fn), "a", {})) for fn in blocked]
        for fn in blocked:
            assert await _wait(fn.started)

        def probe():
            return "free", current_cluster.get()

        spec = _spec(probe, cluster="required" if other else "none")
        assert await call(spec, other, {}) == ("free", other)

        assert not any(task.done() for task in busy)
        release.set()
        assert await asyncio.gather(*busy) == list(range(workers))

    @pytest.mark.asyncio
    async def test_queued_call_times_out_without_starting(
        self, monkeypatch, release, caplog
    ):
        monkeypatch.setattr(registry, "MAX_CONCURRENT_CALLS_PER_CLUSTER", 1)
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 5)
        caplog.set_level(logging.INFO, logger="src.common.registry")
        blocked = _blocking(release)
        first = asyncio.create_task(call(_spec(blocked, name="blocked"), "a", {}))
        assert await _wait(blocked.started)

        # the first call keeps its 5 s; the queued one gets 0.2 s
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 0.2)
        queued_ran = threading.Event()
        with pytest.raises(ToolError) as info:
            await call(_spec(queued_ran.set, name="queued"), "a", {})

        assert str(info.value) == _timeout_message(0.2)
        assert not queued_ran.is_set()
        assert not first.done()

        release.set()
        assert await first == "released"
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 5)
        # the single worker takes queued calls in order: once this one ran, the timed-out one was dropped
        assert await call(_spec(lambda: "next", name="next"), "a", {}) == "next"
        assert not queued_ran.is_set()
        records = {r.tool: r for r in _tool_call_records(caplog)}
        assert set(records) == {"blocked", "queued", "next"}
        assert records["blocked"].outcome == "ok"
        assert records["queued"].outcome == "error"
        assert records["queued"].error == _timeout_message(0.2)
        assert records["queued"].durationMs >= 150
        assert records["next"].outcome == "ok"

    @pytest.mark.asyncio
    async def test_cancelled_queued_call_never_starts(
        self, monkeypatch, release, caplog
    ):
        monkeypatch.setattr(registry, "MAX_CONCURRENT_CALLS_PER_CLUSTER", 1)
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 5)
        caplog.set_level(logging.INFO, logger="src.common.registry")
        blocked = _blocking(release)
        first = asyncio.create_task(call(_spec(blocked, name="blocked"), "a", {}))
        assert await _wait(blocked.started)
        queued_ran = threading.Event()
        queued = asyncio.create_task(
            call(_spec(queued_ran.set, name="queued"), "a", {})
        )
        # one loop iteration submits it to the (busy) pool; give it a few
        await asyncio.sleep(0.05)

        queued.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued

        release.set()
        assert await first == "released"
        assert await call(_spec(lambda: "next", name="next"), "a", {}) == "next"
        assert not queued_ran.is_set()
        outcomes = {r.tool: r.outcome for r in _tool_call_records(caplog)}
        assert outcomes == {"blocked": "ok", "queued": "cancelled", "next": "ok"}

    @pytest.mark.asyncio
    async def test_pool_argument_picks_the_pool(self):
        def probe():
            return threading.current_thread().name, current_cluster.get()

        name, cluster = await call(_spec(probe), None, {}, pool="a")

        # the call runs without a selected cluster, in cluster a's pool
        assert name.startswith("tool-a_")
        assert cluster is None
        assert set(registry._executors) == {"a"}

    @pytest.mark.asyncio
    async def test_pool_defaults_to_the_cluster(self):
        def probe():
            return threading.current_thread().name

        assert (await call(_spec(probe), "b", {})).startswith("tool-b_")
        assert (await call(_spec(probe), "b", {}, pool=None)).startswith("tool-b_")
        assert set(registry._executors) == {"b"}

    @pytest.mark.asyncio
    async def test_implicit_call_runs_in_the_only_cluster_pool(self):
        set_config(_config())

        def probe():
            return threading.current_thread().name, current_cluster.get()

        implicit = wrap(_spec(probe), ["a"], single_cluster=True)

        name, cluster = await implicit()

        assert name.startswith("tool-a_")
        assert cluster is None
        assert set(registry._executors) == {"a"}

    @pytest.mark.parametrize("first_named", [False, True], ids=["implicit", "explicit"])
    @pytest.mark.asyncio
    async def test_implicit_and_explicit_calls_share_the_only_cluster_pool(
        self, monkeypatch, release, first_named
    ):
        """Calls with and without the cluster argument share its MAX_CONCURRENT_CALLS_PER_CLUSTER workers
        (which MAX_CONNECTIONS leaves room for), and a hanging cluster leaves the clusterless pool free."""
        monkeypatch.setattr(registry, "MAX_CONCURRENT_CALLS_PER_CLUSTER", 1)
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 5)
        set_config(_config())
        blocked = _blocking(release)
        first_tool = wrap(_spec(blocked, name="blocked"), ["a"], single_cluster=True)
        first = asyncio.create_task(
            first_tool(cluster="a") if first_named else first_tool()
        )
        assert await _wait(blocked.started)

        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 0.5)
        second_ran = threading.Event()
        second_tool = wrap(
            _spec(second_ran.set, name="second"), ["a"], single_cluster=True
        )
        clusterless = wrap(
            _spec(lambda: "free", name="free", cluster="none"), [], single_cluster=True
        )
        try:
            with pytest.raises(ToolError, match="timeout"):
                await (second_tool() if first_named else second_tool(cluster="a"))
            assert not second_ran.is_set()
            assert await clusterless() == "free"
            assert not first.done()
        finally:
            release.set()
            assert await first == "released"
        assert set(registry._executors) == {"a", ""}


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
            # the only cluster: the parameter is optional
            assert list(tool.inputSchema["properties"])[-1] == "cluster"
            assert "cluster" not in tool.inputSchema.get("required", [])
            cluster = tool.inputSchema["properties"]["cluster"]
            assert {"type": "string", "enum": ["cbtstrds"]} in cluster["anyOf"]
            assert cluster["description"] == (
                ALL_CLUSTERS_DESCRIPTION
                if tool.name == "clusters_list"
                else ONLY_CLUSTER_DESCRIPTION
            )

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
    async def test_tool_timeout(
        self, real_tools, config, redis_client, monkeypatch, release
    ):
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 0.05)
        redis_client.get.side_effect = lambda key: release.wait(5) and "late"
        mcp = build_server(config)

        with pytest.raises(ToolError) as info:
            await mcp.call_tool("get", {"cluster": "rw", "key": "k"})

        assert _timeout_message(0.05) in str(info.value)

    @pytest.mark.asyncio
    async def test_hanging_cluster_does_not_delay_another(
        self, real_tools, config, redis_client, monkeypatch, release
    ):
        monkeypatch.setattr(registry, "MAX_CONCURRENT_CALLS_PER_CLUSTER", 1)
        monkeypatch.setattr(registry, "TOOL_TIMEOUT_SECONDS", 5)
        hanging = threading.Event()

        def get(key):
            if current_cluster.get() == "rw":
                hanging.set()
                release.wait(10)
            return f"{current_cluster.get()}:{key}"

        redis_client.get.side_effect = get
        mcp = build_server(config)
        stuck = asyncio.create_task(mcp.call_tool("get", {"cluster": "rw", "key": "k"}))
        assert await _wait(hanging)

        content, _ = await mcp.call_tool("get", {"cluster": "ro", "key": "k"})

        assert content[0].text == "ro:k"
        assert not stuck.done()
        release.set()
        content, _ = await stuck
        assert content[0].text == "rw:k"

    @pytest.fixture
    def created_clients(self):
        """The real RedisConnectionManager with create_client mocked: records the call's cluster (as the
        worker sees it) and the cluster the manager resolved."""
        client = Mock()
        client.get.return_value = "value"
        client.created_for = []

        def create_client(cluster_config, decode_responses=True, max_connections=None):
            client.created_for.append((current_cluster.get(), cluster_config.name))
            return client

        with patch("src.common.connection.create_client", side_effect=create_client):
            yield client

    @staticmethod
    def _single_cluster(legacy: bool, redis_config):
        if legacy:
            return from_legacy(redis_config), "default"
        return _config(toolsets=["core", "data"]), "a"

    @pytest.mark.parametrize("legacy", [False, True], ids=["yaml", "legacy"])
    @pytest.mark.asyncio
    async def test_single_cluster_call_without_cluster(
        self, real_tools, created_clients, caplog, redis_config, legacy, fresh_executors
    ):
        config, name = self._single_cluster(legacy, redis_config)
        set_config(config)
        mcp = build_server(config)
        caplog.set_level(logging.INFO, logger="src.common.registry")

        content, structured = await mcp.call_tool("get", {"key": "k"})

        assert content[0].text == "value"
        assert structured == {"result": "value"}
        created_clients.get.assert_called_once_with("k")
        # the tool runs without a selected cluster; the connection manager falls back to the only one
        assert created_clients.created_for == [(None, name)]
        # ... in that cluster's worker pool
        assert set(fresh_executors) == {name}
        (record,) = _tool_call_records(caplog)
        assert (record.tool, record.cluster, record.outcome) == ("get", None, "ok")

    @pytest.mark.parametrize("legacy", [False, True], ids=["yaml", "legacy"])
    @pytest.mark.asyncio
    async def test_single_cluster_call_with_cluster(
        self, real_tools, created_clients, redis_config, legacy, fresh_executors
    ):
        config, name = self._single_cluster(legacy, redis_config)
        set_config(config)
        mcp = build_server(config)

        content, _ = await mcp.call_tool("get", {"cluster": name, "key": "k"})
        await mcp.call_tool("get", {"key": "k"})

        assert content[0].text == "value"
        # one client and one worker pool for calls with and without the cluster argument
        assert created_clients.created_for == [(name, name)]
        assert set(fresh_executors) == {name}

    @pytest.mark.asyncio
    async def test_single_cluster_rejects_another_name(
        self, real_tools, created_clients
    ):
        config = _config(toolsets=["core", "data"])
        set_config(config)
        mcp = build_server(config)

        with pytest.raises(ToolError, match="cluster"):
            await mcp.call_tool("get", {"cluster": "b", "key": "k"})

        assert created_clients.created_for == []

    @pytest.mark.asyncio
    async def test_implicit_cluster_needs_a_single_cluster(
        self, real_tools, created_clients
    ):
        # what the optional parameter relies on: without a cluster the manager only picks the only one
        config = _config(clusters=[_cluster("a"), _cluster("b")])
        set_config(config)
        spec = registry._SPECS["get"]

        with pytest.raises(ClusterSelectionError, match="Missing parameter: cluster"):
            await wrap(spec, ["a", "b"], single_cluster=True)(key="k")

        assert created_clients.created_for == []
