"""Tool registry: toolsets, tool kinds and the per-config tool list.

Tools are declared with :func:`redis_tool`, which only records them. Once the config is known,
:func:`build_tools` decides which tools are listed:

- only the toolsets named in the config's ``toolsets`` list,
- write tools only on clusters with an effective ``readOnly: false``,
- destructive tools additionally only while ``disableDestructive`` is false,
- json/search tools only on clusters that list the module in ``modules``.

A tool with no eligible cluster left is not listed. For the others a ``cluster`` parameter is added as an enum of
the eligible cluster names, so a call naming any other cluster fails schema validation.
"""

import asyncio
import enum
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Annotated, Any, Callable, Dict, List, Literal, Optional, Tuple

from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field, WithJsonSchema

from src.common.config_loader import ClusterConfig, ConfigError, ServerConfig
from src.common.connection import current_cluster

_logger = logging.getLogger(__name__)

# Time limit of a tool call; the client socket timeouts are set below it.
TOOL_TIMEOUT_SECONDS = 15

TOOLSETS: Dict[str, str] = {
    "core": "Clusters, server info and generic key operations (type, scan, expire, rename, delete)",
    "data": "Strings, hashes (including vectors), lists, sets, sorted sets and streams",
    "json": "RedisJSON documents (clusters with modules: [json])",
    "search": "Redis Query Engine: indexes, vector and hybrid search (clusters with modules: [search])",
    "pubsub": "Publish and subscribe",
    "docs": "Search the redis.io documentation (needs internet access; not tied to a cluster)",
}


class ToolKind(enum.Enum):
    """READ: always listed. WRITE: only on readOnly=false clusters. DESTRUCTIVE: on top of WRITE, also
    requires disableDestructive=false (deletes, pops, removals)."""

    READ = "read"
    WRITE = "write"
    DESTRUCTIVE = "destructive"

    @property
    def writes(self) -> bool:
        return self is not ToolKind.READ


# required: the tool needs a cluster; optional: it may take one (e.g. clusters_list); none: no cluster at all
ClusterParam = Literal["required", "optional", "none"]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    toolset: str
    kind: ToolKind
    fn: Callable[..., Any]
    description: str
    requires: Optional[str] = None
    cluster: ClusterParam = "required"


_SPECS: Dict[str, ToolSpec] = {}


def redis_tool(
    toolset: str,
    kind: ToolKind = ToolKind.READ,
    requires: Optional[str] = None,
    cluster: ClusterParam = "required",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Declares a tool. The function is returned unchanged; its name and docstring become the tool's.

    ``requires`` names a module (json, search) the cluster must list in ``modules``.
    """
    if toolset not in TOOLSETS:
        raise ValueError(f"Unknown toolset: {toolset}")

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        description = inspect.getdoc(fn) or ""
        if kind.writes and not description.startswith("WRITE:"):
            description = "WRITE: " + description
        spec = ToolSpec(
            name=fn.__name__,
            toolset=toolset,
            kind=kind,
            fn=fn,
            description=description,
            requires=requires,
            cluster=cluster,
        )
        existing = _SPECS.get(spec.name)
        if existing is not None and existing.fn.__module__ != fn.__module__:
            raise ValueError(f"Duplicate tool name: {spec.name}")
        _SPECS[spec.name] = spec
        return fn

    return decorator


def specs() -> List[ToolSpec]:
    return list(_SPECS.values())


def reason_unavailable(
    config: ServerConfig, spec: ToolSpec, cluster: ClusterConfig
) -> Optional[str]:
    """Why the tool cannot run on this cluster; None if it can."""
    if spec.requires and spec.requires not in cluster.modules:
        return f"cluster {cluster.name} does not list the {spec.requires} module in modules"
    if spec.kind.writes and config.is_read_only(cluster):
        return f"cluster {cluster.name} is readOnly; write tools are disabled"
    if spec.kind is ToolKind.DESTRUCTIVE and config.disable_destructive:
        return "disableDestructive is on; delete/pop/remove tools are disabled"
    return None


def eligible_clusters(config: ServerConfig, spec: ToolSpec) -> List[str]:
    return [
        c.name for c in config.clusters if reason_unavailable(config, spec, c) is None
    ]


def enabled_toolsets(config: ServerConfig) -> List[str]:
    if config.all_toolsets:
        return list(TOOLSETS)
    unknown = [t for t in config.toolsets if t not in TOOLSETS]
    if unknown:
        raise ConfigError(f"Unknown toolset: {unknown} (valid: {sorted(TOOLSETS)})")
    return list(config.toolsets)


def select(config: ServerConfig) -> List[Tuple[ToolSpec, List[str]]]:
    """The listed tools with their eligible clusters, in toolset order."""
    selected = []
    for toolset in enabled_toolsets(config):
        for spec in specs():
            if spec.toolset != toolset:
                continue
            if spec.cluster == "none":
                selected.append((spec, []))
                continue
            clusters = eligible_clusters(config, spec)
            if clusters:
                selected.append((spec, clusters))
    return selected


def annotations(spec: ToolSpec) -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=spec.kind is ToolKind.READ,
        destructiveHint=spec.kind is ToolKind.DESTRUCTIVE,
    )


def wrap(spec: ToolSpec, clusters: List[str]) -> Callable[..., Any]:
    """The function FastMCP registers: the tool's own signature plus the ``cluster`` enum parameter. A call
    runs the tool in a worker thread (the tools use the blocking redis client) with the cluster selected and
    a time limit."""
    signature = inspect.signature(spec.fn)
    parameters = list(signature.parameters.values())
    if spec.cluster != "none":
        # an enum even for a single cluster (pydantic would emit "const")
        names = Annotated[
            Literal[tuple(clusters)],  # type: ignore[valid-type]
            WithJsonSchema({"type": "string", "enum": list(clusters)}),
        ]
        if spec.cluster == "required":
            parameters.insert(
                0,
                inspect.Parameter(
                    "cluster",
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Annotated[names, Field(description="Cluster name")],
                ),
            )
        else:
            parameters.append(
                inspect.Parameter(
                    "cluster",
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Annotated[
                        Optional[names],
                        Field(description="Cluster name (default: all clusters)"),
                    ],
                    default=None,
                )
            )

    async def tool(**kwargs: Any) -> Any:
        cluster = kwargs.pop("cluster", None)
        return await call(spec, cluster, kwargs)

    tool.__name__ = spec.name
    tool.__qualname__ = spec.name
    tool.__doc__ = spec.description
    tool.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]
    return tool


def _run(fn: Callable[..., Any], kwargs: Dict[str, Any]) -> Any:
    result = fn(**kwargs)
    if inspect.isawaitable(result):
        return asyncio.run(result)
    return result


async def call(spec: ToolSpec, cluster: Optional[str], kwargs: Dict[str, Any]) -> Any:
    start = time.monotonic()
    outcome, error = "ok", None
    token = current_cluster.set(cluster)
    try:
        # asyncio.to_thread copies the context, so the worker sees current_cluster
        return await asyncio.wait_for(
            asyncio.to_thread(_run, spec.fn, kwargs), TOOL_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        outcome = "error"
        error = (
            f"Tool did not finish within {TOOL_TIMEOUT_SECONDS} s (timeout)."
            " Try a narrower pattern or a smaller count."
        )
        raise ToolError(error) from None
    except Exception as e:
        outcome, error = "error", str(e)
        raise
    finally:
        current_cluster.reset(token)
        duration_ms = int((time.monotonic() - start) * 1000)
        _logger.info(
            "tool call tool=%s cluster=%s durationMs=%d outcome=%s",
            spec.name,
            cluster,
            duration_ms,
            outcome,
            extra={
                "tool": spec.name,
                "cluster": cluster,
                "durationMs": duration_ms,
                "outcome": outcome,
                "error": error,
            },
        )
