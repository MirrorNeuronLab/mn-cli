"""Explicit dependency seams for runtime-model launch orchestration.

The production run path talks to Core, native runtime nodes, Docker Model
Runner, and the local LiteLLM gateway.  Keeping those boundaries in a small
value object lets tests execute the real planning and orchestration logic with
an in-memory cluster instead of patching module globals or starting services.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from mn_sdk import BlueprintModelOps


RuntimeCallable = Callable[..., Any]


@dataclass(frozen=True)
class RuntimeModelGatewayDependencies:
    """Side-effect boundaries used to publish direct DMR routes to LiteLLM."""

    reconcile_cluster_model_remotes: RuntimeCallable | None = None
    installed_model_names: Callable[[], set[str]] | None = None
    local_runtime_node_name: Callable[[], str] | None = None
    sync_litellm_gateway: RuntimeCallable | None = None
    gateway_endpoint_map: RuntimeCallable | None = None
    resolve_model_entry: RuntimeCallable | None = None
    docker_model_runner_endpoint: RuntimeCallable | None = None


@dataclass(frozen=True)
class RuntimeModelDependencies:
    """Injectable inputs and effects for model-aware blueprint preparation.

    Every field is optional so the ordinary CLI keeps resolving the current
    production implementation at call time.  Tests can replace only the
    boundaries they own while still exercising the real catalog parsing,
    hardware fitness, fallback, endpoint projection, and run-handler logic.
    """

    load_blueprint_config: RuntimeCallable | None = None
    load_model_catalog: Callable[[], dict[str, dict[str, Any]]] | None = None
    resolve_cluster_model: RuntimeCallable | None = None
    model_ops: BlueprintModelOps | None = None
    resource_report: Callable[[], dict[str, Any]] | None = None
    system_summary: Callable[[], dict[str, Any]] | None = None
    gateway: RuntimeModelGatewayDependencies | None = None


__all__ = [name for name in globals() if not name.startswith("__")]
