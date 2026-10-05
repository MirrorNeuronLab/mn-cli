"""Default-model selection for manual CLI additions."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mn_sdk import assess_model_compatibility
from mn_sdk.model_catalog import select_default_model_entry


def select_default_add_model(
    *,
    nodes: list[str],
    local: bool,
    backend: str,
    force: bool,
    automatic_node: Callable,
    node_endpoint: Callable,
    remote_compatibility: Callable,
) -> dict[str, Any]:
    def compatible(entry: dict[str, Any]) -> bool:
        if str(entry.get("provider") or "") == "litellm_proxy":
            return not (local or nodes or backend != "auto" or force)
        if (
            local
            and not assess_model_compatibility(entry, backend=backend, force=force).ok
        ):
            return False
        if nodes:
            for node in nodes:
                endpoint = node_endpoint(node)
                result = remote_compatibility(
                    entry, selected_node=node, node=endpoint.get("node") or {}
                )
                if result.get("ok") is False and not force:
                    return False
            return True
        if not local and automatic_node(entry):
            return True
        return assess_model_compatibility(entry, backend=backend, force=force).ok

    return select_default_model_entry(is_compatible=compatible)
