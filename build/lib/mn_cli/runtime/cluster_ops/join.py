from __future__ import annotations

from dataclasses import dataclass

from .identity import NodeIdentity


@dataclass(frozen=True)
class ClusterOperationPlan:
    """Pure description of a cluster operation before side effects are applied."""

    action: str
    local: NodeIdentity
    remote: NodeIdentity | None = None
    docker_network_mode: str = "disabled"
    docker_network_name: str = "mirror-neuron-runtime"

    @property
    def requires_remote(self) -> bool:
        return self.action in {"join", "add", "reconcile"}
