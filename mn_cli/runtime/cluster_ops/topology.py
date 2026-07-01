from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class NodeTopology:
    name: str
    status: str = ""
    self_node: bool = False
    connected_nodes: frozenset[str] = frozenset()
    scheduling_eligible: bool = True

    @classmethod
    def from_summary_node(cls, node: dict[str, Any]) -> "NodeTopology":
        return cls(
            name=str(node.get("name") or node.get("node") or node.get("node_name") or ""),
            status=str(node.get("status") or ""),
            self_node=node.get("self?") is True,
            connected_nodes=frozenset(str(item) for item in node.get("connected_nodes") or []),
            scheduling_eligible=node.get("scheduling_eligible") is not False,
        )


@dataclass(frozen=True)
class ClusterTopology:
    nodes: tuple[NodeTopology, ...] = field(default_factory=tuple)

    @classmethod
    def from_system_summary(cls, summary: dict[str, Any]) -> "ClusterTopology":
        return cls(
            tuple(
                NodeTopology.from_summary_node(node)
                for node in summary.get("nodes") or []
                if isinstance(node, dict)
            )
        )

    @property
    def names(self) -> set[str]:
        return {node.name for node in self.nodes if node.name}

    @property
    def self_node(self) -> NodeTopology | None:
        return next((node for node in self.nodes if node.self_node), None)

    def reciprocal_membership_ok(self, other: "ClusterTopology") -> bool:
        local_self = self.self_node
        remote_self = other.self_node
        if local_self is None or remote_self is None:
            return False
        return remote_self.name in self.names and local_self.name in other.names

    def remote_connected_to_local(self, remote: "ClusterTopology") -> bool:
        local_self = self.self_node
        remote_self = remote.self_node
        if local_self is None or remote_self is None:
            return False
        return local_self.name in remote_self.connected_nodes
