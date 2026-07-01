from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NodeIdentity:
    """Stable identity fields used for cluster membership and diagnostics."""

    name: str
    advertised_host: str = ""
    display_name: str = ""

    @classmethod
    def from_host(cls, host: str, *, display_name: str = "") -> "NodeIdentity":
        advertised_host = str(host or "").strip()
        return cls(
            name=network_node_name(advertised_host),
            advertised_host=advertised_host,
            display_name=display_name,
        )

    @property
    def host(self) -> str:
        return node_host(self.name) or self.advertised_host


def network_node_name(host: str) -> str:
    return f"mirror_neuron@{str(host or '').strip()}"


def node_host(node_name: str) -> str:
    value = str(node_name or "").strip()
    if "@" not in value:
        return ""
    return value.rsplit("@", 1)[1]
