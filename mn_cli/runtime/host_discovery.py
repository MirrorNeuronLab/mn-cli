"""Host discovery shared by model inventory and launch preparation."""

import socket
import urllib.parse
from collections.abc import Callable, Mapping
from typing import Any


def extract_host_candidates_from_text(value: str) -> set[str]:
    candidates: set[str] = set()
    text = str(value or "").strip()
    if not text:
        return candidates
    for part in (text, f"//{text}"):
        parsed = urllib.parse.urlparse(part)
        if parsed.hostname:
            candidates.add(parsed.hostname.lower())
    return candidates


def resolved_local_hostnames() -> set[str]:
    addresses: set[str] = set()
    try:
        addresses.add(socket.gethostbyname(socket.gethostname()).lower())
    except Exception:  # noqa: BLE001, S110 - preserve best-effort discovery
        pass
    try:
        addresses.update(
            addr.lower() for addr in socket.gethostbyname_ex(socket.gethostname())[2]
        )
    except Exception:  # noqa: BLE001, S110 - preserve best-effort discovery
        pass
    try:
        for info in socket.getaddrinfo(
            socket.gethostname(), None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
        ):
            if len(info) >= 5:
                entry = info[4][0]
                if isinstance(entry, str):
                    addresses.add(entry.lower().split("%", 1)[0])
    except Exception:  # noqa: BLE001, S110 - preserve best-effort discovery
        pass
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("10.255.255.255", 1))
            addresses.add(probe.getsockname()[0].lower())
        finally:
            probe.close()
    except Exception:  # noqa: BLE001, S110 - preserve best-effort discovery
        pass
    return addresses


def node_native_sdk_grpc_info(node: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[Any] = [node.get("native_sdk_grpc")]
    hardware = node.get("hardware")
    if isinstance(hardware, dict):
        candidates.append(hardware.get("native_sdk_grpc"))
    node_info = node.get("node_info")
    if isinstance(node_info, dict):
        candidates.append(node_info.get("native_sdk_grpc"))
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return candidate
    return None


def local_host_candidates(
    get_grpc_target: Callable[[], str],
    env: Mapping[str, str],
    *,
    resolve_hostnames: Callable[[], set[str]],
    extract_hosts: Callable[[str], set[str]],
) -> set[str]:
    hostnames = {"localhost", "127.0.0.1", "::1", "::", "0.0.0.0"}
    candidates: set[str] = {address.lower() for address in hostnames}
    try:
        candidates.update(resolve_hostnames())
    except Exception:  # noqa: BLE001, S110 - preserve best-effort discovery
        pass
    try:
        parsed = urllib.parse.urlparse(f"//{get_grpc_target()}")
        if parsed.hostname:
            candidates.add(parsed.hostname.lower())
    except Exception:  # noqa: BLE001, S110 - preserve best-effort discovery
        pass
    for env_key in ("MN_API_HOST", "MN_GRPC_TARGET", "MN_API_BASE_URL"):
        env_value = env.get(env_key, "")
        if env_value:
            candidates.update(extract_hosts(env_value))
    return candidates
