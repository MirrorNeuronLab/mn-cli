"""Refresh desktop endpoint advertisements without recreating a healthy Core."""
from __future__ import annotations

import ipaddress
import os
import re
from urllib.parse import urlsplit, urlunsplit

from mn_cli.runtime.identity import atomic_json, read_identity


def endpoint_updates(env: dict[str, str], host: str) -> dict[str, str]:
    previous = env.get("MN_NETWORK_ADVERTISE_HOST", "")
    updates = {"MN_NETWORK_ADVERTISE_HOST": host}
    for key in ("MN_NATIVE_SDK_GRPC_ADVERTISE_HOST", "MN_LITELLM_ADVERTISE_HOST"):
        if env.get(key, "") in ("", previous):
            updates[key] = host
    url = env.get("MN_ARTIFACT_ADVERTISE_URL", "")
    if url:
        parsed = urlsplit(url)
        if parsed.hostname == previous:
            updates["MN_ARTIFACT_ADVERTISE_URL"] = urlunsplit(
                parsed._replace(netloc=f"{host}:{parsed.port}" if parsed.port else host)
            )
    return updates


def reconnect() -> None:
    from mn_cli.runtime import server
    from mn_cli.libs.runtime_health import collect_runtime_status
    from mn_cli.output import record_result

    report = collect_runtime_status()
    if not report.get("identity", {}).get("valid"):
        raise RuntimeError("MN_NODE_IDENTITY_INVALID: Run mn runtime start to restore the configured identity.")
    env = server._runtime_base_env(server.runtime_compose_available())
    if server._persisted_join_profile(env) or server._docker_network_uses_internal_identity(
        str(env.get("MN_DOCKER_NETWORK_MODE") or "disabled")
    ):
        raise RuntimeError("Endpoint refresh is supported only for independent desktop federation.")
    previous = str(env.get("MN_NETWORK_ADVERTISE_HOST") or "")
    # Keep an explicitly configured DNS endpoint; refresh automatically detected IPs.
    configured = os.getenv("MN_NETWORK_ADVERTISE_HOST", "").strip()
    if not configured and previous:
        try:
            ipaddress.ip_address(previous)
        except ValueError:
            configured = previous
    host = configured or server._detect_lan_ip()
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if not host or (address and (address.is_loopback or address.is_unspecified)):
        record_result({"identity": report["identity"], "offline": True,
                       "detail": "No reachable network address; retaining the last advertised endpoint. Local identity is unchanged."})
        return
    port = int(env.get("MN_GRPC_ADVERTISE_PORT") or env.get("MN_GRPC_PORT") or 55051)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", host) or not 1 <= port <= 65535:
        raise RuntimeError("Invalid advertised endpoint. Configure a valid hostname or IPv4 address and port (1–65535).")
    name = read_identity(server.DIR) or env.get("MN_NODE_NAME", "")
    atomic_json(server.DIR / "runtime-network.json", {
        "version": 1, "node_name": name, "host": host,
        "grpc_port": port,
    })
    server._write_env_file_values(server.RUNTIME_COMPOSE_ENV, endpoint_updates(env, host))
    record_result({"identity": report["identity"], "advertised_host": host,
                   "detail": "Endpoint refresh queued. Unreachable peers may require mn node add with their updated endpoint."})
