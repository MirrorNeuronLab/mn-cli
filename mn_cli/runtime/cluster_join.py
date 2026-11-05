"""CLI orchestration for reciprocal node connections.

The runtime dependency supplies host/service operations and presentation. The
SDK owns the authenticated registration protocol. The server facade passes its
current helpers so existing injection points remain effective.
"""

from __future__ import annotations

from typing import Any


def join_network(
    runtime: Any,
    *,
    local_client: Any,
    join_peer: Any,
    seed_host: str,
    token: str,
    host: str | None = None,
    grpc_port: int = 55051,
    docker_network_mode: str | None = None,
    docker_network_name: str | None = None,
    action: str = "Node join",
) -> dict:

    target = f"{seed_host}:{grpc_port}"
    local_host = (host or runtime._detect_lan_ip()).strip()
    requested_mode = runtime._docker_network_mode(
        docker_network_mode, default="disabled"
    )
    network_name = runtime._docker_network_name(docker_network_name)
    env = runtime._runtime_base_env(runtime.runtime_compose_available())
    env.update(runtime._shared_storage_env_from_runtime_env(env))
    env = runtime._ensure_syncthing_for_runtime(env, advertised_host=local_host)
    local_syncthing = runtime._syncthing_node_info(env, local_host)
    local_node_name = runtime._network_node_name(local_host)
    if requested_mode != "disabled":
        runtime._ensure_network_docker_network(requested_mode, network_name)
        if runtime._docker_network_uses_internal_identity(requested_mode):
            local_node_name = runtime._docker_node_name(
                runtime._resolve_node_alias(env)
            )

    # A running Core is authoritative. Joining must not recreate it because
    # that could interrupt jobs and erase the standalone-store boundary
    # federation is intended to preserve.
    running_node_name = runtime._running_core_node_name(local_client)
    env = runtime._ensure_desktop_identity(env)
    local_node_name = env.get("MN_NODE_NAME") or local_node_name
    if running_node_name and running_node_name != local_node_name:
        raise RuntimeError(
            "MN_NODE_IDENTITY_INVALID: Run mn runtime start to restore the configured identity before joining a peer."
        )
    if not running_node_name:
        runtime._ensure_local_cluster_runtime_for_join(
            local_host=local_host,
            node_name=local_node_name,
            docker_network_mode=requested_mode,
            docker_network_name=network_name,
        )
    runtime.print_info(
        runtime.console, f"Establishing federated peer relationship with {target}…"
    )
    try:
        join_options: dict[str, Any] = {
            "host": seed_host,
            "token": token,
            "grpc_port": grpc_port,
            "local_host": runtime._running_core_advertised_host(local_client)
            or local_host,
        }
        if local_syncthing.get("enabled"):
            join_options["before_registration"] = lambda remote_info: (
                runtime._require_syncthing_peer_connection(
                    local_syncthing,
                    remote_info.get("syncthing")
                    if isinstance(remote_info.get("syncthing"), dict)
                    else {},
                )
            )
        joined = join_peer(local_client, **join_options)
    except Exception as exc:
        if getattr(exc, "code", None):
            raise
        runtime._raise_join_handshake_error(exc, target)

    remote_node = str(joined.get("node_name") or "")
    if not remote_node or remote_node == "nonode@nohost":
        raise RuntimeError(
            "The peer did not return a valid node identity. Repair the peer runtime before retrying mn node add."
        )
    details: list[tuple[str, str]] = [("Node", remote_node)]
    details.append(("Mode", "federated"))
    model_reconcile = runtime._reconcile_cluster_models_after_membership_change()
    if (
        model_reconcile
        and model_reconcile.get("status") == "ok"
        and model_reconcile.get("inventory_complete") is True
    ):
        details.append(
            (
                "Models",
                f"{int(model_reconcile.get('models') or 0)} shared across "
                f"{len(model_reconcile.get('nodes') or [])} nodes",
            )
        )
    else:
        details.append(
            ("Models", "Synchronization pending; the model monitor will retry")
        )
    runtime.print_success_confirmation(
        runtime.console,
        action,
        status="federated",
        details=details,
        next_steps=("mn node list", "mn resource show", "mn model list"),
    )
    return joined
