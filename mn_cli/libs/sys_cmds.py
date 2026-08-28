import subprocess
import os
import time
import json
from typing import Optional
from pathlib import Path

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from mn_cli.banner import format_banner
from mn_cli.shared import console, logger
from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import print_error, print_info, print_success_confirmation, require_confirmation
from mn_cli.output import record_result
from mn_cli.terminal import use_progress
from mn_cli.server_cmds import (
    _start_server,
    _start_network_seed,
    _join_network,
    _refresh_network_token,
    _stop_network_runtime,
    _clear_join_owner_metadata,
    _runtime_base_env,
    _start_api_if_installed,
    _start_web_ui_if_installed,
    _write_runtime_endpoints_file,
    _valid_port_text,
    ensure_context_engine_runtime,
    kill_tree,
    COMPOSE_SENTINEL_CONTAINER,
    SYNCTHING_CONTAINER,
    DEFAULT_HOST,
    DEFAULT_API_PORT,
    DEFAULT_GRPC_PORT,
    DEFAULT_DIST_PORT,
    DEFAULT_WEB_UI_PORT,
    DEFAULT_DOCKER_NETWORK_NAME,
    find_web_ui_dir,
    runtime_compose_available,
    runtime_compose_cmd,
    stop_matching_sidecar_processes as _stop_matching_sidecar_processes,
    api_pid_files,
    web_ui_pid_files,
    native_sdk_grpc_pid_files,
)

CONTEXT_ENGINE_EXPECTATION = (
    "This runtime service powers blueprint context memory. It prepares the context-engine package "
    "and Docker Model Runner model before starting the service."
)

def start(
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help="Federation-reachable host or IP advertised by this node.",
    ),
    grpc_port: int = typer.Option(int(DEFAULT_GRPC_PORT), "--grpc-port", help="Core gRPC port."),
):
    """Start a federation-capable MirrorNeuron runtime"""
    console.print(format_banner("MirrorNeuron Local Runtime"))
    _start_server(host=host, grpc_port=grpc_port)

def join(
    host: str,
    token: str = typer.Option(..., "--token", help="Join token printed by mn runtime start on the remote node."),
    grpc_port: int = typer.Option(int(DEFAULT_GRPC_PORT), "--grpc-port", help="Remote node gRPC port."),
    local_host: Optional[str] = typer.Option(
        None,
        "--local-host",
        help="Advertised host or IP for this primary node. Defaults to the first detected LAN IP.",
    ),
    docker_network_mode: Optional[str] = typer.Option(
        None,
        "--network",
        help="Docker network mode for the join handshake: overlay, bridge, or disabled.",
    ),
    docker_network_name: Optional[str] = typer.Option(
        DEFAULT_DOCKER_NETWORK_NAME,
        "--docker-network",
        help="Docker network name to use for bridge/overlay mode.",
    ),
):
    """Federate this runtime with another MirrorNeuron node"""
    _join_network(
        seed_host=host,
        token=token,
        host=local_host,
        grpc_port=grpc_port,
        docker_network_mode=docker_network_mode,
        docker_network_name=docker_network_name,
        action="Node join",
    )

def expose_node(
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help="Advertised host or IP that the main MirrorNeuron node can reach.",
    ),
    grpc_port: int = typer.Option(int(DEFAULT_GRPC_PORT), "--grpc-port", help="Core gRPC port."),
    dist_port: int = typer.Option(
        int(DEFAULT_DIST_PORT),
        "--dist-port",
        help="Erlang distribution port advertised by the exposed runtime.",
    ),
    redis_port: Optional[int] = typer.Option(
        None,
        "--redis-port",
        help="Redis port advertised by the exposed runtime.",
    ),
    docker_network_mode: Optional[str] = typer.Option(
        None,
        "--network",
        help="Docker network mode for the exposed node: overlay, bridge, or disabled.",
    ),
    docker_network_name: Optional[str] = typer.Option(
        DEFAULT_DOCKER_NETWORK_NAME,
        "--docker-network",
        help="Docker network name to use for bridge/overlay mode.",
    ),
):
    """Expose this box as a core-only node that a main node can add"""
    _start_network_seed(
        host=host,
        grpc_port=grpc_port,
        dist_port=dist_port,
        redis_port=redis_port,
        docker_network_mode=docker_network_mode,
        docker_network_name=docker_network_name,
    )

def add_node(
    host: str,
    token: str = typer.Option(..., "--token", help="Join token printed by mn runtime start on the remote node."),
    grpc_port: int = typer.Option(int(DEFAULT_GRPC_PORT), "--grpc-port", help="Remote exposed node gRPC port."),
    docker_network_mode: Optional[str] = typer.Option(
        None,
        "--network",
        help="Docker network mode for the local add handshake: overlay, bridge, or disabled.",
    ),
    docker_network_name: Optional[str] = typer.Option(
        DEFAULT_DOCKER_NETWORK_NAME,
        "--docker-network",
        help="Docker network name to validate for bridge/overlay mode.",
    ),
):
    """Add a remote runtime as a reciprocal federated peer"""
    _join_network(
        seed_host=host,
        token=token,
        grpc_port=grpc_port,
        docker_network_mode=docker_network_mode,
        docker_network_name=docker_network_name,
        action="Node add",
    )


def remove_node(
    node_name: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Confirm peer removal."),
):
    """Remove one reciprocal federated peer from this runtime."""

    yes = yes is True
    _confirm_node_removal(node_name, yes=yes)
    try:
        from mn_cli.shared import client

        status = client.remove_federated_peer(node_name)
        result = {"node_name": node_name, "status": status}
        print_success_confirmation(
            console,
            "Node removal",
            status=status,
            details={"Node": node_name},
            next_steps="mn node list",
        )
        record_result(result)
    except Exception as exc:
        handle_cli_error(
            exc,
            console,
            "node remove",
            command_context={"node_name": node_name},
        )


def _confirm_node_removal(node_name: str, *, yes: bool) -> None:
    require_confirmation(
        console,
        action="Node removal",
        prompt=(
            f"Remove federated peer {node_name}? Existing jobs stay on their "
            "owner node until the peer is joined again."
        ),
        yes=yes,
    )

def stop():
    """Stop MirrorNeuron services"""
    print_info(console, "Stopping MirrorNeuron services…")
    _clear_join_owner_metadata()
    _stop_network_runtime()
    
    if runtime_compose_available():
        print_info(console, "Stopping Docker runtime (Compose)…")
        subprocess.run(runtime_compose_cmd("down"), stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        try:
            from mn_sdk.native_resources import stop_docker_worker_services

            stop_docker_worker_services()
        except Exception:
            logger.debug("Failed to stop DockerWorker Compose services during runtime stop", exc_info=True)
        subprocess.run(["docker", "rm", "-f", COMPOSE_SENTINEL_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "-f", SYNCTHING_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    else:
        print_info(console, "Stopping Core service (Docker: mirror-neuron-core)…")
        subprocess.run(["docker", "stop", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "-f", SYNCTHING_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

    for pid_file, name in [
        *web_ui_pid_files(),
        *api_pid_files(),
        *native_sdk_grpc_pid_files(),
    ]:
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text().strip())
                try:
                    os.kill(pid, 0)
                    print_info(console, f"Stopping {name} (PID {pid})…")
                    kill_tree(pid)
                    time.sleep(1)
                except OSError:
                    pass
            except ValueError:
                pass
            pid_file.unlink()
    _stop_matching_sidecar_processes("mn-api", "REST API")
    _stop_matching_sidecar_processes("mn-native-sdk-grpc", "Native SDK gRPC")
    _stop_matching_sidecar_processes("mn-web-ui-server", "Web UI")
    print_success_confirmation(
        console,
        "Runtime stop",
        status="stopped",
        details={"Services": "all"},
        next_steps="mn runtime start",
    )


def cleanup(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report confirmed cleanup candidates without removing them.",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Apply cleanup without prompting."),
    include_cache: bool = typer.Option(
        False,
        "--include-cache",
        help="Also apply the managed DockerWorker image cache policy.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
):
    """Safely reconcile node-local Docker and OpenShell resources."""

    from mn_sdk.native_resource_registry import reconcile_native_resources
    from mn_sdk.native_runtime_service import _native_resource_reference_checker

    try:
        checker = _native_resource_reference_checker()
        preview = reconcile_native_resources(
            reference_checker=checker,
            dry_run=True,
            include_cache=include_cache,
            observation_threshold=1,
            discover_legacy_resources=True,
        )
        if dry_run:
            result = _native_cleanup_result(preview, dry_run=True)
        else:
            candidate_count = int(preview.get("removed_count") or 0)
            cache_count = int((preview.get("cache") or {}).get("removed_count") or 0)
            if candidate_count or cache_count:
                require_confirmation(
                    console,
                    action="Native resource cleanup",
                    prompt=(
                        f"Remove {candidate_count} confirmed orphan resource(s)"
                        + (
                            f" and {cache_count} managed cache image(s)"
                            if include_cache
                            else ""
                        )
                        + "?"
                    ),
                    yes=yes is True,
                )
            checker = _native_resource_reference_checker()
            applied = reconcile_native_resources(
                reference_checker=checker,
                dry_run=False,
                include_cache=include_cache,
                observation_threshold=1,
                discover_legacy_resources=True,
            )
            result = _native_cleanup_result(applied, dry_run=False)

        if json_output:
            console.print_json(json.dumps(result, sort_keys=True))
        else:
            status = "dry_run" if dry_run else "completed"
            print_success_confirmation(
                console,
                "Native resource cleanup",
                status=status,
                details={
                    "Candidates" if dry_run else "Removed": result["removed_count"],
                    "Reclaimed bytes": result["reclaimed_bytes"],
                    "Preserved": result["preserved_count"],
                    "Deferred": result["deferred_count"],
                    "Errors": len(result["errors"]),
                },
                next_steps=(
                    "mn runtime cleanup --yes"
                    if dry_run and result["removed_count"]
                    else "mn runtime status"
                ),
            )
        record_result(result)
    except (typer.Exit, typer.Abort):
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "runtime cleanup")


def _native_cleanup_result(summary: dict, *, dry_run: bool) -> dict:
    cache = summary.get("cache") if isinstance(summary.get("cache"), dict) else {}
    errors = [*_sanitized_cleanup_errors(summary.get("errors"))]
    errors.extend(_sanitized_cleanup_errors(cache.get("errors")))
    removed = summary.get("removed") if isinstance(summary.get("removed"), list) else []
    preserved = summary.get("preserved") if isinstance(summary.get("preserved"), list) else []
    deferred = summary.get("deferred") if isinstance(summary.get("deferred"), list) else []
    return {
        "dry_run": dry_run,
        "removed_count": len(removed),
        "cache_removed_count": int(cache.get("removed_count") or 0),
        "reclaimed_bytes": int(cache.get("reclaimed_bytes") or 0),
        "removed": removed,
        "preserved_count": len(preserved),
        "preserved": preserved,
        "deferred_count": len(deferred),
        "deferred": deferred,
        "errors": errors,
    }


def _sanitized_cleanup_errors(values: object) -> list[str]:
    if not isinstance(values, list):
        return []
    return [
        "".join(character for character in str(value) if character.isprintable())[:500]
        for value in values
    ]


def health(
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
    timeout: float = typer.Option(3.0, "--timeout", min=0.1, help="Per-component timeout in seconds."),
    repair: bool = typer.Option(False, "--repair", help="Restart unhealthy API/Web UI sidecars when possible."),
):
    """Report Core gRPC, REST API, and Web UI health"""
    from mn_cli.libs.runtime_health import health as runtime_health

    runtime_health(json_output=json_output, timeout=timeout, repair=repair)


def status(
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
    timeout: float = typer.Option(3.0, "--timeout", min=0.1, help="Per-component timeout in seconds."),
):
    """Report runtime endpoints, health, nodes, jobs, and shared storage"""
    from mn_cli.libs.runtime_health import status as runtime_status

    runtime_status(json_output=json_output, timeout=timeout)


def doctor(
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
    timeout: float = typer.Option(3.0, "--timeout", min=0.1, help="Per-component timeout in seconds."),
    repair: bool = typer.Option(False, "--repair", help="Repair unhealthy replaceable runtime sidecars, then recheck."),
):
    """Check runtime foundation services before running blueprints"""
    from mn_cli.libs.runtime_health import doctor as runtime_doctor

    runtime_doctor(json_output=json_output, timeout=timeout, repair=repair)


def restart_sidecars(
    api: bool = typer.Option(False, "--api", help="Restart the REST API sidecar."),
    web_ui: bool = typer.Option(False, "--web-ui", help="Restart the Web UI sidecar."),
):
    """Restart only the REST API and/or Web UI sidecars"""
    restart_api = bool(api)
    restart_web_ui = bool(web_ui)
    if not restart_api and not restart_web_ui:
        restart_api = True
        restart_web_ui = True

    print_info(console, "Restarting MirrorNeuron runtime sidecars…")
    env = _sidecar_runtime_env()
    details: list[tuple[str, str]] = []
    restarted_any = False

    if restart_api:
        print_info(console, "Restarting REST API sidecar…")
        _stop_sidecar_processes(api_pid_files())
        _stop_matching_sidecar_processes("mn-api", "REST API")
        api_started = _start_api_if_installed(env)
        details.append(("REST API", "restarted" if api_started else "skipped"))
        restarted_any = restarted_any or api_started

    if restart_web_ui:
        print_info(console, "Restarting Web UI sidecar…")
        _stop_sidecar_processes(web_ui_pid_files())
        _stop_matching_sidecar_processes("mn-web-ui-server", "Web UI")
        web_ui_started = _start_web_ui_if_installed(env)
        details.append(("Web UI", "restarted" if web_ui_started else "skipped"))
        restarted_any = restarted_any or web_ui_started

    _write_runtime_endpoints_file(env, web_ui_available=find_web_ui_dir() is not None)

    if not restarted_any:
        print_error(console, "No selected sidecars could be restarted.")
        raise typer.Exit(1)

    print_success_confirmation(
        console,
        "Runtime sidecar restart",
        status="complete",
        details=details,
        next_steps="mn runtime status",
    )

def ensure_context_engine(
    force: bool = typer.Option(False, "--force", help="Recreate the context engine even if it is running."),
):
    """Prepare and start the packaged Membrane context engine service"""
    try:
        console.print(f"[cyan]{CONTEXT_ENGINE_EXPECTATION}[/cyan]")
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
            disable=not use_progress(),
        ) as progress:
            task = progress.add_task(
                "[cyan]Preparing context memory: checking Membrane and Docker Model Runner...",
                total=None,
            )
            summary = ensure_context_engine_runtime(force=force, prepare_image=True)
            progress.update(task, description="[green]Context memory is ready.")
        details = [
            ("Service", summary["service"]),
            ("Model", summary["model"]),
            ("Model status", summary.get("model_status", "unknown")),
        ]
        if summary.get("membrane_dir"):
            details.append(("Membrane", summary["membrane_dir"]))
        if summary.get("engine_image"):
            details.append(("Engine image", summary["engine_image"]))

        print_success_confirmation(
            console,
            "Context engine",
            status=summary["status"],
            details=details,
            next_steps="mn runtime doctor",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "runtime ensure-context-engine")
        raise typer.Exit(1)

def _sidecar_runtime_env() -> dict[str, str]:
    compose_runtime = runtime_compose_available()
    env = _runtime_base_env(compose_runtime)
    env.setdefault("MN_API_HOST", DEFAULT_HOST)
    env["MN_API_PORT"] = _sidecar_port_value(env, "MN_API_PORT", DEFAULT_API_PORT)
    env.setdefault("MN_WEB_UI_HOST", DEFAULT_HOST)
    env["MN_WEB_UI_PORT"] = _sidecar_port_value(env, "MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)
    return env

def _sidecar_port_value(env: dict[str, str], key: str, default: str) -> str:
    value = str(env.get(key) or "").strip()
    if not value:
        value = default
    return _valid_port_text(value, default)

def _stop_sidecar_processes(pid_files: tuple[tuple[Path, str], ...]) -> bool:
    stopped = False
    for pid_file, name in pid_files:
        if not pid_file.exists():
            continue
        try:
            pid = int(pid_file.read_text().strip())
        except (ValueError, OSError):
            _unlink_pid_file(pid_file)
            continue
        try:
            os.kill(pid, 0)
        except OSError:
            _unlink_pid_file(pid_file)
            continue
        print_info(console, f"Stopping {name} (PID {pid})…")
        kill_tree(pid)
        stopped = True
        _unlink_pid_file(pid_file)
    if stopped:
        time.sleep(1)
    return stopped

def _unlink_pid_file(pid_file: Path) -> None:
    try:
        pid_file.unlink(missing_ok=True)
    except OSError:
        pass

def refresh_token():
    """Rotate the persistent MirrorNeuron network join token"""
    token = _refresh_network_token()
    print_success_confirmation(
        console,
        "Network join token refresh",
        details={"New token": token},
        next_steps="restart MirrorNeuron on cluster boxes",
    )
