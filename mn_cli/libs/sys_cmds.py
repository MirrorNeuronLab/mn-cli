import subprocess
import os
import time
from typing import Optional
from pathlib import Path

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from mn_cli.banner import format_banner
from mn_cli.shared import console
from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import print_success_confirmation
from mn_cli.terminal import use_progress
from mn_cli.server_cmds import (
    _start_server,
    _start_network_seed,
    _start_worker_node,
    _join_network,
    _detach_local_docker_node_if_matches,
    _refresh_network_token,
    _stop_network_runtime,
    leave_joined_cluster_before_stop,
    _runtime_base_env,
    _start_api_if_installed,
    _start_web_ui_if_installed,
    _write_runtime_endpoints_file,
    _valid_port_text,
    ensure_context_engine_runtime,
    kill_tree,
    BEAM_PID_FILE,
    COMPOSE_SENTINEL_CONTAINER,
    SYNCTHING_CONTAINER,
    DEFAULT_HOST,
    DEFAULT_API_PORT,
    DEFAULT_GRPC_PORT,
    DEFAULT_DIST_PORT,
    DEFAULT_WEB_UI_PORT,
    DEFAULT_DOCKER_NETWORK_NAME,
    LEGACY_API_PORT,
    LEGACY_WEB_UI_PORT,
    find_web_ui_dir,
    runtime_compose_available,
    runtime_compose_cmd,
    stop_matching_sidecar_processes as _stop_matching_sidecar_processes,
    api_pid_files,
    web_ui_pid_files,
)

CONTEXT_ENGINE_EXPECTATION = (
    "This runtime service powers blueprint context memory. First launch may download the context model "
    "and start the Membrane context engine; keep Docker running and be patient."
)

def start(
    worker_node: bool = typer.Option(
        False,
        "--worker-node",
        help="Start this box as a headless resource-pool node for a primary box to join.",
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help="Advertised host or IP for this node.",
    ),
    join_host: Optional[str] = typer.Option(
        None,
        "--join-host",
        help="Start this runtime already joined to a primary node at this host.",
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        help="Network token from the primary node; required with --join-host.",
    ),
    grpc_port: int = typer.Option(int(DEFAULT_GRPC_PORT), "--grpc-port", help="Core gRPC port."),
):
    """Start MirrorNeuron services"""
    console.print(format_banner("MirrorNeuron Local Runtime"))
    if worker_node and join_host:
        console.print("[red]Error: --worker-node and --join-host cannot be used together.[/red]")
        raise typer.Exit(1)
    if join_host and not token:
        console.print("[red]Error: mn runtime start --join-host requires --token from the primary node.[/red]")
        raise typer.Exit(1)
    if worker_node:
        _start_worker_node(host=host, grpc_port=grpc_port)
    elif join_host:
        _start_server(ip=join_host, token=token, host=host, grpc_port=grpc_port)
    else:
        _start_server(host=host, grpc_port=grpc_port)

def join(
    host: str,
    token: str = typer.Option(..., "--token", help="Worker token printed by mn runtime start --worker-node."),
    grpc_port: int = typer.Option(int(DEFAULT_GRPC_PORT), "--grpc-port", help="Worker node gRPC port."),
    local_host: Optional[str] = typer.Option(
        None,
        "--local-host",
        help="Advertised host or IP for this primary node. Defaults to the first detected LAN IP.",
    ),
    dist_port: int = typer.Option(
        int(DEFAULT_DIST_PORT),
        "--dist-port",
        help="Legacy IP-mode Erlang distribution port.",
        hidden=True,
    ),
    redis_port: Optional[int] = typer.Option(
        None,
        "--redis-port",
        help="Legacy IP-mode Redis port override.",
        hidden=True,
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
    """Join a worker node into this primary MirrorNeuron cluster"""
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
        help="Legacy IP-mode Erlang distribution port.",
        hidden=True,
    ),
    redis_port: Optional[int] = typer.Option(
        None,
        "--redis-port",
        help="Legacy IP-mode Redis port override.",
        hidden=True,
    ),
    force_new_token: bool = typer.Option(
        False,
        "--force-new-token",
        help="Replace the persisted node exposure token.",
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
        force_new_token=force_new_token,
        docker_network_mode=docker_network_mode,
        docker_network_name=docker_network_name,
    )

def add_node(
    host: str,
    token: str = typer.Option(..., "--token", help="Token printed by mn node expose on the remote box."),
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
    """Add a remote exposed node to the local/main MirrorNeuron cluster"""
    _join_network(
        seed_host=host,
        token=token,
        grpc_port=grpc_port,
        docker_network_mode=docker_network_mode,
        docker_network_name=docker_network_name,
        action="Node add",
    )

def stop():
    """Stop MirrorNeuron services"""
    console.print("=> Stopping MirrorNeuron Services...")
    leave_joined_cluster_before_stop()
    _stop_network_runtime()
    
    if runtime_compose_available():
        console.print("   Stopping Docker runtime (Compose)...")
        subprocess.run(runtime_compose_cmd("down"), stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        try:
            from mn_sdk.native_resources import cleanup_docker_worker_services

            cleanup_docker_worker_services(all_services=True)
        except Exception:
            logger.debug("Failed to prune DockerWorker Compose services during runtime stop", exc_info=True)
        subprocess.run(["docker", "rm", "-f", COMPOSE_SENTINEL_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "-f", SYNCTHING_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    else:
        console.print("   Stopping Core Service (Docker: mirror-neuron-core)...")
        subprocess.run(["docker", "stop", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "-f", SYNCTHING_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

    for pid_file, name in [
        *web_ui_pid_files(),
        *api_pid_files(),
        (BEAM_PID_FILE, "Legacy Core Service"),
    ]:
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text().strip())
                try:
                    os.kill(pid, 0)
                    console.print(f"   Stopping {name} (PID: {pid})...")
                    kill_tree(pid)
                    time.sleep(1)
                except OSError:
                    pass
            except ValueError:
                pass
            pid_file.unlink()
    _stop_matching_sidecar_processes("mn-api", "REST API")
    _stop_matching_sidecar_processes("mn-web-ui-server", "Web UI")
    print_success_confirmation(
        console,
        "Runtime stop",
        status="stopped",
        details={"Services": "all"},
        next_steps="mn runtime start",
    )

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
):
    """Check runtime foundation services before running blueprints"""
    from mn_cli.libs.runtime_health import doctor as runtime_doctor

    runtime_doctor(json_output=json_output, timeout=timeout)


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

    console.print("=> Restarting MirrorNeuron runtime sidecars...")
    env = _sidecar_runtime_env()
    details: list[tuple[str, str]] = []
    restarted_any = False

    if restart_api:
        console.print("=> Restarting REST API sidecar...")
        _stop_sidecar_processes(api_pid_files())
        _stop_matching_sidecar_processes("mn-api", "REST API")
        api_started = _start_api_if_installed(env)
        details.append(("REST API", "restarted" if api_started else "skipped"))
        restarted_any = restarted_any or api_started

    if restart_web_ui:
        console.print("=> Restarting Web UI sidecar...")
        _stop_sidecar_processes(web_ui_pid_files())
        _stop_matching_sidecar_processes("mn-web-ui-server", "Web UI")
        web_ui_started = _start_web_ui_if_installed(env)
        details.append(("Web UI", "restarted" if web_ui_started else "skipped"))
        restarted_any = restarted_any or web_ui_started

    _write_runtime_endpoints_file(env, web_ui_available=find_web_ui_dir() is not None)

    if not restarted_any:
        console.print("[red]Error: no selected sidecars could be restarted.[/red]")
        raise typer.Exit(1)

    print_success_confirmation(
        console,
        "Runtime sidecar restart",
        status="complete",
        details=details,
        next_steps="mn runtime health",
    )

def ensure_context_engine(
    force: bool = typer.Option(False, "--force", help="Rebuild and recreate the context engine even if it is running."),
):
    """Ensure the Membrane context engine Compose service is installed and running"""
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
            summary = ensure_context_engine_runtime(force=force)
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
            next_steps="mn runtime health",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "runtime ensure-context-engine")
        raise typer.Exit(1)

def _sidecar_runtime_env() -> dict[str, str]:
    compose_runtime = runtime_compose_available()
    env = _runtime_base_env(compose_runtime)
    env.setdefault("MN_API_HOST", DEFAULT_HOST)
    env["MN_API_PORT"] = _sidecar_port_value(env, "MN_API_PORT", DEFAULT_API_PORT, LEGACY_API_PORT)
    env.setdefault("MN_WEB_UI_HOST", DEFAULT_HOST)
    env["MN_WEB_UI_PORT"] = _sidecar_port_value(env, "MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT, LEGACY_WEB_UI_PORT)
    return env

def _sidecar_port_value(env: dict[str, str], key: str, default: str, legacy_default: str) -> str:
    value = str(env.get(key) or "").strip()
    if not value or value == legacy_default:
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
        console.print(f"   Stopping {name} (PID: {pid})...")
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

def leave(node_name: str):
    """Remove a node from the cluster by its node name (e.g. mirror_neuron@192.168.4.173)"""
    from mn_cli.shared import client, console
    try:
        status = client.remove_node(node_name)
        _detach_local_docker_node_if_matches(node_name)
        print_success_confirmation(
            console,
            "Node leave",
            status=status,
            details={"Node": node_name},
            next_steps="mn node list",
        )
    except Exception as e:
        handle_cli_error(e, console, 'leave')

def refresh_token():
    """Rotate the persistent MirrorNeuron network join token"""
    token = _refresh_network_token()
    print_success_confirmation(
        console,
        "Network join token refresh",
        details={"New token": token},
        next_steps="restart MirrorNeuron on cluster boxes",
    )
