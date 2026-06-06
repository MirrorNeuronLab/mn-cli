import subprocess
import os
import time
from typing import Optional

import typer

from mn_cli.banner import format_banner
from mn_cli.shared import console
from mn_cli.error_handler import handle_cli_error
from mn_cli.server_cmds import (
    _start_server,
    _start_network_seed,
    _start_worker_node,
    _join_network,
    _detach_local_docker_node_if_matches,
    _refresh_network_token,
    _stop_network_runtime,
    kill_tree,
    BEAM_PID_FILE,
    DEFAULT_GRPC_PORT,
    DEFAULT_DIST_PORT,
    DEFAULT_DOCKER_NETWORK_NAME,
    runtime_compose_available,
    runtime_compose_cmd,
    api_pid_files,
    web_ui_pid_files,
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
        help="Advertised host or IP for this worker node.",
    ),
    grpc_port: int = typer.Option(int(DEFAULT_GRPC_PORT), "--grpc-port", help="Core gRPC port."),
):
    """Start MirrorNeuron services"""
    console.print(format_banner("MirrorNeuron Local Runtime"))
    if worker_node:
        _start_worker_node(host=host, grpc_port=grpc_port)
    else:
        _start_server()

def join(
    host: str,
    token: str = typer.Option(..., "--token", help="Worker token printed by mn runtime start --worker-node."),
    grpc_port: int = typer.Option(int(DEFAULT_GRPC_PORT), "--grpc-port", help="Worker node gRPC port."),
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
        grpc_port=grpc_port,
        docker_network_mode=docker_network_mode,
        docker_network_name=docker_network_name,
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
    )

def stop():
    """Stop MirrorNeuron services"""
    console.print("=> Stopping MirrorNeuron Services...")
    _stop_network_runtime()
    
    if runtime_compose_available():
        console.print("   Stopping Docker runtime (Compose)...")
        subprocess.run(runtime_compose_cmd("down"), stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    else:
        console.print("   Stopping Core Service (Docker: mirror-neuron-core)...")
        subprocess.run(["docker", "stop", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

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
    console.print("=> [green]All services stopped.[/green]")

def health(
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
    timeout: float = typer.Option(3.0, "--timeout", min=0.1, help="Per-component timeout in seconds."),
    repair: bool = typer.Option(False, "--repair", help="Restart unhealthy API/Web UI sidecars when possible."),
):
    """Report Core gRPC, REST API, and Web UI health"""
    from mn_cli.libs.runtime_health import health as runtime_health

    runtime_health(json_output=json_output, timeout=timeout, repair=repair)

def leave(node_name: str):
    """Remove a node from the cluster by its node name (e.g. mirror_neuron@192.168.4.173)"""
    from mn_cli.shared import client, console
    try:
        status = client.remove_node(node_name)
        _detach_local_docker_node_if_matches(node_name)
        console.print(f"[green]Successfully requested {node_name} to leave. Status: {status}[/green]")
    except Exception as e:
        handle_cli_error(e, console, 'leave')

def refresh_token():
    """Rotate the persistent MirrorNeuron network join token"""
    token = _refresh_network_token()
    console.print("[green]MirrorNeuron network join token refreshed.[/green]")
    console.print("Restart MirrorNeuron on cluster boxes for the new token to take effect.")
    console.print("New token:")
    console.print(f"  {token}")
