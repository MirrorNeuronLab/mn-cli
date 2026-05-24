import subprocess
import os
import time
from typing import Optional

import typer

from mn_cli.shared import console
from mn_cli.error_handler import handle_cli_error
from mn_cli.server_cmds import (
    _start_server,
    _start_network_seed,
    _join_network,
    _stop_network_runtime,
    kill_tree,
    BEAM_PID_FILE,
    API_PID_FILE,
    WEB_UI_PID_FILE,
    runtime_compose_available,
    runtime_compose_cmd,
)

def start():
    """Start MirrorNeuron services"""
    _start_server()

def join(
    ip: str,
    token: str = typer.Option(..., "--token", help="Network join token printed by mn start."),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help="Advertised host or IP for this joining node.",
    ),
    grpc_port: int = typer.Option(50051, "--grpc-port", help="Main node gRPC port."),
    dist_port: int = typer.Option(4370, "--dist-port", help="Local Erlang distribution port."),
    redis_port: Optional[int] = typer.Option(
        None,
        "--redis-port",
        help="Override the Redis port returned by the main node handshake.",
    ),
):
    """Join a MirrorNeuron cluster using the main node host and token"""
    _start_server(
        ip,
        token=token,
        host=host,
        grpc_port=grpc_port,
        dist_port=dist_port,
        redis_port=redis_port,
    )

def expose_node(
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help="Advertised host or IP that the main MirrorNeuron node can reach.",
    ),
    grpc_port: int = typer.Option(50051, "--grpc-port", help="Core gRPC port."),
    dist_port: int = typer.Option(4370, "--dist-port", help="Erlang distribution port."),
    redis_port: Optional[int] = typer.Option(
        None,
        "--redis-port",
        help="Explicit Redis port for this exposed node; defaults to a persisted dynamic port.",
    ),
    force_new_token: bool = typer.Option(
        False,
        "--force-new-token",
        help="Replace the persisted node exposure token.",
    ),
):
    """Expose this box as a core-only node that a main node can add"""
    _start_network_seed(
        host=host,
        grpc_port=grpc_port,
        dist_port=dist_port,
        redis_port=redis_port,
        force_new_token=force_new_token,
    )

def add_node(
    host: str,
    token: str = typer.Option(..., "--token", help="Token printed by mn expose-node on the remote box."),
    grpc_port: int = typer.Option(50051, "--grpc-port", help="Remote exposed node gRPC port."),
):
    """Add a remote exposed node to the local/main MirrorNeuron cluster"""
    _join_network(
        seed_host=host,
        token=token,
        grpc_port=grpc_port,
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
        (WEB_UI_PID_FILE, "Web UI"),
        (API_PID_FILE, "REST API"),
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

def leave(node_name: str):
    """Remove a node from the cluster by its node name (e.g. mirror_neuron@192.168.4.173)"""
    from mn_cli.shared import client, console
    try:
        status = client.remove_node(node_name)
        console.print(f"[green]Successfully requested {node_name} to leave. Status: {status}[/green]")
    except Exception as e:
        handle_cli_error(e, console, 'leave')
