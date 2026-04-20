import os
import signal
import subprocess
import time
from pathlib import Path
import typer
from rich.console import Console

console = Console()

DIR = Path.home() / ".mirror_neuron"
PID_DIR = DIR / ".pids"
LOG_DIR = DIR / ".logs"
BEAM_PID_FILE = PID_DIR / "beam.pid"
API_PID_FILE = PID_DIR / "api.pid"
BEAM_LOG = LOG_DIR / "beam.log"
API_LOG = LOG_DIR / "api.log"
VENV_DIR = Path.home() / ".local" / "share" / "mn_venv"

def check_status(pid_file: Path) -> int:
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)
            return 0 # Running
        except (ValueError, OSError):
            return 1 # Stale
    return 2 # Not running

def kill_tree(parent_pid: int):
    try:
        os.kill(parent_pid, 0)
    except OSError:
        return
    
    try:
        children = subprocess.check_output(['pgrep', '-P', str(parent_pid)], stderr=subprocess.DEVNULL)
        for child_pid in children.decode().split():
            if child_pid.strip():
                kill_tree(int(child_pid.strip()))
    except subprocess.CalledProcessError:
        pass
    
    try:
        os.kill(parent_pid, signal.SIGTERM)
    except OSError:
        pass

def _start_server(ip: str = None):
    if check_status(API_PID_FILE) == 0:
        console.print("[red]Error: MirrorNeuron API is already running.[/red]")
        console.print("Use 'mn stop' to stop it first.")
        raise typer.Exit(1)
        
    try:
        docker_status = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", "mirror-neuron-core"], capture_output=True, text=True)
        if docker_status.stdout.strip() == "true":
            console.print("[red]Error: MirrorNeuron Core (Docker) is already running.[/red]")
            console.print("Use 'mn stop' to stop it first.")
            raise typer.Exit(1)
    except FileNotFoundError:
        console.print("[red]Error: Docker is not installed or not in PATH.[/red]")
        raise typer.Exit(1)

    PID_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    console.print("===========================================")
    if ip:
        console.print(f"Joining Cluster at {ip} in Detached Mode...")
    else:
        console.print("Starting Services in Detached Mode...")
    console.print("===========================================")

    env = os.environ.copy()
    if ip:
        env["MIRROR_NEURON_CLUSTER_NODES"] = ip

    console.print("=> Starting MirrorNeuron Core Service (Docker)...")
    subprocess.run(["docker", "rm", "-f", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    
    cmd = ["docker", "run", "-d", "--name", "mirror-neuron-core", "--network", "host"]
    if ip:
        cmd.extend(["-e", f"MIRROR_NEURON_CLUSTER_NODES={ip}"])
    cmd.append("mirror-neuron-core:latest")
    
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
        console.print("   [green][Started][/green] Core Service (Docker: mirror-neuron-core)")
    except subprocess.CalledProcessError:
        console.print("[red]Failed to start Core Service Docker container.[/red]")
        raise typer.Exit(1)

    console.print("=> Waiting for Elixir to boot...")
    time.sleep(3)

    api_bin = VENV_DIR / "bin" / "mn-api"
    if api_bin.exists():
        console.print("=> Starting mn-api (REST on port 4001)...")
        with open(API_LOG, "w") as out:
            p_api = subprocess.Popen(
                [str(api_bin)],
                stdout=out,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True
            )
        API_PID_FILE.write_text(str(p_api.pid))
        console.print(f"   [green][Started][/green] REST API (PID: {p_api.pid})")
    else:
        console.print("[yellow]=> Warning: mn-api not found, skipping.[/yellow]")

    console.print("\n===========================================")
    if ip:
        console.print(f"MirrorNeuron is running and attempting to join cluster at {ip}!")
    else:
        console.print("MirrorNeuron is running in the background!")
    console.print("Logs are available at:")
    console.print(f"  Core: {BEAM_LOG}")
    console.print(f"  API:  {API_LOG}")
    console.print("\nRun 'mn stop' to shut down the services.")
    console.print("===========================================")

