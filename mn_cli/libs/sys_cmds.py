import typer
import subprocess
import os
import time
from mn_cli.shared import console
from mn_cli.server_cmds import _start_server, kill_tree, BEAM_PID_FILE, API_PID_FILE

def start():
    """Start MirrorNeuron server"""
    _start_server()

def stop():
    """Stop MirrorNeuron server"""
    console.print("=> Stopping MirrorNeuron Services...")
    
    console.print("   Stopping Core Service (Docker: mirror-neuron-core)...")
    subprocess.run(["docker", "stop", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "rm", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

    for pid_file, name in [(API_PID_FILE, "REST API"), (BEAM_PID_FILE, "Legacy Core Service")]:
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

def join(ip: str):
    """Join a MirrorNeuron cluster using the IP"""
    _start_server(ip)

