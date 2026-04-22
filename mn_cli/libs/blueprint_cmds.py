import os
import json
import subprocess
import typer
from rich.table import Table
from mn_cli.shared import console
from mn_cli.libs.run_cmds import run as _run_bundle

blueprint_app = typer.Typer(help="Manage and run MirrorNeuron blueprints")

@blueprint_app.command("list")
def blueprint_list():
    """List all available blueprints from the local storage shared with mn staff"""
    index_path = os.path.expanduser("~/.mn/blueprints/index.json")
    if not os.path.exists(index_path):
        console.print("[yellow]Blueprint storage not initialized. Run 'mn blueprint run <name>' to initialize.[/yellow]")
        return
    try:
        with open(index_path, "r") as f:
            blueprints = json.load(f)
        table = Table("ID", "Name", "Job Name", "Description")
        for bp in blueprints:
            table.add_row(
                bp.get("id", "N/A"),
                bp.get("name", "N/A"),
                bp.get("job_name", "N/A"),
                bp.get("description", "")
            )
        console.print(table)
    except Exception as e:
        console.print(f"[red]Error reading blueprints index: {e}[/red]")

@blueprint_app.command("run")
def blueprint_run(blueprint_path_name: str):
    """Run a blueprint by name"""
    storage_dir = os.path.expanduser("~/.mn/blueprints")
    if not os.path.exists(storage_dir):
        console.print(f"Initializing blueprint storage at {storage_dir}...")
        os.makedirs(os.path.dirname(storage_dir), exist_ok=True)
        res = subprocess.run(["git", "clone", "https://github.com/MirrorNeuronLab/mn-blueprints", storage_dir], capture_output=True, text=True)
        if res.returncode != 0:
            console.print(f"[red]Failed to clone blueprint repository: {res.stderr}[/red]")
            raise typer.Exit(1)
    else:
        console.print(f"Updating blueprint storage at {storage_dir}...")
        res = subprocess.run(["git", "-C", storage_dir, "pull"], capture_output=True, text=True)
        if res.returncode != 0:
            console.print(f"[yellow]Warning: Failed to update blueprint repository: {res.stderr}[/yellow]")
    
    index_path = os.path.join(storage_dir, "index.json")
    if not os.path.exists(index_path):
        console.print("[red]Error: index.json not found in blueprint storage.[/red]")
        raise typer.Exit(1)
        
    try:
        with open(index_path, "r") as f:
            blueprints = json.load(f)
    except Exception as e:
        console.print(f"[red]Error parsing index.json: {e}[/red]")
        raise typer.Exit(1)
        
    target_bp = None
    for bp in blueprints:
        if bp.get("id") == blueprint_path_name or bp.get("path") == blueprint_path_name:
            target_bp = bp
            break
            
    if not target_bp:
        console.print(f"[red]Error: Blueprint '{blueprint_path_name}' not found in index.[/red]")
        raise typer.Exit(1)
        
    bp_path = os.path.join(storage_dir, target_bp.get("path"))
    
    # validate it first (check manifest.json)
    manifest_path = os.path.join(bp_path, "manifest.json")
    if not os.path.exists(manifest_path):
        console.print(f"[red]Error: Blueprint '{blueprint_path_name}' is missing manifest.json. Validation failed.[/red]")
        raise typer.Exit(1)
        
    console.print(f"[green]Blueprint '{blueprint_path_name}' validated. Running...[/green]")
    _run_bundle(bp_path)
