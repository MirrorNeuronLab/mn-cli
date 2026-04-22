import typer
import json
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from mn_cli.shared import console, client

def _stream_and_format_events(job_id: str):
    console.print(f"Monitoring events for [bold cyan]{job_id}[/bold cyan]... (Press Ctrl+C to detach)")
    
    log_dir = Path(f"/tmp/mn_{job_id}")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "events.log"
    
    console.print(f"Saving raw logs to: [bold]{log_file}[/bold]\n")
    
    status_text = "Unknown / Detached"
    status_color = "yellow"
    msg_count = 0
    current_step = "[cyan]Connecting..."
    
    try:
        with open(log_file, "a") as f:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                # We'll use 4 steps total to reach "Scheduled" (100%) for the initial submission flow.
                # Once it begins processing (messages) or completes, we'll keep it maxed or handle states.
                job_task = progress.add_task("[cyan]Initializing...", total=4)
                
                for event_json in client.stream_events(job_id):
                    f.write(event_json + "\n")
                    f.flush()
                    try:
                        event = json.loads(event_json)
                        event_type = event.get("type")
                        
                        if event_type == "job_pending":
                            current_step = "[cyan]Pending..."
                            progress.update(job_task, completed=1, description=current_step)
                        elif event_type == "job_validated":
                            current_step = "[cyan]Validated..."
                            progress.update(job_task, completed=2, description=current_step)
                        elif event_type == "job_scheduled":
                            current_step = "[green]Scheduled... (Job detached in background)"
                            progress.update(job_task, completed=4, description=current_step)
                        elif event_type == "job_running":
                            current_step = "[cyan]Running..."
                            progress.update(job_task, completed=4, description=current_step)
                        elif event_type in ["agent_message_received", "aggregator_received"]:
                            msg_count += 1
                            current_step = f"[cyan]Running (Processed {msg_count} msgs)..."
                            progress.update(job_task, completed=4, description=current_step)
                        elif event_type == "job_completed":
                            current_step = "[green]Completed successfully!"
                            progress.update(job_task, completed=4, description=current_step)
                            status_text = "Success"
                            status_color = "green"
                            break
                        elif event_type == "job_failed":
                            current_step = "[red]Job failed!"
                            progress.update(job_task, completed=4, description=current_step)
                            status_text = "Failed"
                            status_color = "red"
                            break
                    except Exception:
                        pass
                        
        if status_text in ["Success", "Failed"]:
            from rich.panel import Panel
            panel = Panel(
                f"[bold {status_color}]Job Status: {status_text}[/bold {status_color}]\n\n"
                f"Job ID: {job_id}\n"
                f"Logs: {log_file}",
                title="Job Execution Summary",
                border_style=status_color,
                expand=False
            )
            console.print(panel)
        else:
            if current_step.startswith("[green]Scheduled"):
                console.print(f"\n[green]Job successfully detached in background. It is now scheduled/running.[/green]")
            else:
                console.print(f"\n[yellow]Job stream completed prematurely. Last step: {current_step}[/yellow]")
            console.print(f"To monitor again, run: [bold]mn monitor {job_id}[/bold]")
        
    except KeyboardInterrupt:
        console.print(
            f"\n[yellow]Detached from log stream. Job {job_id} is still running.[/yellow]"
        )
        console.print(f"To monitor again, run: [bold]mn monitor {job_id}[/bold]")


def validate(bundle_path: str):
    """Check if a job bundle in a local folder is valid to run"""
    try:
        bundle_dir = Path(bundle_path)
        if not bundle_dir.is_dir():
            console.print(
                f"[red]Error: '{bundle_path}' is not a directory. Expected a bundle folder.[/red]"
            )
            raise typer.Exit(1)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(
                f"[red]Error: manifest.json not found in '{bundle_path}'[/red]"
            )
            raise typer.Exit(1)

        with open(manifest_file, "r") as f:
            try:
                manifest = json.load(f)
            except json.JSONDecodeError as e:
                console.print(f"[red]Error: manifest.json is not valid JSON. {e}[/red]")
                raise typer.Exit(1)

        required_keys = ["manifest_version", "graph_id", "job_name", "entrypoints", "nodes"]
        missing = [k for k in required_keys if k not in manifest]
        if missing:
            console.print(f"[red]Error: manifest.json is missing required keys: {', '.join(missing)}[/red]")
            raise typer.Exit(1)

        if not isinstance(manifest.get("nodes"), type([])):
            console.print("[red]Error: 'nodes' must be a list in manifest.json[/red]")
            raise typer.Exit(1)

        console.print(f"[green]✓ Job bundle at '{bundle_path}' is valid.[/green]")
        console.print(f"  - Job Name: {manifest.get('job_name')}")
        console.print(f"  - Graph ID: {manifest.get('graph_id')}")
        console.print(f"  - Nodes count: {len(manifest.get('nodes'))}")
        
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Validation failed: {e}[/red]")
        raise typer.Exit(1)


def run(bundle_path: str):
    """Run a job bundle from a local folder directly"""
    try:
        bundle_dir = Path(bundle_path)
        if not bundle_dir.is_dir():
            console.print(
                f"[red]Error: '{bundle_path}' is not a directory. Expected a bundle folder.[/red]"
            )
            raise typer.Exit(1)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(
                f"[red]Error: manifest.json not found in '{bundle_path}'[/red]"
            )
            raise typer.Exit(1)

        with open(manifest_file, "r") as f:
            manifest = f.read()

        payloads = {}
        payloads_dir = bundle_dir / "payloads"
        if payloads_dir.is_dir():
            for filepath in payloads_dir.rglob("*"):
                if filepath.is_file():
                    rel_path = filepath.relative_to(payloads_dir).as_posix()
                    with open(filepath, "rb") as f:
                        payloads[rel_path] = f.read()

        console.print(
            f"Submitting bundle '{bundle_dir.name}' with {len(payloads)} payloads..."
        )
        job_id = client.submit_job(manifest, payloads)
        console.print(f"[green]Job submitted successfully. Job ID: {job_id}[/green]")
        _stream_and_format_events(job_id)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Error running bundle: {e}[/red]")
        raise typer.Exit(1)


def monitor(job_id: str):
    """Stream live events for a job"""
    try:
        _stream_and_format_events(job_id)
    except Exception as e:
        console.print(f"[red]Error streaming events: {e}[/red]")
