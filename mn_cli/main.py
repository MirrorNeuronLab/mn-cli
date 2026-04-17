import typer
import json
import os
from pathlib import Path
from mn_sdk import Client
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="MirrorNeuron CLI")
console = Console()
client = Client()


@app.command()
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

        console.print(f"Monitoring events for {job_id}... (Press Ctrl+C to detach)")
        for event_json in client.stream_events(job_id):
            console.print_json(event_json)

    except KeyboardInterrupt:
        console.print(
            f"\n[yellow]Detached from log stream. Job {job_id} is still running.[/yellow]"
        )
        console.print(f"To monitor again, run: [bold]mn monitor {job_id}[/bold]")
    except Exception as e:
        console.print(f"[red]Error running bundle: {e}[/red]")


@app.command()
def submit(manifest_path: str):
    """Submit a new workflow job"""
    try:
        with open(manifest_path, "r") as f:
            manifest = f.read()

        job_id = client.submit_job(manifest, {})
        console.print(f"[green]Job submitted successfully. Job ID: {job_id}[/green]")
    except Exception as e:
        console.print(f"[red]Error submitting job: {e}[/red]")


@app.command()
def status(job_id: str):
    """Get the status of a job"""
    try:
        job_json = client.get_job(job_id)
        job = json.loads(job_json)
        console.print_json(data=job)
    except Exception as e:
        console.print(f"[red]Error fetching job status: {e}[/red]")


@app.command()
def list():
    """List all jobs"""
    try:
        jobs_json = client.list_jobs()
        data = json.loads(jobs_json)

        table = Table("Job ID", "Graph ID", "Status", "Submitted At")
        for job in data.get("data", []):
            table.add_row(
                job.get("job_id", "N/A"),
                job.get("graph_id", "N/A"),
                job.get("status", "N/A"),
                job.get("submitted_at", "N/A"),
            )
        console.print(table)
    except Exception as e:
        console.print(f"[red]Error listing jobs: {e}[/red]")


@app.command()
def cancel(job_id: str):
    """Cancel a running job"""
    try:
        status = client.cancel_job(job_id)
        console.print(f"[green]Job cancelled. Status: {status}[/green]")
    except Exception as e:
        console.print(f"[red]Error cancelling job: {e}[/red]")


@app.command()
def pause(job_id: str):
    """Pause a running job"""
    try:
        status = client.pause_job(job_id)
        console.print(f"[green]Job paused. Status: {status}[/green]")
    except Exception as e:
        console.print(f"[red]Error pausing job: {e}[/red]")


@app.command()
def resume(job_id: str):
    """Resume a paused job"""
    try:
        status = client.resume_job(job_id)
        console.print(f"[green]Job resumed. Status: {status}[/green]")
    except Exception as e:
        console.print(f"[red]Error resuming job: {e}[/red]")


@app.command()
def nodes():
    """Get system summary and nodes"""
    try:
        summary_json = client.get_system_summary()
        summary = json.loads(summary_json)
        console.print_json(data=summary)
    except Exception as e:
        console.print(f"[red]Error fetching nodes: {e}[/red]")


@app.command()
def monitor(job_id: str):
    """Stream live events for a job"""
    try:
        console.print(f"Monitoring events for {job_id}...")
        for event_json in client.stream_events(job_id):
            console.print_json(event_json)
    except Exception as e:
        console.print(f"[red]Error streaming events: {e}[/red]")


if __name__ == "__main__":
    app()
