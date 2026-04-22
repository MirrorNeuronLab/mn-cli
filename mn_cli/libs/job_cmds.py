import typer
import json
from rich.table import Table
from mn_cli.shared import console, client

def submit(manifest_path: str):
    """Submit a new workflow job"""
    try:
        with open(manifest_path, "r") as f:
            manifest = f.read()

        job_id = client.submit_job(manifest, {})
        console.print(f"[green]Job submitted successfully. Job ID: {job_id}[/green]")
    except Exception as e:
        console.print(f"[red]Error submitting job: {e}[/red]")


def status(job_id: str):
    """Get the status of a job"""
    try:
        job_json = client.get_job(job_id)
        job = json.loads(job_json)
        console.print_json(data=job)
    except Exception as e:
        console.print(f"[red]Error fetching job status: {e}[/red]")


def list_jobs(running_only: bool = typer.Option(False, "--running-only", help="Only show running jobs")):
    """List all jobs"""
    try:
        jobs_json = client.list_jobs()
        data = json.loads(jobs_json)

        table = Table("Job ID", "Graph ID", "Status", "Submitted At")
        for job in data.get("data", []):
            status = job.get("status", "N/A")
            if running_only and status not in ["running", "pending", "scheduled", "validated", "paused"]:
                continue
                
            table.add_row(
                job.get("job_id", "N/A"),
                job.get("graph_id", "N/A"),
                status,
                job.get("submitted_at", "N/A"),
            )
        console.print(table)
    except Exception as e:
        console.print(f"[red]Error listing jobs: {e}[/red]")


def clear():
    """Remove all job records except running ones"""
    try:
        cleared_count = client.clear_jobs()
        console.print(f"[green]Successfully cleared {cleared_count} non-running jobs.[/green]")
    except Exception as e:
        console.print(f"[red]Error clearing jobs: {e}[/red]")

def cancel(job_id: str):
    """Cancel a running job"""
    try:
        status = client.cancel_job(job_id)
        console.print(f"[green]Job cancelled. Status: {status}[/green]")
    except Exception as e:
        console.print(f"[red]Error cancelling job: {e}[/red]")


def pause(job_id: str):
    """Pause a running job"""
    try:
        status = client.pause_job(job_id)
        console.print(f"[green]Job paused. Status: {status}[/green]")
    except Exception as e:
        console.print(f"[red]Error pausing job: {e}[/red]")


def resume(job_id: str):
    """Resume a paused job"""
    try:
        status = client.resume_job(job_id)
        console.print(f"[green]Job resumed. Status: {status}[/green]")
    except Exception as e:
        console.print(f"[red]Error resuming job: {e}[/red]")


def nodes():
    """Get system summary and nodes"""
    try:
        summary_json = client.get_system_summary()
        summary = json.loads(summary_json)
        console.print_json(data=summary)
    except Exception as e:
        console.print(f"[red]Error fetching nodes: {e}[/red]")
