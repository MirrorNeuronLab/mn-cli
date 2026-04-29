import json
from rich.table import Table
from mn_cli.shared import console, client, logger
from mn_cli.error_handler import handle_cli_error
import typer


def submit(manifest_path: str):
    """Submit a new workflow job"""
    try:
        with open(manifest_path, "r") as f:
            manifest = f.read()

        job_id = client.submit_job(manifest, {})
        logger.info("Submitted job id=%s from manifest=%s", job_id, manifest_path)
        console.print(f"[green]Job submitted successfully. Job ID: {job_id}[/green]")
    except Exception as e:
        handle_cli_error(e, console, 'submit')


def status(job_id: str):
    """Get the status of a job"""
    try:
        job_json = client.get_job(job_id)
        job = json.loads(job_json)
        console.print_json(data=job)
    except Exception as e:
        handle_cli_error(e, console, 'status')


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
        handle_cli_error(e, console, 'list_jobs')


def clear():
    """Remove all job records except running ones"""
    try:
        cleared_count = client.clear_jobs()
        logger.info("Cleared %d non-running jobs", cleared_count)
        console.print(f"[green]Successfully cleared {cleared_count} non-running jobs.[/green]")
    except Exception as e:
        handle_cli_error(e, console, 'clear')

def cancel(job_id: str):
    """Cancel a running job"""
    try:
        status = client.cancel_job(job_id)
        console.print(f"[green]Job cancelled. Status: {status}[/green]")
    except Exception as e:
        handle_cli_error(e, console, 'cancel')


def pause(job_id: str):
    """Pause a running job"""
    try:
        status = client.pause_job(job_id)
        console.print(f"[green]Job paused. Status: {status}[/green]")
    except Exception as e:
        handle_cli_error(e, console, 'pause')


def resume(job_id: str):
    """Resume a paused job"""
    try:
        status = client.resume_job(job_id)
        console.print(f"[green]Job resumed. Status: {status}[/green]")
    except Exception as e:
        handle_cli_error(e, console, 'resume')


def nodes():
    """Get system summary and nodes"""
    try:
        summary_json = client.get_system_summary()
        summary = json.loads(summary_json)
        console.print_json(data=summary)
    except Exception as e:
        handle_cli_error(e, console, 'nodes')


def metrics():
    """Show runtime metrics derived from the core system summary"""
    try:
        summary = json.loads(client.get_system_summary())
        if "metrics" in summary:
            console.print_json(data=summary["metrics"])
            return

        jobs = summary.get("jobs", [])
        status_counts = {}
        queue_depth_total = 0
        queue_depth_max = 0
        pressured_agents = 0

        for job in jobs:
            status = job.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
            for agent in job.get("agents", []):
                pressure = agent.get("backpressure", {})
                depth = int(pressure.get("queue_depth", agent.get("mailbox_depth", 0)) or 0)
                queue_depth_total += depth
                queue_depth_max = max(queue_depth_max, depth)
                if pressure.get("backpressure") is True:
                    pressured_agents += 1

        console.print_json(
            data={
                "jobs": {"total": len(jobs), "by_status": status_counts},
                "agents": {
                    "queue_depth_total": queue_depth_total,
                    "queue_depth_max": queue_depth_max,
                    "pressured": pressured_agents,
                },
                "nodes": {"total": len(summary.get("nodes", []))},
                "source": "system_summary",
            }
        )
    except Exception as e:
        handle_cli_error(e, console, "metrics")


def dead_letters(job_id: str):
    """List dead-letter events for a job"""
    try:
        letters = []
        for index, event_json in enumerate(client.stream_events(job_id)):
            event = json.loads(event_json)
            if event.get("type") == "dead_letter":
                letters.append(
                    {
                        "index": len(letters),
                        "event_index": index,
                        "agent_id": event.get("agent_id"),
                        "reason": event.get("reason") or event.get("error"),
                        "timestamp": event.get("timestamp"),
                        "message": event.get("message"),
                    }
                )
        console.print_json(data={"job_id": job_id, "data": letters})
    except Exception as e:
        handle_cli_error(e, console, "dead_letters")
