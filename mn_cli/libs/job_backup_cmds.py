from __future__ import annotations

import typer
from mn_sdk.job_backup import backup_job, restore_job

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import activity, print_success_confirmation
from mn_cli.output import record_result
from mn_cli.shared import client, console


def backup(
    job_id: str = typer.Argument(help="Durable job ID. Pause active runs before backup."),
    output: str = typer.Option(..., "--output", "-o", help="Destination ZIP file."),
    air_gapped: bool = typer.Option(True, "--air-gapped/--no-air-gapped", help="Include model files, Python wheels, and Docker images."),
):
    """Back up a job, its data, history, and offline dependencies to a ZIP."""
    try:
        with activity(console, "Back up job"):
            result = backup_job(client, job_id, output, air_gapped=air_gapped)
        print_success_confirmation(console, "Job backup", details=[("Job ID", job_id), ("ZIP", result["path"])])
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "job backup")


def restore(
    input_path: str = typer.Option(..., "--input", "-i", help="Job backup ZIP file."),
    job_id: str | None = typer.Option(None, "--job-id", help="Optional new durable job ID."),
    start: bool = typer.Option(False, "--start", help="Start a fresh run after restoring the job."),
):
    """Restore a ZIP as a new job without hiring a blueprint."""
    try:
        with activity(console, "Restore job"):
            result = restore_job(client, input_path, job_id=job_id, start=start)
        print_success_confirmation(console, "Job restore", details=[("Job ID", result["job_id"]), ("Run ID", result.get("run_id"))], next_steps=result.get("start_error") or (None if result["started"] else f"mn job start {result['job_id']}"))
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "job restore")
