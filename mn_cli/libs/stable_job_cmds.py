from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.bundles import read_bundle
from mn_cli.libs.ui import print_success_confirmation
from mn_cli.shared import client, console


run_app = typer.Typer(help="Inspect and control individual stable-job runs.")


def create(
    bundle: str = typer.Argument(help="Blueprint/job bundle directory or archive."),
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Optional stable job ID."),
    config: Optional[str] = typer.Option(
        None, "--config", help="Resolved configuration JSON file."
    ),
):
    """Create a stable job definition without starting a run."""
    try:
        manifest_json, payloads = read_bundle(bundle)
        resolved = _read_json_object(config) if config else {}
        result = json.loads(
            client.create_stable_job(
                manifest_json,
                payloads,
                job_id=job_id or "",
                resolved_configuration=resolved,
            )
        )
        print_success_confirmation(
            console,
            "Job create",
            details=[("Job ID", result.get("job_id")), ("Bundle", bundle)],
            next_steps=f"mn job start {result.get('job_id')}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "job create")


def definitions(
    include_archived: bool = typer.Option(False, "--include-archived")
):
    """List stable job definitions (v2)."""
    try:
        console.print_json(
            data=json.loads(
                client.list_stable_jobs(include_archived=include_archived)
            )
        )
    except Exception as exc:
        handle_cli_error(exc, console, "job definitions")


def inspect(job_id: str = typer.Argument(help="Stable job ID.")):
    """Inspect a stable job definition."""
    try:
        console.print_json(data=json.loads(client.get_stable_job(job_id)))
    except Exception as exc:
        handle_cli_error(exc, console, "job inspect")


def archive(job_id: str = typer.Argument(help="Stable job ID.")):
    """Archive a job while retaining its persistent data."""
    try:
        console.print_json(data=json.loads(client.archive_stable_job(job_id)))
    except Exception as exc:
        handle_cli_error(exc, console, "job archive")


def reset_data(
    job_id: str = typer.Argument(help="Stable job ID."),
    yes: bool = typer.Option(False, "--yes", "-y"),
):
    """Clear job data and advance its data generation."""
    if not yes and not typer.confirm(
        f"Reset all persistent data for {job_id}?", default=False
    ):
        return
    try:
        console.print_json(data=json.loads(client.reset_stable_job_data(job_id)))
    except Exception as exc:
        handle_cli_error(exc, console, "job reset-data")


def delete(
    job_id: str = typer.Argument(help="Stable job ID."),
    yes: bool = typer.Option(False, "--yes", "-y"),
):
    """Permanently delete a stable job and its data."""
    if not yes and not typer.confirm(
        f"Permanently delete {job_id} and all shared job data?", default=False
    ):
        return
    try:
        console.print_json(
            data=json.loads(client.delete_stable_job(job_id, confirmed=True))
        )
    except Exception as exc:
        handle_cli_error(exc, console, "job delete")


def start(
    job_id: str = typer.Argument(help="Stable job ID."),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
    inputs: Optional[str] = typer.Option(None, "--inputs", help="Run-input JSON file."),
):
    """Start a new run of a stable job."""
    try:
        result = json.loads(
            client.start_run(
                job_id,
                run_id=run_id or "",
                inputs=_read_json_object(inputs) if inputs else {},
            )
        )
        print_success_confirmation(
            console,
            "Run start",
            details=[("Job ID", job_id), ("Run ID", result.get("run_id"))],
            next_steps=f"mn run status {result.get('run_id')}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "job start")


def runs(job_id: str = typer.Argument(help="Stable job ID.")):
    """List the independent runs belonging to a job."""
    try:
        console.print_json(data=json.loads(client.list_runs(job_id)))
    except Exception as exc:
        handle_cli_error(exc, console, "job runs")


@run_app.command(name="status")
def run_status(run_id: str):
    """Inspect one execution run."""
    _print_run(client.get_run, run_id, "run status")


@run_app.command(name="pause")
def run_pause(run_id: str):
    """Pause one execution run."""
    _print_run(client.pause_run, run_id, "run pause")


@run_app.command(name="resume")
def run_resume(run_id: str):
    """Resume one execution run."""
    _print_run(client.resume_run, run_id, "run resume")


@run_app.command(name="cancel")
def run_cancel(run_id: str):
    """Cancel one execution run without deleting job data."""
    _print_run(client.cancel_run, run_id, "run cancel")


@run_app.command(name="delete")
def run_delete(run_id: str, yes: bool = typer.Option(False, "--yes", "-y")):
    """Delete one terminal run without deleting job data."""
    if not yes and not typer.confirm(f"Delete run {run_id}?", default=False):
        return
    try:
        console.print_json(
            data=json.loads(client.delete_run(run_id, confirmed=True))
        )
    except Exception as exc:
        handle_cli_error(exc, console, "run delete")


def _print_run(operation, run_id: str, label: str) -> None:
    try:
        console.print_json(data=json.loads(operation(run_id)))
    except Exception as exc:
        handle_cli_error(exc, console, label)


def _read_json_object(path: str) -> dict:
    decoded = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise typer.BadParameter(f"{path} must contain a JSON object")
    return decoded
