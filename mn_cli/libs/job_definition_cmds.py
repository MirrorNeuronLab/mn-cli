from __future__ import annotations

import json
from pathlib import Path

import typer
from mn_sdk import prepare_job_submission
from mn_sdk.blueprint_support import make_run_id
from mn_sdk.submission_preparation import prepare_manifest_for_submission

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.bundles import read_bundle
from mn_cli.libs.job_cleanup import JobResourceCleanupError, cleanup_job_resources
from mn_cli.libs.run_cmds.common import _stage_bundle_payloads
from mn_cli.libs.ui import (
    activity,
    print_collection,
    print_detail,
    print_success_confirmation,
    require_confirmation,
)
from mn_cli.output import record_result
from mn_cli.shared import client, console, logger


def create(
    bundle: str = typer.Argument(help="Blueprint/job bundle directory or archive."),
    job_id: str | None = typer.Option(None, "--job-id", help="Optional durable job ID."),
    config: str | None = typer.Option(
        None, "--config", help="Resolved configuration JSON file."
    ),
    node: str | None = typer.Option(None, "--node", help="Core that will own and execute this job."),
):
    """Create a durable job definition without starting a run."""
    try:
        owner_node = node.strip() if isinstance(node, str) else ""
        manifest_json, payloads = read_bundle(bundle)
        resolved = _read_json_object(config) if config else {}
        bundle_path = Path(bundle).expanduser().resolve()
        prepared_manifest = prepare_manifest_for_submission(
            bundle_path,
            json.loads(manifest_json),
            config_overrides=resolved,
        )
        prepared_payloads = (
            _stage_bundle_payloads(bundle_path, prepared_manifest)
            if bundle_path.is_dir()
            else payloads
        )
        prepared = prepare_job_submission(
            prepared_manifest,
            prepared_payloads,
            bundle_dir=str(bundle_path) if bundle_path.is_dir() else None,
            job_id=job_id,
            cluster_client=client,
        )
        result = json.loads(
            client.create_job(
                prepared.manifest_json,
                prepared.payloads,
                job_id=job_id or "",
                resolved_configuration=resolved,
                owner_node=owner_node,
            )
        )
        print_success_confirmation(
            console,
            "Job create",
            details=[("Job ID", result.get("job_id")), ("Bundle", bundle)],
            next_steps=f"mn job start {result.get('job_id')}",
        )
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "job create")


def definitions(
    include_archived: bool = typer.Option(False, "--include-archived")
):
    """List durable job definitions."""
    try:
        payload = json.loads(client.list_jobs(include_archived=include_archived))
        items = payload.get("data") or payload.get("jobs") or payload.get("items") or [] if isinstance(payload, dict) else []
        print_collection(
            console,
            "Jobs",
            items,
            columns=(("ID", "job_id"), ("Kind", "kind"), ("State", "status"), ("Owner", "owner"), ("Updated", "updated_at")),
        )
    except Exception as exc:
        handle_cli_error(exc, console, "job definitions")


def inspect(job_id: str = typer.Argument(help="Durable job ID.")):
    """Inspect a durable job definition."""
    try:
        print_detail(console, "Job", json.loads(client.get_job(job_id)))
    except Exception as exc:
        handle_cli_error(exc, console, "job show")


def archive(job_id: str = typer.Argument(help="Durable job ID.")):
    """Archive a job while retaining its persistent data."""
    try:
        result = json.loads(client.archive_job(job_id))
        print_success_confirmation(console, "Job archive", status=result.get("status"), details={"Job ID": job_id})
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "job archive")


def reset_data(
    job_id: str = typer.Argument(help="Durable job ID."),
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the reset without changing data."),
):
    """Clear job data and advance its data generation."""
    yes = yes is True
    dry_run = dry_run is True
    if dry_run:
        print_success_confirmation(console, "Job data reset dry run", status="planned", details={"Job ID": job_id})
        return
    _confirm_destructive(f"Reset all persistent data for {job_id}?", yes=yes, action="Job data reset")
    try:
        result = json.loads(client.reset_job_data(job_id))
        print_success_confirmation(console, "Job data reset", status=result.get("status"), details={"Job ID": job_id})
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "job reset-data")


def delete(
    job_id: str = typer.Argument(help="Durable job ID."),
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview deletion without changing state."),
):
    """Permanently delete a durable job, its runs, resources, and data."""
    yes = yes is True
    dry_run = dry_run is True
    if dry_run:
        print_success_confirmation(console, "Job delete dry run", status="planned", details={"Job ID": job_id})
        return
    _confirm_destructive(
        f"Permanently delete {job_id}, all runs, runtime resources, and shared job data?",
        yes=yes,
        action="Job deletion",
    )
    try:
        run_ids = _job_run_ids(job_id)
        cleanup_errors = []
        for resource_id in [*run_ids, job_id]:
            try:
                cleanup_job_resources(
                    resource_id, runtime_client=client, log=logger
                )
            except JobResourceCleanupError as error:
                cleanup_errors.append(str(error))
        if cleanup_errors:
            raise JobResourceCleanupError("; ".join(cleanup_errors))

        result = json.loads(client.delete_job(job_id, confirmed=True))
        cleanup_errors = [
            str(error) for error in result.get("resource_cleanup_errors") or []
        ]
        if cleanup_errors:
            raise JobResourceCleanupError("; ".join(cleanup_errors))
        print_success_confirmation(console, "Job delete", status=result.get("status"), details={"Job ID": job_id})
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "job delete")


def start(
    job_id: str = typer.Argument(help="Durable job ID."),
    run_id: str | None = typer.Option(None, "--run-id"),
    inputs: str | None = typer.Option(None, "--inputs", help="Run-input JSON file."),
    force: bool = typer.Option(
        False,
        "--force",
        help="Permanently replace the existing run of a service job.",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Confirm replacement without prompting."),
):
    """Start a new run of a durable job."""
    force = force is True
    yes = yes is True
    if force:
        _confirm_destructive(
            (
                f"Replace the existing run for {job_id}? Active work will be cancelled "
                "and old run history and artifacts will be permanently removed."
            ),
            yes=yes,
            action="Service run replacement",
        )
        run_id = run_id or make_run_id(job_id)

    try:
        result = json.loads(
            client.start_run(
                job_id,
                run_id=run_id or "",
                inputs=_read_json_object(inputs) if inputs else {},
                replace_existing_run=force,
            )
        )
        details = [
            ("Job ID", job_id),
            ("Run ID", result.get("run_id")),
        ]
        if result.get("replaced_run_ids"):
            details.append(
                ("Replaced", ", ".join(result["replaced_run_ids"]))
            )
        if result.get("cleanup_deferred"):
            pending = ", ".join(result.get("cleanup_pending_nodes") or [])
            details.append(("Cleanup", f"deferred{f' on {pending}' if pending else ''}"))
        print_success_confirmation(
            console,
            "Run replace" if force else "Run start",
            details=details,
            next_steps=f"mn run show {result.get('run_id')}",
        )
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "job start")


def runs(job_id: str = typer.Argument(help="Durable job ID.")):
    """List the independent execution runs belonging to a job."""
    try:
        console.print_json(data=json.loads(client.list_runs(job_id)))
    except Exception as exc:
        handle_cli_error(exc, console, "run list")


def run_status(run_id: str):
    """Inspect one execution run."""
    _print_run(client.get_run, run_id, "run show")


def run_pause(run_id: str):
    """Pause one execution run."""
    _print_run(
        client.pause_run,
        run_id,
        "run pause",
        activity_message=f"Pausing run {run_id}…",
    )


def run_resume(run_id: str):
    """Resume one execution run."""
    _print_run(
        client.resume_run,
        run_id,
        "run resume",
        activity_message=f"Resuming run {run_id}…",
    )


def run_cancel(run_id: str):
    """Cancel one execution run without deleting job data."""
    _print_run(
        client.cancel_run,
        run_id,
        "run cancel",
        activity_message=f"Cancelling run {run_id}…",
    )


def run_delete(
    run_id: str,
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview deletion without changing state."),
):
    """Delete one terminal run without deleting job data."""
    yes = yes is True
    dry_run = dry_run is True
    if dry_run:
        print_success_confirmation(console, "Run delete dry run", status="planned", details={"Run ID": run_id})
        return
    _confirm_destructive(f"Delete run {run_id}?", yes=yes, action="Run deletion")
    try:
        result = json.loads(client.delete_run(run_id, confirmed=True))
        print_success_confirmation(console, "Run delete", status=result.get("status"), details={"Run ID": run_id})
        record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, "run delete")


def _print_run(
    operation,
    run_id: str,
    label: str,
    *,
    activity_message: str | None = None,
) -> None:
    try:
        if activity_message:
            with activity(console, activity_message):
                result = json.loads(operation(run_id))
        else:
            result = json.loads(operation(run_id))
        if label == "run show":
            print_detail(console, "Run", result)
        else:
            print_success_confirmation(console, label.replace("run ", "Run "), status=result.get("status"), details={"Run ID": run_id})
            record_result(result)
    except Exception as exc:
        handle_cli_error(exc, console, label)


def _read_json_object(path: str) -> dict:
    decoded = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise typer.BadParameter(f"{path} must contain a JSON object")
    return decoded


def _job_run_ids(job_id: str) -> list[str]:
    try:
        result = json.loads(client.list_runs(job_id))
    except Exception:
        logger.warning(
            "Could not list historical runs before deleting durable job %s",
            job_id,
            exc_info=True,
        )
        return []

    data = (
        result.get("data") or result.get("runs") or result.get("items")
        if isinstance(result, dict)
        else None
    )
    if not isinstance(data, list):
        return []

    return [
        run_id
        for run in data
        if isinstance(run, dict)
        and isinstance((run_id := run.get("run_id")), str)
        and run_id
    ]


def _confirm_destructive(prompt: str, *, yes: bool, action: str) -> None:
    require_confirmation(console, action=action, prompt=prompt, yes=yes)
