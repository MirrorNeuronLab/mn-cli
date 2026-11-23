"""Command declarations for `mn job`."""

from pathlib import Path

import typer
from mn_sdk.runtime_contract.mailbox import Kind

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def job_list(include_archived: bool = typer.Option(False, "--include-archived")):
    "List durable job definitions."
    return display(("job", "list"), dispatch(("job", "list"), locals()))


def job_create(
    bundle: str = typer.Argument(
        ..., help="Blueprint/job bundle directory or archive."
    ),
    job_id: str | None = typer.Option(
        None, "--job-id", help="Optional durable job ID."
    ),
    config: str | None = typer.Option(
        None, "--config", help="Resolved configuration JSON file."
    ),
    node: str | None = typer.Option(
        None, "--node", help="Core that will own and execute this job."
    ),
):
    "Create a durable job definition without starting a run."
    return display(("job", "create"), dispatch(("job", "create"), locals()))


def job_backup(
    job_id: str = typer.Argument(
        ..., help="Durable job ID. Pause active runs before backup."
    ),
    output: str = typer.Option(..., "--output", "-o", help="Destination ZIP file."),
    air_gapped: bool = typer.Option(
        True,
        "--air-gapped/--no-air-gapped",
        help="Include model files, Python wheels, and Docker images.",
    ),
):
    "Back up a job, its data, history, and offline dependencies to a ZIP."
    return display(("job", "backup"), dispatch(("job", "backup"), locals()))


def job_restore(
    input_path: str = typer.Option(..., "--input", "-i", help="Job backup ZIP file."),
    job_id: str | None = typer.Option(
        None, "--job-id", help="Optional new durable job ID."
    ),
    start: bool = typer.Option(
        False, "--start", help="Start a fresh run after restoring the job."
    ),
):
    "Restore a ZIP as a new job without hiring a blueprint."
    return display(("job", "restore"), dispatch(("job", "restore"), locals()))


def job_show(job_id: str = typer.Argument(..., help="Durable job ID.")):
    "Inspect a durable job definition."
    return display(("job", "show"), dispatch(("job", "show"), locals()))


def mailbox_list(
    job_id: str = typer.Argument(...),
    after: int = typer.Option(0, min=0),
    limit: int = typer.Option(25, min=1, max=50),
    include_acknowledged: bool = typer.Option(False, "--all"),
    kind: Kind | None = typer.Option(None, help="Filter by message kind."),
    reply_to: str | None = typer.Option(
        None, help="Filter results for this assignment ID."
    ),
):
    """Read queued messages for this co-worker."""
    return display(
        ("job", "mailbox", "list"), dispatch(("job", "mailbox", "list"), locals())
    )


def mailbox_send(
    job_id: str = typer.Argument(..., help="Sender's stable Job ID."),
    recipient: str = typer.Option(..., "--to"),
    subject: str = typer.Option(...),
    text: str = typer.Option("", "--text"),
    kind: Kind = typer.Option("note", help="Type of message to queue."),
    reply_to: str | None = typer.Option(
        None, help="Original assignment ID for a result."
    ),
    attachment: Path | None = typer.Option(
        None, help="JSON reference with run_id, sha256 and size_bytes."
    ),
    request_id: str | None = typer.Option(None, help="Reuse this ID on retry."),
):
    """Queue a note, manager assignment, or specialist result for a group member."""
    return display(
        ("job", "mailbox", "send"), dispatch(("job", "mailbox", "send"), locals())
    )


def mailbox_ack(
    job_id: str = typer.Argument(...), message_id: str = typer.Argument(...)
):
    """Acknowledge a message in this co-worker's inbox."""
    return display(
        ("job", "mailbox", "ack"), dispatch(("job", "mailbox", "ack"), locals())
    )


def job_relocate(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    node: str = typer.Option(..., "--node", help="Connected destination PC."),
    abort: bool = typer.Option(
        False, "--abort", help="Cancel a move before ownership has committed."
    ),
):
    "Move an idle job and its data to another PC. Pause its schedules first."
    return display(("job", "relocate"), dispatch(("job", "relocate"), locals()))


def job_analysis(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    json_output: bool = typer.Option(
        False, "--json", help="Print the shared analysis as JSON."
    ),
):
    "Show performance statistics for all recorded job history."
    return display(("job", "analysis"), dispatch(("job", "analysis"), locals()))


def job_start(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    run_id: str | None = typer.Option(None, "--run-id"),
    queue: bool = typer.Option(
        False, "--queue", help="Queue this run when resources are busy."
    ),
    inputs: str | None = typer.Option(None, "--inputs", help="Run-input JSON file."),
    force: bool = typer.Option(
        False, "--force", help="Permanently replace the existing run of a service job."
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Confirm replacement without prompting."
    ),
):
    "Start a new run of a durable job."
    return display(("job", "start"), dispatch(("job", "start"), locals()))


def job_archive(job_id: str = typer.Argument(..., help="Durable job ID.")):
    "Archive a job while retaining its persistent data."
    return display(("job", "archive"), dispatch(("job", "archive"), locals()))


def job_reset_data(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview the reset without changing data."
    ),
):
    "Clear job data and advance its data generation."
    return display(("job", "reset-data"), dispatch(("job", "reset-data"), locals()))


def job_delete(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview deletion without changing state."
    ),
):
    "Permanently delete a durable job, its runs, resources, and data."
    return display(("job", "delete"), dispatch(("job", "delete"), locals()))


def register(app: typer.Typer) -> None:
    job_app = typer.Typer(
        help="Create and manage durable job definitions.\n\nExamples:\n  mn job create ./worker-bundle --job-id worker-daily\n  mn job list\n  mn job show worker-daily\n  mn job start worker-daily\n  mn run list --job worker-daily\n",
        cls=RemediatingTyperGroup,
    )
    job_app.command("list", hidden=False)(job_list)
    job_app.command("create", hidden=False)(job_create)
    job_app.command("backup", hidden=False)(job_backup)
    job_app.command("restore", hidden=False)(job_restore)
    job_app.command("show", hidden=False)(job_show)
    job_app.command("relocate", hidden=False)(job_relocate)
    job_app.command("analysis", hidden=False)(job_analysis)
    job_app.command("start", hidden=False)(job_start)
    job_app.command("archive", hidden=False)(job_archive)
    job_app.command("reset-data", hidden=False)(job_reset_data)
    job_app.command("delete", hidden=False)(job_delete)
    from .job_integrations import register as register_integrations
    register_integrations(job_app)
    mailbox_app = typer.Typer(
        help="Exchange durable messages with configured co-workers."
    )
    mailbox_app.command("list")(mailbox_list)
    mailbox_app.command("send")(mailbox_send)
    mailbox_app.command("ack")(mailbox_ack)
    job_app.add_typer(mailbox_app, name="mailbox")
    app.add_typer(job_app, name="job")
