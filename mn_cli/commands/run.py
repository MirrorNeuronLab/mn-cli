"""Command declarations for `mn run`."""

from pathlib import Path
from typing import Annotated

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def run_list(
    job: str | None = typer.Option(
        None, "--job", help="Only show runs belonging to this durable job."
    ),
    blueprint: str | None = typer.Option(
        None, "--blueprint", help="Only show runs for this blueprint ID."
    ),
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Refresh until interrupted."
    ),
    limit: int = typer.Option(20, "--limit", help="Maximum runs to show.", min=1),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
    interval: float = typer.Option(
        2.0, "--interval", help="Refresh interval when following.", min=0.1
    ),
):
    "List execution runs, optionally filtered by job or blueprint."
    return display(("run", "list"), dispatch(("run", "list"), locals()))


def run_show(run_id: str = typer.Argument(..., help="Execution run ID.")):
    "Show one execution run."
    return display(("run", "show"), dispatch(("run", "show"), locals()))


def run_info(run_id: str = typer.Argument(..., help="Execution run ID.")):
    "Show one execution run."
    return display(("run", "info"), dispatch(("run", "info"), locals()))


def run_watch(run_id: str = typer.Argument(..., help="Execution run ID.")):
    "Watch live workflow progress for one execution run."
    return display(("run", "watch"), dispatch(("run", "watch"), locals()))


def run_logs(
    run_id: str = typer.Argument(..., help="Execution run ID."),
    channel: str = typer.Option(
        "logs", "--channel", help="Stream channel: logs, events, or all."
    ),
    lines: int = typer.Option(
        50, "--lines", "-n", help="Number of records to show.", min=1
    ),
    level: str | None = typer.Option(None, "--level", help="Minimum log level."),
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Continue printing records."
    ),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
    interval: float = typer.Option(
        1.0, "--interval", help="Polling interval when following.", min=0.1
    ),
):
    "Show stored run logs, events, or the merged operational stream."
    return display(("run", "logs"), dispatch(("run", "logs"), locals()))


def run_result(
    run_id: str = typer.Argument(..., help="Execution run ID."),
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Directory for fetched result files."),
    ] = None,
):
    "Fetch final and progressive results for one execution run."
    return display(("run", "result"), dispatch(("run", "result"), locals()))


def run_resources(
    run_id: str,
    window: str = typer.Option(
        "24h", "--window", help="History window, for example 24h."
    ),
    bucket: str = typer.Option(
        "1h", "--bucket", help="Aggregation bucket, for example 1h."
    ),
    live: bool = typer.Option(
        False, "--live", help="Refresh resource usage until interrupted."
    ),
    interval: float = typer.Option(
        5.0, "--interval", help="Live refresh interval in seconds."
    ),
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Show CPU, GPU, memory, and LLM token usage for one blueprint run."
    return display(("run", "resources"), dispatch(("run", "resources"), locals()))


def run_compare(
    run_a: str,
    run_b: str,
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Compare two blueprint runs from the shared run store."
    return display(("run", "compare"), dispatch(("run", "compare"), locals()))


def run_pause(run_id: str):
    "Pause one execution run."
    return display(("run", "pause"), dispatch(("run", "pause"), locals()))


def run_resume(run_id: str):
    "Resume one execution run."
    return display(("run", "resume"), dispatch(("run", "resume"), locals()))


def run_retry(
    run_id: str = typer.Argument(..., help="Failed execution run ID."),
    queue: bool = typer.Option(
        False, "--queue", help="Queue this retry when resources are busy."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Verify recovery without starting work."
    ),
    settings: list[str] = typer.Option(
        [], "--set", help="Explicit declared retry setting: path=value. Repeatable."
    ),
    idempotency_key: str | None = typer.Option(
        None, "--idempotency-key", help="Reuse this key after a lost response."
    ),
    expected_attempt: int | None = typer.Option(
        None,
        "--expected-attempt",
        help="Original attempt selection when resubmitting a lost response.",
        min=1,
    ),
    checkpoint_revision: str | None = typer.Option(
        None,
        "--checkpoint-revision",
        help="Original checkpoint selection when resubmitting a lost response.",
    ),
):
    "Retry unfinished work from the failed run's last verified checkpoint."
    return display(("run", "retry"), dispatch(("run", "retry"), locals()))


def run_cancel(
    run_id: str,
    queued_only: bool = typer.Option(
        False, "--queued-only", help="Cancel only if execution has not started."
    ),
):
    "Cancel one execution run without deleting job data."
    return display(("run", "cancel"), dispatch(("run", "cancel"), locals()))


def run_delete(
    run_id: str,
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview deletion without changing state."
    ),
):
    "Delete one run, cancelling it first when active, without deleting job data."
    return display(("run", "delete"), dispatch(("run", "delete"), locals()))


def run_human_list(
    run_id: str = typer.Argument(..., help="Execution run ID."),
    pending: bool = typer.Option(
        False, "--pending", help="Show only pending input requests."
    ),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
):
    "List human collaboration events for one run."
    return display(
        ("run", "human", "list"), dispatch(("run", "human", "list"), locals())
    )


def run_human_respond(
    run_id: str,
    request_id: str,
    decision: str = typer.Option(
        ..., "--decision", help="Decision value, such as approve, revise, or reject."
    ),
    notes: str = typer.Option("", "--notes", help="Optional reviewer notes."),
    reviewer: str = typer.Option("cli", "--reviewer", help="Reviewer identity label."),
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Record a response to a human input request."
    return display(
        ("run", "human", "respond"), dispatch(("run", "human", "respond"), locals())
    )


def run_human_ack(
    run_id: str,
    notice_id: str,
    reviewer: str = typer.Option("cli", "--reviewer", help="Reviewer identity label."),
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Acknowledge a human notice."
    return display(("run", "human", "ack"), dispatch(("run", "human", "ack"), locals()))


def register(app: typer.Typer) -> None:
    run_app = typer.Typer(
        help="Inspect and control individual execution runs.", cls=RemediatingTyperGroup
    )
    run_app.command("list", hidden=False)(run_list)
    run_app.command("show", hidden=False)(run_show)
    run_app.command("info", hidden=False)(run_info)
    run_app.command("watch", hidden=False)(run_watch)
    run_app.command("logs", hidden=False)(run_logs)
    run_app.command("result", hidden=False)(run_result)
    run_app.command("resources", hidden=False)(run_resources)
    run_app.command("compare", hidden=False)(run_compare)
    run_app.command("pause", hidden=False)(run_pause)
    run_app.command("resume", hidden=False)(run_resume)
    run_app.command("retry", hidden=False)(run_retry)
    run_app.command("cancel", hidden=False)(run_cancel)
    run_app.command("delete", hidden=False)(run_delete)
    run_human_app = typer.Typer(
        help="Inspect and respond to human collaboration events.",
        cls=RemediatingTyperGroup,
    )
    run_human_app.command("list", hidden=False)(run_human_list)
    run_human_app.command("respond", hidden=False)(run_human_respond)
    run_human_app.command("ack", hidden=False)(run_human_ack)
    run_app.add_typer(run_human_app, name="human")
    app.add_typer(run_app, name="run")
