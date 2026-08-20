from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from mn_sdk.runtime_config import resolve_mn_home

from mn_cli.libs import blueprint_cmds, job_definition_cmds, run_cmds
from mn_cli.libs.ui import print_collection
from mn_cli.output import emit_stream_record, json_enabled, record_result
from mn_cli.shared import client, console, logger


def list_runs(
    job: str | None = typer.Option(None, "--job", help="Only show runs belonging to this durable job."),
    blueprint: str | None = typer.Option(None, "--blueprint", help="Only show runs for this blueprint ID."),
    follow: bool = typer.Option(False, "--follow", "-f", help="Refresh until interrupted."),
    limit: int = typer.Option(20, "--limit", min=1, help="Maximum runs to show."),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
    interval: float = typer.Option(2.0, "--interval", min=0.1, help="Refresh interval when following."),
) -> None:
    """List execution runs, optionally filtered by job or blueprint."""
    if job:
        payload = json.loads(client.list_runs(job))
        items = (
            payload.get("data") or payload.get("runs") or payload.get("items") or []
            if isinstance(payload, dict)
            else []
        )
        if not isinstance(items, list):
            items = []
        print_collection(
            console,
            "Runs",
            items,
            columns=(("ID", "run_id"), ("Kind", "kind"), ("State", "status"), ("Job", "job_id"), ("Updated", "updated_at")),
        )
        return
    if follow and json_enabled():
        list_from_store, _, _ = blueprint_cmds._load_observability_api()
        try:
            while True:
                items = list_from_store(runs_root=runs_root, blueprint_id=blueprint, limit=limit)
                emit_stream_record("snapshot", data={"items": items, "count": len(items)})
                import time

                time.sleep(max(interval, 0.1))
        except KeyboardInterrupt:
            record_result({"detached": True})
        return
    if follow:
        blueprint_cmds.blueprint_monitor(
            follow=True,
            blueprint_id=blueprint,
            max_runs=limit,
            runs_root=runs_root,
            interval=interval,
        )
        return
    list_from_store, _, _ = blueprint_cmds._load_observability_api()
    local_items = list_from_store(
        runs_root=runs_root, blueprint_id=blueprint, limit=limit
    )
    items = _merge_run_items(
        local_items,
        _runtime_run_items(blueprint_id=blueprint, limit=limit),
        limit=limit,
    )
    print_collection(
        console,
        "Runs",
        items,
        columns=(("ID", "run_id"), ("Kind", "blueprint_id"), ("State", "status"), ("Node / Owner", "job_id"), ("Updated", "updated_at")),
    )


def _runtime_run_items(*, blueprint_id: str | None, limit: int) -> list[dict]:
    """Read all visible durable-job runs when Core is reachable.

    The local run store receives the submission mapping before terminal
    artifacts arrive, so it cannot provide an authoritative lifecycle state
    for an active run.  Core owns that state.
    """

    try:
        jobs_payload = json.loads(client.list_jobs(page_size=max(limit, 50)))
    except Exception:
        logger.debug("Unable to list runtime jobs for run listing", exc_info=True)
        return []

    jobs = (
        jobs_payload.get("data")
        or jobs_payload.get("jobs")
        or jobs_payload.get("items")
        or []
        if isinstance(jobs_payload, dict)
        else []
    )
    if not isinstance(jobs, list):
        return []

    items: list[dict] = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_id = str(job.get("job_id") or "").strip()
        job_blueprint_id = str(job.get("blueprint_id") or "").strip()
        if not job_id or (blueprint_id and job_blueprint_id != blueprint_id):
            continue
        try:
            runs_payload = json.loads(
                client.list_runs(job_id, page_size=max(limit, 50))
            )
        except Exception:
            logger.debug("Unable to list runs for durable job %s", job_id, exc_info=True)
            continue
        runs = (
            runs_payload.get("data")
            or runs_payload.get("runs")
            or runs_payload.get("items")
            or []
            if isinstance(runs_payload, dict)
            else []
        )
        if not isinstance(runs, list):
            continue
        for run in runs:
            if not isinstance(run, dict):
                continue
            item = dict(run)
            item.setdefault("job_id", job_id)
            if job_blueprint_id:
                item.setdefault("blueprint_id", job_blueprint_id)
            items.append(item)
    return items


def _merge_run_items(
    local_items: list[dict], runtime_items: list[dict], *, limit: int
) -> list[dict]:
    """Combine local metadata with Core's authoritative lifecycle records."""

    merged: dict[str, dict] = {}
    for item in local_items:
        if not isinstance(item, dict):
            continue
        run_id = str(item.get("run_id") or "").strip()
        if run_id:
            merged[run_id] = dict(item)
    for item in runtime_items:
        run_id = str(item.get("run_id") or "").strip()
        if not run_id:
            continue
        combined = dict(merged.get(run_id) or {})
        combined.update(item)
        merged[run_id] = combined

    return sorted(
        merged.values(),
        key=lambda item: item.get("updated_at")
        or item.get("ended_at")
        or item.get("started_at")
        or item.get("submitted_at")
        or "",
        reverse=True,
    )[:limit]


def show_run(run_id: str = typer.Argument(help="Execution run ID.")) -> None:
    """Show one execution run."""
    job_definition_cmds.run_status(run_id)


def watch_run(run_id: str = typer.Argument(help="Execution run ID.")) -> None:
    """Watch live workflow progress for one execution run."""
    run_record = json.loads(client.get_run(run_id))
    runtime_job_id = _runtime_job_id(run_id, run_record=run_record)
    if not json_enabled():
        run_cmds.monitor(
            runtime_job_id,
            run_id=run_id,
            stable_job_id=str(run_record.get("job_id") or ""),
        )
        return
    try:
        snapshot = json.loads(client.get_run(run_id))
        emit_stream_record("snapshot", data=snapshot)
        for event_json in client.stream_events(
            runtime_job_id,
            follow=True,
            timeout=None,
            heartbeat_interval_ms=1_000,
        ):
            event = json.loads(event_json)
            if event.get("type") not in {"heartbeat", "stream_heartbeat"}:
                emit_stream_record("event", data=event)
        record_result({"run_id": run_id, "runtime_job_id": runtime_job_id, "detached": False})
    except KeyboardInterrupt:
        record_result({"run_id": run_id, "runtime_job_id": runtime_job_id, "detached": True})


def logs(
    run_id: str = typer.Argument(help="Execution run ID."),
    channel: str = typer.Option("logs", "--channel", help="Stream channel: logs, events, or all."),
    lines: int = typer.Option(50, "--lines", "-n", min=1, help="Number of records to show."),
    level: str | None = typer.Option(None, "--level", help="Minimum log level."),
    follow: bool = typer.Option(False, "--follow", "-f", help="Continue printing records."),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
    interval: float = typer.Option(1.0, "--interval", min=0.1, help="Polling interval when following."),
) -> None:
    """Show stored run logs, events, or the merged operational stream."""
    normalized = channel.strip().lower()
    if normalized not in {"logs", "events", "all"}:
        raise typer.BadParameter("--channel must be logs, events, or all")
    if json_enabled():
        _json_logs(
            run_id,
            channel=normalized,
            lines=lines,
            level=level,
            follow=follow,
            runs_root=runs_root,
            interval=interval,
        )
        return
    if normalized == "logs":
        blueprint_cmds.blueprint_logs(run_id, lines=lines, level=level, follow=follow, runs_root=runs_root, interval=interval)
        return
    if normalized == "events":
        blueprint_cmds.blueprint_tail(run_id, lines=lines, follow=follow, runs_root=runs_root, interval=interval)
        return
    if normalized == "all":
        blueprint_cmds.blueprint_stream(
            run_id,
            channels="events,logs,human,resources",
            lines=lines,
            level=level,
            follow=follow,
            runs_root=runs_root,
            interval=interval,
        )
        return


def _json_logs(
    run_id: str,
    *,
    channel: str,
    lines: int,
    level: str | None,
    follow: bool,
    runs_root: str | None,
    interval: float,
) -> None:
    blueprint_cmds._load_run_or_exit(run_id, runs_root)
    tools = blueprint_cmds._load_observability_tools()
    seen: set[str] = set()
    collected: list[dict] = []
    try:
        while True:
            if channel == "logs":
                records = tools["read_run_logs"](run_id, runs_root=runs_root, level=level, limit=lines)
            elif channel == "events":
                _, _, read_run_events = blueprint_cmds._load_observability_api()
                records = read_run_events(run_id, runs_root=runs_root, limit=lines)
            else:
                records = tools["read_run_stream_records"](
                    run_id,
                    runs_root=runs_root,
                    channels=["events", "logs", "human", "resources"],
                    level=level,
                    limit=lines,
                )
            selected = []
            for record in records:
                cursor = blueprint_cmds._observability_cursor(record)
                if cursor in seen:
                    continue
                seen.add(cursor)
                selected.append(record)
            if follow:
                for record in selected:
                    emit_stream_record("event", data=record)
            else:
                collected.extend(selected)
                record_result({"run_id": run_id, "channel": channel, "items": collected, "count": len(collected)})
                return
            import time

            time.sleep(max(interval, 0.1))
    except KeyboardInterrupt:
        record_result({"run_id": run_id, "channel": channel, "detached": True})


def result(
    run_id: str = typer.Argument(help="Execution run ID."),
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Directory for fetched result files."),
    ] = None,
) -> None:
    """Fetch final and progressive results for one execution run."""
    run_record = json.loads(client.get_run(run_id))
    runtime_job_id = _runtime_job_id(run_id, run_record=run_record)
    destination = output or (resolve_mn_home() / "outputs" / run_id)
    destination.mkdir(parents=True, exist_ok=True)
    run_cmds.fetch_and_save_results(
        runtime_job_id,
        data=run_record,
        output_dir=destination,
    )
    record_result({"run_id": run_id, "runtime_job_id": runtime_job_id, "output": str(destination)})
    if not json_enabled():
        console.print(f"Results saved to {destination}")


def resources(*args, **kwargs) -> None:
    """Show CPU, GPU, memory, and LLM usage for one execution run."""
    blueprint_cmds.blueprint_resources(*args, **kwargs)


def compare(*args, **kwargs) -> None:
    """Compare two execution runs."""
    blueprint_cmds.blueprint_compare(*args, **kwargs)


def human_list(
    run_id: str = typer.Argument(help="Execution run ID."),
    pending: bool = typer.Option(False, "--pending", help="Show only pending input requests."),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
) -> None:
    """List human collaboration events for one run."""
    blueprint_cmds._load_run_or_exit(run_id, runs_root)
    tools = blueprint_cmds._load_observability_tools()
    events = (
        tools["list_pending_human_requests"](run_id, runs_root=runs_root)
        if pending
        else tools["read_human_events"](run_id, runs_root=runs_root)
    )
    records = []
    for event in events:
        if not isinstance(event, dict):
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        records.append(
            {
                **event,
                "id": event.get("id") or payload.get("request_id") or payload.get("notice_id"),
                "kind": event.get("type") or event.get("channel") or "human",
                "state": event.get("state") or payload.get("status") or ("pending" if pending else "recorded"),
                "owner": payload.get("reviewer") or event.get("component") or "",
                "updated_at": event.get("ts") or event.get("timestamp") or "",
            }
        )
    print_collection(
        console,
        "Human events",
        records,
        columns=(("ID", "id"), ("Kind", "kind"), ("State", "state"), ("Owner", "owner"), ("Updated", "updated_at")),
    )


def human_respond(*args, **kwargs) -> None:
    """Respond to a human input request."""
    blueprint_cmds.blueprint_human_respond(*args, **kwargs)


def human_ack(*args, **kwargs) -> None:
    """Acknowledge a human notice."""
    blueprint_cmds.blueprint_human_ack(*args, **kwargs)


def _runtime_job_id(run_id: str, *, run_record: dict | None = None) -> str:
    payload = run_record if isinstance(run_record, dict) else json.loads(client.get_run(run_id))
    for key in ("runtime_job_id", "runtime_run_id", "execution_id"):
        value = payload.get(key) if isinstance(payload, dict) else None
        if value:
            return str(value)

    # v1 executes a durable job under its run ID. Its ``job_id`` is the durable
    # definition and has no coordinator or workflow ledger to monitor.
    resolved_run_id = payload.get("run_id") if isinstance(payload, dict) else None
    return str(resolved_run_id or run_id)
