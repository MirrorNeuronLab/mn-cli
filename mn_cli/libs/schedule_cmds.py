from __future__ import annotations

import json
import time
from typing import Any, Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.deployment_cmds import read_bundle
from mn_cli.libs.ui import print_success_confirmation
from mn_cli.shared import client, console


schedule_app = typer.Typer(help="Periodic, delayed, and event-triggered job schedules")
trigger_app = typer.Typer(help="Event trigger schedules")
event_app = typer.Typer(help="Runtime trigger events")


def _json_option(value: str, flag: str) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{flag} must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise typer.BadParameter(f"{flag} must be a JSON object")
    return parsed


def _duration_ms(value: str) -> int:
    raw = str(value or "").strip().lower()
    if not raw:
        raise typer.BadParameter("duration cannot be empty")
    units = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
    for suffix, multiplier in units.items():
        if raw.endswith(suffix):
            number = raw[: -len(suffix)]
            return int(float(number) * multiplier)
    return int(float(raw) * 1000)


def _local_timezone() -> str:
    return time.tzname[0] or "UTC"


def _print_result(
    result_json: str,
    *,
    action: str | None = None,
    details: dict[str, Any] | list[tuple[str, Any]] | None = None,
    next_steps: str | None = None,
) -> None:
    payload = json.loads(result_json)
    if action is None:
        console.print_json(data=payload)
        return
    detail_items: list[tuple[str, Any]] = []
    if details:
        detail_items.extend(details.items() if isinstance(details, dict) else details)
    detail_items.extend(
        [
            ("Schedule ID", payload.get("schedule_id") or payload.get("id")),
            ("Event ID", payload.get("event_id")),
            ("Job ID", payload.get("job_id")),
        ]
    )
    print_success_confirmation(
        console,
        action,
        status=payload.get("status"),
        details=detail_items,
        next_steps=next_steps,
    )


@schedule_app.command(name="create")
def create_schedule(
    bundle: str,
    cron: list[str] = typer.Option([], "--cron", help="Five-field cron expression. Repeat for multiple schedules."),
    name: str = typer.Option("", "--name", help="Schedule name."),
    timezone_name: str = typer.Option("", "--timezone", help="IANA timezone label stored on the schedule."),
    missed_policy: str = typer.Option("skip", "--missed-policy", help="skip, catchup_one, or catchup_all."),
    catchup_limit: int = typer.Option(10, "--catchup-limit", help="Maximum catch-up runs when catchup_all is used."),
    allow_overlap: bool = typer.Option(False, "--allow-overlap", help="Allow overlapping child jobs."),
    window: str = typer.Option("", "--window", help="Optional run window, e.g. 30m. Window end cancels the child job."),
    schedule_json: str = typer.Option("", "--schedule-json", help="Raw schedule JSON merged with CLI flags."),
):
    """Create a periodic schedule for a bundle."""
    try:
        manifest_json, payloads = read_bundle(bundle)
        schedule = _json_option(schedule_json, "--schedule-json")
        schedule.update(
            {
                "kind": "periodic",
                "crons": cron or schedule.get("crons") or ([schedule["cron"]] if schedule.get("cron") else []),
                "timezone": timezone_name or schedule.get("timezone") or _local_timezone(),
                "missed_policy": missed_policy,
                "catchup_limit": catchup_limit,
                "prohibit_overlap": not allow_overlap,
            }
        )
        if name:
            schedule["name"] = name
        if window:
            schedule["window"] = {"duration_ms": _duration_ms(window), "end_action": "cancel"}
        _print_result(
            client.create_schedule(manifest_json, payloads, schedule=schedule, source={"cli": "schedule create"}),
            action="Schedule create",
            details=[("Bundle", bundle), ("Kind", "periodic"), ("Name", name)],
            next_steps="mn schedule list",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "schedule create")


@schedule_app.command(name="delay")
def delay_schedule(
    bundle: str,
    at: Optional[str] = typer.Option(None, "--at", help="ISO-8601 timestamp to run once."),
    in_: Optional[str] = typer.Option(None, "--in", help="Delay before running once, e.g. 10m."),
    name: str = typer.Option("", "--name", help="Schedule name."),
):
    """Create a one-shot delayed schedule."""
    try:
        if not at and not in_:
            raise typer.BadParameter("provide --at or --in")
        manifest_json, payloads = read_bundle(bundle)
        schedule: dict[str, Any] = {"kind": "delayed", "timezone": _local_timezone()}
        if at:
            schedule["run_at"] = at
        if in_:
            schedule["delay_ms"] = _duration_ms(in_)
        if name:
            schedule["name"] = name
        _print_result(
            client.create_schedule(manifest_json, payloads, schedule=schedule, source={"cli": "schedule delay"}),
            action="Schedule delay",
            details=[("Bundle", bundle), ("Kind", "delayed"), ("Name", name)],
            next_steps="mn schedule list",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "schedule delay")


@schedule_app.command(name="list")
def list_schedules(kind: Optional[str] = typer.Option(None, "--kind"), status: Optional[str] = typer.Option(None, "--status")):
    """List schedules."""
    try:
        _print_result(client.list_schedules(kind=kind, status=status))
    except Exception as exc:
        handle_cli_error(exc, console, "schedule list")


@schedule_app.command(name="status")
def schedule_status(schedule_id: str):
    """Show one schedule."""
    try:
        _print_result(client.get_schedule(schedule_id))
    except Exception as exc:
        handle_cli_error(exc, console, "schedule status")


@schedule_app.command(name="pause")
def pause_schedule(schedule_id: str, reason: str = typer.Option("", "--reason")):
    """Pause a schedule."""
    try:
        _print_result(
            client.pause_schedule(schedule_id, reason=reason),
            action="Schedule pause",
            details={"Schedule": schedule_id},
            next_steps=f"mn schedule status {schedule_id}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "schedule pause")


@schedule_app.command(name="resume")
def resume_schedule(schedule_id: str, reason: str = typer.Option("", "--reason")):
    """Resume a schedule."""
    try:
        _print_result(
            client.resume_schedule(schedule_id, reason=reason),
            action="Schedule resume",
            details={"Schedule": schedule_id},
            next_steps=f"mn schedule status {schedule_id}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "schedule resume")


@schedule_app.command(name="delete")
def delete_schedule(schedule_id: str, reason: str = typer.Option("", "--reason")):
    """Delete a schedule."""
    try:
        _print_result(
            client.delete_schedule(schedule_id, reason=reason),
            action="Schedule delete",
            details={"Schedule": schedule_id},
            next_steps="mn schedule list",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "schedule delete")


@schedule_app.command(name="run-now")
def run_now(schedule_id: str, payload_json: str = typer.Option("", "--payload-json")):
    """Dispatch a schedule immediately."""
    try:
        _print_result(
            client.dispatch_schedule(schedule_id, payload=_json_option(payload_json, "--payload-json"), reason="manual"),
            action="Schedule run now",
            details={"Schedule": schedule_id},
            next_steps="mn job list --running-only",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "schedule run-now")


@trigger_app.command(name="create")
def create_trigger(
    bundle: str,
    event_type: str = typer.Option(..., "--event", help="Event type to match."),
    name: str = typer.Option("", "--name", help="Trigger name."),
    filter_json: str = typer.Option("", "--filter-json", help="Declarative event payload filters."),
    allow_overlap: bool = typer.Option(False, "--allow-overlap", help="Allow overlapping child jobs."),
):
    """Create an event-triggered schedule."""
    try:
        manifest_json, payloads = read_bundle(bundle)
        schedule = {
            "kind": "event",
            "name": name,
            "trigger": {
                "event_type": event_type,
                "filters": _json_option(filter_json, "--filter-json"),
            },
            "prohibit_overlap": not allow_overlap,
        }
        _print_result(
            client.create_schedule(manifest_json, payloads, schedule=schedule, source={"cli": "trigger create"}),
            action="Trigger create",
            details=[("Bundle", bundle), ("Event", event_type), ("Name", name)],
            next_steps="mn trigger list",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "trigger create")


@trigger_app.command(name="list")
def list_triggers():
    """List event schedules."""
    try:
        _print_result(client.list_schedules(kind="event"))
    except Exception as exc:
        handle_cli_error(exc, console, "trigger list")


@trigger_app.command(name="delete")
def delete_trigger(schedule_id: str, reason: str = typer.Option("", "--reason")):
    """Delete an event trigger."""
    delete_schedule(schedule_id, reason=reason)


@event_app.command(name="emit")
def emit_event(
    event_type: str,
    payload_json: str = typer.Option("", "--payload-json", help="Event payload JSON."),
    source: str = typer.Option("cli", "--source", help="Event source label."),
):
    """Emit a runtime event that can trigger schedules."""
    try:
        _print_result(
            client.emit_trigger_event(event_type, payload=_json_option(payload_json, "--payload-json"), source=source),
            action="Event emit",
            details=[("Event", event_type), ("Source", source)],
            next_steps="mn event list",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "event emit")


@event_app.command(name="list")
def list_events(limit: int = typer.Option(100, "--limit")):
    """List recent trigger events."""
    try:
        _print_result(client.list_trigger_events(limit=limit))
    except Exception as exc:
        handle_cli_error(exc, console, "event list")
