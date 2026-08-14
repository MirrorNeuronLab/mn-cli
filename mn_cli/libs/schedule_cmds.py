from __future__ import annotations

import json
from typing import Any, Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.deployment_cmds import read_bundle
from mn_cli.libs.ui import print_collection, print_detail, print_success_confirmation, require_confirmation
from mn_cli.output import record_result
from mn_cli.shared import client, console
from mn_sdk import (
    ValidationError,
    delayed_schedule as sdk_delayed_schedule,
    event_schedule as sdk_event_schedule,
    parse_duration_ms as sdk_parse_duration_ms,
    periodic_schedule as sdk_periodic_schedule,
)


def add_schedule(
    bundle: str = typer.Argument(help="Blueprint/job bundle directory or archive."),
    cron: Optional[list[str]] = typer.Option(None, "--cron", help="Five-field cron expression; repeat for multiple schedules."),
    at: Optional[str] = typer.Option(None, "--at", help="ISO-8601 timestamp for a one-shot schedule."),
    in_: Optional[str] = typer.Option(None, "--in", help="Delay for a one-shot schedule, for example 30m."),
    event: Optional[str] = typer.Option(None, "--event", help="Event type for an event-driven schedule."),
    name: str = typer.Option("", "--name", help="Schedule name."),
    timezone_name: str = typer.Option("", "--timezone", help="IANA timezone for periodic schedules."),
    missed_policy: str = typer.Option("skip", "--missed-policy", help="skip, catchup_one, or catchup_all."),
    catchup_limit: int = typer.Option(10, "--catchup-limit", help="Maximum catch-up runs."),
    allow_overlap: bool = typer.Option(False, "--allow-overlap", help="Allow overlapping child runs."),
    window: str = typer.Option("", "--window", help="Optional run window, for example 30m."),
    filter_json: str = typer.Option("", "--filter", help="JSON event payload filters."),
) -> None:
    """Add one periodic, delayed, or event-driven schedule."""
    periodic = bool(cron)
    delayed = bool(at or in_)
    event_driven = bool(event)
    if sum((periodic, delayed, event_driven)) != 1:
        raise typer.BadParameter("choose exactly one schedule mode: --cron, --at/--in, or --event")
    if at and in_:
        raise typer.BadParameter("use only one of --at or --in")
    try:
        manifest_json, payloads = read_bundle(bundle)
        if periodic:
            schedule = sdk_periodic_schedule(
                crons=cron,
                name=name,
                timezone_name=timezone_name,
                missed_policy=missed_policy,
                catchup_limit=catchup_limit,
                allow_overlap=allow_overlap,
                window=window,
            )
            kind = "periodic"
        elif delayed:
            schedule = sdk_delayed_schedule(at=at, delay=in_, name=name)
            kind = "delayed"
        else:
            schedule = sdk_event_schedule(
                event_type=str(event),
                name=name,
                filters=_json_option(filter_json, "--filter"),
                allow_overlap=allow_overlap,
            )
            kind = "event"
        _print_result(
            client.create_schedule(
                manifest_json,
                payloads,
                schedule=schedule,
                source={"cli": "schedule add"},
            ),
            action="Schedule add",
            details=[("Bundle", bundle), ("Kind", kind), ("Name", name)],
            next_steps="mn schedule list",
        )
    except typer.BadParameter:
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "schedule add")


def show_schedule(schedule_id: str = typer.Argument(help="Schedule ID.")) -> None:
    """Show one schedule."""
    schedule_status(schedule_id)


def remove_schedule(
    schedule_id: str = typer.Argument(help="Schedule ID."),
    reason: str = typer.Option("", "--reason", help="Reason recorded with removal."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Confirm removal without prompting."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview removal without changing state."),
) -> None:
    """Remove one schedule."""
    yes = yes is True
    dry_run = dry_run is True
    if dry_run:
        print_success_confirmation(console, "Schedule remove dry run", status="planned", details={"Schedule ID": schedule_id})
        return
    require_confirmation(
        console,
        action="Schedule removal",
        prompt=f"Remove schedule {schedule_id!r}?",
        yes=yes,
    )
    delete_schedule(schedule_id, reason=reason)


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
    try:
        return sdk_parse_duration_ms(value, default_unit="s")
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _print_result(
    result_json: str,
    *,
    action: str | None = None,
    details: dict[str, Any] | list[tuple[str, Any]] | None = None,
    next_steps: str | None = None,
) -> None:
    payload = json.loads(result_json)
    if action is None:
        items = None
        collection_key = ""
        if isinstance(payload, dict):
            for key in ("schedules", "events", "data", "items"):
                if isinstance(payload.get(key), list):
                    items = payload[key]
                    collection_key = key
                    break
        if items is not None:
            is_events = collection_key == "events"
            print_collection(
                console,
                "Events" if is_events else "Schedules",
                items,
                columns=(
                    ("ID", "event_id" if is_events else "schedule_id"),
                    ("Kind", "event_type" if is_events else "kind"),
                    ("State", "status"),
                    ("Node / Owner", "source" if is_events else "owner"),
                    ("Updated", "timestamp" if is_events else "updated_at"),
                ),
            )
        else:
            print_detail(console, "Schedule", payload if isinstance(payload, dict) else {"value": payload})
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
    record_result(payload)


def list_schedules(kind: Optional[str] = typer.Option(None, "--kind"), status: Optional[str] = typer.Option(None, "--status")):
    """List schedules."""
    try:
        _print_result(client.list_schedules(kind=kind, status=status))
    except Exception as exc:
        handle_cli_error(exc, console, "schedule list")


def schedule_status(schedule_id: str):
    """Show one schedule."""
    try:
        _print_result(client.get_schedule(schedule_id))
    except Exception as exc:
        handle_cli_error(exc, console, "schedule show")


def pause_schedule(schedule_id: str, reason: str = typer.Option("", "--reason")):
    """Pause a schedule."""
    try:
        _print_result(
            client.pause_schedule(schedule_id, reason=reason),
            action="Schedule pause",
            details={"Schedule": schedule_id},
            next_steps=f"mn schedule show {schedule_id}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "schedule pause")


def resume_schedule(schedule_id: str, reason: str = typer.Option("", "--reason")):
    """Resume a schedule."""
    try:
        _print_result(
            client.resume_schedule(schedule_id, reason=reason),
            action="Schedule resume",
            details={"Schedule": schedule_id},
            next_steps=f"mn schedule show {schedule_id}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "schedule resume")


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


def run_now(schedule_id: str, payload_json: str = typer.Option("", "--payload-json")):
    """Dispatch a schedule immediately."""
    try:
        _print_result(
            client.dispatch_schedule(schedule_id, payload=_json_option(payload_json, "--payload-json"), reason="manual"),
            action="Schedule run now",
            details={"Schedule": schedule_id},
            next_steps="mn job list --running-only",
        )
    except typer.BadParameter:
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "schedule run")


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
    except typer.BadParameter:
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "event emit")


def list_events(limit: int = typer.Option(100, "--limit")):
    """List recent trigger events."""
    try:
        _print_result(client.list_trigger_events(limit=limit))
    except Exception as exc:
        handle_cli_error(exc, console, "event list")
