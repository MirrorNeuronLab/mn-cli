from __future__ import annotations

import json
import os
from typing import Any, Callable

import typer
from rich.live import Live
from rich.table import Table

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import print_error, print_info, print_success_confirmation, print_warning
from mn_cli.shared import client, console


_TERMINAL_OPERATION_STATUSES = {"completed", "completed_with_failures", "failed"}
_SUCCESS_ITEM_STATUSES = {
    "cancelled",
    "cleared",
    "completed",
    "recovered",
    "migrated",
    "skipped",
    "ignored",
    "dry_run",
}


def start_and_watch(
    kind: str,
    options: dict[str, Any],
    *,
    action: str,
    stop_on_deferred: bool = False,
    on_accepted_item: Callable[[dict[str, Any]], None] | None = None,
    runtime_client: Any | None = None,
) -> dict[str, Any]:
    selected_client = runtime_client or client
    operation = _json_object(selected_client.start_operation(kind, options))
    operation_id = _operation_id(operation)
    print_info(console, f"{action} started (operation {operation_id}).")

    try:
        return _watch(
            operation,
            action=action,
            stop_on_deferred=stop_on_deferred,
            on_accepted_item=on_accepted_item,
            runtime_client=selected_client,
        )
    except KeyboardInterrupt:
        print_info(console, f"Detached from operation {operation_id}; it continues in the cluster.")
        console.print(f"Operation ID: {operation_id}")
        return operation


def status(
    operation_id: str = typer.Argument(help="Durable operation ID."),
):
    """Show a durable group operation and its item states."""
    try:
        console.print_json(data=_json_object(client.get_operation(operation_id)))
    except Exception as error:
        handle_cli_error(error, console, "operation-status")


def watch(
    operation_id: str = typer.Argument(help="Durable operation ID."),
):
    """Reattach to a durable group operation's progress stream."""
    try:
        _watch(_json_object(client.get_operation(operation_id)), action="Operation")
    except KeyboardInterrupt:
        print_info(console, f"Detached from operation {operation_id}; it continues in the cluster.")
        console.print(f"Operation ID: {operation_id}")
    except Exception as error:
        handle_cli_error(error, console, "operation-watch")


def _watch(
    operation: dict[str, Any],
    *,
    action: str,
    stop_on_deferred: bool = False,
    on_accepted_item: Callable[[dict[str, Any]], None] | None = None,
    runtime_client: Any | None = None,
) -> dict[str, Any]:
    selected_client = runtime_client or client
    operation_id = _operation_id(operation)
    sequence = 0
    items: dict[str, dict[str, Any]] = {}

    if _plain_output():
        for event_json in selected_client.stream_operation_events(
            operation_id,
            after_sequence=sequence,
            follow=True,
            timeout=None,
            heartbeat_interval_ms=1_000,
        ):
            event = _json_object(event_json)
            sequence = max(sequence, _int(event.get("sequence")))
            _record_event(items, event)
            _print_plain_event(event, on_accepted_item)
            if stop_on_deferred and event.get("type") == "operation_deferred":
                break
    else:
        with Live(_operation_table(operation, items), console=console, refresh_per_second=8) as live:
            for event_json in selected_client.stream_operation_events(
                operation_id,
                after_sequence=sequence,
                follow=True,
                timeout=None,
                heartbeat_interval_ms=1_000,
            ):
                event = _json_object(event_json)
                sequence = max(sequence, _int(event.get("sequence")))
                _record_event(items, event)
                live.update(_operation_table(operation, items))
                if on_accepted_item and event.get("status") in {"cancelled", "cancellation_pending"}:
                    on_accepted_item(event)
                if stop_on_deferred and event.get("type") == "operation_deferred":
                    break

    final_operation = _json_object(selected_client.get_operation(operation_id))
    _print_completion(action, final_operation)
    return final_operation


def _print_completion(action: str, operation: dict[str, Any]) -> None:
    operation_id = _operation_id(operation)
    status = str(operation.get("status") or "unknown")
    counters = operation.get("counters") if isinstance(operation.get("counters"), dict) else {}
    failed = _int(counters.get("failed"))
    deferred = _int(counters.get("deferred"))

    if failed:
        print_error(console, f"{action} completed with failures.")
        console.print(f"Operation ID: {operation_id}")
        raise typer.Exit(1)

    if status not in _TERMINAL_OPERATION_STATUSES:
        print_info(console, f"{action} is waiting for deferred work ({deferred} item(s)).")
        console.print(f"Operation ID: {operation_id}")
        return

    message = "completed"
    if deferred:
        message = "completed; queued cleanup continues on owner nodes"
    print_success_confirmation(
        console,
        action,
        status=message,
        details={"Operation ID": operation_id, "Completed": _int(counters.get("finished"))},
    )


def _record_event(items: dict[str, dict[str, Any]], event: dict[str, Any]) -> None:
    item_id = event.get("item_id")
    if isinstance(item_id, str) and item_id:
        # Reinsert updates so the rich "recent results" rows follow event
        # arrival order even when the same item first emitted `started`.
        items.pop(item_id, None)
        items[item_id] = event


def _print_plain_event(
    event: dict[str, Any],
    on_accepted_item: Callable[[dict[str, Any]], None] | None,
) -> None:
    event_type = event.get("type")
    if event_type == "stream_heartbeat":
        return

    item_id = str(event.get("item_id") or "operation")
    status = str(event.get("status") or "")

    if event_type == "item_started":
        print_info(console, f"{item_id}: started")
    elif status == "failed":
        print_warning(console, f"{item_id}: {event.get('error') or 'operation item failed'}")
    elif status == "cancellation_pending":
        print_info(console, f"{item_id}: cancellation accepted; cleanup queued on owner node")
        if on_accepted_item:
            on_accepted_item(event)
    elif event_type in {"item_completed", "item_deferred"}:
        prefix = "✓" if status in _SUCCESS_ITEM_STATUSES else "→"
        console.print(f"{prefix} {item_id}: {status or 'completed'}")
        if on_accepted_item and status == "cancelled":
            on_accepted_item(event)


def _operation_table(operation: dict[str, Any], items: dict[str, dict[str, Any]]) -> Table:
    counters = operation.get("counters") if isinstance(operation.get("counters"), dict) else {}
    completed_items = [
        event
        for event in items.values()
        if str(event.get("status") or "") not in {"", "running"}
    ]
    succeeded = sum(
        1
        for event in completed_items
        if str(event.get("status") or "") in _SUCCESS_ITEM_STATUSES
    )
    deferred = sum(
        1
        for event in completed_items
        if str(event.get("status") or "") == "cancellation_pending"
    )
    failed = sum(
        1
        for event in completed_items
        if str(event.get("status") or "") == "failed"
    )
    finished = len(completed_items)
    total = _int(counters.get("total")) or _int(operation.get("target_count"))

    # The operation snapshot is deliberately immutable while the stream is
    # active. Derive the live counts from replayable item events so the rich
    # display reflects out-of-order completions immediately.
    if not completed_items:
        finished = _int(counters.get("finished"))
        succeeded = _int(counters.get("succeeded"))
        deferred = _int(counters.get("deferred"))
        failed = _int(counters.get("failed"))

    table = Table(title=f"Operation {_operation_id(operation)}", expand=False)
    table.add_column("Progress", no_wrap=True)
    table.add_column("Result", overflow="fold")
    table.add_row(
        f"{finished}/{total}",
        f"succeeded {succeeded}, deferred {deferred}, failed {failed}",
    )
    for item_id, event in list(items.items())[-5:]:
        table.add_row(item_id, str(event.get("status") or event.get("type") or "updated"))
    return table


def _operation_id(operation: dict[str, Any]) -> str:
    operation_id = operation.get("operation_id")
    if not isinstance(operation_id, str) or not operation_id:
        raise ValueError("runtime did not return an operation_id")
    return operation_id


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("runtime returned an invalid operation payload")
    return parsed


def _int(value: Any) -> int:
    return value if isinstance(value, int) else 0


def _plain_output() -> bool:
    # NO_COLOR keeps rich layouts without ANSI color; only the explicit plain
    # mode is a line-oriented contract for scripts.
    return os.getenv("MN_CLI_OUTPUT", "").lower() == "plain"
