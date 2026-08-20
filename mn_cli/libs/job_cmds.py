import json

import typer
from mn_sdk import (
    ValidationError,
)
from mn_sdk import (
    parse_duration_ms as sdk_parse_duration_ms,
)

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.operation_cmds import start_and_watch
from mn_cli.libs.ui import print_success_confirmation
from mn_cli.shared import client, console

_ACTIVE_JOB_STATUSES = {"pending", "validated", "scheduled", "running", "paused", "cancelling"}
_ALL_JOBS_LIMIT = 2_147_483_647






















def nodes():
    """Get system summary and nodes"""
    try:
        summary_json = client.get_system_summary()
        summary = json.loads(summary_json)
        summary = _strip_node_list_restart_history(summary)
        from mn_cli.libs.ui import print_collection
        from mn_cli.output import record_result

        items = summary.get("nodes") if isinstance(summary, dict) else []
        print_collection(
            console,
            "Nodes",
            items if isinstance(items, list) else [],
            columns=(("ID", "name"), ("Kind", "kind"), ("State", "status"), ("Node / Owner", "owner"), ("Updated", "updated_at")),
        )
        record_result(summary)
    except Exception as e:
        handle_cli_error(e, console, 'nodes')


def show_node(node_name: str = typer.Argument(help="Runtime node name.")):
    """Show one runtime node from the current system summary."""
    try:
        summary = json.loads(client.get_system_summary())
        items = summary.get("nodes") if isinstance(summary, dict) else []
        items = items if isinstance(items, list) else []
        node = next(
            (
                item
                for item in items
                if isinstance(item, dict)
                and str(item.get("name") or item.get("node") or item.get("id")) == node_name
            ),
            None,
        )
        if node is None:
            from mn_cli.libs.ui import print_error

            print_error(console, f"Node {node_name!r} was not found.", code="MN_NOT_FOUND")
            raise typer.Exit(2)
        from mn_cli.libs.ui import print_detail

        print_detail(console, "Node", node)
    except typer.Exit:
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "node show")


def _strip_node_list_restart_history(value):
    if isinstance(value, list):
        return [_strip_node_list_restart_history(item) for item in value]
    if not isinstance(value, dict):
        return value

    cleaned = {}
    for key, item in value.items():
        if _node_list_restart_history_key(key):
            continue
        cleaned[key] = _strip_node_list_restart_history(item)
    return cleaned


def _node_list_restart_history_key(key: object) -> bool:
    normalized = "".join(char for char in str(key).lower() if char.isalnum())
    if not normalized:
        return False
    if normalized in {
        "restarthistory",
        "restartreason",
        "restartexhaustedreason",
        "exhaustedreason",
    }:
        return True
    return "restart" in normalized and ("history" in normalized or "reason" in normalized)


def reconcile_node(
    node_name: str,
    reason: str = typer.Option("", "--reason", help="Reason recorded on reconciliation events."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Plan reconciliation without moving work."),
):
    """Reconcile jobs affected by an unavailable node"""
    try:
        start_and_watch(
            "reconcile_node",
            {"node_name": node_name, "reason": reason, "dry_run": dry_run},
            action="Node reconcile",
            runtime_client=client,
        )
    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, 'reconcile-node')


def drain_node(
    node_name: str,
    reason: str = typer.Option("", "--reason", help="Reason recorded on drain events."),
    deadline: str = typer.Option("30m", "--deadline", help="Drain deadline, e.g. 30m, 10s, 1h."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Plan the drain without changing node state."),
    wait: bool = typer.Option(False, "--wait", help="Poll drain status until it completes or blocks."),
    ignore_system_jobs: bool = typer.Option(
        True,
        "--ignore-system-jobs/--include-system-jobs",
        help="Ignore system/sysbatch jobs while draining.",
    ),
):
    """Drain a node and move safe workloads elsewhere"""
    try:
        deadline_ms = parse_duration_ms(deadline)
        start_and_watch(
            "drain_node",
            {
                "node_name": node_name,
                "reason": reason,
                "deadline_ms": deadline_ms,
                "dry_run": dry_run,
                "ignore_system_jobs": ignore_system_jobs,
            },
            action="Node drain",
            stop_on_deferred=not wait and not dry_run,
            runtime_client=client,
        )
    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, 'drain-node')


def undrain_node(
    node_name: str,
    reason: str = typer.Option("", "--reason", help="Reason recorded on undrain events."),
    mark_eligible: bool = typer.Option(
        False,
        "--mark-eligible",
        help="Make the node schedulable after cancelling/completing drain.",
    ),
):
    """Cancel node drain and optionally make the node schedulable"""
    try:
        result_json = client.cancel_node_drain(
            node_name,
            reason=reason,
            mark_eligible=mark_eligible,
        )
        _print_node_mutation_confirmation(
            "Node undrain",
            json.loads(result_json),
            node_name=node_name,
            details={"Mark eligible": mark_eligible},
        )
    except Exception as e:
        handle_cli_error(e, console, 'undrain-node')


def maintenance_node(
    node_name: str,
    enable: bool = typer.Option(
        True,
        "--enable/--disable",
        help="Enable or disable maintenance mode.",
    ),
    reason: str = typer.Option("", "--reason", help="Reason recorded on maintenance events."),
):
    """Toggle node maintenance mode without moving existing work"""
    try:
        result_json = client.set_node_maintenance(node_name, enable, reason=reason)
        _print_node_mutation_confirmation(
            "Node maintenance",
            json.loads(result_json),
            node_name=node_name,
            details={"Mode": "enabled" if enable else "disabled"},
        )
    except Exception as e:
        handle_cli_error(e, console, 'maintenance-node')


def parse_duration_ms(value: str) -> int:
    try:
        return sdk_parse_duration_ms(value, field_name="deadline")
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc


def wait_for_drain(node_name: str, first_result: dict) -> dict:
    import time

    result = first_result
    terminal = {"complete", "blocked_no_placement", "paused_for_review", "dry_run"}

    for _ in range(120):
        if result.get("status") in terminal:
            return result

        time.sleep(1)
        status_json = client.get_node_drain_status(node_name)
        status = json.loads(status_json)
        drain = status.get("drain") or {}
        result = {
            "node": node_name,
            "status": drain.get("status", status.get("status", "unknown")),
            "scheduling_eligible": status.get("scheduling_eligible"),
            "drain": drain,
        }

    return result


def _print_node_mutation_confirmation(
    action: str,
    payload: dict,
    *,
    node_name: str,
    details: dict | None = None,
) -> None:
    detail_items: list[tuple[str, object]] = [("Node", payload.get("node") or node_name)]
    detail_items.extend(
        [
            ("Reason", payload.get("reason")),
            ("Scheduling eligible", payload.get("scheduling_eligible")),
        ]
    )
    if details:
        detail_items.extend(details.items())
    print_success_confirmation(
        console,
        action,
        status=payload.get("status"),
        details=detail_items,
        next_steps="mn node list",
    )


def metrics():
    """Show runtime resource usage derived from the core system summary."""
    try:
        summary = json.loads(client.get_system_summary())
        if "metrics" in summary:
            from mn_cli.libs.ui import print_detail

            print_detail(console, "Resource usage", summary["metrics"])
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

        from mn_cli.libs.ui import print_detail

        print_detail(
            console,
            "Resource usage",
            {
                "jobs": {"total": len(jobs), "by_status": status_counts},
                "agents": {
                    "queue_depth_total": queue_depth_total,
                    "queue_depth_max": queue_depth_max,
                    "pressured": pressured_agents,
                },
                "nodes": {"total": len(summary.get("nodes", []))},
                "source": "system_summary",
            },
        )
    except Exception as e:
        handle_cli_error(e, console, "resource usage")


