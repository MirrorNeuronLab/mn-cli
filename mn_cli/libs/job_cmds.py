import json
import grpc
from pathlib import Path
from typing import Annotated

from rich.table import Table
from mn_cli.shared import console, client, config, logger
from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.job_cleanup import (
    JobResourceCleanupError,
    blueprint_run_id_for_job,
    blueprint_run_id_from_run_store,
    cleanup_cancelled_job_resources,
    cleanup_cleared_job_resources,
)
from mn_cli.libs.blueprint_observability import load_observability_tools
from mn_cli.libs.operation_cmds import start_and_watch
from mn_cli.libs.ui import print_confirmed, print_error, print_success_confirmation

import typer

from mn_sdk.runtime_config import default_runs_root
from mn_sdk import (
    RuntimeService,
    ValidationError,
    parse_duration_ms as sdk_parse_duration_ms,
)

_ACTIVE_JOB_STATUSES = {"pending", "validated", "scheduled", "running", "paused", "cancelling"}
_ALL_JOBS_LIMIT = 2_147_483_647


def submit(
    manifest_path: Annotated[
        str,
        typer.Argument(help="Path to a workflow manifest JSON file."),
    ],
):
    """Submit a workflow manifest to the runtime.

    Examples:
      mn blueprint run <blueprint-id>
    """
    try:
        with open(manifest_path, "r") as f:
            manifest = f.read()

        result = RuntimeService(client).submit_job(
            manifest,
            {},
            bundle_dir=str(Path(manifest_path).expanduser().resolve().parent),
        )
        job_id = result["job_id"]
        logger.info("Submitted job id=%s from manifest=%s", job_id, manifest_path)
        print_success_confirmation(
            console,
            "Job submit",
            details=[("Job ID", job_id), ("Manifest", manifest_path)],
            next_steps=f"mn run show {job_id}",
        )
    except Exception as e:
        handle_cli_error(e, console, 'submit')


def status(
    job_id: Annotated[
        str,
        typer.Argument(help="Job ID returned by submit, run, or schedule output."),
    ],
):
    """Print the raw job status payload as JSON.

    Examples:
      mn run show run-123
    """
    try:
        job_json = client.get_job(job_id)
        job = json.loads(job_json)
        _attach_resource_usage(job_id, job)
        console.print_json(data=job)
    except Exception as e:
        handle_cli_error(e, console, 'status')


def list_jobs(running_only: bool = typer.Option(False, "--running-only", help="Only show active jobs.")):
    """List jobs in a readable table.

    Examples:
      mn job list
      mn job list --running-only
    """
    try:
        jobs_json = client.list_jobs()
        data = json.loads(jobs_json)

        table = recovery_table("Submitted At")
        for job in data.get("data", []):
            status = job.get("status", "N/A")
            if running_only and status not in _ACTIVE_JOB_STATUSES:
                continue

            table.add_row(
                job.get("job_id", "N/A"),
                job.get("graph_id", "N/A"),
                status,
                recovery_label(job),
                job.get("submitted_at", "N/A"),
            )
        console.print(table)
    except Exception as e:
        handle_cli_error(e, console, 'list_jobs')


def clear(
    yes: bool = typer.Option(False, "--yes", "-y", help="Clear terminal jobs without prompting."),
):
    """Remove terminal jobs and all runtime resources they own."""
    cleanup_errors: list[str] = []

    def cleanup_item(event: dict) -> None:
        job_id = str(event.get("item_id") or "")
        try:
            _cleanup_cleared_job_resources(job_id)
        except JobResourceCleanupError as error:
            cleanup_errors.append(str(error))
            event["status"] = "failed"
            event["error"] = f"local runtime cleanup incomplete: {error}"
            logger.error("Local cleanup failed for cleared job %s: %s", job_id, error)

    try:
        if not yes and not typer.confirm(
            "Clear all terminal jobs and their runtime resources?", default=False
        ):
            print_confirmed(console, "Job clear", status="aborted")
            return

        _cleanup_job_ids_or_raise(_terminal_job_ids())
        result = start_and_watch(
            "clear_jobs",
            {},
            action="Job clear",
            on_accepted_item=cleanup_item,
            runtime_client=client,
        )
        logger.info("Finished clear operation %s", result.get("operation_id"))
        if cleanup_errors:
            raise JobResourceCleanupError("; ".join(cleanup_errors))
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.PERMISSION_DENIED and "MN_GRPC_ADMIN_TOKEN" in str(e.details()):
            print_error(console, "ClearJobs admin authorization failed.")
            local_admin_token = str(
                getattr(client, "admin_token", None)
                or getattr(config, "grpc_admin_token", "")
                or ""
            ).strip()
            if local_admin_token:
                console.print(
                    "The running core rejected the fixed gRPC admin token. "
                    "Run mn runtime start to reconcile and recreate stale-token runtime containers."
                )
            else:
                console.print(
                    "The CLI did not load a gRPC admin token from runtime state. "
                    "Run mn runtime start to refresh ~/.mn/docker-compose.env and token files."
                )
            console.print("Retry after: mn runtime start; mn job clear")
            return
        handle_cli_error(e, console, 'clear')
    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, 'clear')


def cancel(
    job_id: Annotated[str, typer.Argument(help="Job ID to cancel.")],
):
    """Cancel a running job.

    Examples:
      mn run cancel run-123
    """
    try:
        status = client.cancel_job(job_id)
        _cleanup_cancelled_job_web_ui(job_id)
        print_success_confirmation(
            console,
            "Job cancel",
            status=status,
            details={"Job ID": job_id},
            next_steps=f"mn run show {job_id}",
        )
    except Exception as e:
        _cleanup_cancelled_job_web_ui(job_id)
        handle_cli_error(e, console, 'cancel')


def cancel_all(
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Cancel all active jobs without prompting."),
    ] = False,
):
    """Cancel all active jobs and report every cancellation result.

    Active jobs include pending, validated, scheduled, running, and paused jobs.

    Examples:
      Administrative bulk cancellation is exposed through the canonical REST API.
    """
    try:
        jobs_json = client.list_jobs(limit=_ALL_JOBS_LIMIT, include_terminal=False)
        data = json.loads(jobs_json)
        jobs = [
            job
            for job in data.get("data", [])
            if job.get("status") in _ACTIVE_JOB_STATUSES
            and isinstance(job.get("job_id"), str)
            and job["job_id"]
        ]

        if not jobs:
            print_confirmed(console, "Job cancel-all", status="no active jobs")
            return

        if not yes and not typer.confirm(f"Cancel all {len(jobs)} active jobs?", default=False):
            print_confirmed(
                console,
                "Job cancel-all",
                status="aborted",
                details={"Active jobs": len(jobs)},
            )
            return

        result = start_and_watch(
            "cancel_all_jobs",
            {},
            action="Job cancel-all",
            on_accepted_item=lambda event: _cleanup_cancelled_job_web_ui(str(event.get("item_id") or "")),
            runtime_client=client,
        )
        logger.info("Finished cancel-all operation %s", result.get("operation_id"))
    except typer.Exit:
        raise
    except Exception as error:
        handle_cli_error(error, console, "cancel_all")


def _cleanup_cancelled_job_web_ui(job_id: str) -> None:
    cleanup_cancelled_job_resources(job_id, runtime_client=client, log=logger)


def _cleanup_cleared_job_resources(job_id: str) -> None:
    cleanup_cleared_job_resources(job_id, runtime_client=client, log=logger)


def _cleanup_job_ids_or_raise(job_ids: list[str]) -> None:
    errors = []
    for job_id in job_ids:
        try:
            _cleanup_cleared_job_resources(job_id)
        except JobResourceCleanupError as error:
            errors.append(str(error))
    if errors:
        raise JobResourceCleanupError("; ".join(errors))


def _terminal_job_ids() -> list[str]:
    result = json.loads(
        client.list_jobs(limit=_ALL_JOBS_LIMIT, include_terminal=True)
    )
    jobs = result.get("data") if isinstance(result, dict) else None
    if not isinstance(jobs, list):
        raise ValueError("runtime returned an invalid job list")
    return [
        job_id
        for job in jobs
        if isinstance(job, dict)
        and job.get("status") in {"completed", "failed", "cancelled"}
        and isinstance((job_id := job.get("job_id")), str)
        and job_id
    ]


def _blueprint_run_id_for_job(job_id: str) -> str | None:
    return blueprint_run_id_for_job(job_id, runtime_client=client)


def _attach_resource_usage(job_id: str, job: dict[str, object]) -> None:
    if "resource_usage" in job:
        return
    run_id = _run_id_from_job_payload(job) or _blueprint_run_id_from_run_store(job_id)
    if not run_id:
        return
    try:
        read_run_resources = load_observability_tools()["read_run_resources"]
        resource_usage = read_run_resources(run_id, runs_root=default_runs_root())
    except Exception:
        return
    if isinstance(resource_usage, dict):
        job["resource_usage"] = resource_usage
        summary = job.get("summary")
        if isinstance(summary, dict):
            summary.setdefault("resource_usage", resource_usage)


def _run_id_from_job_payload(value: object) -> str | None:
    if isinstance(value, dict):
        for key in ("run_id", "runId"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate:
                return candidate
        for key in ("job", "summary", "metadata", "manifest", "payload"):
            candidate = _run_id_from_job_payload(value.get(key))
            if candidate:
                return candidate
    return None


def _blueprint_run_id_from_run_store(job_id: str) -> str | None:
    return blueprint_run_id_from_run_store(job_id)


def pause(job_id: str):
    """Pause a running job"""
    try:
        status = client.pause_job(job_id)
        print_success_confirmation(
            console,
            "Job pause",
            status=status,
            details={"Job ID": job_id},
            next_steps=f"mn run show {job_id}",
        )
    except Exception as e:
        handle_cli_error(e, console, 'pause')


def resume(job_id: str):
    """Resume a paused job"""
    try:
        status = client.resume_job(job_id)
        print_success_confirmation(
            console,
            "Job resume",
            status=status,
            details={"Job ID": job_id},
            next_steps=f"mn run show {job_id}",
        )
    except Exception as e:
        handle_cli_error(e, console, 'resume')


def unfinished():
    """List unfinished jobs that may need recovery or manual resume"""
    try:
        jobs_json = client.list_jobs(include_terminal=False)
        data = json.loads(jobs_json)
        jobs = data.get("data", [])

        if not jobs:
            print_confirmed(console, "Unfinished job check", status="none found")
            return

        table = recovery_table("Updated At", include_review=True)
        for job in jobs:
            table.add_row(
                job.get("job_id", "N/A"),
                job.get("graph_id", "N/A"),
                job.get("status", "N/A"),
                recovery_label(job),
                "yes" if recovery_requires_review(job) else "no",
                job.get("updated_at") or job.get("submitted_at", "N/A"),
            )

        console.print(table)
        for job in jobs:
            review = "yes" if recovery_requires_review(job) else "no"
            console.print(
                f"{job.get('job_id', 'N/A')} recovery={recovery_label(job)} review={review}"
            )
        console.print(
            "Use [bold]mn run show <run_id>[/bold] to inspect and "
            "[bold]mn run resume <run_id>[/bold] to continue a paused run."
        )
    except Exception as e:
        handle_cli_error(e, console, 'list_jobs')


def recovery_label(job: dict) -> str:
    recovery = job.get("recovery") or {}
    return (
        job.get("recovery_status")
        or recovery.get("status")
        or "normal"
    )


def recovery_requires_review(job: dict) -> bool:
    recovery = job.get("recovery") or {}
    return bool(job.get("recovery_requires_review") or recovery.get("requires_review"))


def recovery_table(time_column: str, include_review: bool = False) -> Table:
    table = Table()
    table.add_column("Job ID", overflow="fold", no_wrap=False)
    table.add_column("Workflow ID", overflow="fold", no_wrap=False)
    table.add_column("Status", overflow="fold", no_wrap=False)
    table.add_column("Recovery", overflow="fold", no_wrap=False)
    if include_review:
        table.add_column("Review", overflow="fold", no_wrap=False)
    table.add_column(time_column, overflow="fold", no_wrap=False)
    return table


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


def dead_letters(job_id: str):
    """List dead-letter events for a job"""
    try:
        letters = []
        for index, event_json in enumerate(client.stream_events(job_id)):
            event = json.loads(event_json)
            if event.get("type") == "dead_letter":
                letters.append(
                    {
                        "index": len(letters),
                        "event_index": index,
                        "agent_id": event.get("agent_id"),
                        "reason": event.get("reason") or event.get("error"),
                        "timestamp": event.get("timestamp"),
                        "message": event.get("message"),
                    }
                )
        console.print_json(data={"job_id": job_id, "data": letters})
    except Exception as e:
        handle_cli_error(e, console, "dead_letters")
