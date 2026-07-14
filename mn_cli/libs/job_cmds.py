import json
import subprocess
import grpc
from pathlib import Path
from typing import Annotated

from rich.table import Table
from mn_cli.shared import console, client, config, logger
from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.blueprint_resources import cleanup_blueprint_host_hooks, cleanup_web_ui_process
from mn_cli.libs.blueprint_observability import load_observability_tools
from mn_cli.libs.ui import print_confirmed, print_success_confirmation

import typer

from mn_sdk.runtime_config import default_runs_root
from mn_sdk import RuntimeService, ValidationError, parse_duration_ms as sdk_parse_duration_ms

_ACTIVE_JOB_STATUSES = {"pending", "validated", "scheduled", "running", "paused"}
_ALL_JOBS_LIMIT = 2_147_483_647


def submit(
    manifest_path: Annotated[
        str,
        typer.Argument(help="Path to a workflow manifest JSON file."),
    ],
):
    """Submit a workflow manifest to the runtime.

    Examples:
      mn job submit ./manifest.json
      mn job submit ./examples/tax-review/manifest.json
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
            next_steps=f"mn job status {job_id}",
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
      mn job status job-123
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


def clear():
    """Remove all job records except running ones"""
    try:
        cleared_count = client.clear_jobs()
        logger.info("Cleared %d non-running jobs", cleared_count)
        print_success_confirmation(
            console,
            "Job clear",
            details={"Jobs cleared": f"{cleared_count} non-running"},
            next_steps="mn job list",
        )
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.PERMISSION_DENIED and "MN_GRPC_ADMIN_TOKEN" in str(e.details()):
            console.print("[red]Error: ClearJobs admin authorization failed.[/red]")
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
    except Exception as e:
        handle_cli_error(e, console, 'clear')


def cancel(
    job_id: Annotated[str, typer.Argument(help="Job ID to cancel.")],
):
    """Cancel a running job.

    Examples:
      mn job cancel job-123
    """
    try:
        status = client.cancel_job(job_id)
        _cleanup_cancelled_job_web_ui(job_id)
        print_success_confirmation(
            console,
            "Job cancel",
            status=status,
            details={"Job ID": job_id},
            next_steps=f"mn job status {job_id}",
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
    """Cancel all active jobs, stopping on the first failure.

    Active jobs include pending, validated, scheduled, running, and paused jobs.

    Examples:
      mn job cancel-all
      mn job cancel-all -y
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

        cancelled_count = 0
        for job in jobs:
            job_id = job["job_id"]
            try:
                client.cancel_job(job_id)
                _cleanup_cancelled_job_web_ui(job_id)
                cancelled_count += 1
            except Exception as error:
                _cleanup_cancelled_job_web_ui(job_id)
                not_attempted = len(jobs) - cancelled_count - 1
                console.print(f"[red]Cancellation stopped at job {job_id}.[/red]")
                console.print(
                    f"Cancelled {cancelled_count} of {len(jobs)} active jobs; "
                    f"{not_attempted} not attempted."
                )
                handle_cli_error(
                    error,
                    console,
                    "cancel_all",
                    command_context={"job_id": job_id},
                )

        logger.info("Cancelled %d active jobs", cancelled_count)
        print_success_confirmation(
            console,
            "Job cancel-all",
            status="cancelled",
            details={"Jobs cancelled": cancelled_count},
            next_steps="mn job list --running-only",
        )
    except typer.Exit:
        raise
    except Exception as error:
        handle_cli_error(error, console, "cancel_all")


def _cleanup_cancelled_job_web_ui(job_id: str) -> None:
    run_id = _blueprint_run_id_for_job(job_id)
    if not run_id:
        return

    run_dir = default_runs_root() / run_id
    if not run_dir.is_dir():
        return

    summary = {"process_removed": [], "process_skipped": [], "errors": []}
    cleanup_blueprint_host_hooks(run_dir, dry_run=False, summary=summary, reason="job_cancelled")
    cleanup_web_ui_process(run_dir, dry_run=False, summary=summary, reason="job_cancelled")
    _cleanup_local_openshell_sandboxes(job_id, summary)
    for error in summary["errors"]:
        logger.warning("Failed to cleanup web UI for cancelled job %s: %s", job_id, error)


def _blueprint_run_id_for_job(job_id: str) -> str | None:
    run_id = _blueprint_run_id_from_run_store(job_id)
    if run_id:
        return run_id

    snapshot_path = Path(f"/tmp/mn_{job_id}") / "job_snapshot.json"
    if snapshot_path.is_file():
        try:
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            run_id = snapshot.get("run_id")
            if isinstance(run_id, str) and run_id:
                return run_id
        except (OSError, json.JSONDecodeError):
            pass

    try:
        job = json.loads(client.get_job(job_id))
    except Exception:
        return None

    metadata = (((job.get("job") or {}).get("manifest") or {}).get("metadata") or {})
    mn_cli_metadata = metadata.get("mn_cli") if isinstance(metadata, dict) else {}
    run_id = mn_cli_metadata.get("blueprint_run_id") if isinstance(mn_cli_metadata, dict) else None
    return run_id if isinstance(run_id, str) and run_id else None


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
    runs_root = default_runs_root()
    if not runs_root.is_dir():
        return None
    for job_file in runs_root.glob("*/job.json"):
        try:
            payload = json.loads(job_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("job_id") == job_id:
            run_id = payload.get("run_id") or job_file.parent.name
            return run_id if isinstance(run_id, str) and run_id else None
    return None


def _cleanup_local_openshell_sandboxes(job_id: str, summary: dict[str, list[str]]) -> None:
    try:
        result = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "--filter",
                f"name=openshell-mirror-neuron-job-{job_id}",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception as error:
        summary["errors"].append(f"Failed to list OpenShell sandboxes for {job_id}: {error}")
        return

    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        summary["errors"].append(f"Failed to list OpenShell sandboxes for {job_id}: {detail}")
        return

    for name in [line.strip() for line in result.stdout.splitlines() if line.strip()]:
        remove = subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, timeout=20)
        if remove.returncode == 0:
            summary["process_removed"].append(name)
        else:
            detail = remove.stderr.strip() or remove.stdout.strip()
            summary["errors"].append(f"Failed to remove OpenShell sandbox {name}: {detail}")

def pause(job_id: str):
    """Pause a running job"""
    try:
        status = client.pause_job(job_id)
        print_success_confirmation(
            console,
            "Job pause",
            status=status,
            details={"Job ID": job_id},
            next_steps=f"mn job status {job_id}",
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
            next_steps=f"mn job status {job_id}",
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
            "Use [bold]mn job status <job_id>[/bold] to inspect and "
            "[bold]mn job resume <job_id>[/bold] to continue a paused run."
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
        console.print_json(data=summary)
    except Exception as e:
        handle_cli_error(e, console, 'nodes')


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
        result_json = client.reconcile_node(node_name, reason=reason, dry_run=dry_run)
        _print_node_mutation_confirmation(
            "Node reconcile",
            json.loads(result_json),
            node_name=node_name,
            details={"Dry run": dry_run},
        )
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
        result_json = client.drain_node(
            node_name,
            reason=reason,
            deadline_ms=deadline_ms,
            dry_run=dry_run,
            ignore_system_jobs=ignore_system_jobs,
            wait=wait,
        )
        result = json.loads(result_json)

        if wait and not dry_run:
            result = wait_for_drain(node_name, result)

        _print_node_mutation_confirmation(
            "Node drain",
            result,
            node_name=node_name,
            details={
                "Deadline": deadline,
                "Dry run": dry_run,
                "System jobs": "ignored" if ignore_system_jobs else "included",
            },
        )
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
    """Show runtime metrics derived from the core system summary"""
    try:
        summary = json.loads(client.get_system_summary())
        if "metrics" in summary:
            console.print_json(data=summary["metrics"])
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

        console.print_json(
            data={
                "jobs": {"total": len(jobs), "by_status": status_counts},
                "agents": {
                    "queue_depth_total": queue_depth_total,
                    "queue_depth_max": queue_depth_max,
                    "pressured": pressured_agents,
                },
                "nodes": {"total": len(summary.get("nodes", []))},
                "source": "system_summary",
            }
        )
    except Exception as e:
        handle_cli_error(e, console, "metrics")


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
