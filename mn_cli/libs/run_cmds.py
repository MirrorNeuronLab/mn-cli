import typer
import hashlib
import importlib.resources
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Annotated, Any, Optional
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.live import Live
from mn_cli.libs.ui import (
    JobMonitorState,
    generate_detached_panel,
    generate_live_layout,
    generate_run_submitted_panel,
    generate_summary_panel,
    print_confirmed,
    print_success_confirmation,
)
from mn_cli.libs.workflow_progress import BlueprintWorkflowProgress
from mn_cli.libs.run_logs import (
    JobLogWriter,
    STANDARD_EVENTS,
    extract_web_ui_url as _extract_web_ui_url,
    materialize_sent_email_copy as _materialize_sent_email_copy,
    write_result_stream_event as _write_result_stream_event,
)
from mn_cli.libs.artifacts import promote_large_payloads_to_blob_refs
from mn_cli.libs.run_manifest import (
    add_mn_llm_aliases as _add_mn_llm_aliases,
    blueprint_runtime_environment as _blueprint_runtime_environment,
    inject_node_environment as _inject_node_environment,
    load_blueprint_config,
    prepare_manifest_for_submission,
    runtime_web_ui_support_payloads_for_manifest,
    run_mode_label as _run_mode_label,
    stage_blueprint_support_payloads_for_manifest,
    stage_local_input_payloads_for_manifest,
    with_shared_run_store_config as _with_shared_run_store_config,
)
from mn_cli.libs.blueprint_observability import (
    make_blueprint_run_id as _make_blueprint_run_id,
)
from mn_cli.libs.blueprint_resources import cleanup_blueprint_host_hooks
from mn_cli.shared import console, client, logger
from mn_cli.terminal import use_progress
from mn_cli.error_handler import handle_cli_error
from mn_sdk import (
    make_validation_report,
    prepare_job_submission,
    run_input_validation,
    run_model_validation,
    run_service_validation,
    validate_input_validation_spec_issues,
    validate_requirements_spec_issues,
    validate_resource_spec_issues,
    validate_service_spec_issues,
    workflow_progress_snapshot,
)

FINAL_STATUSES = {"completed", "failed", "cancelled"}
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
PRE_LAUNCH_SCRIPT = Path("scripts/pre-launch.sh")
POST_LAUNCH_SCRIPT = Path("scripts/post-launch.sh")
DEFAULT_BLUEPRINT_WEB_UI_PORT_START = 61000
DEFAULT_BLUEPRINT_WEB_UI_PORT_END = 61049
DETACHED_AFTER_INTERRUPT_MESSAGE = "Detached from workflow UI. Job is still running."
_HELPER_COMPAT = (
    _add_mn_llm_aliases,
    _blueprint_runtime_environment,
    _extract_web_ui_url,
    _inject_node_environment,
    _materialize_sent_email_copy,
)


def fetch_and_save_results(job_id: str, data: dict = None):
    log_dir = Path(f"/tmp/mn_{job_id}")
    log_dir.mkdir(parents=True, exist_ok=True)

    if data is None:
        try:
            job_json = client.get_job(job_id)
            data = json.loads(job_json)
        except Exception:
            logger.exception("Failed to fetch job result for %s", job_id)
            return

    job = data.get("job", {})
    status = job.get("status")

    # Save final result if completed
    if status == "completed":
        result = job.get("result")
        if result:
            with open(log_dir / "result.txt", "w") as f:
                json.dump(result, f, indent=2)

    # Save stream results (progressive)
    stream_events = []

    try:
        full_events = []
        for ev_str in client.stream_events(job_id, follow=False):
            try:
                full_events.append(json.loads(ev_str))
            except Exception:
                logger.exception(
                    "Failed to decode event while saving results for %s", job_id
                )
                pass

        for ev in full_events:
            ev_type = ev.get("type")
            if ev_type not in STANDARD_EVENTS:
                stream_events.append(ev.get("payload", ev))
    except Exception:
        logger.exception("Failed to stream events while saving results for %s", job_id)
        pass

    if stream_events:
        with open(log_dir / "result_stream.txt", "w") as f:
            for se in stream_events:
                f.write(json.dumps(se) + "\n")


def _stream_and_format_events(
    job_id: str,
    log_writer: Optional[JobLogWriter] = None,
    follow_seconds: Optional[float] = None,
    web_ui_url: Optional[str] = None,
    manifest: Optional[dict[str, Any]] = None,
) -> str:
    if manifest is not None:
        return _stream_and_format_workflow_events(
            job_id,
            manifest,
            log_writer=log_writer,
            follow_seconds=follow_seconds,
            web_ui_url=web_ui_url,
        )
    log_writer = log_writer or JobLogWriter(job_id)
    if web_ui_url:
        log_writer.remember_web_ui_url(web_ui_url)
    log_dir = log_writer.log_dir
    follow_seconds = (
        float(os.getenv("MN_RUN_DETACH_LOG_SECONDS", "30"))
        if follow_seconds is None
        else follow_seconds
    )

    status_text = "Unknown / Detached"
    msg_count = 0

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
            disable=not use_progress(),
        ) as progress:
            job_task = progress.add_task("[cyan]Submitting job bundle...", total=None)

            for event_json in client.stream_events(job_id, follow=True, timeout=None, heartbeat_interval_ms=5000):
                try:
                    event = json.loads(event_json)
                    event_type = event.get("type")
                    if event_type == "stream_heartbeat":
                        continue
                    log_writer.write_event_json(event_json)

                    _write_result_stream_event(log_dir, event)
                    web_ui_url = log_writer.record_web_ui_url(event)
                    if web_ui_url:
                        progress.console.print(
                            f"[green]Blueprint Web UI:[/green] {web_ui_url}"
                        )

                    if event_type == "job_pending":
                        progress.update(
                            job_task,
                            description="[cyan]Preparing: job accepted, waiting for validation...",
                        )
                    elif event_type == "job_validated":
                        progress.update(
                            job_task,
                            description="[cyan]Preparing: manifest validated, scheduling agents...",
                        )
                    elif event_type == "job_scheduled":
                        progress.update(
                            job_task,
                            description="[cyan]Starting: agents scheduled, waiting for runtime to report running...",
                        )
                    elif event_type == "job_running":
                        progress.update(
                            job_task,
                            description="[green]Running: streaming live job events...",
                        )
                    elif event_type in [
                        "agent_message_received",
                        "aggregator_received",
                    ]:
                        msg_count += 1
                        progress.update(
                            job_task,
                            description=f"[green]Running: {msg_count} routed messages, {log_writer.event_count} events logged...",
                        )
                    elif event_type == "job_completed":
                        result = event.get("result")
                        if result is not None:
                            with open(log_dir / "result.txt", "w") as f_res:
                                json.dump(result, f_res, indent=2)

                        progress.update(
                            job_task,
                            description="[green]Completed successfully.",
                        )
                        status_text = "Success"
                        break
                    elif event_type == "job_failed":
                        progress.update(job_task, description="[red]Job failed.")
                        status_text = "Failed"
                        break
                    elif event_type == "job_cancelled":
                        progress.update(job_task, description="[red]Job cancelled.")
                        status_text = "Cancelled"
                        break
                    else:
                        progress.update(
                            job_task,
                            description=f"[cyan]Observing: latest event {event_type}, {log_writer.event_count} events logged...",
                        )
                except Exception:
                    log_writer.run_logger.exception("Failed to process streamed event")

        terminal_status = {
            "Success": "completed",
            "Failed": "failed",
            "Cancelled": "cancelled",
        }.get(status_text)
        if terminal_status:
            panel = generate_summary_panel(
                job_id=job_id,
                status=terminal_status,
                log_dir=log_dir,
            )
            console.print(panel)
        else:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
                disable=not use_progress(),
            ) as progress:
                follow_task = progress.add_task(
                    f"[cyan]Following job for {follow_seconds:g}s before detach...",
                    total=None,
                )
                status, _data = _follow_job_events(
                    job_id,
                    log_writer,
                    follow_seconds,
                    progress=progress,
                    task_id=follow_task,
                )
            console.print(
                generate_detached_panel(
                    job_id,
                    log_dir,
                    status,
                    log_writer.event_count,
                    web_ui_url=log_writer.web_ui_url,
                )
            )
            status_text = status

    except (KeyboardInterrupt, EOFError):
        console.print(f"[yellow]{DETACHED_AFTER_INTERRUPT_MESSAGE}[/yellow]")
        status, _data = _follow_job_events(job_id, log_writer, 0)
        console.print(
            generate_detached_panel(
                job_id,
                log_dir,
                status,
                log_writer.event_count,
                web_ui_url=log_writer.web_ui_url,
            )
        )
        status_text = status

    if status_text == "Success":
        return "completed"
    if status_text == "Failed":
        return "failed"
    if status_text == "Cancelled":
        return "cancelled"
    return str(status_text).lower()


def _stream_and_format_workflow_events(
    job_id: str,
    manifest: dict[str, Any],
    *,
    log_writer: Optional[JobLogWriter] = None,
    follow_seconds: Optional[float] = None,
    web_ui_url: Optional[str] = None,
) -> str:
    log_writer = log_writer or JobLogWriter(job_id)
    if web_ui_url:
        log_writer.remember_web_ui_url(web_ui_url)
    log_dir = log_writer.log_dir
    follow_seconds = (
        float(os.getenv("MN_RUN_DETACH_LOG_SECONDS", "30"))
        if follow_seconds is None
        else follow_seconds
    )
    view = BlueprintWorkflowProgress(manifest, job_id=job_id)
    status_text = "running"
    live: Live | None = None

    try:
        if _interactive_live_output():
            live = Live(
                view.render(),
                console=console,
                refresh_per_second=6,
                transient=True,
                screen=True,
            )
            live.start()
        try:
            for event_json in client.stream_events(job_id, follow=True, timeout=None, heartbeat_interval_ms=5000):
                try:
                    event = json.loads(event_json)
                    if event.get("type") == "stream_heartbeat":
                        continue
                    log_writer.write_event_json(event_json)
                    _write_result_stream_event(log_dir, event)
                    web_ui_url = log_writer.record_web_ui_url(event)
                    if web_ui_url:
                        view._remember(f"Blueprint Web UI: {web_ui_url}")
                    view.update(event)
                    if live is not None:
                        live.update(view.render())

                    event_type = event.get("type")
                    if event_type == "job_completed":
                        result = event.get("result")
                        if result is not None:
                            with open(log_dir / "result.txt", "w") as f_res:
                                json.dump(result, f_res, indent=2)
                        status_text = "completed"
                        break
                    if event_type == "job_failed":
                        status_text = "failed"
                        break
                    if event_type == "job_cancelled":
                        status_text = "cancelled"
                        break
                except Exception:
                    log_writer.run_logger.exception("Failed to process streamed event")

            if status_text not in FINAL_STATUSES:
                status_text = _follow_workflow_job_events(
                    job_id,
                    log_writer,
                    follow_seconds,
                    view,
                    live,
                )
        finally:
            if live is not None:
                live.stop()
    except (KeyboardInterrupt, EOFError):
        console.print(f"[yellow]{DETACHED_AFTER_INTERRUPT_MESSAGE}[/yellow]")
        status_text, _data = _follow_job_events(job_id, log_writer, 0)

    if live is None:
        console.print(view.render())

    if status_text in FINAL_STATUSES:
        console.print(
            generate_summary_panel(
                job_id=job_id,
                status=status_text,
                log_dir=log_dir,
            )
        )
    else:
        console.print(
            generate_detached_panel(
                job_id,
                log_dir,
                status_text,
                log_writer.event_count,
                web_ui_url=log_writer.web_ui_url,
            )
        )
    return status_text


def _follow_workflow_job_events(
    job_id: str,
    log_writer: JobLogWriter,
    follow_seconds: float,
    view: BlueprintWorkflowProgress,
    live: Live | None,
) -> str:
    started = time.monotonic()
    last_status, data = _follow_job_events(job_id, log_writer, follow_seconds)
    if isinstance(data, dict):
        for event in reversed(data.get("recent_events", [])):
            if isinstance(event, dict):
                view.update(event)
    remaining = max(follow_seconds - (time.monotonic() - started), 0)
    view.update_follow_status(last_status, log_writer.event_count, remaining)
    if live is not None:
        live.update(view.render())
    return last_status


def _interactive_live_output() -> bool:
    if os.getenv("MN_RUN_DISABLE_LIVE_SCREEN", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False
    return bool(getattr(console, "is_terminal", False) and sys.stdout.isatty())


def _follow_job_events(
    job_id: str,
    log_writer: JobLogWriter,
    follow_seconds: float,
    progress: Optional[Progress] = None,
    task_id=None,
):
    deadline = time.monotonic() + max(follow_seconds, 0)
    last_status = "unknown"
    data = None

    while True:
        try:
            data = json.loads(client.get_job(job_id))
            log_writer.write_snapshot(data)
        except Exception:
            log_writer.run_logger.exception("Failed to poll job status")
            break

        job = data.get("job", {})
        summary = data.get("summary", {})
        last_status = summary.get("status") or job.get("status") or last_status

        recent_events = data.get("recent_events", [])
        for event in reversed(recent_events):
            if log_writer.write_event(event):
                _write_result_stream_event(log_writer.log_dir, event)
                web_ui_url = log_writer.record_web_ui_url(event)
                if web_ui_url and progress is not None:
                    progress.console.print(
                        f"[green]Blueprint Web UI:[/green] {web_ui_url}"
                    )

        if progress is not None and task_id is not None:
            remaining = max(deadline - time.monotonic(), 0)
            progress.update(
                task_id,
                description=(
                    f"[cyan]Following: status {last_status}, "
                    f"{log_writer.event_count} events logged, detach in {remaining:0.1f}s..."
                ),
            )

        if last_status in FINAL_STATUSES:
            result = job.get("result")
            if result is not None:
                with open(log_writer.log_dir / "result.txt", "w") as f_res:
                    json.dump(result, f_res, indent=2, sort_keys=True)
            break

        if time.monotonic() >= deadline:
            break

        time.sleep(float(os.getenv("MN_RUN_LOG_POLL_INTERVAL_SECONDS", "0.5")))

    return last_status, data


def validate(
    bundle_path: Annotated[
        str,
        typer.Argument(help="Path to the local job bundle folder."),
    ],
    output: Annotated[
        str,
        typer.Option("--output", "-o", help="Output format: table or json."),
    ] = "table",
):
    """Validate a local job bundle before submitting it.

    Examples:
      mn blueprint validate ./bundle
      mn blueprint validate ./bundle --output json
    """
    try:
        output_format = _normalize_validation_output(output)
        bundle_dir = Path(bundle_path)
        if not bundle_dir.is_dir():
            console.print(
                f"[red]Error: '{bundle_path}' is not a directory. Expected a bundle folder.[/red]"
            )
            raise typer.Exit(1)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(
                f"[red]Error: manifest.json not found in '{bundle_path}'[/red]"
            )
            raise typer.Exit(1)

        with open(manifest_file, "r") as f:
            try:
                manifest = json.load(f)
            except json.JSONDecodeError as e:
                console.print(f"[red]Error: manifest.json is not valid JSON. {e}[/red]")
                raise typer.Exit(1)

        workflow_manifest = _is_workflow_manifest(manifest)
        if workflow_manifest:
            schema_issues = _validate_workflow_schema_issues(manifest)
            if schema_issues:
                report = make_validation_report(schema_issues)
                _emit_validation_report(
                    report, output_format, title="Workflow manifest schema validation failed"
                )
                raise typer.Exit(1)

            workflow_issues = _validate_workflow_manifest_issues(manifest)
            if workflow_issues:
                report = make_validation_report(workflow_issues)
                _emit_validation_report(
                    report, output_format, title="Workflow manifest validation failed"
                )
                raise typer.Exit(1)
        else:
            required_keys = ["manifest_version", "graph_id", "job_name", "entrypoints", "nodes"]
            missing = [k for k in required_keys if k not in manifest]
            if missing:
                console.print(
                    f"[red]Error: manifest.json is missing required keys: {', '.join(missing)}[/red]"
                )
                raise typer.Exit(1)
            if not isinstance(manifest.get("nodes"), type([])):
                console.print("[red]Error: 'nodes' must be a list in manifest.json[/red]")
                raise typer.Exit(1)

        if "requiredContextEngine" in manifest and not isinstance(
            manifest.get("requiredContextEngine"), bool
        ):
            console.print(
                "[red]Error: 'requiredContextEngine' must be true or false in manifest.json[/red]"
            )
            raise typer.Exit(1)

        python_environment_errors = validate_python_environments(bundle_dir, manifest)
        if python_environment_errors:
            report = make_validation_report(
                [
                    _legacy_validation_issue(error, source="manifest")
                    for error in python_environment_errors
                ]
            )
            _emit_validation_report(
                report, output_format, title="Manifest validation failed"
            )
            raise typer.Exit(1)

        manifest_spec_issues = (
            validate_service_spec_issues(manifest)
            + validate_requirements_spec_issues(manifest)
            + validate_resource_spec_issues(manifest)
            + validate_input_validation_spec_issues(manifest)
        )
        if manifest_spec_issues:
            report = make_validation_report(manifest_spec_issues)
            _emit_validation_report(
                report, output_format, title="Manifest validation failed"
            )
            raise typer.Exit(1)

        service_result = _validate_manifest_services_or_exit(
            bundle_dir, manifest, output_format=output_format
        )

        model_result = _validate_manifest_models_or_exit(
            bundle_dir, manifest, output_format=output_format
        )

        validation_result = _validate_manifest_inputs_or_exit(
            bundle_dir, manifest, output_format=output_format
        )

        if output_format == "json":
            console.print_json(data=validation_result)
            return

        details: list[tuple[str, Any]] = [
            ("Bundle", bundle_path),
            ("Job Name", manifest.get("job_name")),
            ("Workflow ID", _manifest_workflow_id(manifest) if workflow_manifest else manifest.get("graph_id")),
        ]
        if workflow_manifest:
            workflow = manifest.get("workflow", {}) if isinstance(manifest.get("workflow"), dict) else {}
            steps = workflow.get("steps") if isinstance(workflow.get("steps"), list) else []
            details.append(("Workflow steps", len(steps if isinstance(steps, list) else [])))
        else:
            details.append(("Nodes", len(manifest.get("nodes"))))
        details.append(("Service checks", len(service_result.get("results") or [])))
        details.append(("Model checks", len(model_result.get("results") or [])))
        capacity_summary = _model_capacity_summary(model_result)
        if capacity_summary:
            details.append(("Model capacity", capacity_summary))
        details.append(("Input validation rules", len(validation_result.get("results") or [])))
        print_confirmed(
            console,
            "Job bundle validation",
            status="valid",
            details=details,
        )

    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, "validate")
        raise typer.Exit(1)


def _is_workflow_manifest(manifest: dict[str, Any]) -> bool:
    return (
        manifest.get("apiVersion") == "mn.workflow/v1"
        or manifest.get("kind") == "Workflow"
        or isinstance(manifest.get("workflow"), dict)
    )


def _manifest_workflow_id(manifest: dict[str, Any]) -> str | None:
    workflow = manifest.get("workflow") if isinstance(manifest.get("workflow"), dict) else {}
    workflow_id = workflow.get("workflow_id") if isinstance(workflow, dict) else None
    return str(workflow_id) if isinstance(workflow_id, str) and workflow_id.strip() else None


def _workflow_schema_validator() -> Draft202012Validator:
    schema_path = importlib.resources.files("mn_cli").joinpath("schemas/workflow_manifest.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def _validate_workflow_schema_issues(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    deprecated_fields = [
        field
        for field in ("flow", "graph_id", "nodes", "edges", "entrypoints")
        if field in manifest
    ]
    if deprecated_fields:
        return [
            _workflow_validation_issue(
                field,
                f"{field} is not allowed in mn.workflow/v1 manifests",
                code="workflow_manifest.schema_failed",
            )
            for field in deprecated_fields
        ]
    validator = _workflow_schema_validator()
    return [_workflow_schema_issue(error) for error in sorted(validator.iter_errors(manifest), key=_schema_error_sort_key)]


def _schema_error_sort_key(error: ValidationError) -> tuple[str, str]:
    return (_schema_error_path(error), str(error.message))


def _schema_error_path(error: ValidationError) -> str:
    parts = list(error.path)
    schema_parts = list(error.absolute_schema_path)
    if not parts and len(schema_parts) >= 2 and schema_parts[-2] == "properties":
        return str(schema_parts[-1])
    if not parts:
        return "manifest"
    rendered: list[str] = []
    for part in parts:
        if isinstance(part, int) and rendered:
            rendered[-1] = f"{rendered[-1]}[{part}]"
        else:
            rendered.append(str(part))
    return ".".join(rendered)


def _workflow_schema_issue(error: ValidationError) -> dict[str, Any]:
    path = _schema_error_path(error)
    message = _workflow_schema_message(error, path)
    return _workflow_validation_issue(path, message, code="workflow_manifest.schema_failed")


def _workflow_schema_message(error: ValidationError, path: str) -> str:
    if path in {"flow", "graph_id", "nodes", "edges", "entrypoints"}:
        return f"{path} is not allowed in mn.workflow/v1 manifests"
    if error.validator == "required":
        instance = error.instance if isinstance(error.instance, dict) else {}
        missing = ", ".join(str(item) for item in error.validator_value if item not in instance)
        if missing:
            return f"missing required field: {missing}"
    return str(error.message)


def _validate_workflow_manifest_issues(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    workflow = manifest.get("workflow")
    agents = manifest.get("agents")
    runtime = manifest.get("runtime")
    if not isinstance(workflow, dict):
        return [_workflow_validation_issue("workflow", "workflow must be an object")]
    if not isinstance(agents, dict):
        issues.append(_workflow_validation_issue("agents", "agents must be an object"))
    if not isinstance(runtime, dict):
        issues.append(_workflow_validation_issue("runtime", "runtime must be an object"))

    workflow_id = workflow.get("workflow_id")
    if not isinstance(workflow_id, str) or not workflow_id.strip():
        issues.append(_workflow_validation_issue("workflow.workflow_id", "workflow.workflow_id must be a non-empty string"))

    steps = workflow.get("steps")
    if not isinstance(steps, list) or not steps:
        issues.append(_workflow_validation_issue("workflow.steps", "workflow.steps must be a non-empty list"))
        steps = []

    step_ids: set[str] = set()
    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            issues.append(_workflow_validation_issue(f"workflow.steps[{index}]", "workflow step must be an object"))
            continue
        step_id = step.get("id")
        if not isinstance(step_id, str) or not step_id.strip():
            issues.append(_workflow_validation_issue(f"workflow.steps[{index}].id", "workflow step id is required"))
            continue
        if step_id in step_ids:
            issues.append(_workflow_validation_issue(f"workflow.steps[{index}].id", f"duplicate workflow step id: {step_id}"))
        step_ids.add(step_id)
        control = step.get("control")
        if isinstance(control, dict):
            retry = control.get("retry")
            if isinstance(retry, dict):
                attempts = retry.get("max_attempts")
                if attempts is not None and (not isinstance(attempts, int) or attempts < 1):
                    issues.append(_workflow_validation_issue(f"workflow.steps[{index}].control.retry.max_attempts", "retry max_attempts must be a positive integer"))
            timeout = control.get("timeout_seconds")
            if timeout is not None and (not isinstance(timeout, (int, float)) or timeout < 0):
                issues.append(_workflow_validation_issue(f"workflow.steps[{index}].control.timeout_seconds", "timeout_seconds must be zero or greater"))
        join = step.get("join")
        if join is not None:
            if not isinstance(join, dict):
                issues.append(_workflow_validation_issue(f"workflow.steps[{index}].join", "join must be an object"))
            else:
                mode = join.get("mode") or "all_required"
                if mode not in {"all_required", "min_success"}:
                    issues.append(_workflow_validation_issue(f"workflow.steps[{index}].join.mode", "join.mode must be all_required or min_success"))
                if mode == "min_success":
                    min_success = join.get("min_success")
                    if not isinstance(min_success, int) or min_success < 1:
                        issues.append(_workflow_validation_issue(f"workflow.steps[{index}].join.min_success", "join.min_success must be a positive integer"))

    issues.extend(_validate_workflow_graph_issues(workflow, step_ids))
    if isinstance(agents, dict):
        issues.extend(_validate_agent_graph_issues(agents))

    bindings = runtime.get("bindings") if isinstance(runtime, dict) else None
    if bindings is not None and not isinstance(bindings, dict):
        issues.append(_workflow_validation_issue("runtime.bindings", "runtime.bindings must be an object"))
    elif isinstance(bindings, dict):
        for step_id in bindings:
            if step_ids and step_id not in step_ids:
                issues.append(_workflow_validation_issue(f"runtime.bindings.{step_id}", "runtime binding must reference a workflow step id"))

    return issues


def _validate_workflow_graph_issues(workflow: dict[str, Any], step_ids: set[str]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    schema = workflow.get("schema")
    if schema != "mn.workflow.problem_graph/v1":
        issues.append(_workflow_validation_issue("workflow.schema", "workflow.schema must be mn.workflow.problem_graph/v1"))
    mode = workflow.get("mode") or "static_dag"
    if mode != "static_dag":
        issues.append(_workflow_validation_issue("workflow.mode", "workflow.mode must be static_dag"))

    source = workflow.get("source")
    sink = workflow.get("sink")
    entrypoint = workflow.get("entrypoint")
    if source != entrypoint:
        issues.append(_workflow_validation_issue("workflow.source", "workflow.source must match workflow.entrypoint"))
    edges = workflow.get("edges") or []
    if not isinstance(edges, list):
        return [_workflow_validation_issue("workflow.edges", "workflow.edges must be a list")]
    if not edges:
        issues.append(_workflow_validation_issue("workflow.edges", "workflow.edges must be a non-empty list"))
    if source not in step_ids:
        issues.append(_workflow_validation_issue("workflow.source", "workflow.source must reference a workflow step id"))
    if sink not in step_ids:
        issues.append(_workflow_validation_issue("workflow.sink", "workflow.sink must reference a workflow step id"))

    edge_ids: set[str] = set()
    adjacency: dict[str, list[str]] = {step_id: [] for step_id in step_ids}
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            issues.append(_workflow_validation_issue(f"workflow.edges[{index}]", "workflow edge must be an object"))
            continue
        edge_id = edge.get("id")
        if not isinstance(edge_id, str) or not edge_id.strip():
            issues.append(_workflow_validation_issue(f"workflow.edges[{index}].id", "workflow edge id must be a non-empty string"))
        elif edge_id in edge_ids:
            issues.append(_workflow_validation_issue(f"workflow.edges[{index}].id", f"duplicate workflow edge id: {edge_id}"))
        else:
            edge_ids.add(edge_id)
        upstream = edge.get("from")
        downstream = edge.get("to")
        if upstream not in step_ids:
            issues.append(_workflow_validation_issue(f"workflow.edges[{index}].from", "edge from must reference a workflow step id"))
        if downstream not in step_ids:
            issues.append(_workflow_validation_issue(f"workflow.edges[{index}].to", "edge to must reference a workflow step id"))
        if upstream == downstream and upstream in step_ids:
            issues.append(_workflow_validation_issue(f"workflow.edges[{index}].to", "workflow edge cannot point a step to itself"))
        required = edge.get("required", True)
        if not isinstance(required, bool):
            issues.append(_workflow_validation_issue(f"workflow.edges[{index}].required", "workflow edge required must be true or false"))
        accepts = edge.get("accepts")
        if accepts is not None and (not isinstance(accepts, list) or not accepts or not all(isinstance(item, str) and item for item in accepts)):
            issues.append(_workflow_validation_issue(f"workflow.edges[{index}].accepts", "workflow edge accepts must be a non-empty string list"))
        if upstream in step_ids and downstream in step_ids:
            adjacency.setdefault(upstream, []).append(downstream)

    if not issues:
        cycle = _workflow_graph_cycle(adjacency)
        if cycle:
            issues.append(_workflow_validation_issue("workflow.edges", f"workflow graph must be acyclic: {' -> '.join(cycle)}"))
        if isinstance(source, str) and isinstance(sink, str):
            reachable = _workflow_reachable(adjacency, source)
            missing = sorted(step_ids - reachable)
            if missing:
                issues.append(_workflow_validation_issue("workflow.source", f"workflow steps are unreachable from source: {', '.join(missing)}"))
            if sink not in reachable:
                issues.append(_workflow_validation_issue("workflow.sink", "workflow sink is not reachable from source"))

    return issues


def _validate_agent_graph_issues(agents: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if agents.get("schema") != "mn.agents.communication_graph/v1":
        issues.append(_workflow_validation_issue("agents.schema", "agents.schema must be mn.agents.communication_graph/v1"))
    nodes = agents.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        return issues + [_workflow_validation_issue("agents.nodes", "agents.nodes must be a non-empty list")]
    node_ids: set[str] = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            issues.append(_workflow_validation_issue(f"agents.nodes[{index}]", "agent node must be an object"))
            continue
        node_id = node.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            issues.append(_workflow_validation_issue(f"agents.nodes[{index}].node_id", "agent node_id is required"))
        elif node_id in node_ids:
            issues.append(_workflow_validation_issue(f"agents.nodes[{index}].node_id", f"duplicate agent node id: {node_id}"))
        else:
            node_ids.add(node_id)

    entrypoints = agents.get("entrypoints")
    if not isinstance(entrypoints, list) or not entrypoints:
        issues.append(_workflow_validation_issue("agents.entrypoints", "agents.entrypoints must be a non-empty list"))
    else:
        for index, entrypoint in enumerate(entrypoints):
            if entrypoint not in node_ids:
                issues.append(_workflow_validation_issue(f"agents.entrypoints[{index}]", "agent entrypoint must reference an agent node id"))

    edges = agents.get("edges")
    if not isinstance(edges, list):
        return issues + [_workflow_validation_issue("agents.edges", "agents.edges must be a list")]
    edge_ids: set[str] = set()
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            issues.append(_workflow_validation_issue(f"agents.edges[{index}]", "agent edge must be an object"))
            continue
        edge_id = edge.get("edge_id")
        if not isinstance(edge_id, str) or not edge_id.strip():
            issues.append(_workflow_validation_issue(f"agents.edges[{index}].edge_id", "agent edge_id is required"))
        elif edge_id in edge_ids:
            issues.append(_workflow_validation_issue(f"agents.edges[{index}].edge_id", f"duplicate agent edge id: {edge_id}"))
        else:
            edge_ids.add(edge_id)
        if edge.get("from_node") not in node_ids:
            issues.append(_workflow_validation_issue(f"agents.edges[{index}].from_node", "agent edge from_node must reference an agent node id"))
        if edge.get("to_node") not in node_ids:
            issues.append(_workflow_validation_issue(f"agents.edges[{index}].to_node", "agent edge to_node must reference an agent node id"))
    return issues


def _workflow_reachable(adjacency: dict[str, list[str]], source: str) -> set[str]:
    seen: set[str] = set()
    stack = [source]
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(adjacency.get(node, []))
    return seen


def _workflow_graph_cycle(adjacency: dict[str, list[str]]) -> list[str]:
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(node: str) -> list[str]:
        if node in visiting:
            if node in path:
                return path[path.index(node) :] + [node]
            return [node, node]
        if node in visited:
            return []
        visiting.add(node)
        path.append(node)
        for child in adjacency.get(node, []):
            cycle = visit(child)
            if cycle:
                return cycle
        path.pop()
        visiting.remove(node)
        visited.add(node)
        return []

    for node in adjacency:
        cycle = visit(node)
        if cycle:
            return cycle
    return []


def _workflow_validation_issue(path: str, message: str, *, code: str = "workflow_manifest.validation_failed") -> dict[str, Any]:
    pointer = "/manifest" if path == "manifest" else "/manifest/" + path.replace(".", "/")
    return {
        "code": code,
        "message": message,
        "help": "Fix this workflow manifest field and run validation again.",
        "severity": "error",
        "location": {
            "source": "manifest",
            "path": path,
            "pointer": pointer,
        },
    }


def validate_python_environments(
    bundle_dir: Path, manifest: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    nodes = _manifest_agent_nodes(manifest)
    if not nodes:
        return errors

    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        config = node.get("config")
        if not isinstance(config, dict) or "python_environment" not in config:
            continue

        node_id = str(node.get("node_id") or f"nodes[{index}]")
        runner_module = config.get("runner_module")
        python_environment = config.get("python_environment")
        if runner_module != "MirrorNeuron.Runner.HostLocal":
            errors.append(
                f"{node_id}: python_environment is only supported with MirrorNeuron.Runner.HostLocal"
            )
            continue
        if not isinstance(python_environment, dict):
            errors.append(f"{node_id}: python_environment must be an object")
            continue

        requirements = python_environment.get("requirements")
        if requirements not in (None, ""):
            if not isinstance(requirements, str):
                errors.append(
                    f"{node_id}: python_environment.requirements must be a string"
                )
            elif not _is_safe_payload_relative_path(requirements):
                errors.append(
                    f"{node_id}: python_environment.requirements must be a relative path inside payloads/"
                )
            elif not (bundle_dir / "payloads" / requirements).is_file():
                errors.append(
                    f"{node_id}: python_environment requirements file not found: payloads/{requirements}"
                )

        packages = python_environment.get("packages")
        if packages is not None and (
            not isinstance(packages, list)
            or not all(
                isinstance(package, str) and package.strip() for package in packages
            )
        ):
            errors.append(
                f"{node_id}: python_environment.packages must be a list of non-empty strings"
            )

    return errors


def _manifest_agent_nodes(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    agents = manifest.get("agents") if isinstance(manifest.get("agents"), dict) else {}
    agent_nodes = agents.get("nodes") if isinstance(agents, dict) else None
    if isinstance(agent_nodes, list):
        return [node for node in agent_nodes if isinstance(node, dict)]
    root_nodes = manifest.get("nodes")
    if isinstance(root_nodes, list):
        return [node for node in root_nodes if isinstance(node, dict)]
    return []


def _validate_manifest_inputs_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    output_format: str = "table",
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    result = run_input_validation(bundle_dir, manifest, config=config, env=env)
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Input validation failed")
    raise typer.Exit(1)


def _validate_manifest_services_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    output_format: str = "table",
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )

    def resolver(name: str, requirement: dict[str, Any]) -> list[dict[str, Any]]:
        response = client.resolve_service(
            name,
            tags=requirement.get("tags") or [],
            passing_only=True,
        )
        decoded = json.loads(response)
        services = decoded.get("services") if isinstance(decoded, dict) else []
        return services if isinstance(services, list) else []

    result = run_service_validation(
        bundle_dir,
        manifest,
        config=config,
        env=env,
        resolver=resolver,
    )
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Service validation failed")
    raise typer.Exit(1)


def _validate_manifest_models_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    output_format: str = "table",
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    result = run_model_validation(bundle_dir, manifest, config=config, env=env)
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Model validation failed")
    raise typer.Exit(1)


def _mark_manifest_force(manifest: dict[str, Any]) -> None:
    metadata = manifest.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        manifest["metadata"] = metadata
    validation = metadata.setdefault("mn_validation", {})
    if not isinstance(validation, dict):
        validation = {}
        metadata["mn_validation"] = validation
    validation["force"] = True
    validation["status"] = "skipped"
    validation["skipped_checks"] = ["services", "models", "input_validation", "requirements"]


def _normalize_validation_output(output: str) -> str:
    normalized = str(output or "table").strip().lower()
    if normalized in {"table", "rich", "pretty"}:
        return "table"
    if normalized == "json":
        return "json"
    console.print("[red]Unsupported output format. Use 'table' or 'json'.[/red]")
    raise typer.Exit(1)


def _emit_validation_report(
    report: dict[str, Any], output_format: str, *, title: str
) -> None:
    if output_format == "json":
        console.print_json(data=report)
        return

    issues = report.get("issues") if isinstance(report.get("issues"), list) else []
    if not issues:
        for error in report.get("errors") or []:
            console.print(f"[red]{title}: {error}[/red]")
        return

    console.print(f"[red]{title}[/red]")
    console.print("Field | Problem | Fix | Rule", markup=False)
    console.print("--- | --- | --- | ---", markup=False)
    for issue in issues:
        location = (
            issue.get("location") if isinstance(issue.get("location"), dict) else {}
        )
        rule = issue.get("rule") if isinstance(issue.get("rule"), dict) else {}
        console.print(
            " | ".join(
                [
                    str(location.get("path") or location.get("pointer") or "-"),
                    str(
                        issue.get("message") or issue.get("code") or "Validation failed"
                    ),
                    str(issue.get("help") or "-"),
                    str(rule.get("name") or rule.get("id") or "-"),
                ]
            ),
            markup=False,
        )


def _model_capacity_summary(report: dict[str, Any]) -> str:
    summaries: list[str] = []
    for result in report.get("results") or []:
        if not isinstance(result, dict):
            continue
        requirements = result.get("requirements")
        if not isinstance(requirements, dict) or not requirements:
            continue
        parts = [str(result.get("model_id") or result.get("model") or result.get("name") or "model")]
        provider = result.get("provider")
        if provider:
            parts.append(f"provider {provider}")
        min_vram = requirements.get("min_vram_gb")
        if min_vram is not None:
            parts.append(f"GPU >= {min_vram}GB")
        capabilities = requirements.get("required_capabilities")
        if capabilities:
            parts.append("capability any of " + ",".join(str(item) for item in capabilities))
        summaries.append(" ".join(parts))
    return "; ".join(summaries[:3])


def _legacy_validation_issue(error: str, *, source: str) -> dict[str, Any]:
    path = ""
    if ":" in error:
        path = error.split(":", 1)[0].strip()
    return {
        "code": "manifest.validation_failed",
        "message": error,
        "help": "Fix this manifest field and run validation again.",
        "severity": "error",
        "location": {
            "source": source,
            "path": path,
            "pointer": "/" + source + ("/" + path.replace(".", "/") if path else ""),
        },
    }


def _is_safe_payload_relative_path(path: str) -> bool:
    candidate = Path(path)
    return (
        not candidate.is_absolute()
        and path not in ("", ".")
        and ".." not in candidate.parts
    )


def run(
    target: Annotated[
        Optional[str],
        typer.Argument(
            help="Catalog blueprint ID to run. Use --folder for local blueprint or bundle folders.",
        ),
    ] = None,
    folder: Annotated[
        Optional[str],
        typer.Option(
            "--folder",
            help="Run a local blueprint or bundle folder. Local folders must use this option.",
        ),
    ] = None,
    run_id: Annotated[
        Optional[str],
        typer.Option("--run-id", help="Use a specific shared blueprint run ID."),
    ] = None,
    blueprint_repo: Annotated[
        Optional[str],
        typer.Option(
            "--blueprint-repo",
            help="Use this blueprint repository URL/path instead of the default catalog.",
        ),
    ] = None,
    update: Annotated[
        bool,
        typer.Option(
            "--update",
            help="Update the cached blueprint repository before running a catalog blueprint.",
        ),
    ] = False,
    offline: Annotated[
        bool,
        typer.Option(
            "--offline",
            help="Use only local blueprint files; never clone, fetch, or pull.",
        ),
    ] = False,
    revision: Annotated[
        Optional[str],
        typer.Option("--revision", help="Checkout a specific git revision before running."),
    ] = None,
    follow_seconds: Annotated[
        Optional[float],
        typer.Option(
            "--follow-seconds",
            help="Seconds to keep polling job events after the submit stream detaches. Defaults to MN_RUN_DETACH_LOG_SECONDS or 30.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Run even if blueprint input validation or runtime requirements fail.",
        ),
    ] = False,
    detached: Annotated[
        bool,
        typer.Option(
            "-d",
            "--detached",
            help="Start the blueprint run without the live workflow UI.",
        ),
    ] = False,
    web_ui: Annotated[
        bool,
        typer.Option(
            "--web-ui",
            help="Start or register the blueprint Web UI for this run.",
        ),
    ] = False,
    auto_schedule: Annotated[
        bool,
        typer.Option(
            "--auto-schedule",
            help="Queue the run until the cluster has the required agent resources.",
        ),
    ] = False,
    schedule: Annotated[
        Optional[str],
        typer.Option(
            "--schedule",
            help="Create a schedule instead of running now. Accepts JSON, a delay like 30m, or an ISO run_at timestamp.",
        ),
    ] = None,
):
    """Run a catalog blueprint, or a local folder with --folder."""
    if auto_schedule and schedule:
        console.print("[red]Error: pass either --auto-schedule or --schedule, not both.[/red]")
        raise typer.Exit(1)

    if folder and target:
        console.print("[red]Error: pass either a blueprint ID or --folder, not both.[/red]")
        raise typer.Exit(1)

    if folder:
        _run_local_folder(
            folder,
            run_id=run_id,
            follow_seconds=follow_seconds,
            force=force,
            detached=detached,
            web_ui=web_ui,
            auto_schedule=auto_schedule,
            schedule=schedule,
        )
        return

    if not target:
        console.print("[red]Error: mn blueprint run expects a blueprint ID or --folder <path>.[/red]")
        console.print("Use [bold]mn blueprint run <blueprint-id>[/bold] for catalog blueprints.")
        console.print("Use [bold]mn blueprint run --folder <path>[/bold] for local blueprint or bundle folders.")
        raise typer.Exit(1)

    target_path = Path(target).expanduser()
    if target_path.exists():
        console.print("[red]Error: local folders must be passed with --folder.[/red]")
        console.print(f"Use [bold]mn blueprint run --folder {target_path}[/bold].")
        raise typer.Exit(1)

    from mn_cli.libs.blueprint_cmds import run_catalog_blueprint

    run_catalog_blueprint(
        target,
        run_id=run_id,
        blueprint_repo=blueprint_repo,
        update=update,
        offline=offline,
        revision=revision,
        follow_seconds=follow_seconds,
        force=force,
        detached=detached,
        web_ui=web_ui,
        auto_schedule=auto_schedule,
        schedule=schedule,
    )


def _run_local_folder(
    folder: str,
    *,
    run_id: Optional[str],
    follow_seconds: Optional[float],
    force: bool,
    detached: bool = False,
    web_ui: bool = False,
    auto_schedule: bool = False,
    schedule: Optional[str] = None,
) -> None:
    bundle_dir = Path(folder).expanduser()
    manifest = _load_manifest_for_local_run(bundle_dir)
    if manifest is not None and _is_python_source_manifest(manifest):
        from mn_cli.libs.blueprint_cmds import run_local_blueprint_folder

        run_local_blueprint_folder(
            str(bundle_dir),
            run_id=run_id,
            follow_seconds=follow_seconds,
            force=force,
            detached=detached,
            web_ui=web_ui,
            auto_schedule=auto_schedule,
            schedule=schedule,
        )
        return

    env_overrides = {"MN_RUN_ID": run_id} if run_id else None
    submission_metadata = {"blueprint_run_id": run_id} if run_id else None
    run_bundle(
        str(bundle_dir),
        follow_seconds=follow_seconds,
        env_overrides=env_overrides,
        submission_metadata=submission_metadata,
        force=force,
        detached=detached,
        web_ui=web_ui,
        auto_schedule=auto_schedule,
        schedule=schedule,
    )


def _load_manifest_for_local_run(bundle_dir: Path) -> Optional[dict[str, Any]]:
    manifest_file = bundle_dir / "manifest.json"
    if not manifest_file.exists():
        return None
    try:
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    except Exception:
        return None
    return manifest if isinstance(manifest, dict) else None


def _is_python_source_manifest(manifest: dict[str, Any]) -> bool:
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    return metadata.get("python_source_mode") is True or bool(metadata.get("python_workflow"))


def _run_schedule_attrs(*, auto_schedule: bool, schedule: Optional[str]) -> Optional[dict[str, Any]]:
    if auto_schedule and schedule:
        console.print("[red]Error: pass either --auto-schedule or --schedule, not both.[/red]")
        raise typer.Exit(1)
    if auto_schedule:
        return {
            "kind": "resource_wait",
            "retry_interval_ms": int(os.getenv("MN_RESOURCE_WAIT_RETRY_MS", "30000")),
            "metadata": {"requested_by": "mn run --auto-schedule"},
        }
    if not schedule:
        return None

    raw = schedule.strip()
    if not raw:
        raise typer.BadParameter("--schedule cannot be empty")
    if raw.startswith("{"):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter("--schedule JSON must be valid") from exc
        if not isinstance(parsed, dict):
            raise typer.BadParameter("--schedule JSON must be an object")
        return parsed
    if raw.lower() in {"auto", "resource", "resource_wait", "resource-wait"}:
        return {
            "kind": "resource_wait",
            "retry_interval_ms": int(os.getenv("MN_RESOURCE_WAIT_RETRY_MS", "30000")),
            "metadata": {"requested_by": "mn run --schedule"},
        }
    if re.fullmatch(r"\d+(\.\d+)?(ms|s|m|h|d)?", raw.lower()):
        return {
            "kind": "delayed",
            "delay_ms": _duration_ms_for_schedule(raw),
            "metadata": {"requested_by": "mn run --schedule"},
        }
    return {
        "kind": "delayed",
        "run_at": raw,
        "metadata": {"requested_by": "mn run --schedule"},
    }


def _duration_ms_for_schedule(value: str) -> int:
    raw = str(value or "").strip().lower()
    units = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
    for suffix, multiplier in units.items():
        if raw.endswith(suffix):
            return int(float(raw[: -len(suffix)]) * multiplier)
    return int(float(raw) * 1000)


def run_bundle(
    bundle_path: str,
    *,
    follow_seconds: Optional[float] = None,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    force: bool = False,
    detached: bool = False,
    web_ui: bool = False,
    auto_schedule: bool = False,
    schedule: Optional[str] = None,
):
    """Run a bundle after applying optional runtime metadata and environment."""
    pre_launch_process: subprocess.Popen[Any] | None = None
    pre_launch_run_dir: Path | None = None
    submitted_job_id: str | None = None
    submitted_log_writer: JobLogWriter | None = None
    submitted_bundle_dir: Path | None = None
    submitted_manifest: dict[str, Any] | None = None
    submitted_run_dir: Path | None = None
    submitted_web_ui_url: str | None = None
    submitted_config_overrides: dict[str, Any] | None = None
    try:
        env_overrides = dict(env_overrides or {})
        config_overrides = dict(config_overrides or {})
        submitted_config_overrides = config_overrides
        submission_metadata = dict(submission_metadata or {})
        bundle_dir = Path(bundle_path)
        submitted_bundle_dir = bundle_dir
        if not bundle_dir.is_dir():
            console.print(
                f"[red]Error: '{bundle_path}' is not a directory. Expected a bundle folder.[/red]"
            )
            raise typer.Exit(1)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(
                f"[red]Error: manifest.json not found in '{bundle_path}'[/red]"
            )
            raise typer.Exit(1)

        with open(manifest_file, "r") as f:
            manifest_dict = json.load(f)

        if manifest_dict.get("require_config") is True:
            config_script = bundle_dir / "config.py"
            if config_script.exists():
                import subprocess
                import sys

                console.print(
                    f"[yellow]Bundle requires configuration. Auto-running {config_script.name}...[/yellow]"
                )
                res = subprocess.run(
                    [sys.executable, config_script.name], cwd=bundle_dir
                )
                if res.returncode != 0:
                    console.print(
                        "[red]Configuration failed or cancelled. Aborting run.[/red]"
                    )
                    raise typer.Exit(1)

                # Reload manifest after configuration
                with open(manifest_file, "r") as f:
                    manifest_dict = json.load(f)
            else:
                console.print(
                    "[red]Bundle requires configuration, but config.py was not found.[/red]"
                )
                raise typer.Exit(1)

        _ensure_local_run_store_identity(
            bundle_dir,
            manifest_dict,
            env_overrides,
            submission_metadata,
            config_overrides=config_overrides,
        )
        blueprint_run_id = submission_metadata.get(
            "blueprint_run_id"
        ) or env_overrides.get("MN_RUN_ID")
        if blueprint_run_id:
            pre_launch_run_dir = _blueprint_run_dir(
                str(blueprint_run_id), env_overrides
            )
            _register_post_launch_hook(
                bundle_dir, str(blueprint_run_id), env_overrides=env_overrides
            )
            pre_launch_process = _start_pre_launch_hook(
                bundle_dir,
                str(blueprint_run_id),
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
        if not force:
            _validate_manifest_services_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
            _validate_manifest_models_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
            _validate_manifest_inputs_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
        else:
            console.print(
                "[yellow]Validation skipped because --force was provided; service checks, model checks, input checks, and runtime requirements will be bypassed for this run.[/yellow]"
            )
        manifest_dict = prepare_manifest_for_submission(
            bundle_dir,
            manifest_dict,
            env_overrides=env_overrides,
            submission_metadata=submission_metadata,
            config_overrides=config_overrides,
            enable_runtime_web_ui=web_ui,
        )
        if force:
            _mark_manifest_force(manifest_dict)
        _prepare_openshell_custom_images(bundle_dir, manifest_dict)

        payloads = {}
        payloads_dir = bundle_dir / "payloads"
        if payloads_dir.is_dir():
            for filepath in payloads_dir.rglob("*"):
                if filepath.is_file():
                    rel_path = filepath.relative_to(payloads_dir).as_posix()
                    with open(filepath, "rb") as f:
                        payloads[rel_path] = f.read()
        if web_ui:
            payloads.update(runtime_web_ui_support_payloads_for_manifest(manifest_dict))
        stage_blueprint_support_payloads_for_manifest(manifest_dict, payloads, bundle_dir=bundle_dir)

        schedule_attrs = _run_schedule_attrs(auto_schedule=auto_schedule, schedule=schedule)
        if schedule_attrs is not None:
            stage_local_input_payloads_for_manifest(manifest_dict, payloads, bundle_dir=bundle_dir)
            promote_large_payloads_to_blob_refs(manifest_dict, payloads)
            manifest = json.dumps(manifest_dict)
            submitted_manifest = manifest_dict
            result_json = client.create_schedule(
                manifest,
                payloads,
                schedule=schedule_attrs,
                source={"cli": "run", "bundle": bundle_dir.name},
            )
            result = json.loads(result_json)
            print_success_confirmation(
                console,
                "Schedule create",
                status=result.get("status"),
                details=[
                    ("Schedule ID", result.get("schedule_id") or result.get("id")),
                    ("Kind", result.get("kind") or schedule_attrs.get("kind")),
                    ("Bundle", bundle_dir),
                ],
                next_steps="mn schedule list",
            )
            return

        prepared_submission = prepare_job_submission(
            manifest_dict,
            payloads,
            bundle_dir=bundle_dir,
            run_id=blueprint_run_id,
        )
        manifest = prepared_submission.manifest_json
        payloads = prepared_submission.payloads
        submitted_manifest = json.loads(manifest)

        blueprint_run_dir = (
            _blueprint_run_dir(blueprint_run_id, env_overrides)
            if blueprint_run_id
            else None
        )
        submitted_run_dir = blueprint_run_dir
        job_id = client.submit_job(manifest, payloads, force=force)
        submitted_job_id = job_id
        log_writer = JobLogWriter(job_id, run_dir=blueprint_run_dir)
        submitted_log_writer = log_writer
        if blueprint_run_id:
            _write_blueprint_job_mapping(
                blueprint_run_id, job_id, submission_metadata, env_overrides
            )
            if web_ui:
                _write_local_web_ui_handle(
                    bundle_dir,
                    blueprint_run_id,
                    env_overrides=env_overrides,
                    config_overrides=config_overrides,
                )
        web_ui_url = _console_web_ui_url(manifest_dict, blueprint_run_dir) if web_ui else None
        submitted_web_ui_url = web_ui_url
        resolved_follow_seconds = (
            float(os.getenv("MN_RUN_DETACH_LOG_SECONDS", "30"))
            if follow_seconds is None
            else follow_seconds
        )

        console.print(
            generate_run_submitted_panel(
                bundle_name=bundle_dir.name,
                job_id=job_id,
                payload_count=len(payloads),
                log_dir=log_writer.log_dir,
                follow_seconds=resolved_follow_seconds,
                run_mode=_run_mode_label(manifest_dict),
                blueprint_run_id=blueprint_run_id,
                blueprint_revision=submission_metadata.get("blueprint_revision"),
                web_ui_url=web_ui_url,
                detached=detached,
            )
        )
        if detached:
            if web_ui and blueprint_run_dir is not None:
                _start_background_event_relay_if_needed(
                    bundle_dir,
                    manifest_dict,
                    job_id,
                    blueprint_run_dir,
                    "submitted",
                    config_overrides=config_overrides,
                )
            console.print(
                generate_detached_panel(
                    job_id,
                    log_writer.log_dir,
                    "submitted",
                    log_writer.event_count,
                    web_ui_url=log_writer.web_ui_url or web_ui_url,
                )
            )
            return

        final_status = _stream_and_format_events(
            job_id,
            log_writer,
            resolved_follow_seconds,
            web_ui_url=web_ui_url,
            manifest=manifest_dict,
        )
        if blueprint_run_dir is not None:
            if web_ui:
                _start_background_event_relay_if_needed(
                    bundle_dir,
                    manifest_dict,
                    job_id,
                    blueprint_run_dir,
                    final_status,
                    config_overrides=config_overrides,
                )
            if final_status in FINAL_STATUSES:
                cleanup_blueprint_host_hooks(
                    blueprint_run_dir,
                    dry_run=False,
                    summary={
                        "process_removed": [],
                        "process_skipped": [],
                        "errors": [],
                    },
                    reason=f"job_{final_status}",
                )
    except typer.Exit:
        _terminate_pre_launch_process(pre_launch_process, reason="launch_failed")
        if pre_launch_run_dir is not None:
            cleanup_blueprint_host_hooks(
                pre_launch_run_dir,
                dry_run=False,
                summary={"process_removed": [], "process_skipped": [], "errors": []},
                reason="launch_failed",
            )
        raise
    except (KeyboardInterrupt, EOFError):
        if submitted_job_id:
            log_writer = submitted_log_writer or JobLogWriter(
                submitted_job_id, run_dir=submitted_run_dir
            )
            status = "running"
            try:
                status, _data = _follow_job_events(submitted_job_id, log_writer, 0)
                if status == "unknown":
                    status = "running"
            except Exception:
                log_writer.run_logger.exception("Failed to poll detached job status")
            console.print(f"[yellow]{DETACHED_AFTER_INTERRUPT_MESSAGE}[/yellow]")
            if (
                submitted_run_dir is not None
                and submitted_bundle_dir is not None
                and submitted_manifest is not None
            ):
                if web_ui:
                    _start_background_event_relay_if_needed(
                        submitted_bundle_dir,
                        submitted_manifest,
                        submitted_job_id,
                        submitted_run_dir,
                        status,
                        config_overrides=submitted_config_overrides,
                    )
            console.print(
                generate_detached_panel(
                    submitted_job_id,
                    log_writer.log_dir,
                    status,
                    log_writer.event_count,
                    web_ui_url=log_writer.web_ui_url or submitted_web_ui_url,
                )
            )
            return
        _terminate_pre_launch_process(pre_launch_process, reason="launch_interrupted")
        if pre_launch_run_dir is not None:
            cleanup_blueprint_host_hooks(
                pre_launch_run_dir,
                dry_run=False,
                summary={"process_removed": [], "process_skipped": [], "errors": []},
                reason="launch_interrupted",
            )
        raise typer.Exit(130)
    except Exception as e:
        _terminate_pre_launch_process(pre_launch_process, reason="launch_failed")
        if pre_launch_run_dir is not None:
            cleanup_blueprint_host_hooks(
                pre_launch_run_dir,
                dry_run=False,
                summary={"process_removed": [], "process_skipped": [], "errors": []},
                reason="launch_failed",
            )
        handle_cli_error(e, console, "run bundle")
        raise typer.Exit(1)


def _console_web_ui_url(
    manifest_dict: dict[str, Any],
    run_dir: Optional[Path],
) -> Optional[str]:
    return (
        _console_web_ui_url_from_manifest(manifest_dict)
        or _console_web_ui_url_from_run_dir(run_dir)
    )


def _console_web_ui_url_from_manifest(manifest_dict: dict[str, Any]) -> Optional[str]:
    metadata = (
        manifest_dict.get("metadata")
        if isinstance(manifest_dict.get("metadata"), dict)
        else {}
    )
    for candidate in (
        manifest_dict.get("web_ui_service"),
        manifest_dict.get("blueprint_web_ui_service"),
        metadata.get("blueprint_web_ui_service"),
        metadata.get("web_ui_service"),
    ):
        url = _web_ui_url_from_mapping(candidate)
        if url:
            return url

    nodes = manifest_dict.get("nodes")
    if not isinstance(nodes, list):
        return None
    for node in nodes:
        if not isinstance(node, dict):
            continue
        services = node.get("services")
        if not isinstance(services, list):
            continue
        for service in services:
            if not isinstance(service, dict):
                continue
            tags = service.get("tags") if isinstance(service.get("tags"), list) else []
            if service.get("name") != "blueprint-web-ui" and "web_ui" not in tags:
                continue
            url = _web_ui_url_from_mapping(service.get("meta"))
            if url:
                return url
            port = service.get("port")
            if port:
                return f"http://localhost:{port}"
    return None


def _console_web_ui_url_from_run_dir(run_dir: Optional[Path]) -> Optional[str]:
    if run_dir is None:
        return None
    try:
        handle = json.loads((run_dir / "web_ui.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return _web_ui_url_from_mapping(handle)


def _web_ui_url_from_mapping(value: Any) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    for key in ("url", "web_ui_url", "local_url"):
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _ensure_local_run_store_identity(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    env_overrides: dict[str, str],
    submission_metadata: dict[str, Any],
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    if submission_metadata.get("blueprint_run_id") or env_overrides.get("MN_RUN_ID"):
        return

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    identity = config.get("identity") if isinstance(config, dict) else {}
    identity = identity if isinstance(identity, dict) else {}
    metadata = (
        manifest_dict.get("metadata")
        if isinstance(manifest_dict.get("metadata"), dict)
        else {}
    )
    workflow_manifest = _is_workflow_manifest(manifest_dict)
    blueprint_id = (
        identity.get("blueprint_id")
        or metadata.get("blueprint_id")
        or manifest_dict.get("id")
        or (None if workflow_manifest else manifest_dict.get("graph_id"))
        or bundle_dir.name
    )
    run_id = identity.get("run_id") or _make_blueprint_run_id(str(blueprint_id))
    env_overrides["MN_RUN_ID"] = str(run_id)
    submission_metadata.setdefault("blueprint_id", str(blueprint_id))
    submission_metadata["blueprint_run_id"] = str(run_id)


def _prepare_openshell_custom_images(
    bundle_dir: Path, manifest_dict: dict[str, Any]
) -> None:
    nodes = manifest_dict.get("nodes")
    if not isinstance(nodes, list):
        return

    for node in nodes:
        if not isinstance(node, dict):
            continue
        config = node.get("config")
        if not isinstance(config, dict):
            continue
        if config.get("runner_module") != "MirrorNeuron.Sandbox.OpenShell":
            continue

        custom_image = config.get("custom_openshell_image")
        if custom_image is not None:
            source_path = _openshell_local_from_path(bundle_dir, custom_image)
            if source_path is None:
                console.print(
                    f"[red]custom_openshell_image for {node.get('node_id') or 'OpenShell node'} "
                    f"must point to a payload directory or Dockerfile: {custom_image}[/red]"
                )
                raise typer.Exit(1)
        else:
            source_path = _openshell_local_from_path(bundle_dir, config.get("from"))

        if source_path is None:
            continue

        config["from"] = _build_openshell_from_image(
            source_path, node.get("node_id") or "openshell"
        )


def _openshell_gateway_endpoint() -> str:
    configured_endpoint = os.getenv("OPENSHELL_GATEWAY_ENDPOINT")
    if configured_endpoint:
        return configured_endpoint

    gateway_name = _openshell_gateway_name()
    if gateway_name:
        metadata = _openshell_gateway_metadata(gateway_name)
        endpoint = metadata.get("gateway_endpoint")
        if isinstance(endpoint, str) and endpoint.strip():
            return endpoint.strip()

    return f"http://127.0.0.1:{os.getenv('OPENSHELL_GATEWAY_PORT', '58080')}"


def _openshell_env() -> dict[str, str]:
    env = os.environ.copy()
    if env.get("OPENSHELL_GATEWAY_ENDPOINT"):
        return env

    gateway_name = _openshell_gateway_name(env=env)
    if gateway_name:
        env.setdefault("OPENSHELL_GATEWAY", gateway_name)
    else:
        env.setdefault("OPENSHELL_GATEWAY_ENDPOINT", _openshell_gateway_endpoint())
    return env


def _openshell_config_dir() -> Path:
    return Path(
        os.getenv("OPENSHELL_CONFIG_DIR", str(Path.home() / ".config" / "openshell"))
    ).expanduser()


def _openshell_gateway_name(*, env: dict[str, str] | None = None) -> str:
    source_env = env or os.environ
    configured_gateway = source_env.get("OPENSHELL_GATEWAY", "").strip()
    if configured_gateway:
        return configured_gateway

    config_dir = _openshell_config_dir()
    try:
        active_gateway = (
            (config_dir / "active_gateway").read_text(encoding="utf-8").strip()
        )
        if active_gateway:
            return active_gateway
    except OSError:
        pass

    if (config_dir / "gateways" / "openshell" / "metadata.json").is_file():
        return "openshell"
    return ""


def _openshell_gateway_metadata(gateway_name: str) -> dict[str, Any]:
    if not gateway_name:
        return {}

    metadata_path = (
        _openshell_config_dir() / "gateways" / gateway_name / "metadata.json"
    )
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return metadata if isinstance(metadata, dict) else {}


def _openshell_local_from_path(bundle_dir: Path, source: Any) -> Path | None:
    if not isinstance(source, str) or not source.strip():
        return None

    source = source.strip()
    if "://" in source:
        return None

    raw = Path(source).expanduser()
    candidates = (
        [raw]
        if raw.is_absolute()
        else [bundle_dir / "payloads" / source, bundle_dir / source]
    )

    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate.is_dir() and (candidate / "Dockerfile").is_file():
            return candidate
        if candidate.is_file() and candidate.name == "Dockerfile":
            return candidate
    return None


def _build_openshell_from_image(source_path: Path, node_id: Any) -> str:
    console.print(
        f"[yellow]Building OpenShell sandbox image for {node_id} from {source_path}...[/yellow]"
    )
    if _openshell_gateway_uses_local_docker():
        image_ref = _build_local_docker_sandbox_image(source_path)
        print_success_confirmation(
            console,
            "OpenShell sandbox image build",
            status="ready",
            details={"Image": image_ref},
        )
        return image_ref

    result = subprocess.run(
        [
            "openshell",
            "sandbox",
            "create",
            "--from",
            str(source_path),
            "--no-tty",
            "--no-keep",
            "--",
            "true",
        ],
        capture_output=True,
        text=True,
        env=_openshell_env(),
    )
    output = f"{result.stdout}\n{result.stderr}"
    if result.returncode != 0:
        console.print(
            f"[red]Failed to build OpenShell sandbox image for {node_id}.[/red]"
        )
        if output.strip():
            console.print(output.strip())
        raise typer.Exit(1)

    matches = re.findall(r"Image\s+([^\s]+)\s+is available in the gateway", output)
    if not matches:
        console.print(
            f"[red]OpenShell did not report an image reference for {node_id}.[/red]"
        )
        if output.strip():
            console.print(output.strip())
        raise typer.Exit(1)

    image_ref = ANSI_ESCAPE_RE.sub("", matches[-1])
    print_success_confirmation(
        console,
        "OpenShell sandbox image build",
        status="ready",
        details={"Image": image_ref},
    )
    return image_ref


def _openshell_gateway_uses_local_docker() -> bool:
    gateway_name = _openshell_gateway_name()
    if not gateway_name:
        return False

    metadata = _openshell_gateway_metadata(gateway_name)
    if metadata.get("is_remote") is True:
        return False

    endpoint = metadata.get("gateway_endpoint")
    if not isinstance(endpoint, str):
        return False
    parsed = urllib.parse.urlparse(endpoint)
    return parsed.hostname in {"127.0.0.1", "localhost", "::1"}


def _build_local_docker_sandbox_image(source_path: Path) -> str:
    source_path = source_path.resolve()
    digest = hashlib.sha256(str(source_path).encode("utf-8")).hexdigest()[:12]
    image_ref = f"openshell/sandbox-from:{digest}"
    result = subprocess.run(
        ["docker", "build", "-t", image_ref, str(source_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        output = f"{result.stdout}\n{result.stderr}".strip()
        if output:
            console.print(output)
        raise typer.Exit(1)
    return image_ref


def _write_blueprint_job_mapping(
    blueprint_run_id: str,
    job_id: str,
    metadata: dict[str, Any],
    env_overrides: dict[str, str],
) -> None:
    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": blueprint_run_id,
            "job_id": job_id,
            "blueprint_id": metadata.get("blueprint_id"),
            "blueprint_revision": metadata.get("blueprint_revision"),
            "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        tmp = run_dir / f".job.json.{os.getpid()}.tmp"
        tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        tmp.replace(run_dir / "job.json")
    except OSError:
        logger.exception(
            "Failed to write blueprint job mapping for run_id=%s job_id=%s",
            blueprint_run_id,
            job_id,
        )


def _blueprint_run_dir(blueprint_run_id: str, env_overrides: dict[str, str]) -> Path:
    runs_root = Path(
        env_overrides.get("MN_RUNS_ROOT") or os.getenv("MN_RUNS_ROOT") or "~/.mn/runs"
    ).expanduser()
    return runs_root / blueprint_run_id


def _register_post_launch_hook(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
) -> None:
    script_path = (bundle_dir / POST_LAUNCH_SCRIPT).resolve()
    if not script_path.is_file():
        return

    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "post_launch.log"
    ready_file = run_dir / "pre_launch.ready"
    state_file = run_dir / "post_launch_state.json"
    hook_info = {
        "command": ["bash", str(script_path)],
        "script": str(script_path),
        "cwd": str(bundle_dir),
        "log": str(log_path),
        "run_id": blueprint_run_id,
        "bundle_dir": str(bundle_dir),
        "state_file": str(state_file),
        "pre_launch_ready_file": str(ready_file),
        "pre_launch_process_file": str(run_dir / "pre_launch_process.json"),
        "registered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "post_launch_hook.json").write_text(
        json.dumps(hook_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _start_pre_launch_hook(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
    config_overrides: Optional[dict[str, Any]] = None,
) -> subprocess.Popen[Any] | None:
    script_path = (bundle_dir / PRE_LAUNCH_SCRIPT).resolve()
    if not script_path.is_file():
        return None

    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    runs_root = run_dir.parent
    run_dir.mkdir(parents=True, exist_ok=True)
    ready_file = run_dir / "pre_launch.ready"
    try:
        ready_file.unlink()
    except FileNotFoundError:
        pass

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    config = _with_shared_run_store_config(config, blueprint_run_id, str(runs_root))
    runtime_env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )

    env = _openshell_env()
    env.update(runtime_env)
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    env.update(
        {
            "MN_RUN_ID": blueprint_run_id,
            "MN_RUN_DIR": str(run_dir),
            "MN_RUNS_ROOT": str(runs_root),
            "MN_BLUEPRINT_BUNDLE_DIR": str(bundle_dir),
            "MN_BLUEPRINT_CONFIG_JSON": json.dumps(config, sort_keys=True),
            "MN_PRE_LAUNCH_READY_FILE": str(ready_file),
            "MN_POST_LAUNCH_STATE_FILE": str(run_dir / "post_launch_state.json"),
        }
    )

    command = ["bash", str(script_path)]
    log_path = run_dir / "pre_launch.log"
    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                cwd=bundle_dir,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
    except OSError as exc:
        raise RuntimeError(f"Failed to start blueprint pre-launch hook: {exc}") from exc

    process_info = {
        "pid": process.pid,
        "process_group_id": process.pid,
        "command": command,
        "script": str(script_path),
        "log": str(log_path),
        "ready_file": str(ready_file),
        "run_id": blueprint_run_id,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "pre_launch_process.json").write_text(
        json.dumps(process_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    try:
        _wait_for_pre_launch_ready(run_dir, process, ready_file)
        _apply_pre_launch_ready_metadata(
            ready_file,
            env_overrides=env_overrides,
            config_overrides=config_overrides,
        )
    except Exception:
        _terminate_pre_launch_process(process, reason="pre_launch_failed")
        raise

    console.print(
        "[green]Blueprint pre-launch hook ready:[/green] scripts/pre-launch.sh"
    )
    return process


def _wait_for_pre_launch_ready(
    run_dir: Path, process: subprocess.Popen[Any], ready_file: Path
) -> None:
    try:
        timeout = max(float(os.getenv("MN_PRE_LAUNCH_TIMEOUT_SECONDS", "30")), 0)
    except ValueError:
        timeout = 30.0
    deadline = time.monotonic() + timeout
    while time.monotonic() <= deadline:
        if ready_file.exists():
            return
        poll = getattr(process, "poll", None)
        if callable(poll) and poll() is not None:
            raise RuntimeError(
                f"Blueprint pre-launch hook exited before becoming ready. See {run_dir / 'pre_launch.log'}."
            )
        time.sleep(0.1)
    raise RuntimeError(
        f"Blueprint pre-launch hook timed out after {timeout:g}s. See {run_dir / 'pre_launch.log'}."
    )


def _apply_pre_launch_ready_metadata(
    ready_file: Path,
    *,
    env_overrides: dict[str, str],
    config_overrides: dict[str, Any],
) -> None:
    raw = ready_file.read_text(encoding="utf-8").strip()
    if not raw or raw == "ready":
        return
    try:
        metadata = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(
            "Ignoring non-JSON pre-launch ready metadata from %s", ready_file
        )
        return
    if not isinstance(metadata, dict):
        return
    env_patch = metadata.get("env")
    if isinstance(env_patch, dict):
        env_overrides.update(
            {
                str(key): str(value)
                for key, value in env_patch.items()
                if value is not None
            }
        )
    config_patch = metadata.get("config") or metadata.get("config_overrides")
    if isinstance(config_patch, dict):
        merged = _deep_merge_dict(config_overrides, config_patch)
        config_overrides.clear()
        config_overrides.update(merged)


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def _terminate_pre_launch_process(
    process: subprocess.Popen[Any] | None, *, reason: str
) -> None:
    if process is None:
        return
    poll = getattr(process, "poll", None)
    if callable(poll) and poll() is not None:
        return
    pid = getattr(process, "pid", None)
    if not isinstance(pid, int):
        try:
            process.terminate()
        except OSError:
            pass
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except OSError:
        try:
            process.terminate()
        except OSError:
            pass
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pid, signal.SIGKILL)
        except OSError:
            try:
                process.kill()
            except OSError:
                pass
    logger.info("Stopped blueprint pre-launch hook pid=%s reason=%s", pid, reason)


def _write_local_web_ui_handle(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    if not isinstance(config, dict):
        return
    web_ui = config.get("web_ui")
    if not isinstance(web_ui, dict) or web_ui.get("enabled") is False:
        return

    dashboard = (
        web_ui.get("dashboard") if isinstance(web_ui.get("dashboard"), dict) else {}
    )
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    runs_root = Path(
        env_overrides.get("MN_RUNS_ROOT") or os.getenv("MN_RUNS_ROOT") or "~/.mn/runs"
    ).expanduser()
    run_dir = runs_root / blueprint_run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter == "gradio":
        return

    script_path = _web_ui_registration_script(bundle_dir, web_ui, output, dashboard)
    if script_path is not None:
        _launch_blueprint_web_ui_script(
            bundle_dir,
            script_path,
            blueprint_run_id,
            run_dir,
            runs_root,
            config,
            env_overrides=env_overrides,
        )
        return

    module_name = _web_ui_registration_module(web_ui, output, dashboard)
    if module_name is not None:
        _launch_blueprint_web_ui_module(
            bundle_dir,
            module_name,
            blueprint_run_id,
            run_dir,
            runs_root,
            config,
            env_overrides=env_overrides,
        )
        return

    custom_url = _web_ui_custom_url(output)
    if custom_url:
        _write_web_ui_handle(
            run_dir,
            {
                "adapter": str(output.get("adapter") or "custom"),
                "kind": "output",
                "url": custom_url,
                "title": str(
                    output.get("title") or identity.get("name") or bundle_dir.name
                ),
                "status": "external",
                "metadata": {
                    "blueprint_id": identity.get("blueprint_id"),
                    "run_id": blueprint_run_id,
                    "registered_by": "mn_cli",
                    "customer_managed": True,
                },
            },
        )
        return

    _write_static_web_ui_handle(bundle_dir, run_dir, blueprint_run_id, config)


def _start_background_event_relay_if_needed(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    job_id: str,
    run_dir: Path,
    final_status: str,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    if final_status in FINAL_STATUSES:
        return
    if not _is_live_manifest(manifest_dict):
        return
    if os.getenv("MN_RUN_BACKGROUND_EVENT_RELAY", "1").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    if not isinstance(web_ui, dict) or web_ui.get("enabled") is False:
        return

    max_seconds = _background_event_relay_max_seconds(config)
    poll_seconds = _background_event_relay_poll_seconds(config)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "event_relay.log"
    command = [
        sys.executable,
        "-m",
        "mn_blueprint_support.event_relay",
        "--job-id",
        job_id,
        "--run-dir",
        str(run_dir),
        "--poll-seconds",
        f"{poll_seconds:g}",
    ]
    if max_seconds is not None:
        command.extend(["--max-seconds", f"{max_seconds:g}"])

    env = os.environ.copy()
    env["MN_RUN_EVENT_RELAY_CHILD"] = "1"
    _inject_local_blueprint_support_pythonpath(env)
    with open(log_path, "a", encoding="utf-8") as relay_log:
        process = subprocess.Popen(
            command,
            stdout=relay_log,
            stderr=relay_log,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
            env=env,
        )

    relay_info = {
        "job_id": job_id,
        "pid": process.pid,
        "poll_seconds": poll_seconds,
        "max_seconds": max_seconds,
        "log_path": str(log_path),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "event_relay.json").write_text(
        json.dumps(relay_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    console.print(
        "[green]Live event relay:[/green] keeping the local dashboard stream updated in the background."
    )


def _is_live_manifest(manifest_dict: dict[str, Any]) -> bool:
    policies = (
        manifest_dict.get("policies")
        if isinstance(manifest_dict.get("policies"), dict)
        else {}
    )
    scheduler = policies.get("scheduler", {}) if isinstance(policies.get("scheduler"), dict) else {}
    job_type = str(
        policies.get("job_type")
        or scheduler.get("job_type")
        or manifest_dict.get("job_type")
        or manifest_dict.get("type")
        or "batch"
    ).lower()
    return job_type == "service" or policies.get("stream_mode") == "live"


def _background_event_relay_poll_seconds(config: dict[str, Any]) -> float:
    raw = os.getenv("MN_RUN_EVENT_RELAY_POLL_SECONDS")
    if raw is not None:
        try:
            return max(float(raw), 0.1)
        except ValueError:
            return 1.0

    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    try:
        return max(float(output.get("refresh_seconds", 1.0)), 0.1)
    except (TypeError, ValueError):
        return 1.0


def _background_event_relay_max_seconds(config: dict[str, Any]) -> float | None:
    raw = os.getenv("MN_RUN_EVENT_RELAY_MAX_SECONDS")
    if raw is not None:
        if raw.strip().lower() in {"", "0", "none", "infinity"}:
            return None
        try:
            return max(float(raw), 0.0)
        except ValueError:
            return None

    budgets = config.get("budgets") if isinstance(config.get("budgets"), dict) else {}
    try:
        return max(float(budgets.get("max_stream_duration_seconds", 3600)), 0.0)
    except (TypeError, ValueError):
        return 3600.0


def _web_ui_registration_script(
    bundle_dir: Path,
    web_ui: dict[str, Any],
    output: dict[str, Any],
    dashboard: dict[str, Any],
) -> Path | None:
    registration = (
        web_ui.get("registration")
        if isinstance(web_ui.get("registration"), dict)
        else {}
    )
    dashboard_registration = (
        dashboard.get("registration")
        if isinstance(dashboard.get("registration"), dict)
        else {}
    )
    for raw_value in (
        output.get("launch_script"),
        output.get("registration_script"),
        registration.get("script"),
        dashboard_registration.get("script"),
    ):
        script_path = _safe_bundle_file(bundle_dir, raw_value)
        if script_path is not None:
            return script_path
    return None


def _web_ui_registration_module(
    web_ui: dict[str, Any],
    output: dict[str, Any],
    dashboard: dict[str, Any],
) -> str | None:
    registration = (
        web_ui.get("registration")
        if isinstance(web_ui.get("registration"), dict)
        else {}
    )
    dashboard_registration = (
        dashboard.get("registration")
        if isinstance(dashboard.get("registration"), dict)
        else {}
    )
    for raw_value in (
        output.get("launch_module"),
        output.get("registration_module"),
        registration.get("module"),
        dashboard_registration.get("module"),
    ):
        module_name = _safe_python_module(raw_value)
        if module_name is not None:
            return module_name

    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter == "gradio" and output.get("auto_generate", True) is not False:
        return "mn_blueprint_support.gradio_dashboard"
    return None


def _safe_python_module(raw_value: Any) -> str | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    module_name = raw_value.strip()
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", module_name):
        return module_name
    return None


def _safe_bundle_file(bundle_dir: Path, raw_value: Any) -> Path | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    relative = Path(raw_value)
    if relative.is_absolute() or ".." in relative.parts:
        return None
    root = bundle_dir.resolve()
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root) or not candidate.is_file():
        return None
    return candidate


def _launch_blueprint_web_ui_script(
    bundle_dir: Path,
    script_path: Path,
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
) -> None:
    command = [
        sys.executable,
        str(script_path),
    ]
    _launch_blueprint_web_ui_command(
        bundle_dir,
        command,
        blueprint_run_id,
        run_dir,
        runs_root,
        config,
        env_overrides=env_overrides,
        script=str(script_path),
    )


def _launch_blueprint_web_ui_module(
    bundle_dir: Path,
    module_name: str,
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
) -> None:
    command = [
        sys.executable,
        "-m",
        module_name,
    ]
    _launch_blueprint_web_ui_command(
        bundle_dir,
        command,
        blueprint_run_id,
        run_dir,
        runs_root,
        config,
        env_overrides=env_overrides,
        module=module_name,
    )


def _launch_blueprint_web_ui_command(
    bundle_dir: Path,
    command_prefix: list[str],
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
    script: str | None = None,
    module: str | None = None,
) -> None:
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    host = _web_ui_bind_host(output, env_overrides)
    port = _web_ui_port(output, host=host)
    base_url = _web_ui_base_url(output, host, port)

    env = os.environ.copy()
    env.update(env_overrides)
    env.update(
        {
            "MN_RUN_ID": blueprint_run_id,
            "MN_RUN_DIR": str(run_dir),
            "MN_RUNS_ROOT": str(runs_root),
            "MN_BLUEPRINT_BUNDLE_DIR": str(bundle_dir),
            "MN_BLUEPRINT_CONFIG_JSON": json.dumps(config, sort_keys=True),
            "MN_BLUEPRINT_ID": str(identity.get("blueprint_id") or bundle_dir.name),
            "MN_BLUEPRINT_WEB_UI_HOST": host,
            "MN_BLUEPRINT_WEB_UI_PORT": str(port),
            "MN_BLUEPRINT_WEB_UI_BASE_URL": base_url,
        }
    )
    _inject_local_blueprint_support_pythonpath(env)

    command = command_prefix + [
        "--run-id",
        blueprint_run_id,
        "--run-dir",
        str(run_dir),
        "--runs-root",
        str(runs_root),
        "--bundle-dir",
        str(bundle_dir),
        "--host",
        host,
        "--port",
        str(port),
        "--base-url",
        base_url,
    ]
    log_path = run_dir / "web_ui.log"
    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                cwd=bundle_dir,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
        process_info = {
            "pid": process.pid,
            "command": command,
            "log": str(log_path),
            "blueprint_id": identity.get("blueprint_id"),
            "run_id": blueprint_run_id,
            "url": base_url,
        }
        if script:
            process_info["script"] = script
        if module:
            process_info["module"] = module
        (run_dir / "web_ui_process.json").write_text(
            json.dumps(process_info, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        _wait_for_blueprint_web_ui(run_dir, process)
    except OSError:
        logger.exception(
            "Failed to launch blueprint web UI for run_id=%s", blueprint_run_id
        )


def _inject_local_blueprint_support_pythonpath(env: dict[str, str]) -> None:
    repo_root = Path(
        os.getenv("MN_WORKSPACE_ROOT")
        or os.getenv("MIRROR_NEURON_WORKSPACE")
        or os.getenv("OTTERDESK_MIRROR_NEURON_WORKSPACE")
        or Path(__file__).resolve().parents[3]
    ).expanduser()
    support_src = repo_root / "mn-skills" / "blueprint_support_skill" / "src"
    if not support_src.is_dir():
        return
    current = env.get("PYTHONPATH")
    paths = [str(support_src)]
    if current:
        paths.append(current)
    env["PYTHONPATH"] = os.pathsep.join(paths)


def _web_ui_bind_host(output: dict[str, Any], env_overrides: dict[str, str]) -> str:
    for value in (
        env_overrides.get("MN_BLUEPRINT_WEB_UI_BIND_HOST"),
        env_overrides.get("MN_BLUEPRINT_WEB_UI_HOST"),
        os.getenv("MN_BLUEPRINT_WEB_UI_BIND_HOST"),
        os.getenv("MN_BLUEPRINT_WEB_UI_HOST"),
        output.get("host"),
    ):
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "127.0.0.1"


def _parse_web_ui_port(raw_value: Any, *, name: str) -> int | None:
    if raw_value in (None, ""):
        return None
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        raise RuntimeError(f"{name} must be a positive integer.") from None
    if parsed <= 0 or parsed > 65535:
        raise RuntimeError(f"{name} must be between 1 and 65535.")
    return parsed


def _web_ui_port_available(host: str, port: int) -> bool:
    bind_host = host if host and host not in {"::"} else "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((bind_host, port))
        except OSError:
            return False
    return True


def _web_ui_port_range_configured() -> bool:
    return (
        os.getenv("MN_BLUEPRINT_WEB_UI_PORT_START") not in (None, "")
        or os.getenv("MN_BLUEPRINT_WEB_UI_PORT_END") not in (None, "")
    )


def _web_ui_port_range() -> tuple[int, int]:
    start = _parse_web_ui_port(
        os.getenv(
            "MN_BLUEPRINT_WEB_UI_PORT_START", str(DEFAULT_BLUEPRINT_WEB_UI_PORT_START)
        ),
        name="MN_BLUEPRINT_WEB_UI_PORT_START",
    )
    end = _parse_web_ui_port(
        os.getenv(
            "MN_BLUEPRINT_WEB_UI_PORT_END", str(DEFAULT_BLUEPRINT_WEB_UI_PORT_END)
        ),
        name="MN_BLUEPRINT_WEB_UI_PORT_END",
    )
    assert start is not None and end is not None
    if end < start:
        raise RuntimeError(
            "MN_BLUEPRINT_WEB_UI_PORT_END must be greater than or equal to MN_BLUEPRINT_WEB_UI_PORT_START."
        )
    return start, end


def _ephemeral_web_ui_port(host: str) -> int:
    bind_host = host if host and host not in {"::"} else "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((bind_host, 0))
        return int(sock.getsockname()[1])


def _web_ui_port(output: dict[str, Any], *, host: str = "127.0.0.1") -> int:
    raw_port = output.get("port")
    explicit_port = _parse_web_ui_port(raw_port, name="web_ui.output.port")
    if explicit_port is not None:
        if not _web_ui_port_available(host, explicit_port):
            raise RuntimeError(
                f"Blueprint web UI port {explicit_port} is unavailable on {host}."
            )
        return explicit_port

    start, end = _web_ui_port_range()
    for port in range(start, end + 1):
        if _web_ui_port_available(host, port):
            return port
    if not _web_ui_port_range_configured():
        return _ephemeral_web_ui_port(host)
    raise RuntimeError(f"No available blueprint web UI port found in {start}-{end}.")


def _web_ui_base_url(output: dict[str, Any], host: str, port: int) -> str:
    configured_url = output.get("base_url") or os.getenv("MN_BLUEPRINT_WEB_UI_BASE_URL")
    if isinstance(configured_url, str) and configured_url.strip():
        return configured_url.rstrip("/")
    public_host = output.get("public_host") or os.getenv(
        "MN_BLUEPRINT_WEB_UI_PUBLIC_HOST"
    )
    if not isinstance(public_host, str) or not public_host.strip():
        public_host = "localhost" if host in {"127.0.0.1", "0.0.0.0", "::"} else host
    scheme = str(output.get("scheme") or "http")
    return f"{scheme}://{public_host}:{port}"


def _wait_for_blueprint_web_ui(run_dir: Path, process: subprocess.Popen[Any]) -> None:
    try:
        timeout = max(
            float(os.getenv("MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", "5")), 0
        )
    except ValueError:
        timeout = 5.0
    deadline = time.monotonic() + timeout
    handle_path = run_dir / "web_ui.json"
    while time.monotonic() < deadline:
        if handle_path.exists():
            return
        poll = getattr(process, "poll", None)
        if callable(poll) and poll() is not None:
            return
        time.sleep(0.1)


def _web_ui_custom_url(output: dict[str, Any]) -> str:
    adapter = str(output.get("adapter") or "").lower()
    if adapter != "custom":
        return ""
    url = output.get("custom_url") or output.get("url")
    return str(url).strip() if isinstance(url, str) else ""


def _write_static_web_ui_handle(
    bundle_dir: Path,
    run_dir: Path,
    blueprint_run_id: str,
    config: dict[str, Any],
) -> None:
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    dashboard = (
        web_ui.get("dashboard") if isinstance(web_ui.get("dashboard"), dict) else {}
    )
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter not in {"static_html", "html"} and not (
        output.get("path") or dashboard.get("path") or web_ui.get("path")
    ):
        return

    html_path = _safe_bundle_file(
        bundle_dir, output.get("path") or dashboard.get("path") or web_ui.get("path")
    )
    if html_path is None:
        return

    events_path = run_dir / "events.jsonl"
    query: dict[str, str] = {}
    video_source = _web_ui_video_source(config, bundle_dir)
    video_query_param = str(output.get("video_source_query_param") or "video")
    events_query_param = str(output.get("events_query_param") or "events")
    if video_source:
        query[video_query_param] = video_source
    query[events_query_param] = events_path.resolve().as_uri()
    url = html_path.resolve().as_uri()
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"

    _write_web_ui_handle(
        run_dir,
        {
            "adapter": "static_html",
            "kind": "output",
            "url": url,
            "title": str(
                output.get("title") or identity.get("name") or bundle_dir.name
            ),
            "path": str(html_path),
            "status": "available",
            "metadata": {
                "blueprint_id": identity.get("blueprint_id"),
                "run_id": blueprint_run_id,
                "events_path": str(events_path),
                "registered_by": "mn_cli",
                "launch_adapter": "blueprint_static_html",
            },
        },
    )


def _write_web_ui_handle(run_dir: Path, handle: dict[str, Any]) -> None:
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "web_ui.json").write_text(
            json.dumps(handle, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    except OSError:
        logger.exception(
            "Failed to write blueprint web UI handle for run_dir=%s", run_dir
        )


def _web_ui_video_source(config: dict[str, Any], bundle_dir: Path) -> str:
    video_source = (
        (config.get("video_source") or {})
        if isinstance(config.get("video_source"), dict)
        else {}
    ).get("uri")
    if not isinstance(video_source, str) or not video_source:
        dashboard = (
            (config.get("web_ui") or {}).get("dashboard")
            if isinstance(config.get("web_ui"), dict)
            else {}
        )
        video_source = (dashboard or {}).get("default_video_source") or ""
    if not isinstance(video_source, str) or not video_source:
        return ""
    if "://" in video_source:
        return video_source
    for candidate in (
        bundle_dir / "payloads" / "person_detector" / video_source,
        bundle_dir / "payloads" / "web_ui" / video_source,
        bundle_dir / video_source,
    ):
        if candidate.is_file():
            return candidate.resolve().as_uri()
    return video_source


def _workflow_progress_for_monitor(job_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
    manifest = _manifest_from_job_data(data)
    events: list[dict[str, Any]] = []
    try:
        for event_json in client.stream_events(job_id, follow=False):
            try:
                event = json.loads(event_json)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
    except Exception:
        logger.exception("Failed to load workflow events for monitor")
    try:
        return workflow_progress_snapshot(
            manifest,
            events,
            job=data.get("job") if isinstance(data.get("job"), dict) else {},
            summary=data.get("summary") if isinstance(data.get("summary"), dict) else {},
            job_id=job_id,
        )
    except Exception:
        logger.exception("Failed to build workflow progress for monitor")
        return None


def _manifest_from_job_data(data: dict[str, Any]) -> dict[str, Any]:
    job = data.get("job") if isinstance(data.get("job"), dict) else {}
    summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
    for candidate in (data.get("manifest"), job.get("manifest"), summary.get("manifest")):
        if isinstance(candidate, dict) and candidate:
            return candidate
    manifest_ref = job.get("manifest_ref") if isinstance(job.get("manifest_ref"), dict) else summary.get("manifest_ref")
    if isinstance(manifest_ref, dict):
        for raw_path in (
            manifest_ref.get("manifest_path"),
            Path(str(manifest_ref.get("job_path") or "")) / "manifest.json" if manifest_ref.get("job_path") else None,
        ):
            if not raw_path:
                continue
            try:
                path = Path(str(raw_path)).expanduser()
                if path.is_file():
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        return loaded
            except (OSError, json.JSONDecodeError):
                continue
    topology = job.get("runtime_topology") if isinstance(job.get("runtime_topology"), dict) else {}
    topology_nodes = topology.get("nodes") if isinstance(topology.get("nodes"), list) else []
    agents = topology_nodes or (data.get("agents") if isinstance(data.get("agents"), list) else [])
    nodes = []
    for index, agent in enumerate(agents):
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("agent_id") or agent.get("id") or agent.get("node_id") or f"agent_{index + 1}")
        nodes.append(
            {
                "node_id": agent_id,
                "agent_type": str(agent.get("agent_type") or agent.get("type") or "worker"),
                "role": str(agent.get("role") or agent.get("current_task") or agent.get("agent_type") or "worker"),
                "type": str(agent.get("node_type") or agent.get("type") or ""),
                "live": agent.get("live?", agent.get("live", False)),
                "config": {"llm_config": str(agent.get("model") or agent.get("llm_config") or "runtime")},
            }
        )
    job_type = str(job.get("job_type") or job.get("type") or summary.get("job_type") or summary.get("type") or "")
    policies = {"stream_mode": "live"} if job_type.lower() == "service" else {}
    return {
        "id": str(job.get("graph_id") or summary.get("graph_id") or job.get("job_id") or "job"),
        "name": str(job.get("job_name") or summary.get("job_name") or job.get("job_id") or "Job"),
        "description": str(summary.get("description") or job.get("description") or ""),
        "graph_id": str(job.get("graph_id") or summary.get("graph_id") or ""),
        "type": job_type,
        "job_type": job_type,
        "policies": policies,
        "nodes": nodes,
    }


def _live_monitor(job_id: str):
    import sys
    import select
    import time
    from rich.live import Live

    is_tty = sys.stdin.isatty()
    old_settings = None
    if is_tty:
        import tty
        import termios

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setcbreak(fd)

    class MonitorView:
        def __init__(self, state: JobMonitorState):
            self.data = None
            self.state = state

        def __rich__(self):
            if not self.data:
                from rich.panel import Panel

                return Panel("Connecting...", style="cyan")
            if "error" in self.data:
                from rich.panel import Panel

                return Panel(f"Error fetching job: {self.data['error']}", style="red")
            return generate_live_layout(job_id, self.data, state=self.state)

    final_status = "unknown"
    data = None
    monitor_state = JobMonitorState()
    view = MonitorView(monitor_state)

    try:
        with Live(
            view,
            refresh_per_second=12,
            console=console,
            screen=bool(is_tty and getattr(console, "is_terminal", False)),
            transient=bool(is_tty and getattr(console, "is_terminal", False)),
        ):
            while True:
                try:
                    job_json = client.get_job(job_id)
                    data = json.loads(job_json)
                    data["workflow_progress"] = _workflow_progress_for_monitor(job_id, data)
                except Exception as e:
                    data = {"error": str(e)}

                view.data = data

                if data and "error" not in data:
                    status = data.get("summary", {}).get("status", "unknown")
                    if status in ["completed", "failed", "cancelled"]:
                        final_status = status
                        break

                if is_tty:
                    i, o, e = select.select([sys.stdin], [], [], 0.5)
                    if i:
                        key = _read_monitor_key(sys.stdin, select)
                        agent_count = len(data.get("agents", [])) if isinstance(data.get("agents"), list) else 0
                        if not monitor_state.handle_key(key, agent_count):
                            break
                else:
                    time.sleep(0.5)
                    break

    except KeyboardInterrupt:
        pass
    finally:
        if is_tty and old_settings:
            import termios

            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    if final_status in ["completed", "failed", "cancelled"]:
        # Save results and print final summary
        fetch_and_save_results(job_id, data)
        log_dir = Path(f"/tmp/mn_{job_id}")
        panel = generate_summary_panel(job_id, final_status, log_dir)
        console.print(panel)
    else:
        console.print(f"\n[yellow]Exited live monitor for {job_id}[/yellow]")
        fetch_and_save_results(job_id, data)


def _read_monitor_key(stream, select_module) -> str:
    key = stream.read(1)
    if key != "\x1b":
        return key
    parts = [key]
    while True:
        ready, _, _ = select_module.select([stream], [], [], 0.01)
        if not ready:
            break
        parts.append(stream.read(1))
    return "".join(parts)


def monitor(job_id: str):
    """Stream live events for a job"""
    try:
        _live_monitor(job_id)
    except Exception as e:
        handle_cli_error(e, console, "monitor stream")


def result(job_id: str):
    """Fetch and save the final and progressive results for a job"""
    try:
        console.print(f"Fetching results for {job_id}...")
        fetch_and_save_results(job_id)

        log_dir = Path(f"/tmp/mn_{job_id}")
        res_file = log_dir / "result.txt"
        stream_file = log_dir / "result_stream.txt"

        details: list[tuple[str, Path]] = []
        if res_file.exists():
            details.append(("Final result", res_file))
        else:
            console.print(
                "[yellow]No final result found (job might not be completed).[/yellow]"
            )

        if stream_file.exists():
            details.append(("Stream results", stream_file))

        if details:
            print_success_confirmation(
                console,
                "Job result fetch",
                details=[("Job ID", job_id), *details],
            )

    except Exception as e:
        handle_cli_error(e, console, "fetch results")
