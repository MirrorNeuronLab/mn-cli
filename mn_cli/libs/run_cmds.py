import typer
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Annotated, Any, Optional
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
from mn_cli.libs.bundles import load_bundle_payloads
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
    manifest_nodes,
    prepare_manifest_for_submission,
    runtime_web_ui_support_payloads_for_manifest,
    run_mode_label as _run_mode_label,
    stage_blueprint_support_payloads_for_manifest,
    stage_skill_runtime_support_payloads_for_manifest,
    stage_local_input_payloads_for_manifest,
    stage_upload_path_payloads_for_manifest,
    with_shared_run_store_config as _with_shared_run_store_config,
)
from mn_cli.libs.skill_runtime import validate_skill_runtime_requirements
from mn_cli.libs.workflow_validation import (
    _is_workflow_manifest,
    _manifest_workflow_id,
    _validate_workflow_manifest_issues,
    _validate_workflow_schema_issues,
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
    run_hardware_requirements_validation,
    run_input_validation,
    run_model_validation,
    run_service_validation,
    validate_input_validation_spec_issues,
    validate_requirements_spec_issues,
    validate_resource_spec_issues,
    validate_service_spec_issues,
    workflow_progress_snapshot,
)
from mn_sdk.runtime_config import default_runs_root

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


def _is_vc_final_artifact(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    reports = value.get("company_reports") or value.get("companyReports")
    return value.get("type") == "vc_early_heuristic_analysis_reports" or (
        isinstance(reports, list) and any(isinstance(item, dict) and item for item in reports)
    )


def _extract_final_artifact(value: Any, depth: int = 0) -> Optional[dict[str, Any]]:
    if depth > 100 or value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for decoded in _json_values_from_text(text):
            found = _extract_final_artifact(decoded, depth + 1)
            if found:
                return found
        return None
    if isinstance(value, list):
        for item in value:
            found = _extract_final_artifact(item, depth + 1)
            if found:
                return found
        return None
    if not isinstance(value, dict):
        return None
    if _is_vc_final_artifact(value):
        return value
    explicit = value.get("final_artifact") or value.get("finalArtifact")
    if isinstance(explicit, dict) and explicit:
        return explicit
    for key in ("result", "output", "last_message", "lastMessage", "sandbox", "payload", "data", "logs"):
        found = _extract_final_artifact(value.get(key), depth + 1)
        if found:
            return found
    for item in value.values():
        found = _extract_final_artifact(item, depth + 1)
        if found:
            return found
    return None


def _json_values_from_text(text: str) -> list[Any]:
    decoder = json.JSONDecoder()
    values: list[Any] = []
    starts = [0] if text and text[0] in "{[" else []
    starts.extend(index for index, char in enumerate(text) if char in "{[" and index != 0)
    for start in starts[:50]:
        try:
            value, _end = decoder.raw_decode(text[start:])
        except Exception:
            continue
        values.append(value)
        if values:
            break
    return values


def _manifest_config(manifest: dict[str, Any]) -> dict[str, Any]:
    for node in manifest_nodes(manifest):
        environment = ((node.get("config") or {}).get("environment") or {})
        raw_config = environment.get("MN_BLUEPRINT_CONFIG_JSON")
        if isinstance(raw_config, str) and raw_config.strip():
            try:
                decoded = json.loads(raw_config)
            except Exception:
                continue
            if isinstance(decoded, dict):
                return decoded
    return {}


def _expand_user_output_path(value: str) -> Path:
    text = str(value or "").strip()
    home = (
        os.getenv("MN_OUTPUT_HOME")
        or os.getenv("MN_USER_HOME")
        or os.getenv("OTTERDESK_USER_HOME")
        or str(Path.home())
    )
    if text == "~":
        return Path(home).expanduser()
    if text.startswith("~/") or text.startswith("~\\"):
        return Path(home).expanduser() / text[2:]
    return Path(text).expanduser()


def _configured_output_folder(config: dict[str, Any]) -> Optional[Path]:
    payload = ((config.get("inputs") or {}).get("payload") or {})
    outputs = config.get("outputs") or {}
    for value in (
        payload.get("output_folder"),
        outputs.get("folder_path"),
        outputs.get("output_folder"),
    ):
        if isinstance(value, str) and value.strip():
            return _expand_user_output_path(value)
    return None


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return slug or "company"


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def _render_vc_analysis_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# {report.get('company_name') or 'Company'}",
        "",
        f"- Composite score: {report.get('composite_score', 'n/a')}",
        f"- Confidence: {report.get('confidence', 'n/a')}",
        "",
        "## Methods",
        "",
    ]
    methods = report.get("methods") if isinstance(report.get("methods"), dict) else {}
    for method_id, method in methods.items():
        if not isinstance(method, dict):
            continue
        summary = method.get("evidence_summary") or {}
        lines.extend([
            f"### {method_id.replace('_', ' ').title()}",
            "",
            f"- Status: {method.get('status', 'unknown')}",
            f"- Score: {method.get('score', 'n/a')}",
            f"- Evidence refs: {', '.join(method.get('evidence_refs') or []) or 'none'}",
            f"- Why: {summary.get('status_reason') or method.get('evidence_summary') or 'No method explanation provided.'}",
            "",
        ])
        missing = method.get("missing_evidence") or []
        if missing:
            lines.append(f"- Missing evidence: {'; '.join(map(str, missing))}")
            lines.append("")
    warnings = report.get("warnings") or []
    if warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")
    return "\n".join(lines)


def _write_vc_final_artifact_outputs(final_artifact: dict[str, Any], output_folder: Path) -> list[dict[str, str]]:
    reports = final_artifact.get("company_reports")
    if not isinstance(reports, list) or not reports:
        return []
    output_files: list[dict[str, str]] = []
    company_index = {
        "blueprint_id": "vc_assistant",
        "report_only": True,
        "generated_at": final_artifact.get("generated_at"),
        "companies": [
            {
                "company_name": report.get("company_name"),
                "company_slug": report.get("company_slug") or _safe_slug(report.get("company_name")),
                "composite_score": report.get("composite_score"),
                "confidence": report.get("confidence"),
                "method_count": report.get("method_count"),
            }
            for report in reports
            if isinstance(report, dict)
        ],
    }
    _write_json(output_folder / "final_artifact.json", final_artifact)
    _write_json(output_folder / "company_index.json", company_index)
    output_files.extend([
        {"kind": "final_artifact_json", "path": str(output_folder / "final_artifact.json")},
        {"kind": "company_index_json", "path": str(output_folder / "company_index.json")},
    ])
    diagnostic_artifacts = [
        ("action_ledger", "action_ledger_json", "action_ledger.json"),
        ("artifact_quality", "artifact_quality_json", "artifact_quality.json"),
        ("run_health", "run_health_json", "run_health.json"),
    ]
    for artifact_key, kind, filename in diagnostic_artifacts:
        artifact_value = final_artifact.get(artifact_key)
        if isinstance(artifact_value, dict):
            artifact_path = output_folder / filename
            _write_json(artifact_path, artifact_value)
            output_files.append({"kind": kind, "path": str(artifact_path)})
    index_lines = ["# VC Assistant Company Index", ""]
    for company in company_index["companies"]:
        index_lines.append(
            f"- {company.get('company_name')}: score {company.get('composite_score', 'n/a')}, confidence {company.get('confidence', 'n/a')}"
        )
    (output_folder / "company_index.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    (output_folder / "run_summary.md").write_text(
        str(final_artifact.get("executive_summary") or "VC Assistant run completed.") + "\n",
        encoding="utf-8",
    )
    output_files.extend([
        {"kind": "company_index_markdown", "path": str(output_folder / "company_index.md")},
        {"kind": "run_summary_markdown", "path": str(output_folder / "run_summary.md")},
    ])
    for report in reports:
        if not isinstance(report, dict):
            continue
        slug = report.get("company_slug") or _safe_slug(report.get("company_name"))
        company_dir = output_folder / slug
        _write_json(company_dir / "analysis.json", report)
        (company_dir / "analysis.md").write_text(_render_vc_analysis_markdown(report), encoding="utf-8")
        _write_json(company_dir / "method_scores.json", report.get("methods") or {})
        _write_json(company_dir / "evidence.json", report.get("evidence") or [])
        _write_json(company_dir / "warnings.json", report.get("warnings") or [])
        _write_json(company_dir / "research_sources.json", report.get("research_sources") or [])
        output_files.extend([
            {"kind": "analysis", "path": str(company_dir / "analysis.json")},
            {"kind": "analysis_markdown", "path": str(company_dir / "analysis.md")},
            {"kind": "method_scores", "path": str(company_dir / "method_scores.json")},
        ])
    return output_files


def _materialize_completed_blueprint_outputs(log_dir: Path, manifest: dict[str, Any]) -> None:
    result_path = log_dir / "result.txt"
    if not result_path.exists():
        return
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to decode blueprint result for output materialization: %s", result_path)
        return
    final_artifact = _extract_final_artifact(result)
    if not final_artifact:
        return
    config = _manifest_config(manifest)
    output_folder = _configured_output_folder(config)
    if output_folder is None:
        return
    try:
        materialized = _write_vc_final_artifact_outputs(final_artifact, output_folder)
        if materialized:
            console.print(f"[green]Materialized blueprint outputs:[/green] {output_folder}")
    except Exception:
        logger.exception("Failed to materialize blueprint outputs to %s", output_folder)


def _materialize_shared_storage_outputs(storage: dict[str, Any]) -> bool:
    if not isinstance(storage, dict):
        return False
    copied_any = False
    for spec in storage.get("output_copy") or []:
        if not isinstance(spec, dict):
            continue
        source_path = spec.get("source_path") or spec.get("source")
        target_path = spec.get("target_path") or spec.get("target")
        if not isinstance(source_path, str) or not isinstance(target_path, str):
            continue
        source = _host_shared_path_for_runtime_path(storage, source_path)
        target = Path(target_path).expanduser()
        if source is None or not source.exists():
            continue
        try:
            copied = _copy_output_path_to_host(source, target)
        except Exception:
            logger.exception("Failed to copy shared storage output %s to %s", source, target)
            copied = False
        if copied:
            copied_any = True
            console.print(f"[green]Materialized shared outputs:[/green] {target}")
    return copied_any


def _host_shared_path_for_runtime_path(storage: dict[str, Any], runtime_path: str) -> Optional[Path]:
    host_root = storage.get("host_root")
    runtime_root = storage.get("runtime_root")
    if not isinstance(host_root, str) or not isinstance(runtime_root, str):
        return None
    normalized_runtime_root = runtime_root.rstrip("/")
    normalized_runtime_path = runtime_path.rstrip("/")
    if normalized_runtime_path == normalized_runtime_root:
        return Path(host_root).expanduser()
    prefix = normalized_runtime_root + "/"
    if not normalized_runtime_path.startswith(prefix):
        return None
    return Path(host_root).expanduser() / normalized_runtime_path[len(prefix) :]


def _copy_output_path_to_host(source: Path, target: Path) -> bool:
    if source.is_dir():
        entries = list(source.iterdir())
        if not entries:
            return False
        target.mkdir(parents=True, exist_ok=True)
        for entry in entries:
            _copy_path_to_host(entry, target / entry.name)
        return True
    if source.is_file():
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target / source.name)
        return True
    return False


def _copy_path_to_host(source: Path, target: Path) -> None:
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
    elif source.is_file():
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


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

        skill_runtime_errors = validate_skill_runtime_requirements(bundle_dir, manifest)
        if skill_runtime_errors:
            report = make_validation_report(
                [
                    _legacy_validation_issue(error, source="manifest")
                    for error in skill_runtime_errors
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

        _validate_manifest_hardware_or_exit(
            manifest,
            output_format=output_format,
            allow_local_fallback=True,
        )

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


def _validate_manifest_hardware_or_exit(
    manifest: dict[str, Any],
    *,
    force: bool = False,
    output_format: str = "table",
    allow_local_fallback: bool = False,
) -> dict[str, Any]:
    result = run_hardware_requirements_validation(
        manifest,
        resource_report=lambda: _runtime_resource_report(allow_local_fallback=allow_local_fallback),
        force=force,
    )
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Runtime requirements need attention")
    raise typer.Exit(1)


def _runtime_resource_report(*, allow_local_fallback: bool = False) -> dict[str, Any]:
    try:
        decoded = json.loads(client.get_resource())
    except Exception:
        return {} if allow_local_fallback else {"nodes": []}
    return decoded if isinstance(decoded, dict) else ({} if allow_local_fallback else {"nodes": []})


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
    validation_manifest = _manifest_for_model_validation(manifest, config)
    result = run_model_validation(bundle_dir, validation_manifest, config=config, env=env)
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Model validation failed")
    raise typer.Exit(1)


def _manifest_for_model_validation(manifest: dict[str, Any], config: dict[str, Any] | None) -> dict[str, Any]:
    llm = config.get("llm") if isinstance(config, dict) and isinstance(config.get("llm"), dict) else {}
    mode = str(llm.get("mode") or "").strip().lower()
    provider = str(llm.get("provider") or "").strip().lower()
    if mode != "fake" and provider != "fake":
        return manifest
    filtered = json.loads(json.dumps(manifest))
    runtime = filtered.get("runtime") if isinstance(filtered.get("runtime"), dict) else None
    models = runtime.get("models") if isinstance(runtime, dict) and isinstance(runtime.get("models"), dict) else None
    if isinstance(models, dict):
        runtime["models"] = {
            name: entry
            for name, entry in models.items()
            if not isinstance(entry, dict)
            or str(entry.get("provider") or entry.get("mode") or "").strip().lower()
            not in {"", "docker_model_runner", "docker-model-runner", "dmr"}
        }
    return filtered


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
    validation["skipped_checks"] = ["services", "models", "input_validation", "soft_requirements"]


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


def _load_bundle_manifest(bundle_path: str) -> tuple[Path, Path, dict[str, Any]]:
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
        manifest_dict = json.load(f)
    return bundle_dir, manifest_file, manifest_dict


def _configure_bundle_if_required(
    bundle_dir: Path,
    manifest_file: Path,
    manifest_dict: dict[str, Any],
) -> dict[str, Any]:
    if manifest_dict.get("require_config") is not True:
        return manifest_dict

    config_script = bundle_dir / "config.py"
    if not config_script.exists():
        console.print(
            "[red]Bundle requires configuration, but config.py was not found.[/red]"
        )
        raise typer.Exit(1)

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

    with open(manifest_file, "r") as f:
        return json.load(f)


def _stage_bundle_payloads(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    *,
    web_ui: bool,
) -> dict[str, bytes]:
    payloads = load_bundle_payloads(bundle_dir)
    stage_upload_path_payloads_for_manifest(manifest_dict, payloads, bundle_dir=bundle_dir)
    if web_ui:
        payloads.update(runtime_web_ui_support_payloads_for_manifest(manifest_dict))
    stage_blueprint_support_payloads_for_manifest(manifest_dict, payloads, bundle_dir=bundle_dir)
    stage_skill_runtime_support_payloads_for_manifest(manifest_dict, payloads, bundle_dir=bundle_dir)
    return payloads


def _create_schedule_for_bundle(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    payloads: dict[str, bytes],
    schedule_attrs: dict[str, Any],
) -> None:
    stage_local_input_payloads_for_manifest(manifest_dict, payloads, bundle_dir=bundle_dir)
    promote_large_payloads_to_blob_refs(manifest_dict, payloads)
    manifest = json.dumps(manifest_dict)
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


def _cleanup_pre_launch_artifacts(
    process: subprocess.Popen[Any] | None,
    run_dir: Path | None,
    *,
    reason: str,
) -> None:
    _terminate_pre_launch_process(process, reason=reason)
    if run_dir is not None:
        cleanup_blueprint_host_hooks(
            run_dir,
            dry_run=False,
            summary={"process_removed": [], "process_skipped": [], "errors": []},
            reason=reason,
        )


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
        bundle_dir, manifest_file, manifest_dict = _load_bundle_manifest(bundle_path)
        submitted_bundle_dir = bundle_dir
        manifest_dict = _configure_bundle_if_required(
            bundle_dir,
            manifest_file,
            manifest_dict,
        )

        _ensure_local_run_store_identity(
            bundle_dir,
            manifest_dict,
            env_overrides,
            submission_metadata,
            config_overrides=config_overrides,
        )
        _validate_manifest_hardware_or_exit(
            manifest_dict,
            force=force,
            allow_local_fallback=False,
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
                "[yellow]Validation skipped because --force was provided; service checks, model checks, input checks, and non-hard runtime requirements will be bypassed for this run.[/yellow]"
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

        payloads = _stage_bundle_payloads(bundle_dir, manifest_dict, web_ui=web_ui)

        schedule_attrs = _run_schedule_attrs(auto_schedule=auto_schedule, schedule=schedule)
        if schedule_attrs is not None:
            submitted_manifest = manifest_dict
            _create_schedule_for_bundle(
                bundle_dir,
                manifest_dict,
                payloads,
                schedule_attrs,
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
        if final_status in FINAL_STATUSES:
            materialized_shared = _materialize_shared_storage_outputs(prepared_submission.metadata)
            if not materialized_shared:
                _materialize_completed_blueprint_outputs(log_writer.log_dir, manifest_dict)
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
        _cleanup_pre_launch_artifacts(
            pre_launch_process,
            pre_launch_run_dir,
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
        _cleanup_pre_launch_artifacts(
            pre_launch_process,
            pre_launch_run_dir,
            reason="launch_interrupted",
        )
        raise typer.Exit(130)
    except Exception as e:
        _cleanup_pre_launch_artifacts(
            pre_launch_process,
            pre_launch_run_dir,
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
    configured = env_overrides.get("MN_RUNS_ROOT")
    runs_root = Path(configured).expanduser() if configured else default_runs_root()
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
    configured = env_overrides.get("MN_RUNS_ROOT")
    runs_root = Path(configured).expanduser() if configured else default_runs_root()
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
