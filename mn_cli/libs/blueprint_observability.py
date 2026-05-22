from __future__ import annotations

import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Optional

import typer

from mn_cli.shared import console


def load_observability_api() -> tuple[Callable[..., list[dict[str, Any]]], Callable[..., dict[str, Any]], Callable[..., list[dict[str, Any]]]]:
    _ensure_blueprint_support_path()
    try:
        from mn_blueprint_support.observability import list_runs, load_run, read_run_events
    except ModuleNotFoundError:
        console.print(
            "[red]Blueprint observability support is unavailable. "
            "Install the blueprint support package or run from the monorepo checkout.[/red]"
        )
        raise typer.Exit(1)
    return list_runs, load_run, read_run_events


def load_observability_tools() -> dict[str, Callable[..., Any]]:
    _ensure_blueprint_support_path()
    try:
        from mn_blueprint_support.observability import (
            acknowledge_human_notice,
            list_pending_human_requests,
            read_human_events,
            read_run_logs,
            read_run_resources,
            read_run_stream_records,
            record_human_response,
        )
    except ModuleNotFoundError:
        console.print(
            "[red]Blueprint observability support is unavailable. "
            "Install the blueprint support package or run from the monorepo checkout.[/red]"
        )
        raise typer.Exit(1)
    return {
        "acknowledge_human_notice": acknowledge_human_notice,
        "list_pending_human_requests": list_pending_human_requests,
        "read_human_events": read_human_events,
        "read_run_logs": read_run_logs,
        "read_run_resources": read_run_resources,
        "read_run_stream_records": read_run_stream_records,
        "record_human_response": record_human_response,
    }


def _ensure_blueprint_support_path() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    support_src = repo_root / "mn-skills" / "blueprint_support_skill" / "src"
    if support_src.exists() and str(support_src) not in sys.path:
        sys.path.insert(0, str(support_src))


def load_web_ui_api() -> Callable[..., Any]:
    load_observability_api()
    try:
        from mn_blueprint_support.web_ui import write_static_run_report
    except ModuleNotFoundError:
        console.print("[red]Blueprint web UI support is unavailable.[/red]")
        raise typer.Exit(1)
    return write_static_run_report


def make_blueprint_run_id(blueprint_id: str) -> str:
    try:
        load_observability_api()
        from mn_blueprint_support import make_run_id

        return make_run_id(blueprint_id)
    except Exception:
        import uuid

        return f"{blueprint_id}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:10]}"


def display(value: Any, *, max_length: int = 140) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        text = json.dumps(value, sort_keys=True)
    else:
        text = str(value)
    return text if len(text) <= max_length else text[: max_length - 1] + "…"


def run_summary(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "Run ID": run.get("run_id"),
        "Blueprint": run.get("blueprint_id"),
        "Status": run.get("status"),
        "Started": run.get("started_at"),
        "Ended": run.get("ended_at"),
        "Run Directory": run.get("run_dir"),
    }


def run_summary_with_job(record: dict[str, Any]) -> dict[str, Any]:
    summary = run_summary(record.get("run") or record)
    job_id = job_id_from_record(record)
    if job_id:
        summary["Job ID"] = job_id
    return summary


def final_artifact(record: dict[str, Any]) -> dict[str, Any]:
    artifact = record.get("final_artifact") or {}
    if artifact:
        return artifact
    result = record.get("result") or {}
    nested = result.get("final_artifact") if isinstance(result, dict) else None
    return nested if isinstance(nested, dict) else {}


def artifact_headline(artifact: dict[str, Any]) -> str:
    for key in ("recommended_action", "recommendation", "decision", "risk_level", "priority", "summary"):
        if key in artifact:
            return display(artifact[key])
    return display(artifact)


def web_ui_url(record: dict[str, Any]) -> str:
    web_ui = record.get("web_ui") or {}
    return str(web_ui.get("url") or "")


def job_id_from_record(record: dict[str, Any]) -> str:
    job = record.get("job") or {}
    return str(job.get("job_id") or "")


def load_run_or_exit(run_id: str, runs_root: Optional[str], *, include_observability: bool = False) -> dict[str, Any]:
    _, load_run, _ = load_observability_api()
    try:
        return load_run(run_id, runs_root=runs_root, include_observability=include_observability)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)


def markdown_table(rows: list[tuple[str, Any]]) -> list[str]:
    output = ["| Field | Value |", "|---|---|"]
    for key, value in rows:
        escaped_value = display(value).replace("|", "\\|")
        output.append(f"| {key} | {escaped_value} |")
    return output


def render_markdown_export(record: dict[str, Any]) -> str:
    run = record.get("run") or {}
    artifact = final_artifact(record)
    lines = [f"# Blueprint Run {run.get('run_id', 'unknown')}", ""]
    lines.extend(["## Summary", ""])
    lines.extend(markdown_table(list(run_summary_with_job(record).items())))
    lines.extend(["", "## Final Artifact", "", "```json", json.dumps(artifact, indent=2, sort_keys=True), "```"])
    web_ui = record.get("web_ui") or {}
    if web_ui:
        lines.extend(["", "## Web UI", ""])
        lines.extend(markdown_table([("URL", web_ui.get("url")), ("Adapter", web_ui.get("adapter")), ("Status", web_ui.get("status"))]))
    lines.extend(["", "## Result", "", "```json", json.dumps(record.get("result") or {}, indent=2, sort_keys=True), "```"])
    lines.extend(["", "## Inputs", "", "```json", json.dumps(record.get("inputs") or {}, indent=2, sort_keys=True), "```"])
    lines.extend(["", "## Config", "", "```json", json.dumps(record.get("config") or {}, indent=2, sort_keys=True), "```"])
    lines.extend(["", "## Event Tail", "", "```json"])
    for event in (record.get("events") or [])[-20:]:
        lines.append(json.dumps(event, sort_keys=True))
    lines.extend(["```", ""])
    return "\n".join(lines)


def print_events(events: list[dict[str, Any]]) -> None:
    for event in events:
        timestamp = event.get("timestamp") or event.get("time") or event.get("ts") or ""
        event_type = event.get("type") or event.get("event") or event.get("name") or "event"
        details = {
            key: value
            for key, value in event.items()
            if key not in {"timestamp", "time", "ts", "type", "event", "name"}
        }
        detail_text = json.dumps(details, sort_keys=True) if details else ""
        console.print(f"{display(timestamp, max_length=36)} {display(event_type, max_length=48)} {detail_text}", markup=False)
