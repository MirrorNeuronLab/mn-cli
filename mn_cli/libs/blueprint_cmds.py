import os
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

import typer
from rich.table import Table
from mn_cli.shared import console, logger
from mn_cli.libs.run_cmds import run_bundle as _run_bundle

blueprint_app = typer.Typer(help="Manage and run MirrorNeuron blueprints")
DEFAULT_BLUEPRINT_REPO = "https://github.com/MirrorNeuronLab/mn-blueprints"


def _load_observability_api() -> tuple[Callable[..., list[dict[str, Any]]], Callable[..., dict[str, Any]], Callable[..., list[dict[str, Any]]]]:
    try:
        from mn_blueprint_support.observability import list_runs, load_run, read_run_events
    except ModuleNotFoundError:
        repo_root = Path(__file__).resolve().parents[3]
        support_src = repo_root / "mn-skills" / "blueprint_support_skill" / "src"
        if support_src.exists() and str(support_src) not in sys.path:
            sys.path.insert(0, str(support_src))
        try:
            from mn_blueprint_support.observability import list_runs, load_run, read_run_events
        except ModuleNotFoundError:
            console.print(
                "[red]Blueprint observability support is unavailable. "
                "Install the blueprint support package or run from the monorepo checkout.[/red]"
            )
            raise typer.Exit(1)
    return list_runs, load_run, read_run_events


def _load_web_ui_api() -> Callable[..., Any]:
    _load_observability_api()
    try:
        from mn_blueprint_support.web_ui import write_static_run_report
    except ModuleNotFoundError:
        console.print("[red]Blueprint web UI support is unavailable.[/red]")
        raise typer.Exit(1)
    return write_static_run_report


def _make_blueprint_run_id(blueprint_id: str) -> str:
    try:
        _load_observability_api()
        from mn_blueprint_support import make_run_id

        return make_run_id(blueprint_id)
    except Exception:
        import uuid

        return f"{blueprint_id}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:10]}"


def _ensure_blueprint_source(
    *,
    source: Optional[str],
    update: bool,
    offline: bool,
    revision: Optional[str],
) -> str:
    if source:
        source_path = Path(source).expanduser()
        if source_path.exists():
            storage_dir = source_path
        else:
            storage_dir = Path(os.path.expanduser("~/.mn/blueprints"))
            if offline:
                console.print(f"[red]Offline mode cannot clone missing source {source!r}.[/red]")
                raise typer.Exit(1)
            if not storage_dir.exists():
                _clone_blueprint_repo(source, storage_dir)
            elif update:
                _git_pull(storage_dir)
    else:
        storage_dir = Path(os.path.expanduser("~/.mn/blueprints"))
        if not storage_dir.exists():
            if offline:
                console.print(f"[red]Blueprint storage not found at {storage_dir}; offline mode cannot clone it.[/red]")
                raise typer.Exit(1)
            console.print(f"Initializing blueprint storage at {storage_dir}...")
            _clone_blueprint_repo(DEFAULT_BLUEPRINT_REPO, storage_dir)
        elif update:
            _git_pull(storage_dir)
        else:
            console.print(f"Using cached blueprint storage at {storage_dir}. Run 'mn blueprint update' or pass --update to refresh.")

    if revision:
        if offline:
            _git_checkout(storage_dir, revision)
        else:
            _git_fetch(storage_dir)
            _git_checkout(storage_dir, revision)
    return str(storage_dir)


def _clone_blueprint_repo(source: str, storage_dir: Path) -> None:
    storage_dir.parent.mkdir(parents=True, exist_ok=True)
    res = subprocess.run(["git", "clone", source, str(storage_dir)], capture_output=True, text=True)
    if res.returncode != 0:
        logger.error("Failed to clone blueprint repository: %s", res.stderr)
        console.print(f"[red]Failed to clone blueprint repository: {res.stderr}[/red]")
        raise typer.Exit(1)


def _git_pull(storage_dir: Path) -> None:
    console.print(f"Updating blueprint storage at {storage_dir}...")
    res = subprocess.run(["git", "-C", str(storage_dir), "pull", "--ff-only"], capture_output=True, text=True)
    if res.returncode != 0:
        logger.warning("Failed to update blueprint repository: %s", res.stderr)
        console.print(f"[yellow]Warning: Failed to update blueprint repository: {res.stderr}[/yellow]")


def _git_fetch(storage_dir: Path) -> None:
    subprocess.run(["git", "-C", str(storage_dir), "fetch", "--all", "--tags"], capture_output=True, text=True)


def _git_checkout(storage_dir: Path, revision: str) -> None:
    res = subprocess.run(["git", "-C", str(storage_dir), "checkout", revision], capture_output=True, text=True)
    if res.returncode != 0:
        console.print(f"[red]Failed to checkout blueprint revision {revision}: {res.stderr}[/red]")
        raise typer.Exit(1)


def _git_revision(storage_dir: Path) -> Optional[str]:
    res = subprocess.run(["git", "-C", str(storage_dir), "rev-parse", "HEAD"], capture_output=True, text=True)
    if res.returncode != 0:
        return None
    stdout = getattr(res, "stdout", "") or ""
    return str(stdout).strip() or None


def _is_python_source_blueprint(manifest: dict[str, Any]) -> bool:
    metadata = manifest.get("metadata") or {}
    return metadata.get("python_source_mode") is True or bool(metadata.get("python_workflow"))


def _load_blueprint_manifest(blueprint_dir: Path, target_name: str) -> dict[str, Any]:
    manifest_path = blueprint_dir / "manifest.json"
    if not manifest_path.exists():
        console.print(f"[red]Error: Blueprint '{target_name}' is missing manifest.json. Validation failed.[/red]")
        raise typer.Exit(1)
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.exception("Error parsing blueprint manifest")
        console.print(f"[red]Error parsing manifest.json for blueprint '{target_name}': {exc}[/red]")
        raise typer.Exit(1)


def _prepare_blueprint_bundle_for_run(
    blueprint_dir: Path,
    manifest: dict[str, Any],
    run_id: str,
) -> Path:
    if not _is_python_source_blueprint(manifest):
        return blueprint_dir

    generated_root = Path(os.path.expanduser("~/.mn/generated_blueprint_bundles"))
    output_dir = generated_root / run_id
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    console.print(f"Generating Python workflow bundle at {output_dir}...")
    try:
        output_dir = _generate_python_source_bundle(blueprint_dir, output_dir)
    except Exception as exc:
        logger.exception("Failed to generate Python workflow bundle")
        console.print(f"[red]Failed to generate Python workflow bundle: {exc}[/red]")
        raise typer.Exit(1)
    return output_dir


def _generate_python_source_bundle(blueprint_dir: Path, output_dir: Path) -> Path:
    _load_observability_api()
    from mn_blueprint_support.python_workflow_bundle import (
        generate_python_workflow_bundle_from_blueprint_dir,
    )

    return generate_python_workflow_bundle_from_blueprint_dir(
        blueprint_dir,
        output_dir,
    )


def _run_resolved_blueprint(
    *,
    blueprint_dir: Path,
    manifest: dict[str, Any],
    display_name: str,
    blueprint_id: str,
    run_id: Optional[str],
    revision: Optional[str],
    source_label: str,
    follow_seconds: Optional[float],
) -> None:
    shared_run_id = run_id or _make_blueprint_run_id(blueprint_id)
    console.print(f"[green]Blueprint '{display_name}' validated. Running...[/green]")
    console.print(f"Blueprint run_id: [bold green]{shared_run_id}[/bold green]")
    if revision:
        console.print(f"Blueprint revision: {revision}")
    bundle_path = _prepare_blueprint_bundle_for_run(blueprint_dir, manifest, shared_run_id)
    _run_bundle(
        str(bundle_path),
        follow_seconds=follow_seconds,
        env_overrides={
            "MN_RUN_ID": shared_run_id,
            "MN_BLUEPRINT_REVISION": revision or "",
        },
        submission_metadata={
            "blueprint_id": blueprint_id,
            "blueprint_run_id": shared_run_id,
            "blueprint_revision": revision,
            "blueprint_source": source_label,
        },
    )


def _run_local_blueprint_target(
    target: str,
    *,
    run_id: Optional[str],
    follow_seconds: Optional[float],
) -> bool:
    blueprint_dir = Path(target).expanduser()
    if not blueprint_dir.is_dir():
        return False

    manifest = _load_blueprint_manifest(blueprint_dir, target)
    metadata = manifest.get("metadata") or {}
    blueprint_id = str(metadata.get("blueprint_id") or manifest.get("graph_id") or blueprint_dir.name)
    resolved_revision = _git_revision(blueprint_dir)
    _run_resolved_blueprint(
        blueprint_dir=blueprint_dir,
        manifest=manifest,
        display_name=target,
        blueprint_id=blueprint_id,
        run_id=run_id,
        revision=resolved_revision,
        source_label=str(blueprint_dir),
        follow_seconds=follow_seconds,
    )
    return True


def _display(value: Any, *, max_length: int = 140) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        text = json.dumps(value, sort_keys=True)
    else:
        text = str(value)
    return text if len(text) <= max_length else text[: max_length - 1] + "…"


def _run_summary(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "Run ID": run.get("run_id"),
        "Blueprint": run.get("blueprint_id"),
        "Status": run.get("status"),
        "Started": run.get("started_at"),
        "Ended": run.get("ended_at"),
        "Run Directory": run.get("run_dir"),
    }


def _run_summary_with_job(record: dict[str, Any]) -> dict[str, Any]:
    summary = _run_summary(record.get("run") or record)
    job_id = _job_id(record)
    if job_id:
        summary["Job ID"] = job_id
    return summary


def _final_artifact(record: dict[str, Any]) -> dict[str, Any]:
    final_artifact = record.get("final_artifact") or {}
    if final_artifact:
        return final_artifact
    result = record.get("result") or {}
    nested = result.get("final_artifact") if isinstance(result, dict) else None
    return nested if isinstance(nested, dict) else {}


def _artifact_headline(artifact: dict[str, Any]) -> str:
    for key in ("recommended_action", "recommendation", "decision", "risk_level", "priority", "summary"):
        if key in artifact:
            return _display(artifact[key])
    return _display(artifact)


def _web_ui_url(record: dict[str, Any]) -> str:
    web_ui = record.get("web_ui") or {}
    return str(web_ui.get("url") or "")


def _job_id(record: dict[str, Any]) -> str:
    job = record.get("job") or {}
    return str(job.get("job_id") or "")


def _print_run_table(runs: list[dict[str, Any]]) -> None:
    if not runs:
        console.print("[yellow]No blueprint runs found.[/yellow]")
        return
    console.print(f"{'Run ID':<28} {'Job ID':<18} {'Status':<12} {'Ended':<25} {'Blueprint':<42} Web UI", markup=False)
    console.print(f"{'-' * 28} {'-' * 18} {'-' * 12} {'-' * 25} {'-' * 42} {'-' * 6}", markup=False)
    for run in runs:
        console.print(
            f"{_display(run.get('run_id')):<28} "
            f"{_display(_job_id(run), max_length=17):<18} "
            f"{_display(run.get('status')):<12} "
            f"{_display(run.get('ended_at'), max_length=24):<25} "
            f"{_display(run.get('blueprint_id'), max_length=42):<42} "
            f"{_display(_web_ui_url(run), max_length=70)}",
            markup=False,
        )


def _load_run_or_exit(run_id: str, runs_root: Optional[str]) -> dict[str, Any]:
    _, load_run, _ = _load_observability_api()
    try:
        return load_run(run_id, runs_root=runs_root)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)


def _print_events(events: list[dict[str, Any]]) -> None:
    for event in events:
        timestamp = event.get("timestamp") or event.get("time") or event.get("ts") or ""
        event_type = event.get("type") or event.get("event") or event.get("name") or "event"
        details = {
            key: value
            for key, value in event.items()
            if key not in {"timestamp", "time", "ts", "type", "event", "name"}
        }
        detail_text = json.dumps(details, sort_keys=True) if details else ""
        console.print(f"{_display(timestamp, max_length=36)} {_display(event_type, max_length=48)} {detail_text}", markup=False)


def _markdown_table(rows: list[tuple[str, Any]]) -> list[str]:
    output = ["| Field | Value |", "|---|---|"]
    for key, value in rows:
        escaped_value = _display(value).replace("|", "\\|")
        output.append(f"| {key} | {escaped_value} |")
    return output


def _render_markdown_export(record: dict[str, Any]) -> str:
    run = record.get("run") or {}
    artifact = _final_artifact(record)
    lines = [f"# Blueprint Run {run.get('run_id', 'unknown')}", ""]
    lines.extend(["## Summary", ""])
    lines.extend(_markdown_table(list(_run_summary_with_job(record).items())))
    lines.extend(["", "## Final Artifact", "", "```json", json.dumps(artifact, indent=2, sort_keys=True), "```"])
    web_ui = record.get("web_ui") or {}
    if web_ui:
        lines.extend(["", "## Web UI", ""])
        lines.extend(_markdown_table([("URL", web_ui.get("url")), ("Adapter", web_ui.get("adapter")), ("Status", web_ui.get("status"))]))
    lines.extend(["", "## Result", "", "```json", json.dumps(record.get("result") or {}, indent=2, sort_keys=True), "```"])
    lines.extend(["", "## Inputs", "", "```json", json.dumps(record.get("inputs") or {}, indent=2, sort_keys=True), "```"])
    lines.extend(["", "## Config", "", "```json", json.dumps(record.get("config") or {}, indent=2, sort_keys=True), "```"])
    lines.extend(["", "## Event Tail", "", "```json"])
    for event in (record.get("events") or [])[-20:]:
        lines.append(json.dumps(event, sort_keys=True))
    lines.extend(["```", ""])
    return "\n".join(lines)

@blueprint_app.command("list")
def blueprint_list():
    """List all available blueprints from the local storage shared with mn staff"""
    index_path = os.path.expanduser("~/.mn/blueprints/index.json")
    if not os.path.exists(index_path):
        console.print("[yellow]Blueprint storage not initialized. Run 'mn blueprint run <name>' to initialize.[/yellow]")
        return
    try:
        with open(index_path, "r") as f:
            blueprints = json.load(f)
        table = Table("ID", "Name", "Job Name", "Description")
        for bp in blueprints:
            table.add_row(
                bp.get("id", "N/A"),
                bp.get("name", "N/A"),
                bp.get("job_name", "N/A"),
                bp.get("description", "")
            )
        console.print(table)
    except Exception as e:
        logger.exception("Error reading blueprint index")
        console.print(f"[red]Error reading blueprints index: {e}[/red]")

@blueprint_app.command("run")
def blueprint_run(
    blueprint_path_name: str,
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Use a specific shared blueprint run ID."),
    source: Optional[str] = typer.Option(None, "--source", help="Use a local blueprint repo/path or clone URL instead of ~/.mn/blueprints."),
    update: bool = typer.Option(False, "--update", help="Update the cached blueprint repository before running."),
    offline: bool = typer.Option(False, "--offline", help="Use only local blueprint files; never clone, fetch, or pull."),
    revision: Optional[str] = typer.Option(None, "--revision", help="Checkout a specific git revision before running."),
    follow_seconds: Optional[float] = typer.Option(None, "--follow-seconds", help="Seconds to follow runtime events before detaching."),
):
    """Run a blueprint by name or local folder."""
    if _run_local_blueprint_target(
        blueprint_path_name,
        run_id=run_id,
        follow_seconds=follow_seconds,
    ):
        return

    storage_dir = _ensure_blueprint_source(source=source, update=update, offline=offline, revision=revision)
    
    index_path = os.path.join(storage_dir, "index.json")
    if not os.path.exists(index_path):
        console.print("[red]Error: index.json not found in blueprint storage.[/red]")
        raise typer.Exit(1)
        
    try:
        with open(index_path, "r") as f:
            blueprints = json.load(f)
    except Exception as e:
        logger.exception("Error parsing blueprint index")
        console.print(f"[red]Error parsing index.json: {e}[/red]")
        raise typer.Exit(1)
        
    target_bp = None
    for bp in blueprints:
        if bp.get("id") == blueprint_path_name or bp.get("path") == blueprint_path_name:
            target_bp = bp
            break
            
    if not target_bp:
        console.print(f"[red]Error: Blueprint '{blueprint_path_name}' not found in index.[/red]")
        raise typer.Exit(1)
        
    bp_path = os.path.join(storage_dir, target_bp.get("path"))
    
    manifest = _load_blueprint_manifest(Path(bp_path), blueprint_path_name)
    blueprint_id = str((manifest.get("metadata") or {}).get("blueprint_id") or target_bp.get("id") or blueprint_path_name)
    resolved_revision = _git_revision(Path(storage_dir)) or revision
    _run_resolved_blueprint(
        blueprint_dir=Path(bp_path),
        manifest=manifest,
        display_name=blueprint_path_name,
        blueprint_id=blueprint_id,
        run_id=run_id,
        revision=resolved_revision,
        source_label=str(storage_dir),
        follow_seconds=follow_seconds,
    )


@blueprint_app.command("install")
def blueprint_install(
    source: str = typer.Option(DEFAULT_BLUEPRINT_REPO, "--source", help="Blueprint repository URL or local path."),
    force: bool = typer.Option(False, "--force", help="Replace the existing cached repository."),
):
    """Install the blueprint library into ~/.mn/blueprints."""
    storage_dir = Path(os.path.expanduser("~/.mn/blueprints"))
    if storage_dir.exists() and not force:
        console.print(f"[yellow]Blueprint storage already exists at {storage_dir}. Use --force to replace it.[/yellow]")
        return
    if storage_dir.exists() and force:
        import shutil

        shutil.rmtree(storage_dir)
    _clone_blueprint_repo(source, storage_dir)
    console.print(f"[green]Installed blueprints at {storage_dir}.[/green]")


@blueprint_app.command("update")
def blueprint_update(
    source: Optional[str] = typer.Option(None, "--source", help="Cached blueprint repo/path to update."),
):
    """Update the cached blueprint library explicitly."""
    storage_dir = Path(source).expanduser() if source else Path(os.path.expanduser("~/.mn/blueprints"))
    if not storage_dir.exists():
        console.print(f"[red]Blueprint storage not found at {storage_dir}. Run 'mn blueprint install' first.[/red]")
        raise typer.Exit(1)
    _git_pull(storage_dir)


@blueprint_app.command("monitor")
def blueprint_monitor(
    follow: bool = typer.Option(False, "--follow", "-f", help="Refresh the run table until interrupted."),
    blueprint_id: Optional[str] = typer.Option(None, "--blueprint-id", help="Only show runs for one blueprint ID."),
    max_runs: int = typer.Option(20, "--max-runs", help="Maximum number of runs to display."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
    interval: float = typer.Option(2.0, "--interval", help="Refresh interval in seconds when --follow is enabled."),
):
    """Show recent blueprint runs from the shared run store."""
    list_runs, _, _ = _load_observability_api()
    try:
        while True:
            runs = list_runs(runs_root=runs_root, blueprint_id=blueprint_id, limit=max_runs)
            console.print(f"[bold]Blueprint runs[/bold] {time.strftime('%Y-%m-%d %H:%M:%S')}")
            _print_run_table(runs)
            if not follow:
                return
            time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped blueprint monitor.[/yellow]")


@blueprint_app.command("tail")
def blueprint_tail(
    run_id: str,
    lines: int = typer.Option(20, "--lines", "-n", help="Number of events to show."),
    follow: bool = typer.Option(False, "--follow", "-f", help="Continue printing new events until interrupted."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
    interval: float = typer.Option(1.0, "--interval", help="Polling interval in seconds when --follow is enabled."),
):
    """Print the event stream for one blueprint run."""
    _load_run_or_exit(run_id, runs_root)
    _, _, read_run_events = _load_observability_api()
    seen = 0
    try:
        while True:
            events = read_run_events(run_id, runs_root=runs_root)
            if not events:
                console.print(f"[yellow]No events found for run {run_id}.[/yellow]")
            elif seen == 0:
                selected = events[-lines:]
                _print_events(selected)
                seen = len(events)
            else:
                new_events = events[seen:]
                _print_events(new_events)
                seen = len(events)
            if not follow:
                return
            time.sleep(interval)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Stopped tailing {run_id}.[/yellow]")


@blueprint_app.command("compare")
def blueprint_compare(
    run_a: str,
    run_b: str,
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
):
    """Compare two blueprint runs from the shared run store."""
    record_a = _load_run_or_exit(run_a, runs_root)
    record_b = _load_run_or_exit(run_b, runs_root)
    summary_a = _run_summary(record_a.get("run") or {})
    summary_b = _run_summary(record_b.get("run") or {})
    artifact_a = _final_artifact(record_a)
    artifact_b = _final_artifact(record_b)

    table = Table("Field", run_a, run_b)
    for field in ("Blueprint", "Status", "Started", "Ended"):
        table.add_row(field, _display(summary_a.get(field)), _display(summary_b.get(field)))
    table.add_row("Event count", str(len(record_a.get("events") or [])), str(len(record_b.get("events") or [])))
    table.add_row("Final artifact", _artifact_headline(artifact_a), _artifact_headline(artifact_b))

    scalar_keys = sorted(set(artifact_a.keys()) | set(artifact_b.keys()))
    for key in scalar_keys:
        value_a = artifact_a.get(key)
        value_b = artifact_b.get(key)
        if isinstance(value_a, (dict, list)) or isinstance(value_b, (dict, list)):
            continue
        table.add_row(f"artifact.{key}", _display(value_a), _display(value_b))
    console.print(table)


@blueprint_app.command("export")
def blueprint_export(
    run_id: str,
    output_format: str = typer.Option("json", "--format", "-f", help="Export format: json, markdown, or html."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
):
    """Export one blueprint run as JSON, Markdown, or static HTML."""
    record = _load_run_or_exit(run_id, runs_root)
    normalized_format = output_format.lower().strip()
    if normalized_format == "json":
        console.print(json.dumps(record, indent=2, sort_keys=True), markup=False)
    elif normalized_format in {"markdown", "md"}:
        console.print(_render_markdown_export(record), markup=False)
    elif normalized_format in {"html", "static_html", "web"}:
        run_dir = (record.get("run") or {}).get("run_dir")
        if not run_dir:
            console.print("[red]Cannot write HTML export because this run has no run_dir.[/red]")
            raise typer.Exit(1)
        write_static_run_report = _load_web_ui_api()
        handle = write_static_run_report(record, run_dir)
        console.print(handle.url, markup=False)
    else:
        console.print("[red]Unsupported export format. Use 'json', 'markdown', or 'html'.[/red]")
        raise typer.Exit(1)
