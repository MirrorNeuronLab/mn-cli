import os
import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Optional

import typer
from rich.table import Table
from mn_cli.libs.blueprint_observability import (
    artifact_headline as _artifact_headline,
    display as _display,
    final_artifact as _final_artifact,
    job_id_from_record as _job_id,
    load_observability_api as _load_observability_api,
    load_run_or_exit as _load_run_or_exit,
    load_web_ui_api as _load_web_ui_api,
    make_blueprint_run_id as _make_blueprint_run_id,
    print_events as _print_events,
    render_markdown_export as _render_markdown_export,
    run_summary as _run_summary,
    web_ui_url as _web_ui_url,
)
from mn_cli.libs.blueprint_repository import (
    BLUEPRINT_REPO_CONTEXT_KEY,
    DEFAULT_BLUEPRINT_REPO,
    BlueprintIndexError,
    blueprint_cache_dir_for_repo as _blueprint_cache_dir_for_repo,
    blueprint_storage_dir_for_source as _blueprint_storage_dir_for_source,
    clone_blueprint_repo as _clone_blueprint_repo,
    context_blueprint_repo as _context_blueprint_repo,
    default_blueprint_storage_dir as _default_blueprint_storage_dir,
    ensure_blueprint_source as _ensure_blueprint_source,
    git_checkout as _git_checkout,
    git_fetch as _git_fetch,
    git_pull as _git_pull,
    git_revision as _git_revision,
    load_blueprint_index as _load_blueprint_index,
)
from mn_cli.shared import console, logger
from mn_cli.libs.run_cmds import run_bundle as _run_bundle

blueprint_app = typer.Typer(help="Manage and run MirrorNeuron blueprints")
_PATCH_COMPAT = (subprocess, _git_checkout, _git_fetch)


@blueprint_app.callback()
def blueprint_callback(
    ctx: typer.Context,
    blueprint_repo: Optional[str] = typer.Option(
        None,
        "--blueprint-repo",
        help="Use this blueprint repository URL/path instead of the default catalog.",
    ),
) -> None:
    ctx.obj = dict(ctx.obj or {})
    ctx.obj[BLUEPRINT_REPO_CONTEXT_KEY] = blueprint_repo

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


@blueprint_app.command("list")
def blueprint_list(ctx: typer.Context):
    """List all available blueprints from the local storage shared with mn staff"""
    blueprint_repo = _context_blueprint_repo(ctx)
    if blueprint_repo:
        storage_dir = Path(
            _ensure_blueprint_source(
                source=None,
                blueprint_repo=blueprint_repo,
                update=False,
                offline=False,
                revision=None,
            )
        )
        index_path = storage_dir / "index.json"
    else:
        index_path = Path(os.path.expanduser("~/.mn/blueprints/index.json"))
        if not index_path.exists():
            console.print("[yellow]Blueprint storage not initialized. Run 'mn blueprint run <name>' to initialize.[/yellow]")
            return
    try:
        blueprints = _load_blueprint_index(index_path)
        table = Table("ID", "Name", "Job Name", "Description")
        for bp in blueprints:
            table.add_row(
                bp.get("id", "N/A"),
                bp.get("name", "N/A"),
                bp.get("job_name", "N/A"),
                bp.get("description", "")
            )
        console.print(table)
    except BlueprintIndexError as e:
        logger.exception("Error reading blueprint index")
        console.print(f"[red]Error reading blueprints index: {e}[/red]")
        if blueprint_repo:
            raise typer.Exit(1)

@blueprint_app.command("run")
def blueprint_run(
    ctx: typer.Context,
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

    storage_dir = _ensure_blueprint_source(
        source=source,
        blueprint_repo=_context_blueprint_repo(ctx),
        update=update,
        offline=offline,
        revision=revision,
    )
    
    index_path = Path(storage_dir) / "index.json"
    try:
        blueprints = _load_blueprint_index(index_path, require_paths=True)
    except BlueprintIndexError as e:
        logger.exception("Error parsing blueprint index")
        console.print(f"[red]Error: {e}[/red]")
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
    ctx: typer.Context,
    source: Optional[str] = typer.Option(None, "--source", help="Blueprint repository URL or local path."),
    force: bool = typer.Option(False, "--force", help="Replace the existing cached repository."),
):
    """Install the blueprint library into ~/.mn/blueprints."""
    blueprint_repo = _context_blueprint_repo(ctx)
    repo_source = source or blueprint_repo or DEFAULT_BLUEPRINT_REPO
    storage_dir = (
        _blueprint_cache_dir_for_repo(repo_source)
        if source is None and blueprint_repo
        else _default_blueprint_storage_dir()
    )
    if storage_dir.exists() and not force:
        console.print(f"[yellow]Blueprint storage already exists at {storage_dir}. Use --force to replace it.[/yellow]")
        return
    if storage_dir.exists() and force:
        import shutil

        shutil.rmtree(storage_dir)
    _clone_blueprint_repo(repo_source, storage_dir)
    try:
        _load_blueprint_index(storage_dir / "index.json")
    except BlueprintIndexError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    console.print(f"[green]Installed blueprints at {storage_dir}.[/green]")


@blueprint_app.command("update")
def blueprint_update(
    ctx: typer.Context,
    source: Optional[str] = typer.Option(None, "--source", help="Cached blueprint repo/path to update."),
):
    """Update the cached blueprint library explicitly."""
    blueprint_repo = _context_blueprint_repo(ctx)
    if source:
        storage_dir = Path(source).expanduser()
    elif blueprint_repo:
        storage_dir = _blueprint_storage_dir_for_source(blueprint_repo)
    else:
        storage_dir = _default_blueprint_storage_dir()
    if not storage_dir.exists():
        console.print(f"[red]Blueprint storage not found at {storage_dir}. Run 'mn blueprint install' first.[/red]")
        raise typer.Exit(1)
    _git_pull(storage_dir)
    try:
        _load_blueprint_index(storage_dir / "index.json")
    except BlueprintIndexError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


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
