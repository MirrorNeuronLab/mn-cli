import os
import json
import shutil
import subprocess
import sys
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
    load_observability_tools as _load_observability_tools,
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
from mn_cli.libs.blueprint_resources import (
    cleanup_blueprint_resources as _cleanup_blueprint_resources,
    default_bundle_cache_dir as _default_bundle_cache_dir,
    default_generated_bundles_dir as _default_generated_bundles_dir,
    default_python_envs_dir as _default_python_envs_dir,
    default_runs_root as _default_runs_root,
)
from mn_cli.shared import console, logger
from mn_cli.libs.run_cmds import run_bundle as _run_bundle
from mn_cli.libs.run_manifest import load_blueprint_config as _load_blueprint_config

blueprint_app = typer.Typer(help="Manage and run MirrorNeuron blueprints")
human_app = typer.Typer(help="Inspect and respond to human collaboration events")
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

    generated_root = _default_generated_bundles_dir()
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
    force: bool,
) -> None:
    shared_run_id = run_id or _make_blueprint_run_id(blueprint_id)
    console.print(f"[green]Blueprint '{display_name}' validated. Running...[/green]")
    console.print(f"Blueprint run_id: [bold green]{shared_run_id}[/bold green]")
    if revision:
        console.print(f"Blueprint revision: {revision}")
    bundle_path = _prepare_blueprint_bundle_for_run(blueprint_dir, manifest, shared_run_id)
    config_overrides = _collect_init_config_review_overrides(bundle_path, manifest)
    _run_bundle(
        str(bundle_path),
        follow_seconds=follow_seconds,
        env_overrides={
            "MN_RUN_ID": shared_run_id,
            "MN_BLUEPRINT_ID": blueprint_id,
            "MN_BLUEPRINT_REVISION": revision or "",
        },
        submission_metadata={
            "blueprint_id": blueprint_id,
            "blueprint_run_id": shared_run_id,
            "blueprint_revision": revision,
            "blueprint_source": source_label,
        },
        config_overrides=config_overrides,
        force=force,
    )


def _reject_local_blueprint_path(target: str) -> None:
    blueprint_dir = Path(target).expanduser()
    if not blueprint_dir.exists():
        return
    console.print("[red]Error: 'mn blueprint run' accepts catalog blueprint names only.[/red]")
    console.print(f"Use [bold]mn run {blueprint_dir}[/bold] to run a local blueprint folder.")
    raise typer.Exit(1)


def _collect_init_config_review_overrides(
    bundle_path: Path,
    manifest: dict[str, Any],
) -> dict[str, Any] | None:
    review = _manifest_init_config_review(manifest)
    if not isinstance(review, dict):
        return None
    fields = review.get("fields")
    if not isinstance(fields, list) or not fields:
        return None
    if _env_flag("MN_BLUEPRINT_SKIP_INIT_CONFIG_REVIEW"):
        return None
    if not sys.stdin.isatty():
        if review.get("required") is True:
            console.print("[yellow]Blueprint config review requested; keeping current config in this non-interactive run.[/yellow]")
        return None

    config = _load_blueprint_config(bundle_path) or {}
    overrides: dict[str, Any] = {}
    console.print("[bold]Review blueprint config before launch[/bold]")
    instruction = review.get("instruction")
    if isinstance(instruction, str) and instruction.strip():
        console.print(instruction.strip())

    for raw_field in fields:
        if not isinstance(raw_field, dict):
            continue
        path = raw_field.get("path")
        if not isinstance(path, str) or not path.strip():
            continue
        path = path.strip()
        label = str(raw_field.get("label") or path)
        description = raw_field.get("description")
        current = _config_path_get(config, path)
        fallback = raw_field.get("default")
        default_value = current if current is not None else fallback
        if isinstance(description, str) and description.strip():
            console.print(f"{label}: {description.strip()}")
        if default_value is None:
            response = typer.prompt(label, default="", show_default=False)
            if response == "":
                continue
        else:
            response = typer.prompt(label, default=str(default_value), show_default=True)
        parsed = _parse_review_value(response, default_value)
        if parsed != current:
            _config_path_set(overrides, path, parsed)

    return overrides or None


def _manifest_init_config_review(manifest: dict[str, Any]) -> Any:
    if "init_config_review" in manifest:
        return manifest.get("init_config_review")
    metadata = manifest.get("metadata")
    if isinstance(metadata, dict):
        return metadata.get("init_config_review")
    return None


def _config_path_get(config: dict[str, Any], dotted_path: str) -> Any:
    cursor: Any = config
    for part in dotted_path.split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            return None
        cursor = cursor[part]
    return cursor


def _config_path_set(config: dict[str, Any], dotted_path: str, value: Any) -> None:
    cursor = config
    parts = [part for part in dotted_path.split(".") if part]
    for part in parts[:-1]:
        next_value = cursor.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            cursor[part] = next_value
        cursor = next_value
    if parts:
        cursor[parts[-1]] = value


def _parse_review_value(value: str, default_value: Any) -> Any:
    if isinstance(default_value, bool):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    if isinstance(default_value, int) and not isinstance(default_value, bool):
        try:
            return int(value)
        except ValueError:
            return value
    if isinstance(default_value, float):
        try:
            return float(value)
        except ValueError:
            return value
    if isinstance(default_value, (dict, list)):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _env_flag(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


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


def _print_log_records(records: list[dict[str, Any]]) -> None:
    for record in records:
        timestamp = _display(record.get("ts") or record.get("timestamp"), max_length=28)
        level = _display(record.get("level"), max_length=8)
        component = _display(record.get("component"), max_length=28)
        message = _display(record.get("message"), max_length=160)
        console.print(f"{timestamp:<28} {level:<8} {component:<28} {message}", markup=False)


def _observability_cursor(record: dict[str, Any]) -> str:
    return str(record.get("id") or f"{record.get('channel','')}:{record.get('type','')}:{record.get('ts') or record.get('timestamp','')}:{record.get('message','')}")


def _duration_seconds(value: str) -> float:
    text = str(value).strip().lower()
    if not text:
        return 0
    unit = text[-1]
    number_text = text[:-1] if unit.isalpha() else text
    try:
        number = float(number_text)
    except ValueError:
        raise typer.BadParameter(f"invalid duration: {value}")
    if unit == "s" or not unit.isalpha():
        return number
    if unit == "m":
        return number * 60
    if unit == "h":
        return number * 3600
    if unit == "d":
        return number * 86400
    raise typer.BadParameter(f"unsupported duration unit: {unit}")


def _print_resource_summary(summary: dict[str, Any]) -> None:
    table = Table("Start", "Samples", "CPU avg/max", "Memory avg/max MB", "GPU avg/max", "LLM tokens", "LLM calls")
    for bucket in summary.get("buckets") or []:
        if not bucket.get("sample_count") and not (bucket.get("llm") or {}).get("total_tokens"):
            continue
        llm = bucket.get("llm") or {}
        table.add_row(
            _display(bucket.get("start"), max_length=19),
            str(bucket.get("sample_count", 0)),
            f"{_display(bucket.get('cpu_pct_avg'))}/{_display(bucket.get('cpu_pct_max'))}",
            f"{_display(bucket.get('memory_rss_mb_avg'))}/{_display(bucket.get('memory_rss_mb_max'))}",
            f"{_display(bucket.get('gpu_util_pct_avg'))}/{_display(bucket.get('gpu_util_pct_max'))}",
            str(llm.get("total_tokens", 0)),
            str(llm.get("calls", 0)),
        )
    console.print(table)


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
    force: bool = typer.Option(False, "--force", help="Run even if blueprint input validation or runtime requirements fail."),
):
    """Run a blueprint by catalog name."""
    _reject_local_blueprint_path(blueprint_path_name)

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
        force=force,
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
    before_ids = _blueprint_ids_from_storage(storage_dir)
    _git_pull(storage_dir)
    try:
        _load_blueprint_index(storage_dir / "index.json")
    except BlueprintIndexError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    after_ids = _blueprint_ids_from_storage(storage_dir)
    removed_ids = before_ids - after_ids
    cleanup_summary = _cleanup_catalog_resources(
        removed_ids=removed_ids,
        active_ids=after_ids,
        include_docker=True,
        dry_run=False,
    )
    _print_cleanup_summary(cleanup_summary)


@blueprint_app.command("cleanup")
def blueprint_cleanup(
    ctx: typer.Context,
    blueprint_id: Optional[str] = typer.Option(None, "--blueprint-id", help="Clean resources for one blueprint ID."),
    source: Optional[str] = typer.Option(None, "--source", help="Blueprint storage path used to decide which resources are dead."),
    python_envs_dir: Optional[str] = typer.Option(None, "--python-envs-dir", help="Override the blueprint Python environment cache root."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the shared blueprint run store root."),
    generated_bundles_dir: Optional[str] = typer.Option(None, "--generated-bundles-dir", help="Override the generated Python workflow bundle cache root."),
    bundle_cache_dir: Optional[str] = typer.Option(None, "--bundle-cache-dir", help="Override the MirrorNeuron local bundle cache root."),
    include_files: bool = typer.Option(True, "--files/--no-files", help="Also remove blueprint-owned run records, generated bundles, and local bundle cache entries."),
    include_docker: bool = typer.Option(True, "--docker/--no-docker", help="Also remove Docker resources labelled for removed blueprints."),
    include_dead: bool = typer.Option(True, "--dead/--no-dead", help="Remove stale incomplete resources and resources for blueprints no longer in storage."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be removed without deleting anything."),
):
    """Clean blueprint-owned Python envs, ~/.mn files, Docker resources, and stale leftovers."""
    active_ids: set[str] = set()
    explicit_ids = {blueprint_id} if blueprint_id else set()
    if include_dead and not explicit_ids:
        storage_dir = _resolve_blueprint_storage_for_cleanup(ctx, source)
        active_ids = _blueprint_ids_from_storage(storage_dir)
        if not active_ids:
            console.print(f"[yellow]No readable blueprint index found at {storage_dir}; only stale incomplete resources will be cleaned.[/yellow]")

    summary = _cleanup_blueprint_resources(
        blueprint_ids=explicit_ids,
        active_blueprint_ids=active_ids,
        python_envs_dir=Path(python_envs_dir).expanduser() if python_envs_dir else _default_python_envs_dir(),
        runs_root=Path(runs_root).expanduser() if runs_root else _default_runs_root(),
        generated_bundles_dir=Path(generated_bundles_dir).expanduser() if generated_bundles_dir else _default_generated_bundles_dir(),
        bundle_cache_dir=Path(bundle_cache_dir).expanduser() if bundle_cache_dir else _default_bundle_cache_dir(),
        include_dead=include_dead,
        include_docker=include_docker,
        include_files=include_files,
        dry_run=dry_run,
    )
    _print_cleanup_summary(summary)


@blueprint_app.command("uninstall")
def blueprint_uninstall(
    ctx: typer.Context,
    source: Optional[str] = typer.Option(None, "--source", help="Cached blueprint storage path to remove."),
    keep_resources: bool = typer.Option(False, "--keep-resources", help="Remove blueprint files but keep cached runtime resources."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be removed without deleting anything."),
):
    """Remove cached blueprint storage and its owned runtime resources."""
    storage_dir = _resolve_blueprint_storage_for_cleanup(ctx, source)
    blueprint_ids = _blueprint_ids_from_storage(storage_dir)
    if not storage_dir.exists():
        console.print(f"[yellow]Blueprint storage not found at {storage_dir}.[/yellow]")
    elif dry_run:
        console.print(f"Would remove blueprint storage at {storage_dir}.")
    else:
        shutil.rmtree(storage_dir)
        console.print(f"[green]Removed blueprint storage at {storage_dir}.[/green]")

    if keep_resources:
        return

    summary = _cleanup_blueprint_resources(
        blueprint_ids=blueprint_ids,
        active_blueprint_ids=set(),
        include_dead=True,
        include_docker=True,
        include_files=True,
        dry_run=dry_run,
    )
    _print_cleanup_summary(summary)


def _resolve_blueprint_storage_for_cleanup(ctx: typer.Context, source: Optional[str]) -> Path:
    if source:
        return Path(source).expanduser()
    blueprint_repo = _context_blueprint_repo(ctx)
    if blueprint_repo:
        return _blueprint_storage_dir_for_source(blueprint_repo)
    return _default_blueprint_storage_dir()


def _blueprint_ids_from_storage(storage_dir: Path) -> set[str]:
    index_path = storage_dir / "index.json"
    if not index_path.exists():
        return set()
    try:
        entries = _load_blueprint_index(index_path)
    except BlueprintIndexError:
        return set()
    ids: set[str] = set()
    for entry in entries:
        blueprint_id = entry.get("id")
        if isinstance(blueprint_id, str) and blueprint_id.strip():
            ids.add(blueprint_id.strip())
            continue
        path = entry.get("path")
        if isinstance(path, str) and path.strip():
            manifest_path = storage_dir / path / "manifest.json"
            if manifest_path.exists():
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                metadata = manifest.get("metadata") if isinstance(manifest, dict) else {}
                manifest_blueprint_id = metadata.get("blueprint_id") if isinstance(metadata, dict) else None
                if isinstance(manifest_blueprint_id, str) and manifest_blueprint_id.strip():
                    ids.add(manifest_blueprint_id.strip())
    return ids


def _cleanup_catalog_resources(
    *,
    removed_ids: set[str],
    active_ids: set[str],
    include_docker: bool,
    dry_run: bool,
) -> dict[str, Any] | None:
    if not removed_ids and not active_ids:
        return None
    try:
        return _cleanup_blueprint_resources(
            blueprint_ids=removed_ids,
            active_blueprint_ids=active_ids,
            include_dead=True,
            include_docker=include_docker,
            dry_run=dry_run,
        )
    except Exception as exc:
        logger.exception("Failed to clean blueprint resources")
        return {"errors": [str(exc)], "dry_run": dry_run}


def _print_cleanup_summary(summary: dict[str, Any] | None) -> None:
    if not summary:
        return
    python_removed = summary.get("python_removed") or []
    run_removed = summary.get("run_removed") or []
    generated_removed = summary.get("generated_removed") or []
    bundle_removed = summary.get("bundle_removed") or []
    docker_removed = summary.get("docker_removed") or []
    process_removed = summary.get("process_removed") or []
    errors = summary.get("errors") or []
    dry_run = bool(summary.get("dry_run"))
    verb = "Would remove" if dry_run else "Removed"
    if python_removed or run_removed or generated_removed or bundle_removed or docker_removed or process_removed:
        console.print(
            f"[green]{verb} {len(python_removed)} Python env resource(s), "
            f"{len(run_removed)} run record(s), {len(generated_removed)} generated bundle(s), "
            f"{len(bundle_removed)} bundle cache resource(s), {len(docker_removed)} Docker resource(s), "
            f"and {len(process_removed)} web UI process(es).[/green]"
        )
    else:
        console.print("[green]No blueprint runtime resources needed cleanup.[/green]")
    for error in errors:
        console.print(f"[yellow]Cleanup warning: {error}[/yellow]")


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
    seen_ids: set[str] = set()
    last_ts: str | None = None
    poll_interval = max(float(interval), 1.0)
    try:
        while True:
            events = read_run_events(
                run_id,
                runs_root=runs_root,
                limit=max(lines, 1),
                since=last_ts if seen_ids else None,
            )
            if not events:
                console.print(f"[yellow]No events found for run {run_id}.[/yellow]")
            else:
                selected = []
                for event in events:
                    cursor = _observability_cursor(event)
                    if cursor in seen_ids:
                        continue
                    selected.append(event)
                    seen_ids.add(cursor)
                    last_ts = str(event.get("ts") or event.get("timestamp") or last_ts or "")
                _print_events(selected)
            if not follow:
                return
            time.sleep(poll_interval)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Stopped tailing {run_id}.[/yellow]")


@blueprint_app.command("logs")
def blueprint_logs(
    run_id: str,
    lines: int = typer.Option(50, "--lines", "-n", help="Number of log records to show."),
    level: Optional[str] = typer.Option(None, "--level", help="Minimum log level to show."),
    follow: bool = typer.Option(False, "--follow", "-f", help="Continue printing new logs until interrupted."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
    interval: float = typer.Option(1.0, "--interval", help="Polling interval in seconds when --follow is enabled."),
):
    """Print structured logs for one blueprint run."""
    _load_run_or_exit(run_id, runs_root)
    tools = _load_observability_tools()
    read_run_logs = tools["read_run_logs"]
    seen_ids: set[str] = set()
    last_ts: str | None = None
    poll_interval = max(float(interval), 1.0)
    try:
        while True:
            records = read_run_logs(
                run_id,
                runs_root=runs_root,
                level=level,
                limit=max(lines, 1),
                since=last_ts if seen_ids else None,
            )
            selected = []
            for record in records:
                cursor = _observability_cursor(record)
                if cursor in seen_ids:
                    continue
                selected.append(record)
                seen_ids.add(cursor)
                last_ts = str(record.get("ts") or record.get("timestamp") or last_ts or "")
            _print_log_records(selected)
            if not follow:
                return
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Stopped tailing logs for {run_id}.[/yellow]")


@blueprint_app.command("stream")
def blueprint_stream(
    run_id: str,
    channels: str = typer.Option("events,logs,human,resources", "--channels", help="Comma-separated channels to print."),
    lines: int = typer.Option(100, "--lines", "-n", help="Number of stream records to show."),
    level: Optional[str] = typer.Option(None, "--level", help="Minimum log level when logs are included."),
    follow: bool = typer.Option(False, "--follow", "-f", help="Continue printing new stream records until interrupted."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
    interval: float = typer.Option(1.0, "--interval", help="Polling interval in seconds when --follow is enabled."),
):
    """Print merged blueprint events, logs, human events, and resource samples."""
    _load_run_or_exit(run_id, runs_root)
    tools = _load_observability_tools()
    read_run_stream_records = tools["read_run_stream_records"]
    seen_ids: set[str] = set()
    last_ts: str | None = None
    selected_channels = [item.strip() for item in channels.split(",") if item.strip()]
    poll_interval = max(float(interval), 1.0)
    try:
        while True:
            records = read_run_stream_records(
                run_id,
                runs_root=runs_root,
                channels=selected_channels,
                level=level,
                limit=max(lines, 1),
                since=last_ts if seen_ids else None,
            )
            selected = []
            for record in records:
                cursor = _observability_cursor(record)
                if cursor in seen_ids:
                    continue
                selected.append(record)
                seen_ids.add(cursor)
                last_ts = str(record.get("ts") or last_ts or "")
            for record in selected:
                console.print(json.dumps(record, sort_keys=True), markup=False)
            if not follow:
                return
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Stopped streaming {run_id}.[/yellow]")


@blueprint_app.command("resources")
def blueprint_resources(
    run_id: str,
    window: str = typer.Option("24h", "--window", help="History window, for example 24h."),
    bucket: str = typer.Option("1h", "--bucket", help="Aggregation bucket, for example 1h."),
    live: bool = typer.Option(False, "--live", help="Refresh resource usage until interrupted."),
    interval: float = typer.Option(5.0, "--interval", help="Live refresh interval in seconds."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
):
    """Show CPU, GPU, memory, and LLM token usage for one blueprint run."""
    _load_run_or_exit(run_id, runs_root)
    tools = _load_observability_tools()
    read_run_resources = tools["read_run_resources"]
    window_hours = _duration_seconds(window) / 3600.0
    bucket_seconds = max(int(_duration_seconds(bucket)), 1)
    live_interval = max(float(interval), 1.0)
    try:
        while True:
            summary = read_run_resources(run_id, runs_root=runs_root, window_hours=window_hours, bucket_seconds=bucket_seconds)
            _print_resource_summary(summary)
            if not live:
                return
            time.sleep(live_interval)
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Stopped resource monitor for {run_id}.[/yellow]")


@human_app.callback(invoke_without_command=True)
def blueprint_human(
    ctx: typer.Context,
    run_id: Optional[str] = typer.Argument(None, help="Blueprint run ID."),
    pending: bool = typer.Option(False, "--pending", help="Show only pending human input requests."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
):
    """Show human collaboration events for one blueprint run."""
    if ctx.invoked_subcommand is not None:
        return
    if not run_id:
        console.print("[red]run_id is required unless using a human subcommand.[/red]")
        raise typer.Exit(1)
    _load_run_or_exit(run_id, runs_root)
    tools = _load_observability_tools()
    events = (
        tools["list_pending_human_requests"](run_id, runs_root=runs_root)
        if pending
        else tools["read_human_events"](run_id, runs_root=runs_root)
    )
    for event in events:
        console.print(json.dumps(event, sort_keys=True), markup=False)


@human_app.command("respond")
def blueprint_human_respond(
    run_id: str,
    request_id: str,
    decision: str = typer.Option(..., "--decision", help="Decision value, such as approve, revise, or reject."),
    notes: str = typer.Option("", "--notes", help="Optional reviewer notes."),
    reviewer: str = typer.Option("cli", "--reviewer", help="Reviewer identity label."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
):
    """Record a response to a human input request."""
    _load_run_or_exit(run_id, runs_root)
    tools = _load_observability_tools()
    event = tools["record_human_response"](
        run_id,
        request_id,
        {"decision": decision, "notes": notes, "reviewer": reviewer},
        runs_root=runs_root,
    )
    console.print(json.dumps(event, indent=2, sort_keys=True), markup=False)


@human_app.command("ack")
def blueprint_human_ack(
    run_id: str,
    notice_id: str,
    reviewer: str = typer.Option("cli", "--reviewer", help="Reviewer identity label."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
):
    """Acknowledge a human notice."""
    _load_run_or_exit(run_id, runs_root)
    tools = _load_observability_tools()
    event = tools["acknowledge_human_notice"](run_id, notice_id, {"reviewer": reviewer}, runs_root=runs_root)
    console.print(json.dumps(event, indent=2, sort_keys=True), markup=False)


@blueprint_app.command("compare")
def blueprint_compare(
    run_a: str,
    run_b: str,
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default ~/.mn/runs directory."),
):
    """Compare two blueprint runs from the shared run store."""
    record_a = _load_run_or_exit(run_a, runs_root, include_observability=True)
    record_b = _load_run_or_exit(run_b, runs_root, include_observability=True)
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
    record = _load_run_or_exit(run_id, runs_root, include_observability=True)
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


blueprint_app.add_typer(human_app, name="human")
