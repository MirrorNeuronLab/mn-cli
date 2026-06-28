import os
import copy
import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Annotated, Any, Optional

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
    resolved_blueprint_source as _resolved_blueprint_source,
)
from mn_cli.libs.blueprint_resources import (
    cleanup_blueprint_resources as _cleanup_blueprint_resources,
    default_bundle_cache_dir as _default_bundle_cache_dir,
    default_generated_bundles_dir as _default_generated_bundles_dir,
    default_python_envs_dir as _default_python_envs_dir,
    default_runs_root as _default_runs_root,
)
from mn_cli.libs.ui import print_confirmed, print_success_confirmation
from mn_cli.shared import console, logger
from mn_cli.libs.blueprint_models import BlueprintModelOps, blueprint_model_dependency_summary
from mn_cli.libs.run_cmds import run_bundle as _run_bundle
from mn_cli.libs.run_manifest import load_blueprint_config as _load_blueprint_config
from mn_cli.libs.model_cmds import (
    install_model_entry as _install_model_entry,
    model_installed as _model_installed,
    remove_model_ref as _remove_model_ref,
)
from mn_sdk.runtime_config import resolve_mn_home
from mn_sdk.blueprint_support.python_workflow_bundle import (
    generate_python_workflow_bundle_from_blueprint_dir,
)
from mn_sdk import (
    cluster_provided_model as _cluster_provided_model,
    docker_model_name as _docker_model_name,
    load_model_catalog as _load_model_catalog,
    load_model_ownership as _load_model_ownership,
    record_model_owner as _record_model_owner,
    remove_model_owner as _remove_model_owner,
    remove_model_record as _remove_model_record,
    required_blueprint_models as _required_blueprint_models,
    resolve_model_entry as _resolve_model_entry,
)

blueprint_app = typer.Typer(help="Manage and run MirrorNeuron blueprints")
human_app = typer.Typer(
    help="Inspect and respond to human collaboration events",
    invoke_without_command=True,
    no_args_is_help=False,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
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
    detached: bool = False,
    web_ui: bool = False,
    auto_schedule: bool = False,
    schedule: Optional[str] = None,
    fake_llm: bool = False,
) -> None:
    shared_run_id = run_id or _make_blueprint_run_id(blueprint_id)
    _print_blueprint_run_phase(1, 4, "Prepare blueprint bundle")
    bundle_path = _prepare_blueprint_bundle_for_run(blueprint_dir, manifest, shared_run_id)
    _print_blueprint_run_phase(2, 4, "Review launch config")
    config_overrides = _collect_init_config_review_overrides(bundle_path, manifest)
    config = _load_blueprint_config(bundle_path, config_overrides=config_overrides)
    if fake_llm:
        fake_overrides = _fake_llm_config_overrides(config or {})
        config_overrides = _deep_merge(config_overrides or {}, fake_overrides)
        config = _deep_merge(config or {}, fake_overrides)
    model_manifest = _fake_llm_manifest_for_model_dependencies(manifest) if fake_llm else manifest
    _print_blueprint_run_phase(3, 4, "Ensure runtime models")
    model_summary = _install_blueprint_model_dependencies(
        blueprint_id=blueprint_id,
        blueprint_revision=revision,
        bundle_root=bundle_path,
        manifest=model_manifest,
        config=config or {},
        install_source=source_label,
        force=force,
    )
    if model_summary.get("models"):
        _print_model_install_summary(model_summary)
    _print_blueprint_run_phase(4, 4, "Submit runtime job")
    print_confirmed(
        console,
        "Blueprint validation",
        status="valid",
        details=[
            ("Blueprint", display_name),
            ("Run ID", shared_run_id),
            ("Revision", revision),
        ],
    )
    _run_bundle(
        str(bundle_path),
        follow_seconds=follow_seconds,
        env_overrides={
            "MN_RUN_ID": shared_run_id,
            "MN_BLUEPRINT_ID": blueprint_id,
            "MN_BLUEPRINT_REVISION": revision or "",
            **_fake_llm_env_overrides(fake_llm),
        },
        submission_metadata={
            "blueprint_id": blueprint_id,
            "blueprint_run_id": shared_run_id,
            "blueprint_revision": revision,
            "blueprint_source": source_label,
            "fake_llm": fake_llm,
        },
        config_overrides=config_overrides,
        force=force,
        detached=detached,
        web_ui=web_ui,
        auto_schedule=auto_schedule,
        schedule=schedule,
    )


def _fake_llm_env_overrides(enabled: bool) -> dict[str, str]:
    if not enabled:
        return {}
    return {
        "MN_BLUEPRINT_LLM_MODE": "fake",
        "MN_LLM_PROVIDER": "fake",
        "MN_LLM_MODEL": "fake-deterministic-blueprint-agent",
    }


def _fake_llm_config_overrides(config: dict[str, Any]) -> dict[str, Any]:
    llm = config.get("llm") if isinstance(config.get("llm"), dict) else {}
    configs = llm.get("configs") if isinstance(llm.get("configs"), dict) else {}
    config_names = list(configs) or [str(llm.get("default_config") or "primary")]
    fake_config = {
        "provider": "fake",
        "mode": "fake",
        "model": "fake-deterministic-blueprint-agent",
        "runtime_model": None,
        "mock_mode": "fake",
        "api_base": "",
    }
    return {
        "llm": {
            "enabled": True,
            "mode": "fake",
            "provider": "fake",
            "model": "fake-deterministic-blueprint-agent",
            "runtime_model": None,
            "mock_mode": "fake",
            "require_live": False,
            "quick_test_uses_fake": True,
            "configs": {name: dict(fake_config) for name in config_names},
        }
    }


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _fake_llm_manifest_for_model_dependencies(manifest: dict[str, Any]) -> dict[str, Any]:
    runtime = manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
    models = runtime.get("models") if isinstance(runtime.get("models"), dict) else {}
    model_names = list(models) or ["primary"]
    fake_model = {
        "provider": "fake",
        "mode": "fake",
        "model": "fake-deterministic-blueprint-agent",
        "runtime_model": "fake-deterministic-blueprint-agent",
    }
    return _deep_merge(
        manifest,
        {
            "runtime": {"models": {name: dict(fake_model) for name in model_names}},
            "llm": {
                "require_live": False,
                "model": "fake-deterministic-blueprint-agent",
            },
        },
    )


def _print_blueprint_run_phase(step: int, total: int, label: str) -> None:
    console.print(f"[bold]Step {step}/{total}[/bold] {label}")


def _reject_local_blueprint_path(target: str) -> None:
    blueprint_dir = Path(target).expanduser()
    if not blueprint_dir.exists():
        return
    console.print("[red]Error: local folders must be passed with --folder.[/red]")
    console.print(f"Use [bold]mn blueprint run --folder {blueprint_dir}[/bold] to run a local blueprint folder.")
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
    try:
        storage_dir = Path(
            _ensure_blueprint_source(
                source=None,
                blueprint_repo=blueprint_repo,
                update=False,
                offline=False,
                revision=None,
            )
        )
    except ValueError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1)
    index_path = storage_dir / "index.json"
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
        raise typer.Exit(1)

def run_catalog_blueprint(
    blueprint_name: str,
    *,
    run_id: Optional[str] = None,
    blueprint_repo: Optional[str] = None,
    source: Optional[str] = None,
    update: bool = False,
    offline: bool = False,
    revision: Optional[str] = None,
    follow_seconds: Optional[float] = None,
    force: bool = False,
    detached: bool = False,
    web_ui: bool = False,
    auto_schedule: bool = False,
    schedule: Optional[str] = None,
    fake_llm: bool = False,
) -> None:
    """Run a catalog blueprint by name through the shared blueprint runner."""
    _reject_local_blueprint_path(blueprint_name)
    try:
        storage_dir = _ensure_blueprint_source(
            source=source,
            blueprint_repo=blueprint_repo,
            update=update,
            offline=offline,
            revision=revision,
        )
    except ValueError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1)
    
    index_path = Path(storage_dir) / "index.json"
    try:
        blueprints = _load_blueprint_index(index_path, require_paths=True)
    except BlueprintIndexError as e:
        logger.exception("Error parsing blueprint index")
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
        
    target_bp = None
    for bp in blueprints:
        if bp.get("id") == blueprint_name or bp.get("path") == blueprint_name:
            target_bp = bp
            break
            
    if not target_bp:
        console.print(f"[red]Error: Blueprint '{blueprint_name}' not found in index.[/red]")
        raise typer.Exit(1)
        
    bp_path = os.path.join(storage_dir, target_bp.get("path"))
    
    manifest = _load_blueprint_manifest(Path(bp_path), blueprint_name)
    blueprint_id = str((manifest.get("metadata") or {}).get("blueprint_id") or target_bp.get("id") or blueprint_name)
    resolved_revision = _git_revision(Path(storage_dir)) or revision
    _run_resolved_blueprint(
        blueprint_dir=Path(bp_path),
        manifest=manifest,
        display_name=blueprint_name,
        blueprint_id=blueprint_id,
        run_id=run_id,
        revision=resolved_revision,
        source_label=str(storage_dir),
        follow_seconds=follow_seconds,
        force=force,
        detached=detached,
        web_ui=web_ui,
        auto_schedule=auto_schedule,
        schedule=schedule,
        fake_llm=fake_llm,
    )


def run_local_blueprint_folder(
    folder: str,
    *,
    run_id: Optional[str] = None,
    follow_seconds: Optional[float] = None,
    force: bool = False,
    detached: bool = False,
    web_ui: bool = False,
    auto_schedule: bool = False,
    schedule: Optional[str] = None,
    fake_llm: bool = False,
) -> None:
    """Run a local Python source blueprint folder through the shared blueprint runner."""
    blueprint_dir = Path(folder).expanduser()
    manifest = _load_blueprint_manifest(blueprint_dir, str(blueprint_dir))
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    workflow = manifest.get("workflow") if isinstance(manifest.get("workflow"), dict) else {}
    workflow_manifest = manifest.get("apiVersion") == "mn.workflow/v1" or manifest.get("kind") == "Workflow" or isinstance(manifest.get("workflow"), dict)
    blueprint_id = str(
        metadata.get("blueprint_id")
        or manifest.get("id")
        or manifest.get("blueprint_id")
        or manifest.get("workflow_id")
        or workflow.get("workflow_id")
        or (None if workflow_manifest else manifest.get("graph_id"))
        or blueprint_dir.name
    )
    _run_resolved_blueprint(
        blueprint_dir=blueprint_dir,
        manifest=manifest,
        display_name=blueprint_id,
        blueprint_id=blueprint_id,
        run_id=run_id,
        revision=None,
        source_label=str(blueprint_dir),
        follow_seconds=follow_seconds,
        force=force,
        detached=detached,
        web_ui=web_ui,
        auto_schedule=auto_schedule,
        schedule=schedule,
        fake_llm=fake_llm,
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
    fake_llm: bool = False,
) -> None:
    bundle_dir = Path(folder).expanduser()
    manifest = _load_manifest_for_local_route(bundle_dir)
    if manifest is not None and _is_python_source_blueprint(manifest):
        run_local_blueprint_folder(
            str(bundle_dir),
            run_id=run_id,
            follow_seconds=follow_seconds,
            force=force,
            detached=detached,
            web_ui=web_ui,
            auto_schedule=auto_schedule,
            schedule=schedule,
            fake_llm=fake_llm,
        )
        return

    env_overrides = {"MN_RUN_ID": run_id} if run_id else {}
    env_overrides.update(_fake_llm_env_overrides(fake_llm))
    submission_metadata = {"blueprint_run_id": run_id} if run_id else {}
    if fake_llm:
        submission_metadata["fake_llm"] = True
    _run_bundle(
        str(bundle_dir),
        follow_seconds=follow_seconds,
        env_overrides=env_overrides or None,
        submission_metadata=submission_metadata or None,
        config_overrides=_fake_llm_config_overrides(_load_blueprint_config(bundle_dir) or {}) if fake_llm else None,
        force=force,
        detached=detached,
        web_ui=web_ui,
        auto_schedule=auto_schedule,
        schedule=schedule,
    )


def _load_manifest_for_local_route(bundle_dir: Path) -> dict[str, Any] | None:
    manifest_file = bundle_dir / "manifest.json"
    if not manifest_file.exists():
        return None
    try:
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    except Exception:
        return None
    return manifest if isinstance(manifest, dict) else None


@blueprint_app.command("run")
def blueprint_run(
    ctx: typer.Context,
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
    fake_llm: Annotated[
        bool,
        typer.Option(
            "--fake-llm",
            help="Replace blueprint LLM configuration with the deterministic fake LLM for this run.",
        ),
    ] = False,
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
            fake_llm=fake_llm,
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

    run_catalog_blueprint(
        target,
        run_id=run_id,
        blueprint_repo=blueprint_repo or _context_blueprint_repo(ctx),
        update=update,
        offline=offline,
        revision=revision,
        follow_seconds=follow_seconds,
        force=force,
        detached=detached,
        web_ui=web_ui,
        auto_schedule=auto_schedule,
        schedule=schedule,
        fake_llm=fake_llm,
    )


@blueprint_app.command("install")
def blueprint_install(
    ctx: typer.Context,
    blueprint_id: Optional[str] = typer.Argument(None, help="Blueprint ID to install. Omit to install the blueprint library."),
    source: Optional[str] = typer.Option(None, "--source", help="Blueprint repository URL or local path."),
    revision: Optional[str] = typer.Option(None, "--revision", help="Git revision to use when installing a blueprint by ID."),
    force: bool = typer.Option(False, "--force", help="Replace cached library storage, or force model install compatibility for a blueprint."),
):
    """Install the blueprint library or one blueprint plus its required runtime models."""
    if blueprint_id:
        _install_catalog_blueprint_with_models(
            ctx,
            blueprint_id=blueprint_id,
            source=source,
            revision=revision,
            force=force,
        )
        return

    blueprint_repo = _context_blueprint_repo(ctx)
    try:
        repo_source, uses_default_repo = _resolved_blueprint_source(source=source, blueprint_repo=blueprint_repo)
    except ValueError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1)
    storage_dir = (
        _blueprint_cache_dir_for_repo(repo_source)
        if source is None and blueprint_repo
        else _default_blueprint_storage_dir()
        if uses_default_repo
        else _blueprint_storage_dir_for_source(repo_source)
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
    print_success_confirmation(
        console,
        "Blueprint library install",
        details={"Storage": storage_dir},
        next_steps="mn blueprint list",
    )


def _install_catalog_blueprint_with_models(
    ctx: typer.Context,
    *,
    blueprint_id: str,
    source: Optional[str],
    revision: Optional[str],
    force: bool,
) -> None:
    blueprint_repo = _context_blueprint_repo(ctx)
    try:
        storage_dir = Path(
            _ensure_blueprint_source(
                source=source,
                blueprint_repo=blueprint_repo,
                update=False,
                offline=False,
                revision=revision,
            )
        )
    except ValueError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1)
    entry = _blueprint_entry_from_storage(storage_dir, blueprint_id)
    bundle_root = _blueprint_bundle_root_from_entry(storage_dir, entry)
    manifest = _read_json_object(bundle_root / "manifest.json")
    config = _load_blueprint_config(bundle_root)
    try:
        install_source, _uses_default_repo = _resolved_blueprint_source(source=source, blueprint_repo=blueprint_repo)
    except ValueError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1)
    model_summary = _install_blueprint_model_dependencies(
        blueprint_id=blueprint_id,
        blueprint_revision=revision or _git_revision(storage_dir),
        bundle_root=bundle_root,
        manifest=manifest,
        config=config,
        install_source=install_source,
        force=force,
    )
    _record_blueprint_install(
        blueprint_id=blueprint_id,
        storage_dir=storage_dir,
        bundle_root=bundle_root,
        entry=entry,
        manifest=manifest,
        revision=revision or _git_revision(storage_dir),
        install_source=install_source,
        model_summary=model_summary,
    )
    print_success_confirmation(
        console,
        "Blueprint install",
        details=[("Blueprint", blueprint_id), ("Storage", storage_dir), ("Bundle", bundle_root)],
        next_steps=f"mn blueprint run {blueprint_id}",
    )
    _print_model_install_summary(model_summary)


def _install_blueprint_model_dependencies(
    *,
    blueprint_id: str,
    blueprint_revision: str | None,
    bundle_root: Path,
    manifest: dict[str, Any],
    config: dict[str, Any],
    install_source: str,
    force: bool,
) -> dict[str, Any]:
    summary = blueprint_model_dependency_summary(
        blueprint_id=blueprint_id,
        blueprint_revision=blueprint_revision,
        bundle_root=bundle_root,
        manifest=manifest,
        config=config,
        install_source=install_source,
        force=force,
        ops=BlueprintModelOps(
            load_model_catalog=_load_model_catalog,
            required_blueprint_models=_required_blueprint_models,
            load_model_ownership=_load_model_ownership,
            resolve_model_entry=_resolve_model_entry,
            docker_model_name=_docker_model_name,
            cluster_provided_model=_cluster_provided_model,
            record_model_owner=_record_model_owner,
            model_installed=_model_installed,
            install_model_entry=_install_model_entry,
            notify_model_install_start=_print_model_install_start,
        ),
    )
    if summary["errors"]:
        _print_model_install_summary(summary)
        raise typer.Exit(1)
    return summary


def _print_model_install_start(model: dict[str, Any]) -> None:
    label = str(model.get("id") or model.get("model") or "runtime model")
    docker_model = str(model.get("model") or "")
    backend = str(model.get("backend") or "auto")
    detail = f"{label} ({docker_model})" if docker_model and docker_model != label else label
    console.print(
        f"[yellow]Runtime model {detail} is not installed. "
        f"Installing with backend {backend}; this may take a few minutes the first time.[/yellow]"
    )


def _print_model_install_summary(summary: dict[str, Any]) -> None:
    models = summary.get("models") or []
    if not models:
        print_confirmed(
            console,
            "Blueprint model dependency check",
            status="none declared",
        )
        return
    table = Table(title="Blueprint model dependencies", show_header=True, header_style="bold")
    table.add_column("Model")
    table.add_column("Provider")
    table.add_column("Status")
    for item in models:
        table.add_row(
            str(item.get("id") or item.get("model") or ""),
            str(item.get("provider") or ""),
            str(item.get("status") or ""),
        )
    console.print(table)
    for error in summary.get("errors") or []:
        console.print(f"[red]Model install failed: {error}[/red]")


def _uninstall_catalog_blueprint(
    ctx: typer.Context,
    *,
    blueprint_id: str,
    source: Optional[str],
    keep_resources: bool,
    keep_models: bool,
    remove_models: bool,
    dry_run: bool,
) -> None:
    storage_dir = _resolve_blueprint_storage_for_cleanup(ctx, source)
    entry: dict[str, Any] | None = None
    bundle_root: Path | None = None
    if storage_dir.exists():
        try:
            entry = _blueprint_entry_from_storage(storage_dir, blueprint_id)
            bundle_root = _blueprint_bundle_root_from_entry(storage_dir, entry)
        except typer.Exit:
            entry = None
    archive_path = _archive_blueprint_install(
        blueprint_id=blueprint_id,
        storage_dir=storage_dir,
        bundle_root=bundle_root,
        entry=entry,
        dry_run=dry_run,
    )
    if dry_run:
        print_confirmed(
            console,
            "Blueprint uninstall dry run",
            status="planned",
            details=[("Blueprint", blueprint_id), ("Archive", archive_path)],
        )
    else:
        print_success_confirmation(
            console,
            "Blueprint uninstall",
            status="metadata archived",
            details=[("Blueprint", blueprint_id), ("Archive", archive_path)],
        )

    if not keep_resources:
        summary = _cleanup_blueprint_resources(
            blueprint_ids={blueprint_id},
            active_blueprint_ids=set(),
            include_dead=True,
            include_docker=True,
            include_files=True,
            dry_run=dry_run,
        )
        _print_cleanup_summary(summary)

    orphaned = _orphaned_models_after_owner_removal(blueprint_id, dry_run=dry_run)
    if keep_models:
        if orphaned:
            console.print(f"[yellow]Kept {len(orphaned)} orphaned model(s).[/yellow]")
        return
    _remove_or_prompt_for_orphaned_models(orphaned, remove_models=remove_models, dry_run=dry_run)


def _blueprint_entry_from_storage(storage_dir: Path, blueprint_id: str) -> dict[str, Any]:
    try:
        entries = _load_blueprint_index(storage_dir / "index.json", require_paths=True)
    except BlueprintIndexError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1)
    for entry in entries:
        if entry.get("id") == blueprint_id:
            return entry
    console.print(f"[red]Blueprint {blueprint_id!r} was not found in {storage_dir}.[/red]")
    raise typer.Exit(1)


def _blueprint_bundle_root_from_entry(storage_dir: Path, entry: dict[str, Any]) -> Path:
    path = Path(str(entry.get("path") or entry.get("id") or ""))
    bundle_root = path if path.is_absolute() else storage_dir / path
    if not bundle_root.is_dir():
        console.print(f"[red]Blueprint bundle not found at {bundle_root}.[/red]")
        raise typer.Exit(1)
    return bundle_root


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        console.print(f"[red]Could not read {path}: {exc}[/red]")
        raise typer.Exit(1)
    if not isinstance(data, dict):
        console.print(f"[red]{path} must contain a JSON object.[/red]")
        raise typer.Exit(1)
    return data


def _blueprint_installs_dir() -> Path:
    configured = os.getenv("MN_BLUEPRINT_INSTALLS_DIR")
    return Path(configured).expanduser() if configured else resolve_mn_home() / "blueprint_installs"


def _record_blueprint_install(
    *,
    blueprint_id: str,
    storage_dir: Path,
    bundle_root: Path,
    entry: dict[str, Any],
    manifest: dict[str, Any],
    revision: str | None,
    install_source: str,
    model_summary: dict[str, Any],
) -> Path:
    install_dir = _blueprint_installs_dir()
    install_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "schema_version": "mn.blueprint.install.v1",
        "blueprint_id": blueprint_id,
        "name": entry.get("name") or manifest.get("job_name") or blueprint_id,
        "path": entry.get("path"),
        "storage_dir": str(storage_dir),
        "bundle_root": str(bundle_root),
        "revision": revision or "",
        "install_source": install_source,
        "installed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "models": model_summary.get("models") or [],
    }
    target = install_dir / f"{blueprint_id}.json"
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return target


def _archive_blueprint_install(
    *,
    blueprint_id: str,
    storage_dir: Path,
    bundle_root: Path | None,
    entry: dict[str, Any] | None,
    dry_run: bool,
) -> Path:
    install_dir = _blueprint_installs_dir()
    record_path = install_dir / f"{blueprint_id}.json"
    if record_path.is_file():
        payload = _read_json_object(record_path)
    else:
        payload = {
            "version": 1,
            "schema_version": "mn.blueprint.install.v1",
            "blueprint_id": blueprint_id,
            "path": (entry or {}).get("path"),
            "storage_dir": str(storage_dir),
            "bundle_root": str(bundle_root) if bundle_root else "",
        }
    payload.setdefault("version", 1)
    payload["archived_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    archive_dir = install_dir / "archive"
    archive_path = archive_dir / f"{blueprint_id}-{int(time.time())}.json"
    if dry_run:
        return archive_path
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    record_path.unlink(missing_ok=True)
    return archive_path


def _orphaned_models_after_owner_removal(blueprint_id: str, *, dry_run: bool) -> list[dict[str, Any]]:
    if not dry_run:
        return _remove_model_owner(blueprint_id)
    ledger = _load_model_ownership()
    orphaned: list[dict[str, Any]] = []
    for record in ledger.get("models", {}).values():
        if not isinstance(record, dict):
            continue
        owners = dict(record.get("owners") or {})
        owners.pop(blueprint_id, None)
        if not owners and not record.get("manual") and str(record.get("provider") or "docker_model_runner") == "docker_model_runner":
            projected = dict(record)
            projected["owners"] = {}
            orphaned.append(projected)
    return orphaned


def _remove_or_prompt_for_orphaned_models(
    orphaned: list[dict[str, Any]],
    *,
    remove_models: bool,
    dry_run: bool,
) -> None:
    if not orphaned:
        return
    removed = 0
    kept = 0
    for record in orphaned:
        model = str(record.get("docker_model") or record.get("model") or "")
        if not model:
            continue
        should_remove = remove_models
        if not remove_models and not dry_run:
            should_remove = typer.confirm(
                f"Remove orphaned model {model}? It will need to be installed again next time.",
                default=False,
            )
        if should_remove:
            if dry_run:
                console.print(f"Would remove orphaned model {model}.")
            else:
                _remove_model_ref(model, force=True)
                _remove_model_record(model)
            removed += 1
        else:
            kept += 1
    if removed:
        if dry_run:
            print_confirmed(
                console,
                "Orphaned model cleanup dry run",
                status="planned",
                details={"Models": removed},
            )
        else:
            print_success_confirmation(
                console,
                "Orphaned model cleanup",
                status="removed",
                details={"Models": removed},
            )
    if kept:
        console.print(f"[yellow]Kept {kept} orphaned model(s).[/yellow]")


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
        try:
            repo_source, uses_default_repo = _resolved_blueprint_source(source=None, blueprint_repo=None)
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")
            raise typer.Exit(1)
        storage_dir = _default_blueprint_storage_dir() if uses_default_repo else _blueprint_storage_dir_for_source(repo_source)
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
    print_success_confirmation(
        console,
        "Blueprint library update",
        details=[
            ("Storage", storage_dir),
            ("Blueprints before", len(before_ids)),
            ("Blueprints after", len(after_ids)),
            ("Blueprints removed", len(removed_ids)),
        ],
        next_steps="mn blueprint list",
    )


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
    blueprint_id: Optional[str] = typer.Argument(None, help="Blueprint ID to uninstall. Omit to remove cached blueprint storage."),
    source: Optional[str] = typer.Option(None, "--source", help="Cached blueprint storage path to remove."),
    keep_resources: bool = typer.Option(False, "--keep-resources", help="Remove blueprint files but keep cached runtime resources."),
    keep_models: bool = typer.Option(False, "--keep-models", help="Keep orphaned models after removing this blueprint."),
    remove_models: bool = typer.Option(False, "--remove-models", help="Remove orphaned models without prompting."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be removed without deleting anything."),
):
    """Remove cached blueprint storage and its owned runtime resources."""
    if keep_models and remove_models:
        console.print("[red]Use only one of --keep-models or --remove-models.[/red]")
        raise typer.Exit(1)
    if blueprint_id:
        _uninstall_catalog_blueprint(
            ctx,
            blueprint_id=blueprint_id,
            source=source,
            keep_resources=keep_resources,
            keep_models=keep_models,
            remove_models=remove_models,
            dry_run=dry_run,
        )
        return

    storage_dir = _resolve_blueprint_storage_for_cleanup(ctx, source)
    blueprint_ids = _blueprint_ids_from_storage(storage_dir)
    if not storage_dir.exists():
        console.print(f"[yellow]Blueprint storage not found at {storage_dir}.[/yellow]")
    elif dry_run:
        print_confirmed(
            console,
            "Blueprint storage uninstall dry run",
            status="planned",
            details={"Storage": storage_dir},
        )
    else:
        shutil.rmtree(storage_dir)
        print_success_confirmation(
            console,
            "Blueprint storage uninstall",
            status="removed",
            details={"Storage": storage_dir},
        )

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
    repo_source, uses_default_repo = _resolved_blueprint_source(source=None, blueprint_repo=None)
    return _default_blueprint_storage_dir() if uses_default_repo else _blueprint_storage_dir_for_source(repo_source)


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
    if python_removed or run_removed or generated_removed or bundle_removed or docker_removed or process_removed:
        action = "Blueprint cleanup dry run" if dry_run else "Blueprint cleanup"
        printer = print_confirmed if dry_run else print_success_confirmation
        printer(
            console,
            action,
            status="planned" if dry_run else "removed",
            details=[
                ("Python env resources", len(python_removed)),
                ("Run records", len(run_removed)),
                ("Generated bundles", len(generated_removed)),
                ("Bundle cache resources", len(bundle_removed)),
                ("Docker resources", len(docker_removed)),
                ("Web UI processes", len(process_removed)),
            ],
        )
    else:
        print_confirmed(
            console,
            "Blueprint cleanup",
            status="no resources matched",
        )
    for error in errors:
        console.print(f"[yellow]Cleanup warning: {error}[/yellow]")


@blueprint_app.command("monitor")
def blueprint_monitor(
    follow: bool = typer.Option(False, "--follow", "-f", help="Refresh the run table until interrupted."),
    blueprint_id: Optional[str] = typer.Option(None, "--blueprint-id", help="Only show runs for one blueprint ID."),
    max_runs: int = typer.Option(20, "--max-runs", help="Maximum number of runs to display."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
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
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
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
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
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
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
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
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
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


@blueprint_app.command("human")
def blueprint_human_command(
    args: list[str] = typer.Argument(None, help="Run ID, or respond/ack subcommand arguments."),
    pending: bool = typer.Option(False, "--pending", help="Show only pending human input requests."),
    decision: Optional[str] = typer.Option(None, "--decision", help="Decision for respond."),
    notes: str = typer.Option("", "--notes", help="Optional reviewer notes."),
    reviewer: str = typer.Option("cli", "--reviewer", help="Reviewer identity label."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
):
    """Inspect and respond to human collaboration events."""
    args = args or []
    if args and args[0] == "respond":
        if len(args) < 3:
            console.print("[red]respond expects run_id and request_id.[/red]")
            raise typer.Exit(1)
        if not decision:
            console.print("[red]--decision is required for respond.[/red]")
            raise typer.Exit(1)
        return blueprint_human_respond(args[1], args[2], decision=decision, notes=notes, reviewer=reviewer, runs_root=runs_root)

    if args and args[0] == "ack":
        if len(args) < 3:
            console.print("[red]ack expects run_id and notice_id.[/red]")
            raise typer.Exit(1)
        return blueprint_human_ack(args[1], args[2], reviewer=reviewer, runs_root=runs_root)

    run_id = args[0] if args else None
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


@human_app.callback(invoke_without_command=True)
def blueprint_human(
    ctx: typer.Context,
    run_id: Optional[str] = typer.Argument(None, help="Blueprint run ID."),
    pending: bool = typer.Option(False, "--pending", help="Show only pending human input requests."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
):
    """Show human collaboration events for one blueprint run."""
    if ctx.invoked_subcommand is not None:
        return
    if not run_id and ctx.args:
        run_id = ctx.args[0]
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
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
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
    payload = event.get("payload") if isinstance(event, dict) else {}
    print_success_confirmation(
        console,
        "Human response",
        details=[
            ("Run ID", run_id),
            ("Request ID", request_id),
            ("Decision", decision),
            ("Approved", payload.get("approved") if isinstance(payload, dict) else None),
        ],
    )


@human_app.command("ack")
def blueprint_human_ack(
    run_id: str,
    notice_id: str,
    reviewer: str = typer.Option("cli", "--reviewer", help="Reviewer identity label."),
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
):
    """Acknowledge a human notice."""
    _load_run_or_exit(run_id, runs_root)
    tools = _load_observability_tools()
    tools["acknowledge_human_notice"](run_id, notice_id, {"reviewer": reviewer}, runs_root=runs_root)
    print_success_confirmation(
        console,
        "Human notice acknowledgement",
        details=[("Run ID", run_id), ("Notice ID", notice_id), ("Reviewer", reviewer)],
    )


@blueprint_app.command("compare")
def blueprint_compare(
    run_a: str,
    run_b: str,
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
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
    runs_root: Optional[str] = typer.Option(None, "--runs-root", help="Override the default $MN_HOME/runs directory."),
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
