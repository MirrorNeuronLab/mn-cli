import typer
import hashlib
import importlib.util
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Optional
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.live import Live
from rich.table import Table
from mn_cli.libs.ui import (
    JobMonitorState,
    generate_detached_panel,
    generate_live_layout,
    generate_run_submitted_panel,
    generate_summary_panel,
    print_confirmed,
    print_error,
    print_info,
    print_success_confirmation,
    print_warning,
)
from mn_cli.libs.bundles import load_bundle_payloads
from mn_cli.libs.workflow_progress import BlueprintWorkflowProgress
from mn_cli.libs.progress_stream import (
    ProgressSnapshotStream,
    stream_api_workflow_progress,
)
from mn_cli.libs.run_logs import (
    JobLogWriter,
    STANDARD_EVENTS,
    extract_web_ui_url as _extract_web_ui_url,
    materialize_sent_email_copy as _materialize_sent_email_copy,
    write_result_stream_event as _write_result_stream_event,
)
from mn_cli.libs.artifacts import promote_large_payloads_to_blob_refs
from mn_sdk.submission_preparation import (
    add_mn_llm_aliases as _add_mn_llm_aliases,
    blueprint_runtime_environment as _blueprint_runtime_environment,
    inject_node_environment as _inject_node_environment,
    load_blueprint_config,
    manifest_nodes,
    prepare_manifest_for_submission,
    runtime_web_ui_support_payloads_for_manifest,
    run_mode_label as _run_mode_label,
    stage_blueprint_support_payloads_for_manifest,
    stage_skill_dependency_payloads_for_manifest,
    stage_skill_runtime_support_payloads_for_manifest,
    stage_local_input_payloads_for_manifest,
    stage_upload_path_payloads_for_manifest,
    with_shared_run_store_config as _with_shared_run_store_config,
)
from mn_sdk.skill_runtime import validate_skill_runtime_requirements
from mn_sdk.skill_dependencies import gar_requirements_text, skill_dependency_records
from mn_cli.libs.workflow_validation import (
    _is_workflow_manifest,
    _manifest_workflow_id,
    _validate_workflow_manifest_issues,
    _validate_workflow_schema_issues,
)
from mn_cli.libs.blueprint_observability import (
    load_observability_tools,
    make_blueprint_run_id as _make_blueprint_run_id,
)
from mn_cli.libs.model_cmds import install_model_entry, model_installed
from mn_cli.libs.blueprint_resources import cleanup_blueprint_host_hooks
from mn_cli.server_cmds import ensure_context_engine_runtime
from mn_cli.shared import console, client, config, logger
from mn_cli.terminal import use_progress
from mn_cli.error_handler import handle_cli_error
from mn_sdk import (
    Client,
    BlueprintModelOps,
    CUSTOM_MODEL_WARNING,
    DEFAULT_RUNTIME_MODEL_PREPARE_TIMEOUT_SECONDS,
    ModelEndpointMap,
    ModelPrepareError,
    cluster_provided_model,
    cleanup_docker_worker_services,
    docker_model_match_keys,
    docker_api_model_name,
    docker_model_runner_endpoint,
    docker_model_name,
    expand_manifest_source,
    gateway_endpoint_map,
    is_manifest_source,
    installed_model_names,
    load_model_remotes,
    load_model_catalog,
    load_model_ownership,
    make_validation_report,
    model_service_tags,
    prepare_job_submission,
    record_model_owner,
    reconcile_cluster_model_remotes,
    required_blueprint_models,
    resolve_cluster_model_placement,
    resolve_custom_model_placement,
    is_custom_model_requirement,
    resolve_model_endpoint,
    resolve_model_entry,
    resolve_requirement_entry,
    run_hardware_requirements_validation,
    run_input_validation,
    run_model_validation,
    run_service_validation,
    save_model_remotes,
    validate_input_validation_spec_issues,
    validate_requirements_spec_issues,
    validate_resource_spec_issues,
    validate_service_spec_issues,
    sync_litellm_gateway,
    workflow_progress_snapshot,
    blueprint_model_dependency_summary,
    build_prepare_runtime_model_request,
    call_prepare_runtime_model,
    remote_runtime_model_endpoint,
    runtime_model_prepare_timeout_seconds,
)
from mn_sdk.blueprint_support.shared_outputs import (
    materialize_shared_storage_outputs as _sdk_materialize_shared_storage_outputs,
)
from mn_sdk.context_engine import blueprint_requires_context_engine
from mn_sdk.runtime_config import default_runs_root

FINAL_STATUSES = {"completed", "failed", "cancelled"}
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
PRE_LAUNCH_SCRIPT = Path("scripts/pre-launch.sh")
POST_LAUNCH_SCRIPT = Path("scripts/post-launch.sh")
DEFAULT_BLUEPRINT_WEB_UI_PORT_START = 61000
DEFAULT_BLUEPRINT_WEB_UI_PORT_END = 61049
DETACHED_AFTER_INTERRUPT_MESSAGE = "Detached from workflow UI. Job is still running."
CONTEXT_ENGINE_EXPECTATION = (
    "This blueprint uses context memory. First launch may download the context model "
    "and start the Membrane context engine; keep Docker running and be patient."
)
FALSE_VALUES = {"0", "false", "no", "off"}
_HELPER_COMPAT = (
    _add_mn_llm_aliases,
    _blueprint_runtime_environment,
    _extract_web_ui_url,
    _inject_node_environment,
    _materialize_sent_email_copy,
)


def _print_launch_progress(label: str, detail: str | None = None) -> None:
    message = f"{label} — {detail}" if detail else label
    print_info(console, message)


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def _runtime_resource_report(*, allow_local_fallback: bool = False) -> dict[str, Any]:
    try:
        decoded = json.loads(client.get_resource())
    except Exception:
        return {} if allow_local_fallback else {"nodes": []}
    return (
        decoded
        if isinstance(decoded, dict)
        else ({} if allow_local_fallback else {"nodes": []})
    )


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
    validation["skipped_checks"] = [
        "services",
        "models",
        "input_validation",
        "soft_requirements",
    ]


def _is_safe_payload_relative_path(path: str) -> bool:
    candidate = Path(path)
    return (
        not candidate.is_absolute()
        and path not in ("", ".")
        and ".." not in candidate.parts
    )


def _run_schedule_attrs(
    *, auto_schedule: bool, schedule: Optional[str]
) -> Optional[dict[str, Any]]:
    if auto_schedule and schedule:
        print_error(console, "Pass either --auto-schedule or --schedule, not both.")
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
        print_error(console, f"'{bundle_path}' is not a directory. Expected a bundle folder.")
        raise typer.Exit(1)

    manifest_file = bundle_dir / "manifest.json"
    if not manifest_file.exists():
        print_error(console, f"manifest.json not found in '{bundle_path}'.")
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
        print_error(console, "Bundle requires configuration, but config.py was not found.")
        raise typer.Exit(1)

    print_warning(console, f"Bundle requires configuration; auto-running {config_script.name}.")
    res = subprocess.run([sys.executable, config_script.name], cwd=bundle_dir)
    if res.returncode != 0:
        print_error(console, "Configuration failed or was cancelled; aborting run.")
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
    stage_upload_path_payloads_for_manifest(
        manifest_dict, payloads, bundle_dir=bundle_dir
    )
    if web_ui:
        payloads.update(runtime_web_ui_support_payloads_for_manifest(manifest_dict))
    stage_blueprint_support_payloads_for_manifest(
        manifest_dict, payloads, bundle_dir=bundle_dir
    )
    stage_skill_runtime_support_payloads_for_manifest(
        manifest_dict, payloads, bundle_dir=bundle_dir
    )
    stage_skill_dependency_payloads_for_manifest(
        manifest_dict, payloads, bundle_dir=bundle_dir
    )
    return payloads


def _create_schedule_for_bundle(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    payloads: dict[str, bytes],
    schedule_attrs: dict[str, Any],
) -> None:
    stage_local_input_payloads_for_manifest(
        manifest_dict, payloads, bundle_dir=bundle_dir
    )
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


__all__ = [name for name in globals() if not name.startswith("__")]
