from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from mn_sdk import DOCKER_MODEL_RUNNER_CONTAINER_API_BASE, resolve_llm_environment

DEFAULT_RUNS_ROOT = "~/.mn/runs"


def _inject_local_blueprint_support_path() -> None:
    import sys

    repo_root = workspace_root()
    candidate = repo_root / "mn-skills" / "blueprint_support_skill" / "src"
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


def workspace_root() -> Path:
    for name in (
        "MN_WORKSPACE_ROOT",
        "MIRROR_NEURON_WORKSPACE",
        "OTTERDESK_MIRROR_NEURON_WORKSPACE",
    ):
        value = os.getenv(name)
        if value:
            return Path(value).expanduser().resolve()
    return Path(__file__).resolve().parents[3]


def runtime_path_environment() -> dict[str, str]:
    root = workspace_root()
    membrane_project_path = Path(
        os.getenv("MN_MEMBRANE_PROJECT_PATH") or root / "Membrane"
    ).expanduser()
    membrane_sdk_path = Path(
        os.getenv("MN_MEMBRANE_SDK_PATH")
        or os.getenv("MN_CONTEXT_PYTHON_SDK_PATH")
        or membrane_project_path / "mn-context-engine-python-sdk" / "src"
    ).expanduser()
    skills_root = Path(os.getenv("MN_SKILLS_ROOT") or root / "mn-skills").expanduser()
    env = {
        "MN_WORKSPACE_ROOT": str(root),
        "MIRROR_NEURON_WORKSPACE": str(root),
        "OTTERDESK_MIRROR_NEURON_WORKSPACE": str(root),
        "MN_MEMBRANE_PROJECT_PATH": str(membrane_project_path),
        "MN_MEMBRANE_SDK_PATH": str(membrane_sdk_path),
        "MN_SKILLS_ROOT": str(skills_root),
    }
    python_paths = [
        skills_root / "blueprint_support_skill" / "src",
        skills_root / "llm_ocr_skill" / "src",
        skills_root / "pdf_extract_skill" / "src",
    ]
    existing_pythonpath = os.getenv("PYTHONPATH")
    resolved_python_paths = [str(path) for path in python_paths if path.exists()]
    if existing_pythonpath:
        resolved_python_paths.append(existing_pythonpath)
    if resolved_python_paths:
        env["PYTHONPATH"] = os.pathsep.join(resolved_python_paths)
    return env


def prepare_manifest_for_submission(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    enable_runtime_web_ui: bool = True,
) -> dict[str, Any]:
    prepared = json.loads(json.dumps(manifest_dict))
    render_agent_templates_for_submission(prepared)
    metadata = dict(submission_metadata or {})
    run_id = (
        metadata.get("blueprint_run_id")
        or metadata.get("run_id")
        or (env_overrides or {}).get("MN_RUN_ID")
    )
    runs_root = _shared_runs_root(env_overrides)
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    if run_id:
        config = with_shared_run_store_config(config, str(run_id), runs_root)
    if config is not None:
        apply_manifest_config_bindings(prepared, config)
    if enable_runtime_web_ui and run_id and config is not None and blueprint_web_ui_enabled(config):
        inject_runtime_web_ui_service_for_submission(
            prepared,
            bundle_dir=bundle_dir,
            config=config,
            run_id=str(run_id),
            runs_root=runs_root,
            env_overrides=env_overrides,
        )
    runtime_env = blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    if run_id:
        runtime_env.setdefault("MN_RUN_ID", str(run_id))
        runtime_env["MN_RUNS_ROOT"] = runs_root
    runtime_env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    if runtime_env:
        inject_node_environment(prepared, runtime_env)
    normalize_host_local_uploads(prepared)
    if metadata:
        prepared.setdefault("metadata", {}).setdefault("mn_cli", {}).update(metadata)
    return prepared


def blueprint_web_ui_enabled(config: dict[str, Any] | None) -> bool:
    web_ui = config.get("web_ui") if isinstance(config, dict) else None
    output = web_ui.get("output") if isinstance(web_ui, dict) else None
    return (
        isinstance(web_ui, dict)
        and web_ui.get("enabled") is True
        and isinstance(output, dict)
        and output.get("adapter") == "gradio"
    )


def manifest_local_inputs_enabled(manifest: dict[str, Any]) -> bool:
    for node in manifest.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        environment = (node.get("config") or {}).get("environment") or {}
        config_json = environment.get("MN_BLUEPRINT_CONFIG_JSON")
        if not config_json:
            continue
        try:
            config = json.loads(config_json)
        except (TypeError, ValueError):
            continue
        local_inputs = config.get("local_inputs") if isinstance(config, dict) else None
        if isinstance(local_inputs, dict) and (
            local_inputs.get("folders") or local_inputs.get("files")
        ):
            return True
    return False


def inject_runtime_web_ui_service_for_submission(
    manifest: dict[str, Any],
    *,
    bundle_dir: Path,
    config: dict[str, Any],
    run_id: str,
    runs_root: str,
    env_overrides: Optional[dict[str, str]] = None,
) -> dict[str, Any] | None:
    _inject_local_blueprint_support_path()
    try:
        from mn_blueprint_support import inject_runtime_web_ui_service
    except ImportError as exc:
        raise RuntimeError(
            "Blueprint web UI service injection requires mn_blueprint_support."
        ) from exc
    return inject_runtime_web_ui_service(
        manifest,
        bundle_dir=bundle_dir,
        config=config,
        run_id=run_id,
        runs_root=runs_root,
        env_overrides=env_overrides,
    )


def runtime_web_ui_support_payloads_for_manifest(manifest: dict[str, Any]) -> dict[str, bytes]:
    _inject_local_blueprint_support_path()
    try:
        from mn_blueprint_support import runtime_web_ui_service_from_manifest, runtime_web_ui_support_payloads
    except ImportError:
        return {}
    if not runtime_web_ui_service_from_manifest(manifest):
        return {}
    return runtime_web_ui_support_payloads()


def stage_local_input_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    _inject_local_blueprint_support_path()
    try:
        from mn_blueprint_support import stage_local_input_payloads_for_manifest as stage_payloads
    except ImportError as exc:
        if not manifest_local_inputs_enabled(manifest):
            return {}
        raise RuntimeError("Local blueprint input staging requires mn_blueprint_support.") from exc
    return stage_payloads(manifest, payloads, bundle_dir=bundle_dir)


def render_agent_templates_for_submission(manifest: dict[str, Any]) -> None:
    nodes = manifest.get("nodes")
    if not isinstance(nodes, list) or not any(
        isinstance(node, dict) and "uses" in node for node in nodes
    ):
        return
    _inject_local_blueprint_support_path()
    try:
        from mn_blueprint_support import render_manifest_agent_templates
    except ImportError as exc:
        raise RuntimeError(
            "Manifest uses agent templates, but mn_blueprint_support is not installed."
        ) from exc
    rendered = render_manifest_agent_templates(manifest)
    manifest.clear()
    manifest.update(rendered)


def _shared_runs_root(env_overrides: Optional[dict[str, str]] = None) -> str:
    return str(
        Path(
            (env_overrides or {}).get("MN_RUNS_ROOT")
            or os.getenv("MN_RUNS_ROOT")
            or DEFAULT_RUNS_ROOT
        ).expanduser()
    )


def with_shared_run_store_config(
    config: Optional[dict[str, Any]],
    run_id: str,
    runs_root: str,
) -> dict[str, Any]:
    resolved = json.loads(json.dumps(config or {}))
    identity = resolved.setdefault("identity", {})
    if isinstance(identity, dict):
        identity["run_id"] = run_id
    outputs = resolved.setdefault("outputs", {})
    if isinstance(outputs, dict):
        outputs["run_root"] = runs_root
        outputs.setdefault("write_run_store", True)
    return resolved


def blueprint_runtime_environment(
    bundle_dir: Path,
    *,
    config: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, str]:
    env: dict[str, str] = runtime_path_environment()
    if config is None:
        config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    if config is not None:
        env["MN_BLUEPRINT_CONFIG_JSON"] = json.dumps(config, sort_keys=True)
        projected_config = load_blueprint_config_overwrites(
            bundle_dir, config_overrides=config_overrides
        )
        if projected_config is not None:
            env.update(config_to_environment(projected_config))
        docker_model_env = resolve_llm_environment(config)
        if docker_model_env:
            env.update(docker_model_env)

    scenario_path = bundle_dir / "scenario.json"
    if scenario_path.exists():
        env["MN_BLUEPRINT_SCENARIO_JSON"] = scenario_path.read_text(encoding="utf-8")
    return env


def apply_manifest_config_bindings(
    manifest: dict[str, Any], config: dict[str, Any]
) -> None:
    bindings = config.get("manifest_config_bindings") or []
    if not isinstance(bindings, list):
        return
    for binding in bindings:
        if not isinstance(binding, dict):
            continue
        config_path = binding.get("config_path") or binding.get("from")
        manifest_path = binding.get("manifest_path") or binding.get("to")
        if not isinstance(config_path, str) or not isinstance(manifest_path, str):
            continue
        value = config_path_get(config, config_path)
        if value is None and not binding.get("allow_null", False):
            continue
        if binding.get("stringify") is True:
            value = str(value).lower() if isinstance(value, bool) else str(value)
        set_manifest_path(manifest, manifest_path, value)


def config_to_environment(config: dict[str, Any]) -> dict[str, str]:
    env: dict[str, str] = {}
    docker_model_env = resolve_llm_environment(config)
    for path, names in (
        ("video_source.uri", ("VIDEO_SOURCE_URI",)),
        ("video_source.transport", ("VIDEO_SOURCE_TRANSPORT",)),
        ("video_source.codec", ("VIDEO_SOURCE_CODEC",)),
        ("video_source.camera_id", ("VIDEO_SOURCE_CAMERA_ID",)),
        ("video_source.frame_sample_seconds", ("FRAME_SAMPLE_SECONDS",)),
        ("video_source.frame_jpeg_max_width", ("FRAME_JPEG_MAX_WIDTH",)),
        ("vl_model.base_url", ("VL_MODEL_BASE_URL", "OLLAMA_BASE_URL")),
        ("vl_model.model", ("VL_MODEL_NAME", "OLLAMA_MODEL")),
        (
            "vl_model.timeout_seconds",
            ("VL_MODEL_TIMEOUT_SECONDS", "OLLAMA_TIMEOUT_SECONDS"),
        ),
        ("vl_model.temperature", ("VL_MODEL_TEMPERATURE", "OLLAMA_TEMPERATURE")),
    ):
        value = config_path_get(config, path)
        if value is None:
            continue
        for name in names:
            env[name] = str(value)

    if docker_model_env:
        env.update(docker_model_env)
        return env

    for path, names in (
        ("llm.api_base", ("MN_LLM_API_BASE", "LITELLM_API_BASE")),
        ("llm.model", ("MN_LLM_MODEL", "LITELLM_MODEL")),
        ("llm.timeout_seconds", ("MN_LLM_TIMEOUT_SECONDS", "LITELLM_TIMEOUT_SECONDS")),
        ("llm.max_tokens", ("MN_LLM_MAX_TOKENS", "LITELLM_MAX_TOKENS")),
        ("llm.num_retries", ("MN_LLM_NUM_RETRIES", "LITELLM_NUM_RETRIES")),
    ):
        value = config_path_get(config, path)
        if value is None:
            continue
        for name in names:
            env[name] = str(value)
    return env


def set_manifest_path(target: Any, dotted_path: str, value: Any) -> None:
    parts = [part for part in dotted_path.split(".") if part]
    _set_path(target, parts, value)


def _set_path(cursor: Any, parts: list[str], value: Any) -> None:
    if not parts:
        return
    part = parts[0]
    rest = parts[1:]

    if isinstance(cursor, list):
        for item in _list_targets(cursor, part):
            _set_path(item, rest, value)
        return

    if not isinstance(cursor, dict):
        return

    if len(parts) == 1:
        cursor[part] = value
        return

    next_value = cursor.get(part)
    if isinstance(next_value, list):
        _set_path(next_value, rest, value)
        return
    if not isinstance(next_value, dict):
        next_value = {}
        cursor[part] = next_value
    _set_path(next_value, rest, value)


def _list_targets(items: list[Any], selector: str) -> list[Any]:
    if selector == "*":
        return [item for item in items if isinstance(item, dict)]
    if selector.isdigit():
        index = int(selector)
        if 0 <= index < len(items):
            return [items[index]]
        return []
    if selector.endswith("*"):
        prefix = selector[:-1]
        return [
            item
            for item in items
            if isinstance(item, dict)
            and str(item.get("node_id") or item.get("id") or "").startswith(prefix)
        ]
    return [
        item
        for item in items
        if isinstance(item, dict)
        and (
            item.get("node_id") == selector
            or item.get("id") == selector
            or item.get("edge_id") == selector
        )
    ]


def config_path_get(config: dict[str, Any], dotted_path: str) -> Any:
    cursor: Any = config
    for part in dotted_path.split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            return None
        cursor = cursor[part]
    return cursor


def load_blueprint_config(
    bundle_dir: Path,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any] | None:
    config: dict[str, Any] = {}
    loaded = False
    for path in (
        bundle_dir / "config" / "default.json",
        bundle_dir / "config" / "overwrite.json",
    ):
        if path.exists():
            config = deep_merge(config, read_json_object(path))
            loaded = True
    if config_overrides:
        config = deep_merge(config, config_overrides)
        loaded = True
    return config if loaded else None


def load_blueprint_config_overwrites(
    bundle_dir: Path,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any] | None:
    config: dict[str, Any] = {}
    loaded = False
    overwrite_path = bundle_dir / "config" / "overwrite.json"
    if overwrite_path.exists():
        config = deep_merge(config, read_json_object(overwrite_path))
        loaded = True
    if config_overrides:
        config = deep_merge(config, config_overrides)
        loaded = True
    return config if loaded else None


def inject_node_environment(manifest: dict[str, Any], env: dict[str, str]) -> None:
    for node in manifest.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        config = node.setdefault("config", {})
        if not isinstance(config, dict):
            continue
        environment = config.setdefault("environment", {})
        if not isinstance(environment, dict):
            continue
        node_env = dict(env)
        if environment.get("PYTHONPATH") and node_env.get("PYTHONPATH"):
            node_env["PYTHONPATH"] = merge_path_values(
                str(environment["PYTHONPATH"]),
                str(node_env["PYTHONPATH"]),
            )
        adjust_llm_environment_for_node(node_env, node)
        environment.update(node_env)
        add_mn_llm_aliases(environment)


def merge_path_values(*values: str) -> str:
    merged: list[str] = []
    for value in values:
        for item in value.split(os.pathsep):
            item = item.strip()
            if item and item not in merged:
                merged.append(item)
    return os.pathsep.join(merged)


def add_mn_llm_aliases(environment: dict[str, Any]) -> None:
    for legacy, primary in (
        ("LITELLM_MODEL", "MN_LLM_MODEL"),
        ("LITELLM_API_BASE", "MN_LLM_API_BASE"),
        ("LITELLM_API_KEY", "MN_LLM_API_KEY"),
        ("LITELLM_TIMEOUT_SECONDS", "MN_LLM_TIMEOUT_SECONDS"),
        ("LITELLM_MAX_TOKENS", "MN_LLM_MAX_TOKENS"),
        ("LITELLM_NUM_RETRIES", "MN_LLM_NUM_RETRIES"),
        ("LITELLM_RETRY_BACKOFF_SECONDS", "MN_LLM_RETRY_BACKOFF_SECONDS"),
    ):
        if primary not in environment and legacy in environment:
            environment[primary] = environment[legacy]


def adjust_llm_environment_for_node(environment: dict[str, Any], node: dict[str, Any]) -> None:
    if environment.get("MN_LLM_PROVIDER") != "docker_model_runner":
        return
    config = node.get("config") if isinstance(node.get("config"), dict) else {}
    if config.get("runner_module") == "MirrorNeuron.Runner.HostLocal":
        return
    api_base = str(environment.get("MN_LLM_API_BASE") or "")
    if "localhost:12434" in api_base or "127.0.0.1:12434" in api_base:
        environment["MN_LLM_API_BASE"] = DOCKER_MODEL_RUNNER_CONTAINER_API_BASE


def normalize_host_local_uploads(manifest: dict[str, Any]) -> None:
    for node in manifest.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        config = node.get("config")
        if not isinstance(config, dict):
            continue
        if config.get("runner_module") != "MirrorNeuron.Runner.HostLocal":
            continue
        upload_paths = config.get("upload_paths")
        if not isinstance(upload_paths, list) or len(upload_paths) <= 1:
            continue
        config["upload_path"] = "."
        config["upload_as"] = "."
        config.pop("upload_paths", None)


def run_mode_label(manifest: dict) -> str:
    policies = manifest.get("policies", {}) if isinstance(manifest.get("policies"), dict) else {}
    scheduler = policies.get("scheduler", {}) if isinstance(policies.get("scheduler"), dict) else {}
    manifest_type = str(
        policies.get("job_type")
        or scheduler.get("job_type")
        or manifest.get("job_type")
        or manifest.get("type")
        or "batch"
    ).lower()
    is_live = manifest_type == "service" or policies.get("stream_mode") == "live"
    if manifest_type == "service":
        return "Live service"
    if is_live:
        return "Live"
    return "Batch"


def read_json_object(path: Path) -> dict[str, Any]:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return decoded


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
