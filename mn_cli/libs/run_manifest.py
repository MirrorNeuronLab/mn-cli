from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

DEFAULT_RUNS_ROOT = "~/.mn/runs"


def prepare_manifest_for_submission(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    prepared = json.loads(json.dumps(manifest_dict))
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
    runtime_env = blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    if run_id:
        runtime_env.setdefault("MN_RUN_ID", str(run_id))
        runtime_env["MN_RUNS_ROOT"] = runs_root
    runtime_env.update({key: str(value) for key, value in (env_overrides or {}).items() if value is not None})
    if runtime_env:
        inject_node_environment(prepared, runtime_env)
    if metadata:
        prepared.setdefault("metadata", {}).setdefault("mn_cli", {}).update(metadata)
    return prepared


def _shared_runs_root(env_overrides: Optional[dict[str, str]] = None) -> str:
    return str(
        (env_overrides or {}).get("MN_RUNS_ROOT")
        or os.getenv("MN_RUNS_ROOT")
        or DEFAULT_RUNS_ROOT
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
    env: dict[str, str] = {}
    if config is None:
        config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    if config is not None:
        env["MN_BLUEPRINT_CONFIG_JSON"] = json.dumps(config, sort_keys=True)
        projected_config = load_blueprint_config_overwrites(bundle_dir, config_overrides=config_overrides)
        if projected_config is not None:
            env.update(config_to_environment(projected_config))

    scenario_path = bundle_dir / "scenario.json"
    if scenario_path.exists():
        env["MN_BLUEPRINT_SCENARIO_JSON"] = scenario_path.read_text(encoding="utf-8")
    return env


def apply_manifest_config_bindings(manifest: dict[str, Any], config: dict[str, Any]) -> None:
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
    for path, names in (
        ("video_source.uri", ("VIDEO_SOURCE_URI",)),
        ("video_source.transport", ("VIDEO_SOURCE_TRANSPORT",)),
        ("video_source.codec", ("VIDEO_SOURCE_CODEC",)),
        ("video_source.camera_id", ("VIDEO_SOURCE_CAMERA_ID",)),
        ("video_source.frame_sample_seconds", ("FRAME_SAMPLE_SECONDS",)),
        ("video_source.frame_jpeg_max_width", ("FRAME_JPEG_MAX_WIDTH",)),
        ("vl_model.base_url", ("VL_MODEL_BASE_URL", "OLLAMA_BASE_URL")),
        ("vl_model.model", ("VL_MODEL_NAME", "OLLAMA_MODEL")),
        ("vl_model.timeout_seconds", ("VL_MODEL_TIMEOUT_SECONDS", "OLLAMA_TIMEOUT_SECONDS")),
        ("vl_model.temperature", ("VL_MODEL_TEMPERATURE", "OLLAMA_TEMPERATURE")),
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
            if isinstance(item, dict) and str(item.get("node_id") or "").startswith(prefix)
        ]
    return [
        item
        for item in items
        if isinstance(item, dict) and (item.get("node_id") == selector or item.get("edge_id") == selector)
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
        environment.update(env)
        add_mn_llm_aliases(environment)


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


def run_mode_label(manifest: dict) -> str:
    is_live = manifest.get("daemon") is True or manifest.get("policies", {}).get("stream_mode") == "live"
    if is_live and manifest.get("daemon") is True:
        return "Live daemon"
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
