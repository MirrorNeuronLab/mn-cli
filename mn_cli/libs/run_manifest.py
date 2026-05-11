from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional


def prepare_manifest_for_submission(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    prepared = json.loads(json.dumps(manifest_dict))
    runtime_env = blueprint_runtime_environment(bundle_dir)
    runtime_env.update({key: str(value) for key, value in (env_overrides or {}).items() if value is not None})
    if runtime_env:
        inject_node_environment(prepared, runtime_env)
    metadata = dict(submission_metadata or {})
    if metadata:
        prepared.setdefault("metadata", {}).setdefault("mn_cli", {}).update(metadata)
    return prepared


def blueprint_runtime_environment(bundle_dir: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for filename, env_name in (
        ("config/default.json", "MN_BLUEPRINT_CONFIG_JSON"),
        ("scenario.json", "MN_BLUEPRINT_SCENARIO_JSON"),
    ):
        path = bundle_dir / filename
        if path.exists():
            env[env_name] = path.read_text(encoding="utf-8")
    return env


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
