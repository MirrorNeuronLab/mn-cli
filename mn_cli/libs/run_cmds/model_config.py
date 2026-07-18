"""CLI compatibility façade for SDK-owned runtime-model preparation policy."""

from .common import *
from mn_sdk.model_preparation import (
    config_with_auto_runtime_model_profile,
    config_with_runtime_model_endpoints,
    config_with_runtime_model_fallbacks,
    config_with_runtime_model_profile,
    manifest_for_model_validation,
    model_validation_inputs_with_prepared_models,
    prepared_runtime_model_keys,
    prepared_runtime_models_json,
    runtime_model_llm_environment,
)


def _prepared_model_installed_resolver(model_install_summary: Optional[dict[str, Any]]):
    prepared = _prepared_runtime_model_keys(model_install_summary)

    def resolver(model_name: str, requirement: dict[str, Any]) -> bool:
        keys = {
            str(model_name or "").strip(),
            str(requirement.get("model") or "").strip(),
            str(requirement.get("runtime_model") or "").strip(),
            str(requirement.get("name") or "").strip(),
        }
        if any(key and key in prepared for key in keys):
            return True
        return model_installed(model_name)

    return resolver


def _merge_runtime_model_config_overrides(
    config_overrides: dict[str, Any],
    model_install_summary: Optional[dict[str, Any]],
) -> None:
    patch = model_install_summary.get("config_overrides") if isinstance(model_install_summary, dict) else None
    if not isinstance(patch, dict) or not patch:
        return
    merged = _deep_merge_dict(config_overrides, patch)
    config_overrides.clear()
    config_overrides.update(merged)


# Keep the historical CLI-private helpers as compatibility facades while the
# policy itself lives in the SDK for API/CLI parity.
_prepared_runtime_model_keys = prepared_runtime_model_keys
_prepared_runtime_models_json = prepared_runtime_models_json
_config_with_runtime_model_fallbacks = config_with_runtime_model_fallbacks
_config_with_runtime_model_profile = config_with_runtime_model_profile
_config_with_auto_runtime_model_profile = config_with_auto_runtime_model_profile
_config_with_runtime_model_endpoints = config_with_runtime_model_endpoints
_runtime_model_fallback_llm_env = runtime_model_llm_environment
_model_validation_inputs_with_prepared_models = model_validation_inputs_with_prepared_models
_manifest_for_model_validation = manifest_for_model_validation


__all__ = [name for name in globals() if not name.startswith("__")]
