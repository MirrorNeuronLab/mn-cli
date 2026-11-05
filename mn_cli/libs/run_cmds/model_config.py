"""CLI configuration composition for SDK-owned runtime-model preparation."""

from .common import (
    Any,
    Optional,
    _deep_merge_dict,
    docker_model_match_keys,
    model_installed,
)
from mn_sdk.model_preparation import prepared_runtime_model_keys


def _prepared_model_installed_resolver(model_install_summary: Optional[dict[str, Any]]):
    prepared = prepared_runtime_model_keys(model_install_summary)
    prepared_match_keys = {
        match_key
        for reference in prepared
        for match_key in docker_model_match_keys(reference)
    }

    def resolver(model_name: str, requirement: dict[str, Any]) -> bool:
        references = {
            str(model_name or "").strip(),
            str(requirement.get("model") or "").strip(),
            str(requirement.get("runtime_model") or "").strip(),
            str(requirement.get("name") or "").strip(),
        }
        candidate_match_keys = {
            match_key
            for reference in references
            for match_key in docker_model_match_keys(reference)
        }
        if candidate_match_keys & prepared_match_keys:
            return True
        return model_installed(model_name)

    return resolver


def _merge_runtime_model_config_overrides(
    config_overrides: dict[str, Any],
    model_install_summary: Optional[dict[str, Any]],
) -> None:
    patch = (
        model_install_summary.get("config_overrides")
        if isinstance(model_install_summary, dict)
        else None
    )
    if not isinstance(patch, dict) or not patch:
        return
    merged = _deep_merge_dict(config_overrides, patch)
    config_overrides.clear()
    config_overrides.update(merged)


__all__ = [name for name in globals() if not name.startswith("__")]
