from .common import *

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

def _prepared_runtime_models_json(model_install_summary: Optional[dict[str, Any]]) -> str:
    keys = _prepared_runtime_model_keys(model_install_summary)
    return json.dumps(sorted(keys), separators=(",", ":")) if keys else ""

def _prepared_runtime_model_keys(model_install_summary: Optional[dict[str, Any]]) -> set[str]:
    prepared_statuses = {
        "installed",
        "already_installed",
        "runtime_node_install",
        "runtime_node_already_installed",
        "runtime_node_installed",
        "fallback_model",
        "cluster_provided",
        "service_registry",
        "model_remote",
        "explicit_config",
    }
    keys: set[str] = set()
    models = model_install_summary.get("models") if isinstance(model_install_summary, dict) else []
    for item in models or []:
        if not isinstance(item, dict) or str(item.get("status") or "") not in prepared_statuses:
            continue
        for key in ("id", "model", "runtime_model", "name"):
            value = str(item.get(key) or "").strip()
            if value:
                keys.add(value)
        endpoint = item.get("endpoint") if isinstance(item.get("endpoint"), dict) else {}
        for key in ("model", "runtime_model"):
            value = str(endpoint.get(key) or "").strip()
            if value:
                keys.add(value)
        fallback = item.get("fallback") if isinstance(item.get("fallback"), dict) else {}
        for key in ("id", "model", "runtime_model", "name"):
            value = str(fallback.get(key) or "").strip()
            if value:
                keys.add(value)
    return keys

def _config_with_runtime_model_fallbacks(config: dict[str, Any], model_install_summary: dict[str, Any]) -> dict[str, Any]:
    fallback_items = [
        item for item in model_install_summary.get("models", [])
        if isinstance(item, dict)
        and str(item.get("status") or "") == "fallback_model"
        and isinstance(item.get("fallback"), dict)
    ] if isinstance(model_install_summary, dict) else []
    if not fallback_items:
        return config
    config_copy = json.loads(json.dumps(config))
    for item in fallback_items:
        fallback = item["fallback"]
        path = str(item.get("path") or "")
        if path.startswith("llm"):
            _apply_llm_model_fallback(config_copy, path, fallback)
    return config_copy

def _config_with_runtime_model_profile(config: dict[str, Any]) -> dict[str, Any]:
    llm = config.get("llm") if isinstance(config.get("llm"), dict) else {}
    if not llm:
        return config
    profile_name, profile = _matching_llm_model_profile(llm, _active_llm_model_ref(llm))
    if not profile:
        return config
    config_copy = json.loads(json.dumps(config))
    copy_llm = config_copy.setdefault("llm", {})
    if not isinstance(copy_llm, dict):
        return config
    _apply_llm_model_profile(copy_llm, profile_name, profile)
    return config_copy

def _active_llm_model_ref(llm: dict[str, Any]) -> str:
    config_name = str(llm.get("default_config") or "primary")
    configs = llm.get("configs") if isinstance(llm.get("configs"), dict) else {}
    primary = configs.get(config_name) if isinstance(configs.get(config_name), dict) else {}
    return str(
        primary.get("runtime_model")
        or primary.get("model")
        or llm.get("runtime_model")
        or llm.get("model")
        or ""
    ).strip()

def _matching_llm_model_profile(llm: dict[str, Any], model_ref: str) -> tuple[str, dict[str, Any] | None]:
    wanted = _runtime_model_match_keys(model_ref)
    if not wanted:
        return "", None
    for profile_name in ("large_model_profile", "small_model_profile", "live_model_profile"):
        profile = llm.get(profile_name)
        if not isinstance(profile, dict):
            continue
        profile_keys = _runtime_model_match_keys(str(profile.get("runtime_model") or profile.get("model") or ""))
        profile_keys.update(_runtime_model_match_keys(str(profile.get("api_model") or "")))
        if wanted & profile_keys:
            return profile_name, profile
    return "", None

def _apply_llm_model_profile(llm: dict[str, Any], profile_name: str, profile: dict[str, Any]) -> None:
    top_level_keys = {
        "provider",
        "model",
        "runtime_model",
        "fallback_model",
        "backend",
        "api_base",
        "timeout_seconds",
        "max_tokens",
        "num_retries",
        "retry_backoff_seconds",
        "context_size",
        "quantization",
        "parameter_count_b",
        "strict_json",
        "prefer_shared_skill",
        "require_live",
        "mode",
        "mock_mode",
    }
    profile_fields = {
        key: value
        for key, value in profile.items()
        if key in top_level_keys and value not in (None, "")
    }
    for stale_key in ("context_size", "quantization", "parameter_count_b"):
        if stale_key not in profile_fields:
            llm.pop(stale_key, None)
    llm.update(profile_fields)
    llm["active_model_profile"] = profile_name

    configs = llm.get("configs") if isinstance(llm.get("configs"), dict) else {}
    config_name = str(llm.get("default_config") or "primary")
    primary = configs.get(config_name)
    if not isinstance(primary, dict):
        return
    primary_keys = top_level_keys - {"strict_json", "prefer_shared_skill", "require_live"}
    primary_fields = {
        key: value
        for key, value in profile.items()
        if key in primary_keys and value not in (None, "")
    }
    for stale_key in ("context_size", "quantization", "parameter_count_b"):
        if stale_key not in primary_fields:
            primary.pop(stale_key, None)
    primary.update(primary_fields)

def _config_with_runtime_model_endpoints(config: dict[str, Any], model_install_summary: dict[str, Any]) -> dict[str, Any]:
    endpoints = model_install_summary.get("endpoints") if isinstance(model_install_summary, dict) else {}
    if not isinstance(endpoints, dict) or not endpoints:
        return config
    llm = config.get("llm") if isinstance(config.get("llm"), dict) else {}
    if not llm:
        return config

    config_copy = json.loads(json.dumps(config))
    copy_llm = config_copy.setdefault("llm", {})
    if not isinstance(copy_llm, dict):
        return config

    changed = _apply_llm_endpoint_if_matching(copy_llm, endpoints)
    configs = copy_llm.get("configs") if isinstance(copy_llm.get("configs"), dict) else {}
    for profile in configs.values():
        if isinstance(profile, dict):
            changed = _apply_llm_endpoint_if_matching(profile, endpoints) or changed
    return config_copy if changed else config

def _apply_llm_endpoint_if_matching(llm: dict[str, Any], endpoints: dict[str, Any]) -> bool:
    model_ref = str(llm.get("runtime_model") or llm.get("model") or "").strip()
    endpoint = _endpoint_for_model_ref(model_ref, endpoints)
    if not endpoint:
        return False
    api_base = str(endpoint.get("api_base") or "").strip()
    if not api_base:
        return False
    llm["api_base"] = api_base
    llm["provider"] = str(endpoint.get("provider") or llm.get("provider") or "docker_model_runner")
    if endpoint.get("model"):
        llm["model"] = str(endpoint["model"])
    if endpoint.get("runtime_model"):
        llm["runtime_model"] = str(endpoint["runtime_model"])
    if endpoint.get("api_key"):
        llm["api_key"] = str(endpoint["api_key"])
    return True

def _endpoint_for_model_ref(model_ref: str, endpoints: dict[str, Any]) -> dict[str, Any] | None:
    if not model_ref:
        return None
    wanted = _runtime_model_match_keys(model_ref)
    for key, endpoint in endpoints.items():
        if not isinstance(endpoint, dict):
            continue
        endpoint_keys = _runtime_model_match_keys(str(key))
        endpoint_keys.update(_runtime_model_match_keys(str(endpoint.get("model") or "")))
        endpoint_keys.update(_runtime_model_match_keys(str(endpoint.get("runtime_model") or "")))
        endpoint_keys.update(_runtime_model_match_keys(str(endpoint.get("api_model") or "")))
        if wanted & endpoint_keys:
            return endpoint
    return None

def _runtime_model_match_keys(value: str) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()
    keys = {text}
    changed = True
    while changed:
        changed = False
        for key in list(keys):
            variants = {
                key.removeprefix("docker.io/") if key.startswith("docker.io/") else key,
                key.removeprefix("registry-1.docker.io/") if key.startswith("registry-1.docker.io/") else key,
                key.removeprefix("ai/") if key.startswith("ai/") else key,
                key.removesuffix(":latest") if key.endswith(":latest") else key,
            }
            if "/" not in key:
                variants.add(f"ai/{key}")
            for variant in variants:
                if variant and variant not in keys:
                    keys.add(variant)
                    changed = True
    return keys

def _apply_llm_model_fallback(config: dict[str, Any], path: str, fallback: dict[str, Any]) -> None:
    llm = config.setdefault("llm", {})
    if not isinstance(llm, dict):
        return
    fallback_id = str(fallback.get("id") or "").strip()
    fallback_model = fallback_id or str(fallback.get("model") or "").strip()
    fallback_backend = str(fallback.get("backend") or "auto").strip()
    context_size = fallback.get("context_size")
    fields = {
        "provider": str(fallback.get("provider") or "docker_model_runner"),
        "model": fallback_model,
        "runtime_model": fallback_model,
        "backend": fallback_backend,
    }
    if context_size is not None:
        fields["context_size"] = context_size
    for key, value in fields.items():
        if value not in (None, ""):
            llm[key] = value
    llm.pop("quantization", None)
    llm.pop("parameter_count_b", None)
    live_profile = llm.get("live_model_profile")
    if isinstance(live_profile, dict):
        live_profile.update({key: value for key, value in fields.items() if value not in (None, "")})
        live_profile.pop("quantization", None)
        live_profile.pop("parameter_count_b", None)
        live_profile.pop("hardware", None)
        live_profile["fallback_from"] = str(fallback.get("reason") or "runtime_model_fallback")

    configs = llm.get("configs") if isinstance(llm.get("configs"), dict) else {}
    config_name = _llm_config_name_from_path(path) or str(llm.get("default_config") or "primary")
    primary = configs.get(config_name)
    if isinstance(primary, dict):
        primary.update({key: value for key, value in fields.items() if value not in (None, "")})
        primary.pop("quantization", None)
        primary.pop("parameter_count_b", None)

def _llm_config_name_from_path(path: str) -> str:
    parts = [part for part in str(path or "").split(".") if part]
    if len(parts) >= 3 and parts[0] == "llm" and parts[1] == "configs":
        return parts[2]
    return ""

def _runtime_model_fallback_llm_env(config: dict[str, Any]) -> dict[str, str]:
    llm = config.get("llm") if isinstance(config.get("llm"), dict) else {}
    config_name = str(llm.get("default_config") or "primary")
    configs = llm.get("configs") if isinstance(llm.get("configs"), dict) else {}
    primary = configs.get(config_name) if isinstance(configs.get(config_name), dict) else {}
    model_ref = str(primary.get("runtime_model") or primary.get("model") or llm.get("runtime_model") or llm.get("model") or "").strip()
    env = {}
    if model_ref:
        try:
            entry = resolve_model_entry(model_ref)
        except KeyError:
            entry = {}
        env["MN_LLM_MODEL"] = docker_api_model_name(entry) if entry else model_ref
        env["MN_LLM_RUNTIME_MODEL"] = docker_model_name(entry) if entry else model_ref
    provider = str(primary.get("provider") or llm.get("provider") or "docker_model_runner").strip()
    if provider:
        env["MN_LLM_PROVIDER"] = provider
    api_base = str(primary.get("api_base") or llm.get("api_base") or "").strip()
    if api_base:
        env["MN_LLM_API_BASE"] = api_base
        env["LITELLM_API_BASE"] = api_base
    backend = str(primary.get("backend") or llm.get("backend") or "").strip()
    if backend:
        env["MN_LLM_BACKEND"] = backend
    context_size = primary.get("context_size") or llm.get("context_size")
    if context_size is not None:
        env["MN_LLM_CONTEXT_SIZE"] = str(context_size)
    return env

def _model_validation_inputs_with_prepared_models(
    manifest: dict[str, Any],
    config: dict[str, Any],
    model_install_summary: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    prepared = _prepared_runtime_model_keys(model_install_summary)
    if not prepared:
        return manifest, config
    manifest_copy = json.loads(json.dumps(manifest))
    config_copy = json.loads(json.dumps(config))
    runtime = manifest_copy.get("runtime") if isinstance(manifest_copy.get("runtime"), dict) else {}
    models = runtime.get("models") if isinstance(runtime.get("models"), dict) else {}
    for entry in models.values():
        if isinstance(entry, dict) and _model_config_matches_prepared(entry, prepared):
            entry["install_mode"] = "cluster_provided"
    llm = config_copy.get("llm") if isinstance(config_copy.get("llm"), dict) else {}
    configs = llm.get("configs") if isinstance(llm.get("configs"), dict) else {}
    for entry in configs.values():
        if isinstance(entry, dict) and _model_config_matches_prepared(entry, prepared):
            entry["install_mode"] = "cluster_provided"
    if isinstance(llm, dict) and _model_config_matches_prepared(llm, prepared):
        llm["install_mode"] = "cluster_provided"
    return manifest_copy, config_copy

def _model_config_matches_prepared(config: dict[str, Any], prepared: set[str]) -> bool:
    values = {
        str(config.get("runtime_model") or "").strip(),
        str(config.get("model") or "").strip(),
        str(config.get("model_alias") or "").strip(),
    }
    return any(_model_match_keys(value) & prepared for value in values if value)

def _model_match_keys(model: str) -> set[str]:
    value = str(model or "").strip()
    if not value:
        return set()
    keys = {value}
    lower = value.lower()
    if lower.startswith("ai/"):
        keys.add(value[3:])
    elif "/" not in value:
        keys.add(f"ai/{value}")
    return keys

def _manifest_for_model_validation(manifest: dict[str, Any], config: dict[str, Any] | None) -> dict[str, Any]:
    llm = config.get("llm") if isinstance(config, dict) and isinstance(config.get("llm"), dict) else {}
    mode = str(llm.get("mode") or "").strip().lower()
    provider = str(llm.get("provider") or "").strip().lower()
    if mode != "fake" and provider != "fake":
        return manifest
    filtered = json.loads(json.dumps(manifest))
    runtime = filtered.get("runtime") if isinstance(filtered.get("runtime"), dict) else None
    models = runtime.get("models") if isinstance(runtime, dict) and isinstance(runtime.get("models"), dict) else None
    if isinstance(models, dict):
        runtime["models"] = {
            name: entry
            for name, entry in models.items()
            if not isinstance(entry, dict)
            or str(entry.get("provider") or entry.get("mode") or "").strip().lower()
            not in {"", "docker_model_runner", "docker-model-runner", "dmr"}
        }
    return filtered


__all__ = [name for name in globals() if not name.startswith("__")]
