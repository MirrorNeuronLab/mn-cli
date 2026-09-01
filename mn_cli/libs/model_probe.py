from __future__ import annotations

from pathlib import Path
from typing import Any

from mn_sdk import (
    DOCKER_MODEL_RUNNER_HOST_API_BASE,
    MODEL_CAPABILITIES,
    AppError,
    ModelCapabilityReport,
    docker_model_match_keys,
    docker_model_name,
    docker_model_runner_endpoint,
    ensure_model_capabilities,
    get_registered_model,
    litellm_gateway_health,
    load_model_catalog,
    model_installed,
    normalize_model_capabilities,
    resolve_model_entry,
)
from mn_sdk.model_access import runtime_model_gateway_name


def run_model_probe(
    model: str,
    capabilities: object = None,
    *,
    local_node: str = "",
    mn_home: str | Path | None = None,
) -> dict[str, Any]:
    """Probe a model directly and through LiteLLM, then cache effective results."""

    required = normalize_model_capabilities(capabilities) or MODEL_CAPABILITIES
    record = get_registered_model(model, mn_home=mn_home)
    entry = _model_probe_entry(record, model, mn_home=mn_home)
    proxy_endpoint = _model_probe_gateway_endpoint(entry, record=record)

    direct_report: ModelCapabilityReport | None = None
    direct_endpoint = _local_model_probe_endpoint(
        entry,
        record=record,
        local_node=local_node,
    )
    if direct_endpoint is not None:
        direct_report = ensure_model_capabilities(
            str(entry.get("id") or model),
            required,
            entry=entry,
            endpoint=direct_endpoint,
            force=True,
            persist=False,
        )

    proxy_report = ensure_model_capabilities(
        str(entry.get("id") or model),
        required,
        entry=entry,
        endpoint=proxy_endpoint,
        force=True,
        persist=True,
        mn_home=mn_home,
    )
    payload = _model_probe_payload(
        entry,
        required=required,
        direct_endpoint=direct_endpoint,
        direct_report=direct_report,
        proxy_endpoint=proxy_endpoint,
        proxy_report=proxy_report,
    )
    incomplete = [
        path
        for path, report in (("direct", direct_report), ("proxy", proxy_report))
        if report is not None and (report.unknown or report.errors)
    ]
    if incomplete:
        raise AppError(
            "MN_MODEL_PROBE_FAILED",
            f"Capability probing was incomplete on: {', '.join(incomplete)}.",
            hint="Check model and gateway health, then run the probe again.",
            details=payload,
        )
    if payload.get("parity") is False:
        differences = ", ".join(payload.get("differences") or [])
        raise AppError(
            "MN_MODEL_CAPABILITY_MISMATCH",
            f"LiteLLM changed model capabilities: {differences}.",
            hint=(
                "Inspect the LiteLLM route and request translation before trusting "
                "the cached matrix."
            ),
            details=payload,
        )
    return payload


def _model_probe_entry(
    record: dict[str, Any] | None,
    model: str,
    *,
    mn_home: str | Path | None,
) -> dict[str, Any]:
    if record is not None:
        if record.get("kind") == "provider":
            return dict(record)
        definition = record.get("definition")
        if isinstance(definition, dict):
            return dict(definition)
        return {
            "id": record.get("id") or model,
            "provider": "docker_model_runner",
            "model": record.get("model") or model,
            "api_model": record.get("api_model"),
        }
    try:
        return resolve_model_entry(model, catalog=load_model_catalog(mn_home=mn_home))
    except KeyError:
        requested = str(model or "").strip()
        if not requested:
            raise ValueError("model reference cannot be empty") from None
        return {
            "id": requested,
            "provider": "docker_model_runner",
            "model": requested,
            "api_model": requested,
            "backend": "unknown",
            "requirements": {},
            "verification": "unverified",
        }


def _model_probe_gateway_endpoint(
    entry: dict[str, Any],
    *,
    record: dict[str, Any] | None,
) -> dict[str, Any]:
    health = litellm_gateway_health()
    if not health.get("ok"):
        raise AppError(
            "MN_MODEL_GATEWAY_UNAVAILABLE",
            "The managed LiteLLM gateway is unavailable.",
            hint="Run 'mn runtime doctor --repair', then retry the model probe.",
        )
    model_id = str(entry.get("id") or (record or {}).get("id") or "").strip()
    preferred = (
        model_id
        if (record or {}).get("kind") == "provider"
        else runtime_model_gateway_name(entry, fallback=model_id)
    )
    gateway_models = [
        str(value).strip()
        for value in health.get("models") or []
        if str(value or "").strip()
    ]
    route_model = preferred if preferred in gateway_models else ""
    if not route_model:
        wanted = docker_model_match_keys(preferred or model_id)
        route_model = next(
            (
                candidate
                for candidate in gateway_models
                if wanted & docker_model_match_keys(candidate)
            ),
            "",
        )
    if not route_model:
        raise AppError(
            "MN_MODEL_ROUTE_MISSING",
            f"Model {model_id!r} is not routed through the managed LiteLLM gateway.",
            hint=f"Run 'mn model add {model_id}' or 'mn model update {model_id}', then retry.",
        )
    models_url = str(health.get("url") or "").strip().rstrip("/")
    if not models_url.endswith("/models"):
        raise RuntimeError("LiteLLM gateway health did not return its models URL")
    return {
        "provider": "litellm_proxy",
        "model": route_model,
        "runtime_model": str(entry.get("model") or route_model),
        "api_model": route_model,
        "api_base": models_url.removesuffix("/models"),
        "api_key": "not-needed",
        "node": str((record or {}).get("selected_node") or ""),
        "source": "managed_litellm_gateway",
    }


def _local_model_probe_endpoint(
    entry: dict[str, Any],
    *,
    record: dict[str, Any] | None,
    local_node: str,
) -> dict[str, Any] | None:
    if (record or {}).get("kind") == "provider":
        return None
    selected_node = str((record or {}).get("selected_node") or "").strip()
    if selected_node not in {"", "local", local_node}:
        return None
    target = docker_model_name(entry)
    if not model_installed(target):
        return None
    return docker_model_runner_endpoint(
        entry,
        node=local_node or "local",
        api_base=DOCKER_MODEL_RUNNER_HOST_API_BASE,
        source="local_dmr_direct_probe",
    )


def _model_probe_payload(
    entry: dict[str, Any],
    *,
    required: tuple[str, ...],
    direct_endpoint: dict[str, Any] | None,
    direct_report: ModelCapabilityReport | None,
    proxy_endpoint: dict[str, Any],
    proxy_report: ModelCapabilityReport,
) -> dict[str, Any]:
    proxy_matrix = {
        capability: proxy_report.capabilities[capability]
        for capability in required
        if capability in proxy_report.capabilities
    }
    paths: dict[str, Any] = {
        "proxy": _model_probe_path_payload(proxy_endpoint, proxy_report),
    }
    parity: bool | None = None
    differences: list[str] = []
    if direct_endpoint is not None and direct_report is not None:
        direct_matrix = {
            capability: direct_report.capabilities[capability]
            for capability in required
            if capability in direct_report.capabilities
        }
        differences = [
            capability
            for capability in required
            if direct_matrix.get(capability) != proxy_matrix.get(capability)
        ]
        parity = not differences
        paths["direct"] = _model_probe_path_payload(direct_endpoint, direct_report)
    else:
        paths["direct"] = {
            "status": "not_run",
            "reason": (
                "Direct DMR comparison is available only when this model's "
                "selected installation is local."
            ),
        }
    incomplete = bool(proxy_report.unknown or proxy_report.errors) or bool(
        direct_report is not None
        and (direct_report.unknown or direct_report.errors)
    )
    return {
        "status": (
            "incomplete" if incomplete else "mismatch" if differences else "verified"
        ),
        "model": str(entry.get("id") or proxy_report.model_id),
        "required": list(required),
        "capabilities": proxy_matrix,
        "unsupported": [
            capability
            for capability in required
            if proxy_matrix.get(capability) is False
        ],
        "unknown": [
            capability for capability in required if capability not in proxy_matrix
        ],
        "parity": parity,
        "differences": differences,
        "catalog_path": proxy_report.catalog_path,
        "paths": paths,
    }


def _model_probe_path_payload(
    endpoint: dict[str, Any],
    report: ModelCapabilityReport,
) -> dict[str, Any]:
    report_payload = report.to_dict()
    satisfies_required = bool(report_payload.pop("ok"))
    return {
        "status": "evaluated",
        "complete": not report.unknown and not report.errors,
        "satisfies_required": satisfies_required,
        "endpoint": {
            "source": str(endpoint.get("source") or ""),
            "api_base": str(endpoint.get("api_base") or ""),
            "api_model": str(endpoint.get("api_model") or ""),
            "node": str(endpoint.get("node") or ""),
        },
        **report_payload,
    }
