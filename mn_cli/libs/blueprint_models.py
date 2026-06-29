from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class BlueprintModelOps:
    load_model_catalog: Callable[[], Any]
    required_blueprint_models: Callable[..., list[dict[str, Any]]]
    load_model_ownership: Callable[[], dict[str, Any]]
    resolve_model_entry: Callable[..., dict[str, Any]]
    docker_model_name: Callable[[dict[str, Any]], str]
    cluster_provided_model: Callable[[dict[str, Any]], bool]
    record_model_owner: Callable[..., Any]
    model_installed: Callable[[str], bool]
    install_model_entry: Callable[..., dict[str, Any]]
    resolve_model_endpoint: Callable[..., dict[str, Any] | None] | None = None
    notify_model_install_start: Callable[[dict[str, Any]], Any] | None = None


def blueprint_model_dependency_summary(
    *,
    blueprint_id: str,
    blueprint_revision: str | None,
    bundle_root: Path,
    manifest: dict[str, Any],
    config: dict[str, Any],
    install_source: str,
    force: bool,
    ops: BlueprintModelOps,
) -> dict[str, Any]:
    catalog = ops.load_model_catalog()
    requirements = ops.required_blueprint_models(manifest, config, catalog=catalog)
    ledger = ops.load_model_ownership()
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    endpoints: dict[str, dict[str, Any]] = {}
    for requirement in requirements:
        model_ref = str(requirement.get("model") or "")
        try:
            entry = ops.resolve_model_entry(model_ref, catalog=catalog)
        except KeyError:
            message = f"{requirement.get('path')}: unknown runtime model {model_ref!r}"
            results.append({"model": model_ref, "status": "failed", "error": message})
            errors.append(message)
            continue

        docker_model = ops.docker_model_name(entry)
        provider = str(entry.get("provider") or "docker_model_runner")
        backend = str(requirement.get("backend") or entry.get("backend") or "auto")
        base_result = {
            "id": entry.get("id"),
            "model": docker_model,
            "provider": provider,
            "backend": backend,
            "path": requirement.get("path"),
        }
        if ops.cluster_provided_model(requirement):
            results.append({**base_result, "status": "cluster_provided"})
            continue

        if provider != "docker_model_runner":
            ops.record_model_owner(
                entry,
                blueprint_id=blueprint_id,
                blueprint_revision=blueprint_revision,
                install_source=install_source,
                backend=backend,
            )
            results.append({**base_result, "status": "service_required"})
            continue

        endpoint = None
        if ops.resolve_model_endpoint is not None:
            endpoint = ops.resolve_model_endpoint(requirement=requirement, entry=entry)
        if endpoint:
            keys = {
                str(requirement.get("name") or "").strip(),
                str(requirement.get("model") or "").strip(),
                str(entry.get("id") or "").strip(),
                docker_model,
                str(endpoint.get("model") or "").strip(),
                str(endpoint.get("runtime_model") or "").strip(),
            }
            for key in keys:
                if key:
                    endpoints[key] = endpoint
            results.append({**base_result, "status": endpoint.get("source") or "cluster_provided", "endpoint": endpoint})
            continue

        preexisting_record = ledger.get("models", {}).get(docker_model)
        try:
            installed = ops.model_installed(docker_model)
        except Exception:
            installed = False
        try:
            if installed:
                ops.record_model_owner(
                    entry,
                    blueprint_id=blueprint_id,
                    blueprint_revision=blueprint_revision,
                    install_source=install_source,
                    backend=backend,
                    preexisting_manual=not isinstance(preexisting_record, dict),
                )
                results.append({**base_result, "status": "already_installed"})
                continue
            if ops.notify_model_install_start is not None:
                ops.notify_model_install_start(base_result)
            install_result = ops.install_model_entry(
                entry,
                backend=backend,
                context_size=requirement.get("context_size"),
                force=force,
            )
            compatibility = install_result.get("compatibility") or {}
            ops.record_model_owner(
                entry,
                blueprint_id=blueprint_id,
                blueprint_revision=blueprint_revision,
                install_source=install_source,
                backend=str(compatibility.get("backend") or backend),
            )
            results.append({**base_result, "status": "installed", "compatibility": compatibility})
        except Exception as exc:
            message = str(exc)
            results.append({**base_result, "status": "failed", "error": message})
            errors.append(message)

    return {
        "blueprint_id": blueprint_id,
        "bundle_root": str(bundle_root),
        "models": results,
        "endpoints": endpoints,
        "errors": errors,
        "ok": not errors,
    }
