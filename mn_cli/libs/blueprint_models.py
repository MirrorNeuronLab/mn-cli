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
    resolve_cluster_model: Callable[..., dict[str, Any] | None] | None = None
    install_cluster_model: Callable[..., dict[str, Any]] | None = None
    notify_model_install_start: Callable[[dict[str, Any]], Any] | None = None
    install_model_with_progress: Callable[..., dict[str, Any]] | None = None


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
            keys.update(str(alias or "").strip() for alias in entry.get("aliases") or [])
            for key in keys:
                if key:
                    endpoints[key] = endpoint
            results.append({**base_result, "status": endpoint.get("source") or "cluster_provided", "endpoint": endpoint})
            continue

        cluster_model = None
        if ops.resolve_cluster_model is not None:
            cluster_model = ops.resolve_cluster_model(requirement=requirement, entry=entry)
        if cluster_model:
            if ops.install_cluster_model is None:
                results.append({**base_result, "status": "runtime_node_install", "cluster": cluster_model})
                continue
            try:
                install_result = ops.install_cluster_model(
                    requirement=requirement,
                    entry=entry,
                    model=base_result,
                    cluster=cluster_model,
                    backend=backend,
                    context_size=requirement.get("context_size"),
                    force=force,
                )
                endpoint = install_result.get("endpoint") if isinstance(install_result, dict) else None
                if isinstance(endpoint, dict):
                    keys = {
                        str(requirement.get("name") or "").strip(),
                        str(requirement.get("model") or "").strip(),
                        str(entry.get("id") or "").strip(),
                        docker_model,
                        str(endpoint.get("model") or "").strip(),
                        str(endpoint.get("runtime_model") or "").strip(),
                    }
                    keys.update(str(alias or "").strip() for alias in entry.get("aliases") or [])
                    for key in keys:
                        if key:
                            endpoints[key] = endpoint
                install_status = ""
                if isinstance(install_result, dict) and isinstance(install_result.get("install"), dict):
                    install_status = str(install_result["install"].get("status") or "").strip().lower()
                result_status = (
                    "runtime_node_already_installed"
                    if install_status == "already_installed"
                    else "runtime_node_installed"
                )
                results.append(
                    {
                        **base_result,
                        "status": result_status,
                        "cluster": cluster_model,
                        **(install_result if isinstance(install_result, dict) else {}),
                    }
                )
            except Exception as exc:
                message = str(exc)
                results.append({**base_result, "status": "failed", "cluster": cluster_model, "error": message})
                errors.append(message)
            continue

        fallback_ref = str(entry.get("fallback_model") or "").strip()
        if fallback_ref:
            try:
                fallback_entry = ops.resolve_model_entry(fallback_ref, catalog=catalog)
                fallback_result = _prepare_fallback_model(
                    fallback_entry=fallback_entry,
                    original=base_result,
                    requirement=requirement,
                    blueprint_id=blueprint_id,
                    blueprint_revision=blueprint_revision,
                    install_source=install_source,
                    force=force,
                    ledger=ledger,
                    ops=ops,
                )
                results.append(fallback_result)
            except Exception as exc:
                message = str(exc)
                results.append({**base_result, "status": "failed", "error": message})
                errors.append(message)
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
            install_result = _install_model_entry(
                ops,
                entry,
                model=base_result,
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


def _prepare_fallback_model(
    *,
    fallback_entry: dict[str, Any],
    original: dict[str, Any],
    requirement: dict[str, Any],
    blueprint_id: str,
    blueprint_revision: str | None,
    install_source: str,
    force: bool,
    ledger: dict[str, Any],
    ops: BlueprintModelOps,
) -> dict[str, Any]:
    fallback_model = ops.docker_model_name(fallback_entry)
    fallback_backend = str(fallback_entry.get("backend") or requirement.get("backend") or "auto")
    preexisting_record = ledger.get("models", {}).get(fallback_model)
    try:
        installed = ops.model_installed(fallback_model)
    except Exception:
        installed = False
    fallback_status = "already_installed" if installed else "installed"
    compatibility = None
    if installed:
        ops.record_model_owner(
            fallback_entry,
            blueprint_id=blueprint_id,
            blueprint_revision=blueprint_revision,
            install_source=install_source,
            backend=fallback_backend,
            preexisting_manual=not isinstance(preexisting_record, dict),
        )
    else:
        fallback_base = {
            "id": fallback_entry.get("id"),
            "model": fallback_model,
            "provider": str(fallback_entry.get("provider") or "docker_model_runner"),
            "backend": fallback_backend,
            "path": requirement.get("path"),
        }
        if ops.notify_model_install_start is not None:
            ops.notify_model_install_start(fallback_base)
        install_result = _install_model_entry(
            ops,
            fallback_entry,
            model=fallback_base,
            backend=fallback_backend,
            context_size=fallback_entry.get("context_size") or requirement.get("context_size"),
            force=force,
        )
        compatibility = install_result.get("compatibility") or {}
        fallback_backend = str(compatibility.get("backend") or fallback_backend)
        ops.record_model_owner(
            fallback_entry,
            blueprint_id=blueprint_id,
            blueprint_revision=blueprint_revision,
            install_source=install_source,
            backend=fallback_backend,
        )
    fallback = {
        "id": fallback_entry.get("id"),
        "model": fallback_model,
        "provider": str(fallback_entry.get("provider") or "docker_model_runner"),
        "backend": fallback_backend,
        "context_size": fallback_entry.get("context_size"),
        "status": fallback_status,
        "reason": "no_capable_cluster_node",
    }
    if compatibility is not None:
        fallback["compatibility"] = compatibility
    return {
        **original,
        "status": "fallback_model",
        "fallback": fallback,
        "effective": fallback,
    }


def _install_model_entry(
    ops: BlueprintModelOps,
    entry: dict[str, Any],
    *,
    model: dict[str, Any],
    backend: str,
    context_size: Any,
    force: bool,
) -> dict[str, Any]:
    install = ops.install_model_with_progress or ops.install_model_entry
    if ops.install_model_with_progress is not None:
        return install(
            entry,
            model=model,
            backend=backend,
            context_size=context_size,
            force=force,
        )
    return install(
        entry,
        backend=backend,
        context_size=context_size,
        force=force,
    )
