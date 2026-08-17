from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import subprocess
import urllib.parse
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

import typer
from mn_sdk import (
    DEFAULT_MODEL_ID,
    DOCKER_MODEL_RUNNER_HOST_API_BASE,
    AppError,
    Client,
    ModelRegistryError,
    add_registered_models,
    assess_model_compatibility,
    build_litellm_gateway_config,
    build_prepare_runtime_model_request,
    call_prepare_runtime_model,
    default_model_registry_path,
    detect_host_hardware,
    dmr_api_list_models,
    dmr_api_pull_model,
    dmr_registration,
    docker_model_match_keys,
    docker_model_name,
    docker_model_runner_endpoint,
    docker_runner_command,
    get_registered_model,
    list_model_entries,
    litellm_gateway_health,
    load_model_catalog,
    load_model_ownership,
    load_model_registry,
    load_model_remotes,
    load_provider_definition,
    model_cluster_gpu_requirement,
    reconcile_cluster_model_remotes,
    record_manual_model_install,
    registered_model_records,
    remote_model_api_base,
    remote_runtime_model_endpoint,
    remove_litellm_gateway_route,
    remove_model_record,
    remove_registered_model,
    replace_registered_model,
    resolve_cluster_model_placement,
    resolve_custom_model_placement,
    resolve_model_entry,
    run_hardware_requirements_validation,
    runtime_model_prepare_timeout_seconds,
    save_model_registry,
    set_registered_default_model,
    save_model_remotes,
    sync_litellm_gateway,
    upsert_litellm_external_routes,
    validate_litellm_gateway_config_file,
)
from mn_sdk import (
    docker_status as sdk_docker_status,
)
from mn_sdk import (
    install_model_entry as sdk_install_model_entry,
)
from mn_sdk import (
    installed_model_names as sdk_installed_model_names,
)
from mn_sdk import (
    model_entry_payload as sdk_model_entry_payload,
)
from mn_sdk import (
    model_installed as sdk_model_installed,
)
from mn_sdk import (
    parse_model_list as sdk_parse_model_list,
)
from mn_sdk import (
    remove_model_ref as sdk_remove_model_ref,
)
from mn_sdk.model_access import runtime_model_gateway_name

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.model_rendering import (
    print_compatibility as _print_compatibility,
)
from mn_cli.libs.model_rendering import (
    print_dmr_doctor as _print_doctor,
)
from mn_cli.libs.model_rendering import (
    print_model_detail as _print_model_detail,
)
from mn_cli.libs.model_rendering import (
    print_model_table as _print_model_table,
)
from mn_cli.libs.model_rendering import (
    print_provider_doctor as _print_provider_doctor,
)
from mn_cli.libs.ui import print_success_confirmation, print_warning, require_confirmation
from mn_cli.output import RemediatingTyperGroup, record_result
from mn_cli.shared import client, console, logger
from mn_cli.shared import config as cli_config

model_app = typer.Typer(
    help="Manage Docker Model Runner and provider-backed runtime models",
    cls=RemediatingTyperGroup,
)

REMOTE_DMR_SOURCE = "remote-dmr"
REMOTE_LITELLM_GATEWAY_SOURCE = "remote_litellm_gateway"
CLUSTER_REMOTE_MODEL_SOURCES = {
    REMOTE_DMR_SOURCE,
    REMOTE_LITELLM_GATEWAY_SOURCE,
}


def _handle_model_error(error: Exception, context: str) -> None:
    if isinstance(error, (ValueError, ModelRegistryError)):
        error = AppError(
            "MN_MODEL_INVALID",
            str(error),
            internal_message=str(error),
            hint="Review the model command arguments or definition and try again.",
            exit_code=2,
            http_status=422,
            cause=error,
        )
    handle_cli_error(error, console, context)


@model_app.command(name="list")
def list_models(
    available: Annotated[bool, typer.Option("--available", help="Include catalog models that have not been added.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """List registered models and discovered DMR artifacts."""
    try:
        try:
            installed_models = _installed_model_names()
        except Exception:
            installed_models = set()
        registry = load_model_registry()
        records = registered_model_records(registry)
        remote_records = [
            remote
            for remote in (load_model_remotes().get("remotes") or {}).values()
            if isinstance(remote, dict) and str(remote.get("managed_by") or "") == "mirror-neuron-cluster"
        ]
        gateway = litellm_gateway_health()
        routed_names = {str(name) for name in gateway.get("models") or []}
        models = [
            _registered_model_payload(
                record,
                installed_models=installed_models,
                remote_records=remote_records,
                routed_names=routed_names,
            )
            for record in records
        ]
        registered_dmr_keys = {
            key
            for record in records
            if record.get("kind") == "dmr"
            for key in docker_model_match_keys(str(record.get("model") or ""))
        }
        for installed_model in sorted(installed_models):
            if docker_model_match_keys(installed_model) & registered_dmr_keys:
                continue
            models.append(_unmanaged_dmr_payload(installed_model, routed_names=routed_names))
        known_model_keys = {
            key
            for item in models
            for key in _model_payload_match_keys(item)
        }
        for remote in remote_records:
            if _remote_record_match_keys(remote) & known_model_keys:
                continue
            unmanaged = _unmanaged_remote_dmr_payload(remote, routed_names=routed_names)
            models.append(unmanaged)
            known_model_keys.update(_model_payload_match_keys(unmanaged))
        if available:
            existing_ids = {str(model.get("id") or "") for model in models}
            for entry in list_model_entries(load_model_catalog()):
                model_id = str(entry.get("id") or "")
                if not model_id or model_id in existing_ids:
                    continue
                models.append(_available_model_payload(entry))
        payload = {"models": sorted(models, key=lambda item: str(item.get("id") or "")), "registry": str(default_model_registry_path())}
        if json_output:
            record_result(payload)
            return
        _print_model_table(payload["models"])
    except Exception as exc:
        _handle_model_error(exc, "model list")
        raise typer.Exit(1)


def show_model(
    model: Annotated[str, typer.Argument(help="Registered model id, catalog id, alias, or DMR reference.")] = DEFAULT_MODEL_ID,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Show stored model facts. Use `mn model doctor` for live checks."""
    try:
        record = get_registered_model(model)
        if record is not None:
            payload = _stored_registered_model_payload(record)
            payload["registration"] = record
        else:
            entry = resolve_model_entry(model, catalog=load_model_catalog())
            payload = _available_model_payload(entry)
        if json_output:
            record_result(payload)
            return
        _print_model_detail(payload)
    except Exception as exc:
        _handle_model_error(exc, "model show")
        raise typer.Exit(1)


@model_app.command(name="add")
def add_model(
    model: Annotated[str | None, typer.Argument(help="Catalog id or Docker Model Runner model reference.")] = None,
    definition_file: Annotated[Path | None, typer.Option("--file", help="Provider model definition JSON file.")] = None,
    backend: Annotated[str, typer.Option("--backend", help="Backend: auto, llama.cpp, or vllm.")] = "auto",
    context_size: Annotated[int | None, typer.Option("--context-size", help="Override model context size.")] = None,
    force: Annotated[bool, typer.Option("--force", help="Add even when hardware compatibility fails.")] = False,
    node: Annotated[str | None, typer.Option("--node", help="Add on a named runtime cluster node.")] = None,
    local: Annotated[bool, typer.Option("--local", help="Force local Docker Model Runner placement.")] = False,
    default: Annotated[bool, typer.Option("--default", help="Make the added model the highest-priority logical default.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Add a DMR model or provider definition."""
    try:
        if bool(model) == bool(definition_file):
            raise ValueError("provide exactly one MODEL argument or --file <definition.json>")
        if definition_file is not None:
            if local or node or backend != "auto" or context_size is not None or force:
                raise ValueError("DMR placement, backend, context, and force options cannot be used with --file")
            _add_provider_definition(definition_file, json_output=json_output, make_default=default)
            return
        if local and node:
            raise ValueError("--local and --node cannot be used together")
        requested = str(model or "").strip()
        if get_registered_model(requested) is not None:
            raise ValueError(f"model {requested!r} is already registered; run 'mn model remove {requested}' before adding it again")
        catalog = load_model_catalog()
        try:
            entry = resolve_model_entry(requested, catalog=catalog)
            cataloged = True
        except KeyError:
            entry = _uncataloged_dmr_entry(requested, backend=backend, context_size=context_size)
            cataloged = False
        target = docker_model_name(entry)
        requested_node = "" if local else str(node or _selected_model_install_node() or "")
        local_artifact_installed = _model_installed(target) if not requested_node else False
        selected_node = None
        if not local:
            selected_node = requested_node
            if not selected_node and not local_artifact_installed:
                selected_node = _installed_cluster_model_node(target) or _automatic_model_install_node(entry)
        if selected_node:
            result = _install_model_on_cluster_node(
                entry,
                node=selected_node,
                backend=backend,
                context_size=context_size,
                force=force,
            )
        else:
            if local_artifact_installed:
                compatibility_result = assess_model_compatibility(entry, backend=backend, force=force)
                if not compatibility_result.ok:
                    raise RuntimeError(compatibility_result.message)
                result = {
                    "entry": entry,
                    "docker_model": target,
                    "compatibility": compatibility_result.to_dict(),
                    "transport": "existing",
                    "reused": True,
                }
            elif json_output:
                result = install_model_entry(entry, backend=backend, context_size=context_size, force=force)
            else:
                result = install_model_entry_with_progress(entry, backend=backend, context_size=context_size, force=force)
        compatibility = result["compatibility"]
        target = result["docker_model"]
        if not selected_node:
            record_manual_model_install(entry, backend=compatibility["backend"])
        # Cluster-node installation already synchronizes and records the owner
        # gateway route while the target endpoint is authoritative. Running the
        # full inventory reconciliation again here can immediately replace that
        # fresh route with an older shared-status snapshot.
        if not selected_node:
            try:
                _sync_installed_model_gateway_route(entry, result=result, node=None, strict=True)
            except Exception as exc:
                raise AppError(
                    "MN_MODEL_ROUTE_FAILED",
                    f"The DMR artifact for {entry.get('id')!r} is installed but route finalization failed; it remains unmanaged.",
                    internal_message=str(exc),
                    hint=f"Run 'mn model doctor {entry.get('id')}' after fixing the managed gateway.",
                    cause=exc,
                ) from exc
        _record_runtime_model_install(entry)
        registration = dmr_registration(
            entry,
            selected_node=selected_node or _local_runtime_node_name() or "local",
            cataloged=cataloged,
        )
        registry_snapshot = load_model_registry()
        try:
            added, registry_path = add_registered_models([registration])
            if default:
                added[0], registry_path = set_registered_default_model(added[0]["id"])
                sync_litellm_gateway(restart=True)
                cluster_results = _sync_default_model_across_cluster(
                    restart=True,
                    quiet=json_output,
                )
                if any(result.get("status") == "error" for result in cluster_results):
                    raise RuntimeError(
                        "the custom default could not be synchronized to every cluster gateway"
                    )
        except Exception as exc:
            save_model_registry(registry_snapshot)
            _sync_gateway_best_effort(restart=True, quiet=True)
            _sync_default_model_across_cluster(restart=True, quiet=True)
            raise AppError(
                "MN_MODEL_REGISTRY_FAILED",
                f"The DMR artifact for {entry.get('id')!r} is installed but registration failed; it remains unmanaged.",
                internal_message=str(exc),
                hint=f"Run 'mn model doctor {entry.get('id')}' after fixing the registry.",
                cause=exc,
            ) from exc
        payload = {
            "status": "ready",
            "model": added[0],
            "docker_model": target,
            "node": selected_node or _local_runtime_node_name() or "local",
            "reused": bool(result.get("reused") or result.get("status") == "ready"),
            "registry": str(registry_path),
            "default": bool(default),
        }
        if json_output:
            record_result(payload)
            return
        print_success_confirmation(
            console,
            "Model add",
            status="ready",
            details=[
                ("Model", entry.get("id")),
                ("Docker model", target),
                ("Backend", compatibility.get("backend")),
                ("Node", payload["node"]),
                ("Route", "remote-dmr" if selected_node else "local-litellm-gateway"),
                ("Default", "yes" if default else "no"),
            ],
            next_steps=f"mn model doctor {entry.get('id')}",
        )
        if compatibility.get("warnings"):
            for warning in compatibility["warnings"]:
                print_warning(console, warning)
    except typer.Exit:
        raise
    except Exception as exc:
        _handle_model_error(exc, "model add")
        raise typer.Exit(1)


model_app.command(name="show")(show_model)


@model_app.command(name="update")
def update_model(
    model: Annotated[str | None, typer.Argument(help="Registered model id.")] = None,
    all_models: Annotated[bool, typer.Option("--all", help="Update all registered models.")] = False,
    force: Annotated[bool, typer.Option("--force", help="Update even when hardware compatibility fails.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Update a registered model from its source."""
    try:
        if bool(model) == bool(all_models):
            raise ValueError("provide one registered MODEL or --all")
        records = registered_model_records()
        selected = records if all_models else [get_registered_model(str(model or ""))]
        selected = [record for record in selected if isinstance(record, dict)]
        if not selected:
            raise ValueError(f"model is not registered: {model}")
        results = []
        provider_cache: dict[str, list[dict[str, Any]]] = {}
        for record in selected:
            if record.get("kind") == "provider":
                results.append(_update_provider_registration(record, provider_cache=provider_cache))
            else:
                results.append(_update_dmr_registration(record, force=force, json_output=json_output))
        payload = {"updated": len(results), "models": results}
        if json_output:
            record_result(payload)
            return
        for result in results:
            print_success_confirmation(
                console,
                "Model update",
                status="ready",
                details=[("Model", result.get("id")), ("Kind", result.get("kind")), ("Node", result.get("node") or "")],
                next_steps=f"mn model doctor {result.get('id')}",
            )
    except typer.Exit:
        raise
    except Exception as exc:
        _handle_model_error(exc, "model update")
        raise typer.Exit(1)


@model_app.command(name="remove")
def remove_model(
    model: Annotated[str, typer.Argument(help="Registered model id or unmanaged DMR reference.")],
    force: Annotated[bool, typer.Option("--force", help="Remove a DMR model even when blueprint owners remain.")] = False,
    keep_artifact: Annotated[bool, typer.Option("--keep-artifact", help="Unregister a DMR model without deleting its artifact.")] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Confirm removal without prompting.")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview removal without changing state.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Remove a registered model or unmanaged DMR artifact."""
    try:
        record = get_registered_model(model)
        kind = str(record.get("kind") or "dmr") if record else "dmr"
        if kind == "provider" and keep_artifact:
            raise ValueError("--keep-artifact applies only to DMR models")
        if dry_run:
            payload = {
                "status": "planned",
                "id": str((record or {}).get("id") or model),
                "kind": kind,
                "artifact_removed": kind == "dmr" and not keep_artifact,
            }
            if json_output:
                record_result(payload)
                return
            print_success_confirmation(console, "Model remove dry run", status="planned", details={"Model": payload["id"], "Kind": kind})
            return
        require_confirmation(
            console,
            action="Model removal",
            prompt=f"Remove model {model!r}?",
            yes=yes,
        )
        if kind == "provider":
            snapshot = load_model_registry()
            removed, registry_path = remove_registered_model(model)
            if removed is None:
                raise ValueError(f"model is not registered: {model}")
            try:
                remove_litellm_gateway_route(str(removed.get("id") or model))
                sync_litellm_gateway(restart=True)
                cluster_results = _remove_gateway_route_across_cluster(
                    str(removed.get("id") or model),
                    restart=True,
                    quiet=json_output,
                )
                cluster_results.extend(
                    _sync_external_litellm_config_across_cluster(
                        {"model_list": []},
                        source_path=Path(str(removed.get("definition_path") or default_model_registry_path())),
                        restart=True,
                        quiet=json_output,
                    )
                )
                if any(result.get("status") == "error" for result in cluster_results):
                    raise RuntimeError("provider route removal could not be synchronized to every cluster node")
            except Exception as exc:
                save_model_registry(snapshot)
                _sync_gateway_best_effort(restart=True, quiet=True)
                if isinstance(removed.get("litellm_entry"), dict):
                    _sync_external_litellm_config_across_cluster(
                        {"model_list": [removed["litellm_entry"]]},
                        source_path=Path(str(removed.get("definition_path") or default_model_registry_path())),
                        restart=True,
                        quiet=True,
                    )
                raise AppError(
                    "MN_MODEL_ROUTE_CLEANUP_FAILED",
                    f"Provider model {removed.get('id')!r} could not be removed from every managed gateway; its registration was restored.",
                    internal_message=str(exc),
                    hint=f"Run 'mn model doctor {removed.get('id')}' and retry removal.",
                    cause=exc,
                ) from exc
            payload = {"status": "removed", "id": removed["id"], "kind": "provider", "artifact_removed": False, "registry": str(registry_path)}
        else:
            entry = _dmr_entry_for_record_or_ref(record, model)
            target = docker_model_name(entry)
            was_default = bool((record or {}).get("default"))
            owners = _model_owner_ids(target)
            if owners and not force:
                raise ValueError(f"model is still owned by blueprints: {', '.join(owners)}; use --force to remove it")
            node = str((record or {}).get("selected_node") or "")
            artifact_removed = False
            if not keep_artifact:
                if node and node not in {"local", _local_runtime_node_name()}:
                    _remove_runtime_model_on_cluster_node(target, node=node, force=force)
                elif _model_installed(target):
                    remove_model_ref(target, force=force)
                artifact_removed = True
            registry_snapshot = load_model_registry()
            try:
                remove_model_record(target)
                remove_litellm_gateway_route(target)
                remove_litellm_gateway_route(str(entry.get("id") or model))
                _remove_remote_model_records(model, node=node or None)
                if record is not None:
                    remove_registered_model(str(record.get("id") or model))
                sync_litellm_gateway(restart=True)
                reconcile_cluster_model_routes(restart=True)
                if was_default:
                    cluster_results = _sync_default_model_across_cluster(
                        restart=True,
                        quiet=json_output,
                    )
                    if any(result.get("status") == "error" for result in cluster_results):
                        raise RuntimeError(
                            "the updated default could not be synchronized to every cluster gateway"
                        )
            except Exception as exc:
                save_model_registry(registry_snapshot)
                _sync_gateway_best_effort(restart=True, quiet=True)
                if was_default:
                    _sync_default_model_across_cluster(restart=True, quiet=True)
                raise AppError(
                    "MN_MODEL_ROUTE_CLEANUP_FAILED",
                    f"Route cleanup failed for {entry.get('id')!r} after its DMR artifact was {'removed' if artifact_removed else 'retained'}; the registration is degraded and retained for retry.",
                    internal_message=str(exc),
                    hint=f"Run 'mn model doctor {entry.get('id')}' and retry removal.",
                    cause=exc,
                ) from exc
            payload = {"status": "removed", "id": str((record or {}).get("id") or model), "kind": "dmr", "artifact_removed": artifact_removed, "artifact": target, "node": node}
        if json_output:
            record_result(payload)
            return
        print_success_confirmation(
            console,
            "Model remove",
            status="removed",
            details={"Model": payload["id"], "Kind": payload["kind"], "Artifact removed": str(payload["artifact_removed"]).lower()},
            next_steps="mn model list",
        )
    except typer.Exit:
        raise
    except Exception as exc:
        _handle_model_error(exc, "model remove")
        raise typer.Exit(1)


@model_app.command(name="doctor")
def doctor_model(
    model: Annotated[str, typer.Argument(help="Registered model id, catalog id, or DMR reference.")] = DEFAULT_MODEL_ID,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Check model definition, runtime state, and route health."""
    try:
        record = get_registered_model(model)
        if record is not None and record.get("kind") == "provider":
            payload = _doctor_provider_registration(record)
            if json_output:
                record_result(payload)
                return
            _print_provider_doctor(payload)
            if not payload.get("ok"):
                raise typer.Exit(1)
            return
        entry = _dmr_entry_for_record_or_ref(record, model)
        target = docker_model_name(entry)
        selected_node = str((record or {}).get("selected_node") or _local_runtime_node_name() or "local")
        local_node = _local_runtime_node_name()
        remote_selected = selected_node not in {"", "local", local_node}
        inventory: list[dict[str, Any]] = []
        inventory_error = ""
        if remote_selected:
            try:
                node_endpoint = next(
                    endpoint
                    for endpoint in _cluster_runtime_status_endpoints(quiet=True)
                    if str(endpoint.get("node_name") or "") == selected_node
                )
                inventory = _runtime_model_inventory_for_node(node_endpoint)
                installed = any(
                    docker_model_match_keys(target)
                    & _model_payload_match_keys(candidate)
                    for candidate in inventory
                )
                node_status = str((node_endpoint.get("node") or {}).get("status") or "healthy").lower()
                runner_running = node_status in {"healthy", "joining"}
                endpoint_ok = True
                status = {"node": selected_node, "status": node_status, "inventory": inventory}
                hardware_payload = (node_endpoint.get("node") or {}).get("resources") or node_endpoint.get("node") or {}
                compatibility_payload = _remote_node_compatibility(
                    entry,
                    selected_node=selected_node,
                    node=node_endpoint.get("node") or {},
                )
            except (RuntimeError, StopIteration) as exc:
                installed = False
                runner_running = False
                endpoint_ok = False
                inventory_error = str(exc)
                status = {"node": selected_node, "status": "unavailable", "inventory": []}
                hardware_payload = {}
                compatibility_payload = {
                    "ok": False,
                    "status": "unavailable",
                    "message": f"Could not inspect selected runtime node {selected_node!r}.",
                    "help": "Restore the node and rerun model doctor.",
                }
        else:
            compatibility = assess_model_compatibility(entry)
            compatibility_payload = compatibility.to_dict()
            status = _docker_status()
            installed = _model_installed(target)
            endpoint_ok = _endpoint_responds()
            runner_running = bool(status.get("running")) or "running" in json.dumps(status).lower()
            hardware_payload = detect_host_hardware().to_dict()
        gateway_health = litellm_gateway_health()
        gateway_config = build_litellm_gateway_config()
        gateway_config_file = validate_litellm_gateway_config_file()
        routed = bool(
            _model_route_keys(entry)
            & {str(name) for name in gateway_health.get("models") or []}
        )
        payload = {
            "model": {
                **_entry_payload(entry, installed=installed),
                "kind": "dmr",
                "registered": record is not None,
                "verification": str((record or {}).get("verification") or entry.get("verification") or "catalog"),
                "node": selected_node,
            },
            "compatibility": compatibility_payload,
            "docker_model_runner": {
                "status": status,
                "running": runner_running,
                "endpoint": DOCKER_MODEL_RUNNER_HOST_API_BASE if not remote_selected else selected_node,
                "endpoint_ok": endpoint_ok,
                "inventory_error": inventory_error,
            },
            "litellm_gateway": {
                "service": "mn-litellm-proxy",
                "endpoint": gateway_health["url"].removesuffix("/models"),
                "endpoint_ok": bool(gateway_health.get("ok")),
                "routed": routed,
                "models": gateway_health.get("models") or [],
                "config_model_count": int(gateway_config_file.get("model_count") or len(gateway_config.get("model_list") or [])),
                "config_ok": bool(gateway_config_file.get("ok")) and isinstance(gateway_config.get("model_list"), list),
                "config_path": gateway_config_file.get("path"),
                "config_error": gateway_config_file.get("error"),
            },
            "hardware": hardware_payload,
            "ok": bool(
                (compatibility_payload.get("ok") is not False)
                and installed
                and runner_running
                and endpoint_ok
                and gateway_health.get("ok")
                and routed
            ),
        }
        if json_output:
            record_result(payload)
            return
        _print_doctor(payload)
        if not payload.get("ok"):
            raise typer.Exit(1)
    except typer.Exit:
        raise
    except Exception as exc:
        _handle_model_error(exc, "model doctor")
        raise typer.Exit(1)


def _cluster_managed_remote_records() -> list[dict[str, Any]]:
    return [
        record
        for record in (load_model_remotes().get("remotes") or {}).values()
        if isinstance(record, dict) and str(record.get("managed_by") or "") == "mirror-neuron-cluster"
    ]


def _remote_node_compatibility(
    entry: dict[str, Any],
    *,
    selected_node: str,
    node: dict[str, Any],
) -> dict[str, Any]:
    if str(entry.get("verification") or "").lower() == "unverified":
        return {
            "ok": True,
            "status": "unverified_custom_model",
            "message": (
                f"{selected_node} is available, but {entry.get('id')!r} has no catalog "
                "requirements to verify against its reported resources."
            ),
            "help": "Docker Model Runner determines compatibility for this custom model during add/update.",
        }
    gpu_requirement = model_cluster_gpu_requirement(entry)
    if gpu_requirement is None:
        return {
            "ok": True,
            "status": "compatible",
            "message": f"{selected_node} is available and this catalog model has no node-specific GPU requirement.",
        }
    report = run_hardware_requirements_validation(
        {"requirements": {"gpu": gpu_requirement}},
        resource_report={"nodes": [{**node, "name": selected_node}]},
    )
    matching_nodes = {
        str(name)
        for result in report.get("results") or []
        if isinstance(result, dict)
        for name in result.get("matching_nodes") or []
    }
    compatible = bool(report.get("ok") and selected_node in matching_nodes)
    return {
        "ok": compatible,
        "status": "compatible" if compatible else "incompatible",
        "message": (
            f"{selected_node} satisfies the catalog hardware requirement."
            if compatible
            else f"{selected_node} does not satisfy the catalog hardware requirement."
        ),
        "help": "Run model update to select a compatible node." if not compatible else "",
        "requirement": gpu_requirement,
    }


def _registered_model_payload(
    record: dict[str, Any],
    *,
    installed_models: set[str],
    remote_records: list[dict[str, Any]],
    routed_names: set[str],
) -> dict[str, Any]:
    model_id = str(record.get("id") or "")
    kind = str(record.get("kind") or "")
    route_keys = {
        str(record.get("id") or ""),
        str(record.get("model") or ""),
        str(record.get("api_model") or ""),
    }
    routed = bool(
        {key for value in route_keys if value for key in docker_model_match_keys(value)}
        & {key for value in routed_names for key in docker_model_match_keys(value)}
    )
    if kind == "provider":
        installed = False
        node = ""
        state = "ready" if routed else "degraded"
    else:
        installed_keys = {key for installed_model in installed_models for key in docker_model_match_keys(installed_model)}
        installed = bool(docker_model_match_keys(str(record.get("model") or "")) & installed_keys)
        remote_installations = _remote_installations_for_model(record, remote_records)
        installed = installed or bool(remote_installations)
        node = str(record.get("selected_node") or (remote_installations[0].get("node") if remote_installations else "") or _local_runtime_node_name() or "local")
        state = "ready" if installed and routed else "degraded"
    return {
        "id": model_id,
        "name": record.get("name") or model_id,
        "kind": kind,
        "source": record.get("source") or "",
        "state": state,
        "registered": True,
        "installed": installed,
        "routed": routed,
        "node": node,
        "model": record.get("model") or model_id,
        "docker_model": record.get("model") or model_id if kind == "dmr" else None,
        "api_model": record.get("api_model") or model_id,
        "backend": record.get("backend") or "",
        "cataloged": bool(record.get("cataloged")),
        "verification": record.get("verification") or "unverified",
        "default": bool(record.get("default")),
    }


def _stored_registered_model_payload(record: dict[str, Any]) -> dict[str, Any]:
    """Render registry facts without turning ``show`` into a health probe.

    A successful add/update records a ready registration. Whether its DMR
    artifact or gateway route is still live is deliberately left unknown here;
    ``model list`` discovers inventory and ``model doctor`` performs the deep
    checks.
    """

    model_id = str(record.get("id") or "")
    kind = str(record.get("kind") or "")
    return {
        "id": model_id,
        "name": record.get("name") or model_id,
        "kind": kind,
        "source": record.get("source") or "",
        "state": str(record.get("state") or "ready"),
        "registered": True,
        "installed": None,
        "routed": None,
        "node": str(record.get("selected_node") or "") if kind == "dmr" else "",
        "model": record.get("model") or model_id,
        "docker_model": (record.get("model") or model_id) if kind == "dmr" else None,
        "api_model": record.get("api_model") or model_id,
        "backend": record.get("backend") or "",
        "cataloged": bool(record.get("cataloged")),
        "verification": record.get("verification") or "unverified",
        "default": bool(record.get("default")),
        "created_at": record.get("created_at") or "",
        "updated_at": record.get("updated_at") or "",
    }


def _unmanaged_dmr_payload(model: str, *, routed_names: set[str]) -> dict[str, Any]:
    routed = bool(docker_model_match_keys(model) & {key for name in routed_names for key in docker_model_match_keys(name)})
    entry = next(
        (
            candidate
            for candidate in list_model_entries(load_model_catalog())
            if docker_model_match_keys(docker_model_name(candidate)) & docker_model_match_keys(model)
        ),
        None,
    )
    model_id = str((entry or {}).get("id") or model)
    return {
        "id": model_id,
        "name": str((entry or {}).get("name") or model_id),
        "kind": "dmr",
        "source": "docker_model_runner",
        "state": "unmanaged",
        "registered": False,
        "installed": True,
        "routed": routed,
        "node": _local_runtime_node_name() or "local",
        "model": model,
        "docker_model": model,
        "api_model": model,
        "backend": str((entry or {}).get("backend") or "unknown"),
        "cataloged": entry is not None,
        "verification": str((entry or {}).get("verification") or ("catalog" if entry else "unverified")),
        "default": False,
    }


def _unmanaged_remote_dmr_payload(remote: dict[str, Any], *, routed_names: set[str]) -> dict[str, Any]:
    model = str(remote.get("model") or remote.get("api_model") or "").strip()
    entry = next(
        (
            candidate
            for candidate in list_model_entries(load_model_catalog())
            if docker_model_match_keys(docker_model_name(candidate)) & docker_model_match_keys(model)
        ),
        None,
    )
    model_id = str((entry or {}).get("id") or remote.get("cluster_model_id") or remote.get("name") or model)
    route_keys = {
        model_id,
        model,
        str(remote.get("api_model") or ""),
        str(remote.get("name") or ""),
    }
    routed = bool(
        {key for value in route_keys if value for key in docker_model_match_keys(value)}
        & {key for value in routed_names for key in docker_model_match_keys(value)}
    )
    return {
        "id": model_id,
        "name": str((entry or {}).get("name") or model_id),
        "kind": "dmr",
        "source": "docker_model_runner",
        "state": "unmanaged",
        "registered": False,
        "installed": True,
        "routed": routed,
        "node": str(remote.get("node") or "remote"),
        "model": model,
        "docker_model": model,
        "api_model": str(remote.get("api_model") or model),
        "backend": str((entry or {}).get("backend") or "unknown"),
        "cataloged": entry is not None,
        "verification": str((entry or {}).get("verification") or ("catalog" if entry else "unverified")),
        "default": False,
    }


def _available_model_payload(entry: dict[str, Any]) -> dict[str, Any]:
    provider = str(entry.get("provider") or "docker_model_runner")
    kind = "provider" if provider == "litellm_proxy" else "dmr"
    return {
        "id": str(entry.get("id") or entry.get("model") or ""),
        "name": entry.get("name") or entry.get("id") or entry.get("model"),
        "kind": kind,
        "source": "catalog",
        "state": "available",
        "registered": False,
        "installed": False,
        "routed": False,
        "node": "",
        "model": entry.get("model") or "",
        "api_model": entry.get("api_model") or entry.get("model") or "",
        "backend": entry.get("backend") or "",
        "cataloged": True,
        "default": False,
        "verification": entry.get("verification") or "catalog",
        "requirements": entry.get("requirements") or {},
    }


def _uncataloged_dmr_entry(model: str, *, backend: str, context_size: int | None) -> dict[str, Any]:
    requested = str(model or "").strip()
    if not requested or any(character.isspace() for character in requested) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*(?::[A-Za-z0-9][A-Za-z0-9._-]*)?", requested):
        raise ValueError("unknown models must use a valid Docker Model Runner or Hugging Face reference")
    target = docker_model_name({"id": requested, "model": requested})
    return {
        "id": requested,
        "name": requested,
        "provider": "docker_model_runner",
        "model": target,
        "api_model": target,
        "backend": "llama.cpp" if backend == "auto" else backend,
        "backends": ["llama.cpp" if backend == "auto" else backend],
        "context_size": context_size,
        "requirements": {},
        "cataloged": False,
        "verification": "unverified",
        "risk_assumed": True,
        "source_model": requested,
    }


def _add_provider_definition(
    definition_file: Path,
    *,
    json_output: bool,
    make_default: bool = False,
) -> None:
    records = load_provider_definition(definition_file)
    if make_default and len(records) != 1:
        raise ValueError("--default requires a provider definition containing exactly one model")
    conflicts = sorted(
        str(record.get("id") or "")
        for record in records
        if get_registered_model(str(record.get("id") or "")) is not None
    )
    if conflicts:
        commands = ", ".join(f"mn model remove {model_id}" for model_id in conflicts)
        raise ValueError(
            f"provider model IDs are already registered: {', '.join(conflicts)}; run {commands} before adding the replacement"
        )
    snapshot = load_model_registry()
    added, registry_path = add_registered_models(records)
    if make_default:
        added[0], registry_path = set_registered_default_model(added[0]["id"])
    litellm_config = {"model_list": [record["litellm_entry"] for record in added]}
    try:
        sync_litellm_gateway(restart=True)
        cluster_results = _sync_external_litellm_config_across_cluster(
            litellm_config,
            source_path=definition_file.expanduser().resolve(),
            restart=True,
            quiet=json_output,
        )
        failed = [result for result in cluster_results if result.get("status") == "error"]
        if failed:
            raise RuntimeError("provider definition could not be synchronized to every cluster node")
    except Exception as exc:
        save_model_registry(snapshot)
        _sync_gateway_best_effort(restart=True, quiet=True)
        for record in added:
            _remove_gateway_route_across_cluster(str(record.get("id") or ""), restart=True, quiet=True)
        _sync_external_litellm_config_across_cluster(
            {"model_list": []},
            source_path=definition_file.expanduser().resolve(),
            restart=True,
            quiet=True,
        )
        raise AppError(
            "MN_MODEL_GATEWAY_SYNC_FAILED",
            "The provider definition could not be synchronized to every managed gateway; registry and gateway state were restored.",
            internal_message=str(exc),
            hint="Check managed gateway health and add the provider definition again.",
            cause=exc,
        ) from exc
    payload = {
        "status": "ready",
        "models": [
            {"id": record["id"], "kind": "provider", "state": "ready", "source": record["source"]}
            for record in added
        ],
        "registry": str(registry_path),
        "definition": str(definition_file.expanduser().resolve()),
        "default": added[0]["id"] if make_default else None,
    }
    if json_output:
        record_result(payload)
        return
    print_success_confirmation(
        console,
        "Model add",
        status="ready",
        details=[
            ("Models", ", ".join(record["id"] for record in added)),
            ("Kind", "provider"),
            ("Gateway", "managed"),
            ("Default", added[0]["id"] if make_default else "no"),
        ],
        next_steps="mn model list",
    )


def _update_provider_registration(record: dict[str, Any], *, provider_cache: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    path = str(record.get("definition_path") or "")
    if not path:
        raise ValueError(f"provider model {record.get('id')!r} has no stored definition path")
    if path not in provider_cache:
        provider_cache[path] = load_provider_definition(path)
    records = provider_cache[path]
    replacement = next((candidate for candidate in records if candidate.get("id") == record.get("id")), None)
    if replacement is None:
        raise ValueError(f"provider definition no longer declares model {record.get('id')!r}")
    snapshot = load_model_registry()
    replacement["created_at"] = record.get("created_at")
    updated, _path = replace_registered_model(replacement)
    try:
        sync_litellm_gateway(restart=True)
        cluster_results = _sync_external_litellm_config_across_cluster(
            {"model_list": [updated["litellm_entry"]]},
            source_path=Path(path),
            restart=True,
            quiet=True,
        )
        if any(result.get("status") == "error" for result in cluster_results):
            raise RuntimeError("provider update could not be synchronized to every cluster node")
    except Exception as exc:
        save_model_registry(snapshot)
        _sync_gateway_best_effort(restart=True, quiet=True)
        if isinstance(record.get("litellm_entry"), dict):
            _sync_external_litellm_config_across_cluster(
                {"model_list": [record["litellm_entry"]]},
                source_path=Path(path),
                restart=True,
                quiet=True,
            )
        raise AppError(
            "MN_MODEL_GATEWAY_SYNC_FAILED",
            f"Provider model {record.get('id')!r} could not be updated on every managed gateway; its previous registration was restored.",
            internal_message=str(exc),
            hint=f"Run 'mn model doctor {record.get('id')}' after fixing the managed gateway.",
            cause=exc,
        ) from exc
    return {"id": updated["id"], "kind": "provider", "node": "", "state": "ready"}


def _update_dmr_registration(record: dict[str, Any], *, force: bool, json_output: bool) -> dict[str, Any]:
    entry = _dmr_entry_for_record_or_ref(record, str(record.get("id") or ""))
    node = str(record.get("selected_node") or "")
    if node and node not in {"local", _local_runtime_node_name()}:
        result = _install_model_on_cluster_node(
            entry,
            node=node,
            backend=str(record.get("backend") or "auto"),
            context_size=record.get("context_size"),
            force=force,
            update=True,
        )
    else:
        result = install_model_entry(entry, backend=str(record.get("backend") or "auto"), context_size=record.get("context_size"), force=force)
        record_manual_model_install(entry, backend=str(result.get("compatibility", {}).get("backend") or record.get("backend") or "auto"))
        try:
            _sync_installed_model_gateway_route(entry, result=result, node=None, strict=True)
        except Exception as exc:
            raise AppError(
                "MN_MODEL_ROUTE_FAILED",
                f"DMR model {record.get('id')!r} was updated but route reconciliation failed.",
                internal_message=str(exc),
                hint=f"Run 'mn model doctor {record.get('id')}' and retry the update.",
                cause=exc,
            ) from exc
    replacement = dmr_registration(
        entry,
        selected_node=node or _local_runtime_node_name() or "local",
        cataloged=bool(record.get("cataloged")),
        verification=str(record.get("verification") or "unverified"),
    )
    replacement["created_at"] = record.get("created_at")
    replace_registered_model(replacement)
    return {"id": replacement["id"], "kind": "dmr", "node": replacement["selected_node"], "state": "ready"}


def _dmr_entry_for_record_or_ref(record: dict[str, Any] | None, model: str) -> dict[str, Any]:
    if isinstance(record, dict):
        if record.get("kind") != "dmr":
            raise ValueError(f"model {model!r} is not a DMR model")
        definition = record.get("definition")
        if isinstance(definition, dict):
            return dict(definition)
        return {"id": record.get("id"), "provider": "docker_model_runner", "model": record.get("model"), "api_model": record.get("api_model")}
    try:
        entry = resolve_model_entry(model)
    except KeyError:
        entry = _uncataloged_dmr_entry(model, backend="auto", context_size=None)
    return entry


def _model_owner_ids(model: str) -> list[str]:
    wanted = docker_model_match_keys(model)
    for key, record in (load_model_ownership().get("models") or {}).items():
        if not isinstance(record, dict):
            continue
        keys = docker_model_match_keys(str(key)) | docker_model_match_keys(str(record.get("docker_model") or ""))
        if wanted & keys:
            owners = record.get("owners") if isinstance(record.get("owners"), dict) else {}
            return sorted(str(owner) for owner in owners)
    return []


def _remove_runtime_model_on_cluster_node(model: str, *, node: str, force: bool) -> None:
    node_endpoint = _cluster_node_endpoint(node)
    runtime_client = _native_runtime_client_for_node(node_endpoint)
    response = runtime_client.prepare_runtime_model({"action": "remove", "model": model, "node": node, "force": force, "source": "mn-cli"})
    payload = json.loads(response) if isinstance(response, str) else response
    if not isinstance(payload, dict) or payload.get("status") not in {"removed", "not_found"}:
        raise RuntimeError(str((payload or {}).get("error") if isinstance(payload, dict) else "remote model removal failed"))


def _doctor_provider_registration(record: dict[str, Any]) -> dict[str, Any]:
    definition_path = str(record.get("definition_path") or "")
    definition_ok = False
    definition_error = "definition path is missing"
    if definition_path:
        try:
            records = load_provider_definition(definition_path, require_environment=False)
            definition_ok = any(candidate.get("id") == record.get("id") for candidate in records)
            definition_error = "" if definition_ok else "model id is absent from the definition"
        except Exception as exc:
            definition_error = str(exc)
    env_name = str(record.get("api_key_env") or "")
    environment_ok = not env_name or bool(os.environ.get(env_name))
    health = litellm_gateway_health()
    routed = str(record.get("id") or "") in {str(name) for name in health.get("models") or []}
    return {
        "model": {"id": record.get("id"), "kind": "provider", "registered": True, "state": "ready" if definition_ok and environment_ok and health.get("ok") and routed else "degraded"},
        "definition": {"path": definition_path, "ok": definition_ok, "error": definition_error},
        "environment": {"name": env_name, "ok": environment_ok},
        "litellm_gateway": {**health, "routed": routed},
        "ok": bool(definition_ok and environment_ok and health.get("ok") and routed),
    }


def _install_model_on_cluster_node(
    entry: dict[str, Any],
    *,
    node: str,
    backend: str,
    context_size: int | None,
    force: bool,
    update: bool = False,
) -> dict[str, Any]:
    node_endpoint = _cluster_node_endpoint(node)
    runtime_client = _native_runtime_client_for_node(node_endpoint)
    docker_model = docker_model_name(entry)
    request = build_prepare_runtime_model_request(
        requirement={"model": docker_model},
        entry=entry,
        model={"id": entry.get("id"), "model": docker_model},
        node=node,
        backend=backend,
        context_size=context_size,
        force=force,
        source="mn-cli",
        action="update" if update else None,
    )
    payload = call_prepare_runtime_model(runtime_client, request, logger=logger)
    install = payload.get("install") if isinstance(payload.get("install"), dict) else {}
    compatibility = install.get("compatibility") if isinstance(install.get("compatibility"), dict) else {}
    if not compatibility:
        compatibility = {"backend": backend, "warnings": []}
    remote_endpoint = _cluster_gateway_endpoint(entry, node_endpoint=node_endpoint, payload=payload)
    try:
        sync_litellm_gateway(
            runtime_endpoints={key: remote_endpoint for key in _model_route_keys(entry)},
            restart=True,
        )
    except Exception as exc:
        raise AppError(
            "MN_MODEL_ROUTE_FAILED",
            f"The DMR artifact for {entry.get('id')!r} is installed on {node!r}, but route finalization failed.",
            internal_message=str(exc),
            hint=f"Run 'mn model doctor {entry.get('id')}' after fixing the managed gateway.",
            cause=exc,
        ) from exc
    reconcile_cluster_model_remotes(
        {key: remote_endpoint for key in _model_route_keys(entry)},
        local_installed_models=_installed_model_names(),
        local_node=_local_runtime_node_name(),
        replace=False,
    )
    return {
        "entry": entry,
        "docker_model": str(payload.get("docker_model") or docker_model),
        "compatibility": compatibility,
        "transport": "runtime_node_grpc",
        "status": str(payload.get("status") or "installed"),
        "reused": str(payload.get("status") or "") == "already_installed",
        "prepare": payload,
        "endpoint": remote_endpoint,
    }


def _selected_model_install_node() -> str:
    for name in (
        "MN_MODEL_INSTALL_NODE",
        "MN_RUNTIME_MODEL_NODE",
        "MN_SELECTED_RUNTIME_NODE",
        "MN_RUNTIME_SELECTED_NODE",
    ):
        value = str(os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def _automatic_model_install_node(entry: dict[str, Any]) -> str:
    try:
        raw_resources = client.get_resource()
        raw_systems = client.get_system_summary()
        resource_report = json.loads(raw_resources) if isinstance(raw_resources, str) else raw_resources
        system_summary = json.loads(raw_systems) if isinstance(raw_systems, str) else raw_systems
        if not isinstance(resource_report, dict) or not isinstance(system_summary, dict):
            return ""
        if str(entry.get("verification") or "").lower() == "unverified":
            placement = resolve_custom_model_placement(
                resource_report=resource_report,
                system_summary=system_summary,
            )
        else:
            systems = {
                str(node.get("name") or node.get("node") or "").strip(): node
                for node in system_summary.get("nodes") or []
                if isinstance(node, dict)
            }
            merged_nodes = []
            for resource in resource_report.get("nodes") or []:
                if not isinstance(resource, dict):
                    continue
                name = str(resource.get("name") or resource.get("node") or "").strip()
                system = systems.get(name)
                if not name or system is None:
                    continue
                merged = {**resource, **system, "name": name}
                if resource.get("devices") is not None:
                    merged["devices"] = resource["devices"]
                if resource.get("hardware") is not None:
                    merged["hardware"] = resource["hardware"]
                merged_nodes.append(merged)
            placement = resolve_cluster_model_placement(
                entry,
                resource_report={"nodes": merged_nodes},
                prefer_local=False,
            )
        return str((placement or {}).get("node") or "")
    except Exception:
        # A manual add remains usable before Core/cluster startup. Local
        # compatibility validation is still enforced by the installer.
        return ""


def _installed_cluster_model_node(model: str) -> str:
    for node_endpoint in sorted(
        _cluster_runtime_status_endpoints(quiet=True),
        key=lambda endpoint: str(endpoint.get("node_name") or ""),
    ):
        try:
            inventory = _runtime_model_inventory_for_node(node_endpoint)
        except RuntimeError:
            continue
        if any(
            docker_model_match_keys(model) & _model_payload_match_keys(candidate)
            for candidate in inventory
        ):
            return str(node_endpoint.get("node_name") or "")
    return ""


def _cluster_gateway_endpoint(
    entry: dict[str, Any],
    *,
    node_endpoint: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    return remote_runtime_model_endpoint(
        entry=entry,
        node=str(node_endpoint.get("node_name") or ""),
        node_host=str(node_endpoint.get("host") or ""),
        payload=payload,
    )


def _node_litellm_gateway_api_base(node_endpoint: dict[str, Any]) -> str:
    """Return the node's cluster-reachable LiteLLM gateway URL."""

    host = str(node_endpoint.get("host") or "").strip()
    node = node_endpoint.get("node") if isinstance(node_endpoint.get("node"), dict) else {}
    gateway = (
        node_endpoint.get("litellm_gateway")
        if isinstance(node_endpoint.get("litellm_gateway"), dict)
        else node.get("litellm_gateway")
        if isinstance(node.get("litellm_gateway"), dict)
        else {}
    )
    return remote_model_api_base({"gateway": gateway}, {}, host)


def _cluster_node_endpoint(node_name: str) -> dict[str, Any]:
    node_name = str(node_name or "").strip()
    if not node_name:
        raise RuntimeError("runtime node name is required")
    try:
        summary = json.loads(client.get_system_summary())
    except Exception as exc:
        raise RuntimeError(f"could not inspect cluster nodes for {node_name}: {exc}") from exc
    nodes = summary.get("nodes") if isinstance(summary, dict) else None
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        if str(node.get("name") or node.get("node") or "").strip() != node_name:
            continue
        host = str(node.get("grpc_host") or node.get("address") or "").strip()
        port = str(node.get("grpc_port") or "").strip()
        if not host or not port:
            raise RuntimeError(f"cluster node {node_name} does not advertise grpc_host/grpc_port")
        return {"grpc_target": f"{host}:{port}", "host": host, "port": port, "node": node, "node_name": node_name}
    raise RuntimeError(f"cluster node {node_name} was not found in runtime summary")


def _cluster_node_endpoints(*, quiet: bool = False) -> list[dict[str, Any]]:
    try:
        summary = json.loads(client.get_system_summary())
    except Exception as exc:
        if not quiet:
            print_warning(console, f"Could not inspect cluster nodes for LiteLLM gateway sync: {exc}")
        return []
    nodes = summary.get("nodes") if isinstance(summary, dict) else None
    endpoints: list[dict[str, Any]] = []
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        node_name = str(node.get("name") or node.get("node") or "").strip()
        host = str(node.get("grpc_host") or node.get("address") or "").strip()
        port = str(node.get("grpc_port") or "").strip()
        if not node_name or not host or not port:
            continue
        endpoints.append(
            {
                "grpc_target": f"{host}:{port}",
                "host": host,
                "port": port,
                "node": node,
                "node_name": node_name,
                "self": bool(node.get("self?") is True or node.get("self") is True),
            }
        )
    return endpoints


def _cluster_runtime_status_endpoints(*, quiet: bool = False) -> list[dict[str, Any]]:
    live_endpoints = _cluster_node_endpoints(quiet=quiet)
    if not live_endpoints:
        return []
    try:
        payload = json.loads(client.get_runtime_statuses())
    except Exception as exc:
        if not quiet:
            print_warning(console, f"Could not read shared runtime node status: {exc}")
        return []

    nodes = payload.get("nodes") if isinstance(payload, dict) else None
    events = payload.get("events") if isinstance(payload, dict) else None
    event_ids = [
        str(event.get("id") or "").strip()
        for event in events or []
        if isinstance(event, dict)
        and str(event.get("domain") or "").strip().lower() == "models"
        and str(event.get("id") or "").strip()
    ]
    snapshots: dict[str, dict[str, Any]] = {}
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        node_name = str(node.get("name") or node.get("node") or "").strip()
        if not node_name:
            continue
        snapshots[node_name] = node

    endpoints: list[dict[str, Any]] = []
    for live_endpoint in live_endpoints:
        node_name = str(live_endpoint.get("node_name") or "").strip()
        host = str(live_endpoint.get("host") or "").strip()
        if not node_name or not host:
            continue
        live_node = (
            live_endpoint.get("node")
            if isinstance(live_endpoint.get("node"), dict)
            else {}
        )
        snapshot = snapshots.get(node_name, {})
        node = {**snapshot, **live_node}
        if isinstance(snapshot.get("runtime_status"), dict):
            node["runtime_status"] = snapshot["runtime_status"]
        endpoint = {
            "host": host,
            "node": node,
            "node_name": node_name,
            "self": bool(live_endpoint.get("self") is True),
            "self_authoritative": True,
        }
        if endpoint["self"] and event_ids:
            endpoint["status_event_ids"] = event_ids
        endpoints.append(endpoint)
    return endpoints


def _local_cluster_node_endpoint() -> dict[str, Any] | None:
    for node_endpoint in _cluster_node_endpoints(quiet=True):
        if node_endpoint.get("self"):
            return node_endpoint
    return None


def _local_runtime_node_name() -> str:
    endpoint = _local_cluster_node_endpoint()
    return str((endpoint or {}).get("node_name") or "")


def _cluster_node_is_local(node_endpoint: dict[str, Any]) -> bool:
    if node_endpoint.get("self") is True:
        return True
    if node_endpoint.get("self_authoritative") is True:
        return False

    node = node_endpoint.get("node")
    if isinstance(node, dict) and (node.get("self?") is True or node.get("self") is True):
        return True

    host = str(node_endpoint.get("host") or "").strip().lower()
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True
    if host in _local_host_addresses():
        return True

    node_name = str(node.get("name") or node.get("node") or "").strip() if isinstance(node, dict) else ""
    return bool(node_name and node_name == _local_runtime_node_name())


@lru_cache(maxsize=1)
def _local_host_addresses() -> set[str]:
    hostnames = {"localhost", "127.0.0.1", "::1", "::", "0.0.0.0"}
    candidates: set[str] = {address.lower() for address in hostnames}
    try:
        candidates.update(_resolved_local_hostnames())
    except Exception:
        pass
    try:
        parsed = urllib.parse.urlparse(f"//{cli_config.grpc_target}")
        if parsed.hostname:
            candidates.add(parsed.hostname.lower())
    except Exception:
        pass
    for env_key in ("MN_API_HOST", "MN_GRPC_TARGET", "MN_API_BASE_URL"):
        env_value = os.getenv(env_key, "")
        if env_value:
            candidates.update(_extract_host_candidates_from_text(env_value))
    return candidates


def _extract_host_candidates_from_text(value: str) -> set[str]:
    candidates: set[str] = set()
    text = str(value or "").strip()
    if not text:
        return candidates
    for part in (text, f"//{text}"):
        parsed = urllib.parse.urlparse(part)
        if parsed.hostname:
            candidates.add(parsed.hostname.lower())
    return candidates


def _resolved_local_hostnames() -> set[str]:
    addresses: set[str] = set()
    try:
        addresses.add(socket.gethostbyname(socket.gethostname()).lower())
    except Exception:
        pass
    try:
        addresses.update(addr.lower() for addr in socket.gethostbyname_ex(socket.gethostname())[2])
    except Exception:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM):
            if len(info) >= 5:
                entry = info[4][0]
                if isinstance(entry, str):
                    addresses.add(entry.lower().split("%", 1)[0])
    except Exception:
        pass
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("10.255.255.255", 1))
            addresses.add(probe.getsockname()[0].lower())
        finally:
            probe.close()
    except Exception:
        pass
    return addresses


def _native_runtime_client_for_node(node_endpoint: dict[str, Any]) -> Client:
    if _cluster_node_is_local(node_endpoint):
        return client

    node_name = str(node_endpoint.get("node_name") or "")
    native_endpoint = _cluster_node_native_sdk_endpoint(node_name, node_endpoint["node"])
    return Client(
        target=native_endpoint["target"],
        timeout=_runtime_model_prepare_timeout_seconds(),
        auth_token=cli_config.grpc_auth_token,
        admin_token=cli_config.grpc_admin_token,
    )


def _node_native_sdk_grpc_info(node: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[Any] = [node.get("native_sdk_grpc")]
    hardware = node.get("hardware")
    if isinstance(hardware, dict):
        candidates.append(hardware.get("native_sdk_grpc"))
    node_info = node.get("node_info")
    if isinstance(node_info, dict):
        candidates.append(node_info.get("native_sdk_grpc"))
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return candidate
    return None


def _cluster_node_native_sdk_endpoint(node_name: str, node: dict[str, Any]) -> dict[str, str]:
    native = _node_native_sdk_grpc_info(node)
    if not native:
        raise RuntimeError(
            f"cluster node {node_name} does not advertise native SDK gRPC; "
            "restart that worker with an updated `mn runtime start --worker`"
        )
    if native.get("enabled") is False:
        raise RuntimeError(f"cluster node {node_name} advertises native SDK gRPC as disabled")
    target = str(native.get("target") or "").strip()
    host = str(native.get("host") or "").strip()
    port = str(native.get("port") or "").strip()
    if target and (not host or not port) and ":" in target:
        parsed_host, parsed_port = target.rsplit(":", 1)
        host = host or parsed_host.strip()
        port = port or parsed_port.strip()
    if not target and host and port:
        target = f"{host}:{port}"
    if not target or not host or not port:
        raise RuntimeError(f"cluster node {node_name} advertises incomplete native SDK gRPC metadata")
    return {"target": target, "host": host, "port": port}


def _runtime_model_prepare_timeout_seconds() -> float:
    return runtime_model_prepare_timeout_seconds()


def _sync_installed_model_gateway_route(
    entry: dict[str, Any],
    *,
    result: dict[str, Any],
    node: str | None,
    strict: bool = False,
) -> None:
    endpoint = result.get("endpoint") if isinstance(result.get("endpoint"), dict) else None
    if endpoint is None:
        endpoint = docker_model_runner_endpoint(entry, node=node, source="local-dmr")
    runtime_endpoints = {key: endpoint for key in _model_route_keys(entry)}
    if strict:
        sync_litellm_gateway(runtime_endpoints=runtime_endpoints, restart=True)
    else:
        _sync_gateway_best_effort(runtime_endpoints=runtime_endpoints, restart=True)
    reconcile_cluster_model_routes(restart=True)


def _sync_gateway_best_effort(
    *,
    runtime_endpoints: dict[str, dict[str, Any]] | None = None,
    restart: bool,
    quiet: bool = False,
) -> None:
    try:
        sync_litellm_gateway(runtime_endpoints=runtime_endpoints or {}, restart=restart)
    except Exception as exc:
        if not quiet:
            print_warning(console, f"Could not sync LiteLLM gateway: {exc}")


def _sync_external_litellm_config_across_cluster(
    litellm_config: dict[str, Any],
    *,
    source_path: Path,
    restart: bool,
    quiet: bool = False,
) -> list[dict[str, Any]]:
    forwarded_config = dict(litellm_config)
    forwarded_config["mn_default_model_id"] = str(
        load_model_registry().get("default_model_id") or ""
    )
    # Persist the fanout contract locally before contacting native runtime
    # services. The local gateway can then retain the operator-selected
    # default even when a runtime service has no access to the CLI registry.
    upsert_litellm_external_routes(
        forwarded_config,
        source_path=source_path,
        managed_by="model_registry_fanout",
    )
    results: list[dict[str, Any]] = []
    for node_endpoint in _cluster_node_endpoints(quiet=True):
        node_name = str(node_endpoint.get("node_name") or "")
        try:
            runtime_client = _native_runtime_client_for_node(node_endpoint)
            response = runtime_client.sync_litellm_gateway(
                {
                    "node": node_name,
                    "external_litellm_config": forwarded_config,
                    "external_source_path": str(source_path),
                    "external_managed_by": "model_registry_fanout",
                    "restart": restart,
                    "source": "mn-cli-model-registry-fanout",
                }
            )
            results.append({"node": node_name, "status": "ok", "response": response})
        except Exception as exc:
            results.append({"node": node_name, "status": "error", "error": str(exc)})
            if not quiet:
                print_warning(console, f"Could not sync LiteLLM proxy config on {node_name}: {exc}")
    # A cluster fanout can include this host through an independently packaged
    # native service. Reconcile once more with the CLI's current registry so an
    # older service cannot silently replace the selected default locally.
    sync_litellm_gateway(restart=restart)
    return results


def _sync_default_model_across_cluster(
    *,
    restart: bool,
    quiet: bool = False,
) -> list[dict[str, Any]]:
    """Publish only the registry default marker to every managed gateway."""
    return _sync_external_litellm_config_across_cluster(
        {"model_list": []},
        source_path=default_model_registry_path(),
        restart=restart,
        quiet=quiet,
    )


def _sync_gateway_runtime_endpoints_across_cluster(
    runtime_endpoints: dict[str, dict[str, Any]],
    *,
    restart: bool,
    quiet: bool = False,
    skip_local: bool = False,
) -> list[dict[str, Any]]:
    """Publish runtime routes to every proxy that does not own their upstream.

    A route backed by another node's Docker Model Runner must never be
    installed on its owner: the owner's local DMR route is authoritative.
    Every other node receives the owner's cluster-reachable LiteLLM URL.
    """
    results: list[dict[str, Any]] = []
    for node_endpoint in _cluster_node_endpoints(quiet=True):
        node_name = str(node_endpoint.get("node_name") or "")
        if skip_local and _cluster_node_is_local(node_endpoint):
            continue
        node_routes = _runtime_endpoints_for_gateway_node(runtime_endpoints, node_endpoint)
        if not node_routes:
            continue
        try:
            runtime_client = _native_runtime_client_for_node(node_endpoint)
            response = runtime_client.sync_litellm_gateway(
                {
                    "node": node_name,
                    "runtime_endpoints": node_routes,
                    "restart": restart,
                    "source": "mn-cli-runtime-endpoint-fanout",
                }
            )
            results.append({"node": node_name, "status": "ok", "response": response})
        except Exception as exc:
            results.append({"node": node_name, "status": "error", "error": str(exc)})
            if not quiet:
                print_warning(console, f"Could not sync LiteLLM gateway on {node_name}: {exc}")
    return results


def reconcile_cluster_model_routes(
    *,
    restart: bool = True,
    quiet: bool = False,
    expected_nodes: set[str] | None = None,
) -> dict[str, Any]:
    """Publish local model status and reconcile only this node's LiteLLM proxy."""

    node_endpoints = _cluster_runtime_status_endpoints(quiet=quiet)
    if not node_endpoints:
        return {
            "status": "unavailable",
            "models": 0,
            "routes": 0,
            "nodes": [],
            "errors": [
                {
                    "node": "local",
                    "stage": "membership",
                    "error": "no runtime nodes were available from shared runtime status",
                }
            ],
            "inventory_complete": False,
        }

    inventories: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    errors: list[dict[str, str]] = []
    node_results: list[dict[str, Any]] = []
    observed_nodes = {
        str(endpoint.get("node_name") or "").strip()
        for endpoint in node_endpoints
        if str(endpoint.get("node_name") or "").strip()
    }
    for missing_node in sorted((expected_nodes or set()) - observed_nodes):
        errors.append(
            {
                "node": missing_node,
                "stage": "membership",
                "error": "expected cluster node is temporarily absent from shared runtime status",
            }
        )

    local_endpoint = next(
        (endpoint for endpoint in node_endpoints if _cluster_node_is_local(endpoint)),
        None,
    )
    local_installed_models = _installed_model_names() if local_endpoint is not None else set()
    local_entries = _model_entries_for_installed_names(local_installed_models)
    local_revision = _runtime_model_inventory_revision(local_entries)
    reconciled_registry_models: list[dict[str, str]] = []
    if local_endpoint is None:
        errors.append(
            {
                "node": "local",
                "stage": "membership",
                "error": "local runtime node was not present in shared runtime status",
            }
        )
    else:
        local_node = str(local_endpoint.get("node_name") or "")
        try:
            reconciled_registry_models = _reconcile_stale_local_model_registrations(
                local_node=local_node,
                live_nodes=observed_nodes,
                installed_models=local_installed_models,
            )
        except Exception as exc:
            errors.append({"node": local_node, "stage": "registry", "error": str(exc)})
            if not quiet:
                print_warning(console, f"Could not reconcile local model registry ownership: {exc}")
        try:
            publish_ack = _publish_local_runtime_model_inventory(
                local_entries,
                revision=local_revision,
            )
            node_results.append(
                {
                    "node": local_node,
                    "status": "ok",
                    "inventory_revision": local_revision,
                    "publish_ack": publish_ack,
                }
            )
        except Exception as exc:
            errors.append(
                {
                    "node": local_node,
                    "stage": "publish",
                    "error": str(exc),
                }
            )
            node_results.append(
                {
                    "node": local_node,
                    "status": "error",
                    "inventory_revision": local_revision,
                    "error": str(exc),
                }
            )

    for node_endpoint in node_endpoints:
        node_name = str(node_endpoint.get("node_name") or "")
        if _cluster_node_is_local(node_endpoint):
            inventories.append((node_endpoint, local_entries))
            continue
        try:
            entries = _runtime_model_inventory_for_node(node_endpoint)
        except Exception as exc:
            entries = []
            errors.append(
                {
                    "node": node_name,
                    "stage": "inventory",
                    "error": str(exc),
                }
            )
            node_results.append(
                {"node": node_name, "status": "error", "error": str(exc)}
            )
            if not quiet:
                print_warning(console, f"No synchronized runtime model status for {node_name}: {exc}")
        else:
            snapshot = _runtime_model_status_snapshot(node_endpoint)
            node_results.append(
                {
                    "node": node_name,
                    "status": "ok",
                    "inventory_revision": str(snapshot.get("revision") or ""),
                }
            )
        inventories.append((node_endpoint, entries))

    routes = _cluster_routes_from_inventories(inventories)
    inventory_complete = not any(
        error.get("stage") in {"inventory", "membership"} for error in errors
    )
    route_version = hashlib.sha256(
        json.dumps(routes, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    sync_id = uuid.uuid4().hex
    if local_endpoint is not None:
        local_node = str(local_endpoint.get("node_name") or "")
        failure_stage = "gateway"
        try:
            local_routes = _runtime_endpoints_for_local_gateway(
                routes,
                local_endpoint,
                local_installed_models=local_installed_models,
            )
            remotes = reconcile_cluster_model_remotes(
                local_routes,
                local_installed_models=local_installed_models,
                local_node=local_node,
                replace=inventory_complete,
            )
            gateway = sync_litellm_gateway(
                runtime_endpoints=local_routes,
                restart=restart,
            )
            gateway_status = str(gateway.get("status") or "configured").strip().lower()
            if gateway_status not in {"configured", "ok", "running"}:
                raise RuntimeError(
                    "local LiteLLM reconciliation returned unsuccessful status "
                    f"{gateway_status or 'missing'}"
                )
            local_result = next(
                (result for result in node_results if result.get("node") == local_node),
                None,
            )
            if local_result is None:
                local_result = {"node": local_node, "status": "ok"}
                node_results.append(local_result)
            if local_result.get("status") == "ok":
                local_result["gateway_ack"] = {
                    "status": gateway_status,
                    "sync_id": sync_id,
                    "route_version": route_version,
                    "accepted_routes": sorted(local_routes),
                    "cluster_reconcile": inventory_complete,
                    "cluster_remote_count": sum(
                        1
                        for remote in (remotes.get("remotes") or {}).values()
                        if isinstance(remote, dict)
                        and remote.get("managed_by") == "mirror-neuron-cluster"
                    ),
                }
                event_ids = [
                    str(event_id)
                    for event_id in local_endpoint.get("status_event_ids") or []
                    if str(event_id).strip()
                ]
                if inventory_complete and not errors and event_ids:
                    failure_stage = "event_ack"
                    event_ack = _validated_runtime_status_event_ack(
                        client.ack_runtime_status_events(event_ids),
                        event_ids=event_ids,
                    )
                    local_result["gateway_ack"]["status_event_ack"] = event_ack
        except Exception as exc:
            errors.append({"node": local_node, "stage": failure_stage, "error": str(exc)})
            local_result = next(
                (result for result in node_results if result.get("node") == local_node),
                None,
            )
            if local_result is None:
                node_results.append(
                    {"node": local_node, "status": "error", "error": str(exc)}
                )
            else:
                local_result["status"] = "error"
                local_result["error"] = str(exc)
            if not quiet:
                print_warning(console, f"Could not reconcile local cluster model routes: {exc}")

    model_ids = {
        str(endpoint.get("cluster_model_id") or endpoint.get("runtime_model") or key)
        for key, endpoint in routes.items()
        if isinstance(endpoint, dict)
    }
    return {
        "status": "ok" if not errors else "warning",
        "models": len(model_ids),
        "routes": len(routes),
        "nodes": node_results,
        "errors": errors,
        "inventory_complete": inventory_complete,
        "reconciled_registry_models": reconciled_registry_models,
        "sync_id": sync_id,
        "route_version": route_version,
    }


def _reconcile_stale_local_model_registrations(
    *,
    local_node: str,
    live_nodes: set[str],
    installed_models: set[str],
) -> list[dict[str, str]]:
    """Rehome local DMR records when a laptop's advertised address changes.

    The registry is local to the CLI host, while a Core node name can contain a
    DHCP address. Only registrations whose old owner is absent from live Core
    membership and whose DMR artifact is confirmed locally installed are moved.
    Remote owners and absent artifacts remain untouched.
    """
    if not local_node or not installed_models:
        return []

    installed_keys = {
        key
        for model in installed_models
        for key in docker_model_match_keys(model)
    }
    if not installed_keys:
        return []

    registry = load_model_registry()
    records = registry.get("models") if isinstance(registry, dict) else None
    if not isinstance(records, dict):
        return []

    reconciled: list[dict[str, str]] = []
    for model_id, record in records.items():
        if not isinstance(record, dict) or record.get("kind") != "dmr":
            continue
        previous_node = str(record.get("selected_node") or "").strip()
        if not previous_node or previous_node == local_node or previous_node in live_nodes:
            continue
        if not (_model_payload_match_keys(record) & installed_keys):
            continue
        record["selected_node"] = local_node
        record["updated_at"] = datetime.now(UTC).isoformat()
        reconciled.append(
            {
                "id": str(record.get("id") or model_id),
                "previous_node": previous_node,
                "node": local_node,
            }
        )

    if reconciled:
        save_model_registry(registry)
        logger.info(
            "Rehomed %d local DMR registry record(s) after node-address change",
            len(reconciled),
        )
    return reconciled


def _validated_runtime_status_ack(
    response: Any,
    *,
    revision: str,
) -> dict[str, Any]:
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError as exc:
            raise RuntimeError("runtime model status publish returned invalid acknowledgement JSON") from exc
    if not isinstance(response, dict):
        raise RuntimeError("runtime model status publish returned no acknowledgement")
    if str(response.get("domain") or "") != "models":
        raise RuntimeError("runtime model status publish acknowledged the wrong domain")
    if str(response.get("revision") or "") != revision:
        raise RuntimeError("runtime model status publish acknowledged the wrong revision")
    status = str(response.get("status") or "").strip().lower()
    if status not in {"accepted", "unchanged"}:
        raise RuntimeError(
            "runtime model status publish returned unsuccessful acknowledgement "
            f"status {status or 'missing'}"
        )
    return response


def _runtime_model_inventory_revision(entries: list[dict[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps(entries, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _validated_runtime_status_event_ack(
    response: Any,
    *,
    event_ids: list[str],
) -> dict[str, Any]:
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError as exc:
            raise RuntimeError("runtime status event acknowledgement returned invalid JSON") from exc
    if not isinstance(response, dict) or response.get("status") != "acked":
        raise RuntimeError("runtime status events were not acknowledged")
    expected_ids = list(dict.fromkeys(event_ids))
    acked_ids = [str(event_id) for event_id in response.get("event_ids") or []]
    if acked_ids != expected_ids or int(response.get("acked_count") or 0) != len(expected_ids):
        raise RuntimeError("runtime status event acknowledgement was incomplete")
    return response


def _publish_local_runtime_model_inventory(
    entries: list[dict[str, Any]],
    *,
    revision: str,
) -> dict[str, Any]:
    response = client.publish_runtime_status(
        {
            "domain": "models",
            "revision": revision,
            "status": {"models": entries},
        }
    )
    return _validated_runtime_status_ack(response, revision=revision)


def _runtime_model_status_snapshot(node_endpoint: dict[str, Any]) -> dict[str, Any]:
    node = node_endpoint.get("node")
    runtime_status = node.get("runtime_status") if isinstance(node, dict) else None
    snapshot = runtime_status.get("models") if isinstance(runtime_status, dict) else None
    if not isinstance(snapshot, dict):
        node_name = str(node_endpoint.get("node_name") or "")
        raise RuntimeError(f"cluster node {node_name} has not published model status")
    if not str(snapshot.get("revision") or "").strip():
        node_name = str(node_endpoint.get("node_name") or "")
        raise RuntimeError(f"cluster node {node_name} published model status without a revision")
    return snapshot


def _runtime_model_inventory_for_node(
    node_endpoint: dict[str, Any],
) -> list[dict[str, Any]]:
    node_name = str(node_endpoint.get("node_name") or "")
    snapshot = _runtime_model_status_snapshot(node_endpoint)
    status = snapshot.get("status")
    models = status.get("models") if isinstance(status, dict) else None
    if not isinstance(models, list):
        raise RuntimeError(f"cluster node {node_name} published a non-list model inventory")
    return [
        entry
        for entry in models
        if isinstance(entry, dict)
        and entry.get("installed") is not False
        and str(entry.get("provider") or "docker_model_runner").strip().lower()
        == "docker_model_runner"
    ]


def _model_entries_for_installed_names(installed_models: set[str]) -> list[dict[str, Any]]:
    catalog = load_model_catalog()
    installed_keys = {
        key for model in installed_models for key in docker_model_match_keys(model)
    }
    matched_keys: set[str] = set()
    entries: list[dict[str, Any]] = []
    for entry in list_model_entries(catalog):
        model_keys = docker_model_match_keys(docker_model_name(entry))
        if not model_keys & installed_keys:
            continue
        entries.append(entry)
        matched_keys.update(model_keys)
    for model in sorted(installed_models):
        if docker_model_match_keys(model) & matched_keys:
            continue
        entries.append(
            {
                "id": model,
                "provider": "docker_model_runner",
                "model": model,
                "api_model": model,
                "backend": "unknown",
                "aliases": [],
            }
        )
    return entries


def _cluster_routes_from_inventories(
    inventories: list[tuple[dict[str, Any], list[dict[str, Any]]]],
) -> dict[str, dict[str, Any]]:
    routes: dict[str, dict[str, Any]] = {}
    for node_endpoint, entries in sorted(
        inventories,
        key=lambda item: str(item[0].get("node_name") or ""),
    ):
        owner = str(node_endpoint.get("node_name") or "").strip()
        host = str(node_endpoint.get("host") or "").strip()
        if not owner or not host:
            continue
        for entry in entries:
            if (
                str(entry.get("provider") or "docker_model_runner").strip().lower()
                != "docker_model_runner"
            ):
                continue
            try:
                runtime_model = docker_model_name(entry)
            except Exception:
                continue
            model_id = str(entry.get("id") or runtime_model).strip()
            owner_gateway_model = runtime_model_gateway_name(
                entry,
                fallback=model_id,
            )
            route_keys = _model_route_keys(entry)
            explicit_aliases = entry.get("route_aliases")
            endpoint = {
                "provider": str(entry.get("provider") or "docker_model_runner"),
                "model": owner_gateway_model,
                "runtime_model": runtime_model,
                "api_model": owner_gateway_model,
                "api_base": _node_litellm_gateway_api_base(node_endpoint),
                "api_key": "not-needed",
                "node": owner,
                "source": REMOTE_LITELLM_GATEWAY_SOURCE,
                "cluster_model_id": model_id,
                "route_aliases": (
                    [str(alias) for alias in explicit_aliases if str(alias or "").strip()]
                    if isinstance(explicit_aliases, list) and explicit_aliases
                    else sorted(route_keys)
                ),
            }
            for key in sorted(route_keys):
                routes.setdefault(key, endpoint)
    return routes


def _runtime_endpoints_for_gateway_node(
    runtime_endpoints: dict[str, dict[str, Any]],
    node_endpoint: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Return routes safe to configure on ``node_name``.

    Endpoint ownership is optional for manually declared external services, so
    those routes remain visible to every proxy.  A named owner is excluded
    only from its own proxy configuration.
    """
    target = str(node_endpoint.get("node_name") or "").strip()
    target_host = str(node_endpoint.get("host") or "").strip().lower()

    def owned_by_target(endpoint: dict[str, Any]) -> bool:
        if str(endpoint.get("node") or "").strip() == target:
            return True
        if str(endpoint.get("source") or "").strip() not in CLUSTER_REMOTE_MODEL_SOURCES:
            return False
        try:
            endpoint_host = str(urllib.parse.urlparse(str(endpoint.get("api_base") or "")).hostname or "").lower()
        except ValueError:
            return False
        return bool(target_host and endpoint_host == target_host)

    return {
        str(key): endpoint
        for key, endpoint in runtime_endpoints.items()
        if str(key).strip()
        and isinstance(endpoint, dict)
        and not owned_by_target(endpoint)
    }


def _runtime_endpoints_for_local_gateway(
    runtime_endpoints: dict[str, dict[str, Any]],
    node_endpoint: dict[str, Any],
    *,
    local_installed_models: set[str],
) -> dict[str, dict[str, Any]]:
    installed_keys = {
        key
        for model in local_installed_models
        for key in docker_model_match_keys(model)
    }
    candidates = _runtime_endpoints_for_gateway_node(runtime_endpoints, node_endpoint)

    return {
        alias: endpoint
        for alias, endpoint in candidates.items()
        if not (
            docker_model_match_keys(alias)
            | docker_model_match_keys(
                str(
                    endpoint.get("runtime_model")
                    or endpoint.get("model")
                    or endpoint.get("api_model")
                    or ""
                )
            )
        )
        & installed_keys
    }


def _remove_gateway_route_on_cluster_node(model: str, *, node: str, restart: bool) -> str:
    node_endpoint = _cluster_node_endpoint(node)
    runtime_client = _native_runtime_client_for_node(node_endpoint)
    return runtime_client.remove_litellm_gateway_route(
        {"node": node, "model": model, "restart": restart, "source": "mn-cli-remove-route"}
    )


def _remove_remote_model_records_for_node(model: str, *, node: str) -> list[dict[str, Any]]:
    return _remove_remote_model_records(model, node=node)


def _remove_remote_model_records(model: str, *, node: str | None = None) -> list[dict[str, Any]]:
    wanted = docker_model_match_keys(model)
    ledger = load_model_remotes()
    remotes = ledger.setdefault("remotes", {})
    removed: list[dict[str, Any]] = []
    for key, remote in list(remotes.items()):
        if not isinstance(remote, dict):
            continue
        if node is not None and str(remote.get("node") or "").strip() != str(node or "").strip():
            continue
        candidates = {
            str(key or "").strip(),
            str(remote.get("name") or "").strip(),
            str(remote.get("model") or "").strip(),
            str(remote.get("api_model") or "").strip(),
            str(remote.get("runtime_model") or "").strip(),
        }
        if any(candidate == model or docker_model_match_keys(candidate) & wanted for candidate in candidates if candidate):
            removed.append(remotes.pop(key))
    if removed:
        save_model_remotes(ledger)
    return removed


def _remove_gateway_route_across_cluster(
    model: str,
    *,
    origin_node: str | None = None,
    restart: bool,
    quiet: bool = False,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for node_endpoint in _cluster_node_endpoints(quiet=True):
        node_name = str(node_endpoint.get("node_name") or "")
        if origin_node and node_name == origin_node:
            continue
        try:
            runtime_client = _native_runtime_client_for_node(node_endpoint)
            response = runtime_client.remove_litellm_gateway_route(
                {
                    "node": node_name,
                    "model": model,
                    "origin_node": origin_node or "",
                    "restart": restart,
                    "source": "mn-cli-remove-route-fanout",
                }
            )
            results.append({"node": node_name, "status": "ok", "response": response})
        except Exception as exc:
            results.append({"node": node_name, "status": "error", "error": str(exc)})
            if not quiet:
                print_warning(console, f"Could not remove LiteLLM route on {node_name}: {exc}")
    return results


def _model_route_keys(entry: dict[str, Any]) -> set[str]:
    keys = {
        str(entry.get("id") or "").strip(),
        docker_model_name(entry),
        str(entry.get("api_model") or "").strip(),
    }
    keys.update(str(alias or "").strip() for alias in entry.get("aliases") or [])
    return {key for key in keys if key}


def _remote_installations_for_model(
    model: dict[str, Any],
    remote_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    model_keys = _model_payload_match_keys(model)
    installations: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for remote in remote_records:
        remote_keys = _remote_record_match_keys(remote)
        if not model_keys & remote_keys:
            continue
        node = str(remote.get("node") or "").strip()
        runtime_model = str(remote.get("model") or remote.get("api_model") or "").strip()
        base_url = str(remote.get("base_url") or "").strip()
        identity = (node, runtime_model, base_url)
        if identity in seen:
            continue
        seen.add(identity)
        installations.append(
            {
                "node": node or "remote",
                "installed": True,
                "local": False,
                "model": runtime_model,
                "api_model": remote.get("api_model") or runtime_model,
                "api_base": base_url,
                "route_source": REMOTE_DMR_SOURCE if node else "manual-remote",
            }
        )
    return sorted(
        installations,
        key=lambda item: (str(item.get("node") or ""), str(item.get("model") or "")),
    )


def _model_payload_match_keys(model: dict[str, Any]) -> set[str]:
    values = {
        str(model.get("id") or ""),
        str(model.get("model") or ""),
        str(model.get("docker_model") or ""),
        str(model.get("api_model") or ""),
    }
    values.update(str(alias or "") for alias in model.get("aliases") or [])
    return {
        key
        for value in values
        for key in docker_model_match_keys(value)
    }


def _remote_record_match_keys(remote: dict[str, Any]) -> set[str]:
    values = {
        str(remote.get("name") or ""),
        str(remote.get("model") or ""),
        str(remote.get("api_model") or ""),
        str(remote.get("cluster_model_id") or ""),
    }
    values.update(str(alias or "") for alias in remote.get("route_aliases") or [])
    return {
        key
        for value in values
        for key in docker_model_match_keys(value)
    }


def install_model_entry(
    entry: dict[str, Any],
    *,
    backend: str = "auto",
    context_size: int | None = None,
    force: bool = False,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    try:
        return sdk_install_model_entry(
            entry,
            backend=backend,
            context_size=context_size,
            force=force,
            progress_callback=progress_callback,
        )
    except RuntimeError as exc:
        compatibility = assess_model_compatibility(entry, backend=backend, force=force)
        _print_compatibility(compatibility.to_dict())
        raise exc


def install_model_entry_with_progress(
    entry: dict[str, Any],
    *,
    backend: str = "auto",
    context_size: int | None = None,
    force: bool = False,
    label: str | None = None,
) -> dict[str, Any]:
    display = label or str(entry.get("id") or entry.get("model") or "runtime model")
    with typer.progressbar(length=100, label=f"Pulling {display}") as progress:
        completed = {"value": 0}

        def on_progress(event: dict[str, Any]) -> None:
            percent = event.get("percent")
            if percent is None:
                return
            try:
                value = int(max(0, min(100, round(float(percent)))))
            except (TypeError, ValueError):
                return
            delta = value - completed["value"]
            if delta > 0:
                progress.update(delta)
                completed["value"] = value

        result = install_model_entry(
            entry,
            backend=backend,
            context_size=context_size,
            force=force,
            progress_callback=on_progress,
        )
        if completed["value"] < 100:
            progress.update(100 - completed["value"])
        return result


def _model_pull_timeout_seconds() -> float:
    try:
        return max(float(os.getenv("MN_DOCKER_MODEL_PULL_TIMEOUT_SECONDS", "3600")), 1.0)
    except ValueError:
        return 3600.0


def _pull_model(target: str) -> dict[str, Any]:
    if _endpoint_responds():
        api_result = dmr_api_pull_model(target, timeout=_model_pull_timeout_seconds())
        return {"transport": "docker_model_runner_api", "api": api_result}
    _docker_model_pull(target)
    return {"transport": "docker_cli"}


def _docker_model_pull(target: str, *, attempts: int = 2) -> None:
    last_error: RuntimeError | None = None
    for attempt in range(1, attempts + 1):
        try:
            _docker(["model", "pull", target], timeout=900, stream=True)
            return
        except RuntimeError as exc:
            if _model_installed(target):
                return
            last_error = exc
            if attempt < attempts:
                console.print("[yellow]Docker model pull failed; retrying once...[/yellow]")
    if last_error is not None:
        raise last_error


def remove_model_ref(model: str, *, force: bool = False) -> None:
    sdk_remove_model_ref(model, force=force)


def installed_model_names() -> set[str]:
    return sdk_installed_model_names()


def model_installed(model: str) -> bool:
    return sdk_model_installed(model)


def _record_runtime_model_install(entry: dict[str, Any]) -> None:
    try:
        from mn_cli.server_cmds import record_runtime_model_install

        record_runtime_model_install(entry)
    except Exception as exc:
        print_warning(console, f"Could not update runtime model advertisement: {exc}")


def _entry_payload(
    entry: dict[str, Any],
    *,
    installed: bool,
    ownership: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return sdk_model_entry_payload(entry, installed=installed, ownership=ownership)


def _ensure_docker_model_cli() -> None:
    if not _docker_model_cli_available():
        raise RuntimeError("Docker Model Runner CLI is not available. Upgrade Docker or install the docker-model plugin.")


def _docker_model_cli_available() -> bool:
    result = _docker(["model", "--help"], check=False, timeout=15)
    return result.returncode == 0


def _docker_available() -> bool:
    result = _docker(["--version"], check=False, timeout=15)
    return result.returncode == 0


def _docker_model_run_supports_context_size() -> bool:
    result = _docker(["model", "run", "--help"], check=False, timeout=15)
    return result.returncode == 0 and "--context-size" in (result.stdout or result.stderr or "")


def _ensure_runner(backend: str, accelerator: str) -> None:
    status = _docker_status()
    backends = status.get("backends") if isinstance(status.get("backends"), dict) else {}
    backend_text = str(backends.get(backend) or "").lower()
    running = bool(status.get("running")) or "running" in json.dumps(status).lower()
    if running and "running" in backend_text:
        return
    command = docker_runner_command(backend, already_running=running, accelerator=accelerator)
    _docker(command[1:], timeout=300)


def _installed_model_names() -> set[str]:
    return sdk_installed_model_names()


def _model_installed(model: str) -> bool:
    return sdk_model_installed(model)


def _docker_status() -> dict[str, Any]:
    return sdk_docker_status()


def _endpoint_responds() -> bool:
    try:
        dmr_api_list_models(timeout=3)
        return True
    except Exception:
        return False


def _parse_model_list(output: str) -> set[str]:
    return sdk_parse_model_list(output)


def _model_name_candidates(item: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for key, value in item.items():
        lowered = key.lower()
        if lowered in {"name", "model", "id", "ref", "repository"} and isinstance(value, str):
            names.add(value)
        elif lowered in {"tags", "names"} and isinstance(value, list):
            names.update(str(tag) for tag in value if tag)
    return names


def _docker(
    args: list[str],
    *,
    check: bool = True,
    timeout: float = 120,
    stream: bool = False,
) -> subprocess.CompletedProcess[str]:
    command = ["docker", *args]
    try:
        result = subprocess.run(
            command,
            capture_output=not stream,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        result = subprocess.CompletedProcess(command, 127, "", str(exc))
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"{' '.join(command)} failed{': ' + detail if detail else ''}")
    return result
