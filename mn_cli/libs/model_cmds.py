from __future__ import annotations

import json
import os
import hashlib
import re
import subprocess
import socket
import urllib.parse
import uuid
from pathlib import Path
from functools import lru_cache
from typing import Annotated, Any, Callable, Optional

import typer
from rich.table import Table

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import print_confirmation, print_confirmed, print_success_confirmation, print_warning
from mn_cli.shared import client, config as cli_config, console, logger
from mn_sdk import (
    Client,
    DEFAULT_MODEL_ID,
    DOCKER_MODEL_RUNNER_HOST_API_BASE,
    build_prepare_runtime_model_request,
    call_prepare_runtime_model,
    build_litellm_gateway_config,
    assess_model_compatibility,
    default_model_proxies_path,
    detect_host_hardware,
    dmr_api_list_models,
    dmr_api_pull_model,
    docker_model_match_keys,
    docker_model_runner_endpoint,
    docker_model_name,
    docker_runner_command,
    default_model_remotes_path,
    litellm_gateway_health,
    litellm_gateway_internal_api_base,
    list_model_entries,
    load_model_catalog,
    load_model_ownership,
    load_model_remotes,
    merge_catalog_and_installed_models,
    proxy_model_ids,
    reconcile_cluster_model_remotes,
    record_manual_model_install,
    remove_litellm_gateway_route,
    remove_model_proxy,
    remove_model_remote,
    remove_model_record,
    save_model_remotes,
    sync_litellm_gateway,
    validate_litellm_gateway_config_file,
    resolve_model_entry,
    runtime_model_prepare_timeout_seconds,
    upsert_model_proxy,
    upsert_model_remote,
)
from mn_sdk import (
    docker_status as sdk_docker_status,
    install_model_entry as sdk_install_model_entry,
    installed_model_names as sdk_installed_model_names,
    model_entry_payload as sdk_model_entry_payload,
    model_installed as sdk_model_installed,
    parse_model_list as sdk_parse_model_list,
    remove_model_ref as sdk_remove_model_ref,
)


model_app = typer.Typer(help="Manage local Docker Model Runner models")
remote_app = typer.Typer(help="Manage remote model endpoints")
model_app.add_typer(remote_app, name="remote")

REMOTE_DMR_SOURCE = "remote-dmr"
LEGACY_REMOTE_LITELLM_SOURCE = "remote_litellm_gateway"
CLUSTER_REMOTE_MODEL_SOURCES = {
    REMOTE_DMR_SOURCE,
    LEGACY_REMOTE_LITELLM_SOURCE,
}


@model_app.command(name="list")
def list_models(
    installed: Annotated[bool, typer.Option("--installed", help="Only show installed Docker Model Runner models.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """List known or installed runtime models."""
    try:
        catalog = load_model_catalog()
        try:
            installed_models = _installed_model_names()
        except Exception:
            installed_models = set()
        installed_model_ids = installed_models | proxy_model_ids()
        ownership = load_model_ownership()
        entries = merge_catalog_and_installed_models(
            catalog=catalog,
            installed_models=installed_model_ids,
            ownership=ownership,
        )
        remote_records = [
            remote
            for remote in (load_model_remotes().get("remotes") or {}).values()
            if isinstance(remote, dict)
        ]
        installed_model_keys = {key for model in installed_model_ids for key in docker_model_match_keys(model)}

        def is_installed(entry: dict[str, Any]) -> bool:
            if _is_proxy_entry(entry):
                return True
            return bool(docker_model_match_keys(docker_model_name(entry)) & installed_model_keys)

        payload = {
            "models": [
                _entry_payload(
                    entry,
                    installed=is_installed(entry),
                    ownership=ownership,
                )
                for entry in entries
            ]
        }
        for model_payload in payload["models"]:
            local_installed = bool(model_payload.get("installed"))
            remote_installations = _remote_installations_for_model(
                model_payload,
                remote_records,
            )
            installations: list[dict[str, Any]] = []
            if local_installed:
                installations.append(
                    {
                        "node": _local_runtime_node_name() or "local",
                        "installed": True,
                        "local": True,
                        "route_source": "local-dmr",
                    }
                )
            installations.extend(remote_installations)
            model_payload["installations"] = installations
            if remote_installations and not local_installed:
                preferred = remote_installations[0]
                model_payload["installed"] = True
                model_payload["backend"] = "remote-dmr"
                model_payload["status"] = "remote"
                model_payload["node"] = preferred.get("node") or ""
                model_payload["route_source"] = REMOTE_DMR_SOURCE
            else:
                model_payload["route_source"] = _route_source_for_model_payload(model_payload)
        payload["models"].extend(
            _remote_model_payloads(
                existing_entries=payload["models"],
                remote_records=remote_records,
            )
        )
        if installed:
            payload["models"] = [entry for entry in payload["models"] if entry.get("installed")]
        if json_output:
            console.print_json(data=payload)
            return
        _print_model_table(payload["models"])
    except Exception as exc:
        handle_cli_error(exc, console, "model list")
        raise typer.Exit(1)


@model_app.command(name="show")
def show_model(
    model: Annotated[str, typer.Argument(help="Model id, alias, or Docker model reference.")] = DEFAULT_MODEL_ID,
    compatibility: Annotated[bool, typer.Option("--compatibility", help="Include host hardware compatibility.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Show model catalog details."""
    try:
        catalog = load_model_catalog()
        entry = resolve_model_entry(model, catalog=catalog)
        payload = _entry_payload(entry, installed=_model_installed(docker_model_name(entry)))
        if compatibility:
            payload["compatibility"] = assess_model_compatibility(entry).to_dict()
            payload["hardware"] = detect_host_hardware().to_dict()
        if json_output:
            console.print_json(data=payload)
            return
        _print_model_detail(payload)
    except Exception as exc:
        handle_cli_error(exc, console, "model show")
        raise typer.Exit(1)


@model_app.command(name="install")
def install_model(
    model: Annotated[str, typer.Argument(help="Model id, alias, or Docker model reference.")] = DEFAULT_MODEL_ID,
    backend: Annotated[str, typer.Option("--backend", help="Backend: auto, llama.cpp, or vllm.")] = "auto",
    context_size: Annotated[Optional[int], typer.Option("--context-size", help="Override model context size.")] = None,
    force: Annotated[bool, typer.Option("--force", help="Install even when hardware compatibility fails.")] = False,
    node: Annotated[Optional[str], typer.Option("--node", help="Install on a named runtime cluster node.")] = None,
    local: Annotated[bool, typer.Option("--local", help="Force local Docker Model Runner install.")] = False,
):
    """Pull and start a Docker Model Runner model."""
    try:
        if local and node:
            raise ValueError("--local and --node cannot be used together")
        catalog = load_model_catalog()
        entry = resolve_model_entry(model, catalog=catalog)
        selected_node = None if local else (node or _selected_model_install_node())
        if selected_node:
            result = _install_model_on_cluster_node(
                entry,
                node=selected_node,
                backend=backend,
                context_size=context_size,
                force=force,
            )
        else:
            result = install_model_entry_with_progress(entry, backend=backend, context_size=context_size, force=force)
        compatibility = result["compatibility"]
        target = result["docker_model"]
        if not selected_node:
            record_manual_model_install(entry, backend=compatibility["backend"])
        _sync_installed_model_gateway_route(entry, result=result, node=selected_node)
        _record_runtime_model_install(entry)
        print_success_confirmation(
            console,
            "Model install",
            status="running",
            details=[
                ("Model", entry.get("id")),
                ("Docker model", target),
                ("Backend", compatibility.get("backend")),
                ("Route", "remote-dmr" if selected_node else "local-litellm-gateway"),
            ],
            next_steps=f"mn model doctor {entry.get('id')}",
        )
        if compatibility.get("warnings"):
            for warning in compatibility["warnings"]:
                print_warning(console, warning)
    except typer.Exit:
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "model install")
        raise typer.Exit(1)


@model_app.command(name="update")
def update_model(
    model: Annotated[Optional[str], typer.Argument(help="Model id, alias, or Docker model reference.")] = None,
    all_models: Annotated[bool, typer.Option("--all", help="Update all installed catalog models.")] = False,
    force: Annotated[bool, typer.Option("--force", help="Update even when hardware compatibility fails.")] = False,
):
    """Pull the latest model artifact and restart it."""
    try:
        catalog = load_model_catalog()
        entries = list_model_entries(catalog) if all_models else [resolve_model_entry(model or DEFAULT_MODEL_ID, catalog=catalog)]
        installed = _installed_model_names() if all_models else None
        updated = 0
        for entry in entries:
            target = docker_model_name(entry)
            if installed is not None and target not in installed:
                continue
            compatibility = assess_model_compatibility(entry, force=force)
            if not compatibility.ok:
                _print_compatibility(compatibility.to_dict())
                raise typer.Exit(1)
            _ensure_docker_model_cli()
            _ensure_runner(compatibility.backend, compatibility.accelerator)
            _docker(["model", "pull", target], timeout=900, stream=True)
            _docker(["model", "run", "--detach", target], timeout=300)
            record_manual_model_install(entry, backend=compatibility.backend)
            _sync_installed_model_gateway_route(
                entry,
                result={
                    "entry": entry,
                    "docker_model": target,
                    "compatibility": compatibility.to_dict(),
                },
                node=None,
            )
            _record_runtime_model_install(entry)
            print_success_confirmation(
                console,
                "Model update",
                status="running",
                details=[("Model", entry.get("id")), ("Docker model", target), ("Backend", compatibility.backend)],
                next_steps=f"mn model doctor {entry.get('id')}",
            )
            updated += 1
        if all_models and updated == 0:
            console.print("[yellow]No installed catalog models found to update.[/yellow]")
    except typer.Exit:
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "model update")
        raise typer.Exit(1)


@model_app.command(name="remove")
def remove_model(
    model: Annotated[str, typer.Argument(help="Model id, alias, or Docker model reference.")],
    force: Annotated[bool, typer.Option("--force", help="Force removal when Docker supports it.")] = False,
    node: Annotated[Optional[str], typer.Option("--node", help="Remove this model route from a named runtime cluster node and reconcile all node gateways.")] = None,
):
    """Remove a Docker Model Runner model."""
    try:
        if node:
            _remove_gateway_route_on_cluster_node(model, node=node, restart=True)
            removed_remotes = _remove_remote_model_records_for_node(model, node=node)
            remove_litellm_gateway_route(model)
            for removed_remote in removed_remotes:
                remove_litellm_gateway_route(str(removed_remote.get("name") or ""))
                remove_litellm_gateway_route(str(removed_remote.get("model") or ""))
                remove_litellm_gateway_route(str(removed_remote.get("api_model") or ""))
            _sync_gateway_best_effort(restart=True)
            reconcile_cluster_model_routes(restart=True)
            print_success_confirmation(
                console,
                "Model route remove",
                status="removed",
                details={"Model": model, "Node": node},
                next_steps="mn model list --installed",
            )
            return
        try:
            entry = resolve_model_entry(model)
        except KeyError:
            entry = None
        if _is_proxy_entry(entry) or entry is None:
            proxy_name = str(entry.get("id") or model) if entry else model
            removed = remove_model_proxy(proxy_name)
            if removed is not None or _is_proxy_entry(entry):
                remove_litellm_gateway_route(proxy_name)
                _remove_gateway_route_across_cluster(proxy_name, restart=True)
                print_success_confirmation(
                    console,
                    "Model proxy",
                    status="removed",
                    details={"Model": proxy_name, "Backend": "proxy"},
                    next_steps="mn model list --installed",
                )
                return
        target = docker_model_name(entry) if entry else _resolve_or_raw_model(model)
        removed_remotes = _remove_remote_model_records(model)
        local_installed = model_installed(target)
        if local_installed:
            remove_model_ref(target, force=force)
        remove_model_record(target)
        remove_litellm_gateway_route(target)
        if entry:
            remove_litellm_gateway_route(str(entry.get("id") or ""))
        for removed_remote in removed_remotes:
            remove_litellm_gateway_route(str(removed_remote.get("name") or ""))
            remove_litellm_gateway_route(str(removed_remote.get("model") or ""))
            remove_litellm_gateway_route(str(removed_remote.get("api_model") or ""))
        _sync_gateway_best_effort(restart=True)
        reconcile_cluster_model_routes(restart=True)
        print_success_confirmation(
            console,
            "Model remove",
            status="removed",
            details={"Model": target, "Routes cleared": str(len(removed_remotes))},
            next_steps="mn model list --installed",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "model remove")
        raise typer.Exit(1)


@model_app.command(name="proxy")
def proxy_model(
    config: Annotated[Path, typer.Option("--config", help="Proxy config file. Supports LiteLLM model_list or MirrorNeuron provider JSON.")],
    port: Annotated[int, typer.Option("--port", help="Host port for the LiteLLM proxy.")] = 4000,
    host: Annotated[str, typer.Option("--host", help="Host bind address for the LiteLLM proxy.")] = "127.0.0.1",
    image: Annotated[str, typer.Option("--image", help="LiteLLM Docker image.")] = "ghcr.io/berriai/litellm:main-latest",
    container_name: Annotated[Optional[str], typer.Option("--container-name", help="Docker container name.")] = None,
    no_start: Annotated[bool, typer.Option("--no-start", help="Only generate config and register proxy models.")] = False,
    replace: Annotated[bool, typer.Option("--replace", help="Replace an existing proxy container with the same name.")] = False,
    standalone: Annotated[bool, typer.Option("--standalone", help="Use the legacy standalone LiteLLM container instead of the runtime gateway.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Create a LiteLLM proxy and register its models as installed runtime models."""
    try:
        config_path = config.expanduser().resolve()
        proxy_spec = _build_proxy_spec(config_path, host=host, port=port)
        if standalone:
            resolved_container = container_name or proxy_spec["container_name"]
            generated_config = _write_litellm_proxy_config(proxy_spec["litellm_config"], resolved_container)
            if not no_start:
                _start_litellm_proxy(
                    generated_config,
                    container_name=resolved_container,
                    host=host,
                    port=port,
                    image=image,
                    env_names=proxy_spec["env_names"],
                    replace=replace,
                )
            base_url = f"http://{host}:{port}/v1"
            service_name = resolved_container
            resolved_image = image
        else:
            gateway = sync_litellm_gateway(
                external_litellm_config=proxy_spec["litellm_config"],
                external_source_path=config_path,
                restart=not no_start,
            )
            _sync_external_litellm_config_across_cluster(
                proxy_spec["litellm_config"],
                source_path=config_path,
                restart=not no_start,
                quiet=json_output,
            )
            generated_config = Path(str(gateway["config_path"]))
            base_url = litellm_gateway_internal_api_base()
            service_name = "mn-litellm-proxy"
            resolved_container = "mn-litellm-proxy"
            resolved_image = "mirror-neuron-core:latest"

        proxies = [
            upsert_model_proxy(
                model["id"],
                display_name=model.get("name"),
                source_model=model.get("source_model"),
                api_model=model["id"],
                base_url=base_url,
                config_path=config_path,
                litellm_config_path=generated_config,
                container_name=resolved_container,
                image=resolved_image,
                port=port if standalone else 4000,
                host=host if standalone else "mn-litellm-proxy",
            )
            for model in proxy_spec["models"]
        ]
        payload = {
            "status": "registered" if no_start else "running",
            "models": proxies,
            "container_name": resolved_container,
            "service": service_name,
            "base_url": base_url,
            "config": str(generated_config),
            "ledger": str(default_model_proxies_path()),
        }
        if json_output:
            console.print_json(data=payload)
            return
        print_success_confirmation(
            console,
            "Model proxy",
            status=payload["status"],
            details=[
                ("Models", ", ".join(proxy["id"] for proxy in proxies)),
                ("Backend", "proxy"),
                ("Base URL", base_url),
                ("Config", str(generated_config)),
            ],
            next_steps="mn model list --installed",
        )
    except typer.Exit:
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "model proxy")
        raise typer.Exit(1)


@model_app.command(name="doctor")
def doctor_model(
    model: Annotated[str, typer.Argument(help="Model id, alias, or Docker model reference.")] = DEFAULT_MODEL_ID,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Check Docker Model Runner, model install state, and host compatibility."""
    try:
        entry = resolve_model_entry(model)
        target = docker_model_name(entry)
        compatibility = assess_model_compatibility(entry)
        status = _docker_status()
        installed = _model_installed(target)
        endpoint_ok = _endpoint_responds()
        gateway_health = litellm_gateway_health()
        gateway_config = build_litellm_gateway_config()
        gateway_config_file = validate_litellm_gateway_config_file()
        runner_running = bool(status.get("running")) or "running" in json.dumps(status).lower()
        payload = {
            "model": _entry_payload(entry, installed=installed),
            "compatibility": compatibility.to_dict(),
            "docker_model_runner": {
                "status": status,
                "running": runner_running,
                "endpoint": DOCKER_MODEL_RUNNER_HOST_API_BASE,
                "endpoint_ok": endpoint_ok,
            },
            "litellm_gateway": {
                "service": "mn-litellm-proxy",
                "endpoint": gateway_health["url"].removesuffix("/models"),
                "endpoint_ok": bool(gateway_health.get("ok")),
                "models": gateway_health.get("models") or [],
                "config_model_count": int(gateway_config_file.get("model_count") or len(gateway_config.get("model_list") or [])),
                "config_ok": bool(gateway_config_file.get("ok")) and isinstance(gateway_config.get("model_list"), list),
                "config_path": gateway_config_file.get("path"),
                "config_error": gateway_config_file.get("error"),
            },
            "hardware": detect_host_hardware().to_dict(),
            "ok": compatibility.ok and installed and runner_running and endpoint_ok and bool(gateway_health.get("ok")),
        }
        if json_output:
            console.print_json(data=payload)
            return
        _print_doctor(payload)
    except Exception as exc:
        handle_cli_error(exc, console, "model doctor")
        raise typer.Exit(1)


@remote_app.command(name="add")
def add_remote_model(
    model: Annotated[str, typer.Argument(help="Model id, alias, or Docker/OpenAI model reference.")],
    base_url: Annotated[str, typer.Option("--base-url", help="OpenAI-compatible base URL for this model.")],
    name: Annotated[Optional[str], typer.Option("--name", help="Stable remote endpoint name.")] = None,
    api_model: Annotated[Optional[str], typer.Option("--api-model", help="Model name to pass to the remote API.")] = None,
    api_key: Annotated[str, typer.Option("--api-key", help="API key for the remote endpoint.")] = "not-needed",
    node: Annotated[Optional[str], typer.Option("--node", help="Optional node label for display/advertisement.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Declare an already-running remote model endpoint."""
    try:
        try:
            entry = resolve_model_entry(model)
            runtime_model = docker_model_name(entry)
            resolved_api_model = api_model or str(entry.get("api_model") or runtime_model)
            remote_name = name or str(entry.get("id") or runtime_model).replace("/", "-").replace(":", "-")
        except Exception:
            runtime_model = model
            resolved_api_model = api_model or model
            remote_name = name or model.replace("/", "-").replace(":", "-")
        remote = upsert_model_remote(
            remote_name,
            runtime_model,
            base_url,
            api_key=api_key,
            api_model=resolved_api_model,
            node=node,
        )
        _sync_gateway_best_effort(
            runtime_endpoints={
                remote["model"]: {
                    "provider": remote.get("provider") or "docker_model_runner",
                    "model": remote.get("api_model") or remote.get("model"),
                    "runtime_model": remote.get("model") or remote.get("api_model"),
                    "api_model": remote.get("api_model") or remote.get("model"),
                    "api_base": remote.get("base_url"),
                    "api_key": remote.get("api_key") or "not-needed",
                    "node": remote.get("node") or "",
                    "source": "manual-remote",
                }
            },
            restart=True,
            quiet=json_output,
        )
        _sync_gateway_runtime_endpoints_across_cluster(
            {
                remote["model"]: {
                    "provider": remote.get("provider") or "docker_model_runner",
                    "model": remote.get("api_model") or remote.get("model"),
                    "runtime_model": remote.get("model") or remote.get("api_model"),
                    "api_model": remote.get("api_model") or remote.get("model"),
                    "api_base": remote.get("base_url"),
                    "api_key": remote.get("api_key") or "not-needed",
                    "node": remote.get("node") or "",
                    "source": "manual-remote",
                }
            },
            restart=True,
            quiet=json_output,
        )
        payload = {"remote": remote, "path": str(default_model_remotes_path())}
        if json_output:
            console.print_json(data=payload)
            return
        print_success_confirmation(
            console,
            "Remote model",
            status="registered",
            details=[
                ("Name", remote["name"]),
                ("Model", remote["model"]),
                ("API base", remote["base_url"]),
            ],
            next_steps="mn model remote list",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "model remote add")
        raise typer.Exit(1)


@remote_app.command(name="list")
def list_remote_models(
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """List declared remote model endpoints."""
    try:
        ledger = load_model_remotes()
        remotes = list((ledger.get("remotes") or {}).values())
        payload = {"remotes": remotes, "path": str(default_model_remotes_path())}
        if json_output:
            console.print_json(data=payload)
            return
        table = Table(title="Remote model endpoints")
        table.add_column("Name")
        table.add_column("Model")
        table.add_column("API model")
        table.add_column("Base URL")
        for remote in remotes:
            table.add_row(
                str(remote.get("name") or ""),
                str(remote.get("model") or ""),
                str(remote.get("api_model") or ""),
                str(remote.get("base_url") or ""),
            )
        console.print(table)
    except Exception as exc:
        handle_cli_error(exc, console, "model remote list")
        raise typer.Exit(1)


@remote_app.command(name="remove")
def remove_remote_model(
    name: Annotated[str, typer.Argument(help="Remote endpoint name.")],
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Remove a declared remote model endpoint."""
    try:
        removed = remove_model_remote(name)
        remove_litellm_gateway_route(name)
        if removed:
            remove_litellm_gateway_route(str(removed.get("model") or ""))
        _sync_gateway_best_effort(restart=True, quiet=json_output)
        _remove_gateway_route_across_cluster(name, restart=True, quiet=json_output)
        if removed:
            _remove_gateway_route_across_cluster(str(removed.get("model") or ""), restart=True, quiet=json_output)
        payload = {"removed": removed, "path": str(default_model_remotes_path())}
        if json_output:
            console.print_json(data=payload)
            return
        if removed:
            print_success_confirmation(
                console,
                "Remote model",
                status="removed",
                details=[("Name", removed.get("name")), ("Model", removed.get("model"))],
                next_steps="mn model remote list",
            )
        else:
            console.print(f"[yellow]Remote model endpoint {name!r} was not registered.[/yellow]")
    except Exception as exc:
        handle_cli_error(exc, console, "model remote remove")
        raise typer.Exit(1)


def _install_model_on_cluster_node(
    entry: dict[str, Any],
    *,
    node: str,
    backend: str,
    context_size: Optional[int],
    force: bool,
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
    )
    payload = call_prepare_runtime_model(runtime_client, request, logger=logger)
    install = payload.get("install") if isinstance(payload.get("install"), dict) else {}
    compatibility = install.get("compatibility") if isinstance(install.get("compatibility"), dict) else {}
    if not compatibility:
        compatibility = {"backend": backend, "warnings": []}
    remote_endpoint = _cluster_gateway_endpoint(entry, node_endpoint=node_endpoint, payload=payload)
    _sync_gateway_best_effort(
        runtime_endpoints={key: remote_endpoint for key in _model_route_keys(entry)},
        restart=True,
    )
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


def _cluster_gateway_endpoint(
    entry: dict[str, Any],
    *,
    node_endpoint: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    endpoint = payload.get("endpoint") if isinstance(payload.get("endpoint"), dict) else {}
    docker_model = docker_model_name(entry)
    api_model = str(endpoint.get("api_model") or endpoint.get("model") or entry.get("api_model") or docker_model)
    result = {
        "provider": str(endpoint.get("provider") or entry.get("provider") or "docker_model_runner"),
        "model": api_model,
        "runtime_model": str(endpoint.get("runtime_model") or docker_model),
        "api_model": api_model,
        "api_base": _node_docker_model_runner_api_base(node_endpoint),
        "api_key": str(endpoint.get("api_key") or "not-needed"),
        "node": str(endpoint.get("node") or node_endpoint.get("node_name") or ""),
        "source": REMOTE_DMR_SOURCE,
    }
    aliases = entry.get("route_aliases")
    if isinstance(aliases, list) and aliases:
        result["route_aliases"] = [str(alias) for alias in aliases if str(alias or "").strip()]
    return result


def _node_docker_model_runner_api_base(node_endpoint: dict[str, Any]) -> str:
    """Return a cluster-reachable DMR URL without routing through owner LiteLLM."""

    host = str(node_endpoint.get("host") or "").strip()
    if not host:
        raise RuntimeError("cluster node does not advertise a Docker Model Runner host")

    parsed = urllib.parse.urlsplit(DOCKER_MODEL_RUNNER_HOST_API_BASE)
    hostname = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    advertised_host = f"[{hostname}]" if ":" in hostname else hostname
    port = parsed.port or 12434
    path = parsed.path or "/engines/v1"
    return urllib.parse.urlunsplit(
        (parsed.scheme or "http", f"{advertised_host}:{port}", path, "", "")
    ).rstrip("/")


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
    endpoints: list[dict[str, Any]] = []
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        node_name = str(node.get("name") or node.get("node") or "").strip()
        host = str(node.get("grpc_host") or node.get("address") or "").strip()
        if not host and "@" in node_name:
            host = node_name.split("@", 1)[1].strip()
        if not node_name or not host:
            continue
        endpoint = {
            "host": host,
            "node": node,
            "node_name": node_name,
            "self": bool(node.get("self") is True or node.get("self?") is True),
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
            "restart that worker with an updated `mn runtime start --worker-node`"
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
) -> None:
    endpoint = result.get("endpoint") if isinstance(result.get("endpoint"), dict) else None
    if endpoint is None:
        endpoint = docker_model_runner_endpoint(entry, node=node, source="local-dmr")
    _sync_gateway_best_effort(
        runtime_endpoints={key: endpoint for key in _model_route_keys(entry)},
        restart=True,
    )
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
    results: list[dict[str, Any]] = []
    for node_endpoint in _cluster_node_endpoints(quiet=True):
        node_name = str(node_endpoint.get("node_name") or "")
        try:
            runtime_client = _native_runtime_client_for_node(node_endpoint)
            response = runtime_client.sync_litellm_gateway(
                {
                    "node": node_name,
                    "external_litellm_config": litellm_config,
                    "external_source_path": str(source_path),
                    "restart": restart,
                    "source": "mn-cli-proxy-fanout",
                }
            )
            results.append({"node": node_name, "status": "ok", "response": response})
        except Exception as exc:
            results.append({"node": node_name, "status": "error", "error": str(exc)})
            if not quiet:
                print_warning(console, f"Could not sync LiteLLM proxy config on {node_name}: {exc}")
    return results


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
    Every other node receives the owner's direct, cluster-reachable DMR URL.
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
    local_entries = (
        _model_entries_for_installed_names(_installed_model_names())
        if local_endpoint is not None
        else []
    )
    local_revision = _runtime_model_inventory_revision(local_entries)
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
                local_installed_models=_installed_model_names(),
            )
            remotes = reconcile_cluster_model_remotes(
                local_routes,
                local_installed_models=_installed_model_names(),
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
        "sync_id": sync_id,
        "route_version": route_version,
    }


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
            api_model = str(entry.get("api_model") or runtime_model).strip()
            route_keys = _model_route_keys(entry)
            explicit_aliases = entry.get("route_aliases")
            endpoint = {
                "provider": str(entry.get("provider") or "docker_model_runner"),
                "model": api_model,
                "runtime_model": runtime_model,
                "api_model": api_model,
                "api_base": _node_docker_model_runner_api_base(node_endpoint),
                "api_key": "not-needed",
                "node": owner,
                "source": REMOTE_DMR_SOURCE,
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


def _route_source_for_model_payload(payload: dict[str, Any]) -> str:
    provider = str(payload.get("provider") or "").strip().lower()
    backend = str(payload.get("backend") or "").strip().lower()
    if provider == "litellm_proxy" or backend == "proxy":
        return "external-proxy"
    if payload.get("installed"):
        return "local-dmr"
    return ""


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


def _remote_model_payloads(
    *,
    existing_entries: list[dict[str, Any]],
    remote_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    existing_keys = {
        key
        for entry in existing_entries
        for key in _model_payload_match_keys(entry)
    }
    remotes = []
    seen: set[tuple[str, str]] = set()
    for remote in remote_records:
        model_id = str(remote.get("name") or remote.get("model") or "").strip()
        if not model_id or existing_keys & _remote_record_match_keys(remote):
            continue
        identity = (model_id, str(remote.get("node") or ""))
        if identity in seen:
            continue
        seen.add(identity)
        installation = _remote_installations_for_model(
            {
                "id": model_id,
                "model": remote.get("model"),
                "api_model": remote.get("api_model"),
                "aliases": remote.get("route_aliases") or [],
            },
            remote_records,
        )
        remotes.append(
            {
                "id": model_id,
                "name": model_id,
                "provider": remote.get("provider") or "docker_model_runner",
                "model": remote.get("model"),
                "api_model": remote.get("api_model") or remote.get("model"),
                "backend": "remote-dmr" if remote.get("node") else "remote",
                "installed": True,
                "status": "remote",
                "owner_count": 0,
                "route_source": REMOTE_DMR_SOURCE if remote.get("node") else "manual-remote",
                "node": remote.get("node") or "",
                "installations": installation,
            }
        )
    return sorted(remotes, key=lambda item: str(item.get("id") or ""))


def _build_proxy_spec(config_path: Path, *, host: str, port: int) -> dict[str, Any]:
    raw = _load_proxy_config(config_path)
    model_entries: list[dict[str, Any]] = []
    env_names: set[str] = set()

    if isinstance(raw.get("model_list"), list):
        litellm_config = dict(raw)
        for item in raw["model_list"]:
            if not isinstance(item, dict):
                continue
            model_name = str(item.get("model_name") or "").strip()
            params = item.get("litellm_params") if isinstance(item.get("litellm_params"), dict) else {}
            source_model = str(params.get("model") or model_name).strip()
            if not model_name:
                continue
            model_entries.append({"id": model_name, "name": str(item.get("name") or model_name), "source_model": source_model})
            for value in params.values():
                if isinstance(value, str) and value.startswith("os.environ/"):
                    env_names.add(value.split("/", 1)[1])
    else:
        litellm_models, model_entries, env_names = _provider_config_to_litellm_models(raw)
        litellm_config = {"model_list": litellm_models}

    if not model_entries:
        raise ValueError(f"{config_path} does not declare any proxy models")

    digest = hashlib.sha256(str(config_path).encode("utf-8")).hexdigest()[:10]
    return {
        "container_name": f"mn-litellm-proxy-{digest}",
        "litellm_config": litellm_config,
        "models": model_entries,
        "env_names": sorted(env_names),
        "base_url": f"http://{host}:{port}/v1",
    }


def _load_proxy_config(config_path: Path) -> dict[str, Any]:
    if not config_path.is_file():
        raise FileNotFoundError(f"proxy config file not found: {config_path}")
    raw = config_path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            import yaml  # type: ignore
        except ModuleNotFoundError as exc:
            raise ValueError(f"{config_path} is not JSON; install PyYAML or provide JSON config") from exc
        data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError(f"{config_path} must contain a JSON/YAML object")
    return data


def _provider_config_to_litellm_models(config: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    providers = config.get("provider")
    if not isinstance(providers, dict):
        raise ValueError("proxy config must contain either model_list or provider")

    litellm_models: list[dict[str, Any]] = []
    model_entries: list[dict[str, Any]] = []
    env_names: set[str] = set()
    for provider_key, provider in providers.items():
        if not isinstance(provider, dict):
            continue
        options = provider.get("options") if isinstance(provider.get("options"), dict) else {}
        api_base = str(options.get("baseURL") or options.get("base_url") or options.get("api_base") or "").strip()
        api_key_env = str(options.get("apiKeyEnv") or options.get("api_key_env") or "").strip()
        if api_key_env:
            env_names.add(api_key_env)
        models = provider.get("models") if isinstance(provider.get("models"), dict) else {}
        for model_id, model_config in models.items():
            if not isinstance(model_config, dict):
                continue
            proxy_model_id = str(model_id).strip()
            source_model = _litellm_source_model(str(provider_key), str(model_config.get("model") or proxy_model_id), api_base)
            if not proxy_model_id or not source_model:
                continue
            params: dict[str, Any] = {"model": source_model}
            if api_base:
                params["api_base"] = api_base
            if api_key_env:
                params["api_key"] = f"os.environ/{api_key_env}"
            timeout = model_config.get("timeout_seconds")
            if timeout is not None:
                params["timeout"] = timeout
            litellm_entry: dict[str, Any] = {
                "model_name": proxy_model_id,
                "litellm_params": params,
            }
            rpm = model_config.get("rate_limit_rpm") or model_config.get("rpm")
            if rpm is not None:
                litellm_entry["rpm"] = rpm
            litellm_models.append(litellm_entry)
            model_entries.append(
                {
                    "id": proxy_model_id,
                    "name": str(model_config.get("name") or proxy_model_id),
                    "source_model": source_model,
                }
            )
    return litellm_models, model_entries, env_names


def _litellm_source_model(provider_key: str, model: str, api_base: str) -> str:
    source = str(model or "").strip()
    if not source:
        return ""
    if "/" in source:
        return source
    provider = provider_key.strip().lower().replace("_", "-")
    if provider in {"openai", "openai-compatible"} and ("api.openai.com" in api_base or provider == "openai"):
        return f"openai/{source}"
    if provider and provider not in {"openai-compatible", "compatible"}:
        return f"{provider}/{source}"
    return source


def _write_litellm_proxy_config(litellm_config: dict[str, Any], container_name: str) -> Path:
    root = default_model_proxies_path().parent / "proxies" / _safe_path_name(container_name)
    root.mkdir(parents=True, exist_ok=True)
    path = root / "config.yaml"
    path.write_text(json.dumps(litellm_config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _start_litellm_proxy(
    config_path: Path,
    *,
    container_name: str,
    host: str,
    port: int,
    image: str,
    env_names: list[str],
    replace: bool,
) -> None:
    if replace:
        _docker(["rm", "-f", container_name], check=False, timeout=60)
    args = [
        "run",
        "-d",
        "--name",
        container_name,
        "-p",
        f"{host}:{port}:4000",
        "-v",
        f"{config_path}:/app/config.yaml:ro",
    ]
    for env_name in env_names:
        if env_name in os.environ:
            args.extend(["-e", env_name])
        else:
            print_warning(console, f"Environment variable {env_name} is not set for the proxy container.")
            args.extend(["-e", env_name])
    args.extend([image, "--config", "/app/config.yaml"])
    _docker(args, timeout=300)


def _safe_path_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", str(value or "").strip()).strip("-._") or "proxy"


def install_model_entry(
    entry: dict[str, Any],
    *,
    backend: str = "auto",
    context_size: Optional[int] = None,
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
    context_size: Optional[int] = None,
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


def _is_proxy_entry(entry: dict[str, Any] | None) -> bool:
    if not isinstance(entry, dict):
        return False
    return str(entry.get("provider") or "").strip().lower() == "litellm_proxy" or str(entry.get("backend") or "").strip().lower() == "proxy"


def _print_model_table(models: list[dict[str, Any]]) -> None:
    table = Table(title="Runtime models", show_header=True, header_style="bold")
    table.add_column("ID")
    table.add_column("Model")
    table.add_column("Backend")
    table.add_column("Route")
    table.add_column("Installed")
    table.add_column("Status")
    table.add_column("Owners")
    for model in models:
        table.add_row(
            str(model.get("id") or ""),
            str(model.get("model") or ""),
            str(model.get("backend") or ""),
            str(model.get("route_source") or ""),
            "yes" if model.get("installed") else "no",
            str(model.get("status") or ""),
            str(model.get("owner_count") or 0),
        )
    console.print(table)


def _print_model_detail(payload: dict[str, Any]) -> None:
    title = f"{payload.get('id')}" if payload.get('id') else "Model"
    if payload.get('name'):
        title = f"{title} - {payload.get('name')}"
    details = [
        ("Model", payload.get('model')),
        ("Backend", payload.get('backend')),
        ("Installed", "yes" if payload.get('installed') else "no"),
        ("Owners", payload.get('owner_count', 0)),
    ]
    if payload.get("default"):
        details.append(("Status", "default"))
    requirements = payload.get("requirements") or {}
    if requirements:
        details.append(("Requirements", json.dumps(requirements, sort_keys=True)))
    console.print(f"[bold]{title}[/bold]")
    print_confirmation(
        console,
        "Model detail",
        status="installed" if payload.get('installed') else "not installed",
        details=details,
    )
    if payload.get("compatibility"):
        _print_compatibility(payload["compatibility"])


def _print_compatibility(payload: dict[str, Any]) -> None:
    status = str(payload.get("status") or "unknown")
    color = "green" if payload.get("ok") else "yellow" if status == "warning" else "red"
    console.print(f"[{color}]Compatibility: {status}[/{color}] {payload.get('message')}")
    if payload.get("help"):
        console.print(str(payload["help"]))


def _print_doctor(payload: dict[str, Any]) -> None:
    model = payload["model"]
    runner = payload["docker_model_runner"]
    gateway = payload.get("litellm_gateway") or {}
    print_confirmed(
        console,
        "Model doctor",
        status="ready" if payload.get("ok") else "attention needed",
        details={
            "Model": model.get("id"),
            "Installed": "yes" if model.get('installed') else "no",
            "Runner": "running" if runner.get('running') else "not running",
            "Endpoint": "ok" if runner.get('endpoint_ok') else "not reachable",
            "LiteLLM gateway": "ok" if gateway.get("endpoint_ok") else "not reachable",
            "Gateway config": "ok" if gateway.get("config_ok") else str(gateway.get("config_error") or "invalid"),
        },
    )
    _print_compatibility(payload["compatibility"])
    if payload.get("ok") is not True:
        console.print("[yellow]Model runtime needs attention.[/yellow]")


def _resolve_or_raw_model(model: str) -> str:
    try:
        return docker_model_name(resolve_model_entry(model))
    except KeyError:
        return model


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
