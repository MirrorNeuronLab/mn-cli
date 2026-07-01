from __future__ import annotations

import json
import os
import hashlib
import re
import subprocess
from pathlib import Path
from typing import Annotated, Any, Optional

import typer
from rich.table import Table

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import print_confirmation, print_confirmed, print_success_confirmation
from mn_cli.shared import console
from mn_sdk import (
    DEFAULT_MODEL_ID,
    DOCKER_MODEL_RUNNER_HOST_API_BASE,
    assess_model_compatibility,
    default_model_proxies_path,
    detect_host_hardware,
    dmr_api_list_models,
    dmr_api_model_installed,
    dmr_api_pull_model,
    dmr_api_remove_model,
    docker_model_match_keys,
    docker_model_name,
    docker_runner_command,
    default_model_remotes_path,
    list_model_entries,
    load_model_catalog,
    load_model_ownership,
    load_model_remotes,
    merge_catalog_and_installed_models,
    model_ownership_metadata,
    proxy_model_ids,
    record_manual_model_install,
    remove_model_proxy,
    remove_model_remote,
    remove_model_record,
    resolve_model_entry,
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
):
    """Pull and start a Docker Model Runner model."""
    try:
        catalog = load_model_catalog()
        entry = resolve_model_entry(model, catalog=catalog)
        result = install_model_entry_with_progress(entry, backend=backend, context_size=context_size, force=force)
        compatibility = result["compatibility"]
        target = result["docker_model"]
        record_manual_model_install(entry, backend=compatibility["backend"])
        _record_runtime_model_install(entry)
        print_success_confirmation(
            console,
            "Model install",
            status="running",
            details=[("Model", entry.get("id")), ("Docker model", target), ("Backend", compatibility.get("backend"))],
            next_steps=f"mn model doctor {entry.get('id')}",
        )
        if compatibility.get("warnings"):
            for warning in compatibility["warnings"]:
                console.print(f"[yellow]Warning: {warning}[/yellow]")
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
):
    """Remove a Docker Model Runner model."""
    try:
        try:
            entry = resolve_model_entry(model)
        except KeyError:
            entry = None
        if _is_proxy_entry(entry):
            removed = remove_model_proxy(str(entry.get("id") or model))
            print_success_confirmation(
                console,
                "Model proxy",
                status="removed",
                details={"Model": str(entry.get("id") or model), "Backend": "proxy"},
                next_steps="mn model list --installed",
            )
            return
        target = docker_model_name(entry) if entry else _resolve_or_raw_model(model)
        remove_model_ref(target, force=force)
        remove_model_record(target)
        print_success_confirmation(
            console,
            "Model remove",
            status="removed",
            details={"Model": target},
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
    json_output: Annotated[bool, typer.Option("--json", help="Print machine-readable JSON.")] = False,
):
    """Create a LiteLLM proxy and register its models as installed runtime models."""
    try:
        config_path = config.expanduser().resolve()
        proxy_spec = _build_proxy_spec(config_path, host=host, port=port)
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
                image=image,
                port=port,
                host=host,
            )
            for model in proxy_spec["models"]
        ]
        payload = {
            "status": "registered" if no_start else "running",
            "models": proxies,
            "container_name": resolved_container,
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
            "hardware": detect_host_hardware().to_dict(),
            "ok": compatibility.ok and installed and runner_running and endpoint_ok,
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
            console.print(f"[yellow]Warning: environment variable {env_name} is not set for the proxy container.[/yellow]")
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
        console.print(f"[yellow]Warning: could not update runtime model Compose wiring: {exc}[/yellow]")


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
    table.add_column("Installed")
    table.add_column("Status")
    table.add_column("Owners")
    for model in models:
        table.add_row(
            str(model.get("id") or ""),
            str(model.get("model") or ""),
            str(model.get("backend") or ""),
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
    print_confirmed(
        console,
        "Model doctor",
        status="ready" if payload.get("ok") else "attention needed",
        details={
            "Model": model.get("id"),
            "Installed": "yes" if model.get('installed') else "no",
            "Runner": "running" if runner.get('running') else "not running",
            "Endpoint": "ok" if runner.get('endpoint_ok') else "not reachable",
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
