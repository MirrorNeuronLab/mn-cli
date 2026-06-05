from __future__ import annotations

import json
import subprocess
from typing import Annotated, Any, Optional

import typer
from rich.table import Table

from mn_cli.error_handler import handle_cli_error
from mn_cli.shared import console
from mn_sdk import (
    DEFAULT_MODEL_ID,
    DOCKER_MODEL_RUNNER_HOST_API_BASE,
    assess_model_compatibility,
    detect_host_hardware,
    dmr_api_list_models,
    dmr_api_model_installed,
    dmr_api_pull_model,
    dmr_api_remove_model,
    docker_model_match_keys,
    docker_model_name,
    docker_runner_command,
    list_model_entries,
    load_model_catalog,
    load_model_ownership,
    merge_catalog_and_installed_models,
    model_ownership_metadata,
    record_manual_model_install,
    remove_model_record,
    resolve_model_entry,
)


model_app = typer.Typer(help="Manage local Docker Model Runner models")


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
        ownership = load_model_ownership()
        entries = merge_catalog_and_installed_models(
            catalog=catalog,
            installed_models=installed_models,
            ownership=ownership,
        )
        installed_model_keys = {key for model in installed_models for key in docker_model_match_keys(model)}

        def is_installed(entry: dict[str, Any]) -> bool:
            return bool(docker_model_match_keys(docker_model_name(entry)) & installed_model_keys)

        if installed:
            entries = [entry for entry in entries if is_installed(entry)]
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
        result = install_model_entry(entry, backend=backend, context_size=context_size, force=force)
        compatibility = result["compatibility"]
        target = result["docker_model"]
        record_manual_model_install(entry, backend=compatibility["backend"])
        console.print(f"[green]✓ Installed and started {entry.get('id')} ({target}).[/green]")
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
            _docker(["model", "pull", target], timeout=900)
            _docker(["model", "run", "--detach", target], timeout=300)
            console.print(f"[green]✓ Updated {entry.get('id')} ({target}).[/green]")
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
        target = _resolve_or_raw_model(model)
        remove_model_ref(target, force=force)
        remove_model_record(target)
        console.print(f"[green]✓ Removed {target}.[/green]")
    except Exception as exc:
        handle_cli_error(exc, console, "model remove")
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


def install_model_entry(
    entry: dict[str, Any],
    *,
    backend: str = "auto",
    context_size: Optional[int] = None,
    force: bool = False,
) -> dict[str, Any]:
    compatibility = assess_model_compatibility(entry, backend=backend, force=force)
    payload = compatibility.to_dict()
    if not compatibility.ok:
        _print_compatibility(payload)
        raise RuntimeError(compatibility.message)
    target = docker_model_name(entry)
    if _docker_model_cli_available():
        _ensure_runner(compatibility.backend, compatibility.accelerator)
        _docker(["model", "pull", target], timeout=900)
        run_command = ["model", "run", "--detach"]
        resolved_context = context_size or entry.get("context_size")
        if resolved_context and _docker_model_run_supports_context_size():
            run_command.extend(["--context-size", str(resolved_context)])
        run_command.append(target)
        _docker(run_command, timeout=300)
        return {"entry": entry, "docker_model": target, "compatibility": payload, "transport": "docker_cli"}

    api_result = dmr_api_pull_model(target, timeout=900)
    return {
        "entry": entry,
        "docker_model": target,
        "compatibility": payload,
        "transport": "docker_model_runner_api",
        "api": api_result,
    }


def remove_model_ref(model: str, *, force: bool = False) -> None:
    if _docker_model_cli_available():
        command = ["model", "rm"]
        if force:
            command.append("--force")
        command.append(model)
        _docker(command, timeout=120)
        return
    dmr_api_remove_model(model, timeout=120)


def installed_model_names() -> set[str]:
    return _installed_model_names()


def model_installed(model: str) -> bool:
    return _model_installed(model)


def _entry_payload(
    entry: dict[str, Any],
    *,
    installed: bool,
    ownership: dict[str, Any] | None = None,
) -> dict[str, Any]:
    docker_model = docker_model_name(entry)
    owner_payload = model_ownership_metadata(docker_model, installed=installed, ledger=ownership)
    return {
        "id": entry.get("id"),
        "name": entry.get("name"),
        "provider": entry.get("provider", "docker_model_runner"),
        "model": docker_model,
        "docker_model": docker_model,
        "backend": entry.get("backend", "llama.cpp"),
        "aliases": list(entry.get("aliases") or []),
        "context_size": entry.get("context_size"),
        "requirements": entry.get("requirements") or {},
        "installed": installed,
        **owner_payload,
    }


def _print_model_table(models: list[dict[str, Any]]) -> None:
    table = Table(title="Runtime models", show_header=True, header_style="bold")
    table.add_column("ID")
    table.add_column("Model")
    table.add_column("Backend")
    table.add_column("Installed")
    table.add_column("Owners")
    for model in models:
        table.add_row(
            str(model.get("id") or ""),
            str(model.get("model") or ""),
            str(model.get("backend") or ""),
            "yes" if model.get("installed") else "no",
            str(model.get("owner_count") or 0),
        )
    console.print(table)


def _print_model_detail(payload: dict[str, Any]) -> None:
    console.print(f"[bold]{payload.get('id')}[/bold] - {payload.get('name')}")
    console.print(f"  Model: {payload.get('model')}")
    console.print(f"  Backend: {payload.get('backend')}")
    console.print(f"  Installed: {'yes' if payload.get('installed') else 'no'}")
    console.print(f"  Owners: {payload.get('owner_count', 0)}")
    requirements = payload.get("requirements") or {}
    if requirements:
        console.print(f"  Requirements: {json.dumps(requirements, sort_keys=True)}")
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
    console.print(f"[bold]Model doctor: {model.get('id')}[/bold]")
    console.print(f"  Installed: {'yes' if model.get('installed') else 'no'}")
    console.print(f"  Runner: {'running' if runner.get('running') else 'not running'}")
    console.print(f"  Endpoint: {'ok' if runner.get('endpoint_ok') else 'not reachable'}")
    _print_compatibility(payload["compatibility"])
    if payload.get("ok"):
        console.print("[green]✓ Model runtime is ready.[/green]")
    else:
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
    if not _docker_model_cli_available():
        return dmr_api_list_models(timeout=60)
    result = _docker(["model", "list", "--format", "json"], check=False, timeout=60)
    if result.returncode != 0:
        result = _docker(["model", "list"], check=False, timeout=60)
    return _parse_model_list(result.stdout or "")


def _model_installed(model: str) -> bool:
    if _docker_model_cli_available():
        result = _docker(["model", "inspect", model], check=False, timeout=30)
        if result.returncode == 0:
            return True
    try:
        return dmr_api_model_installed(model, timeout=30)
    except Exception:
        return False


def _docker_status() -> dict[str, Any]:
    if not _docker_model_cli_available():
        return {"running": _endpoint_responds(), "backends": {}, "transport": "docker_model_runner_api"}
    result = _docker(["model", "status", "--json"], check=False, timeout=30)
    if result.returncode != 0:
        return {"running": False, "error": (result.stderr or result.stdout or "").strip()}
    try:
        value = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        return {"running": "running" in (result.stdout or "").lower(), "raw": result.stdout}
    return value if isinstance(value, dict) else {"raw": value}


def _endpoint_responds() -> bool:
    try:
        dmr_api_list_models(timeout=3)
        return True
    except Exception:
        return False


def _parse_model_list(output: str) -> set[str]:
    names: set[str] = set()
    stripped = output.strip()
    if not stripped:
        return names
    try:
        decoded = json.loads(stripped)
    except json.JSONDecodeError:
        decoded = None
    if isinstance(decoded, list):
        for item in decoded:
            if isinstance(item, dict):
                names.update(_model_name_candidates(item))
            elif isinstance(item, str):
                names.add(item)
        return names
    if isinstance(decoded, dict):
        items = decoded.get("models") if isinstance(decoded.get("models"), list) else [decoded]
        for item in items:
            if isinstance(item, dict):
                names.update(_model_name_candidates(item))
        return names

    for line in stripped.splitlines():
        line = line.strip()
        if not line or line.lower().startswith(("name", "model")):
            continue
        if line.startswith("{"):
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                item = {}
            if isinstance(item, dict):
                names.update(_model_name_candidates(item))
                continue
        names.add(line.split()[0])
    return names


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
) -> subprocess.CompletedProcess[str]:
    command = ["docker", *args]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    except FileNotFoundError as exc:
        result = subprocess.CompletedProcess(command, 127, "", str(exc))
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"{' '.join(command)} failed{': ' + detail if detail else ''}")
    return result
