from __future__ import annotations

import json
import urllib.request
from typing import Any

import typer
from rich.console import Group
from rich.padding import Padding
from rich.table import Table
from rich.text import Text
from mn_sdk import RuntimeConfig, collect_runtime_status as sdk_collect_runtime_status, health_report_from_status

from mn_cli.runtime_state import read_json_file
from mn_cli.shared import client, console
from mn_cli.server_cmds import (
    RUNTIME_ENDPOINTS_FILE,
    DEFAULT_API_PORT,
    DEFAULT_GRPC_PORT,
    DEFAULT_WEB_UI_PORT,
    LEGACY_API_PORT,
    LEGACY_GRPC_PORT,
    LEGACY_WEB_UI_PORT,
    _start_api_if_installed,
    _start_web_ui_if_installed,
    _runtime_base_env,
    _runtime_endpoint_snapshot,
    _valid_port_text,
    _write_runtime_endpoints_file,
    find_web_ui_dir,
    runtime_compose_available,
)


def health(
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
    timeout: float = typer.Option(3.0, "--timeout", min=0.1, help="Per-component timeout in seconds."),
    repair: bool = typer.Option(False, "--repair", help="Restart unhealthy API/Web UI sidecars when possible."),
) -> None:
    """Report Core gRPC, REST API, and Web UI health."""
    report = collect_runtime_health(timeout)
    if repair and _repair_runtime_sidecars(report):
        report = collect_runtime_health(timeout)
    if json_output:
        console.print_json(data=report)
    else:
        print_health_report(report)
    if report["overall"] == "critical":
        raise typer.Exit(1)


def status(
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
    timeout: float = typer.Option(3.0, "--timeout", min=0.1, help="Per-component timeout in seconds."),
) -> None:
    """Report runtime endpoints, health, nodes, jobs, and shared storage."""
    report = collect_runtime_status(timeout)
    if json_output:
        console.print_json(data=report)
    else:
        print_status_report(report)
    if report["overall"] == "critical":
        raise typer.Exit(1)


def collect_runtime_health(timeout: float = 3.0, *, core_client: Any | None = None) -> dict[str, Any]:
    return health_report_from_status(
        collect_runtime_status(timeout, core_client=core_client)
    )


def collect_runtime_status(timeout: float = 3.0, *, core_client: Any | None = None) -> dict[str, Any]:
    installed_web_ui = find_web_ui_dir() is not None
    config = _runtime_config(web_ui_installed=installed_web_ui)
    return sdk_collect_runtime_status(
        config=config,
        client=core_client if core_client is not None else client,
        timeout=timeout,
        http_opener=urllib.request.urlopen,
        web_ui_installed=installed_web_ui,
    )


def _runtime_config(*, web_ui_installed: bool) -> RuntimeConfig:
    env = _runtime_base_env(runtime_compose_available())
    if runtime_compose_available():
        env = _compose_native_port_env(env)
    snapshot = _runtime_endpoint_snapshot(env, web_ui_available=web_ui_installed)
    persisted = _read_runtime_endpoints()
    endpoints = dict(snapshot)
    for key in ("api", "grpc", "web_ui"):
        if isinstance(persisted.get(key), dict):
            endpoints[key] = persisted[key]
    return RuntimeConfig.from_env(runtime_env=env, runtime_endpoints=endpoints)


def print_health_report(report: dict[str, Any]) -> None:
    table = Table(title=f"Runtime health: {report['overall']}", show_header=True, header_style="bold")
    table.add_column("Component")
    table.add_column("Status")
    table.add_column("Target")
    table.add_column("Detail")
    for component in report["components"]:
        detail = component.get("error") or component.get("detail") or ""
        if isinstance(detail, dict):
            detail = json.dumps(detail, sort_keys=True)
        table.add_row(
            component["name"],
            component["status"],
            str(component.get("target") or ""),
            str(detail),
        )
    console.print(table)


def print_status_report(report: dict[str, Any]) -> None:
    components = {
        str(component.get("name")): component
        for component in report.get("components", [])
        if isinstance(component, dict)
    }
    nodes = report.get("nodes") if isinstance(report.get("nodes"), dict) else {}
    jobs = report.get("jobs") if isinstance(report.get("jobs"), dict) else {}
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    endpoints = report.get("endpoints") if isinstance(report.get("endpoints"), dict) else {}
    storage = report.get("shared_storage") if isinstance(report.get("shared_storage"), dict) else {}

    overall = str(report.get("overall") or "unknown")
    sections = [
        Text.assemble("Runtime status: ", (overall, _status_style(overall)), overflow="fold"),
        _status_section(
            "Runtime",
            overall,
            [
                ("mode", runtime.get("mode") or "local"),
                ("mn_home", runtime.get("mn_home")),
            ],
        ),
    ]
    for name, label, endpoint_key in (
        ("core_grpc", "Core gRPC", "core_grpc"),
        ("api", "REST API", "api"),
        ("web_ui", "Web UI", "web_ui"),
    ):
        component = components.get(name, {})
        sections.append(
            _status_section(
                label,
                str(component.get("status") or "unknown"),
                [
                    ("endpoint", endpoints.get(endpoint_key) or component.get("target")),
                    ("detail", component.get("error") or component.get("detail")),
                ],
            )
        )
    sections.extend(
        [
            _status_section(
                "Nodes",
                _availability_status(nodes),
                [
                    ("total", _format_count(nodes.get("total"))),
                    ("by_status", _format_counts(nodes.get("by_status"))),
                ],
            ),
            _status_section(
                "Active jobs",
                _availability_status(jobs),
                [
                    ("total", _format_count(jobs.get("active"))),
                    ("by_status", _format_counts(jobs.get("active_by_status"))),
                ],
            ),
            _status_section(
                "Shared storage",
                "configured" if storage.get("configured") else "default",
                [
                    ("host_root", storage.get("host_root")),
                    ("runtime_root", storage.get("runtime_root")),
                ],
            ),
        ]
    )
    console.print(Group(*sections))


def overall_status(components: list[dict[str, Any]]) -> str:
    statuses = {component["status"] for component in components}
    if "critical" in statuses:
        return "critical"
    if "warning" in statuses:
        return "warning"
    return "passing"


def _repair_runtime_sidecars(report: dict[str, Any]) -> bool:
    components = {
        str(component.get("name")): component
        for component in report.get("components", [])
        if isinstance(component, dict)
    }
    needs_api = components.get("api", {}).get("status") == "critical"
    needs_web_ui = components.get("web_ui", {}).get("status") == "critical"
    if not needs_api and not needs_web_ui:
        return False

    env = _runtime_base_env(runtime_compose_available())
    if runtime_compose_available():
        env = _compose_native_port_env(env)
    env.setdefault("MN_API_HOST", "localhost")
    env.setdefault("MN_API_PORT", "54001")
    env.setdefault("MN_WEB_UI_HOST", "localhost")
    env.setdefault("MN_WEB_UI_PORT", "55173")

    changed = False
    if needs_api:
        console.print("[yellow]=> Repair: restarting REST API sidecar...[/yellow]")
        changed = _start_api_if_installed(env) or changed
    if needs_web_ui:
        console.print("[yellow]=> Repair: restarting Web UI sidecar...[/yellow]")
        changed = _start_web_ui_if_installed(env) or changed
    if changed:
        _write_runtime_endpoints_file(env, web_ui_available=find_web_ui_dir() is not None)
    return changed


def _compose_native_port_env(env: dict[str, str]) -> dict[str, str]:
    adjusted = dict(env)
    adjusted["MN_GRPC_PORT"] = _port_value(adjusted, "MN_GRPC_PORT", DEFAULT_GRPC_PORT, LEGACY_GRPC_PORT)
    adjusted["MN_API_PORT"] = _port_value(adjusted, "MN_API_PORT", DEFAULT_API_PORT, LEGACY_API_PORT)
    adjusted["MN_WEB_UI_PORT"] = _port_value(adjusted, "MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT, LEGACY_WEB_UI_PORT)
    return adjusted


def _port_value(env: dict[str, str], key: str, default: str, legacy_default: str) -> str:
    value = str(env.get(key) or "").strip()
    if not value or value == legacy_default:
        value = default
    return _valid_port_text(value, default)


def _targets(snapshot: dict[str, Any], persisted: dict[str, Any]) -> dict[str, str]:
    merged = dict(snapshot)
    for key in ("api", "grpc", "web_ui"):
        if isinstance(persisted.get(key), dict):
            merged[key] = persisted[key]
    api = merged.get("api") if isinstance(merged.get("api"), dict) else {}
    grpc = merged.get("grpc") if isinstance(merged.get("grpc"), dict) else {}
    web_ui = merged.get("web_ui") if isinstance(merged.get("web_ui"), dict) else {}
    return {
        "api": str(api.get("base_url") or f"http://{api.get('host', 'localhost')}:{api.get('port', '54001')}/api/v1").rstrip("/"),
        "core_grpc": str(grpc.get("target") or f"{grpc.get('host', 'localhost')}:{grpc.get('port', '55051')}"),
        "web_ui": str(web_ui.get("url") or "").rstrip("/"),
    }


def _read_runtime_endpoints() -> dict[str, Any]:
    data = read_json_file(RUNTIME_ENDPOINTS_FILE)
    return data if isinstance(data, dict) else {}


def _append_path(base: str | None, path: str) -> str:
    if not base:
        return path
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def _format_count(value: Any) -> str:
    return "unknown" if value is None else str(value)


def _format_counts(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return ""
    return ", ".join(f"{key}: {count}" for key, count in sorted(value.items()))


def _availability_status(value: dict[str, Any]) -> str:
    if value.get("available") is False:
        return "unavailable"
    if value.get("available") is True:
        return "available"
    return "unknown"


def _status_section(label: str, status: str, items: list[tuple[str, Any]]) -> Group:
    section: list[Any] = [
        Text.assemble((label, "bold"), "  ", (status, _status_style(status)), overflow="fold")
    ]
    for item_label, value in items:
        value_text = _format_status_value(value)
        if not value_text:
            continue
        if _needs_status_value_block(value_text):
            section.append(Text(f"  {item_label}:", style="dim"))
            section.append(Padding(Text(value_text, overflow="fold"), (0, 0, 0, 4)))
        else:
            section.append(Text.assemble(("  " + item_label + ": ", "dim"), value_text, overflow="fold"))
    return Group(*section)


def _format_status_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        value = json.dumps(value, sort_keys=True)
    return str(value).strip().replace("\n", "\n  ")


def _needs_status_value_block(value: str) -> bool:
    return "\n" in value or len(value) > 56


def _status_style(status: Any) -> str:
    normalized = str(status or "").lower()
    if normalized in {"passing", "healthy", "available", "configured"}:
        return "green"
    if normalized in {"warning", "default", "unknown"}:
        return "yellow"
    if normalized in {"critical", "unavailable"}:
        return "red"
    return ""
