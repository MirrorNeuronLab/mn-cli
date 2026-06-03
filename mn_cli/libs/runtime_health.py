from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

import typer
from rich.table import Table

from mn_cli.shared import client, console
from mn_cli.server_cmds import (
    RUNTIME_ENDPOINTS_FILE,
    _ensure_compose_native_port_settings,
    _runtime_base_env,
    _runtime_endpoint_snapshot,
    find_web_ui_dir,
    runtime_compose_available,
)


def health(
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
    timeout: float = typer.Option(3.0, "--timeout", min=0.1, help="Per-component timeout in seconds."),
) -> None:
    """Report Core gRPC, REST API, and Web UI health."""
    report = collect_runtime_health(timeout)
    if json_output:
        console.print_json(data=report)
    else:
        print_health_report(report)
    if report["overall"] == "critical":
        raise typer.Exit(1)


def collect_runtime_health(timeout: float = 3.0) -> dict[str, Any]:
    env = _runtime_base_env(runtime_compose_available())
    if runtime_compose_available():
        env = _ensure_compose_native_port_settings(env)
    installed_web_ui = find_web_ui_dir() is not None
    snapshot = _runtime_endpoint_snapshot(env, web_ui_available=installed_web_ui)
    persisted = _read_runtime_endpoints()
    targets = _targets(snapshot, persisted)

    components = [
        check_core_grpc(targets["core_grpc"], timeout),
        check_http_component("api", _append_path(targets["api"], "/health"), timeout, expected_component=None),
        check_web_ui(targets.get("web_ui"), timeout, installed_web_ui, "web_ui" in persisted),
    ]
    overall = overall_status(components)
    return {
        "overall": overall,
        "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "components": components,
    }


def check_core_grpc(target: str, timeout: float) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        client.get_system_summary()
        return _component("core_grpc", "passing", target, started, detail="system summary ok")
    except Exception as exc:
        return _component("core_grpc", "critical", target, started, error=str(exc))


def check_web_ui(target: str | None, timeout: float, installed: bool, advertised: bool) -> dict[str, Any]:
    if not target:
        status = "critical" if installed or advertised else "warning"
        detail = "endpoint missing" if installed or advertised else "web ui is not installed"
        return _component("web_ui", status, None, time.perf_counter(), detail=detail)
    if not installed and not advertised:
        return _component("web_ui", "warning", target, time.perf_counter(), detail="web ui is not installed")
    return check_http_component("web_ui", _append_path(target, "/health"), timeout, expected_component="web-ui")


def check_http_component(name: str, target: str, timeout: float, expected_component: str | None) -> dict[str, Any]:
    started = time.perf_counter()
    request = urllib.request.Request(target, headers={"User-Agent": "mn-runtime-health/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(4096)
            status_code = int(getattr(response, "status", response.getcode()))
    except urllib.error.HTTPError as exc:
        return _component(name, "critical", target, started, error=f"HTTP {exc.code}")
    except Exception as exc:
        return _component(name, "critical", target, started, error=str(exc))

    payload = _json_body(body)
    service_status = str(payload.get("status") or "").lower() if isinstance(payload, dict) else ""
    component = str(payload.get("component") or "") if isinstance(payload, dict) else ""
    if status_code >= 500:
        return _component(name, "critical", target, started, error=f"HTTP {status_code}")
    if service_status and service_status != "ok":
        return _component(name, "critical", target, started, detail=payload)
    if expected_component and component != expected_component:
        return _component(name, "critical", target, started, error=f"unexpected component {component!r}")
    return _component(name, "passing", target, started, detail=payload or f"HTTP {status_code}")


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


def overall_status(components: list[dict[str, Any]]) -> str:
    statuses = {component["status"] for component in components}
    if "critical" in statuses:
        return "critical"
    if "warning" in statuses:
        return "warning"
    return "passing"


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
    try:
        data = json.loads(RUNTIME_ENDPOINTS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _append_path(base: str | None, path: str) -> str:
    if not base:
        return path
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def _component(
    name: str,
    status: str,
    target: str | None,
    started: float,
    *,
    detail: Any = None,
    error: str | None = None,
) -> dict[str, Any]:
    component: dict[str, Any] = {
        "name": name,
        "status": status,
        "target": target,
        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
    }
    if detail is not None:
        component["detail"] = detail
    if error:
        component["error"] = error
    return component


def _json_body(body: bytes) -> Any:
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
