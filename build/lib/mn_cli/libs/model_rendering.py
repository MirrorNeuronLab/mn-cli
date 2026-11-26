from __future__ import annotations

import json
from typing import Any

from rich.table import Table

from mn_cli.libs.ui import print_confirmation, print_doctor_checks
from mn_cli.shared import console


def print_model_table(models: list[dict[str, Any]]) -> None:
    table = Table(title="Runtime models", show_header=True, header_style="bold")
    table.add_column("ID")
    table.add_column("Kind")
    table.add_column("Source")
    table.add_column("State")
    table.add_column("Node")
    for model in models:
        table.add_row(
            str(model.get("id") or ""),
            str(model.get("kind") or ""),
            str(model.get("source") or ""),
            str(model.get("state") or ""),
            str(model.get("node") or ""),
        )
    console.print(table)


def print_model_detail(payload: dict[str, Any]) -> None:
    title = f"{payload.get('id')}" if payload.get("id") else "Model"
    if payload.get("name"):
        title = f"{title} - {payload.get('name')}"
    details = [
        ("Kind", payload.get("kind")),
        ("Model", payload.get("model")),
        ("Backend", payload.get("backend")),
        ("State", payload.get("state")),
        ("Registered", "yes" if payload.get("registered") else "no"),
        ("Installed", _observation(payload.get("installed"))),
        ("Routed", _observation(payload.get("routed"))),
        ("Node", payload.get("node")),
        ("Verification", payload.get("verification")),
    ]
    requirements = payload.get("requirements") or {}
    if requirements:
        details.append(("Requirements", json.dumps(requirements, sort_keys=True)))
    console.print(f"[bold]{title}[/bold]")
    print_confirmation(
        console,
        "Model detail",
        status=str(payload.get("state") or "unknown"),
        details=details,
    )
    if payload.get("compatibility"):
        print_compatibility(payload["compatibility"])


def _observation(value: Any) -> str:
    if value is None:
        return "not checked"
    return "yes" if value else "no"


def print_compatibility(payload: dict[str, Any]) -> None:
    status = str(payload.get("status") or "unknown")
    color = "green" if payload.get("ok") else "yellow" if status == "warning" else "red"
    console.print(f"[{color}]Compatibility: {status}[/{color}] {payload.get('message')}")
    if payload.get("help"):
        console.print(str(payload["help"]))


def print_dmr_doctor(payload: dict[str, Any]) -> None:
    model = payload["model"]
    runner = payload["docker_model_runner"]
    gateway = payload.get("litellm_gateway") or {}
    compatibility = payload.get("compatibility") or {}
    print_doctor_checks(
        console,
        f"Model doctor: {model.get('id')}",
        [
            {"check": "Compatibility", "state": compatibility.get("status"), "detail": compatibility.get("message"), "fix": compatibility.get("help")},
            {"check": "DMR artifact", "state": "ready" if model.get("installed") else "missing", "detail": model.get("docker_model") or model.get("model"), "fix": f"mn model update {model.get('id')}" if not model.get("installed") else ""},
            {"check": "Docker Model Runner", "state": "ready" if runner.get("running") and runner.get("endpoint_ok") else "critical", "detail": runner.get("inventory_error") or "", "fix": "Start Docker Model Runner and retry." if not runner.get("endpoint_ok") else ""},
            {"check": "Gateway config", "state": "ready" if gateway.get("config_ok") else "critical", "detail": gateway.get("config_error") or "", "fix": "mn runtime doctor --repair" if not gateway.get("config_ok") else ""},
            {"check": "Model route", "state": "ready" if gateway.get("routed") else "critical", "detail": gateway.get("endpoint") or "", "fix": f"mn model update {model.get('id')}" if not gateway.get("routed") else ""},
        ],
    )


def print_provider_doctor(payload: dict[str, Any]) -> None:
    model = payload["model"]
    print_doctor_checks(
        console,
        f"Model doctor: {model.get('id')}",
        [
            {"check": "Definition", "state": "ready" if payload["definition"].get("ok") else "critical", "detail": payload["definition"].get("path") or payload["definition"].get("error"), "fix": "Restore or correct the stored definition JSON." if not payload["definition"].get("ok") else ""},
            {"check": "Environment", "state": "ready" if payload["environment"].get("ok") else "critical", "detail": payload["environment"].get("name") or "No secret required", "fix": f"Set {payload['environment'].get('name')} and retry." if not payload["environment"].get("ok") else ""},
            {"check": "Gateway", "state": "ready" if payload["litellm_gateway"].get("ok") else "critical", "detail": payload["litellm_gateway"].get("url") or payload["litellm_gateway"].get("error"), "fix": "mn runtime doctor --repair" if not payload["litellm_gateway"].get("ok") else ""},
            {"check": "Route", "state": "ready" if payload["litellm_gateway"].get("routed") else "critical", "detail": model.get("id"), "fix": f"mn model update {model.get('id')}" if not payload["litellm_gateway"].get("routed") else ""},
        ],
    )
