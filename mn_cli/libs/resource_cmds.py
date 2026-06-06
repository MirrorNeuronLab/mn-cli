import json
from numbers import Number
from typing import Any, Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import print_success_confirmation
from mn_cli.shared import client, console

resource_app = typer.Typer(help="Inspect and update core resource limits")


@resource_app.command(name="list")
def list_resources():
    """Show CPU, GPU, memory, and disk resources reported by the core"""
    try:
        resource = json.loads(client.get_resource())
        enriched = ensure_combined_resource_totals(resource)
        if isinstance(enriched, dict):
            enriched = dict(enriched)
            enriched["native_ports"] = native_ports_payload()
            enriched["runtime_health_command"] = "mn runtime health"
        console.print_json(data=enriched)
    except Exception as e:
        handle_cli_error(e, console, "resource list")


@resource_app.command(name="ports")
def list_native_ports():
    """Show native OS ports used by the local runtime"""
    from mn_cli.server_cmds import _print_service_endpoints

    _print_service_endpoints(ip=None, web_ui_available=True)


@resource_app.command(name="set")
def set_resources(
    cpu: Optional[int] = typer.Option(
        None,
        "--cpu",
        help="Maximum CPU use percentage: 25, 50, 75, or 100",
    ),
    gpu: Optional[int] = typer.Option(
        None,
        "--gpu",
        help="Maximum GPU use percentage: 25, 50, 75, or 100",
    ),
    memory: Optional[int] = typer.Option(
        None,
        "--memory",
        help="Maximum memory use percentage: 25, 50, 75, or 100",
    ),
    disk: Optional[int] = typer.Option(
        None,
        "--disk",
        help="Maximum disk use percentage: 25, 50, 75, or 100",
    ),
):
    """Set core resource use percentages"""
    try:
        payload = {
            key: value
            for key, value in {"cpu": cpu, "gpu": gpu, "memory": memory, "disk": disk}.items()
            if value is not None
        }
        resource = ensure_combined_resource_totals(json.loads(client.set_resource(payload)))
        print_success_confirmation(
            console,
            "Resource set",
            status=resource.get("status") if isinstance(resource, dict) else None,
            details=[
                ("CPU", payload.get("cpu")),
                ("GPU", payload.get("gpu")),
                ("Memory", payload.get("memory")),
                ("Disk", payload.get("disk")),
            ],
            next_steps="mn resource list",
        )
        if isinstance(resource, dict):
            console.print_json(data=resource)
    except Exception as e:
        handle_cli_error(e, console, "resource set")


RESOURCE_TOTAL_KEYS = (
    "cpu_cores",
    "gpu_count",
    "gpu_memory_total_mb",
    "gpu_memory_free_mb",
    "gpu_memory_total_gb",
    "gpu_memory_free_gb",
    "memory_gb",
    "memory_total_gb",
    "memory_available_gb",
    "disk_gb",
    "disk_available_gb",
)
INTEGER_RESOURCE_KEYS = {"cpu_cores", "gpu_count"}


def ensure_combined_resource_totals(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    enriched = dict(payload)

    if isinstance(enriched.get("nodes"), list):
        enriched["nodes"] = [
            normalize_resource_totals(node) if isinstance(node, dict) else node
            for node in enriched["nodes"]
        ]

    if isinstance(enriched.get("totals"), dict):
        enriched["totals"] = normalize_resource_totals(enriched["totals"])

    if isinstance(enriched.get("usable"), dict):
        enriched["usable"] = normalize_resource_totals(enriched["usable"])

    if isinstance(enriched.get("combined"), dict):
        combined = enriched["combined"]
    elif isinstance(enriched.get("totals"), dict):
        combined = enriched["totals"]
    elif isinstance(enriched.get("nodes"), list):
        combined = combine_node_resources(enriched["nodes"])
    else:
        return enriched

    enriched["combined"] = normalize_resource_totals(combined)
    return enriched


def combine_node_resources(nodes: Any) -> dict[str, Any]:
    combined: dict[str, float] = {key: 0.0 for key in RESOURCE_TOTAL_KEYS}

    if not isinstance(nodes, list):
        return combined

    for node in nodes:
        if not isinstance(node, dict):
            continue
        node = normalize_resource_totals(node)
        for key in RESOURCE_TOTAL_KEYS:
            combined[key] += resource_number(node.get(key))

    return normalize_resource_totals(combined)


def normalize_resource_totals(totals: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(totals)

    if "memory_total_gb" not in normalized and "memory_gb" in normalized:
        normalized["memory_total_gb"] = normalized["memory_gb"]
    if "memory_gb" not in normalized and "memory_total_gb" in normalized:
        normalized["memory_gb"] = normalized["memory_total_gb"]
    if "memory_available_gb" not in normalized:
        normalized["memory_available_gb"] = 0.0

    if "gpu_memory_total_gb" not in normalized and "gpu_memory_total_mb" in normalized:
        normalized["gpu_memory_total_gb"] = resource_number(normalized["gpu_memory_total_mb"]) / 1024
    if "gpu_memory_free_gb" not in normalized and "gpu_memory_free_mb" in normalized:
        normalized["gpu_memory_free_gb"] = resource_number(normalized["gpu_memory_free_mb"]) / 1024
    if "gpu_memory_total_mb" not in normalized and "gpu_memory_total_gb" in normalized:
        normalized["gpu_memory_total_mb"] = resource_number(normalized["gpu_memory_total_gb"]) * 1024
    if "gpu_memory_free_mb" not in normalized and "gpu_memory_free_gb" in normalized:
        normalized["gpu_memory_free_mb"] = resource_number(normalized["gpu_memory_free_gb"]) * 1024

    for key in RESOURCE_TOTAL_KEYS:
        if key not in totals:
            if key not in normalized:
                normalized[key] = 0 if key in INTEGER_RESOURCE_KEYS else 0.0
            value = resource_number(normalized.get(key))
            normalized[key] = int(value) if key in INTEGER_RESOURCE_KEYS else round(value, 2)
            continue
        value = resource_number(totals.get(key))
        normalized[key] = int(value) if key in INTEGER_RESOURCE_KEYS else round(value, 2)
    return normalized


def resource_number(value: Any) -> float:
    if isinstance(value, Number):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return 0.0
    return 0.0


def native_ports_payload() -> list[dict[str, str]]:
    from mn_cli.server_cmds import native_service_ports

    return native_service_ports()
