import json
from numbers import Number
from typing import Any, Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.shared import client, console

resource_app = typer.Typer(help="Inspect and update core resource limits")


@resource_app.command(name="list")
def list_resources():
    """Show CPU, GPU, memory, and disk resources reported by the core"""
    try:
        resource = json.loads(client.get_resource())
        console.print_json(data=ensure_combined_resource_totals(resource))
    except Exception as e:
        handle_cli_error(e, console, "resource list")


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
        resource = json.loads(client.set_resource(payload))
        console.print_json(data=ensure_combined_resource_totals(resource))
    except Exception as e:
        handle_cli_error(e, console, "resource set")


RESOURCE_TOTAL_KEYS = (
    "cpu_cores",
    "gpu_count",
    "gpu_memory_total_mb",
    "gpu_memory_free_mb",
    "memory_gb",
    "disk_gb",
    "disk_available_gb",
)
INTEGER_RESOURCE_KEYS = {"cpu_cores", "gpu_count"}


def ensure_combined_resource_totals(payload: Any) -> Any:
    if not isinstance(payload, dict) or isinstance(payload.get("combined"), dict):
        return payload

    if isinstance(payload.get("totals"), dict):
        combined = payload["totals"]
    elif isinstance(payload.get("nodes"), list):
        combined = combine_node_resources(payload["nodes"])
    else:
        return payload

    enriched = dict(payload)
    enriched["combined"] = normalize_resource_totals(combined)
    return enriched


def combine_node_resources(nodes: Any) -> dict[str, Any]:
    combined: dict[str, float] = {key: 0.0 for key in RESOURCE_TOTAL_KEYS}

    if not isinstance(nodes, list):
        return combined

    for node in nodes:
        if not isinstance(node, dict):
            continue
        for key in RESOURCE_TOTAL_KEYS:
            combined[key] += resource_number(node.get(key))

    return normalize_resource_totals(combined)


def normalize_resource_totals(totals: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(totals)
    for key in RESOURCE_TOTAL_KEYS:
        if key not in totals:
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
