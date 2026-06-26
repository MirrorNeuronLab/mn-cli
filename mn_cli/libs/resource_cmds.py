import json
from typing import Optional

import typer
from mn_sdk import ensure_combined_resource_totals

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


def native_ports_payload() -> list[dict[str, str]]:
    from mn_cli.server_cmds import native_service_ports

    return native_service_ports()
