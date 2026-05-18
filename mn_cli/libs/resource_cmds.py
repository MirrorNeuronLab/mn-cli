import json
from typing import Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.shared import client, console

resource_app = typer.Typer(help="Inspect and update core resource limits")


@resource_app.command(name="list")
def list_resources():
    """Show CPU, GPU, and memory resources reported by the core"""
    try:
        console.print_json(data=json.loads(client.get_resource()))
    except Exception as e:
        handle_cli_error(e, console, "resource list")


@resource_app.command(name="set")
def set_resources(
    cpu: Optional[int] = typer.Option(None, "--cpu", help="Maximum CPU use percentage: 25, 50, 75, or 100"),
    gpu: Optional[int] = typer.Option(None, "--gpu", help="Maximum GPU use percentage: 25, 50, 75, or 100"),
    memory: Optional[int] = typer.Option(
        None,
        "--memory",
        help="Maximum memory use percentage: 25, 50, 75, or 100",
    ),
):
    """Set core resource use percentages"""
    try:
        payload = {
            key: value
            for key, value in {"cpu": cpu, "gpu": gpu, "memory": memory}.items()
            if value is not None
        }
        console.print_json(data=json.loads(client.set_resource(payload)))
    except Exception as e:
        handle_cli_error(e, console, "resource set")
