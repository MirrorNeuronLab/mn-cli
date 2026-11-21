"""Command declarations for `mn resource`."""

from typing import Optional

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def resource_show():
    "Show CPU, GPU, memory, and disk resources reported by the core"
    return display(("resource", "show"), dispatch(("resource", "show"), locals()))


def resource_usage():
    "Show runtime resource usage derived from the core system summary."
    return display(("resource", "usage"), dispatch(("resource", "usage"), locals()))


def resource_set(
    cpu: Optional[int] = typer.Option(
        None, "--cpu", help="Maximum CPU use percentage: 25, 50, 75, or 100"
    ),
    gpu: Optional[int] = typer.Option(
        None, "--gpu", help="Maximum GPU use percentage: 25, 50, 75, or 100"
    ),
    memory: Optional[int] = typer.Option(
        None, "--memory", help="Maximum memory use percentage: 25, 50, 75, or 100"
    ),
    disk: Optional[int] = typer.Option(
        None, "--disk", help="Maximum disk use percentage: 25, 50, 75, or 100"
    ),
):
    "Set core resource use percentages"
    return display(("resource", "set"), dispatch(("resource", "set"), locals()))


def register(app: typer.Typer) -> None:
    resource_app = typer.Typer(
        help="Inspect runtime capacity, usage, and limits.", cls=RemediatingTyperGroup
    )
    resource_app.command("show", hidden=False)(resource_show)
    resource_app.command("usage", hidden=False)(resource_usage)
    resource_app.command("set", hidden=False)(resource_set)
    app.add_typer(resource_app, name="resource")
