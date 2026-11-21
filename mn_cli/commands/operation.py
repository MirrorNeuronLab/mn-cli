"""Command declarations for `mn operation`."""

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def operation_show(
    operation_id: str = typer.Argument(..., help="Durable operation ID."),
):
    "Show a durable group operation and its item states."
    return display(("operation", "show"), dispatch(("operation", "show"), locals()))


def operation_watch(
    operation_id: str = typer.Argument(..., help="Durable operation ID."),
):
    "Reattach to a durable group operation's progress stream."
    return display(("operation", "watch"), dispatch(("operation", "watch"), locals()))


def register(app: typer.Typer) -> None:
    operation_app = typer.Typer(
        help="Inspect or reattach to durable group operations.",
        cls=RemediatingTyperGroup,
    )
    operation_app.command("show", hidden=False)(operation_show)
    operation_app.command("watch", hidden=False)(operation_watch)
    app.add_typer(operation_app, name="operation")
