"""Command declarations for `mn schedule`."""

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def schedule_list(
    node: str = typer.Option("", "--node", help="Selected PC's owner node."),
):
    ""
    return display(("schedule", "list"), dispatch(("schedule", "list"), locals()))


def schedule_show(schedule_id: str, node: str = typer.Option("", "--node")):
    ""
    return display(("schedule", "show"), dispatch(("schedule", "show"), locals()))


def schedule_set_time(
    schedule_id: str,
    at: str = typer.Option(
        ..., "--at", help="Future ISO 8601 date and time with timezone."
    ),
    node: str = typer.Option("", "--node"),
):
    ""
    return display(
        ("schedule", "set-time"), dispatch(("schedule", "set-time"), locals())
    )


def schedule_cancel(schedule_id: str, node: str = typer.Option("", "--node")):
    ""
    return display(("schedule", "cancel"), dispatch(("schedule", "cancel"), locals()))


def register(app: typer.Typer) -> None:
    schedule_app = typer.Typer(
        help="Inspect schedules and automatically queued starts.",
        cls=RemediatingTyperGroup,
    )
    schedule_app.command("list", hidden=False)(schedule_list)
    schedule_app.command("show", hidden=False)(schedule_show)
    schedule_app.command("set-time", hidden=False)(schedule_set_time)
    schedule_app.command("cancel", hidden=False)(schedule_cancel)
    app.add_typer(schedule_app, name="schedule")
