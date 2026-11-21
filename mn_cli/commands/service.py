"""Command declarations for `mn service`."""

from typing import Annotated, Optional

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def service_list(
    name: Annotated[
        Optional[str], typer.Option("--name", help="Filter by service name.")
    ] = None,
    node: Annotated[
        Optional[str], typer.Option("--node", help="Filter by node name.")
    ] = None,
    job_id: Annotated[
        Optional[str], typer.Option("--job-id", help="Filter by job ID.")
    ] = None,
    status: Annotated[
        Optional[str], typer.Option("--status", help="Filter by health status.")
    ] = None,
    all_statuses: Annotated[
        bool, typer.Option("--all", help="Show warning and critical services too.")
    ] = False,
):
    "List registered services"
    return display(("service", "list"), dispatch(("service", "list"), locals()))


def service_show(
    name: str,
    tag: Annotated[
        Optional[list[str]], typer.Option("--tag", help="Require a service tag.")
    ] = None,
    node: Annotated[
        Optional[str], typer.Option("--node", help="Filter by node name.")
    ] = None,
    all_statuses: Annotated[
        bool, typer.Option("--all", help="Return warning and critical services too.")
    ] = False,
):
    "Resolve healthy instances for one service"
    return display(("service", "show"), dispatch(("service", "show"), locals()))


def register(app: typer.Typer) -> None:
    service_app = typer.Typer(
        help="Inspect MirrorNeuron service discovery.", cls=RemediatingTyperGroup
    )
    service_app.command("list", hidden=False)(service_list)
    service_app.command("show", hidden=False)(service_show)
    app.add_typer(service_app, name="service")
