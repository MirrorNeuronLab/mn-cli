"""Command declarations for `mn node`."""

from typing import Optional

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def node_list():
    "List connected nodes with their reachable endpoint and job role."
    return display(("node", "list"), dispatch(("node", "list"), locals()))


def node_show(node_name: str = typer.Argument(..., help="Runtime node name.")):
    "Show one runtime node from the current system summary."
    return display(("node", "show"), dispatch(("node", "show"), locals()))


def node_add(
    host: str,
    token: Optional[str] = typer.Option(
        None, "--token", help="Runtime node control token; required for remote hosts."
    ),
    grpc_port: Optional[int] = typer.Option(None, "--grpc-port", min=1, max=65535),
    runtime_home: Optional[str] = typer.Option(
        None, "--runtime-home", help="Installation path for mn node add local."
    ),
):
    """Connect to a runtime; subsequent additions join peers to its cluster."""
    return display(("node", "add"), dispatch(("node", "add"), locals()))


def node_remove(
    node_name: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Confirm peer removal."),
):
    "Remove one reciprocal federated peer from this runtime."
    return display(("node", "remove"), dispatch(("node", "remove"), locals()))


def node_reconcile(
    node_name: str,
    reason: str = typer.Option(
        "", "--reason", help="Reason recorded on reconciliation events."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Plan reconciliation without moving work."
    ),
):
    "Reconcile jobs affected by an unavailable node"
    return display(("node", "reconcile"), dispatch(("node", "reconcile"), locals()))


def node_drain(
    node_name: str,
    reason: str = typer.Option("", "--reason", help="Reason recorded on drain events."),
    deadline: str = typer.Option(
        "30m", "--deadline", help="Drain deadline, e.g. 30m, 10s, 1h."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Plan the drain without changing node state."
    ),
    wait: bool = typer.Option(
        False, "--wait", help="Poll drain status until it completes or blocks."
    ),
    ignore_system_jobs: bool = typer.Option(
        True,
        "--ignore-system-jobs/--include-system-jobs",
        help="Ignore system/sysbatch jobs while draining.",
    ),
):
    "Drain a node and move safe workloads elsewhere"
    return display(("node", "drain"), dispatch(("node", "drain"), locals()))


def node_undrain(
    node_name: str,
    reason: str = typer.Option(
        "", "--reason", help="Reason recorded on undrain events."
    ),
    mark_eligible: bool = typer.Option(
        False,
        "--mark-eligible",
        help="Make the node schedulable after cancelling/completing drain.",
    ),
):
    "Cancel node drain and optionally make the node schedulable"
    return display(("node", "undrain"), dispatch(("node", "undrain"), locals()))


def node_maintenance(
    node_name: str,
    enable: bool = typer.Option(
        True, "--enable/--disable", help="Enable or disable maintenance mode."
    ),
    reason: str = typer.Option(
        "", "--reason", help="Reason recorded on maintenance events."
    ),
):
    "Toggle node maintenance mode without moving existing work"
    return display(("node", "maintenance"), dispatch(("node", "maintenance"), locals()))


def node_refresh_token():
    "Rotate the persistent MirrorNeuron network join token"
    return display(
        ("node", "refresh-token"), dispatch(("node", "refresh-token"), locals())
    )


def register(app: typer.Typer) -> None:
    node_app = typer.Typer(
        help="Inspect cluster nodes and manage node membership or maintenance.\n\nExamples:\n  mn node list\n  mn node drain <node-name> --reason maintenance --wait\n  mn node add <host> --token <token>\n  mn node remove <node-name> --yes\n",
        cls=RemediatingTyperGroup,
    )
    node_app.command("list", hidden=False)(node_list)
    node_app.command("show", hidden=False)(node_show)
    node_app.command("add", hidden=False)(node_add)
    node_app.command("remove", hidden=False)(node_remove)
    node_app.command("reconcile", hidden=False)(node_reconcile)
    node_app.command("drain", hidden=False)(node_drain)
    node_app.command("undrain", hidden=False)(node_undrain)
    node_app.command("maintenance", hidden=False)(node_maintenance)
    node_app.command("refresh-token", hidden=False)(node_refresh_token)
    app.add_typer(node_app, name="node")
