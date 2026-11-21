"""Command declarations for `mn runtime`."""

from typing import Optional

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def runtime_start(
    host: Optional[str] = typer.Option(
        None, "--host", help="Federation-reachable host or IP advertised by this node."
    ),
    grpc_port: int = typer.Option(55051, "--grpc-port", help="Core gRPC port."),
):
    "Start a federation-capable MirrorNeuron runtime"
    return display(("runtime", "start"), dispatch(("runtime", "start"), locals()))


def runtime_stop():
    "Stop MirrorNeuron services"
    return display(("runtime", "stop"), dispatch(("runtime", "stop"), locals()))


def runtime_status(
    json_output: bool = typer.Option(
        False, "--json", help="Print machine-readable JSON."
    ),
    timeout: float = typer.Option(
        3.0, "--timeout", help="Per-component timeout in seconds.", min=0.1
    ),
):
    "Report runtime endpoints, health, nodes, jobs, and shared storage"
    return display(("runtime", "status"), dispatch(("runtime", "status"), locals()))


def runtime_reconnect():
    ""
    return display(
        ("runtime", "reconnect"), dispatch(("runtime", "reconnect"), locals())
    )


def runtime_doctor(
    json_output: bool = typer.Option(
        False, "--json", help="Print machine-readable JSON."
    ),
    timeout: float = typer.Option(
        3.0, "--timeout", help="Per-component timeout in seconds.", min=0.1
    ),
    repair: bool = typer.Option(
        False,
        "--repair",
        help="Repair unhealthy replaceable runtime sidecars, then recheck.",
    ),
):
    "Check runtime foundation services before running blueprints"
    return display(("runtime", "doctor"), dispatch(("runtime", "doctor"), locals()))


def runtime_cleanup(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report confirmed cleanup candidates without removing them.",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Apply cleanup without prompting."
    ),
    include_cache: bool = typer.Option(
        False,
        "--include-cache",
        help="Also apply the managed DockerWorker image cache policy.",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print machine-readable JSON."
    ),
):
    "Safely reconcile node-local Docker and OpenShell resources."
    return display(("runtime", "cleanup"), dispatch(("runtime", "cleanup"), locals()))


def runtime_restart_sidecars(
    api: bool = typer.Option(False, "--api", help="Restart the REST API sidecar."),
    web_ui: bool = typer.Option(False, "--web-ui", help="Restart the Web UI sidecar."),
):
    "Restart only the REST API and/or Web UI sidecars"
    return display(
        ("runtime", "restart-sidecars"),
        dispatch(("runtime", "restart-sidecars"), locals()),
    )


def runtime_ensure_context_engine(
    force: bool = typer.Option(
        False, "--force", help="Recreate the context engine even if it is running."
    ),
):
    "Prepare and start the packaged Membrane context engine service"
    return display(
        ("runtime", "ensure-context-engine"),
        dispatch(("runtime", "ensure-context-engine"), locals()),
    )


def runtime_upgrade(
    yes: bool = typer.Option(False, "--yes", "-y", help="Upgrade without prompting."),
    check_only: bool = typer.Option(
        False, "--check-only", help="Only check for upgrades; do not install them."
    ),
):
    "Install available release-pinned runtime upgrades."
    return display(("runtime", "upgrade"), dispatch(("runtime", "upgrade"), locals()))


def register(app: typer.Typer) -> None:
    runtime_app = typer.Typer(
        help="Start, stop, upgrade, and diagnose the local MirrorNeuron runtime.\n\nExamples:\n  mn runtime start\n  mn runtime upgrade\n  mn runtime status\n  mn runtime doctor\n  mn runtime doctor --json\n  mn runtime ensure-context-engine\n  mn runtime restart-sidecars --api\n  mn runtime restart-sidecars --web-ui\n  mn runtime stop\n",
        cls=RemediatingTyperGroup,
    )
    runtime_app.command("start", hidden=False)(runtime_start)
    runtime_app.command("stop", hidden=False)(runtime_stop)
    runtime_app.command("status", hidden=False)(runtime_status)
    runtime_app.command("reconnect", hidden=False)(runtime_reconnect)
    runtime_app.command("doctor", hidden=False)(runtime_doctor)
    runtime_app.command("cleanup", hidden=False)(runtime_cleanup)
    runtime_app.command("restart-sidecars", hidden=False)(runtime_restart_sidecars)
    runtime_app.command("ensure-context-engine", hidden=False)(
        runtime_ensure_context_engine
    )
    runtime_app.command("upgrade", hidden=False)(runtime_upgrade)
    app.add_typer(runtime_app, name="runtime")
