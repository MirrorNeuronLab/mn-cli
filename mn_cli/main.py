from importlib import metadata
import os
import sys

import typer
from rich.console import Console

from mn_cli.config import bootstrap_environment
from mn_cli.terminal import ui_width

bootstrap_environment()

from mn_cli import update_cmds
from mn_cli.banner import format_banner
from mn_cli.libs import blueprint_cmds, deployment_cmds, job_cmds, model_cmds, operation_cmds, resource_cmds, run_cmds, run_public, schedule_cmds, service_cmds, stable_job_cmds, sys_cmds
from mn_cli.error_handler import handle_cli_error, set_debug
from mn_cli.output import RemediatingTyperGroup, instrument_typer
from mn_cli.runtime_mode import local_runtime_mode
from mn_cli.terminal import is_ci, is_interactive

PACKAGE_NAME = "mirrorneuron-cli"
FALLBACK_VERSION = "0.0.0"
CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"], "max_content_width": ui_width()}
ROOT_HELP = """Run and operate MirrorNeuron workflows, blueprints, jobs, and local runtime services.

Examples:
  mn blueprint list
  mn blueprint run <blueprint-id>
  mn job show <job-id>
  mn run show <run-id>
  mn runtime status
  mn runtime doctor --json

Notes:
  Runtime connection is read from MN_GRPC_TARGET or ~/.mn/runtime-endpoints.json.
  Set NO_COLOR=1 or MN_CLI_OUTPUT=plain for plain terminal output.
"""
JOB_HELP = """Create and manage durable job definitions.

Examples:
  mn job create ./worker-bundle --job-id worker-daily
  mn job list
  mn job show worker-daily
  mn job start worker-daily
  mn run list --job worker-daily
"""
NODE_HELP = """Inspect cluster nodes and manage node membership or maintenance.

Examples:
  mn node list
  mn node drain <node-name> --reason maintenance --wait
  mn node add <host> --token <token>
"""
RUNTIME_HELP = """Start, stop, update, and diagnose the local MirrorNeuron runtime.

Examples:
  mn runtime start
  mn runtime status
  mn runtime doctor
  mn runtime doctor --json
  mn runtime ensure-context-engine
  mn runtime restart-sidecars --api
  mn runtime restart-sidecars --web-ui
  mn runtime stop
"""

app = typer.Typer(
    help=ROOT_HELP,
    invoke_without_command=True,
    no_args_is_help=False,
    context_settings=CONTEXT_SETTINGS,
    pretty_exceptions_enable=False,
    cls=RemediatingTyperGroup,
)
job_app = typer.Typer(help=JOB_HELP, context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
node_app = typer.Typer(help=NODE_HELP, context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
operation_app = typer.Typer(help="Inspect or reattach to durable group operations.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
runtime_app = typer.Typer(help=RUNTIME_HELP, context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
blueprint_app = typer.Typer(help="List, add, inspect, run, and diagnose MirrorNeuron blueprints.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
run_app = typer.Typer(help="Inspect and control individual execution runs.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
human_app = typer.Typer(help="Inspect and respond to human collaboration events.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
resource_app = typer.Typer(help="Inspect runtime capacity, usage, and limits.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
service_app = typer.Typer(help="Inspect MirrorNeuron service discovery.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
deployment_app = typer.Typer(help="Deploy and manage versioned runtime workloads.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
schedule_app = typer.Typer(help="Manage periodic, delayed, and event-driven schedules.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)
event_app = typer.Typer(help="Emit and inspect runtime trigger events.", context_settings=CONTEXT_SETTINGS, cls=RemediatingTyperGroup)


def get_version() -> str:
    try:
        return metadata.version(PACKAGE_NAME)
    except metadata.PackageNotFoundError:
        return FALLBACK_VERSION


def format_version() -> str:
    lines = [format_banner("MirrorNeuron CLI", width=ui_width()), f"version {get_version()}"]
    mode = _runtime_mode_line(capitalize=False)
    if mode:
        lines.append(mode)
    return "\n".join(lines)


def version_callback(value: bool):
    if value:
        typer.echo(format_version())
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show the installed MirrorNeuron CLI version.",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Show sanitized diagnostic details for unexpected errors.",
    ),
):
    debug_enabled = debug
    set_debug(debug_enabled)
    if debug_enabled:
        # Native preparation runs in the same process but outside Rich's error
        # renderer. Keep noisy Docker build progress behind the global flag too.
        os.environ["MN_DEBUG"] = "1"
    ctx.max_content_width = ui_width()
    if ctx.invoked_subcommand is None:
        typer.echo(format_banner("MirrorNeuron CLI", width=ui_width()))
        mode = _runtime_mode_line()
        if mode:
            typer.echo(mode)
        typer.echo(ctx.get_help())
        raise typer.Exit()
    if "--json" not in sys.argv and not is_ci() and is_interactive():
        update_cmds.maybe_prompt_for_update(ctx.invoked_subcommand)


def _runtime_mode_line(*, capitalize: bool = True) -> str | None:
    if local_runtime_mode() != "worker":
        return None
    prefix = "Runtime mode" if capitalize else "runtime mode"
    return f"{prefix}: worker"

# Blueprint commands
blueprint_app.callback()(blueprint_cmds.blueprint_callback)
blueprint_app.command(name="list")(blueprint_cmds.blueprint_list)
blueprint_app.command(name="add")(blueprint_cmds.blueprint_add)
blueprint_app.command(name="show")(blueprint_cmds.blueprint_show)
blueprint_app.command(name="update")(blueprint_cmds.blueprint_update)
blueprint_app.command(name="remove")(blueprint_cmds.blueprint_remove)
blueprint_app.command(name="run")(blueprint_cmds.blueprint_run)
blueprint_app.command(name="validate")(run_cmds.validate)
blueprint_app.command(name="doctor")(blueprint_cmds.blueprint_doctor)
blueprint_app.command(name="cleanup")(blueprint_cmds.blueprint_cleanup)
blueprint_app.command(name="export")(blueprint_cmds.blueprint_export)

# Job commands
job_app.command(name="list")(stable_job_cmds.definitions)
job_app.command(name="create")(stable_job_cmds.create)
job_app.command(name="show")(stable_job_cmds.inspect)
job_app.command(name="start")(stable_job_cmds.start)
job_app.command(name="archive")(stable_job_cmds.archive)
job_app.command(name="reset-data")(stable_job_cmds.reset_data)
job_app.command(name="delete")(stable_job_cmds.delete)

# Run commands
run_app.command(name="list")(run_public.list_runs)
run_app.command(name="show")(run_public.show_run)
run_app.command(name="watch")(run_public.watch_run)
run_app.command(name="logs")(run_public.logs)
run_app.command(name="result")(run_public.result)
run_app.command(name="resources")(blueprint_cmds.blueprint_resources)
run_app.command(name="compare")(blueprint_cmds.blueprint_compare)
run_app.command(name="pause")(stable_job_cmds.run_pause)
run_app.command(name="resume")(stable_job_cmds.run_resume)
run_app.command(name="cancel")(stable_job_cmds.run_cancel)
run_app.command(name="delete")(stable_job_cmds.run_delete)
human_app.command(name="list")(run_public.human_list)
human_app.command(name="respond")(blueprint_cmds.blueprint_human_respond)
human_app.command(name="ack")(blueprint_cmds.blueprint_human_ack)
run_app.add_typer(human_app, name="human")

# Node commands
node_app.command(name="list")(job_cmds.nodes)
node_app.command(name="show")(job_cmds.show_node)
node_app.command(name="add")(sys_cmds.add_node)
node_app.command(name="remove")(sys_cmds.leave)
node_app.command(name="reconcile")(job_cmds.reconcile_node)
node_app.command(name="drain")(job_cmds.drain_node)
node_app.command(name="undrain")(job_cmds.undrain_node)
node_app.command(name="maintenance")(job_cmds.maintenance_node)
node_app.command(name="refresh-token")(sys_cmds.refresh_token)

# Durable group operations
operation_app.command(name="show")(operation_cmds.status)
operation_app.command(name="watch")(operation_cmds.watch)

# Runtime commands
runtime_app.command(name="start")(sys_cmds.start)
runtime_app.command(name="stop")(sys_cmds.stop)
runtime_app.command(name="status")(sys_cmds.status)
runtime_app.command(name="doctor")(sys_cmds.doctor)
runtime_app.command(name="restart-sidecars")(sys_cmds.restart_sidecars)
runtime_app.command(name="ensure-context-engine")(sys_cmds.ensure_context_engine)
runtime_app.command(name="update")(update_cmds.update)

# Resource commands
resource_app.command(name="show")(resource_cmds.list_resources)
resource_app.command(name="usage")(job_cmds.metrics)
resource_app.command(name="set")(resource_cmds.set_resources)

# Service commands
service_app.command(name="list")(service_cmds.list_services)
service_app.command(name="show")(service_cmds.resolve_service)

# Deployment commands
deployment_app.command(name="list")(deployment_cmds.list_deployments)
deployment_app.command(name="deploy")(deployment_cmds.deploy)
deployment_app.command(name="show")(deployment_cmds.status)
deployment_app.command(name="promote")(deployment_cmds.promote)
deployment_app.command(name="rollback")(deployment_cmds.rollback)
deployment_app.command(name="pause")(deployment_cmds.pause)
deployment_app.command(name="resume")(deployment_cmds.resume)
deployment_app.command(name="fail")(deployment_cmds.fail)

# Schedule and event commands
schedule_app.command(name="list")(schedule_cmds.list_schedules)
schedule_app.command(name="add")(schedule_cmds.add_schedule)
schedule_app.command(name="show")(schedule_cmds.schedule_status)
schedule_app.command(name="pause")(schedule_cmds.pause_schedule)
schedule_app.command(name="resume")(schedule_cmds.resume_schedule)
schedule_app.command(name="run")(schedule_cmds.run_now)
schedule_app.command(name="remove")(schedule_cmds.remove_schedule)
event_app.command(name="list")(schedule_cmds.list_events)
event_app.command(name="emit")(schedule_cmds.emit_event)

# Sub-apps
app.add_typer(blueprint_app, name="blueprint")
app.add_typer(job_app, name="job")
app.add_typer(run_app, name="run")
app.add_typer(model_cmds.model_app, name="model")
app.add_typer(runtime_app, name="runtime")
app.add_typer(node_app, name="node")
app.add_typer(operation_app, name="operation")
app.add_typer(resource_app, name="resource")
app.add_typer(service_app, name="service")
app.add_typer(deployment_app, name="deployment")
app.add_typer(schedule_app, name="schedule")
app.add_typer(event_app, name="event")

instrument_typer(app)

def cli() -> None:
    try:
        app(standalone_mode=True)
    except typer.Exit:
        raise
    except SystemExit:
        raise
    except Exception as exc:
        try:
            handle_cli_error(
                exc,
                Console(stderr=True),
                " ".join(sys.argv[1:2]) or "command",
                command_context={"argv": sys.argv[1:]},
            )
        except typer.Exit as exit_exc:
            raise SystemExit(exit_exc.exit_code) from exc


if __name__ == "__main__":
    cli()
