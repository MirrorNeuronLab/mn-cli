from importlib import metadata

import typer
from mn_cli import update_cmds
from mn_cli.banner import MN_ASCII_ART, format_banner
from mn_cli.libs import deployment_cmds, job_cmds, resource_cmds, run_cmds, schedule_cmds, service_cmds, sys_cmds
from mn_cli.libs.blueprint_cmds import blueprint_app

PACKAGE_NAME = "mirrorneuron-cli"
FALLBACK_VERSION = "0.0.0"

app = typer.Typer(help="MirrorNeuron CLI")


def get_version() -> str:
    try:
        return metadata.version(PACKAGE_NAME)
    except metadata.PackageNotFoundError:
        return FALLBACK_VERSION


def format_version() -> str:
    return f"{format_banner('MirrorNeuron CLI')}\nversion {get_version()}"


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
):
    update_cmds.maybe_prompt_for_update(ctx.invoked_subcommand)

# Run commands
app.command(name="validate")(run_cmds.validate)
app.command(name="run")(run_cmds.run)
app.command(name="deploy")(deployment_cmds.deploy)
app.command(name="monitor")(run_cmds.monitor)
app.command(name="result")(run_cmds.result)

# Job commands
app.command(name="submit")(job_cmds.submit)
app.command(name="status")(job_cmds.status)
app.command(name="list")(job_cmds.list_jobs)
app.command(name="clear")(job_cmds.clear)
app.command(name="cancel")(job_cmds.cancel)
app.command(name="pause")(job_cmds.pause)
app.command(name="resume")(job_cmds.resume)
app.command(name="unfinished")(job_cmds.unfinished)
app.command(name="nodes")(job_cmds.nodes)
app.command(name="reconcile-node")(job_cmds.reconcile_node)
app.command(name="drain-node")(job_cmds.drain_node)
app.command(name="undrain-node")(job_cmds.undrain_node)
app.command(name="maintenance-node")(job_cmds.maintenance_node)
app.command(name="metrics")(job_cmds.metrics)
app.command(name="dead-letters")(job_cmds.dead_letters)

# System commands
app.command(name="start")(sys_cmds.start)
app.command(name="stop")(sys_cmds.stop)
app.command(name="join")(sys_cmds.join)
app.command(name="expose-node")(sys_cmds.expose_node)
app.command(name="add-node")(sys_cmds.add_node)
app.command(name="leave")(sys_cmds.leave)
app.command(name="update")(update_cmds.update)

# Sub-apps
app.add_typer(blueprint_app, name="blueprint")
app.add_typer(resource_cmds.resource_app, name="resource")
app.add_typer(service_cmds.service_app, name="service")
app.add_typer(deployment_cmds.deployment_app, name="deployment")
app.add_typer(schedule_cmds.schedule_app, name="schedule")
app.add_typer(schedule_cmds.trigger_app, name="trigger")
app.add_typer(schedule_cmds.event_app, name="event")

if __name__ == "__main__":
    app()
