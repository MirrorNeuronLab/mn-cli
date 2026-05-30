from importlib import metadata

import typer
from mn_cli import update_cmds
from mn_cli.banner import MN_ASCII_ART, format_banner
from mn_cli.libs import backup_cmds, deployment_cmds, job_cmds, resource_cmds, run_cmds, schedule_cmds, service_cmds, sys_cmds
from mn_cli.libs.blueprint_cmds import blueprint_app

PACKAGE_NAME = "mirrorneuron-cli"
FALLBACK_VERSION = "0.0.0"

app = typer.Typer(help="MirrorNeuron CLI")
job_app = typer.Typer(help="Job commands")
node_app = typer.Typer(help="Node and cluster commands")
runtime_app = typer.Typer(help="Local runtime commands")


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

# Blueprint commands
blueprint_app.command(name="validate")(run_cmds.validate)

# Job commands
job_app.command(name="submit")(job_cmds.submit)
job_app.command(name="status")(job_cmds.status)
job_app.command(name="list")(job_cmds.list_jobs)
job_app.command(name="clear")(job_cmds.clear)
job_app.command(name="cancel")(job_cmds.cancel)
job_app.command(name="pause")(job_cmds.pause)
job_app.command(name="resume")(job_cmds.resume)
job_app.command(name="backup")(backup_cmds.backup)
job_app.command(name="restore")(backup_cmds.restore)
job_app.command(name="unfinished")(job_cmds.unfinished)
job_app.command(name="monitor")(run_cmds.monitor)
job_app.command(name="result")(run_cmds.result)
job_app.command(name="dead-letters")(job_cmds.dead_letters)

# Node commands
node_app.command(name="list")(job_cmds.nodes)
node_app.command(name="reconcile")(job_cmds.reconcile_node)
node_app.command(name="drain")(job_cmds.drain_node)
node_app.command(name="undrain")(job_cmds.undrain_node)
node_app.command(name="maintenance")(job_cmds.maintenance_node)
node_app.command(name="join")(sys_cmds.join)
node_app.command(name="expose")(sys_cmds.expose_node)
node_app.command(name="add")(sys_cmds.add_node)
node_app.command(name="leave")(sys_cmds.leave)
node_app.command(name="refresh-token")(sys_cmds.refresh_token)

# Runtime commands
runtime_app.command(name="start")(sys_cmds.start)
runtime_app.command(name="stop")(sys_cmds.stop)
runtime_app.command(name="update")(update_cmds.update)
runtime_app.command(name="metrics")(job_cmds.metrics)

# Deployment commands
deployment_cmds.deployment_app.command(name="deploy")(deployment_cmds.deploy)

# Sub-apps
app.add_typer(blueprint_app, name="blueprint")
app.add_typer(job_app, name="job")
app.add_typer(node_app, name="node")
app.add_typer(runtime_app, name="runtime")
app.add_typer(resource_cmds.resource_app, name="resource")
app.add_typer(service_cmds.service_app, name="service")
app.add_typer(deployment_cmds.deployment_app, name="deployment")
app.add_typer(schedule_cmds.schedule_app, name="schedule")
app.add_typer(schedule_cmds.trigger_app, name="trigger")
app.add_typer(schedule_cmds.event_app, name="event")

if __name__ == "__main__":
    app()
