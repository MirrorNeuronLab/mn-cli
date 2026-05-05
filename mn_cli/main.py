import typer
from mn_cli import update_cmds
from mn_cli.libs import job_cmds, run_cmds, sys_cmds
from mn_cli.libs.blueprint_cmds import blueprint_app

app = typer.Typer(help="MirrorNeuron CLI")


@app.callback()
def main(ctx: typer.Context):
    update_cmds.maybe_prompt_for_update(ctx.invoked_subcommand)

# Run commands
app.command(name="validate")(run_cmds.validate)
app.command(name="run")(run_cmds.run)
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
app.command(name="nodes")(job_cmds.nodes)
app.command(name="metrics")(job_cmds.metrics)
app.command(name="dead-letters")(job_cmds.dead_letters)

# System commands
app.command(name="start")(sys_cmds.start)
app.command(name="stop")(sys_cmds.stop)
app.command(name="join")(sys_cmds.join)
app.command(name="leave")(sys_cmds.leave)
app.command(name="update")(update_cmds.update)

# Sub-apps
app.add_typer(blueprint_app, name="blueprint")

if __name__ == "__main__":
    app()
