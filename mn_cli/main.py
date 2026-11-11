import os
import sys
from importlib import metadata

import typer
from rich.console import Console

from mn_cli.config import bootstrap_environment
from mn_cli.terminal import ui_width

bootstrap_environment()

from mn_cli.banner import format_banner
from mn_cli.error_handler import handle_cli_error, set_debug
from mn_cli.output import RemediatingTyperGroup, instrument_typer
from mn_cli.commands import register_commands

PACKAGE_NAME = "mirrorneuron-cli"
FALLBACK_VERSION = "0.0.0"
CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
    "max_content_width": ui_width(),
}
ROOT_HELP = """Run and operate MirrorNeuron workflows, blueprints, jobs, and local runtime services.

Examples:
  mn blueprint list
  mn blueprint run <blueprint-id>
  mn job show <job-id>
  mn run show <run-id>
  mn runtime status
  mn runtime doctor --json

Notes:
  Connect with mn node add <HOST> --token <RUNTIME_NODE_TOKEN>, or mn node add local.
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
  mn node remove <node-name> --yes
"""
RUNTIME_HELP = """Start, stop, upgrade, and diagnose the local MirrorNeuron runtime.

Examples:
  mn runtime start
  mn runtime upgrade
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


def get_version() -> str:
    try:
        return metadata.version(PACKAGE_NAME)
    except metadata.PackageNotFoundError:
        return FALLBACK_VERSION


def format_version() -> str:
    lines = [
        format_banner("MirrorNeuron CLI", width=ui_width()),
        f"version {get_version()}",
    ]
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
        os.environ["MN_DEBUG"] = "1"
    ctx.max_content_width = ui_width()
    if ctx.invoked_subcommand is None:
        typer.echo(format_banner("MirrorNeuron CLI", width=ui_width()))
        mode = _runtime_mode_line()
        if mode:
            typer.echo(mode)
        typer.echo(ctx.get_help())
        raise typer.Exit()


def _runtime_mode_line(*, capitalize: bool = True) -> str:
    from mn_sdk.runtime_connection import installation_role

    role = installation_role()
    return (
        "Client mode: connected runtime execution"
        if role == "client"
        else "Full mode: surface and runtime"
    )


register_commands(app)
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
            command_parts = [
                part for part in sys.argv[1:] if part and not part.startswith("-")
            ]
            command = " ".join(command_parts[:2]) or "command"
            handle_cli_error(
                exc,
                Console(stderr=True),
                command,
                command_context={"command": command, "argv": sys.argv[1:]},
            )
        except typer.Exit as exit_exc:
            raise SystemExit(exit_exc.exit_code) from exc


if __name__ == "__main__":
    cli()
