"""Select remote command adapters before entering any local runtime callback."""

from __future__ import annotations

import functools
import inspect

import click
import typer
from mn_sdk.client_install import is_client_install
from mn_sdk.errors import AppError


def unsupported(message: str = "This command manages local runtime state."):
    raise AppError(
        "MN_CLIENT_UNSUPPORTED",
        message,
        hint="Client mode uses the remote catalog and runtime. Run host setup on the runtime node.",
        exit_code=2,
        http_status=422,
    )


def confirm(action: str, values: dict):
    from mn_cli.libs.ui import require_confirmation
    from mn_cli.shared import console

    require_confirmation(
        console, action=action, prompt=action + "?", yes=values.get("yes") is True
    )


def display(command: tuple[str, ...], result):
    from mn_cli.libs.ui import print_collection, print_detail
    from mn_cli.output import record_result
    from mn_cli.shared import console

    if result is None:
        result = {"status": "complete"}
    if isinstance(result, list):
        result = {"items": result}
    if isinstance(result, dict) and isinstance(result.get("items"), list):
        items = result["items"]
        fields = (
            "id",
            "blueprint_id",
            "job_id",
            "run_id",
            "node_name",
            "node",
            "name",
            "model",
            "status",
            "level",
            "message",
            "type",
        )
        columns = [
            (key.replace("_", " ").title(), key)
            for key in fields
            if any(key in row for row in items)
        ]
        print_collection(
            console,
            " ".join(command).title(),
            items,
            columns=tuple(columns or [("ID", "id")]),
        )
    else:
        print_detail(console, " ".join(command).title(), result)
    record_result(result)
    return result


def dispatch(command: tuple[str, ...], values: dict):
    from mn_sdk.client_install import disconnect, load_connection
    from mn_sdk.remote_api import RemoteApiClient, connect

    family, action = command[:2]
    if family == "node" and action == "add":
        context = click.get_current_context(silent=True)
        for key in ("grpc_port", "docker_network_mode", "docker_network_name"):
            if (
                context
                and context.get_parameter_source(key)
                == click.core.ParameterSource.COMMANDLINE
            ):
                unsupported(
                    "Client node add accepts the remote API URL and API token; omit runtime network options."
                )
        return connect(values["host"], values["token"])
    if family == "node" and action == "remove":
        confirm("Disconnect the client from the remote node", values)
        return disconnect(values["node_name"])
    if family == "runtime":
        if action == "status":
            try:
                connection = load_connection().public()
            except AppError as exc:
                if exc.code != "MN_CLIENT_NOT_CONNECTED":
                    raise
                connection = {"status": "disconnected"}
            return {
                "mode": "client",
                "runtime_installed": False,
                "connection": connection,
            }
        if action != "doctor":
            unsupported("No local runtime is installed in client mode.")
    # Local path overrides never reach remote workers as implicit local reads.
    for key in (
        "runs_root",
        "python_envs_dir",
        "generated_bundles_dir",
        "bundle_cache_dir",
        "local",
    ):
        if values.get(key):
            unsupported(
                "Local runtime and filesystem overrides are unavailable in client mode."
            )
    api = RemoteApiClient()
    if family == "blueprint":
        from mn_cli.client_blueprints import execute
    elif family == "run":
        from mn_cli.client_runs import execute
    else:
        from mn_cli.client_management import execute
    return execute(api, command, values)


def _wrap(callback, command):
    signature = inspect.signature(callback)

    @functools.wraps(callback)
    def wrapped(*args, **kwargs):
        if not is_client_install():
            return callback(*args, **kwargs)
        values = dict(signature.bind(*args, **kwargs).arguments)
        try:
            return display(command, dispatch(command, values))
        except (typer.Exit, click.ClickException):
            raise
        except Exception as exc:
            from mn_cli.error_handler import handle_cli_error
            from mn_cli.shared import console

            handle_cli_error(exc, console, " ".join(command))

    return wrapped


def bind_client_commands(app: typer.Typer, prefix: tuple[str, ...] = ()) -> None:
    for info in app.registered_commands:
        if info.callback is not None:
            name = info.name or info.callback.__name__.replace("_", "-")
            info.callback = _wrap(info.callback, (*prefix, name))
    for group in app.registered_groups:
        bind_client_commands(group.typer_instance, (*prefix, group.name))
