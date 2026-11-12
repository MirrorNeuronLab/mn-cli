"""Select remote command adapters before entering any local runtime callback."""

from __future__ import annotations

import functools
import inspect

import click
import typer
from mn_sdk.errors import AppError


def unsupported(message: str = "This command manages local runtime state."):
    raise AppError(
        "MN_CLIENT_UNSUPPORTED",
        message,
        hint="Run host setup on the execution node.",
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
    title = " ".join(command).title()
    details = result
    if isinstance(result, dict):
        from mn_cli.output import json_enabled

        admission = result.get("admission") or {}
        if (
            not json_enabled()
            and isinstance(admission, dict)
            and admission.get("message")
        ):
            console.print(str(admission["message"]), markup=False)
        if isinstance(admission, dict) and admission.get("state") == "queued":
            title = "Run queued"
            details = {
                key: value
                for key, value in result.items()
                if key in {"job_id", "run_id", "status"}
            }
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
        if command == ("blueprint", "list"):
            fields = ("id", "name", "tagline", "type")
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
        print_detail(console, title, details)
    record_result(result)
    return result


def dispatch(command: tuple[str, ...], values: dict):
    if command == ("blueprint", "list"):
        from mn_sdk.blueprint_catalog import list_catalog_blueprints

        ctx = values.get("ctx")
        repository = (ctx.obj or {}).get("blueprint_repo") if ctx is not None else None
        return list_catalog_blueprints(repository=repository)

    from mn_sdk.runtime_connection import (
        connect,
        load_connection,
        remove_node,
        installation_role,
    )
    from mn_sdk.runtime_resources import RuntimeResources

    family, action = command[:2]
    if family == "node" and action == "add":
        return connect(
            values["host"],
            values.get("token"),
            grpc_port=values.get("grpc_port"),
            runtime_home=values.get("runtime_home"),
        )
    if family == "node" and action == "remove":
        confirm("Remove this runtime from the shared cluster", values)
        return remove_node(values["node_name"])
    if family == "runtime":
        if action in {
            "start",
            "stop",
            "restart-sidecars",
            "ensure-context-engine",
            "reconnect",
            "cleanup",
            "upgrade",
            "doctor",
        }:
            if action == "cleanup" and not values.get("dry_run"):
                confirm("Clean unused runtime resources", values)
            if action == "upgrade" and not values.get("check_only"):
                confirm("Upgrade the installed runtime", values)
                values["yes"] = True
            from mn_sdk.installed_lifecycle import run

            return run(action, values)
        if action == "status" and installation_role() == "client":
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
        if action == "status":
            from mn_sdk.installed_lifecycle import run

            return run("status", values)
    api = RuntimeResources()
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
