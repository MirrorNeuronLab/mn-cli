"""CLI adaptation for source-neutral model start, stop, and unload."""

import json
from typing import Annotated

import typer
from mn_sdk.model_service import (
    start_runtime_model,
    stop_runtime_model,
    unload_runtime_model,
)

from mn_cli.libs.ui import print_success_confirmation
from mn_cli.output import record_result


def _operate(action, model, *, node, local, json_output):
    from mn_cli.libs import model_cmds

    try:
        if local and node:
            raise ValueError("--local cannot be combined with --node")
        record = model_cmds.get_registered_model(model) or {}
        installations = record.get("installations") or []
        if not local and not node and len(installations) > 1:
            raise ValueError(
                "This model has multiple installations. Select --node or --local."
            )
        selected = "" if local else node or record.get("selected_node") or ""
        if selected and selected not in {
            "local",
            model_cmds._local_runtime_node_name(),
        }:
            endpoint = model_cmds._cluster_node_endpoint(selected)
            client = model_cmds._native_runtime_client_for_node(endpoint)
            response = client.prepare_runtime_model(
                {"action": action, "model": model, "node": selected}
            )
            result = json.loads(response) if isinstance(response, str) else response
        else:
            result = {
                "start": start_runtime_model,
                "stop": stop_runtime_model,
                "unload": unload_runtime_model,
            }[action](model)
        if not isinstance(result, dict) or result.get("status") not in {
            "running",
            "stopped",
            "unloaded",
        }:
            raise RuntimeError("Model lifecycle operation did not complete")
        if json_output:
            record_result(result)
        else:
            print_success_confirmation(
                model_cmds.console,
                f"Model {action}",
                status=result["status"],
                details={"Model": model, "Node": selected or "local"},
            )
    except Exception as exc:
        model_cmds._handle_model_error(exc, f"model {action}", model=model)
        raise typer.Exit(1)


def start(
    model: Annotated[str, typer.Argument(help="Installed model id or alias.")],
    node: Annotated[
        str | None, typer.Option("--node", help="Runtime node owning the model.")
    ] = None,
    local: Annotated[
        bool, typer.Option("--local", help="Use the local installation.")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    """Start a model and wait for readiness."""
    _operate("start", model, node=node, local=local, json_output=json_output)


def stop(
    model: Annotated[str, typer.Argument(help="Installed model id or alias.")],
    node: Annotated[
        str | None, typer.Option("--node", help="Runtime node owning the model.")
    ] = None,
    local: Annotated[
        bool, typer.Option("--local", help="Use the local installation.")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    """Stop serving a model while retaining downloaded artifacts."""
    _operate("stop", model, node=node, local=local, json_output=json_output)


def unload(
    model: Annotated[str, typer.Argument(help="Installed model id or alias.")],
    node: Annotated[
        str | None, typer.Option("--node", help="Runtime node owning the model.")
    ] = None,
    local: Annotated[
        bool, typer.Option("--local", help="Use the local installation.")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    """Release model GPU memory while retaining downloaded artifacts."""
    _operate("unload", model, node=node, local=local, json_output=json_output)
