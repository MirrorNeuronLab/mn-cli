"""Command declarations for `mn model`."""

from pathlib import Path
from typing import Annotated

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def model_list(
    available: Annotated[
        bool,
        typer.Option(
            "--available", help="Include catalog models that have not been added."
        ),
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    "List registered models and discovered DMR artifacts."
    return display(("model", "list"), dispatch(("model", "list"), locals()))


def model_add(
    model: Annotated[
        str | None,
        typer.Argument(..., help="Catalog id or Docker Model Runner model reference."),
    ] = None,
    definition_file: Annotated[
        Path | None, typer.Option("--file", help="Provider model definition JSON file.")
    ] = None,
    backend: Annotated[
        str, typer.Option("--backend", help="Backend: auto, llama.cpp, vllm, or nim.")
    ] = "auto",
    context_size: Annotated[
        int | None, typer.Option("--context-size", help="Override model context size.")
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Add even when hardware compatibility fails."),
    ] = False,
    node: Annotated[
        list[str] | None,
        typer.Option(
            "--node",
            help="Add on a named runtime cluster node. Repeat for multiple replicas.",
        ),
    ] = None,
    local: Annotated[
        bool, typer.Option("--local", help="Use the local model installation.")
    ] = False,
    default: Annotated[
        bool,
        typer.Option(
            "--default",
            help="Install the configured default, or make MODEL/--file the logical default.",
        ),
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    "Add a DMR or NVIDIA Docker model, or a provider definition."
    return display(("model", "add"), dispatch(("model", "add"), locals()))


def model_show(
    model: Annotated[
        str | None,
        typer.Argument(
            ...,
            help="Model id, alias, or DMR reference. Omit to show the full catalog.",
        ),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    "Show all catalog models or one model. Use `mn model doctor` for live checks."
    return display(("model", "show"), dispatch(("model", "show"), locals()))


def model_probe(
    model: Annotated[
        str | None,
        typer.Argument(
            ...,
            help="Registered model id, catalog id, alias, or DMR reference. Omit to probe every model in `mn model list`.",
        ),
    ] = None,
    capabilities: Annotated[
        str,
        typer.Option(
            "--capabilities",
            help="Comma-separated capability subset. Defaults to embeddings, image input, structured output, streaming, and thinking.",
        ),
    ] = "",
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    "Live-probe one or all runtime models through LiteLLM and cache results."
    return display(("model", "probe"), dispatch(("model", "probe"), locals()))


def model_update(
    model: Annotated[
        str | None, typer.Argument(..., help="Registered model id.")
    ] = None,
    all_models: Annotated[
        bool, typer.Option("--all", help="Update all registered models.")
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Update even when hardware compatibility fails."),
    ] = False,
    node: Annotated[
        list[str] | None,
        typer.Option(
            "--node", help="Update the installation on a named node. Repeat as needed."
        ),
    ] = None,
    local: Annotated[
        bool, typer.Option("--local", help="Update only the local installation.")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    "Update a registered model from its source."
    return display(("model", "update"), dispatch(("model", "update"), locals()))


def model_remove(
    model: Annotated[
        str, typer.Argument(..., help="Registered model id or unmanaged DMR reference.")
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force", help="Remove a DMR model even when blueprint owners remain."
        ),
    ] = False,
    keep_artifact: Annotated[
        bool,
        typer.Option(
            "--keep-artifact",
            help="Unregister a DMR model without deleting its artifact.",
        ),
    ] = False,
    node: Annotated[
        list[str] | None,
        typer.Option(
            "--node", help="Remove the installation on a named node. Repeat as needed."
        ),
    ] = None,
    local: Annotated[
        bool, typer.Option("--local", help="Remove the local installation.")
    ] = False,
    all_nodes: Annotated[
        bool, typer.Option("--all-nodes", help="Remove every recorded installation.")
    ] = False,
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Confirm removal without prompting.")
    ] = False,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Preview removal without changing state.")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    "Remove a registered model or unmanaged DMR artifact."
    return display(("model", "remove"), dispatch(("model", "remove"), locals()))


def model_doctor(
    model: Annotated[
        str,
        typer.Argument(..., help="Registered model id, catalog id, or DMR reference."),
    ] = "nemotron-3.5-bolt-e4b:q4_k_m",
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
):
    "Check model definition, runtime state, and route health."
    return display(("model", "doctor"), dispatch(("model", "doctor"), locals()))


def model_start(
    model: Annotated[str, typer.Argument(..., help="Installed model id or alias.")],
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
    "Start a model and wait for readiness."
    return display(("model", "start"), dispatch(("model", "start"), locals()))


def model_stop(
    model: Annotated[str, typer.Argument(..., help="Installed model id or alias.")],
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
    "Stop serving a model while retaining downloaded artifacts."
    return display(("model", "stop"), dispatch(("model", "stop"), locals()))


def model_unload(
    model: Annotated[str, typer.Argument(..., help="Installed model id or alias.")],
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
    "Release model GPU memory while retaining downloaded artifacts."
    return display(("model", "unload"), dispatch(("model", "unload"), locals()))


def register(app: typer.Typer) -> None:
    model_app = typer.Typer(
        help="Manage Docker Model Runner, NVIDIA Docker, and provider-backed runtime models",
        cls=RemediatingTyperGroup,
    )
    model_app.command("list", hidden=False)(model_list)
    model_app.command("add", hidden=False)(model_add)
    model_app.command("show", hidden=False)(model_show)
    model_app.command("probe", hidden=False)(model_probe)
    model_app.command("update", hidden=False)(model_update)
    model_app.command("remove", hidden=False)(model_remove)
    model_app.command("doctor", hidden=False)(model_doctor)
    model_app.command("start", hidden=False)(model_start)
    model_app.command("stop", hidden=False)(model_stop)
    model_app.command("unload", hidden=False)(model_unload)
    app.add_typer(model_app, name="model")
