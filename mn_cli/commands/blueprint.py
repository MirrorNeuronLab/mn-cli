"""Command declarations for `mn blueprint`."""

from typing import Annotated, Optional

import typer

from mn_cli.client_commands import dispatch, display
from mn_cli.output import RemediatingTyperGroup


def blueprint_list(ctx: typer.models.Context):
    "List blueprints from this surface's catalog without connecting a runtime."
    return display(("blueprint", "list"), dispatch(("blueprint", "list"), locals()))


def blueprint_add(
    ctx: typer.models.Context,
    blueprint_id: str = typer.Argument(..., help="Catalog blueprint ID to add."),
    source: str | None = typer.Option(
        None, "--source", help="Blueprint repository URL or local path."
    ),
    revision: str | None = typer.Option(
        None, "--revision", help="Git revision to add."
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Override compatibility checks; does not replace confirmation.",
    ),
):
    "Add one catalog blueprint and its required runtime models."
    return display(("blueprint", "add"), dispatch(("blueprint", "add"), locals()))


def blueprint_show(
    ctx: typer.models.Context,
    blueprint_id: str = typer.Argument(..., help="Catalog blueprint ID."),
    source: str | None = typer.Option(
        None, "--source", help="Blueprint repository URL or local path."
    ),
):
    "Show one catalog blueprint without preparing or running it."
    return display(("blueprint", "show"), dispatch(("blueprint", "show"), locals()))


def blueprint_update(
    ctx: typer.models.Context,
    blueprint_id: str | None = typer.Argument(
        None, help="Blueprint ID to refresh; omit only with --all."
    ),
    all_: bool = typer.Option(
        False, "--all", help="Refresh the complete blueprint catalog."
    ),
    source: str | None = typer.Option(
        None, "--source", help="Cached blueprint repo/path to update."
    ),
):
    "Refresh one blueprint or the complete catalog and its dependencies."
    return display(("blueprint", "update"), dispatch(("blueprint", "update"), locals()))


def blueprint_remove(
    ctx: typer.models.Context,
    blueprint_id: str | None = typer.Argument(
        None, help="Blueprint ID to remove; omit only with --all."
    ),
    all_: bool = typer.Option(
        False, "--all", help="Remove the complete cached blueprint catalog."
    ),
    source: str | None = typer.Option(
        None, "--source", help="Cached blueprint storage path."
    ),
    keep_resources: bool = typer.Option(
        False, "--keep-resources", help="Keep blueprint-owned cached runtime resources."
    ),
    keep_models: bool = typer.Option(
        False, "--keep-models", help="Keep orphaned runtime models."
    ),
    remove_models: bool = typer.Option(
        False, "--remove-models", help="Remove orphaned models without a second prompt."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview removal without changing state."
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Confirm removal without prompting."
    ),
):
    "Remove one blueprint or the complete cached catalog."
    return display(("blueprint", "remove"), dispatch(("blueprint", "remove"), locals()))


def blueprint_run(
    ctx: typer.models.Context,
    target: Annotated[
        str,
        typer.Argument(
            ..., help="Catalog blueprint ID or explicit local path such as ./blueprint."
        ),
    ],
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Use a specific shared blueprint run ID."),
    ] = None,
    job_id: Annotated[
        str | None,
        typer.Option(
            "--job-id",
            help="Run an existing durable job instead of creating a new job.",
        ),
    ] = None,
    owner_node: Annotated[
        str | None,
        typer.Option(
            "--node", help="Create and execute the job on this federated Core."
        ),
    ] = None,
    blueprint_repo: Annotated[
        str | None,
        typer.Option(
            "--blueprint-repo",
            help="Use this blueprint repository URL/path instead of the default catalog.",
        ),
    ] = None,
    update: Annotated[
        bool,
        typer.Option(
            "--update",
            help="Update the cached blueprint repository before running a catalog blueprint.",
        ),
    ] = False,
    offline: Annotated[
        bool,
        typer.Option(
            "--offline",
            help="Use only local blueprint files; never clone, fetch, or pull.",
        ),
    ] = False,
    revision: Annotated[
        str | None,
        typer.Option(
            "--revision", help="Checkout a specific git revision before running."
        ),
    ] = None,
    follow_seconds: Annotated[
        float | None,
        typer.Option(
            "--follow-seconds",
            help="Seconds to keep polling job events after the submit stream detaches. Defaults to MN_RUN_DETACH_LOG_SECONDS or 30.",
        ),
    ] = None,
    set_values: Annotated[
        list[str] | None,
        typer.Option(
            "--set",
            metavar="PATH=VALUE",
            help="Override one config value for this run. Repeat for multiple values.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Run even if blueprint input validation or runtime requirements fail.",
        ),
    ] = False,
    replace_existing_run: Annotated[
        bool,
        typer.Option(
            "--replace-existing-run",
            help="Permanently replace the run attached to an existing service job.",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes", "-y", help="Confirm service-run replacement without prompting."
        ),
    ] = False,
    detached: Annotated[
        bool,
        typer.Option(
            "-d",
            "--detached",
            help="Start the blueprint run without the live workflow UI.",
        ),
    ] = False,
    web_ui: Annotated[
        bool,
        typer.Option(
            "--web-ui", help="Start or register the blueprint Web UI for this run."
        ),
    ] = False,
    web_ui_host: Annotated[
        str | None,
        typer.Option(
            "--web-ui-host",
            help="Override the blueprint Web UI listener host for this run.",
        ),
    ] = None,
    web_ui_port: Annotated[
        int | None,
        typer.Option(
            "--web-ui-port",
            help="Override the blueprint Web UI listener port for this run.",
            min=1,
            max=65535,
        ),
    ] = None,
    queue: Annotated[
        bool, typer.Option("--queue", help="Queue this run when resources are busy.")
    ] = False,
    auto_schedule: Annotated[
        bool,
        typer.Option(
            "--auto-schedule",
            help="Queue the run until the cluster has the required agent resources.",
        ),
    ] = False,
    schedule: Annotated[
        str | None,
        typer.Option(
            "--schedule",
            help="Create a schedule instead of running now. Accepts JSON, a delay like 30m, or an ISO run_at timestamp.",
        ),
    ] = None,
    fake_llm: Annotated[
        bool,
        typer.Option(
            "--fake-llm",
            help="Replace blueprint LLM configuration with the deterministic fake LLM for this run.",
        ),
    ] = False,
    fake_skills: Annotated[
        bool,
        typer.Option(
            "--fake-skills",
            help="Use deterministic mock outputs for supported external blueprint skills.",
        ),
    ] = False,
    benchmark: Annotated[
        bool,
        typer.Option(
            "--benchmark",
            help="Write per-step and per-skill timing artifacts into the blueprint run folder.",
        ),
    ] = False,
    debug: Annotated[
        bool,
        typer.Option(
            "--debug",
            help="Enable verbose blueprint runtime debug events and logs for this run.",
        ),
    ] = False,
):
    "Run a catalog blueprint or an explicitly addressed local folder."
    return display(("blueprint", "run"), dispatch(("blueprint", "run"), locals()))


def blueprint_validate(
    bundle_path: Annotated[
        str, typer.Argument(..., help="Path to the local job bundle folder.")
    ],
):
    "Validate a local job bundle before submitting it.\n\nExamples:\n  mn blueprint validate ./bundle\n  mn blueprint validate ./bundle --json"
    return display(
        ("blueprint", "validate"), dispatch(("blueprint", "validate"), locals())
    )


def blueprint_doctor(
    ctx: typer.models.Context,
    target: Annotated[
        str,
        typer.Argument(
            ..., help="Catalog blueprint ID or explicit local path such as ./blueprint."
        ),
    ],
    blueprint_repo: Annotated[
        str | None,
        typer.Option(
            "--blueprint-repo",
            help="Use this blueprint repository URL/path instead of the default catalog.",
        ),
    ] = None,
    update: Annotated[
        bool,
        typer.Option(
            "--update",
            help="Update the cached blueprint repository before diagnosing a catalog blueprint.",
        ),
    ] = False,
    offline: Annotated[
        bool,
        typer.Option(
            "--offline",
            help="Use only local blueprint files; never clone, fetch, or pull.",
        ),
    ] = False,
    revision: Annotated[
        str | None,
        typer.Option(
            "--revision", help="Checkout a specific git revision before diagnosing."
        ),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print the doctor report as JSON.")
    ] = False,
    timeout: Annotated[
        float, typer.Option("--timeout", help="Per-probe timeout in seconds.", min=0.1)
    ] = 3.0,
    check_only: Annotated[
        bool,
        typer.Option(
            "--check-only",
            help="Validate declared dependencies without preparing Docker, OpenShell, or native environments.",
        ),
    ] = False,
    no_llm_call: Annotated[
        bool,
        typer.Option(
            "--no-llm-call", help="Skip tiny LLM and embedding smoke requests."
        ),
    ] = False,
    cleanup: Annotated[
        bool,
        typer.Option(
            "--cleanup",
            help="Clean up doctor-created DockerWorker resources after probing.",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Continue preparation even if runtime requirement checks fail.",
        ),
    ] = False,
    debug: Annotated[
        bool,
        typer.Option(
            "--debug",
            help="Enable verbose blueprint runtime debug environment variables.",
        ),
    ] = False,
):
    "Diagnose a catalog blueprint or local folder without submitting a job."
    return display(("blueprint", "doctor"), dispatch(("blueprint", "doctor"), locals()))


def blueprint_cleanup(
    ctx: typer.models.Context,
    blueprint_id: str | None = typer.Option(
        None, "--blueprint-id", help="Clean resources for one blueprint ID."
    ),
    source: str | None = typer.Option(
        None,
        "--source",
        help="Blueprint storage path used to decide which resources are dead.",
    ),
    python_envs_dir: str | None = typer.Option(
        None,
        "--python-envs-dir",
        help="Override the blueprint Python environment cache root.",
    ),
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the shared blueprint run store root."
    ),
    generated_bundles_dir: str | None = typer.Option(
        None,
        "--generated-bundles-dir",
        help="Override the generated Python workflow bundle cache root.",
    ),
    bundle_cache_dir: str | None = typer.Option(
        None,
        "--bundle-cache-dir",
        help="Override the MirrorNeuron local bundle cache root.",
    ),
    include_files: bool = typer.Option(
        True,
        "--files/--no-files",
        help="Also remove blueprint-owned run records, generated bundles, and local bundle cache entries.",
    ),
    include_docker: bool = typer.Option(
        True,
        "--docker/--no-docker",
        help="Also remove Docker resources labelled for removed blueprints.",
    ),
    include_dead: bool = typer.Option(
        True,
        "--dead/--no-dead",
        help="Remove stale incomplete resources and resources for blueprints no longer in storage.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be removed without deleting anything."
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Confirm cleanup without prompting."
    ),
):
    "Clean blueprint-owned Python envs, ~/.mn files, Docker resources, and stale leftovers."
    return display(
        ("blueprint", "cleanup"), dispatch(("blueprint", "cleanup"), locals())
    )


def blueprint_export(
    run_id: str,
    output_format: str = typer.Option(
        "json", "--format", "-f", help="Export format: json, markdown, or html."
    ),
    output: str | None = typer.Option(
        None, "--output", "-o", help="Write the export to this path instead of stdout."
    ),
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Export one blueprint run as JSON, Markdown, or static HTML."
    return display(("blueprint", "export"), dispatch(("blueprint", "export"), locals()))


def blueprint_options(
    ctx: typer.Context,
    blueprint_repo: Optional[str] = typer.Option(
        None,
        "--blueprint-repo",
        help="Catalog source for list or an attached local runtime.",
    ),
):
    ctx.ensure_object(dict)
    ctx.obj["blueprint_repo"] = blueprint_repo


def register(app: typer.Typer) -> None:
    blueprint_app = typer.Typer(
        help="List, add, inspect, run, and diagnose MirrorNeuron blueprints.",
        cls=RemediatingTyperGroup,
    )
    blueprint_app.callback()(blueprint_options)
    blueprint_app.command("list", hidden=False)(blueprint_list)
    blueprint_app.command("add", hidden=False)(blueprint_add)
    blueprint_app.command("show", hidden=False)(blueprint_show)
    blueprint_app.command("update", hidden=False)(blueprint_update)
    blueprint_app.command("remove", hidden=False)(blueprint_remove)
    blueprint_app.command("run", hidden=False)(blueprint_run)
    blueprint_app.command("validate", hidden=False)(blueprint_validate)
    blueprint_app.command("doctor", hidden=False)(blueprint_doctor)
    blueprint_app.command("cleanup", hidden=False)(blueprint_cleanup)
    blueprint_app.command("export", hidden=False)(blueprint_export)
    app.add_typer(blueprint_app, name="blueprint")
