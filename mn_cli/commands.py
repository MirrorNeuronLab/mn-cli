"""Public command declarations; every operation delegates to the SDK runtime client."""

from pathlib import Path
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


def job_list(include_archived: bool = typer.Option(False, "--include-archived")):
    "List durable job definitions."
    return display(("job", "list"), dispatch(("job", "list"), locals()))


def job_create(
    bundle: str = typer.Argument(
        ..., help="Blueprint/job bundle directory or archive."
    ),
    job_id: str | None = typer.Option(
        None, "--job-id", help="Optional durable job ID."
    ),
    config: str | None = typer.Option(
        None, "--config", help="Resolved configuration JSON file."
    ),
    node: str | None = typer.Option(
        None, "--node", help="Core that will own and execute this job."
    ),
):
    "Create a durable job definition without starting a run."
    return display(("job", "create"), dispatch(("job", "create"), locals()))


def job_backup(
    job_id: str = typer.Argument(
        ..., help="Durable job ID. Pause active runs before backup."
    ),
    output: str = typer.Option(..., "--output", "-o", help="Destination ZIP file."),
    air_gapped: bool = typer.Option(
        True,
        "--air-gapped/--no-air-gapped",
        help="Include model files, Python wheels, and Docker images.",
    ),
):
    "Back up a job, its data, history, and offline dependencies to a ZIP."
    return display(("job", "backup"), dispatch(("job", "backup"), locals()))


def job_restore(
    input_path: str = typer.Option(..., "--input", "-i", help="Job backup ZIP file."),
    job_id: str | None = typer.Option(
        None, "--job-id", help="Optional new durable job ID."
    ),
    start: bool = typer.Option(
        False, "--start", help="Start a fresh run after restoring the job."
    ),
):
    "Restore a ZIP as a new job without hiring a blueprint."
    return display(("job", "restore"), dispatch(("job", "restore"), locals()))


def job_show(job_id: str = typer.Argument(..., help="Durable job ID.")):
    "Inspect a durable job definition."
    return display(("job", "show"), dispatch(("job", "show"), locals()))


def job_relocate(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    node: str = typer.Option(..., "--node", help="Connected destination PC."),
    abort: bool = typer.Option(
        False, "--abort", help="Cancel a move before ownership has committed."
    ),
):
    "Move an idle job and its data to another PC. Pause its schedules first."
    return display(("job", "relocate"), dispatch(("job", "relocate"), locals()))


def job_analysis(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    json_output: bool = typer.Option(
        False, "--json", help="Print the shared analysis as JSON."
    ),
):
    "Show performance statistics for all recorded job history."
    return display(("job", "analysis"), dispatch(("job", "analysis"), locals()))


def job_start(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    run_id: str | None = typer.Option(None, "--run-id"),
    queue: bool = typer.Option(
        False, "--queue", help="Queue this run when resources are busy."
    ),
    inputs: str | None = typer.Option(None, "--inputs", help="Run-input JSON file."),
    force: bool = typer.Option(
        False, "--force", help="Permanently replace the existing run of a service job."
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Confirm replacement without prompting."
    ),
):
    "Start a new run of a durable job."
    return display(("job", "start"), dispatch(("job", "start"), locals()))


def job_archive(job_id: str = typer.Argument(..., help="Durable job ID.")):
    "Archive a job while retaining its persistent data."
    return display(("job", "archive"), dispatch(("job", "archive"), locals()))


def job_reset_data(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview the reset without changing data."
    ),
):
    "Clear job data and advance its data generation."
    return display(("job", "reset-data"), dispatch(("job", "reset-data"), locals()))


def job_delete(
    job_id: str = typer.Argument(..., help="Durable job ID."),
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview deletion without changing state."
    ),
):
    "Permanently delete a durable job, its runs, resources, and data."
    return display(("job", "delete"), dispatch(("job", "delete"), locals()))


def schedule_list(
    node: str = typer.Option("", "--node", help="Selected PC's owner node."),
):
    ""
    return display(("schedule", "list"), dispatch(("schedule", "list"), locals()))


def schedule_show(schedule_id: str, node: str = typer.Option("", "--node")):
    ""
    return display(("schedule", "show"), dispatch(("schedule", "show"), locals()))


def schedule_set_time(
    schedule_id: str,
    at: str = typer.Option(
        ..., "--at", help="Future ISO 8601 date and time with timezone."
    ),
    node: str = typer.Option("", "--node"),
):
    ""
    return display(
        ("schedule", "set-time"), dispatch(("schedule", "set-time"), locals())
    )


def schedule_cancel(schedule_id: str, node: str = typer.Option("", "--node")):
    ""
    return display(("schedule", "cancel"), dispatch(("schedule", "cancel"), locals()))


def run_list(
    job: str | None = typer.Option(
        None, "--job", help="Only show runs belonging to this durable job."
    ),
    blueprint: str | None = typer.Option(
        None, "--blueprint", help="Only show runs for this blueprint ID."
    ),
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Refresh until interrupted."
    ),
    limit: int = typer.Option(20, "--limit", help="Maximum runs to show.", min=1),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
    interval: float = typer.Option(
        2.0, "--interval", help="Refresh interval when following.", min=0.1
    ),
):
    "List execution runs, optionally filtered by job or blueprint."
    return display(("run", "list"), dispatch(("run", "list"), locals()))


def run_show(run_id: str = typer.Argument(..., help="Execution run ID.")):
    "Show one execution run."
    return display(("run", "show"), dispatch(("run", "show"), locals()))


def run_info(run_id: str = typer.Argument(..., help="Execution run ID.")):
    "Show one execution run."
    return display(("run", "info"), dispatch(("run", "info"), locals()))


def run_watch(run_id: str = typer.Argument(..., help="Execution run ID.")):
    "Watch live workflow progress for one execution run."
    return display(("run", "watch"), dispatch(("run", "watch"), locals()))


def run_logs(
    run_id: str = typer.Argument(..., help="Execution run ID."),
    channel: str = typer.Option(
        "logs", "--channel", help="Stream channel: logs, events, or all."
    ),
    lines: int = typer.Option(
        50, "--lines", "-n", help="Number of records to show.", min=1
    ),
    level: str | None = typer.Option(None, "--level", help="Minimum log level."),
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Continue printing records."
    ),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
    interval: float = typer.Option(
        1.0, "--interval", help="Polling interval when following.", min=0.1
    ),
):
    "Show stored run logs, events, or the merged operational stream."
    return display(("run", "logs"), dispatch(("run", "logs"), locals()))


def run_result(
    run_id: str = typer.Argument(..., help="Execution run ID."),
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Directory for fetched result files."),
    ] = None,
):
    "Fetch final and progressive results for one execution run."
    return display(("run", "result"), dispatch(("run", "result"), locals()))


def run_resources(
    run_id: str,
    window: str = typer.Option(
        "24h", "--window", help="History window, for example 24h."
    ),
    bucket: str = typer.Option(
        "1h", "--bucket", help="Aggregation bucket, for example 1h."
    ),
    live: bool = typer.Option(
        False, "--live", help="Refresh resource usage until interrupted."
    ),
    interval: float = typer.Option(
        5.0, "--interval", help="Live refresh interval in seconds."
    ),
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Show CPU, GPU, memory, and LLM token usage for one blueprint run."
    return display(("run", "resources"), dispatch(("run", "resources"), locals()))


def run_compare(
    run_a: str,
    run_b: str,
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Compare two blueprint runs from the shared run store."
    return display(("run", "compare"), dispatch(("run", "compare"), locals()))


def run_pause(run_id: str):
    "Pause one execution run."
    return display(("run", "pause"), dispatch(("run", "pause"), locals()))


def run_resume(run_id: str):
    "Resume one execution run."
    return display(("run", "resume"), dispatch(("run", "resume"), locals()))


def run_retry(
    run_id: str = typer.Argument(..., help="Failed execution run ID."),
    queue: bool = typer.Option(
        False, "--queue", help="Queue this retry when resources are busy."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Verify recovery without starting work."
    ),
    settings: list[str] = typer.Option(
        [], "--set", help="Explicit declared retry setting: path=value. Repeatable."
    ),
    idempotency_key: str | None = typer.Option(
        None, "--idempotency-key", help="Reuse this key after a lost response."
    ),
    expected_attempt: int | None = typer.Option(
        None,
        "--expected-attempt",
        help="Original attempt selection when resubmitting a lost response.",
        min=1,
    ),
    checkpoint_revision: str | None = typer.Option(
        None,
        "--checkpoint-revision",
        help="Original checkpoint selection when resubmitting a lost response.",
    ),
):
    "Retry unfinished work from the failed run's last verified checkpoint."
    return display(("run", "retry"), dispatch(("run", "retry"), locals()))


def run_cancel(
    run_id: str,
    queued_only: bool = typer.Option(
        False, "--queued-only", help="Cancel only if execution has not started."
    ),
):
    "Cancel one execution run without deleting job data."
    return display(("run", "cancel"), dispatch(("run", "cancel"), locals()))


def run_delete(
    run_id: str,
    yes: bool = typer.Option(False, "--yes", "-y"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview deletion without changing state."
    ),
):
    "Delete one run, cancelling it first when active, without deleting job data."
    return display(("run", "delete"), dispatch(("run", "delete"), locals()))


def run_human_list(
    run_id: str = typer.Argument(..., help="Execution run ID."),
    pending: bool = typer.Option(
        False, "--pending", help="Show only pending input requests."
    ),
    runs_root: str | None = typer.Option(None, "--runs-root", hidden=True),
):
    "List human collaboration events for one run."
    return display(
        ("run", "human", "list"), dispatch(("run", "human", "list"), locals())
    )


def run_human_respond(
    run_id: str,
    request_id: str,
    decision: str = typer.Option(
        ..., "--decision", help="Decision value, such as approve, revise, or reject."
    ),
    notes: str = typer.Option("", "--notes", help="Optional reviewer notes."),
    reviewer: str = typer.Option("cli", "--reviewer", help="Reviewer identity label."),
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Record a response to a human input request."
    return display(
        ("run", "human", "respond"), dispatch(("run", "human", "respond"), locals())
    )


def run_human_ack(
    run_id: str,
    notice_id: str,
    reviewer: str = typer.Option("cli", "--reviewer", help="Reviewer identity label."),
    runs_root: str | None = typer.Option(
        None, "--runs-root", help="Override the default $MN_HOME/runs directory."
    ),
):
    "Acknowledge a human notice."
    return display(("run", "human", "ack"), dispatch(("run", "human", "ack"), locals()))


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


def runtime_start(
    host: Optional[str] = typer.Option(
        None, "--host", help="Federation-reachable host or IP advertised by this node."
    ),
    grpc_port: int = typer.Option(55051, "--grpc-port", help="Core gRPC port."),
):
    "Start a federation-capable MirrorNeuron runtime"
    return display(("runtime", "start"), dispatch(("runtime", "start"), locals()))


def runtime_stop():
    "Stop MirrorNeuron services"
    return display(("runtime", "stop"), dispatch(("runtime", "stop"), locals()))


def runtime_status(
    json_output: bool = typer.Option(
        False, "--json", help="Print machine-readable JSON."
    ),
    timeout: float = typer.Option(
        3.0, "--timeout", help="Per-component timeout in seconds.", min=0.1
    ),
):
    "Report runtime endpoints, health, nodes, jobs, and shared storage"
    return display(("runtime", "status"), dispatch(("runtime", "status"), locals()))


def runtime_reconnect():
    ""
    return display(
        ("runtime", "reconnect"), dispatch(("runtime", "reconnect"), locals())
    )


def runtime_doctor(
    json_output: bool = typer.Option(
        False, "--json", help="Print machine-readable JSON."
    ),
    timeout: float = typer.Option(
        3.0, "--timeout", help="Per-component timeout in seconds.", min=0.1
    ),
    repair: bool = typer.Option(
        False,
        "--repair",
        help="Repair unhealthy replaceable runtime sidecars, then recheck.",
    ),
):
    "Check runtime foundation services before running blueprints"
    return display(("runtime", "doctor"), dispatch(("runtime", "doctor"), locals()))


def runtime_cleanup(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report confirmed cleanup candidates without removing them.",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Apply cleanup without prompting."
    ),
    include_cache: bool = typer.Option(
        False,
        "--include-cache",
        help="Also apply the managed DockerWorker image cache policy.",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print machine-readable JSON."
    ),
):
    "Safely reconcile node-local Docker and OpenShell resources."
    return display(("runtime", "cleanup"), dispatch(("runtime", "cleanup"), locals()))


def runtime_restart_sidecars(
    api: bool = typer.Option(False, "--api", help="Restart the REST API sidecar."),
    web_ui: bool = typer.Option(False, "--web-ui", help="Restart the Web UI sidecar."),
):
    "Restart only the REST API and/or Web UI sidecars"
    return display(
        ("runtime", "restart-sidecars"),
        dispatch(("runtime", "restart-sidecars"), locals()),
    )


def runtime_ensure_context_engine(
    force: bool = typer.Option(
        False, "--force", help="Recreate the context engine even if it is running."
    ),
):
    "Prepare and start the packaged Membrane context engine service"
    return display(
        ("runtime", "ensure-context-engine"),
        dispatch(("runtime", "ensure-context-engine"), locals()),
    )


def runtime_upgrade(
    yes: bool = typer.Option(False, "--yes", "-y", help="Upgrade without prompting."),
    check_only: bool = typer.Option(
        False, "--check-only", help="Only check for upgrades; do not install them."
    ),
):
    "Install available release-pinned runtime upgrades."
    return display(("runtime", "upgrade"), dispatch(("runtime", "upgrade"), locals()))


def node_list():
    "List connected nodes with their reachable endpoint and job role."
    return display(("node", "list"), dispatch(("node", "list"), locals()))


def node_show(node_name: str = typer.Argument(..., help="Runtime node name.")):
    "Show one runtime node from the current system summary."
    return display(("node", "show"), dispatch(("node", "show"), locals()))


def node_add(
    host: str,
    token: Optional[str] = typer.Option(
        None, "--token", help="Runtime node control token; required for remote hosts."
    ),
    grpc_port: Optional[int] = typer.Option(None, "--grpc-port", min=1, max=65535),
    runtime_home: Optional[str] = typer.Option(
        None, "--runtime-home", help="Installation path for mn node add local."
    ),
):
    """Connect to a runtime; subsequent additions join peers to its cluster."""
    return display(("node", "add"), dispatch(("node", "add"), locals()))


def node_remove(
    node_name: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Confirm peer removal."),
):
    "Remove one reciprocal federated peer from this runtime."
    return display(("node", "remove"), dispatch(("node", "remove"), locals()))


def node_reconcile(
    node_name: str,
    reason: str = typer.Option(
        "", "--reason", help="Reason recorded on reconciliation events."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Plan reconciliation without moving work."
    ),
):
    "Reconcile jobs affected by an unavailable node"
    return display(("node", "reconcile"), dispatch(("node", "reconcile"), locals()))


def node_drain(
    node_name: str,
    reason: str = typer.Option("", "--reason", help="Reason recorded on drain events."),
    deadline: str = typer.Option(
        "30m", "--deadline", help="Drain deadline, e.g. 30m, 10s, 1h."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Plan the drain without changing node state."
    ),
    wait: bool = typer.Option(
        False, "--wait", help="Poll drain status until it completes or blocks."
    ),
    ignore_system_jobs: bool = typer.Option(
        True,
        "--ignore-system-jobs/--include-system-jobs",
        help="Ignore system/sysbatch jobs while draining.",
    ),
):
    "Drain a node and move safe workloads elsewhere"
    return display(("node", "drain"), dispatch(("node", "drain"), locals()))


def node_undrain(
    node_name: str,
    reason: str = typer.Option(
        "", "--reason", help="Reason recorded on undrain events."
    ),
    mark_eligible: bool = typer.Option(
        False,
        "--mark-eligible",
        help="Make the node schedulable after cancelling/completing drain.",
    ),
):
    "Cancel node drain and optionally make the node schedulable"
    return display(("node", "undrain"), dispatch(("node", "undrain"), locals()))


def node_maintenance(
    node_name: str,
    enable: bool = typer.Option(
        True, "--enable/--disable", help="Enable or disable maintenance mode."
    ),
    reason: str = typer.Option(
        "", "--reason", help="Reason recorded on maintenance events."
    ),
):
    "Toggle node maintenance mode without moving existing work"
    return display(("node", "maintenance"), dispatch(("node", "maintenance"), locals()))


def node_refresh_token():
    "Rotate the persistent MirrorNeuron network join token"
    return display(
        ("node", "refresh-token"), dispatch(("node", "refresh-token"), locals())
    )


def operation_show(
    operation_id: str = typer.Argument(..., help="Durable operation ID."),
):
    "Show a durable group operation and its item states."
    return display(("operation", "show"), dispatch(("operation", "show"), locals()))


def operation_watch(
    operation_id: str = typer.Argument(..., help="Durable operation ID."),
):
    "Reattach to a durable group operation's progress stream."
    return display(("operation", "watch"), dispatch(("operation", "watch"), locals()))


def resource_show():
    "Show CPU, GPU, memory, and disk resources reported by the core"
    return display(("resource", "show"), dispatch(("resource", "show"), locals()))


def resource_usage():
    "Show runtime resource usage derived from the core system summary."
    return display(("resource", "usage"), dispatch(("resource", "usage"), locals()))


def resource_set(
    cpu: Optional[int] = typer.Option(
        None, "--cpu", help="Maximum CPU use percentage: 25, 50, 75, or 100"
    ),
    gpu: Optional[int] = typer.Option(
        None, "--gpu", help="Maximum GPU use percentage: 25, 50, 75, or 100"
    ),
    memory: Optional[int] = typer.Option(
        None, "--memory", help="Maximum memory use percentage: 25, 50, 75, or 100"
    ),
    disk: Optional[int] = typer.Option(
        None, "--disk", help="Maximum disk use percentage: 25, 50, 75, or 100"
    ),
):
    "Set core resource use percentages"
    return display(("resource", "set"), dispatch(("resource", "set"), locals()))


def service_list(
    name: Annotated[
        Optional[str], typer.Option("--name", help="Filter by service name.")
    ] = None,
    node: Annotated[
        Optional[str], typer.Option("--node", help="Filter by node name.")
    ] = None,
    job_id: Annotated[
        Optional[str], typer.Option("--job-id", help="Filter by job ID.")
    ] = None,
    status: Annotated[
        Optional[str], typer.Option("--status", help="Filter by health status.")
    ] = None,
    all_statuses: Annotated[
        bool, typer.Option("--all", help="Show warning and critical services too.")
    ] = False,
):
    "List registered services"
    return display(("service", "list"), dispatch(("service", "list"), locals()))


def service_show(
    name: str,
    tag: Annotated[
        Optional[list[str]], typer.Option("--tag", help="Require a service tag.")
    ] = None,
    node: Annotated[
        Optional[str], typer.Option("--node", help="Filter by node name.")
    ] = None,
    all_statuses: Annotated[
        bool, typer.Option("--all", help="Return warning and critical services too.")
    ] = False,
):
    "Resolve healthy instances for one service"
    return display(("service", "show"), dispatch(("service", "show"), locals()))


def blueprint_options(
    ctx: typer.Context,
    blueprint_repo: Optional[str] = typer.Option(
        None, "--blueprint-repo", help="Catalog source for list or an attached local runtime."
    ),
):
    ctx.ensure_object(dict)
    ctx.obj["blueprint_repo"] = blueprint_repo


def register_commands(app):
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
    job_app = typer.Typer(
        help="Create and manage durable job definitions.\n\nExamples:\n  mn job create ./worker-bundle --job-id worker-daily\n  mn job list\n  mn job show worker-daily\n  mn job start worker-daily\n  mn run list --job worker-daily\n",
        cls=RemediatingTyperGroup,
    )
    job_app.command("list", hidden=False)(job_list)
    job_app.command("create", hidden=False)(job_create)
    job_app.command("backup", hidden=False)(job_backup)
    job_app.command("restore", hidden=False)(job_restore)
    job_app.command("show", hidden=False)(job_show)
    job_app.command("relocate", hidden=False)(job_relocate)
    job_app.command("analysis", hidden=False)(job_analysis)
    job_app.command("start", hidden=False)(job_start)
    job_app.command("archive", hidden=False)(job_archive)
    job_app.command("reset-data", hidden=False)(job_reset_data)
    job_app.command("delete", hidden=False)(job_delete)
    app.add_typer(job_app, name="job")
    schedule_app = typer.Typer(
        help="Inspect schedules and automatically queued starts.",
        cls=RemediatingTyperGroup,
    )
    schedule_app.command("list", hidden=False)(schedule_list)
    schedule_app.command("show", hidden=False)(schedule_show)
    schedule_app.command("set-time", hidden=False)(schedule_set_time)
    schedule_app.command("cancel", hidden=False)(schedule_cancel)
    app.add_typer(schedule_app, name="schedule")
    run_app = typer.Typer(
        help="Inspect and control individual execution runs.", cls=RemediatingTyperGroup
    )
    run_app.command("list", hidden=False)(run_list)
    run_app.command("show", hidden=False)(run_show)
    run_app.command("info", hidden=False)(run_info)
    run_app.command("watch", hidden=False)(run_watch)
    run_app.command("logs", hidden=False)(run_logs)
    run_app.command("result", hidden=False)(run_result)
    run_app.command("resources", hidden=False)(run_resources)
    run_app.command("compare", hidden=False)(run_compare)
    run_app.command("pause", hidden=False)(run_pause)
    run_app.command("resume", hidden=False)(run_resume)
    run_app.command("retry", hidden=False)(run_retry)
    run_app.command("cancel", hidden=False)(run_cancel)
    run_app.command("delete", hidden=False)(run_delete)
    run_human_app = typer.Typer(
        help="Inspect and respond to human collaboration events.",
        cls=RemediatingTyperGroup,
    )
    run_human_app.command("list", hidden=False)(run_human_list)
    run_human_app.command("respond", hidden=False)(run_human_respond)
    run_human_app.command("ack", hidden=False)(run_human_ack)
    run_app.add_typer(run_human_app, name="human")
    app.add_typer(run_app, name="run")
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
    runtime_app = typer.Typer(
        help="Start, stop, upgrade, and diagnose the local MirrorNeuron runtime.\n\nExamples:\n  mn runtime start\n  mn runtime upgrade\n  mn runtime status\n  mn runtime doctor\n  mn runtime doctor --json\n  mn runtime ensure-context-engine\n  mn runtime restart-sidecars --api\n  mn runtime restart-sidecars --web-ui\n  mn runtime stop\n",
        cls=RemediatingTyperGroup,
    )
    runtime_app.command("start", hidden=False)(runtime_start)
    runtime_app.command("stop", hidden=False)(runtime_stop)
    runtime_app.command("status", hidden=False)(runtime_status)
    runtime_app.command("reconnect", hidden=False)(runtime_reconnect)
    runtime_app.command("doctor", hidden=False)(runtime_doctor)
    runtime_app.command("cleanup", hidden=False)(runtime_cleanup)
    runtime_app.command("restart-sidecars", hidden=False)(runtime_restart_sidecars)
    runtime_app.command("ensure-context-engine", hidden=False)(
        runtime_ensure_context_engine
    )
    runtime_app.command("upgrade", hidden=False)(runtime_upgrade)
    app.add_typer(runtime_app, name="runtime")
    node_app = typer.Typer(
        help="Inspect cluster nodes and manage node membership or maintenance.\n\nExamples:\n  mn node list\n  mn node drain <node-name> --reason maintenance --wait\n  mn node add <host> --token <token>\n  mn node remove <node-name> --yes\n",
        cls=RemediatingTyperGroup,
    )
    node_app.command("list", hidden=False)(node_list)
    node_app.command("show", hidden=False)(node_show)
    node_app.command("add", hidden=False)(node_add)
    node_app.command("remove", hidden=False)(node_remove)
    node_app.command("reconcile", hidden=False)(node_reconcile)
    node_app.command("drain", hidden=False)(node_drain)
    node_app.command("undrain", hidden=False)(node_undrain)
    node_app.command("maintenance", hidden=False)(node_maintenance)
    node_app.command("refresh-token", hidden=False)(node_refresh_token)
    app.add_typer(node_app, name="node")
    operation_app = typer.Typer(
        help="Inspect or reattach to durable group operations.",
        cls=RemediatingTyperGroup,
    )
    operation_app.command("show", hidden=False)(operation_show)
    operation_app.command("watch", hidden=False)(operation_watch)
    app.add_typer(operation_app, name="operation")
    resource_app = typer.Typer(
        help="Inspect runtime capacity, usage, and limits.", cls=RemediatingTyperGroup
    )
    resource_app.command("show", hidden=False)(resource_show)
    resource_app.command("usage", hidden=False)(resource_usage)
    resource_app.command("set", hidden=False)(resource_set)
    app.add_typer(resource_app, name="resource")
    service_app = typer.Typer(
        help="Inspect MirrorNeuron service discovery.", cls=RemediatingTyperGroup
    )
    service_app.command("list", hidden=False)(service_list)
    service_app.command("show", hidden=False)(service_show)
    app.add_typer(service_app, name="service")
