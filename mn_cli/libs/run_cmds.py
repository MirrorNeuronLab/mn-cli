import typer
import hashlib
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Annotated, Any, Optional
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from mn_cli.libs.ui import (
    generate_detached_panel,
    generate_live_layout,
    generate_run_submitted_panel,
    generate_summary_panel,
)
from mn_cli.libs.run_logs import (
    JobLogWriter,
    STANDARD_EVENTS,
    extract_web_ui_url as _extract_web_ui_url,
    materialize_sent_email_copy as _materialize_sent_email_copy,
    write_result_stream_event as _write_result_stream_event,
)
from mn_cli.libs.run_manifest import (
    add_mn_llm_aliases as _add_mn_llm_aliases,
    blueprint_runtime_environment as _blueprint_runtime_environment,
    inject_node_environment as _inject_node_environment,
    load_blueprint_config,
    prepare_manifest_for_submission,
    runtime_web_ui_support_payloads_for_manifest,
    run_mode_label as _run_mode_label,
    stage_local_input_payloads_for_manifest,
    with_shared_run_store_config as _with_shared_run_store_config,
)
from mn_cli.libs.blueprint_observability import (
    make_blueprint_run_id as _make_blueprint_run_id,
)
from mn_cli.libs.blueprint_resources import cleanup_blueprint_host_hooks
from mn_cli.shared import console, client, logger
from mn_cli.error_handler import handle_cli_error
from mn_sdk import (
    make_validation_report,
    run_input_validation,
    run_service_validation,
    validate_input_validation_spec_issues,
    validate_requirements_spec_issues,
    validate_resource_spec_issues,
    validate_service_spec_issues,
)

FINAL_STATUSES = {"completed", "failed", "cancelled"}
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
PRE_LAUNCH_SCRIPT = Path("scripts/pre-launch.sh")
POST_LAUNCH_SCRIPT = Path("scripts/post-launch.sh")
DEFAULT_BLUEPRINT_WEB_UI_PORT_START = 61000
DEFAULT_BLUEPRINT_WEB_UI_PORT_END = 61049
_HELPER_COMPAT = (
    _add_mn_llm_aliases,
    _blueprint_runtime_environment,
    _extract_web_ui_url,
    _inject_node_environment,
    _materialize_sent_email_copy,
)


def fetch_and_save_results(job_id: str, data: dict = None):
    log_dir = Path(f"/tmp/mn_{job_id}")
    log_dir.mkdir(parents=True, exist_ok=True)

    if data is None:
        try:
            job_json = client.get_job(job_id)
            data = json.loads(job_json)
        except Exception:
            logger.exception("Failed to fetch job result for %s", job_id)
            return

    job = data.get("job", {})
    status = job.get("status")

    # Save final result if completed
    if status == "completed":
        result = job.get("result")
        if result:
            with open(log_dir / "result.txt", "w") as f:
                json.dump(result, f, indent=2)

    # Save stream results (progressive)
    stream_events = []

    try:
        full_events = []
        for ev_str in client.stream_events(job_id):
            try:
                full_events.append(json.loads(ev_str))
            except Exception:
                logger.exception(
                    "Failed to decode event while saving results for %s", job_id
                )
                pass

        for ev in full_events:
            ev_type = ev.get("type")
            if ev_type not in STANDARD_EVENTS:
                stream_events.append(ev.get("payload", ev))
    except Exception:
        logger.exception("Failed to stream events while saving results for %s", job_id)
        pass

    if stream_events:
        with open(log_dir / "result_stream.txt", "w") as f:
            for se in stream_events:
                f.write(json.dumps(se) + "\n")


def _stream_and_format_events(
    job_id: str,
    log_writer: Optional[JobLogWriter] = None,
    follow_seconds: Optional[float] = None,
    web_ui_url: Optional[str] = None,
) -> str:
    log_writer = log_writer or JobLogWriter(job_id)
    if web_ui_url:
        log_writer.remember_web_ui_url(web_ui_url)
    log_dir = log_writer.log_dir
    follow_seconds = (
        float(os.getenv("MN_RUN_DETACH_LOG_SECONDS", "30"))
        if follow_seconds is None
        else follow_seconds
    )

    status_text = "Unknown / Detached"
    msg_count = 0

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            job_task = progress.add_task("[cyan]Submitting job bundle...", total=None)

            for event_json in client.stream_events(job_id):
                log_writer.write_event_json(event_json)
                try:
                    event = json.loads(event_json)
                    event_type = event.get("type")

                    _write_result_stream_event(log_dir, event)
                    web_ui_url = log_writer.record_web_ui_url(event)
                    if web_ui_url:
                        progress.console.print(
                            f"[green]Blueprint Web UI:[/green] {web_ui_url}"
                        )

                    if event_type == "job_pending":
                        progress.update(
                            job_task,
                            description="[cyan]Preparing: job accepted, waiting for validation...",
                        )
                    elif event_type == "job_validated":
                        progress.update(
                            job_task,
                            description="[cyan]Preparing: manifest validated, scheduling agents...",
                        )
                    elif event_type == "job_scheduled":
                        progress.update(
                            job_task,
                            description="[cyan]Starting: agents scheduled, waiting for runtime to report running...",
                        )
                    elif event_type == "job_running":
                        progress.update(
                            job_task,
                            description="[green]Running: streaming live job events...",
                        )
                    elif event_type in [
                        "agent_message_received",
                        "aggregator_received",
                    ]:
                        msg_count += 1
                        progress.update(
                            job_task,
                            description=f"[green]Running: {msg_count} routed messages, {log_writer.event_count} events logged...",
                        )
                    elif event_type == "job_completed":
                        result = event.get("result")
                        if result is not None:
                            with open(log_dir / "result.txt", "w") as f_res:
                                json.dump(result, f_res, indent=2)

                        progress.update(
                            job_task,
                            description="[green]Completed successfully.",
                        )
                        status_text = "Success"
                        break
                    elif event_type == "job_failed":
                        progress.update(job_task, description="[red]Job failed.")
                        status_text = "Failed"
                        break
                    else:
                        progress.update(
                            job_task,
                            description=f"[cyan]Observing: latest event {event_type}, {log_writer.event_count} events logged...",
                        )
                except Exception:
                    log_writer.run_logger.exception("Failed to process streamed event")

        if status_text in ["Success", "Failed"]:
            panel = generate_summary_panel(
                job_id=job_id,
                status="completed" if status_text == "Success" else "failed",
                log_dir=log_dir,
            )
            console.print(panel)
        else:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                follow_task = progress.add_task(
                    f"[cyan]Following job for {follow_seconds:g}s before detach...",
                    total=None,
                )
                status, _data = _follow_job_events(
                    job_id,
                    log_writer,
                    follow_seconds,
                    progress=progress,
                    task_id=follow_task,
                )
            console.print(
                generate_detached_panel(
                    job_id,
                    log_dir,
                    status,
                    log_writer.event_count,
                    web_ui_url=log_writer.web_ui_url,
                )
            )
            status_text = status

    except KeyboardInterrupt:
        console.print("[yellow]Detached from log stream.[/yellow]")
        status, _data = _follow_job_events(job_id, log_writer, 0)
        console.print(
            generate_detached_panel(
                job_id,
                log_dir,
                status,
                log_writer.event_count,
                web_ui_url=log_writer.web_ui_url,
            )
        )
        status_text = status

    if status_text == "Success":
        return "completed"
    if status_text == "Failed":
        return "failed"
    return str(status_text).lower()


def _follow_job_events(
    job_id: str,
    log_writer: JobLogWriter,
    follow_seconds: float,
    progress: Optional[Progress] = None,
    task_id=None,
):
    deadline = time.monotonic() + max(follow_seconds, 0)
    last_status = "unknown"
    data = None

    while True:
        try:
            data = json.loads(client.get_job(job_id))
            log_writer.write_snapshot(data)
        except Exception:
            log_writer.run_logger.exception("Failed to poll job status")
            break

        job = data.get("job", {})
        summary = data.get("summary", {})
        last_status = summary.get("status") or job.get("status") or last_status

        recent_events = data.get("recent_events", [])
        for event in reversed(recent_events):
            if log_writer.write_event(event):
                _write_result_stream_event(log_writer.log_dir, event)
                web_ui_url = log_writer.record_web_ui_url(event)
                if web_ui_url and progress is not None:
                    progress.console.print(
                        f"[green]Blueprint Web UI:[/green] {web_ui_url}"
                    )

        if progress is not None and task_id is not None:
            remaining = max(deadline - time.monotonic(), 0)
            progress.update(
                task_id,
                description=(
                    f"[cyan]Following: status {last_status}, "
                    f"{log_writer.event_count} events logged, detach in {remaining:0.1f}s..."
                ),
            )

        if last_status in FINAL_STATUSES:
            result = job.get("result")
            if result is not None:
                with open(log_writer.log_dir / "result.txt", "w") as f_res:
                    json.dump(result, f_res, indent=2, sort_keys=True)
            break

        if time.monotonic() >= deadline:
            break

        time.sleep(float(os.getenv("MN_RUN_LOG_POLL_INTERVAL_SECONDS", "0.5")))

    return last_status, data


def validate(
    bundle_path: str,
    output: Annotated[
        str,
        typer.Option("--output", "-o", help="Output format: table or json."),
    ] = "table",
):
    """Check if a job bundle in a local folder is valid to run"""
    try:
        output_format = _normalize_validation_output(output)
        bundle_dir = Path(bundle_path)
        if not bundle_dir.is_dir():
            console.print(
                f"[red]Error: '{bundle_path}' is not a directory. Expected a bundle folder.[/red]"
            )
            raise typer.Exit(1)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(
                f"[red]Error: manifest.json not found in '{bundle_path}'[/red]"
            )
            raise typer.Exit(1)

        with open(manifest_file, "r") as f:
            try:
                manifest = json.load(f)
            except json.JSONDecodeError as e:
                console.print(f"[red]Error: manifest.json is not valid JSON. {e}[/red]")
                raise typer.Exit(1)

        required_keys = [
            "manifest_version",
            "graph_id",
            "job_name",
            "entrypoints",
            "nodes",
        ]
        missing = [k for k in required_keys if k not in manifest]
        if missing:
            console.print(
                f"[red]Error: manifest.json is missing required keys: {', '.join(missing)}[/red]"
            )
            raise typer.Exit(1)

        if not isinstance(manifest.get("nodes"), type([])):
            console.print("[red]Error: 'nodes' must be a list in manifest.json[/red]")
            raise typer.Exit(1)

        if "requiredContextEngine" in manifest and not isinstance(
            manifest.get("requiredContextEngine"), bool
        ):
            console.print(
                "[red]Error: 'requiredContextEngine' must be true or false in manifest.json[/red]"
            )
            raise typer.Exit(1)

        python_environment_errors = validate_python_environments(bundle_dir, manifest)
        if python_environment_errors:
            report = make_validation_report(
                [
                    _legacy_validation_issue(error, source="manifest")
                    for error in python_environment_errors
                ]
            )
            _emit_validation_report(
                report, output_format, title="Manifest validation failed"
            )
            raise typer.Exit(1)

        manifest_spec_issues = (
            validate_service_spec_issues(manifest)
            + validate_requirements_spec_issues(manifest)
            + validate_resource_spec_issues(manifest)
            + validate_input_validation_spec_issues(manifest)
        )
        if manifest_spec_issues:
            report = make_validation_report(manifest_spec_issues)
            _emit_validation_report(
                report, output_format, title="Manifest validation failed"
            )
            raise typer.Exit(1)

        service_result = _validate_manifest_services_or_exit(
            bundle_dir, manifest, output_format=output_format
        )

        validation_result = _validate_manifest_inputs_or_exit(
            bundle_dir, manifest, output_format=output_format
        )

        if output_format == "json":
            console.print_json(data=validation_result)
            return

        console.print(f"[green]✓ Job bundle at '{bundle_path}' is valid.[/green]")
        console.print(f"  - Job Name: {manifest.get('job_name')}")
        console.print(f"  - Graph ID: {manifest.get('graph_id')}")
        console.print(f"  - Nodes count: {len(manifest.get('nodes'))}")
        console.print(
            f"  - Service checks: {len(service_result.get('results') or [])}"
        )
        console.print(
            f"  - Input validation rules: {len(validation_result.get('results') or [])}"
        )

    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, "validate")
        raise typer.Exit(1)


def validate_python_environments(
    bundle_dir: Path, manifest: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    nodes = manifest.get("nodes")
    if not isinstance(nodes, list):
        return errors

    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        config = node.get("config")
        if not isinstance(config, dict) or "python_environment" not in config:
            continue

        node_id = str(node.get("node_id") or f"nodes[{index}]")
        runner_module = config.get("runner_module")
        python_environment = config.get("python_environment")
        if runner_module != "MirrorNeuron.Runner.HostLocal":
            errors.append(
                f"{node_id}: python_environment is only supported with MirrorNeuron.Runner.HostLocal"
            )
            continue
        if not isinstance(python_environment, dict):
            errors.append(f"{node_id}: python_environment must be an object")
            continue

        requirements = python_environment.get("requirements")
        if requirements not in (None, ""):
            if not isinstance(requirements, str):
                errors.append(
                    f"{node_id}: python_environment.requirements must be a string"
                )
            elif not _is_safe_payload_relative_path(requirements):
                errors.append(
                    f"{node_id}: python_environment.requirements must be a relative path inside payloads/"
                )
            elif not (bundle_dir / "payloads" / requirements).is_file():
                errors.append(
                    f"{node_id}: python_environment requirements file not found: payloads/{requirements}"
                )

        packages = python_environment.get("packages")
        if packages is not None and (
            not isinstance(packages, list)
            or not all(
                isinstance(package, str) and package.strip() for package in packages
            )
        ):
            errors.append(
                f"{node_id}: python_environment.packages must be a list of non-empty strings"
            )

    return errors


def _validate_manifest_inputs_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    output_format: str = "table",
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    result = run_input_validation(bundle_dir, manifest, config=config, env=env)
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Input validation failed")
    raise typer.Exit(1)


def _validate_manifest_services_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    output_format: str = "table",
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )

    def resolver(name: str, requirement: dict[str, Any]) -> list[dict[str, Any]]:
        response = client.resolve_service(
            name,
            tags=requirement.get("tags") or [],
            passing_only=True,
        )
        decoded = json.loads(response)
        services = decoded.get("services") if isinstance(decoded, dict) else []
        return services if isinstance(services, list) else []

    result = run_service_validation(
        bundle_dir,
        manifest,
        config=config,
        env=env,
        resolver=resolver,
    )
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Service validation failed")
    raise typer.Exit(1)


def _mark_manifest_force(manifest: dict[str, Any]) -> None:
    metadata = manifest.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        manifest["metadata"] = metadata
    validation = metadata.setdefault("mn_validation", {})
    if not isinstance(validation, dict):
        validation = {}
        metadata["mn_validation"] = validation
    validation["force"] = True
    validation["status"] = "skipped"
    validation["skipped_checks"] = ["services", "input_validation", "requirements"]


def _normalize_validation_output(output: str) -> str:
    normalized = str(output or "table").strip().lower()
    if normalized in {"table", "rich", "pretty"}:
        return "table"
    if normalized == "json":
        return "json"
    console.print("[red]Unsupported output format. Use 'table' or 'json'.[/red]")
    raise typer.Exit(1)


def _emit_validation_report(
    report: dict[str, Any], output_format: str, *, title: str
) -> None:
    if output_format == "json":
        console.print_json(data=report)
        return

    issues = report.get("issues") if isinstance(report.get("issues"), list) else []
    if not issues:
        for error in report.get("errors") or []:
            console.print(f"[red]{title}: {error}[/red]")
        return

    console.print(f"[red]{title}[/red]")
    console.print("Field | Problem | Fix | Rule", markup=False)
    console.print("--- | --- | --- | ---", markup=False)
    for issue in issues:
        location = (
            issue.get("location") if isinstance(issue.get("location"), dict) else {}
        )
        rule = issue.get("rule") if isinstance(issue.get("rule"), dict) else {}
        console.print(
            " | ".join(
                [
                    str(location.get("path") or location.get("pointer") or "-"),
                    str(
                        issue.get("message") or issue.get("code") or "Validation failed"
                    ),
                    str(issue.get("help") or "-"),
                    str(rule.get("name") or rule.get("id") or "-"),
                ]
            ),
            markup=False,
        )


def _legacy_validation_issue(error: str, *, source: str) -> dict[str, Any]:
    path = ""
    if ":" in error:
        path = error.split(":", 1)[0].strip()
    return {
        "code": "manifest.validation_failed",
        "message": error,
        "help": "Fix this manifest field and run validation again.",
        "severity": "error",
        "location": {
            "source": source,
            "path": path,
            "pointer": "/" + source + ("/" + path.replace(".", "/") if path else ""),
        },
    }


def _is_safe_payload_relative_path(path: str) -> bool:
    candidate = Path(path)
    return (
        not candidate.is_absolute()
        and path not in ("", ".")
        and ".." not in candidate.parts
    )


def run(
    target: Annotated[
        Optional[str],
        typer.Argument(
            help="Catalog blueprint ID to run. Use --folder for local blueprint or bundle folders.",
        ),
    ] = None,
    folder: Annotated[
        Optional[str],
        typer.Option(
            "--folder",
            help="Run a local blueprint or bundle folder. Local folders must use this option.",
        ),
    ] = None,
    run_id: Annotated[
        Optional[str],
        typer.Option("--run-id", help="Use a specific shared blueprint run ID."),
    ] = None,
    blueprint_repo: Annotated[
        Optional[str],
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
        Optional[str],
        typer.Option("--revision", help="Checkout a specific git revision before running."),
    ] = None,
    follow_seconds: Annotated[
        Optional[float],
        typer.Option(
            "--follow-seconds",
            help="Seconds to keep polling job events after the submit stream detaches. Defaults to MN_RUN_DETACH_LOG_SECONDS or 30.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Run even if blueprint input validation or runtime requirements fail.",
        ),
    ] = False,
):
    """Run a catalog blueprint, or a local folder with --folder."""
    if folder and target:
        console.print("[red]Error: pass either a blueprint ID or --folder, not both.[/red]")
        raise typer.Exit(1)

    if folder:
        _run_local_folder(folder, run_id=run_id, follow_seconds=follow_seconds, force=force)
        return

    if not target:
        console.print("[red]Error: mn blueprint run expects a blueprint ID or --folder <path>.[/red]")
        console.print("Use [bold]mn blueprint run <blueprint-id>[/bold] for catalog blueprints.")
        console.print("Use [bold]mn blueprint run --folder <path>[/bold] for local blueprint or bundle folders.")
        raise typer.Exit(1)

    target_path = Path(target).expanduser()
    if target_path.exists():
        console.print("[red]Error: local folders must be passed with --folder.[/red]")
        console.print(f"Use [bold]mn blueprint run --folder {target_path}[/bold].")
        raise typer.Exit(1)

    from mn_cli.libs.blueprint_cmds import run_catalog_blueprint

    run_catalog_blueprint(
        target,
        run_id=run_id,
        blueprint_repo=blueprint_repo,
        update=update,
        offline=offline,
        revision=revision,
        follow_seconds=follow_seconds,
        force=force,
    )


def _run_local_folder(
    folder: str,
    *,
    run_id: Optional[str],
    follow_seconds: Optional[float],
    force: bool,
) -> None:
    bundle_dir = Path(folder).expanduser()
    manifest = _load_manifest_for_local_run(bundle_dir)
    if manifest is not None and _is_python_source_manifest(manifest):
        from mn_cli.libs.blueprint_cmds import run_local_blueprint_folder

        run_local_blueprint_folder(
            str(bundle_dir),
            run_id=run_id,
            follow_seconds=follow_seconds,
            force=force,
        )
        return

    env_overrides = {"MN_RUN_ID": run_id} if run_id else None
    submission_metadata = {"blueprint_run_id": run_id} if run_id else None
    run_bundle(
        str(bundle_dir),
        follow_seconds=follow_seconds,
        env_overrides=env_overrides,
        submission_metadata=submission_metadata,
        force=force,
    )


def _load_manifest_for_local_run(bundle_dir: Path) -> Optional[dict[str, Any]]:
    manifest_file = bundle_dir / "manifest.json"
    if not manifest_file.exists():
        return None
    try:
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    except Exception:
        return None
    return manifest if isinstance(manifest, dict) else None


def _is_python_source_manifest(manifest: dict[str, Any]) -> bool:
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    return metadata.get("python_source_mode") is True or bool(metadata.get("python_workflow"))


def run_bundle(
    bundle_path: str,
    *,
    follow_seconds: Optional[float] = None,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    force: bool = False,
):
    """Run a bundle after applying optional runtime metadata and environment."""
    pre_launch_process: subprocess.Popen[Any] | None = None
    pre_launch_run_dir: Path | None = None
    try:
        env_overrides = dict(env_overrides or {})
        config_overrides = dict(config_overrides or {})
        submission_metadata = dict(submission_metadata or {})
        bundle_dir = Path(bundle_path)
        if not bundle_dir.is_dir():
            console.print(
                f"[red]Error: '{bundle_path}' is not a directory. Expected a bundle folder.[/red]"
            )
            raise typer.Exit(1)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(
                f"[red]Error: manifest.json not found in '{bundle_path}'[/red]"
            )
            raise typer.Exit(1)

        with open(manifest_file, "r") as f:
            manifest_dict = json.load(f)

        if manifest_dict.get("require_config") is True:
            config_script = bundle_dir / "config.py"
            if config_script.exists():
                import subprocess
                import sys

                console.print(
                    f"[yellow]Bundle requires configuration. Auto-running {config_script.name}...[/yellow]"
                )
                res = subprocess.run(
                    [sys.executable, config_script.name], cwd=bundle_dir
                )
                if res.returncode != 0:
                    console.print(
                        "[red]Configuration failed or cancelled. Aborting run.[/red]"
                    )
                    raise typer.Exit(1)

                # Reload manifest after configuration
                with open(manifest_file, "r") as f:
                    manifest_dict = json.load(f)
            else:
                console.print(
                    "[red]Bundle requires configuration, but config.py was not found.[/red]"
                )
                raise typer.Exit(1)

        _ensure_local_run_store_identity(
            bundle_dir,
            manifest_dict,
            env_overrides,
            submission_metadata,
            config_overrides=config_overrides,
        )
        blueprint_run_id = submission_metadata.get(
            "blueprint_run_id"
        ) or env_overrides.get("MN_RUN_ID")
        if blueprint_run_id:
            pre_launch_run_dir = _blueprint_run_dir(
                str(blueprint_run_id), env_overrides
            )
            _register_post_launch_hook(
                bundle_dir, str(blueprint_run_id), env_overrides=env_overrides
            )
            pre_launch_process = _start_pre_launch_hook(
                bundle_dir,
                str(blueprint_run_id),
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
        if not force:
            _validate_manifest_services_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
            _validate_manifest_inputs_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
        else:
            console.print(
                "[yellow]Validation skipped because --force was provided; service checks, input checks, and runtime requirements will be bypassed for this run.[/yellow]"
            )
        manifest_dict = prepare_manifest_for_submission(
            bundle_dir,
            manifest_dict,
            env_overrides=env_overrides,
            submission_metadata=submission_metadata,
            config_overrides=config_overrides,
        )
        if force:
            _mark_manifest_force(manifest_dict)
        _prepare_openshell_custom_images(bundle_dir, manifest_dict)

        payloads = {}
        payloads_dir = bundle_dir / "payloads"
        if payloads_dir.is_dir():
            for filepath in payloads_dir.rglob("*"):
                if filepath.is_file():
                    rel_path = filepath.relative_to(payloads_dir).as_posix()
                    with open(filepath, "rb") as f:
                        payloads[rel_path] = f.read()
        payloads.update(runtime_web_ui_support_payloads_for_manifest(manifest_dict))
        stage_local_input_payloads_for_manifest(manifest_dict, payloads, bundle_dir=bundle_dir)
        manifest = json.dumps(manifest_dict)

        blueprint_run_dir = (
            _blueprint_run_dir(blueprint_run_id, env_overrides)
            if blueprint_run_id
            else None
        )
        job_id = client.submit_job(manifest, payloads, force=force)
        log_writer = JobLogWriter(job_id, run_dir=blueprint_run_dir)
        if blueprint_run_id:
            _write_blueprint_job_mapping(
                blueprint_run_id, job_id, submission_metadata, env_overrides
            )
            _write_local_web_ui_handle(
                bundle_dir,
                blueprint_run_id,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
        web_ui_url = _console_web_ui_url(manifest_dict, blueprint_run_dir)
        resolved_follow_seconds = (
            float(os.getenv("MN_RUN_DETACH_LOG_SECONDS", "30"))
            if follow_seconds is None
            else follow_seconds
        )

        console.print(
            generate_run_submitted_panel(
                bundle_name=bundle_dir.name,
                job_id=job_id,
                payload_count=len(payloads),
                log_dir=log_writer.log_dir,
                follow_seconds=resolved_follow_seconds,
                run_mode=_run_mode_label(manifest_dict),
                blueprint_run_id=blueprint_run_id,
                blueprint_revision=submission_metadata.get("blueprint_revision"),
                web_ui_url=web_ui_url,
            )
        )
        final_status = _stream_and_format_events(
            job_id,
            log_writer,
            resolved_follow_seconds,
            web_ui_url=web_ui_url,
        )
        if blueprint_run_dir is not None:
            _start_background_event_relay_if_needed(
                bundle_dir,
                manifest_dict,
                job_id,
                blueprint_run_dir,
                final_status,
                config_overrides=config_overrides,
            )
            if final_status in FINAL_STATUSES:
                cleanup_blueprint_host_hooks(
                    blueprint_run_dir,
                    dry_run=False,
                    summary={
                        "process_removed": [],
                        "process_skipped": [],
                        "errors": [],
                    },
                    reason=f"job_{final_status}",
                )
    except typer.Exit:
        _terminate_pre_launch_process(pre_launch_process, reason="launch_failed")
        if pre_launch_run_dir is not None:
            cleanup_blueprint_host_hooks(
                pre_launch_run_dir,
                dry_run=False,
                summary={"process_removed": [], "process_skipped": [], "errors": []},
                reason="launch_failed",
            )
        raise
    except Exception as e:
        _terminate_pre_launch_process(pre_launch_process, reason="launch_failed")
        if pre_launch_run_dir is not None:
            cleanup_blueprint_host_hooks(
                pre_launch_run_dir,
                dry_run=False,
                summary={"process_removed": [], "process_skipped": [], "errors": []},
                reason="launch_failed",
            )
        handle_cli_error(e, console, "run bundle")
        raise typer.Exit(1)


def _console_web_ui_url(
    manifest_dict: dict[str, Any],
    run_dir: Optional[Path],
) -> Optional[str]:
    return (
        _console_web_ui_url_from_manifest(manifest_dict)
        or _console_web_ui_url_from_run_dir(run_dir)
    )


def _console_web_ui_url_from_manifest(manifest_dict: dict[str, Any]) -> Optional[str]:
    metadata = (
        manifest_dict.get("metadata")
        if isinstance(manifest_dict.get("metadata"), dict)
        else {}
    )
    for candidate in (
        manifest_dict.get("web_ui_service"),
        manifest_dict.get("blueprint_web_ui_service"),
        metadata.get("blueprint_web_ui_service"),
        metadata.get("web_ui_service"),
    ):
        url = _web_ui_url_from_mapping(candidate)
        if url:
            return url

    nodes = manifest_dict.get("nodes")
    if not isinstance(nodes, list):
        return None
    for node in nodes:
        if not isinstance(node, dict):
            continue
        services = node.get("services")
        if not isinstance(services, list):
            continue
        for service in services:
            if not isinstance(service, dict):
                continue
            tags = service.get("tags") if isinstance(service.get("tags"), list) else []
            if service.get("name") != "blueprint-web-ui" and "web_ui" not in tags:
                continue
            url = _web_ui_url_from_mapping(service.get("meta"))
            if url:
                return url
            port = service.get("port")
            if port:
                return f"http://localhost:{port}"
    return None


def _console_web_ui_url_from_run_dir(run_dir: Optional[Path]) -> Optional[str]:
    if run_dir is None:
        return None
    try:
        handle = json.loads((run_dir / "web_ui.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return _web_ui_url_from_mapping(handle)


def _web_ui_url_from_mapping(value: Any) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    for key in ("url", "web_ui_url", "local_url"):
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _ensure_local_run_store_identity(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    env_overrides: dict[str, str],
    submission_metadata: dict[str, Any],
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    if submission_metadata.get("blueprint_run_id") or env_overrides.get("MN_RUN_ID"):
        return

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    identity = config.get("identity") if isinstance(config, dict) else {}
    identity = identity if isinstance(identity, dict) else {}
    metadata = (
        manifest_dict.get("metadata")
        if isinstance(manifest_dict.get("metadata"), dict)
        else {}
    )
    blueprint_id = (
        identity.get("blueprint_id")
        or metadata.get("blueprint_id")
        or manifest_dict.get("graph_id")
        or bundle_dir.name
    )
    run_id = identity.get("run_id") or _make_blueprint_run_id(str(blueprint_id))
    env_overrides["MN_RUN_ID"] = str(run_id)
    submission_metadata.setdefault("blueprint_id", str(blueprint_id))
    submission_metadata["blueprint_run_id"] = str(run_id)


def _prepare_openshell_custom_images(
    bundle_dir: Path, manifest_dict: dict[str, Any]
) -> None:
    nodes = manifest_dict.get("nodes")
    if not isinstance(nodes, list):
        return

    for node in nodes:
        if not isinstance(node, dict):
            continue
        config = node.get("config")
        if not isinstance(config, dict):
            continue
        if config.get("runner_module") != "MirrorNeuron.Sandbox.OpenShell":
            continue

        custom_image = config.get("custom_openshell_image")
        if custom_image is not None:
            source_path = _openshell_local_from_path(bundle_dir, custom_image)
            if source_path is None:
                console.print(
                    f"[red]custom_openshell_image for {node.get('node_id') or 'OpenShell node'} "
                    f"must point to a payload directory or Dockerfile: {custom_image}[/red]"
                )
                raise typer.Exit(1)
        else:
            source_path = _openshell_local_from_path(bundle_dir, config.get("from"))

        if source_path is None:
            continue

        config["from"] = _build_openshell_from_image(
            source_path, node.get("node_id") or "openshell"
        )


def _openshell_gateway_endpoint() -> str:
    configured_endpoint = os.getenv("OPENSHELL_GATEWAY_ENDPOINT")
    if configured_endpoint:
        return configured_endpoint

    gateway_name = _openshell_gateway_name()
    if gateway_name:
        metadata = _openshell_gateway_metadata(gateway_name)
        endpoint = metadata.get("gateway_endpoint")
        if isinstance(endpoint, str) and endpoint.strip():
            return endpoint.strip()

    return f"http://127.0.0.1:{os.getenv('OPENSHELL_GATEWAY_PORT', '58080')}"


def _openshell_env() -> dict[str, str]:
    env = os.environ.copy()
    if env.get("OPENSHELL_GATEWAY_ENDPOINT"):
        return env

    gateway_name = _openshell_gateway_name(env=env)
    if gateway_name:
        env.setdefault("OPENSHELL_GATEWAY", gateway_name)
    else:
        env.setdefault("OPENSHELL_GATEWAY_ENDPOINT", _openshell_gateway_endpoint())
    return env


def _openshell_config_dir() -> Path:
    return Path(
        os.getenv("OPENSHELL_CONFIG_DIR", str(Path.home() / ".config" / "openshell"))
    ).expanduser()


def _openshell_gateway_name(*, env: dict[str, str] | None = None) -> str:
    source_env = env or os.environ
    configured_gateway = source_env.get("OPENSHELL_GATEWAY", "").strip()
    if configured_gateway:
        return configured_gateway

    config_dir = _openshell_config_dir()
    try:
        active_gateway = (
            (config_dir / "active_gateway").read_text(encoding="utf-8").strip()
        )
        if active_gateway:
            return active_gateway
    except OSError:
        pass

    if (config_dir / "gateways" / "openshell" / "metadata.json").is_file():
        return "openshell"
    return ""


def _openshell_gateway_metadata(gateway_name: str) -> dict[str, Any]:
    if not gateway_name:
        return {}

    metadata_path = (
        _openshell_config_dir() / "gateways" / gateway_name / "metadata.json"
    )
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return metadata if isinstance(metadata, dict) else {}


def _openshell_local_from_path(bundle_dir: Path, source: Any) -> Path | None:
    if not isinstance(source, str) or not source.strip():
        return None

    source = source.strip()
    if "://" in source:
        return None

    raw = Path(source).expanduser()
    candidates = (
        [raw]
        if raw.is_absolute()
        else [bundle_dir / "payloads" / source, bundle_dir / source]
    )

    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate.is_dir() and (candidate / "Dockerfile").is_file():
            return candidate
        if candidate.is_file() and candidate.name == "Dockerfile":
            return candidate
    return None


def _build_openshell_from_image(source_path: Path, node_id: Any) -> str:
    console.print(
        f"[yellow]Building OpenShell sandbox image for {node_id} from {source_path}...[/yellow]"
    )
    if _openshell_gateway_uses_local_docker():
        image_ref = _build_local_docker_sandbox_image(source_path)
        console.print(f"[green]✓ OpenShell sandbox image ready:[/green] {image_ref}")
        return image_ref

    result = subprocess.run(
        [
            "openshell",
            "sandbox",
            "create",
            "--from",
            str(source_path),
            "--no-tty",
            "--no-keep",
            "--",
            "true",
        ],
        capture_output=True,
        text=True,
        env=_openshell_env(),
    )
    output = f"{result.stdout}\n{result.stderr}"
    if result.returncode != 0:
        console.print(
            f"[red]Failed to build OpenShell sandbox image for {node_id}.[/red]"
        )
        if output.strip():
            console.print(output.strip())
        raise typer.Exit(1)

    matches = re.findall(r"Image\s+([^\s]+)\s+is available in the gateway", output)
    if not matches:
        console.print(
            f"[red]OpenShell did not report an image reference for {node_id}.[/red]"
        )
        if output.strip():
            console.print(output.strip())
        raise typer.Exit(1)

    image_ref = ANSI_ESCAPE_RE.sub("", matches[-1])
    console.print(f"[green]✓ OpenShell sandbox image ready:[/green] {image_ref}")
    return image_ref


def _openshell_gateway_uses_local_docker() -> bool:
    gateway_name = _openshell_gateway_name()
    if not gateway_name:
        return False

    metadata = _openshell_gateway_metadata(gateway_name)
    if metadata.get("is_remote") is True:
        return False

    endpoint = metadata.get("gateway_endpoint")
    if not isinstance(endpoint, str):
        return False
    parsed = urllib.parse.urlparse(endpoint)
    return parsed.hostname in {"127.0.0.1", "localhost", "::1"}


def _build_local_docker_sandbox_image(source_path: Path) -> str:
    source_path = source_path.resolve()
    digest = hashlib.sha256(str(source_path).encode("utf-8")).hexdigest()[:12]
    image_ref = f"openshell/sandbox-from:{digest}"
    result = subprocess.run(
        ["docker", "build", "-t", image_ref, str(source_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        output = f"{result.stdout}\n{result.stderr}".strip()
        if output:
            console.print(output)
        raise typer.Exit(1)
    return image_ref


def _write_blueprint_job_mapping(
    blueprint_run_id: str,
    job_id: str,
    metadata: dict[str, Any],
    env_overrides: dict[str, str],
) -> None:
    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": blueprint_run_id,
            "job_id": job_id,
            "blueprint_id": metadata.get("blueprint_id"),
            "blueprint_revision": metadata.get("blueprint_revision"),
            "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        tmp = run_dir / f".job.json.{os.getpid()}.tmp"
        tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        tmp.replace(run_dir / "job.json")
    except OSError:
        logger.exception(
            "Failed to write blueprint job mapping for run_id=%s job_id=%s",
            blueprint_run_id,
            job_id,
        )


def _blueprint_run_dir(blueprint_run_id: str, env_overrides: dict[str, str]) -> Path:
    runs_root = Path(
        env_overrides.get("MN_RUNS_ROOT") or os.getenv("MN_RUNS_ROOT") or "~/.mn/runs"
    ).expanduser()
    return runs_root / blueprint_run_id


def _register_post_launch_hook(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
) -> None:
    script_path = (bundle_dir / POST_LAUNCH_SCRIPT).resolve()
    if not script_path.is_file():
        return

    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "post_launch.log"
    ready_file = run_dir / "pre_launch.ready"
    state_file = run_dir / "post_launch_state.json"
    hook_info = {
        "command": ["bash", str(script_path)],
        "script": str(script_path),
        "cwd": str(bundle_dir),
        "log": str(log_path),
        "run_id": blueprint_run_id,
        "bundle_dir": str(bundle_dir),
        "state_file": str(state_file),
        "pre_launch_ready_file": str(ready_file),
        "pre_launch_process_file": str(run_dir / "pre_launch_process.json"),
        "registered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "post_launch_hook.json").write_text(
        json.dumps(hook_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _start_pre_launch_hook(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
    config_overrides: Optional[dict[str, Any]] = None,
) -> subprocess.Popen[Any] | None:
    script_path = (bundle_dir / PRE_LAUNCH_SCRIPT).resolve()
    if not script_path.is_file():
        return None

    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    runs_root = run_dir.parent
    run_dir.mkdir(parents=True, exist_ok=True)
    ready_file = run_dir / "pre_launch.ready"
    try:
        ready_file.unlink()
    except FileNotFoundError:
        pass

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    config = _with_shared_run_store_config(config, blueprint_run_id, str(runs_root))
    runtime_env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )

    env = _openshell_env()
    env.update(runtime_env)
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    env.update(
        {
            "MN_RUN_ID": blueprint_run_id,
            "MN_RUN_DIR": str(run_dir),
            "MN_RUNS_ROOT": str(runs_root),
            "MN_BLUEPRINT_BUNDLE_DIR": str(bundle_dir),
            "MN_BLUEPRINT_CONFIG_JSON": json.dumps(config, sort_keys=True),
            "MN_PRE_LAUNCH_READY_FILE": str(ready_file),
            "MN_POST_LAUNCH_STATE_FILE": str(run_dir / "post_launch_state.json"),
        }
    )

    command = ["bash", str(script_path)]
    log_path = run_dir / "pre_launch.log"
    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                cwd=bundle_dir,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
    except OSError as exc:
        raise RuntimeError(f"Failed to start blueprint pre-launch hook: {exc}") from exc

    process_info = {
        "pid": process.pid,
        "command": command,
        "script": str(script_path),
        "log": str(log_path),
        "ready_file": str(ready_file),
        "run_id": blueprint_run_id,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "pre_launch_process.json").write_text(
        json.dumps(process_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    try:
        _wait_for_pre_launch_ready(run_dir, process, ready_file)
        _apply_pre_launch_ready_metadata(
            ready_file,
            env_overrides=env_overrides,
            config_overrides=config_overrides,
        )
    except Exception:
        _terminate_pre_launch_process(process, reason="pre_launch_failed")
        raise

    console.print(
        "[green]Blueprint pre-launch hook ready:[/green] scripts/pre-launch.sh"
    )
    return process


def _wait_for_pre_launch_ready(
    run_dir: Path, process: subprocess.Popen[Any], ready_file: Path
) -> None:
    try:
        timeout = max(float(os.getenv("MN_PRE_LAUNCH_TIMEOUT_SECONDS", "30")), 0)
    except ValueError:
        timeout = 30.0
    deadline = time.monotonic() + timeout
    while time.monotonic() <= deadline:
        if ready_file.exists():
            return
        poll = getattr(process, "poll", None)
        if callable(poll) and poll() is not None:
            raise RuntimeError(
                f"Blueprint pre-launch hook exited before becoming ready. See {run_dir / 'pre_launch.log'}."
            )
        time.sleep(0.1)
    raise RuntimeError(
        f"Blueprint pre-launch hook timed out after {timeout:g}s. See {run_dir / 'pre_launch.log'}."
    )


def _apply_pre_launch_ready_metadata(
    ready_file: Path,
    *,
    env_overrides: dict[str, str],
    config_overrides: dict[str, Any],
) -> None:
    raw = ready_file.read_text(encoding="utf-8").strip()
    if not raw or raw == "ready":
        return
    try:
        metadata = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(
            "Ignoring non-JSON pre-launch ready metadata from %s", ready_file
        )
        return
    if not isinstance(metadata, dict):
        return
    env_patch = metadata.get("env")
    if isinstance(env_patch, dict):
        env_overrides.update(
            {
                str(key): str(value)
                for key, value in env_patch.items()
                if value is not None
            }
        )
    config_patch = metadata.get("config") or metadata.get("config_overrides")
    if isinstance(config_patch, dict):
        merged = _deep_merge_dict(config_overrides, config_patch)
        config_overrides.clear()
        config_overrides.update(merged)


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def _terminate_pre_launch_process(
    process: subprocess.Popen[Any] | None, *, reason: str
) -> None:
    if process is None:
        return
    poll = getattr(process, "poll", None)
    if callable(poll) and poll() is not None:
        return
    pid = getattr(process, "pid", None)
    if not isinstance(pid, int):
        try:
            process.terminate()
        except OSError:
            pass
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except OSError:
        try:
            process.terminate()
        except OSError:
            pass
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pid, signal.SIGKILL)
        except OSError:
            try:
                process.kill()
            except OSError:
                pass
    logger.info("Stopped blueprint pre-launch hook pid=%s reason=%s", pid, reason)


def _write_local_web_ui_handle(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    if not isinstance(config, dict):
        return
    web_ui = config.get("web_ui")
    if not isinstance(web_ui, dict) or web_ui.get("enabled") is False:
        return

    dashboard = (
        web_ui.get("dashboard") if isinstance(web_ui.get("dashboard"), dict) else {}
    )
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    runs_root = Path(
        env_overrides.get("MN_RUNS_ROOT") or os.getenv("MN_RUNS_ROOT") or "~/.mn/runs"
    ).expanduser()
    run_dir = runs_root / blueprint_run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter == "gradio":
        return

    script_path = _web_ui_registration_script(bundle_dir, web_ui, output, dashboard)
    if script_path is not None:
        _launch_blueprint_web_ui_script(
            bundle_dir,
            script_path,
            blueprint_run_id,
            run_dir,
            runs_root,
            config,
            env_overrides=env_overrides,
        )
        return

    module_name = _web_ui_registration_module(web_ui, output, dashboard)
    if module_name is not None:
        _launch_blueprint_web_ui_module(
            bundle_dir,
            module_name,
            blueprint_run_id,
            run_dir,
            runs_root,
            config,
            env_overrides=env_overrides,
        )
        return

    custom_url = _web_ui_custom_url(output)
    if custom_url:
        _write_web_ui_handle(
            run_dir,
            {
                "adapter": str(output.get("adapter") or "custom"),
                "kind": "output",
                "url": custom_url,
                "title": str(
                    output.get("title") or identity.get("name") or bundle_dir.name
                ),
                "status": "external",
                "metadata": {
                    "blueprint_id": identity.get("blueprint_id"),
                    "run_id": blueprint_run_id,
                    "registered_by": "mn_cli",
                    "customer_managed": True,
                },
            },
        )
        return

    _write_static_web_ui_handle(bundle_dir, run_dir, blueprint_run_id, config)


def _start_background_event_relay_if_needed(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    job_id: str,
    run_dir: Path,
    final_status: str,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    if final_status in FINAL_STATUSES:
        return
    if not _is_live_manifest(manifest_dict):
        return
    if os.getenv("MN_RUN_BACKGROUND_EVENT_RELAY", "1").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    if not isinstance(web_ui, dict) or web_ui.get("enabled") is False:
        return

    max_seconds = _background_event_relay_max_seconds(config)
    poll_seconds = _background_event_relay_poll_seconds(config)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "event_relay.log"
    command = [
        sys.executable,
        "-m",
        "mn_blueprint_support.event_relay",
        "--job-id",
        job_id,
        "--run-dir",
        str(run_dir),
        "--poll-seconds",
        f"{poll_seconds:g}",
    ]
    if max_seconds is not None:
        command.extend(["--max-seconds", f"{max_seconds:g}"])

    env = os.environ.copy()
    env["MN_RUN_EVENT_RELAY_CHILD"] = "1"
    _inject_local_blueprint_support_pythonpath(env)
    with open(log_path, "a", encoding="utf-8") as relay_log:
        process = subprocess.Popen(
            command,
            stdout=relay_log,
            stderr=relay_log,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
            env=env,
        )

    relay_info = {
        "job_id": job_id,
        "pid": process.pid,
        "poll_seconds": poll_seconds,
        "max_seconds": max_seconds,
        "log_path": str(log_path),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "event_relay.json").write_text(
        json.dumps(relay_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    console.print(
        "[green]Live event relay:[/green] keeping the local dashboard stream updated in the background."
    )


def _is_live_manifest(manifest_dict: dict[str, Any]) -> bool:
    policies = (
        manifest_dict.get("policies")
        if isinstance(manifest_dict.get("policies"), dict)
        else {}
    )
    scheduler = policies.get("scheduler", {}) if isinstance(policies.get("scheduler"), dict) else {}
    job_type = str(
        policies.get("job_type")
        or scheduler.get("job_type")
        or manifest_dict.get("job_type")
        or manifest_dict.get("type")
        or "batch"
    ).lower()
    return job_type == "service" or policies.get("stream_mode") == "live"


def _background_event_relay_poll_seconds(config: dict[str, Any]) -> float:
    raw = os.getenv("MN_RUN_EVENT_RELAY_POLL_SECONDS")
    if raw is not None:
        try:
            return max(float(raw), 0.1)
        except ValueError:
            return 1.0

    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    try:
        return max(float(output.get("refresh_seconds", 1.0)), 0.1)
    except (TypeError, ValueError):
        return 1.0


def _background_event_relay_max_seconds(config: dict[str, Any]) -> float | None:
    raw = os.getenv("MN_RUN_EVENT_RELAY_MAX_SECONDS")
    if raw is not None:
        if raw.strip().lower() in {"", "0", "none", "infinity"}:
            return None
        try:
            return max(float(raw), 0.0)
        except ValueError:
            return None

    budgets = config.get("budgets") if isinstance(config.get("budgets"), dict) else {}
    try:
        return max(float(budgets.get("max_stream_duration_seconds", 3600)), 0.0)
    except (TypeError, ValueError):
        return 3600.0


def _web_ui_registration_script(
    bundle_dir: Path,
    web_ui: dict[str, Any],
    output: dict[str, Any],
    dashboard: dict[str, Any],
) -> Path | None:
    registration = (
        web_ui.get("registration")
        if isinstance(web_ui.get("registration"), dict)
        else {}
    )
    dashboard_registration = (
        dashboard.get("registration")
        if isinstance(dashboard.get("registration"), dict)
        else {}
    )
    for raw_value in (
        output.get("launch_script"),
        output.get("registration_script"),
        registration.get("script"),
        dashboard_registration.get("script"),
    ):
        script_path = _safe_bundle_file(bundle_dir, raw_value)
        if script_path is not None:
            return script_path
    return None


def _web_ui_registration_module(
    web_ui: dict[str, Any],
    output: dict[str, Any],
    dashboard: dict[str, Any],
) -> str | None:
    registration = (
        web_ui.get("registration")
        if isinstance(web_ui.get("registration"), dict)
        else {}
    )
    dashboard_registration = (
        dashboard.get("registration")
        if isinstance(dashboard.get("registration"), dict)
        else {}
    )
    for raw_value in (
        output.get("launch_module"),
        output.get("registration_module"),
        registration.get("module"),
        dashboard_registration.get("module"),
    ):
        module_name = _safe_python_module(raw_value)
        if module_name is not None:
            return module_name

    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter == "gradio" and output.get("auto_generate", True) is not False:
        return "mn_blueprint_support.gradio_dashboard"
    return None


def _safe_python_module(raw_value: Any) -> str | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    module_name = raw_value.strip()
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", module_name):
        return module_name
    return None


def _safe_bundle_file(bundle_dir: Path, raw_value: Any) -> Path | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    relative = Path(raw_value)
    if relative.is_absolute() or ".." in relative.parts:
        return None
    root = bundle_dir.resolve()
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root) or not candidate.is_file():
        return None
    return candidate


def _launch_blueprint_web_ui_script(
    bundle_dir: Path,
    script_path: Path,
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
) -> None:
    command = [
        sys.executable,
        str(script_path),
    ]
    _launch_blueprint_web_ui_command(
        bundle_dir,
        command,
        blueprint_run_id,
        run_dir,
        runs_root,
        config,
        env_overrides=env_overrides,
        script=str(script_path),
    )


def _launch_blueprint_web_ui_module(
    bundle_dir: Path,
    module_name: str,
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
) -> None:
    command = [
        sys.executable,
        "-m",
        module_name,
    ]
    _launch_blueprint_web_ui_command(
        bundle_dir,
        command,
        blueprint_run_id,
        run_dir,
        runs_root,
        config,
        env_overrides=env_overrides,
        module=module_name,
    )


def _launch_blueprint_web_ui_command(
    bundle_dir: Path,
    command_prefix: list[str],
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
    script: str | None = None,
    module: str | None = None,
) -> None:
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    host = _web_ui_bind_host(output, env_overrides)
    port = _web_ui_port(output, host=host)
    base_url = _web_ui_base_url(output, host, port)

    env = os.environ.copy()
    env.update(env_overrides)
    env.update(
        {
            "MN_RUN_ID": blueprint_run_id,
            "MN_RUN_DIR": str(run_dir),
            "MN_RUNS_ROOT": str(runs_root),
            "MN_BLUEPRINT_BUNDLE_DIR": str(bundle_dir),
            "MN_BLUEPRINT_CONFIG_JSON": json.dumps(config, sort_keys=True),
            "MN_BLUEPRINT_ID": str(identity.get("blueprint_id") or bundle_dir.name),
            "MN_BLUEPRINT_WEB_UI_HOST": host,
            "MN_BLUEPRINT_WEB_UI_PORT": str(port),
            "MN_BLUEPRINT_WEB_UI_BASE_URL": base_url,
        }
    )
    _inject_local_blueprint_support_pythonpath(env)

    command = command_prefix + [
        "--run-id",
        blueprint_run_id,
        "--run-dir",
        str(run_dir),
        "--runs-root",
        str(runs_root),
        "--bundle-dir",
        str(bundle_dir),
        "--host",
        host,
        "--port",
        str(port),
        "--base-url",
        base_url,
    ]
    log_path = run_dir / "web_ui.log"
    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                cwd=bundle_dir,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
        process_info = {
            "pid": process.pid,
            "command": command,
            "log": str(log_path),
            "blueprint_id": identity.get("blueprint_id"),
            "run_id": blueprint_run_id,
            "url": base_url,
        }
        if script:
            process_info["script"] = script
        if module:
            process_info["module"] = module
        (run_dir / "web_ui_process.json").write_text(
            json.dumps(process_info, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        _wait_for_blueprint_web_ui(run_dir, process)
    except OSError:
        logger.exception(
            "Failed to launch blueprint web UI for run_id=%s", blueprint_run_id
        )


def _inject_local_blueprint_support_pythonpath(env: dict[str, str]) -> None:
    repo_root = Path(
        os.getenv("MN_WORKSPACE_ROOT")
        or os.getenv("MIRROR_NEURON_WORKSPACE")
        or os.getenv("OTTERDESK_MIRROR_NEURON_WORKSPACE")
        or Path(__file__).resolve().parents[3]
    ).expanduser()
    support_src = repo_root / "mn-skills" / "blueprint_support_skill" / "src"
    if not support_src.is_dir():
        support_src = repo_root / "mn-skills" / "blueprint-support-skill" / "src"
    if not support_src.is_dir():
        return
    current = env.get("PYTHONPATH")
    paths = [str(support_src)]
    if current:
        paths.append(current)
    env["PYTHONPATH"] = os.pathsep.join(paths)


def _web_ui_bind_host(output: dict[str, Any], env_overrides: dict[str, str]) -> str:
    for value in (
        env_overrides.get("MN_BLUEPRINT_WEB_UI_BIND_HOST"),
        env_overrides.get("MN_BLUEPRINT_WEB_UI_HOST"),
        os.getenv("MN_BLUEPRINT_WEB_UI_BIND_HOST"),
        os.getenv("MN_BLUEPRINT_WEB_UI_HOST"),
        output.get("host"),
    ):
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "127.0.0.1"


def _parse_web_ui_port(raw_value: Any, *, name: str) -> int | None:
    if raw_value in (None, ""):
        return None
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        raise RuntimeError(f"{name} must be a positive integer.") from None
    if parsed <= 0 or parsed > 65535:
        raise RuntimeError(f"{name} must be between 1 and 65535.")
    return parsed


def _web_ui_port_available(host: str, port: int) -> bool:
    bind_host = host if host and host not in {"::"} else "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((bind_host, port))
        except OSError:
            return False
    return True


def _web_ui_port_range_configured() -> bool:
    return (
        os.getenv("MN_BLUEPRINT_WEB_UI_PORT_START") not in (None, "")
        or os.getenv("MN_BLUEPRINT_WEB_UI_PORT_END") not in (None, "")
    )


def _web_ui_port_range() -> tuple[int, int]:
    start = _parse_web_ui_port(
        os.getenv(
            "MN_BLUEPRINT_WEB_UI_PORT_START", str(DEFAULT_BLUEPRINT_WEB_UI_PORT_START)
        ),
        name="MN_BLUEPRINT_WEB_UI_PORT_START",
    )
    end = _parse_web_ui_port(
        os.getenv(
            "MN_BLUEPRINT_WEB_UI_PORT_END", str(DEFAULT_BLUEPRINT_WEB_UI_PORT_END)
        ),
        name="MN_BLUEPRINT_WEB_UI_PORT_END",
    )
    assert start is not None and end is not None
    if end < start:
        raise RuntimeError(
            "MN_BLUEPRINT_WEB_UI_PORT_END must be greater than or equal to MN_BLUEPRINT_WEB_UI_PORT_START."
        )
    return start, end


def _ephemeral_web_ui_port(host: str) -> int:
    bind_host = host if host and host not in {"::"} else "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((bind_host, 0))
        return int(sock.getsockname()[1])


def _web_ui_port(output: dict[str, Any], *, host: str = "127.0.0.1") -> int:
    raw_port = output.get("port")
    explicit_port = _parse_web_ui_port(raw_port, name="web_ui.output.port")
    if explicit_port is not None:
        if not _web_ui_port_available(host, explicit_port):
            raise RuntimeError(
                f"Blueprint web UI port {explicit_port} is unavailable on {host}."
            )
        return explicit_port

    start, end = _web_ui_port_range()
    for port in range(start, end + 1):
        if _web_ui_port_available(host, port):
            return port
    if not _web_ui_port_range_configured():
        return _ephemeral_web_ui_port(host)
    raise RuntimeError(f"No available blueprint web UI port found in {start}-{end}.")


def _web_ui_base_url(output: dict[str, Any], host: str, port: int) -> str:
    configured_url = output.get("base_url") or os.getenv("MN_BLUEPRINT_WEB_UI_BASE_URL")
    if isinstance(configured_url, str) and configured_url.strip():
        return configured_url.rstrip("/")
    public_host = output.get("public_host") or os.getenv(
        "MN_BLUEPRINT_WEB_UI_PUBLIC_HOST"
    )
    if not isinstance(public_host, str) or not public_host.strip():
        public_host = "localhost" if host in {"127.0.0.1", "0.0.0.0", "::"} else host
    scheme = str(output.get("scheme") or "http")
    return f"{scheme}://{public_host}:{port}"


def _wait_for_blueprint_web_ui(run_dir: Path, process: subprocess.Popen[Any]) -> None:
    try:
        timeout = max(
            float(os.getenv("MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", "5")), 0
        )
    except ValueError:
        timeout = 5.0
    deadline = time.monotonic() + timeout
    handle_path = run_dir / "web_ui.json"
    while time.monotonic() < deadline:
        if handle_path.exists():
            return
        poll = getattr(process, "poll", None)
        if callable(poll) and poll() is not None:
            return
        time.sleep(0.1)


def _web_ui_custom_url(output: dict[str, Any]) -> str:
    adapter = str(output.get("adapter") or "").lower()
    if adapter != "custom":
        return ""
    url = output.get("custom_url") or output.get("url")
    return str(url).strip() if isinstance(url, str) else ""


def _write_static_web_ui_handle(
    bundle_dir: Path,
    run_dir: Path,
    blueprint_run_id: str,
    config: dict[str, Any],
) -> None:
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    dashboard = (
        web_ui.get("dashboard") if isinstance(web_ui.get("dashboard"), dict) else {}
    )
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter not in {"static_html", "html"} and not (
        output.get("path") or dashboard.get("path") or web_ui.get("path")
    ):
        return

    html_path = _safe_bundle_file(
        bundle_dir, output.get("path") or dashboard.get("path") or web_ui.get("path")
    )
    if html_path is None:
        return

    events_path = run_dir / "events.jsonl"
    query: dict[str, str] = {}
    video_source = _web_ui_video_source(config, bundle_dir)
    video_query_param = str(output.get("video_source_query_param") or "video")
    events_query_param = str(output.get("events_query_param") or "events")
    if video_source:
        query[video_query_param] = video_source
    query[events_query_param] = events_path.resolve().as_uri()
    url = html_path.resolve().as_uri()
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"

    _write_web_ui_handle(
        run_dir,
        {
            "adapter": "static_html",
            "kind": "output",
            "url": url,
            "title": str(
                output.get("title") or identity.get("name") or bundle_dir.name
            ),
            "path": str(html_path),
            "status": "available",
            "metadata": {
                "blueprint_id": identity.get("blueprint_id"),
                "run_id": blueprint_run_id,
                "events_path": str(events_path),
                "registered_by": "mn_cli",
                "launch_adapter": "blueprint_static_html",
            },
        },
    )


def _write_web_ui_handle(run_dir: Path, handle: dict[str, Any]) -> None:
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "web_ui.json").write_text(
            json.dumps(handle, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    except OSError:
        logger.exception(
            "Failed to write blueprint web UI handle for run_dir=%s", run_dir
        )


def _web_ui_video_source(config: dict[str, Any], bundle_dir: Path) -> str:
    video_source = (
        (config.get("video_source") or {})
        if isinstance(config.get("video_source"), dict)
        else {}
    ).get("uri")
    if not isinstance(video_source, str) or not video_source:
        dashboard = (
            (config.get("web_ui") or {}).get("dashboard")
            if isinstance(config.get("web_ui"), dict)
            else {}
        )
        video_source = (dashboard or {}).get("default_video_source") or ""
    if not isinstance(video_source, str) or not video_source:
        return ""
    if "://" in video_source:
        return video_source
    for candidate in (
        bundle_dir / "payloads" / "person_detector" / video_source,
        bundle_dir / "payloads" / "web_ui" / video_source,
        bundle_dir / video_source,
    ):
        if candidate.is_file():
            return candidate.resolve().as_uri()
    return video_source


def _live_monitor(job_id: str):
    import sys
    import select
    import time
    from rich.live import Live

    is_tty = sys.stdin.isatty()
    old_settings = None
    if is_tty:
        import tty
        import termios

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setcbreak(fd)

    class MonitorView:
        def __init__(self):
            self.data = None

        def __rich__(self):
            if not self.data:
                from rich.panel import Panel

                return Panel("Connecting...", style="cyan")
            if "error" in self.data:
                from rich.panel import Panel

                return Panel(f"Error fetching job: {self.data['error']}", style="red")
            return generate_live_layout(job_id, self.data)

    final_status = "unknown"
    view = MonitorView()

    try:
        with Live(view, refresh_per_second=12, console=console):
            while True:
                try:
                    job_json = client.get_job(job_id)
                    data = json.loads(job_json)
                except Exception as e:
                    data = {"error": str(e)}

                view.data = data

                if data and "error" not in data:
                    status = data.get("summary", {}).get("status", "unknown")
                    if status in ["completed", "failed", "cancelled"]:
                        final_status = status
                        break

                if is_tty:
                    i, o, e = select.select([sys.stdin], [], [], 0.5)
                    if i:
                        key = sys.stdin.read(1)
                        if key.lower() == "q" or key == "\x03":  # \x03 is Ctrl-C
                            break
                else:
                    time.sleep(0.5)
                    break

    except KeyboardInterrupt:
        pass
    finally:
        if is_tty and old_settings:
            import termios

            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    if final_status in ["completed", "failed", "cancelled"]:
        # Save results and print final summary
        fetch_and_save_results(job_id, data)
        log_dir = Path(f"/tmp/mn_{job_id}")
        panel = generate_summary_panel(job_id, final_status, log_dir)
        console.print(panel)
    else:
        console.print(f"\n[yellow]Exited live monitor for {job_id}[/yellow]")
        fetch_and_save_results(job_id, data)


def monitor(job_id: str):
    """Stream live events for a job"""
    try:
        _live_monitor(job_id)
    except Exception as e:
        handle_cli_error(e, console, "monitor stream")


def result(job_id: str):
    """Fetch and save the final and progressive results for a job"""
    try:
        console.print(f"Fetching results for {job_id}...")
        fetch_and_save_results(job_id)

        log_dir = Path(f"/tmp/mn_{job_id}")
        res_file = log_dir / "result.txt"
        stream_file = log_dir / "result_stream.txt"

        if res_file.exists():
            console.print(f"[green]Final result saved to: {res_file}[/green]")
        else:
            console.print(
                "[yellow]No final result found (job might not be completed).[/yellow]"
            )

        if stream_file.exists():
            console.print(f"[green]Stream results saved to: {stream_file}[/green]")

    except Exception as e:
        handle_cli_error(e, console, "fetch results")
