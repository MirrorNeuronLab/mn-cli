import typer
import json
import os
import re
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
    run_mode_label as _run_mode_label,
)
from mn_cli.libs.blueprint_observability import make_blueprint_run_id as _make_blueprint_run_id
from mn_cli.shared import console, client, logger
from mn_cli.error_handler import handle_cli_error

FINAL_STATUSES = {"completed", "failed", "cancelled"}
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
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
                logger.exception("Failed to decode event while saving results for %s", job_id)
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
) -> str:
    log_writer = log_writer or JobLogWriter(job_id)
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
                        progress.console.print(f"[green]Blueprint Web UI:[/green] {web_ui_url}")

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
                    elif event_type in ["agent_message_received", "aggregator_received"]:
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
                log_dir=log_dir
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
            console.print(generate_detached_panel(job_id, log_dir, status, log_writer.event_count))
            status_text = status
        
    except KeyboardInterrupt:
        console.print("[yellow]Detached from log stream.[/yellow]")
        status, _data = _follow_job_events(job_id, log_writer, 0)
        console.print(generate_detached_panel(job_id, log_dir, status, log_writer.event_count))
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
                    progress.console.print(f"[green]Blueprint Web UI:[/green] {web_ui_url}")

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


def validate(bundle_path: str):
    """Check if a job bundle in a local folder is valid to run"""
    try:
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

        required_keys = ["manifest_version", "graph_id", "job_name", "entrypoints", "nodes"]
        missing = [k for k in required_keys if k not in manifest]
        if missing:
            console.print(f"[red]Error: manifest.json is missing required keys: {', '.join(missing)}[/red]")
            raise typer.Exit(1)

        if not isinstance(manifest.get("nodes"), type([])):
            console.print("[red]Error: 'nodes' must be a list in manifest.json[/red]")
            raise typer.Exit(1)

        if "requiredContextEngine" in manifest and not isinstance(manifest.get("requiredContextEngine"), bool):
            console.print("[red]Error: 'requiredContextEngine' must be true or false in manifest.json[/red]")
            raise typer.Exit(1)

        python_environment_errors = validate_python_environments(bundle_dir, manifest)
        if python_environment_errors:
            for error in python_environment_errors:
                console.print(f"[red]Error: {error}[/red]")
            raise typer.Exit(1)

        console.print(f"[green]✓ Job bundle at '{bundle_path}' is valid.[/green]")
        console.print(f"  - Job Name: {manifest.get('job_name')}")
        console.print(f"  - Graph ID: {manifest.get('graph_id')}")
        console.print(f"  - Nodes count: {len(manifest.get('nodes'))}")
        
    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, 'validate')
        raise typer.Exit(1)


def validate_python_environments(bundle_dir: Path, manifest: dict[str, Any]) -> list[str]:
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
            errors.append(f"{node_id}: python_environment is only supported with MirrorNeuron.Runner.HostLocal")
            continue
        if not isinstance(python_environment, dict):
            errors.append(f"{node_id}: python_environment must be an object")
            continue

        requirements = python_environment.get("requirements")
        if requirements not in (None, ""):
            if not isinstance(requirements, str):
                errors.append(f"{node_id}: python_environment.requirements must be a string")
            elif not _is_safe_payload_relative_path(requirements):
                errors.append(
                    f"{node_id}: python_environment.requirements must be a relative path inside payloads/"
                )
            elif not (bundle_dir / "payloads" / requirements).is_file():
                errors.append(f"{node_id}: python_environment requirements file not found: payloads/{requirements}")

        packages = python_environment.get("packages")
        if packages is not None and (
            not isinstance(packages, list)
            or not all(isinstance(package, str) and package.strip() for package in packages)
        ):
            errors.append(f"{node_id}: python_environment.packages must be a list of non-empty strings")

    return errors


def _is_safe_payload_relative_path(path: str) -> bool:
    candidate = Path(path)
    return not candidate.is_absolute() and path not in ("", ".") and ".." not in candidate.parts


def run(
    bundle_path: str,
    follow_seconds: Annotated[
        Optional[float],
        typer.Option(
            "--follow-seconds",
            help="Seconds to keep polling job events after the submit stream detaches. Defaults to MN_RUN_DETACH_LOG_SECONDS or 30.",
        ),
    ] = None,
):
    """Run a job bundle from a local folder directly"""
    run_bundle(bundle_path, follow_seconds=follow_seconds)


def run_bundle(
    bundle_path: str,
    *,
    follow_seconds: Optional[float] = None,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
):
    """Run a bundle after applying optional runtime metadata and environment."""
    try:
        env_overrides = dict(env_overrides or {})
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
                console.print(f"[yellow]Bundle requires configuration. Auto-running {config_script.name}...[/yellow]")
                res = subprocess.run([sys.executable, config_script.name], cwd=bundle_dir)
                if res.returncode != 0:
                    console.print("[red]Configuration failed or cancelled. Aborting run.[/red]")
                    raise typer.Exit(1)
                
                # Reload manifest after configuration
                with open(manifest_file, "r") as f:
                    manifest_dict = json.load(f)
            else:
                console.print("[red]Bundle requires configuration, but config.py was not found.[/red]")
                raise typer.Exit(1)

        _ensure_local_run_store_identity(
            bundle_dir,
            manifest_dict,
            env_overrides,
            submission_metadata,
            config_overrides=config_overrides,
        )
        manifest_dict = prepare_manifest_for_submission(
            bundle_dir,
            manifest_dict,
            env_overrides=env_overrides,
            submission_metadata=submission_metadata,
            config_overrides=config_overrides,
        )
        _prepare_openshell_custom_images(bundle_dir, manifest_dict)
        manifest = json.dumps(manifest_dict)

        payloads = {}
        payloads_dir = bundle_dir / "payloads"
        if payloads_dir.is_dir():
            for filepath in payloads_dir.rglob("*"):
                if filepath.is_file():
                    rel_path = filepath.relative_to(payloads_dir).as_posix()
                    with open(filepath, "rb") as f:
                        payloads[rel_path] = f.read()

        blueprint_run_id = submission_metadata.get("blueprint_run_id") or env_overrides.get("MN_RUN_ID")
        blueprint_run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides) if blueprint_run_id else None
        job_id = client.submit_job(manifest, payloads)
        log_writer = JobLogWriter(job_id, run_dir=blueprint_run_dir)
        if blueprint_run_id:
            _write_blueprint_job_mapping(blueprint_run_id, job_id, submission_metadata, env_overrides)
            _write_local_web_ui_handle(
                bundle_dir,
                blueprint_run_id,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
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
            )
        )
        final_status = _stream_and_format_events(job_id, log_writer, resolved_follow_seconds)
        if blueprint_run_dir is not None:
            _start_background_event_relay_if_needed(
                bundle_dir,
                manifest_dict,
                job_id,
                blueprint_run_dir,
                final_status,
                config_overrides=config_overrides,
            )
    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, 'run bundle')
        raise typer.Exit(1)


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
    metadata = manifest_dict.get("metadata") if isinstance(manifest_dict.get("metadata"), dict) else {}
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


def _prepare_openshell_custom_images(bundle_dir: Path, manifest_dict: dict[str, Any]) -> None:
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

        config["from"] = _build_openshell_from_image(source_path, node.get("node_id") or "openshell")


def _openshell_local_from_path(bundle_dir: Path, source: Any) -> Path | None:
    if not isinstance(source, str) or not source.strip():
        return None

    source = source.strip()
    if "://" in source:
        return None

    raw = Path(source).expanduser()
    candidates = [raw] if raw.is_absolute() else [bundle_dir / "payloads" / source, bundle_dir / source]

    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate.is_dir() and (candidate / "Dockerfile").is_file():
            return candidate
        if candidate.is_file() and candidate.name == "Dockerfile":
            return candidate
    return None


def _build_openshell_from_image(source_path: Path, node_id: Any) -> str:
    console.print(f"[yellow]Building OpenShell sandbox image for {node_id} from {source_path}...[/yellow]")
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
    )
    output = f"{result.stdout}\n{result.stderr}"
    if result.returncode != 0:
        console.print(f"[red]Failed to build OpenShell sandbox image for {node_id}.[/red]")
        if output.strip():
            console.print(output.strip())
        raise typer.Exit(1)

    matches = re.findall(r"Image\s+([^\s]+)\s+is available in the gateway", output)
    if not matches:
        console.print(f"[red]OpenShell did not report an image reference for {node_id}.[/red]")
        if output.strip():
            console.print(output.strip())
        raise typer.Exit(1)

    image_ref = ANSI_ESCAPE_RE.sub("", matches[-1])
    console.print(f"[green]✓ OpenShell sandbox image ready:[/green] {image_ref}")
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
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(run_dir / "job.json")
    except OSError:
        logger.exception("Failed to write blueprint job mapping for run_id=%s job_id=%s", blueprint_run_id, job_id)


def _blueprint_run_dir(blueprint_run_id: str, env_overrides: dict[str, str]) -> Path:
    runs_root = Path(env_overrides.get("MN_RUNS_ROOT") or os.getenv("MN_RUNS_ROOT") or "~/.mn/runs").expanduser()
    return runs_root / blueprint_run_id


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

    dashboard = web_ui.get("dashboard") if isinstance(web_ui.get("dashboard"), dict) else {}
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = config.get("identity") if isinstance(config.get("identity"), dict) else {}
    runs_root = Path(env_overrides.get("MN_RUNS_ROOT") or os.getenv("MN_RUNS_ROOT") or "~/.mn/runs").expanduser()
    run_dir = runs_root / blueprint_run_id
    run_dir.mkdir(parents=True, exist_ok=True)

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
                "title": str(output.get("title") or identity.get("name") or bundle_dir.name),
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
    if os.getenv("MN_RUN_BACKGROUND_EVENT_RELAY", "1").strip().lower() in {"0", "false", "no", "off"}:
        return

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    if not isinstance(web_ui, dict) or web_ui.get("enabled") is False:
        return
    if not (run_dir / "web_ui.json").exists() and not (run_dir / "ui.json").exists():
        return

    max_seconds = _background_event_relay_max_seconds(config)
    poll_seconds = _background_event_relay_poll_seconds(config)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "event_relay.log"
    command = [
        sys.executable,
        "-m",
        "mn_cli.libs.event_relay",
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
    console.print("[green]Live event relay:[/green] keeping the local dashboard stream updated in the background.")


def _is_live_manifest(manifest_dict: dict[str, Any]) -> bool:
    policies = manifest_dict.get("policies") if isinstance(manifest_dict.get("policies"), dict) else {}
    return manifest_dict.get("daemon") is True or policies.get("stream_mode") == "live"


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
    registration = web_ui.get("registration") if isinstance(web_ui.get("registration"), dict) else {}
    dashboard_registration = dashboard.get("registration") if isinstance(dashboard.get("registration"), dict) else {}
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
    registration = web_ui.get("registration") if isinstance(web_ui.get("registration"), dict) else {}
    dashboard_registration = dashboard.get("registration") if isinstance(dashboard.get("registration"), dict) else {}
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
    identity = config.get("identity") if isinstance(config.get("identity"), dict) else {}
    host = str(output.get("host") or env_overrides.get("MN_BLUEPRINT_WEB_UI_HOST") or os.getenv("MN_BLUEPRINT_WEB_UI_HOST") or "127.0.0.1")
    port = _web_ui_port(output)
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
        (run_dir / "web_ui_process.json").write_text(json.dumps(process_info, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _wait_for_blueprint_web_ui(run_dir, process)
    except OSError:
        logger.exception("Failed to launch blueprint web UI for run_id=%s", blueprint_run_id)


def _inject_local_blueprint_support_pythonpath(env: dict[str, str]) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    support_src = repo_root / "mn-skills" / "blueprint_support_skill" / "src"
    if not support_src.is_dir():
        return
    current = env.get("PYTHONPATH")
    paths = [str(support_src)]
    if current:
        paths.append(current)
    env["PYTHONPATH"] = os.pathsep.join(paths)


def _web_ui_port(output: dict[str, Any]) -> int:
    raw_port = output.get("port")
    if raw_port not in (None, ""):
        try:
            parsed = int(raw_port)
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            pass
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _web_ui_base_url(output: dict[str, Any], host: str, port: int) -> str:
    configured_url = output.get("base_url") or os.getenv("MN_BLUEPRINT_WEB_UI_BASE_URL")
    if isinstance(configured_url, str) and configured_url.strip():
        return configured_url.rstrip("/")
    public_host = output.get("public_host")
    if not isinstance(public_host, str) or not public_host.strip():
        public_host = "localhost" if host in {"127.0.0.1", "0.0.0.0", "::"} else host
    scheme = str(output.get("scheme") or "http")
    return f"{scheme}://{public_host}:{port}"


def _wait_for_blueprint_web_ui(run_dir: Path, process: subprocess.Popen[Any]) -> None:
    try:
        timeout = max(float(os.getenv("MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", "5")), 0)
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
    dashboard = web_ui.get("dashboard") if isinstance(web_ui.get("dashboard"), dict) else {}
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = config.get("identity") if isinstance(config.get("identity"), dict) else {}
    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter not in {"static_html", "html"} and not (output.get("path") or dashboard.get("path") or web_ui.get("path")):
        return

    html_path = _safe_bundle_file(bundle_dir, output.get("path") or dashboard.get("path") or web_ui.get("path"))
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
            "title": str(output.get("title") or identity.get("name") or bundle_dir.name),
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
        (run_dir / "web_ui.json").write_text(json.dumps(handle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError:
        logger.exception("Failed to write blueprint web UI handle for run_dir=%s", run_dir)


def _web_ui_video_source(config: dict[str, Any], bundle_dir: Path) -> str:
    video_source = ((config.get("video_source") or {}) if isinstance(config.get("video_source"), dict) else {}).get("uri")
    if not isinstance(video_source, str) or not video_source:
        dashboard = (config.get("web_ui") or {}).get("dashboard") if isinstance(config.get("web_ui"), dict) else {}
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
                        if key.lower() == 'q' or key == '\x03': # \x03 is Ctrl-C
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
        handle_cli_error(e, console, 'monitor stream')


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
            console.print("[yellow]No final result found (job might not be completed).[/yellow]")
            
        if stream_file.exists():
            console.print(f"[green]Stream results saved to: {stream_file}[/green]")
            
    except Exception as e:
        handle_cli_error(e, console, 'fetch results')
