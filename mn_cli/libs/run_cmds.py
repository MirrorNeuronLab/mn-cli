import typer
import json
import os
import time
import logging
from pathlib import Path
from typing import Annotated, Any, Optional
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from logging.handlers import RotatingFileHandler
from mn_cli.libs.ui import (
    generate_detached_panel,
    generate_live_layout,
    generate_run_submitted_panel,
    generate_summary_panel,
)
from mn_cli.shared import console, client, logger
from mn_cli.error_handler import handle_cli_error

STANDARD_EVENTS = {
    "init", "job_pending", "job_validated", "job_scheduled", "job_running",
    "job_completed", "job_failed", "job_paused", "job_resumed", "job_cancelled",
    "agent_recovery_started", "agent_recovered",
    "agent_message_received", "aggregator_received", "aggregator_duplicate_ignored",
    "executor_lease_requested", "executor_lease_acquired", "executor_lease_released",
    "sandbox_job_started", "sandbox_job_completed", "sandbox_job_failed",
    "node_up", "node_down"
}

FINAL_STATUSES = {"completed", "failed", "cancelled"}


class JobLogWriter:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.log_dir = Path(f"/tmp/mn_{job_id}")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.events_file = self.log_dir / "events.log"
        self.snapshot_file = self.log_dir / "job_snapshot.json"
        self.seen = set()
        self.web_ui_urls = set()
        self.event_count = 0
        self.max_bytes = int(
            os.getenv("MN_RUN_EVENT_LOG_MAX_BYTES", str(10 * 1024 * 1024))
        )
        self.backup_count = int(os.getenv("MN_RUN_EVENT_LOG_BACKUP_COUNT", "5"))
        self.run_logger = self._build_run_logger()

    def _build_run_logger(self) -> logging.Logger:
        run_logger = logging.getLogger(f"mn-cli.run.{self.job_id}")
        run_logger.setLevel(os.getenv("MN_RUN_LOG_LEVEL", "INFO").upper())
        run_logger.propagate = False

        if run_logger.handlers:
            return run_logger

        handler = RotatingFileHandler(
            self.log_dir / "run.log",
            maxBytes=int(os.getenv("MN_RUN_LOG_MAX_BYTES", str(2 * 1024 * 1024))),
            backupCount=int(os.getenv("MN_RUN_LOG_BACKUP_COUNT", "5")),
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        run_logger.addHandler(handler)
        return run_logger

    def write_event_json(self, event_json: str) -> bool:
        try:
            event = json.loads(event_json)
        except Exception:
            self.run_logger.warning("Skipping invalid event JSON: %r", event_json)
            return False
        return self.write_event(event)

    def write_event(self, event: dict) -> bool:
        key = self._event_key(event)
        if key in self.seen:
            return False

        self.seen.add(key)
        self._rotate_if_needed()
        with open(self.events_file, "a") as f:
            f.write(json.dumps(event, sort_keys=True) + "\n")
        self.event_count += 1

        event_type = event.get("type", "unknown")
        if event_type in {"slow_event_processed", "stream_metrics_updated"}:
            payload = event.get("payload", {})
            self.run_logger.info(
                "slow_agent_event=%s agent=%s payload=%s",
                event_type,
                event.get("agent_id") or payload.get("worker") or event.get("node"),
                json.dumps(payload, sort_keys=True),
            )
        elif event_type in {"backpressure_state", "external_input_rejected"}:
            self.run_logger.info(
                "backpressure_event=%s agent=%s payload=%s",
                event_type,
                event.get("agent_id"),
                json.dumps(event.get("payload", {}), sort_keys=True),
            )
        elif event_type in {"job_failed", "sandbox_job_failed"}:
            self.run_logger.error(
                "event=%s payload=%s", event_type, json.dumps(event, sort_keys=True)
            )
        elif event_type not in STANDARD_EVENTS:
            self.run_logger.info(
                "custom_event=%s payload=%s",
                event_type,
                json.dumps(event, sort_keys=True),
            )
        else:
            self.run_logger.info("event=%s", event_type)
        return True

    def write_snapshot(self, data: dict):
        with open(self.snapshot_file, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def _rotate_if_needed(self):
        if not self.events_file.exists() or self.events_file.stat().st_size < self.max_bytes:
            return

        for index in range(self.backup_count - 1, 0, -1):
            src = self.log_dir / f"events.log.{index}"
            dst = self.log_dir / f"events.log.{index + 1}"
            if src.exists():
                if dst.exists():
                    dst.unlink()
                src.rename(dst)

        first_backup = self.log_dir / "events.log.1"
        if first_backup.exists():
            first_backup.unlink()
        self.events_file.rename(first_backup)

    @staticmethod
    def _event_key(event: dict):
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        return (
            event.get("timestamp"),
            event.get("type"),
            event.get("agent_id"),
            event.get("node"),
            event.get("message_id") or payload.get("message_id"),
        )

    def record_web_ui_url(self, event: dict) -> Optional[str]:
        url = _extract_web_ui_url(event)
        if not url or url in self.web_ui_urls:
            return None
        self.web_ui_urls.add(url)
        return url


def _extract_web_ui_url(event: dict) -> Optional[str]:
    payload = event.get("payload") if isinstance(event, dict) else None
    if not isinstance(payload, dict):
        payload = event if isinstance(event, dict) else {}
    web_ui = payload.get("web_ui") if isinstance(payload.get("web_ui"), dict) else payload
    url = web_ui.get("url") or web_ui.get("web_ui_url") or web_ui.get("local_url")
    return str(url) if url else None


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
):
    log_writer = log_writer or JobLogWriter(job_id)
    log_dir = log_writer.log_dir
    follow_seconds = (
        float(os.getenv("MN_RUN_DETACH_LOG_SECONDS", "30"))
        if follow_seconds is None
        else follow_seconds
    )
    
    status_text = "Unknown / Detached"
    status_color = "yellow"
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
                        status_color = "green"
                        break
                    elif event_type == "job_failed":
                        progress.update(job_task, description="[red]Job failed.")
                        status_text = "Failed"
                        status_color = "red"
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
        
    except KeyboardInterrupt:
        console.print("[yellow]Detached from log stream.[/yellow]")
        status, _data = _follow_job_events(job_id, log_writer, 0)
        console.print(generate_detached_panel(job_id, log_dir, status, log_writer.event_count))


def _write_result_stream_event(log_dir: Path, event: dict):
    if event.get("type") in STANDARD_EVENTS:
        return
    payload = event.get("payload", event)
    _materialize_sent_email_copy(log_dir, payload)
    with open(log_dir / "result_stream.txt", "a") as f_stream:
        f_stream.write(json.dumps(payload, sort_keys=True) + "\n")


def _materialize_sent_email_copy(log_dir: Path, payload: dict):
    if not isinstance(payload, dict):
        return
    sent_copy = payload.get("sent_email_copy")
    if not isinstance(sent_copy, dict):
        return
    html_content = sent_copy.get("html_content")
    text_content = sent_copy.get("text_content")
    metadata = sent_copy.get("metadata")
    if html_content is None and text_content is None and not isinstance(metadata, dict):
        return

    email_dir = log_dir / "sent_emails"
    email_dir.mkdir(parents=True, exist_ok=True)

    def resolve_path(raw_path: Optional[str], suffix: str) -> Path:
        if raw_path:
            return email_dir / Path(raw_path).name
        stem = str(payload.get("provider_id") or payload.get("subject") or time.time_ns())
        safe_stem = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in stem)[:96]
        return email_dir / f"{safe_stem or time.time_ns()}.{suffix}"

    html_path = resolve_path(sent_copy.get("html_path"), "html")
    text_path = resolve_path(sent_copy.get("text_path"), "txt")
    metadata_path = resolve_path(sent_copy.get("metadata_path"), "json")

    if html_content is not None:
        html_path.write_text(str(html_content), encoding="utf-8")
    if text_content is not None:
        text_path.write_text(str(text_content), encoding="utf-8")
    if isinstance(metadata, dict):
        host_metadata = {
            **metadata,
            "host_html_path": str(html_path),
            "host_text_path": str(text_path),
            "host_metadata_path": str(metadata_path),
        }
        metadata_path.write_text(json.dumps(host_metadata, indent=2, sort_keys=True), encoding="utf-8")


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

        console.print(f"[green]✓ Job bundle at '{bundle_path}' is valid.[/green]")
        console.print(f"  - Job Name: {manifest.get('job_name')}")
        console.print(f"  - Graph ID: {manifest.get('graph_id')}")
        console.print(f"  - Nodes count: {len(manifest.get('nodes'))}")
        
    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, 'validate')
        raise typer.Exit(1)


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
):
    """Run a bundle after applying optional runtime metadata and environment."""
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
                
        manifest_dict = prepare_manifest_for_submission(
            bundle_dir,
            manifest_dict,
            env_overrides=env_overrides,
            submission_metadata=submission_metadata,
        )
        manifest = json.dumps(manifest_dict)

        payloads = {}
        payloads_dir = bundle_dir / "payloads"
        if payloads_dir.is_dir():
            for filepath in payloads_dir.rglob("*"):
                if filepath.is_file():
                    rel_path = filepath.relative_to(payloads_dir).as_posix()
                    with open(filepath, "rb") as f:
                        payloads[rel_path] = f.read()

        job_id = client.submit_job(manifest, payloads)
        log_writer = JobLogWriter(job_id)
        blueprint_run_id = (submission_metadata or {}).get("blueprint_run_id") or (env_overrides or {}).get("MN_RUN_ID")
        if blueprint_run_id:
            _write_blueprint_job_mapping(blueprint_run_id, job_id, submission_metadata or {})
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
                blueprint_revision=(submission_metadata or {}).get("blueprint_revision"),
            )
        )
        _stream_and_format_events(job_id, log_writer, resolved_follow_seconds)
    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, 'run bundle')
        raise typer.Exit(1)


def prepare_manifest_for_submission(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    prepared = json.loads(json.dumps(manifest_dict))
    runtime_env = _blueprint_runtime_environment(bundle_dir)
    runtime_env.update({key: str(value) for key, value in (env_overrides or {}).items() if value is not None})
    if runtime_env:
        _inject_node_environment(prepared, runtime_env)
    metadata = dict(submission_metadata or {})
    if metadata:
        prepared.setdefault("metadata", {}).setdefault("mn_cli", {}).update(metadata)
    return prepared


def _blueprint_runtime_environment(bundle_dir: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for filename, env_name in (
        ("config/default.json", "MN_BLUEPRINT_CONFIG_JSON"),
        ("scenario.json", "MN_BLUEPRINT_SCENARIO_JSON"),
        ("product.json", "MN_BLUEPRINT_PRODUCT_JSON"),
    ):
        path = bundle_dir / filename
        if path.exists():
            env[env_name] = path.read_text(encoding="utf-8")
    return env


def _inject_node_environment(manifest: dict[str, Any], env: dict[str, str]) -> None:
    for node in manifest.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        config = node.setdefault("config", {})
        if not isinstance(config, dict):
            continue
        environment = config.setdefault("environment", {})
        if not isinstance(environment, dict):
            continue
        environment.update(env)
        _add_mn_llm_aliases(environment)


def _add_mn_llm_aliases(environment: dict[str, Any]) -> None:
    for legacy, primary in (
        ("LITELLM_MODEL", "MN_LLM_MODEL"),
        ("LITELLM_API_BASE", "MN_LLM_API_BASE"),
        ("LITELLM_API_KEY", "MN_LLM_API_KEY"),
        ("LITELLM_TIMEOUT_SECONDS", "MN_LLM_TIMEOUT_SECONDS"),
        ("LITELLM_MAX_TOKENS", "MN_LLM_MAX_TOKENS"),
        ("LITELLM_NUM_RETRIES", "MN_LLM_NUM_RETRIES"),
        ("LITELLM_RETRY_BACKOFF_SECONDS", "MN_LLM_RETRY_BACKOFF_SECONDS"),
    ):
        if primary not in environment and legacy in environment:
            environment[primary] = environment[legacy]


def _write_blueprint_job_mapping(blueprint_run_id: str, job_id: str, metadata: dict[str, Any]) -> None:
    run_dir = Path(os.getenv("MN_RUNS_ROOT", "~/.mn/runs")).expanduser() / blueprint_run_id
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": blueprint_run_id,
            "job_id": job_id,
            "blueprint_revision": metadata.get("blueprint_revision"),
            "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        tmp = run_dir / f".job.json.{os.getpid()}.tmp"
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(run_dir / "job.json")
    except OSError:
        logger.exception("Failed to write blueprint job mapping for run_id=%s job_id=%s", blueprint_run_id, job_id)


def _run_mode_label(manifest: dict) -> str:
    is_live = manifest.get("daemon") is True or manifest.get("policies", {}).get("stream_mode") == "live"
    if is_live and manifest.get("daemon") is True:
        return "Live daemon"
    if is_live:
        return "Live"
    return "Batch"
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
        with Live(view, refresh_per_second=12, console=console) as live:
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
            console.print(f"[yellow]No final result found (job might not be completed).[/yellow]")
            
        if stream_file.exists():
            console.print(f"[green]Stream results saved to: {stream_file}[/green]")
            
    except Exception as e:
        handle_cli_error(e, console, 'fetch results')
