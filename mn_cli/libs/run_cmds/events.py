from .common import *
from .live import *

def _stream_and_format_events(
    job_id: str,
    log_writer: Optional[JobLogWriter] = None,
    follow_seconds: Optional[float] = None,
    web_ui_url: Optional[str] = None,
    manifest: Optional[dict[str, Any]] = None,
) -> str:
    if manifest is not None and _is_workflow_manifest(manifest):
        return _stream_and_format_workflow_events(
            job_id,
            manifest,
            log_writer=log_writer,
            follow_seconds=follow_seconds,
            web_ui_url=web_ui_url,
        )
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
    stage_map = {
        "job_pending": 12,
        "job_validated": 25,
        "job_scheduled": 50,
        "job_running": 62,
        "job_completed": 100,
        "job_failed": 100,
        "job_cancelled": 100,
    }

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
            disable=not use_progress(),
        ) as progress:
            job_task = progress.add_task("[cyan]Submitting job bundle...", total=100)

            try:
                for event_json in client.stream_events(job_id, follow=True, timeout=None, heartbeat_interval_ms=5000):
                    try:
                        event = json.loads(event_json)
                        event_type = event.get("type")
                        if event_type == "stream_heartbeat":
                            continue
                        log_writer.write_event_json(event_json)

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
                                completed=stage_map.get("job_pending", 0),
                            )
                        elif event_type == "job_validated":
                            progress.update(
                                job_task,
                                description="[cyan]Preparing: manifest validated, scheduling agents...",
                                completed=stage_map.get("job_validated", 0),
                            )
                        elif event_type == "job_scheduled":
                            progress.update(
                                job_task,
                                description="[cyan]Starting: agents scheduled, waiting for runtime to report running...",
                                completed=stage_map.get("job_scheduled", 0),
                            )
                        elif event_type == "job_running":
                            progress.update(
                                job_task,
                                description="[green]Running: streaming live job events...",
                                completed=stage_map.get("job_running", 0),
                            )
                        elif event_type in [
                            "agent_message_received",
                            "aggregator_received",
                        ]:
                            msg_count += 1
                            progress.update(
                                job_task,
                                description=f"[green]Running: {msg_count} routed messages, {log_writer.event_count} events logged...",
                                completed=min(
                                    stage_map.get("job_running", 62) + (log_writer.event_count % 35),
                                    90,
                                ),
                            )
                        elif event_type == "job_completed":
                            result = event.get("result")
                            if result is not None:
                                with open(log_dir / "result.txt", "w") as f_res:
                                    json.dump(result, f_res, indent=2)

                            progress.update(
                                job_task,
                                description="[green]Completed successfully.",
                                completed=stage_map.get("job_completed", 100),
                            )
                            status_text = "Success"
                            break
                        elif event_type == "job_failed":
                            progress.update(
                                job_task,
                                description="[red]Job failed.",
                                completed=stage_map.get("job_failed", 100),
                            )
                            status_text = "Failed"
                            break
                        elif event_type == "job_cancelled":
                            progress.update(
                                job_task,
                                description="[red]Job cancelled.",
                                completed=stage_map.get("job_cancelled", 100),
                            )
                            status_text = "Cancelled"
                            break
                        else:
                            progress.update(
                                job_task,
                                description=f"[cyan]Observing: latest event {event_type}, {log_writer.event_count} events logged...",
                                completed=stage_map.get("job_running", 62),
                            )
                    except Exception:
                        log_writer.run_logger.exception("Failed to process streamed event")
            except Exception:
                log_writer.run_logger.exception(
                    "Job event stream failed; falling back to status polling"
                )

        terminal_status = {
            "Success": "completed",
            "Failed": "failed",
            "Cancelled": "cancelled",
        }.get(status_text)
        if terminal_status:
            panel = generate_summary_panel(
                job_id=job_id,
                status=terminal_status,
                log_dir=log_dir,
            )
            console.print(panel)
        else:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TimeElapsedColumn(),
                console=console,
                disable=not use_progress(),
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

    except (KeyboardInterrupt, EOFError):
        console.print(f"[yellow]{DETACHED_AFTER_INTERRUPT_MESSAGE}[/yellow]")
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
    if status_text == "Cancelled":
        return "cancelled"
    return str(status_text).lower()

def _stream_and_format_workflow_events(
    job_id: str,
    manifest: dict[str, Any],
    *,
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
    view = BlueprintWorkflowProgress(manifest, job_id=job_id)
    monitor_state = JobMonitorState()
    view.set_monitor_state(monitor_state)
    progress_stream = ProgressSnapshotStream(view)
    status_text = "running"
    live: Live | None = None
    is_tty = _interactive_live_output()
    old_settings = None
    if is_tty:
        import select
        import termios
        import tty

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setcbreak(fd)
        select_module = select
    else:
        select_module = None

    detached = False

    def process_event(event_json: str) -> bool:
        nonlocal status_text, web_ui_url

        event = json.loads(event_json)
        if event.get("type") == "stream_heartbeat":
            return False

        log_writer.write_event_json(event_json)
        _write_result_stream_event(log_dir, event)
        web_ui_url = log_writer.record_web_ui_url(event)
        if web_ui_url:
            view._remember(f"Blueprint Web UI: {web_ui_url}")
        view.record_event_token_usage(event)
        should_render = progress_stream.observe_event(event)
        if live is not None and should_render:
            live.update(view.render())

        event_type = event.get("type")
        if event_type == "job_completed":
            result = event.get("result")
            if result is not None:
                with open(log_dir / "result.txt", "w") as f_res:
                    json.dump(result, f_res, indent=2)
            status_text = "completed"
        elif event_type == "job_failed":
            status_text = "failed"
        elif event_type == "job_cancelled":
            status_text = "cancelled"
        return event_type in {"job_completed", "job_failed", "job_cancelled"}

    try:
        if is_tty:
            live = Live(
                view.render(),
                console=console,
                refresh_per_second=6,
                transient=True,
                screen=bool(is_tty and getattr(console, "is_terminal", False)),
            )
            live.start()
        try:
            if is_tty:
                # The SDK stream blocks while a job is quiet. Read terminal
                # input on the main thread while consuming the stream in a
                # daemon worker so navigation and detach stay responsive.
                import queue
                import threading

                event_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
                stop_stream = threading.Event()
                stream = iter(
                    client.stream_events(
                        job_id,
                        follow=True,
                        timeout=None,
                        heartbeat_interval_ms=5000,
                    )
                )

                def consume_stream() -> None:
                    try:
                        for event_json in stream:
                            if stop_stream.is_set():
                                break
                            event_queue.put(("event", event_json))
                    except BaseException as exc:
                        if not stop_stream.is_set():
                            event_queue.put(("error", exc))
                    finally:
                        event_queue.put(("done", None))

                stream_worker = threading.Thread(target=consume_stream, daemon=True)
                stream_worker.start()
                try:
                    stream_finished = False
                    while not stream_finished and status_text not in FINAL_STATUSES:
                        if not _handle_live_workflow_key(
                            monitor_state,
                            {"workflow_progress": view.snapshot()},
                            select_module=select_module,
                            is_tty=True,
                            block_seconds=0.05,
                        ):
                            detached = True
                            break

                        try:
                            kind, payload = event_queue.get_nowait()
                        except queue.Empty:
                            if live is not None and progress_stream.flush_due():
                                live.update(view.render())
                            continue

                        if kind == "error":
                            log_writer.run_logger.warning(
                                "Workflow event stream failed; falling back to status polling: %s",
                                payload,
                            )
                            stream_finished = True
                        elif kind == "done":
                            stream_finished = True
                        elif kind == "event":
                            try:
                                process_event(str(payload))
                            except Exception:
                                log_writer.run_logger.exception("Failed to process streamed event")
                            if live is not None and progress_stream.flush_due():
                                live.update(view.render())
                finally:
                    stop_stream.set()
                    close_stream = getattr(stream, "close", None)
                    if callable(close_stream):
                        try:
                            close_stream()
                        except Exception:
                            pass
            else:
                try:
                    for event_json in client.stream_events(
                        job_id,
                        follow=True,
                        timeout=None,
                        heartbeat_interval_ms=5000,
                    ):
                        try:
                            if process_event(event_json):
                                break
                        except Exception:
                            log_writer.run_logger.exception("Failed to process streamed event")
                        if live is not None and progress_stream.flush_due():
                            live.update(view.render())
                except Exception:
                    log_writer.run_logger.exception(
                        "Workflow event stream failed; falling back to status polling"
                    )

            if detached:
                status_text, _data = _follow_job_events(job_id, log_writer, 0)
            elif status_text not in FINAL_STATUSES:
                status_text = _follow_workflow_job_events(
                    job_id,
                    log_writer,
                    follow_seconds,
                    view,
                    live,
                    monitor_state=monitor_state if is_tty else None,
                )
        finally:
            if live is not None:
                live.stop()
            if old_settings is not None:
                import termios

                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_settings)
    except (KeyboardInterrupt, EOFError):
        console.print(f"[yellow]{DETACHED_AFTER_INTERRUPT_MESSAGE}[/yellow]")
        status_text, _data = _follow_job_events(job_id, log_writer, 0)

    if not view.has_token_usage():
        _attach_workflow_resource_tokens(view, job_id)

    if live is None:
        console.print(view.render())

    if status_text in FINAL_STATUSES:
        console.print(
            generate_summary_panel(
                job_id=job_id,
                status=status_text,
                log_dir=log_dir,
            )
        )
    else:
        console.print(
            generate_detached_panel(
                job_id,
                log_dir,
                status_text,
                log_writer.event_count,
                web_ui_url=log_writer.web_ui_url,
            )
        )
    return status_text

def _follow_workflow_job_events(
    job_id: str,
    log_writer: JobLogWriter,
    follow_seconds: float,
    view: BlueprintWorkflowProgress,
    live: Live | None,
    monitor_state: JobMonitorState | None = None,
) -> str:
    started = time.monotonic()
    last_status, data = _follow_job_events(job_id, log_writer, follow_seconds)
    if isinstance(data, dict):
        for event in reversed(data.get("recent_events", [])):
            if isinstance(event, dict):
                view.record_event_token_usage(event)
                view.update(event)
    remaining = max(follow_seconds - (time.monotonic() - started), 0)
    view.update_follow_status(last_status, log_writer.event_count, remaining)
    if live is not None:
        live.update(view.render())
    return last_status

def _extract_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def _run_id_from_payload(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in ("run_id", "runId"):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate:
                return candidate
        nested = payload.get("job") or payload.get("summary") or payload.get("metadata") or payload.get("manifest") or payload.get("payload")
        if isinstance(nested, (dict, list)):
            return _run_id_from_payload(nested)
        if isinstance(payload.get("recent_events"), list):
            for event in payload.get("recent_events"):
                if isinstance(event, dict):
                    candidate = _run_id_from_payload(event)
                    if candidate:
                        return candidate
    if isinstance(payload, list):
        for item in payload:
            candidate = _run_id_from_payload(item)
            if candidate:
                return candidate
    return None

def _read_workflow_resource_tokens(run_id: str) -> int | None:
    try:
        resource_reader = load_observability_tools().get("read_run_resources")
        if not callable(resource_reader):
            return None
        usage = resource_reader(run_id, runs_root=default_runs_root())
    except Exception:
        return None
    if not isinstance(usage, dict):
        return None
    llm_usage = usage.get("llm")
    if isinstance(llm_usage, dict):
        total = _extract_int(llm_usage.get("total_tokens"))
        if total is not None:
            return total
        input_tokens = _extract_int(llm_usage.get("input_tokens"))
        output_tokens = _extract_int(llm_usage.get("output_tokens"))
        if input_tokens is not None or output_tokens is not None:
            return (input_tokens or 0) + (output_tokens or 0)
    total = _extract_int(usage.get("total_tokens"))
    return total

def _attach_workflow_resource_tokens(view: BlueprintWorkflowProgress, job_id: str) -> None:
    try:
        job_json = client.get_job(job_id)
        payload = json.loads(job_json)
        run_id = _run_id_from_payload(payload)
    except Exception:
        run_id = None
    if not run_id:
        return
    resource_tokens = _read_workflow_resource_tokens(run_id)
    if resource_tokens is not None:
        view.set_resource_token_total(resource_tokens)

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


__all__ = [name for name in globals() if not name.startswith("__")]
