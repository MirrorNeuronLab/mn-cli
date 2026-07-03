from ..common import *
from ..live import *
from ..outputs import *

def _workflow_progress_for_monitor(job_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
    manifest = _manifest_from_job_data(data)
    events: list[dict[str, Any]] = []
    try:
        for event_json in client.stream_events(job_id, follow=False):
            try:
                event = json.loads(event_json)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
    except Exception:
        logger.exception("Failed to load workflow events for monitor")
    try:
        return workflow_progress_snapshot(
            manifest,
            events,
            job=data.get("job") if isinstance(data.get("job"), dict) else {},
            summary=data.get("summary") if isinstance(data.get("summary"), dict) else {},
            job_id=job_id,
        )
    except Exception:
        logger.exception("Failed to build workflow progress for monitor")
        return None

def _manifest_from_job_data(data: dict[str, Any]) -> dict[str, Any]:
    job = data.get("job") if isinstance(data.get("job"), dict) else {}
    summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
    for candidate in (data.get("manifest"), job.get("manifest"), summary.get("manifest")):
        if isinstance(candidate, dict) and candidate:
            return candidate
    manifest_ref = job.get("manifest_ref") if isinstance(job.get("manifest_ref"), dict) else summary.get("manifest_ref")
    if isinstance(manifest_ref, dict):
        for raw_path in (
            manifest_ref.get("manifest_path"),
            Path(str(manifest_ref.get("job_path") or "")) / "manifest.json" if manifest_ref.get("job_path") else None,
        ):
            if not raw_path:
                continue
            try:
                path = Path(str(raw_path)).expanduser()
                if path.is_file():
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        return loaded
            except (OSError, json.JSONDecodeError):
                continue
    topology = job.get("runtime_topology") if isinstance(job.get("runtime_topology"), dict) else {}
    topology_nodes = topology.get("nodes") if isinstance(topology.get("nodes"), list) else []
    agents = topology_nodes or (data.get("agents") if isinstance(data.get("agents"), list) else [])
    nodes = []
    for index, agent in enumerate(agents):
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("agent_id") or agent.get("id") or agent.get("node_id") or f"agent_{index + 1}")
        nodes.append(
            {
                "node_id": agent_id,
                "agent_type": str(agent.get("agent_type") or agent.get("type") or "worker"),
                "role": str(agent.get("role") or agent.get("current_task") or agent.get("agent_type") or "worker"),
                "type": str(agent.get("node_type") or agent.get("type") or ""),
                "live": agent.get("live?", agent.get("live", False)),
                "config": {"llm_config": str(agent.get("model") or agent.get("llm_config") or "runtime")},
            }
        )
    job_type = str(job.get("job_type") or job.get("type") or summary.get("job_type") or summary.get("type") or "")
    policies = {"stream_mode": "live"} if job_type.lower() == "service" else {}
    return {
        "id": str(job.get("graph_id") or summary.get("graph_id") or job.get("job_id") or "job"),
        "name": str(job.get("job_name") or summary.get("job_name") or job.get("job_id") or "Job"),
        "description": str(summary.get("description") or job.get("description") or ""),
        "graph_id": str(job.get("graph_id") or summary.get("graph_id") or ""),
        "type": job_type,
        "job_type": job_type,
        "policies": policies,
        "nodes": nodes,
    }

def _live_monitor_api_stream(job_id: str) -> bool:
    api_base_url = str(getattr(config, "api_base_url", "") or "").strip()
    if not api_base_url or os.getenv("MN_JOB_MONITOR_DISABLE_API_STREAM", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False

    import queue
    import threading
    import select
    import sys

    event_queue: queue.Queue[tuple[str, Any]] = queue.Queue()

    def reader() -> None:
        try:
            for snapshot in stream_api_workflow_progress(
                api_base_url,
                job_id,
                api_token=str(getattr(config, "api_token", "") or ""),
                timeout=float(os.getenv("MN_JOB_MONITOR_API_STREAM_TIMEOUT", "10")),
            ):
                event_queue.put(("snapshot", snapshot))
                if str(snapshot.get("status") or "").lower() in FINAL_STATUSES:
                    break
        except Exception as exc:
            event_queue.put(("error", exc))
        finally:
            event_queue.put(("done", None))

    class MonitorView:
        def __init__(self, state: JobMonitorState):
            self.data: dict[str, Any] | None = None
            self.state = state

        def __rich__(self):
            if not self.data:
                from rich.panel import Panel

                return Panel("Connecting to workflow progress stream...", style="cyan")
            return generate_live_layout(job_id, self.data, state=self.state)

    monitor_state = JobMonitorState()
    view = MonitorView(monitor_state)
    worker = threading.Thread(target=reader, daemon=True)
    worker.start()
    is_tty = _interactive_live_output()
    old_settings = None
    if is_tty:
        import tty
        import termios

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setcbreak(fd)

    saw_snapshot = False
    try:
        with Live(
            view,
            refresh_per_second=12,
            console=console,
            screen=bool(is_tty and getattr(console, "is_terminal", False)),
            transient=bool(is_tty and getattr(console, "is_terminal", False)),
        ):
            while True:
                try:
                    kind, payload = event_queue.get(timeout=0.5)
                except queue.Empty:
                    if not _handle_live_workflow_key(
                        monitor_state,
                        view.data,
                        is_tty=is_tty,
                        select_module=select,
                    ):
                        return False
                    continue
                if kind == "error":
                    if not saw_snapshot:
                        return False
                    logger.warning("Workflow progress stream ended: %s", payload)
                    break
                if kind == "done":
                    break
                if kind == "snapshot" and isinstance(payload, dict):
                    saw_snapshot = True
                    view.data = {
                        "workflow_progress": payload,
                        "summary": {"status": payload.get("status")},
                        "job": {"job_id": job_id, "status": payload.get("status")},
                    }
                    if str(payload.get("status") or "").lower() in FINAL_STATUSES:
                        break
        return saw_snapshot
    finally:
        if is_tty and old_settings:
            import termios

            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_settings)

def _live_monitor(job_id: str):
    if _live_monitor_api_stream(job_id):
        return

    import sys
    import select
    import time
    from rich.live import Live

    is_tty = _interactive_live_output()
    old_settings = None
    if is_tty:
        import tty
        import termios

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setcbreak(fd)

    class MonitorView:
        def __init__(self, state: JobMonitorState):
            self.data = None
            self.state = state

        def __rich__(self):
            if not self.data:
                from rich.panel import Panel

                return Panel("Connecting...", style="cyan")
            if "error" in self.data:
                from rich.panel import Panel

                return Panel(f"Error fetching job: {self.data['error']}", style="red")
            return generate_live_layout(job_id, self.data, state=self.state)

    final_status = "unknown"
    data = None
    monitor_state = JobMonitorState()
    view = MonitorView(monitor_state)

    try:
        with Live(
            view,
            refresh_per_second=12,
            console=console,
            screen=bool(is_tty and getattr(console, "is_terminal", False)),
            transient=bool(is_tty and getattr(console, "is_terminal", False)),
        ):
            while True:
                try:
                    job_json = client.get_job(job_id)
                    data = json.loads(job_json)
                    data["workflow_progress"] = _workflow_progress_for_monitor(job_id, data)
                except Exception as e:
                    data = {"error": str(e)}

                view.data = data

                if data and "error" not in data:
                    status = data.get("summary", {}).get("status", "unknown")
                    if status in ["completed", "failed", "cancelled"]:
                        final_status = status
                        break

                if not _handle_live_workflow_key(
                    monitor_state,
                    data,
                    is_tty=is_tty,
                    select_module=select,
                    block_seconds=0.5,
                ):
                    break
                if not is_tty:
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


__all__ = [name for name in globals() if not name.startswith("__")]
