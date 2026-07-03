from .common import *

def _interactive_live_output() -> bool:
    if os.getenv("MN_RUN_DISABLE_LIVE_SCREEN", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False
    return bool(getattr(console, "is_terminal", False) and sys.stdout.isatty())

def _workflow_progress_agent_count(data: dict[str, Any] | None) -> int:
    progress = data.get("workflow_progress") if isinstance(data, dict) else None
    if not isinstance(progress, dict):
        return 0
    current_step_ids = {
        str(step_id)
        for step_id in progress.get("current_step_ids", [])
        if str(step_id).strip()
    }
    step = progress.get("current_step") if isinstance(progress.get("current_step"), dict) else {}
    if isinstance(step, dict):
        step_id = str(step.get("id") or "")
        if step_id:
            current_step_ids.add(step_id)
    steps = progress.get("steps") if isinstance(progress.get("steps"), list) else []
    if not current_step_ids:
        current_step = progress.get("current_step") if isinstance(progress.get("current_step"), dict) else {}
        current_agents = current_step.get("agents") if isinstance(current_step, dict) else []
        return len(current_agents) if isinstance(current_agents, list) else 0

    total_agents = 0
    for step_data in steps:
        if not isinstance(step_data, dict):
            continue
        if str(step_data.get("id") or "") not in current_step_ids:
            continue
        agents = step_data.get("agents")
        if isinstance(agents, list):
            total_agents += len(agents)
    return total_agents

def _workflow_layout_agent_count(data: dict[str, Any] | None) -> int:
    if not isinstance(data, dict):
        return 0
    if isinstance(data.get("workflow_progress"), dict):
        return _workflow_progress_agent_count(data)
    agents = data.get("agents")
    return len(agents) if isinstance(agents, list) else 0

def _handle_live_workflow_key(
    monitor_state: JobMonitorState,
    data: dict[str, Any] | None,
    *,
    is_tty: bool,
    select_module,
    block_seconds: float = 0.0,
) -> bool:
    if not is_tty:
        return True
    readable, _, _ = select_module.select([sys.stdin], [], [], block_seconds)
    if not readable:
        return True
    key = _read_monitor_key(sys.stdin, select_module)
    return monitor_state.handle_key(key, _workflow_layout_agent_count(data))

def _read_monitor_key(stream, select_module) -> str:
    key = stream.read(1)
    if key != "\x1b":
        return key
    parts = [key]
    while True:
        ready, _, _ = select_module.select([stream], [], [], 0.01)
        if not ready:
            break
        parts.append(stream.read(1))
    return "".join(parts)


__all__ = [name for name in globals() if not name.startswith("__")]
