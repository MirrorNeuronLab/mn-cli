from __future__ import annotations

import json
import os
import time
import textwrap

from rich import box
from rich.console import Group
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
import typer
from collections.abc import Iterable, Mapping
from typing import Dict, Any, Optional, Union

from mn_cli.terminal import truncate_for_width, ui_width


ConfirmationDetails = Union[Mapping[str, Any], Iterable[tuple[str, Any]]]


class JobMonitorState:
    def __init__(self, allow_ctrl_d: bool = True) -> None:
        self.selected_index = 0
        self.detail_mode = False
        self.allow_ctrl_d = allow_ctrl_d

    def handle_key(self, key: str, agent_count: int) -> bool:
        if key in {"q", "Q", "\x03"}:
            return False
        if key == "\x04":
            return not self.allow_ctrl_d
        if key in {"j", "J", "\t", "\x1b[B"}:
            self.selected_index = min(self.selected_index + 1, max(agent_count - 1, 0))
            return True
        if key in {"k", "K", "\x1b[A"}:
            self.selected_index = max(self.selected_index - 1, 0)
            return True
        if key in {"d", "D", "\r", "\n"}:
            self.detail_mode = True
            return True
        if key in {"o", "O", "\x1b"}:
            self.detail_mode = False
            return True
        if key.isdigit() and key != "0":
            self.selected_index = min(int(key) - 1, max(agent_count - 1, 0))
            return True
        return True

    def clamp(self, agent_count: int) -> None:
        self.selected_index = min(self.selected_index, max(agent_count - 1, 0))


def _monitor_footer_text(state: Optional[JobMonitorState] = None) -> str:
    allow_ctrl_d = True
    if state is not None:
        allow_ctrl_d = bool(getattr(state, "allow_ctrl_d", True))
    detach_text = "q or Ctrl+C detach" if not allow_ctrl_d else "q or Ctrl+D/Ctrl+C detach"
    return f"keys: j/k or arrows select agent, Enter/d details, 1-9 jump, o overview, {detach_text}"


def print_success_confirmation(
    console,
    action: str,
    *,
    status: Any = None,
    details: ConfirmationDetails | None = None,
    next_steps: str | Iterable[str] | None = None,
) -> None:
    print_confirmation(
        console,
        action,
        verb="successful",
        status=status,
        details=details,
        next_steps=next_steps,
    )


def print_confirmed(
    console,
    action: str,
    *,
    status: Any = None,
    details: ConfirmationDetails | None = None,
    next_steps: str | Iterable[str] | None = None,
) -> None:
    print_confirmation(
        console,
        action,
        verb="confirmed",
        status=status,
        details=details,
        next_steps=next_steps,
    )


def print_confirmation(
    console,
    action: str,
    *,
    verb: str = "successful",
    status: Any = None,
    details: ConfirmationDetails | None = None,
    next_steps: str | Iterable[str] | None = None,
) -> None:
    if _is_plain_confirmation_mode():
        _print_confirmation_plain(console, action, verb=verb, status=status, details=details, next_steps=next_steps)
        return

    rows: list[tuple[str, Any]] = []
    if _present(status):
        rows.append(("Status", status))
    rows.extend(_confirmation_detail_items(details))
    for next_step in _confirmation_next_steps(next_steps):
        rows.append(("Next", next_step))

    console.print(f"[green]{action} {verb}.[/green]")
    if not rows:
        return
    console.print(_confirmation_panel(rows))


def _is_plain_confirmation_mode() -> bool:
    return os.getenv("MN_CLI_OUTPUT", "").lower() == "plain"


def _print_confirmation_plain(
    console,
    action: str,
    *,
    verb: str,
    status: Any,
    details: ConfirmationDetails | None,
    next_steps: str | Iterable[str] | None,
) -> None:
    lines = [f"{action} {verb}."]
    if _present(status):
        lines.append(f"Status: {status}")
    for label, value in _confirmation_detail_items(details):
        lines.append(f"{label}: {value}")
    for next_step in _confirmation_next_steps(next_steps):
        lines.append(f"Next: {next_step}")

    output = getattr(console, "file", None)
    width = ui_width()
    for line in lines:
        wrapped = textwrap.wrap(
            str(line),
            width=width,
            break_long_words=True,
            break_on_hyphens=False,
        ) or [""]
        for item in wrapped:
            if output is None:
                typer.secho(item, color=None)
            else:
                typer.secho(item, color=None, file=output)


def _confirmation_panel(rows: list[tuple[str, Any]]) -> Panel:
    label_width = min(max(len(str(label)) + 1 for label, _ in rows), max(12, ui_width() // 3))
    table = Table(
        show_header=False,
        box=box.SIMPLE,
        show_lines=False,
        padding=(0, 1),
        expand=True,
    )
    table.add_column("Field", min_width=label_width, no_wrap=True, overflow="ellipsis")
    table.add_column("Value", overflow="fold", ratio=1, no_wrap=False)
    for label, value in rows:
        table.add_row(f"{label}:", _confirmation_value(value))
    return Panel(table, border_style="green", title="Details", title_align="left")


def _confirmation_value(value: Any) -> str:
    return truncate_for_width(value, ui_width() - 12)


def _confirmation_detail_items(details: ConfirmationDetails | None) -> list[tuple[str, Any]]:
    if not details:
        return []
    raw_items = details.items() if isinstance(details, Mapping) else details
    items: list[tuple[str, Any]] = []
    for label, value in raw_items:
        if not _present(label) or not _present(value):
            continue
        items.append((str(label), value))
    return items


def _confirmation_next_steps(next_steps: str | Iterable[str] | None) -> list[str]:
    if not next_steps:
        return []
    if isinstance(next_steps, str):
        candidates = [next_steps]
    else:
        candidates = [str(step) for step in next_steps]
    return [step for step in candidates if step.strip()]


def _present(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def generate_live_layout(job_id: str, data: Dict[str, Any], state: Optional[JobMonitorState] = None) -> Panel:
    workflow_progress = data.get("workflow_progress")
    if isinstance(workflow_progress, dict) and workflow_progress.get("steps"):
        return generate_workflow_progress_layout(job_id, workflow_progress, state=state)

    summary = data.get("summary", {})
    job = data.get("job", {})
    agents = _sorted_agents(data.get("agents", []))
    state = state or JobMonitorState()
    state.clamp(len(agents))

    status = str(summary.get("status") or job.get("status") or "unknown")
    color = _status_color(status)
    selected_agent = agents[state.selected_index] if agents else None
    total_agents = len(agents)
    completed_agents = sum(1 for agent in agents if _is_terminal_status(agent.get("status")))
    elapsed_label = _elapsed_label(summary, job)
    spinner = _spinner(status)

    header = Table.grid(expand=True)
    header.add_column(ratio=2)
    header.add_column(justify="right")
    title = Text(str(job.get("job_name") or job.get("name") or job_id), style="bold bright_blue")
    meta = Text(
        f"{completed_agents}/{total_agents} agents  |  {elapsed_label}  |  {status}",
        style=f"bold {color}",
    )
    header.add_row(title, meta)

    subtitle = Text(
        f"Job {job_id}  |  Workflow {job.get('graph_id', 'N/A')}  |  Live {summary.get('live?', False)}  {spinner}",
        style="dim",
    )

    body = Table.grid(expand=True)
    body.add_column(ratio=1)
    body.add_column(ratio=3)
    body.add_row(
        _job_phase_table(summary, job, agents),
        _agent_detail_panel(selected_agent) if state.detail_mode else _agent_table(agents, state.selected_index),
    )

    footer = Text(_monitor_footer_text(state), style="dim")
    last_event = summary.get("last_event")
    if last_event:
        footer.append(f"\nlatest event: {last_event}", style="dim")

    return Panel(
        Group(header, subtitle, body, footer),
        title=f"Live Job Monitor  {job_id}",
        border_style=color,
        box=box.ROUNDED,
    )


def generate_workflow_progress_layout(
    job_id: str,
    progress: dict[str, Any],
    *,
    state: Optional[JobMonitorState] = None,
) -> Panel:
    state = state or JobMonitorState()
    steps = [step for step in progress.get("steps", []) if isinstance(step, dict)]
    current_step_ids = [
        str(step_id)
        for step_id in progress.get("current_step_ids", [])
        if step_id not in (None, "")
    ]
    current_step = progress.get("current_step") if isinstance(progress.get("current_step"), dict) else None
    if current_step is None:
        current_step = next((step for step in steps if step.get("current")), steps[0] if steps else {})
    if current_step_ids:
        active_steps = [step for step in steps if str(step.get("id")) in set(current_step_ids)]
    else:
        active_steps = [current_step] if current_step else []
    agents = [
        agent
        for step in active_steps
        for agent in (step.get("agents", []) if isinstance(step, dict) else [])
        if isinstance(agent, dict)
    ]
    state.clamp(len(agents))

    status = str(progress.get("status") or "unknown")
    color = _status_color(status)
    workflow_kind = str(progress.get("workflow_kind") or "batch").lower()
    shown_steps, total_steps = _workflow_summary_step_counts(steps, workflow_kind=workflow_kind)
    elapsed_label = _format_elapsed(progress.get("elapsed_seconds"))

    header = Table.grid(expand=True)
    header.add_column(ratio=2)
    header.add_column(justify="right")
    header.add_row(
        Text(str(progress.get("workflow_id") or progress.get("name") or job_id), style="bold bright_blue"),
        Text(f"{shown_steps}/{total_steps} steps  |  {elapsed_label}  |  {status}", style=f"bold {color}"),
    )

    subtitle = Text(str(progress.get("description") or f"Job {job_id}"), style="dim")

    body = Table.grid(expand=True)
    body.add_column(ratio=1)
    body.add_column(ratio=3)
    agent_title = "Agents"
    if active_steps:
        first_step = active_steps[0]
        if isinstance(first_step, dict):
            agent_title = str(first_step.get("label") or first_step.get("id") or "Agents")
    body.add_row(
        _workflow_phase_table(steps, workflow_kind=workflow_kind),
        _agent_detail_panel(agents[state.selected_index] if agents and state.detail_mode else None)
        if state.detail_mode
        else _workflow_agent_table(agents, state.selected_index, title_prefix=agent_title),
    )

    footer = Text(_monitor_footer_text(state), style="dim")
    messages = [str(message) for message in progress.get("messages", []) if message]
    if messages:
        footer.append(f"\nlatest event: {messages[-1]}", style="dim")
    run_tokens = progress.get("resource_tokens")
    if run_tokens:
        footer.append(f"\nrun tokens (resource): {_format_tokens(run_tokens)}", style="dim")

    token_summary = _workflow_agent_token_summary(agents)
    if token_summary:
        footer.append(f"\n{token_summary}", style="dim")

    return Panel(
        Group(header, subtitle, body, footer),
        title=f"Workflow Job Monitor  {job_id}",
        border_style=color,
        box=box.ROUNDED,
    )


def _generate_workflow_progress_layout(job_id: str, progress: dict[str, Any], state: Optional[JobMonitorState] = None) -> Panel:
    # Backward-compatible wrapper for existing callsites.
    return generate_workflow_progress_layout(job_id, progress, state=state)


def _workflow_agent_token_summary(agents: list[dict[str, Any]]) -> str:
    total_used = 0
    total_budget = 0
    for agent in agents:
        tokens_used = _agent_token_count(agent, used=True)
        if tokens_used is not None:
            total_used += tokens_used
        token_budget = _agent_token_count(agent, budget=True)
        if token_budget is not None:
            total_budget += token_budget

    if total_used:
        if total_budget and total_budget != total_used:
            return f"run used {_format_tokens(total_used)} / budget {_format_tokens(total_budget)} tok"
        return f"run used {_format_tokens(total_used)} tok"
    if total_budget:
        return f"run budget {_format_tokens(total_budget)} tok"
    return ""


def _agent_token_count(agent: dict[str, Any], *, used: bool = False, budget: bool = False) -> int | None:
    if used:
        value = agent.get("tokens_used")
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass
        return None
    if budget:
        for key in ("token_budget",):
            value = agent.get(key)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    pass
    return None


def _workflow_summary_step_counts(
    steps: list[dict[str, Any]], *, workflow_kind: str = "batch"
) -> tuple[int, int]:
    total_steps = len(steps)
    if workflow_kind == "service":
        shown_steps = sum(
            1
            for step in steps
            if str(step.get("status") or "").lower() in {"done", "completed", "running", "idle"}
            or int(step.get("ready_count") or 0) > 0
        )
    else:
        shown_steps = sum(
            1
            for step in steps
            if str(step.get("status") or "").lower() in {"done", "completed"}
        )
    return shown_steps, total_steps


def _workflow_phase_table(steps: list[dict[str, Any]], *, workflow_kind: str = "batch") -> Table:
    table = Table(title="Phases", box=box.SIMPLE, show_header=False, expand=True)
    table.add_column("step")
    table.add_column("agents", justify="right", no_wrap=True)
    has_graph_layers = any("layer" in step or step.get("parents") or step.get("children") for step in steps)
    for index, step in enumerate(steps, start=1):
        status = str(step.get("status") or "unknown")
        current = bool(step.get("current"))
        icon = _status_icon("running" if current and status not in {"done", "completed"} else status)
        layer = int(step.get("layer") or 0)
        branch_prefix = f"L{layer + 1} " if has_graph_layers else ""
        label = f"{icon} {branch_prefix}{index} {step.get('label') or step.get('id') or 'Step'}"
        if workflow_kind == "service" and status not in {"done", "completed"}:
            label = f"{label} ({status})"
        ready_count = int(step.get("ready_count") or step.get("done_count") or 0)
        done_count = int(step.get("done_count") or 0)
        count_value = ready_count if workflow_kind == "service" else done_count
        count = f"{count_value}/{int(step.get('total_count') or 0)}"
        table.add_row(label, count, style="bright_blue" if current else _status_color(status))
    if not steps:
        table.add_row(". 1 Runtime", "0/0", style="cyan")
    return table


def _workflow_agent_table(
    agents: list[dict[str, Any]],
    selected_index: int,
    title_prefix: str = "Agents",
) -> Table:
    table = Table(title=f"{title_prefix}  |  {len(agents)} agents", box=box.SIMPLE, expand=True)
    table.add_column("#", justify="right", no_wrap=True)
    table.add_column("agent", no_wrap=True)
    table.add_column("working on")
    table.add_column("progress", justify="right", no_wrap=True, width=26)
    table.add_column("tokens", justify="right", no_wrap=True)
    table.add_column("mail", justify="right", no_wrap=True)

    if not agents:
        table.add_row("-", "none", "No agents reported by the runtime yet.", "-", "-", "-")
        return table

    start = 0 if selected_index < 20 else selected_index - 19
    for index, agent in enumerate(agents[start : start + 20], start=start):
        status = str(agent.get("status") or "unknown")
        marker = ">" if index == selected_index else " "
        row_style = "reverse" if index == selected_index else _status_color(status)
        table.add_row(
            f"{marker}{index + 1}",
            _agent_id(agent),
            _workflow_agent_summary(agent),
            _progress_renderable(agent),
            _agent_token_column(agent),
            str(_int_value(agent, "mailbox_depth", "queue_depth", "mailbox")),
            style=row_style,
        )
    return table


def _workflow_agent_summary(agent: dict[str, Any]) -> str:
    return _working_on(agent)


def _agent_token_column(agent: dict[str, Any]) -> str:
    tokens_used = agent.get("tokens_used")
    token_budget = agent.get("token_budget")
    if tokens_used is not None:
        text = _format_tokens(tokens_used)
        if token_budget is not None and token_budget != tokens_used:
            return f"{text}/{_format_tokens(token_budget)} tok"
        return f"{text} tok"
    if token_budget is not None:
        return f"{_format_tokens(token_budget)} tok budget"
    return "-"


def _format_tokens(value: float) -> str:
    if value >= 1000:
        return f"{value / 1000:.1f}k".replace(".0k", "k")
    return str(int(value))


def _sorted_agents(raw_agents: Any) -> list[dict[str, Any]]:
    agents = [agent for agent in raw_agents if isinstance(agent, dict)] if isinstance(raw_agents, list) else []
    return sorted(
        agents,
        key=lambda agent: (
            -_int_value(agent, "processed_messages", "messages_processed", "processed"),
            -_int_value(agent, "mailbox_depth", "queue_depth", "mailbox"),
            str(agent.get("agent_id") or agent.get("id") or agent.get("node_id") or ""),
        ),
    )


def _job_phase_table(summary: dict[str, Any], job: dict[str, Any], agents: list[dict[str, Any]]) -> Table:
    table = Table(title="Phases", box=box.SIMPLE, show_header=False, expand=True)
    table.add_column("phase")
    table.add_column("agents", justify="right", no_wrap=True)
    nodes = summary.get("nodes") or job.get("nodes") or []
    rows: list[tuple[str, str, str]] = []

    if isinstance(nodes, list) and nodes:
        for index, node in enumerate(nodes, start=1):
            if isinstance(node, dict):
                node_id = str(node.get("node_id") or node.get("id") or node.get("name") or f"node_{index}")
                status = str(node.get("status") or "unknown")
            else:
                node_id = str(node)
                status = "unknown"
            matching = [agent for agent in agents if str(agent.get("node_id") or agent.get("node") or agent.get("agent_id") or "").startswith(node_id)]
            total = len(matching)
            done = sum(1 for agent in matching if _is_terminal_status(agent.get("status")))
            rows.append((f"{_status_icon(status)} {index} {node_id}", f"{done}/{total}" if total else "-", status))
    elif agents:
        groups: dict[str, list[dict[str, Any]]] = {}
        for agent in agents:
            groups.setdefault(str(agent.get("agent_type") or agent.get("type") or "worker"), []).append(agent)
        for index, (label, group) in enumerate(sorted(groups.items()), start=1):
            done = sum(1 for agent in group if _is_terminal_status(agent.get("status")))
            status = _group_status(group)
            rows.append((f"{_status_icon(status)} {index} {label}", f"{done}/{len(group)}", status))
    else:
        rows.append((". 1 Runtime", "0/0", "unknown"))

    for label, count, status in rows:
        table.add_row(label, count, style=_status_color(status) if status else None)
    return table


def _agent_table(agents: list[dict[str, Any]], selected_index: int) -> Table:
    table = Table(title=f"Agents  |  {len(agents)} workers", box=box.SIMPLE, expand=True)
    table.add_column("#", justify="right", no_wrap=True)
    table.add_column("agent", no_wrap=True)
    table.add_column("status", no_wrap=True)
    table.add_column("working on")
    table.add_column("progress", justify="right", no_wrap=True, width=26)
    table.add_column("mail", justify="right", no_wrap=True)

    if not agents:
        table.add_row("-", "none", "unknown", "No agents reported by the runtime yet.", "-", "-")
        return table

    start = 0 if selected_index < 20 else selected_index - 19
    for index, agent in enumerate(agents[start : start + 20], start=start):
        status = str(agent.get("status") or "unknown")
        marker = ">" if index == selected_index else " "
        table.add_row(
            f"{marker}{index + 1}",
            _agent_id(agent),
            status,
            _working_on(agent),
            _progress_renderable(agent),
            str(_int_value(agent, "mailbox_depth", "queue_depth", "mailbox")),
            style="reverse" if index == selected_index else _status_color(status),
        )
    return table


def _agent_detail_panel(agent: dict[str, Any] | None) -> Panel:
    if not agent:
        return Panel("No agent selected.", title="Agent Detail", border_style="yellow")

    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold")
    table.add_column()
    rows = [
        ("Agent", _agent_id(agent)),
        ("Type", str(agent.get("agent_type") or agent.get("type") or "worker")),
        ("Status", str(agent.get("status") or "unknown")),
        ("Node", str(agent.get("node_id") or agent.get("node") or "-")),
        ("Working On", _working_on(agent)),
        ("Tokens", _agent_token_column(agent)),
        ("Progress", _progress_text(agent)),
        ("Processed", str(_int_value(agent, "processed_messages", "messages_processed", "processed"))),
        ("Mailbox", str(_int_value(agent, "mailbox_depth", "queue_depth", "mailbox"))),
        ("Parent Job", str(agent.get("parent_job_id") or agent.get("job_id") or "-")),
        ("Started", str(agent.get("started_at") or agent.get("created_at") or "-")),
        ("Updated", str(agent.get("updated_at") or agent.get("last_seen_at") or "-")),
    ]
    error = agent.get("error") or agent.get("last_error")
    if error:
        rows.append(("Error", _fit_text(str(error), 120)))
    resources = agent.get("resources") or agent.get("resource_usage") or agent.get("stats")
    if isinstance(resources, dict):
        rows.append(("Resources", _fit_text(json.dumps(resources, sort_keys=True), 120)))
    for label, value in rows:
        table.add_row(label, _fit_text(value, 160))
    return Panel(table, title=f"Agent Detail  {_agent_id(agent)}", border_style=_status_color(str(agent.get("status") or "")), box=box.ROUNDED)


def _agent_id(agent: dict[str, Any]) -> str:
    return str(agent.get("agent_id") or agent.get("id") or agent.get("node_id") or "agent")


def _working_on(agent: dict[str, Any]) -> str:
    for key in ("working_on", "current_task", "task", "role", "status_detail", "last_event", "last_message"):
        value = agent.get(key)
        if isinstance(value, str) and value.strip():
            return _fit_text(value.strip(), 90)
    current_message = agent.get("current_message")
    if isinstance(current_message, dict):
        message_type = current_message.get("type") or current_message.get("message_type")
        content = current_message.get("content") or current_message.get("body")
        if message_type or content:
            return _fit_text(f"{message_type or 'message'}: {content or ''}", 90)
    return str(agent.get("agent_type") or agent.get("type") or "worker")


def _progress_text(agent: dict[str, Any]) -> str:
    progress = _agent_progress(agent)
    return f"{_bar(progress)} {progress * 100:>3.0f}%"


def _progress_renderable(agent: dict[str, Any]):
    progress = _agent_progress(agent)
    grid = Table.grid(expand=False, padding=(0, 1))
    grid.add_column(width=16, no_wrap=True)
    grid.add_column(justify="right", width=5, no_wrap=True)
    grid.add_row(
        ProgressBar(
            total=100,
            completed=progress * 100,
            width=16,
            style="grey35",
            complete_style=_progress_bar_color(agent),
            finished_style=_progress_bar_color(agent),
        ),
        Text(f"{progress * 100:>3.0f}%"),
    )
    return grid


def _progress_bar_color(agent: dict[str, Any]) -> str:
    status = str(agent.get("status") or "").lower()
    if status in {"failed", "cancelled"}:
        return "red"
    if status in {"partial", "skipped"}:
        return "yellow"
    if status in {"completed", "done", "finished", "succeeded"}:
        return "green"
    return "cyan"


def _agent_progress(agent: dict[str, Any]) -> float:
    raw = agent.get("progress")
    if raw is not None:
        try:
            value = float(raw)
            if value > 1:
                value /= 100
            return max(0.0, min(1.0, value))
        except (TypeError, ValueError):
            pass
    status = str(agent.get("status") or "").lower()
    if status in {"completed", "done", "finished", "succeeded", "failed", "cancelled", "partial", "skipped"}:
        return 1.0
    if status in {"running", "busy", "active", "processing"}:
        return 0.5
    if status == "idle":
        return 0.1
    if status in {"ready", "queued", "pending", "scheduled"}:
        return 0.1
    return 0.0


def _bar(progress: float, width: int = 10) -> str:
    filled = int(max(0.0, min(1.0, progress)) * width)
    return "[" + "#" * filled + "." * (width - filled) + "]"


def _int_value(mapping: dict[str, Any], *keys: str) -> int:
    for key in keys:
        try:
            return int(mapping.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return 0


def _is_terminal_status(status: Any) -> bool:
    return str(status or "").lower() in {"completed", "done", "finished", "succeeded", "failed", "cancelled", "partial", "skipped"}


def _group_status(agents: list[dict[str, Any]]) -> str:
    statuses = {str(agent.get("status") or "unknown").lower() for agent in agents}
    if statuses <= {"completed", "done", "finished", "succeeded"}:
        return "completed"
    if statuses & {"partial", "skipped"}:
        return "partial"
    if statuses & {"failed", "cancelled"}:
        return "failed"
    if statuses & {"running", "busy", "active", "processing"}:
        return "running"
    if statuses & {"idle"}:
        return "idle"
    if statuses & {"ready", "queued", "pending", "scheduled"}:
        return "pending"
    return "unknown"


def _status_color(status: Any) -> str:
    normalized = str(status or "").lower()
    if normalized in {"completed", "done", "finished", "succeeded", "running", "active"}:
        return "green"
    if normalized in {"partial", "skipped"}:
        return "yellow"
    if normalized in {"failed", "cancelled", "error"}:
        return "red"
    if normalized in {"idle", "ready"}:
        return "cyan"
    if normalized in {"ready", "busy", "queued", "pending", "scheduled", "preparing"}:
        return "yellow"
    return "cyan"


def _status_icon(status: Any) -> str:
    normalized = str(status or "").lower()
    if normalized in {"completed", "done", "finished", "succeeded"}:
        return "v"
    if normalized == "partial":
        return "~"
    if normalized == "skipped":
        return "-"
    if normalized in {"failed", "cancelled", "error"}:
        return "x"
    if normalized in {"running", "busy", "active", "processing"}:
        return ">"
    if normalized == "idle":
        return "."
    return "."


def _elapsed_label(summary: dict[str, Any], job: dict[str, Any]) -> str:
    raw = summary.get("elapsed_seconds") or job.get("elapsed_seconds") or job.get("duration_seconds")
    try:
        elapsed = float(raw)
        return f"{elapsed:.0f}s" if elapsed < 60 else f"{elapsed / 60:.1f}m"
    except (TypeError, ValueError):
        return "--"


def _format_elapsed(raw: Any) -> str:
    try:
        elapsed = float(raw)
    except (TypeError, ValueError):
        return "--"
    if elapsed < 60:
        return f"{elapsed:.0f}s"
    return f"{elapsed / 60:.1f}m"


def _spinner(status: Any) -> str:
    if str(status or "").lower() in {"completed", "failed", "cancelled"}:
        return ""
    frames = ["|", "/", "-", "\\"]
    return frames[int(time.time() * 8) % len(frames)]


def _fit_text(value: Any, max_length: int) -> str:
    text = str(value)
    if len(text) <= max_length:
        return text
    return text[: max(0, max_length - 1)] + "..."

def generate_summary_panel(job_id: str, status: str, log_dir) -> Panel:
    status_text = "Unknown"
    status_color = "yellow"
    if status == "completed":
        status_text = "Success"
        status_color = "green"
    elif status == "failed":
        status_text = "Failed"
        status_color = "red"
    elif status == "cancelled":
        status_text = "Cancelled"
        status_color = "red"
        
    log_file = log_dir / "events.log"
    
    panel_text = (
        f"[bold {status_color}]Job Status: {status_text}[/bold {status_color}]\n\n"
        f"Job ID: {job_id}\n"
        f"Outputs:\n"
        f"  Logs:   {log_file}"
    )
    if (log_dir / "result.txt").exists():
        panel_text += f"\n  Result: {log_dir / 'result.txt'}"
    if (log_dir / "result_stream.txt").exists():
        panel_text += f"\n  Stream: {log_dir / 'result_stream.txt'}"
        
    return Panel(
        panel_text,
        title="Job Execution Summary",
        border_style=status_color,
        expand=False
    )

def generate_run_submitted_panel(
    *,
    bundle_name: str,
    job_id: str,
    payload_count: int,
    log_dir,
    follow_seconds: float,
    run_mode: str = "Batch",
    blueprint_run_id: Optional[str] = None,
    blueprint_revision: Optional[str] = None,
    web_ui_url: Optional[str] = None,
    detached: bool = False,
) -> Panel:
    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold")
    table.add_column()
    table.add_row("Bundle", bundle_name)
    table.add_row("Job ID", f"[bold cyan]{job_id}[/bold cyan]")
    if blueprint_run_id:
        table.add_row("Blueprint Run ID", f"[bold green]{blueprint_run_id}[/bold green]")
    if blueprint_revision:
        table.add_row("Blueprint Revision", blueprint_revision[:12])
    table.add_row("Type", run_mode)
    if web_ui_url:
        table.add_row("Web UI", f"[bold green]{web_ui_url}[/bold green]")
    table.add_row("Payloads", str(payload_count))
    table.add_row("Logs", str(log_dir / "events.log"))
    table.add_row("Snapshot", str(log_dir / "job_snapshot.json"))
    table.add_row("Follow", "Detached immediately" if detached else f"{follow_seconds:g}s event tail, then detach")

    return Panel(
        table,
        title="Job submit successful",
        border_style="cyan",
        expand=False,
    )

def generate_detached_panel(
    job_id: str,
    log_dir,
    status: str,
    event_count: int,
    *,
    web_ui_url: Optional[str] = None,
) -> Panel:
    status_color = (
        "green"
        if status == "completed"
        else "red"
        if status in {"failed", "cancelled"}
        else "yellow"
    )
    status_label = status.replace("_", " ").title() if status else "Unknown"

    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold")
    table.add_column()
    table.add_row("Status", f"[{status_color}]{status_label}[/{status_color}]")
    table.add_row("Job ID", f"[bold cyan]{job_id}[/bold cyan]")
    table.add_row("Events Logged", str(event_count))
    table.add_row("Raw Events", str(log_dir / "events.log"))
    table.add_row("Run Log", str(log_dir / "run.log"))
    if web_ui_url:
        table.add_row("Web UI", f"[bold green]{web_ui_url}[/bold green]")
    table.add_row("Monitor", f"mn job monitor {job_id}")

    message = Text()
    if status in {"completed", "failed", "cancelled"}:
        message.append("Final job state reached.", style=status_color)
    else:
        message.append("Detached while job is still scheduled/running.", style="yellow")

    return Panel(
        Group(message, table),
        title="Run Detached",
        border_style=status_color,
        expand=False,
    )
