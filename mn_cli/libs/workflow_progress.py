from __future__ import annotations

import time
from typing import Any

from mn_sdk.workflow_progress import (
    AgentProgress,
    BlueprintWorkflowProgress as SdkBlueprintWorkflowProgress,
    StepProgress,
)
from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


class BlueprintWorkflowProgress(SdkBlueprintWorkflowProgress):
    def render(self) -> Panel:
        elapsed = time.time() - self.started_at
        done_agents = sum(step.done_count for step in self.steps)
        ready_agents = sum(step.ready_count for step in self.steps)
        total_agents = sum(step.total_count for step in self.steps)
        shown_agents = ready_agents if self.workflow_kind == "service" else done_agents
        elapsed_label = f"{elapsed:.0f}s" if elapsed < 60 else f"{elapsed / 60:.1f}m"
        header = Text(self.workflow_id, style="bold bright_blue")
        meta = f"{shown_agents}/{total_agents} agents  |  {elapsed_label}  |  {self.status_label}"
        phases = self._phase_table()
        agents = self._agent_table()
        body = Table.grid(expand=True)
        body.add_column(ratio=1)
        body.add_column(ratio=3)
        body.add_row(phases, agents)
        messages = Text("\n".join(self.messages[-3:]), style="dim")
        top = Table.grid(expand=True)
        top.add_column(ratio=2)
        top.add_column(justify="right")
        top.add_row(header, Text(meta, style="bold"))
        footer = Text("keys: Ctrl+D or Ctrl+C detach monitor; job keeps running", style="dim")
        if self.description:
            subtitle: Any = Text(self.description, style="dim")
            content = Group(top, subtitle, body, messages, footer)
        else:
            content = Group(top, body, messages, footer)
        title = "mn blueprint run"
        if self.job_id:
            title += f"  {self.job_id}"
        return Panel(content, title=title, border_style="cyan", box=box.ROUNDED)

    def _phase_table(self) -> Table:
        table = Table(title="Phases", box=box.SIMPLE, show_header=False, expand=True)
        table.add_column("phase")
        table.add_column("count", justify="right")
        for index, step in enumerate(self.steps, start=1):
            icon = _step_icon(step, step.id == self.current_step_id)
            style = "bright_blue" if step.id == self.current_step_id and step.status != "done" else None
            label = f"{icon} {index} {step.label}"
            shown_count = step.ready_count if self.workflow_kind == "service" else step.done_count
            table.add_row(label, f"{shown_count}/{step.total_count}", style=style)
        return table

    def _agent_table(self) -> Table:
        step = self.steps_by_id.get(self.current_step_id or "") if self.current_step_id else None
        title = "Agents"
        if step:
            title = f"{step.label}  |  {step.total_count} agents"
        table = Table(title=title, box=box.SIMPLE, expand=True)
        table.add_column("agent", no_wrap=True)
        table.add_column("working on")
        table.add_column("model", no_wrap=True)
        table.add_column("progress", justify="right", no_wrap=True)
        for agent in (step.agents if step else []):
            table.add_row(
                f"{_agent_icon(agent.status)} {agent.id}",
                agent.working_on or agent.role or "worker",
                agent.model or "-",
                _agent_metrics(agent),
                style=_agent_style(agent.status),
            )
        if step and not step.agents:
            table.add_row("- none declared -", step.goal or "workflow step", "-", "-")
        return table


def _step_icon(step: StepProgress, current: bool) -> str:
    if step.status == "done":
        return "✓"
    if step.status == "failed":
        return "✗"
    if current or step.status == "running":
        return "›"
    if step.status == "idle":
        return "•"
    return "•"


def _agent_icon(status: str) -> str:
    return {"done": "✓", "running": "›", "idle": "•", "failed": "✗", "cancelled": "✗"}.get(status, "•")


def _agent_style(status: str) -> str:
    normalized = str(status or "").lower()
    if normalized == "running":
        return "bright_blue"
    if normalized == "done":
        return "green"
    if normalized in {"failed", "cancelled"}:
        return "red"
    return "dim"


def _agent_metrics(agent: AgentProgress) -> str:
    if agent.status == "pending":
        return "pending"
    if agent.status == "idle":
        parts = ["idle"]
    else:
        parts = [f"{agent.progress * 100:>3.0f}%"]
    if agent.tokens:
        parts.append(f"{_format_tokens(agent.tokens * agent.progress)} tok")
    if agent.tools is not None:
        parts.append(f"{agent.tools} tools")
    if agent.started_at is not None:
        parts.append(f"{agent.elapsed:.0f}s")
    return " · ".join(parts)


def _format_tokens(value: float) -> str:
    if value >= 1000:
        return f"{value / 1000:.1f}k".replace(".0k", "k")
    return str(int(value))
