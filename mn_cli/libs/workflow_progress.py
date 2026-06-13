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
    def _steps_from_manifest(self, manifest: dict[str, Any]) -> list[StepProgress]:
        workflow = (
            manifest.get("workflow")
            if isinstance(manifest.get("workflow"), dict)
            else {}
        )
        workflow_steps = workflow.get("steps") if isinstance(workflow, dict) else None
        if isinstance(workflow_steps, list) and workflow_steps:
            runtime = (
                manifest.get("runtime")
                if isinstance(manifest.get("runtime"), dict)
                else {}
            )
            return self._workflow_steps_from_manifest(workflow_steps, runtime)
        return super()._steps_from_manifest(manifest)

    def _workflow_steps_from_manifest(
        self, raw_steps: list[Any], runtime: dict[str, Any]
    ) -> list[StepProgress]:
        bindings = (
            runtime.get("bindings") if isinstance(runtime.get("bindings"), dict) else {}
        )
        steps: list[StepProgress] = []
        graph_steps = (
            self.graph.get("steps") if isinstance(self.graph.get("steps"), dict) else {}
        )

        for raw in raw_steps:
            if not isinstance(raw, dict):
                continue
            step_id = str(raw.get("id") or f"step_{len(steps) + 1}")
            binding_key = str(raw.get("run") or step_id)
            binding = bindings.get(binding_key) if isinstance(bindings, dict) else None
            binding_dict = binding if isinstance(binding, dict) else {}
            emits = str(raw.get("emits") or "") or None
            transition = raw.get("on") if isinstance(raw.get("on"), dict) else {}
            target = transition.get(emits) if emits and isinstance(transition, dict) else None
            next_step = target.get("to") if isinstance(target, dict) else target
            graph_step = graph_steps.get(step_id) if isinstance(graph_steps, dict) else {}
            live = self.workflow_kind == "service" and (
                _truthy(raw.get("live"))
                or _truthy(binding_dict.get("live"))
                or str(
                    raw.get("kind")
                    or binding_dict.get("kind")
                    or binding_dict.get("type")
                    or ""
                ).lower()
                in {"stream", "service", "watch", "watcher", "daemon", "listener"}
            )
            agents = _agents_from_runtime_binding(binding_dict, step_id, live=live)

            steps.append(
                StepProgress(
                    id=step_id,
                    label=str(raw.get("label") or step_id.replace("_", " ").title()),
                    goal=str(raw.get("goal") or raw.get("action") or ""),
                    emits=emits,
                    next_step=str(next_step) if next_step else None,
                    parents=list(graph_step.get("parents") or []),
                    children=list(graph_step.get("children") or []),
                    layer=int(graph_step.get("layer") or 0),
                    requires=_text_list(raw.get("requires")),
                    provides=_text_list(raw.get("provides")),
                    agents=agents,
                    live=live or any(agent.live for agent in agents),
                )
            )
        return steps

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._apply_run_binding_aliases()

    def _apply_run_binding_aliases(self) -> None:
        workflow = (
            self.manifest.get("workflow")
            if isinstance(self.manifest.get("workflow"), dict)
            else {}
        )
        raw_steps = workflow.get("steps") if isinstance(workflow, dict) else None
        runtime = (
            self.manifest.get("runtime")
            if isinstance(self.manifest.get("runtime"), dict)
            else {}
        )
        bindings = runtime.get("bindings") if isinstance(runtime.get("bindings"), dict) else {}
        if not isinstance(raw_steps, list) or not isinstance(bindings, dict):
            return

        changed = False
        for raw_step in raw_steps:
            if not isinstance(raw_step, dict):
                continue
            step_id = str(raw_step.get("id") or "")
            run_key = str(raw_step.get("run") or "")
            if not step_id or not run_key or run_key == step_id:
                continue
            binding = bindings.get(run_key)
            step = self.steps_by_id.get(step_id)
            if not isinstance(binding, dict) or step is None:
                continue
            agents = _agents_from_runtime_binding(binding, step_id, live=step.live)
            if agents:
                step.agents = agents
                changed = True

        if changed:
            self.agent_to_step = {
                agent.id: step.id for step in self.steps for agent in step.agents
            }

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
        body.add_column()
        body.add_row(phases)
        body.add_row(agents)
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
            style = (
                "bright_blue"
                if step.id == self.current_step_id and step.status != "done"
                else None
            )
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
        table.add_column("working on", min_width=12)
        table.add_column("progress", justify="right")
        for agent in (step.agents if step else []):
            table.add_row(
                f"{_agent_icon(agent.status)} {agent.id}",
                agent.working_on or agent.role or "worker",
                _agent_progress_detail(agent),
                style=_agent_style(agent.status),
            )
        if step and not step.agents:
            table.add_row("- none declared -", step.goal or "workflow step", "-")
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
    return {
        "done": "✓",
        "running": "›",
        "idle": "•",
        "failed": "✗",
        "cancelled": "✗",
    }.get(status, "•")


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


def _agent_progress_detail(agent: AgentProgress) -> str:
    metrics = _agent_metrics(agent)
    if agent.model:
        return f"{agent.model} · {metrics}" if metrics else agent.model
    return metrics


def _format_tokens(value: float) -> str:
    if value >= 1000:
        return f"{value / 1000:.1f}k".replace(".0k", "k")
    return str(int(value))


def _agents_from_runtime_binding(
    binding: dict[str, Any], step_id: str, *, live: bool = False
) -> list[AgentProgress]:
    raw_workers = binding.get("workers")
    if not isinstance(raw_workers, list) or not raw_workers:
        raw_workers = [binding.get("worker") or binding or {"id": step_id}]

    agents: list[AgentProgress] = []
    for index, raw_worker in enumerate(raw_workers):
        raw = raw_worker if isinstance(raw_worker, dict) else {"id": str(raw_worker)}
        agent_id = str(raw.get("id") or raw.get("node_id") or f"{step_id}:{index + 1}")
        role = str(raw.get("role") or raw.get("working_on") or "worker")
        agents.append(
            AgentProgress(
                id=agent_id,
                alias=_first_text(raw, "alias"),
                display_name=_first_text(raw, "display_name", "label", "name"),
                role=role,
                working_on=role,
                model=str(
                    raw.get("model")
                    or raw.get("uses")
                    or binding.get("uses")
                    or "runtime"
                ),
                tools=_optional_int(raw.get("tools")),
                tokens=_optional_int(raw.get("tokens")),
                live=live or _truthy(raw.get("live")),
            )
        )
    return agents


def _first_text(mapping: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _text_list(value: Any) -> list[str]:
    return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
