from __future__ import annotations

from datetime import datetime
from typing import Any

from mn_sdk.workflow_progress import (
    AgentProgress,
    BlueprintWorkflowProgress as SdkBlueprintWorkflowProgress,
    StepProgress,
)
from rich import box
from rich.panel import Panel
from rich.table import Table
from mn_cli.libs.ui import generate_workflow_progress_layout
from mn_cli.libs.ui import JobMonitorState


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
        self._monitor_state: JobMonitorState | None = None
        self._workflow_token_usage: dict[str, int] = {}
        self._resource_token_total: int | None = None

    def set_monitor_state(self, state: JobMonitorState | None) -> None:
        self._monitor_state = state

    def set_resource_token_total(self, total: int | None) -> None:
        self._resource_token_total = _optional_int(total)

    def has_token_usage(self) -> bool:
        return bool(self._workflow_token_usage)

    def _apply_event_token_usage(self, event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else event
        token_count = _extract_token_count(payload)
        if token_count is None:
            return
        agent_ids = _extract_workflow_agent_ids(payload)
        for agent_id in agent_ids:
            previous = self._workflow_token_usage.get(agent_id, 0)
            self._workflow_token_usage[agent_id] = previous + token_count

    def record_event_token_usage(self, event: dict[str, Any]) -> None:
        self._apply_event_token_usage(event)

    def snapshot(self) -> dict[str, Any]:
        snapshot = super().snapshot()
        if self._resource_token_total is not None:
            snapshot["resource_tokens"] = self._resource_token_total
        if not self._workflow_token_usage:
            return snapshot
        _attach_workflow_token_usage(snapshot, self._workflow_token_usage)
        return snapshot

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
        return generate_workflow_progress_layout(
            self.job_id or "workflow",
            self.snapshot(),
            state=self._monitor_state,
        )

    def _summary_step_counts(self) -> tuple[int, int]:
        total_steps = len(self.steps)
        if self.workflow_kind == "service":
            shown_steps = sum(
                1
                for step in self.steps
                if step.status in {"done", "running", "idle"}
                or step.ready_count > 0
            )
        else:
            shown_steps = sum(1 for step in self.steps if step.status == "done")
        return shown_steps, total_steps

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


def build_workflow_progress_snapshot(
    manifest: dict[str, Any] | None,
    events: list[dict[str, Any]] | None,
    *,
    job: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Build a monitor snapshot with the same model used by ``blueprint run``.

    Runtime jobs retain their public workflow ledger separately from the lowered
    agent topology.  Keeping this reconstruction here ensures the live runner
    and the later monitor render the same public steps and agent contract.
    """

    job = job if isinstance(job, dict) else {}
    summary = summary if isinstance(summary, dict) else {}
    view = BlueprintWorkflowProgress(
        manifest or {},
        job_id=job_id or _first_nonempty_text(job, summary, keys=("job_id", "id")),
        started_at=_job_started_at(job, summary),
        job=job,
        summary=summary,
    )
    for event in events or []:
        if not isinstance(event, dict):
            continue
        view.record_event_token_usage(event)
        view.update(event)
    view.apply_workflow_state(_workflow_state_from(job, summary))
    view.apply_job_status(job, summary)
    return view.snapshot()


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
        suffix = " est." if getattr(agent, "progress_source", "estimated") not in {"explicit", "items", "complete"} else ""
        parts = [f"{agent.progress * 100:>3.0f}%{suffix}"]
    items_total = getattr(agent, "items_total", None)
    if items_total:
        parts.append(f"{getattr(agent, 'items_done', 0) or 0}/{items_total} items")
    tokens_used = getattr(agent, "tokens_used", None)
    token_budget = getattr(agent, "token_budget", None)
    if tokens_used and token_budget:
        parts.append(f"{_format_tokens(tokens_used)}/{_format_tokens(token_budget)} tok")
    elif tokens_used:
        parts.append(f"{_format_tokens(tokens_used)} tok")
    elif token_budget:
        parts.append(f"{_format_tokens(token_budget)} tok budget")
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


def _extract_workflow_agent_ids(payload: Any) -> set[str]:
    agent_ids: set[str] = set()
    if not isinstance(payload, dict):
        return agent_ids
    for value_key in ("worker", "worker_id", "agent", "agent_id", "node_id"):
        candidate = payload.get(value_key)
        if isinstance(candidate, str) and candidate.strip():
            agent_ids.add(candidate.strip())

    for key, value in payload.items():
        if key in {"agent", "worker", "step", "step_id", "current_step", "step_attempt"}:
            continue
        if isinstance(value, dict):
            agent_ids.update(_extract_workflow_agent_ids(value))
        elif isinstance(value, list):
            for item in value:
                agent_ids.update(_extract_workflow_agent_ids(item))
    if not agent_ids:
        direct_agent = payload.get("id")
        if isinstance(direct_agent, str) and direct_agent.strip():
            agent_ids.add(direct_agent.strip())
    return agent_ids


def _extract_token_count(payload: Any) -> int | None:
    if isinstance(payload, (int, float)):
        if int(payload) > 0:
            return int(payload)
        return None

    if not isinstance(payload, dict):
        if isinstance(payload, list):
            total = 0
            found = False
            for item in payload:
                value = _extract_token_count(item)
                if value is not None:
                    found = True
                    total += value
            return total if found else None
        return None

    # Prefer explicit total usage fields.
    explicit_total = _optional_int(payload.get("total_tokens"))
    if explicit_total is not None:
        return explicit_total

    has_budget_field = any(
        key in payload
        for key in ("token_budget", "max_tokens", "budget", "budget_tokens")
    )
    if not has_budget_field:
        direct = _optional_int(payload.get("tokens"))
        if direct is not None:
            return direct

    direct = _optional_int(payload.get("tokens_used"))
    if direct is not None:
        return direct

    direct = _optional_int(payload.get("token_count"))
    if direct is not None:
        return direct

    direct = _optional_int(payload.get("used_tokens"))
    if direct is not None:
        return direct

    # Usage component sums.
    usage_parts = [
        _optional_int(payload.get("input_tokens")),
        _optional_int(payload.get("output_tokens")),
        _optional_int(payload.get("prompt_tokens")),
        _optional_int(payload.get("completion_tokens")),
    ]
    usage_sum = sum(value for value in usage_parts if value is not None)
    if usage_sum:
        return usage_sum

    nested_keys = ("usage", "llm", "llm_usage", "token_usage")
    for key in nested_keys:
        nested = payload.get(key)
        if isinstance(nested, (dict, list)):
            value = _extract_token_count(nested)
            if value is not None:
                return value

    total = 0
    found = False
    skip_keys = {"token_budget", "max_tokens", "budget", "budget_tokens"}
    if has_budget_field:
        skip_keys.add("tokens")
    for key, value in payload.items():
        if key in skip_keys:
            continue
        nested = _extract_token_count(value)
        if nested is not None:
            total += nested
            found = True
    return total if found else None


def _attach_workflow_token_usage(
    snapshot: dict[str, Any],
    token_usage: dict[str, int],
) -> None:
    steps = snapshot.get("steps")
    if not isinstance(steps, list):
        return
    for step in steps:
        if not isinstance(step, dict):
            continue
        agents = step.get("agents")
        if not isinstance(agents, list):
            continue
        for agent in agents:
            if not isinstance(agent, dict):
                continue
            agent_id = str(agent.get("id") or agent.get("agent") or "")
            if not agent_id:
                continue
            used = _optional_int(token_usage.get(agent_id))
            if used is not None:
                agent["tokens_used"] = used


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
                token_budget=_optional_int(raw.get("token_budget")),
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


def _first_nonempty_text(
    *mappings: dict[str, Any], keys: tuple[str, ...]
) -> str | None:
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        value = _first_text(mapping, *keys)
        if value:
            return value
    return None


def _workflow_state_from(*mappings: dict[str, Any]) -> dict[str, Any] | None:
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        workflow_state = mapping.get("workflow_state")
        if isinstance(workflow_state, dict):
            return workflow_state
    return None


def _job_started_at(*mappings: dict[str, Any]) -> float | None:
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        for key in ("submitted_at", "started_at", "created_at", "created"):
            value = mapping.get(key)
            if isinstance(value, (int, float)):
                return float(value)
            if not isinstance(value, str) or not value.strip():
                continue
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                continue
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
