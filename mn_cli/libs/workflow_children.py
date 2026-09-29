"""Bounded, pageable presentation of runtime-created child steps."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from rich import box
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
    from mn_cli.libs.ui import JobMonitorState

PAGE_SIZE = 4


def child_steps_table(steps: list[dict[str, Any]], state: JobMonitorState) -> Table | None:
    from mn_cli.libs.ui import _monitor_safe_text

    children = [step for step in steps if step.get("parent_step_id")]
    state.child_count = len(children)
    if not children:
        return None
    done = sum(step.get("status") in {"done", "completed", "skipped"} for step in children)
    failed = sum(step.get("status") in {"failed", "blocked"} for step in children)
    failed_focus = next(
        (i for i, step in enumerate(children) if step.get("status") in {"failed", "blocked"}),
        len(children) - 1,
    )
    focus = next(
        (i for i, step in enumerate(children) if step.get("status") == "running"),
        failed_focus,
    )
    start = (focus // PAGE_SIZE) * PAGE_SIZE if state.child_offset is None else state.child_offset
    start = min(max(start, 0), ((len(children) - 1) // PAGE_SIZE) * PAGE_SIZE)
    state.child_page_start = start
    title = (
        f"Sub-workflow steps · {done}/{len(children)} done · {failed} failed"
        f" · {start + 1}–{min(start + PAGE_SIZE, len(children))}"
        + (" · following" if state.child_offset is None else " · browsing")
    )
    page = children[start : start + PAGE_SIZE]
    parents = {str(step["parent_step_id"]) for step in page}
    if len(parents) == 1:
        title += "\n" + _monitor_safe_text(next(iter(parents)))
    table = Table(title=Text(title), box=box.SIMPLE, expand=True)
    table.add_column("R", width=2)
    table.add_column("Task", ratio=3, overflow="fold")
    table.add_column("Status", width=7)
    table.add_column("Time", justify="right", width=5)
    for step in page:
        status = str(step.get("status") or "pending")
        color = {
            "running": "cyan", "failed": "red", "blocked": "red",
            "completed": "green", "done": "green",
        }.get(status, "dim")
        task_id = str(step.get("id") or "?")
        if len(parents) == 1:
            task_id = task_id.removeprefix(str(step["parent_step_id"]) + ":")
        task = Text(_monitor_safe_text(task_id), style=color)
        task.append(" · " + _monitor_safe_text(step.get("child_phase") or "task"))
        failure = step.get("failure")
        reason = step.get("status_reason") or (
            failure.get("message") if isinstance(failure, dict) else failure
        )
        if reason and status in {"failed", "blocked", "retry_wait"}:
            task.append("\n" + _monitor_safe_text(reason, limit=240), style="red")
        seconds = max(float(step.get("elapsed_seconds") or 0), 0)
        elapsed = f"{int(seconds) // 60}:{int(seconds) % 60:02d}" if seconds else "—"
        table.add_row(
            Text(str(step.get("child_round", "?"))),
            task, Text(status, style=color), Text(elapsed),
        )
    table.caption = "[ / ] page child steps · f follow active step"
    return table
