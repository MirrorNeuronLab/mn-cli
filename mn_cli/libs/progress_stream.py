from __future__ import annotations

import time
import json
import urllib.parse
import urllib.request
from typing import Any

from mn_cli.libs.workflow_progress import BlueprintWorkflowProgress


TERMINAL_EVENT_TYPES = {"job_completed", "job_failed", "job_cancelled"}
IMMEDIATE_PROGRESS_EVENTS = {
    "job_pending",
    "job_validated",
    "job_scheduled",
    "job_running",
    "job_pausing",
    "job_paused",
    "job_resumed",
    "workflow_step_started",
    "blueprint_phase_started",
    "workflow_step_completed",
    "blueprint_phase_completed",
    "workflow_step_failed",
    "blueprint_phase_failed",
    "workflow_step_timed_out",
    "workflow_step_attempt_retry_scheduled",
    "workflow_step_attempt_timed_out",
    "workflow_step_blocked",
    "runtime_model_selection_started",
    "runtime_model_selected",
    "runtime_model_install_started",
    "runtime_model_ready",
    "runtime_model_install_failed",
}


class ProgressSnapshotStream:
    def __init__(self, view: BlueprintWorkflowProgress, *, min_interval: float = 0.5) -> None:
        self.view = view
        self.min_interval = max(float(min_interval), 0.5)
        self._last_emit_at = 0.0
        self._pending = False

    def observe_event(self, event: dict[str, Any]) -> bool:
        self.view.update(event)
        event_type = str(event.get("type") or "")
        now = time.monotonic()
        if self._event_should_flush(event_type) or now - self._last_emit_at >= self.min_interval:
            self._last_emit_at = now
            self._pending = False
            return True
        self._pending = True
        return False

    def flush_due(self) -> bool:
        if not self._pending:
            return False
        now = time.monotonic()
        if now - self._last_emit_at < self.min_interval:
            return False
        self._last_emit_at = now
        self._pending = False
        return True

    @staticmethod
    def _event_should_flush(event_type: str) -> bool:
        normalized = str(event_type or "").strip().lower()
        if normalized in TERMINAL_EVENT_TYPES or normalized in IMMEDIATE_PROGRESS_EVENTS:
            return True
        return "failed" in normalized or "error" in normalized or "timed_out" in normalized


def stream_api_workflow_progress(
    api_base_url: str,
    job_id: str,
    *,
    api_token: str = "",
    timeout: float = 10.0,
):
    base = str(api_base_url or "").rstrip("/")
    if not base:
        return
    quoted_job_id = urllib.parse.quote(str(job_id), safe="")
    url = f"{base}/jobs/{quoted_job_id}/workflow-progress/stream"
    headers = {"Accept": "text/event-stream"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        event_name = "message"
        data_lines: list[str] = []
        for raw_line in response:
            line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
            if line == "":
                if event_name == "snapshot" and data_lines:
                    payload = json.loads("\n".join(data_lines))
                    if isinstance(payload, dict):
                        yield payload
                event_name = "message"
                data_lines = []
                continue
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event_name = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
