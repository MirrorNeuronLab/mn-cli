from __future__ import annotations

import json
import time
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
    "runtime_model_install_progress",
    "runtime_model_ready",
    "runtime_model_install_failed",
}


class ProgressSnapshotStream:
    def __init__(
        self, view: BlueprintWorkflowProgress, *, min_interval: float = 0.5
    ) -> None:
        self.view = view
        self.min_interval = max(float(min_interval), 0.5)
        self._last_emit_at = 0.0
        self._pending = False

    def observe_event(self, event: dict[str, Any]) -> bool:
        self.view.update(event)
        event_type = str(event.get("type") or "")
        now = time.monotonic()
        if (
            self._event_should_flush(event_type)
            or now - self._last_emit_at >= self.min_interval
        ):
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
        if (
            normalized in TERMINAL_EVENT_TYPES
            or normalized in IMMEDIATE_PROGRESS_EVENTS
        ):
            return True
        return (
            "failed" in normalized or "error" in normalized or "timed_out" in normalized
        )


def _progress_request(api_base_url, run_id, api_token):
    base = str(api_base_url or "").rstrip("/")
    path = urllib.parse.quote(str(run_id), safe="")
    headers = {"Accept": "text/event-stream"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    return f"{base}/runs/{path}/events/stream", headers


class _SnapshotDecoder:
    def __init__(self):
        self.event = "message"
        self.data = []

    def feed(self, line):
        if line.startswith("event:"):
            self.event = line[6:].strip()
        elif line.startswith("data:"):
            self.data.append(line[5:].lstrip())
        elif not line:
            event, data = self.event, self.data
            self.event, self.data = "message", []
            if event in {"snapshot", "run.snapshot", "message"} and data:
                payload = json.loads("\n".join(data))
                if isinstance(payload, dict):
                    if payload.get("type") == "run.snapshot" and isinstance(
                        payload.get("data"), dict
                    ):
                        return payload["data"]
                    if "type" not in payload:
                        return payload
        return None


def stream_api_workflow_progress(
    api_base_url: str,
    run_id: str,
    *,
    api_token: str = "",
    timeout: float = 10.0,
):
    if not api_base_url:
        return
    url, headers = _progress_request(api_base_url, run_id, api_token)
    decoder = _SnapshotDecoder()
    with urllib.request.urlopen(
        urllib.request.Request(url, headers=headers), timeout=timeout
    ) as response:
        for raw in response:
            snapshot = decoder.feed(
                raw.decode("utf-8", errors="replace").rstrip("\r\n")
            )
            if snapshot is not None:
                yield snapshot


async def watch_api_workflow_progress(
    api_base_url, run_id, *, api_token="", timeout=10.0
):
    import httpx
    from mn_sdk.errors import AppError

    url, headers = _progress_request(api_base_url, run_id, api_token)
    decoder = _SnapshotDecoder()
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        async with client.stream("GET", url, headers=headers) as response:
            if response.status_code in (401, 403):
                raise AppError(
                    "MN_PERMISSION_DENIED",
                    "Permission was denied for this MirrorNeuron operation.",
                    exit_code=13,
                    http_status=response.status_code,
                )
            if 400 <= response.status_code < 500 and response.status_code != 404:
                from mn_sdk.errors import ValidationError

                raise ValidationError("The workflow progress request was rejected.")
            response.raise_for_status()
            async for line in response.aiter_lines():
                snapshot = decoder.feed(line)
                if snapshot is not None:
                    yield snapshot
