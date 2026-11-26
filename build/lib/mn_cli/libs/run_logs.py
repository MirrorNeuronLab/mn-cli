from __future__ import annotations

import json
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
import time
from typing import Optional


STANDARD_EVENTS = {
    "init", "job_pending", "job_validated", "job_scheduled", "job_running",
    "job_completed", "job_failed", "job_paused", "job_resumed", "job_cancelled",
    "agent_recovery_started", "agent_recovered",
    "agent_message_received", "aggregator_received", "aggregator_duplicate_ignored",
    "executor_lease_requested", "executor_lease_acquired", "executor_lease_released",
    "sandbox_job_started", "sandbox_job_completed", "sandbox_job_failed",
    "node_up", "node_down"
}


class JobLogWriter:
    def __init__(self, job_id: str, run_dir: Optional[Path] = None):
        self.job_id = job_id
        self.log_dir = Path(f"/tmp/mn_{job_id}")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.events_file = self.log_dir / "events.log"
        self.run_dir = run_dir
        self.run_events_file = run_dir / "events.jsonl" if run_dir is not None else None
        self.snapshot_file = self.log_dir / "job_snapshot.json"
        self.seen = set()
        self.web_ui_urls = set()
        self.web_ui_url: Optional[str] = None
        self.event_count = 0
        self.max_bytes = int(
            os.getenv("MN_RUN_EVENT_LOG_MAX_BYTES", str(10 * 1024 * 1024))
        )
        self.backup_count = int(os.getenv("MN_RUN_EVENT_LOG_BACKUP_COUNT", "5"))
        self.run_logger = self._build_run_logger()
        self._load_existing_event_keys()

    def _build_run_logger(self) -> logging.Logger:
        run_logger = logging.getLogger(f"mn-cli.run.{self.job_id}")
        run_logger.setLevel(os.getenv("MN_RUN_LOG_LEVEL", "INFO").upper())
        run_logger.propagate = False

        if run_logger.handlers:
            return run_logger

        handler = RotatingFileHandler(
            self.log_dir / "run.log",
            maxBytes=int(os.getenv("MN_RUN_LOG_MAX_BYTES", str(2 * 1024 * 1024))),
            backupCount=int(os.getenv("MN_RUN_LOG_BACKUP_COUNT", "5")),
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        run_logger.addHandler(handler)
        return run_logger

    def write_event_json(self, event_json: str) -> bool:
        try:
            event = json.loads(event_json)
        except Exception:
            self.run_logger.warning("Skipping invalid event JSON: %r", event_json)
            return False
        return self.write_event(event)

    def write_event(self, event: dict) -> bool:
        key = self._event_key(event)
        if key in self.seen:
            return False

        self.seen.add(key)
        self._rotate_if_needed()
        with open(self.events_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, sort_keys=True) + "\n")
        if self.run_events_file is not None:
            self.run_events_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.run_events_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, sort_keys=True) + "\n")
        self.event_count += 1

        event_type = event.get("type", "unknown")
        if event_type in {"slow_event_processed", "stream_metrics_updated"}:
            payload = event.get("payload", {})
            self.run_logger.info(
                "slow_agent_event=%s agent=%s payload=%s",
                event_type,
                event.get("agent_id") or payload.get("worker") or event.get("node"),
                json.dumps(payload, sort_keys=True),
            )
        elif event_type in {"backpressure_state", "external_input_rejected"}:
            self.run_logger.info(
                "backpressure_event=%s agent=%s payload=%s",
                event_type,
                event.get("agent_id"),
                json.dumps(event.get("payload", {}), sort_keys=True),
            )
        elif event_type in {"job_failed", "sandbox_job_failed"}:
            self.run_logger.error(
                "event=%s payload=%s", event_type, json.dumps(event, sort_keys=True)
            )
        elif event_type not in STANDARD_EVENTS:
            self.run_logger.info(
                "custom_event=%s payload=%s",
                event_type,
                json.dumps(event, sort_keys=True),
            )
        else:
            self.run_logger.info("event=%s", event_type)
        return True

    def write_snapshot(self, data: dict):
        with open(self.snapshot_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def _rotate_if_needed(self):
        if not self.events_file.exists() or self.events_file.stat().st_size < self.max_bytes:
            return

        for index in range(self.backup_count - 1, 0, -1):
            src = self.log_dir / f"events.log.{index}"
            dst = self.log_dir / f"events.log.{index + 1}"
            if src.exists():
                if dst.exists():
                    dst.unlink()
                src.rename(dst)

        first_backup = self.log_dir / "events.log.1"
        if first_backup.exists():
            first_backup.unlink()
        self.events_file.rename(first_backup)

    @staticmethod
    def _event_key(event: dict):
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        return (
            event.get("timestamp"),
            event.get("type"),
            event.get("agent_id"),
            event.get("node"),
            event.get("message_id") or payload.get("message_id"),
        )

    def _load_existing_event_keys(self) -> None:
        for path in (self.events_file, self.run_events_file):
            if path is None or not path.exists():
                continue
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                self.run_logger.exception("Failed to load existing event keys from %s", path)
                continue
            for line in lines:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    self.seen.add(self._event_key(event))

    def record_web_ui_url(self, event: dict) -> Optional[str]:
        url = extract_web_ui_url(event)
        if not url or url in self.web_ui_urls:
            return None
        self.remember_web_ui_url(url)
        return url

    def remember_web_ui_url(self, url: str) -> None:
        if not url:
            return
        normalized = str(url)
        self.web_ui_urls.add(normalized)
        self.web_ui_url = normalized


def extract_web_ui_url(event: dict) -> Optional[str]:
    payload = event.get("payload") if isinstance(event, dict) else None
    if not isinstance(payload, dict):
        payload = event if isinstance(event, dict) else {}
    web_ui = payload.get("web_ui") if isinstance(payload.get("web_ui"), dict) else payload
    url = web_ui.get("url") or web_ui.get("web_ui_url") or web_ui.get("local_url")
    return str(url) if url else None


def write_result_stream_event(log_dir: Path, event: dict):
    if event.get("type") in STANDARD_EVENTS:
        return
    payload = event.get("payload", event)
    materialize_sent_email_copy(log_dir, payload)
    with open(log_dir / "result_stream.txt", "a", encoding="utf-8") as f_stream:
        f_stream.write(json.dumps(payload, sort_keys=True) + "\n")


def materialize_sent_email_copy(log_dir: Path, payload: dict):
    if not isinstance(payload, dict):
        return
    sent_copy = payload.get("sent_email_copy")
    if not isinstance(sent_copy, dict):
        return
    html_content = sent_copy.get("html_content")
    text_content = sent_copy.get("text_content")
    metadata = sent_copy.get("metadata")
    if html_content is None and text_content is None and not isinstance(metadata, dict):
        return

    email_dir = log_dir / "sent_emails"
    email_dir.mkdir(parents=True, exist_ok=True)

    def resolve_path(raw_path: Optional[str], suffix: str) -> Path:
        if raw_path:
            return email_dir / Path(raw_path).name
        stem = str(payload.get("provider_id") or payload.get("subject") or time.time_ns())
        safe_stem = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in stem)[:96]
        return email_dir / f"{safe_stem or time.time_ns()}.{suffix}"

    html_path = resolve_path(sent_copy.get("html_path"), "html")
    text_path = resolve_path(sent_copy.get("text_path"), "txt")
    metadata_path = resolve_path(sent_copy.get("metadata_path"), "json")

    if html_content is not None:
        html_path.write_text(str(html_content), encoding="utf-8")
    if text_content is not None:
        text_path.write_text(str(text_content), encoding="utf-8")
    if isinstance(metadata, dict):
        host_metadata = {
            **metadata,
            "host_html_path": str(html_path),
            "host_text_path": str(text_path),
            "host_metadata_path": str(metadata_path),
        }
        metadata_path.write_text(json.dumps(host_metadata, indent=2, sort_keys=True), encoding="utf-8")
