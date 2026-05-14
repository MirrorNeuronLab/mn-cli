from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from mn_cli.libs.run_logs import JobLogWriter, write_result_stream_event
from mn_cli.shared import client, logger

FINAL_STATUSES = {"completed", "failed", "cancelled"}


def run_event_relay(
    job_id: str,
    run_dir: Path,
    *,
    poll_seconds: float = 1.0,
    max_seconds: float | None = None,
) -> int:
    writer = JobLogWriter(job_id, run_dir=run_dir)
    started_at = time.monotonic()

    while True:
        try:
            data = json.loads(client.get_job(job_id))
        except Exception:
            logger.exception("Failed to poll job %s for background event relay", job_id)
            time.sleep(max(poll_seconds, 0.1))
            if max_seconds is not None and time.monotonic() - started_at >= max_seconds:
                return 1
            continue

        writer.write_snapshot(data)
        for event in reversed(data.get("recent_events") or []):
            if isinstance(event, dict) and writer.write_event(event):
                write_result_stream_event(writer.log_dir, event)
                writer.record_web_ui_url(event)

        job = data.get("job") if isinstance(data.get("job"), dict) else {}
        summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
        status = summary.get("status") or job.get("status")
        if status in FINAL_STATUSES:
            return 0

        if max_seconds is not None and time.monotonic() - started_at >= max_seconds:
            return 0

        time.sleep(max(poll_seconds, 0.1))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mirror job events into a local blueprint run store.")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--max-seconds", type=float, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    max_seconds = args.max_seconds
    if isinstance(max_seconds, float) and max_seconds <= 0:
        max_seconds = None
    return run_event_relay(
        args.job_id,
        Path(args.run_dir).expanduser(),
        poll_seconds=args.poll_seconds,
        max_seconds=max_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
