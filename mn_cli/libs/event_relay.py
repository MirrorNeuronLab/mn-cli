from __future__ import annotations

from pathlib import Path

from mn_sdk.blueprint_support.event_relay import build_parser, run_event_relay

from mn_cli.shared import client


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    max_seconds = args.max_seconds
    if isinstance(max_seconds, float) and max_seconds <= 0:
        max_seconds = None
    return run_event_relay(
        args.job_id,
        Path(args.run_dir).expanduser(),
        client=client,
        poll_seconds=args.poll_seconds,
        max_seconds=max_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
