from __future__ import annotations

from pathlib import Path

from mn_cli.shared import client


def _ensure_blueprint_support_path() -> None:
    import os
    import sys

    repo_root = Path(
        os.getenv("MN_WORKSPACE_ROOT")
        or Path(__file__).resolve().parents[3]
    ).expanduser()
    support_src = repo_root / "mn-skills" / "blueprint_support_skill" / "src"
    if support_src.is_dir() and str(support_src) not in sys.path:
        sys.path.insert(0, str(support_src))


_ensure_blueprint_support_path()

from mn_blueprint_support.event_relay import build_parser, run_event_relay


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
