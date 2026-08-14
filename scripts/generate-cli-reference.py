from __future__ import annotations

import argparse
from pathlib import Path

from mn_cli.command_tree import replace_generated_command_tree
from mn_cli.main import app


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update or check mn-docs/cli.md from the registered Typer command tree."
    )
    parser.add_argument("--check", action="store_true", help="Fail when the reference is stale.")
    args = parser.parse_args()

    cli_repo = Path(__file__).resolve().parents[1]
    reference = cli_repo.parent / "mn-docs" / "cli.md"
    current = reference.read_text(encoding="utf-8")
    generated = replace_generated_command_tree(current, app)
    if args.check:
        if generated != current:
            parser.error(f"{reference} is stale; run {Path(__file__).name}")
        return 0
    reference.write_text(generated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
