from __future__ import annotations

import os
import sys


def color_disabled(output_mode: str | None = None) -> bool:
    return (output_mode or "").lower() == "plain" or "NO_COLOR" in os.environ


def is_ci() -> bool:
    return _truthy(os.getenv("CI")) or _truthy(os.getenv("GITHUB_ACTIONS"))


def is_interactive(stream=None) -> bool:
    stream = stream or sys.stdout
    return bool(getattr(stream, "isatty", lambda: False)())


def use_progress(stream=None) -> bool:
    return is_interactive(stream) and not is_ci()


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
