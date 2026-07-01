from __future__ import annotations

import os
import shutil
import sys


def terminal_columns(*, default: int = 120) -> int:
    """Return detected terminal width with a safe fallback."""
    env_width = os.environ.get("COLUMNS")
    if env_width:
        try:
            parsed = int(env_width)
            if parsed > 0:
                return parsed
        except ValueError:
            pass

    try:
        return shutil.get_terminal_size(fallback=(default, 24)).columns
    except Exception:
        return default


def ui_width(*, default: int = 120, cap: int = 120, minimum: int = 40) -> int:
    """Return a terminal-aware width cap for rich/typer layouts."""
    width = terminal_columns(default=default)
    if cap and cap > 0:
        width = min(width, cap)
    if minimum and minimum > 0:
        width = max(width, minimum)
    return width


def truncate_for_width(value: object, width: int | None, suffix: str = "…") -> str:
    if value is None:
        return ""
    if width is None or width <= 0:
        return str(value)
    text = str(value)
    if len(text) <= width:
        return text
    if width <= 1:
        return suffix[:width]
    trim_width = width - len(suffix)
    if trim_width <= 0:
        return suffix[:width]
    head = text[: trim_width - 1].rstrip()
    if not head:
        return suffix[:width]
    return f"{head} {suffix}"


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
