"""Command-line value parsing, without runtime preparation."""

import json
import os
import re
from typing import Any
import typer
from mn_cli.libs.ui import print_error
from mn_cli.shared import console


def _config_path_set(config: dict[str, Any], dotted_path: str, value: Any) -> None:
    cursor = config
    parts = [part for part in dotted_path.split(".") if part]
    for part in parts[:-1]:
        next_value = cursor.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            cursor[part] = next_value
        cursor = next_value
    if parts:
        cursor[parts[-1]] = value


def _parse_config_set_overrides(values: list[str] | None) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for assignment in values or []:
        if "=" not in assignment:
            raise ValueError(
                f"Invalid --set value {assignment!r}; expected dotted.path=value."
            )
        raw_path, raw_value = assignment.split("=", 1)
        path = raw_path.strip()
        if not path or any(not part for part in path.split(".")):
            raise ValueError(
                f"Invalid --set path {raw_path!r}; expected non-empty dotted path segments."
            )
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value
        _config_path_set(overrides, path, value)
    return overrides


def _run_schedule_attrs(
    *, auto_schedule: bool, schedule: str | None
) -> dict[str, Any] | None:
    if auto_schedule and schedule:
        print_error(console, "Pass either --auto-schedule or --schedule, not both.")
        raise typer.Exit(1)
    if auto_schedule:
        return {
            "kind": "resource_wait",
            "retry_interval_ms": int(os.getenv("MN_RESOURCE_WAIT_RETRY_MS", "30000")),
            "metadata": {"requested_by": "mn run --auto-schedule"},
        }
    if not schedule:
        return None

    raw = schedule.strip()
    if not raw:
        raise typer.BadParameter("--schedule cannot be empty")
    if raw.startswith("{"):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter("--schedule JSON must be valid") from exc
        if not isinstance(parsed, dict):
            raise typer.BadParameter("--schedule JSON must be an object")
        return parsed
    if raw.lower() in {"auto", "resource", "resource_wait", "resource-wait"}:
        return {
            "kind": "resource_wait",
            "retry_interval_ms": int(os.getenv("MN_RESOURCE_WAIT_RETRY_MS", "30000")),
            "metadata": {"requested_by": "mn run --schedule"},
        }
    if re.fullmatch(r"\d+(\.\d+)?(ms|s|m|h|d)?", raw.lower()):
        return {
            "kind": "delayed",
            "delay_ms": _duration_ms_for_schedule(raw),
            "metadata": {"requested_by": "mn run --schedule"},
        }
    return {
        "kind": "delayed",
        "run_at": raw,
        "metadata": {"requested_by": "mn run --schedule"},
    }


def _duration_ms_for_schedule(value: str) -> int:
    raw = str(value or "").strip().lower()
    units = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
    for suffix, multiplier in units.items():
        if raw.endswith(suffix):
            return int(float(raw[: -len(suffix)]) * multiplier)
    return int(float(raw) * 1000)
