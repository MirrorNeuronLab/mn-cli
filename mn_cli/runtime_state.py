from __future__ import annotations

import json
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any

from mn_cli.config import load_config


def mn_home() -> Path:
    configured_home = load_config(app_name="mn-cli").path("MN_HOME")
    return configured_home or Path.home() / ".mn"


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values

    for line in lines:
        parsed = _parse_env_assignment(line)
        if parsed is None:
            continue
        key, value = parsed
        values[key] = value
    return values


def write_env_file_values(path: Path, updates: dict[str, str]) -> None:
    try:
        original_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        original_lines = []

    lines = _updated_env_lines(original_lines, updates)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_private_text(path, "\n".join(lines) + "\n")


def remove_env_file_keys(path: Path, keys: set[str]) -> bool:
    if not keys:
        return False

    try:
        original_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False

    lines, changed = _env_lines_without_keys(original_lines, keys)
    if not changed:
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    write_private_text(path, "\n".join(lines) + ("\n" if lines else ""))
    return True


def _parse_env_assignment(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    return stripped.split("=", 1)


def _updated_env_lines(original_lines: list[str], updates: dict[str, str]) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for line in original_lines:
        parsed = _parse_env_assignment(line)
        if parsed is None or parsed[0] not in updates:
            lines.append(line)
            continue
        key, _ = parsed
        lines.append(f"{key}={updates[key]}")
        seen.add(key)

    lines.extend(f"{key}={value}" for key, value in updates.items() if key not in seen)
    return lines


def _env_lines_without_keys(original_lines: list[str], keys: set[str]) -> tuple[list[str], bool]:
    lines: list[str] = []
    changed = False
    for line in original_lines:
        parsed = _parse_env_assignment(line)
        if parsed is not None and parsed[0] in keys:
            changed = True
        else:
            lines.append(line)
    return lines, changed


def read_configured_token_file(env_name: str) -> str:
    path = os.getenv(env_name)
    if not path:
        return ""
    return read_text_stripped(Path(path).expanduser())


def read_token_file(name: str) -> str:
    return read_text_stripped(mn_home() / name)


def read_text_stripped(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def write_private_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def chmod_private(path: Path) -> None:
    with suppress(OSError):
        path.chmod(0o600)


def read_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def read_json_object(path: Path) -> dict[str, Any]:
    data = read_json_file(path)
    return data if isinstance(data, dict) else {}
