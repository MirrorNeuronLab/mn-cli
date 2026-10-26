"""Bind native SDK gateway configuration to the watchdog that inherited it."""

import hashlib
import json
from pathlib import Path

from mn_cli.runtime_state import read_json_object, write_private_text


def _fingerprint(env):
    # Hash the allowlisted launch settings, never persist credentials or an
    # environment dump. The watchdog gives every replacement child this env.
    settings = {key: env.get(key, "") for key in (
        "OPENSHELL_GATEWAY_ENDPOINT", "OPENSHELL_GATEWAY",
        "OPENSHELL_GATEWAY_INSECURE", "OPENSHELL_CONFIG_DIR",
    )}
    return hashlib.sha256(json.dumps(settings, sort_keys=True).encode()).hexdigest()


def receipt_path(pid_file: Path) -> Path:
    return pid_file.with_suffix(".config.json")


def matches(pid_file: Path, env) -> bool:
    try:
        receipt = read_json_object(receipt_path(pid_file))
        pid = int(pid_file.read_text().strip())
    except (OSError, ValueError):
        return False
    return (receipt.get("version") == 1 and pid > 0
            and receipt.get("watchdog_pid") == pid
            and receipt.get("fingerprint") == _fingerprint(env))


def record(pid_file: Path, pid: int, env) -> None:
    write_private_text(receipt_path(pid_file), json.dumps({
        "version": 1, "watchdog_pid": pid, "fingerprint": _fingerprint(env),
    }) + "\n")
