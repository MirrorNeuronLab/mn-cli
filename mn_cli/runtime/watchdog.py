"""Shared launch mechanics for runtime sidecar watchdogs."""

import json
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def watchdog_restart_settings(
    env: Mapping[str, str],
    prefix: str,
    *,
    defaults: tuple[Any, Any, Any, Any],
) -> dict[str, Any]:
    return {
        key: env.get(f"{prefix}_{suffix}", default)
        for key, suffix, default in (
            ("restart_delay", "RESTART_DELAY_SECONDS", defaults[0]),
            ("max_restart_delay", "MAX_RESTART_DELAY_SECONDS", defaults[1]),
            ("max_consecutive_failures", "MAX_CONSECUTIVE_FAILURES", defaults[2]),
            ("min_uptime_seconds", "MIN_UPTIME_SECONDS", defaults[3]),
        )
    }


def launch_watchdog(
    config: dict[str, Any],
    *,
    env: dict[str, str],
    watchdog_log: Path,
    script: str,
) -> subprocess.Popen:
    with open(watchdog_log, "w") as out:
        return subprocess.Popen(
            [sys.executable, "-c", script, json.dumps(config)],
            stdout=out,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=env,
            start_new_session=True,
        )
