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
            cwd=config.get("cwd"),
            stdout=out,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=env,
            start_new_session=True,
        )


def _web_ui_watchdog_script() -> str:
    return r"""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

config = json.loads(sys.argv[1])
command = config["command"]
cwd = config["cwd"]
pid_file = Path(config["pid_file"])
log_file = Path(config["log_file"])
service_name = str(config.get("service_name") or "service")

def nonnegative_float(value, default):
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return float(default)

def positive_int(value, default):
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return int(default)

restart_delay = nonnegative_float(config.get("restart_delay"), 2)
max_restart_delay = max(
    nonnegative_float(config.get("max_restart_delay"), 30),
    restart_delay,
)
max_consecutive_failures = positive_int(
    config.get("max_consecutive_failures"),
    5,
)
min_uptime_seconds = nonnegative_float(
    config.get("min_uptime_seconds"),
    10,
)
stopping = False
child = None
consecutive_failures = 0

def log(message):
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    try:
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write(f"[watchdog {timestamp}] {message}\n")
    except OSError:
        pass

def signal_child(signum):
    if child is None or child.poll() is not None:
        return
    try:
        if hasattr(os, "killpg"):
            os.killpg(child.pid, signum)
        else:
            child.send_signal(signum)
    except OSError:
        pass

def remove_owned_pid_file():
    if child is None:
        return
    try:
        if pid_file.read_text(encoding="utf-8").strip() == str(child.pid):
            pid_file.unlink()
    except OSError:
        pass

def request_stop(_signum, _frame):
    global stopping
    stopping = True
    signal_child(signal.SIGTERM)

signal.signal(signal.SIGTERM, request_stop)
signal.signal(signal.SIGINT, request_stop)

pid_file.parent.mkdir(parents=True, exist_ok=True)
log_file.parent.mkdir(parents=True, exist_ok=True)

try:
    while not stopping:
        if not pid_file.parent.is_dir():
            break
        started_at = time.monotonic()
        with log_file.open("a", encoding="utf-8", buffering=1) as output:
            child = subprocess.Popen(
                command,
                cwd=cwd,
                stdout=output,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=os.environ.copy(),
                start_new_session=True,
            )
            pid_file.write_text(str(child.pid), encoding="utf-8")
            log(f"started {service_name} child pid={child.pid}")
            while not stopping:
                exit_code = child.poll()
                if exit_code is not None:
                    break
                if not pid_file.parent.is_dir():
                    stopping = True
                    signal_child(signal.SIGTERM)
                    break
                time.sleep(1)

        if stopping:
            break

        uptime = time.monotonic() - started_at
        if uptime >= min_uptime_seconds:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
        if consecutive_failures >= max_consecutive_failures:
            log(
                f"{service_name} child exited code={exit_code} after "
                f"{uptime:.2f}s {consecutive_failures} consecutive times; "
                "watchdog circuit breaker is stopping"
            )
            break

        delay = min(
            max_restart_delay,
            restart_delay * (2 ** max(consecutive_failures - 1, 0)),
        )
        log(
            f"{service_name} child exited code={exit_code}; "
            f"restarting in {delay:g}s"
        )
        time.sleep(delay)
finally:
    if child is not None and child.poll() is None:
        signal_child(signal.SIGTERM)
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            signal_child(signal.SIGKILL)
        except OSError:
            pass
    remove_owned_pid_file()
    log(f"{service_name} watchdog stopped")
"""
