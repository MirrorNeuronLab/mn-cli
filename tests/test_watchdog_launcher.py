import json
import subprocess
import sys
from unittest.mock import Mock

import pytest

from mn_cli.runtime import server, watchdog


@pytest.mark.parametrize(
    ("name", "prefix", "service"),
    [
        ("native_sdk_grpc", "MN_NATIVE_SDK_GRPC", "Native SDK gRPC"),
        ("api", "MN_API", "REST API"),
        ("web_ui", "MN_WEB_UI", "Web UI"),
    ],
)
def test_service_wrappers_preserve_launch_contract(
    tmp_path, monkeypatch, name, prefix, service
):
    constant = prefix.removeprefix("MN_")
    for suffix in ("PID_FILE", "LOG", "WATCHDOG_LOG"):
        monkeypatch.setattr(server, f"{constant}_{suffix}", tmp_path / suffix.lower())
    command = ["service-bin", "--test"]
    monkeypatch.setattr(server, f"_{name}_command", lambda *args: command)
    monkeypatch.setattr(server, "_sidecar_workdir", lambda: tmp_path)
    monkeypatch.setattr(server, "_web_ui_watchdog_script", lambda: "watchdog-script")
    env = {
        f"{prefix}_RESTART_DELAY_SECONDS": "7",
        f"{prefix}_MAX_RESTART_DELAY_SECONDS": "17",
        f"{prefix}_MAX_CONSECUTIVE_FAILURES": "4",
        f"{prefix}_MIN_UPTIME_SECONDS": "22",
        "KEEP_ME": "yes",
    }
    process = Mock()
    observed = {}

    def spawn(args, **kwargs):
        observed.update(args=args, kwargs=kwargs, open_log=not kwargs["stdout"].closed)
        return process

    monkeypatch.setattr(server.subprocess, "Popen", spawn)
    launcher = getattr(server, f"_start_{name}_watchdog")
    result = (
        launcher(tmp_path, env, "localhost", "8080")
        if name == "web_ui"
        else launcher(env)
    )
    assert result is process
    assert observed["args"][:3] == [sys.executable, "-c", "watchdog-script"]
    assert json.loads(observed["args"][3]) == {
        "command": command,
        "cwd": str(tmp_path),
        "pid_file": str(tmp_path / "pid_file"),
        "log_file": str(tmp_path / "log"),
        "service_name": service,
        "restart_delay": "7",
        "max_restart_delay": "17",
        "max_consecutive_failures": "4",
        "min_uptime_seconds": "22",
    }
    kwargs = observed["kwargs"]
    assert kwargs["env"] is env
    assert kwargs["stdin"] == subprocess.DEVNULL
    assert kwargs["stderr"] == subprocess.STDOUT
    assert kwargs["start_new_session"] is True
    assert observed["open_log"] and kwargs["stdout"].closed


def test_restart_defaults_and_partial_override():
    assert watchdog.watchdog_restart_settings(
        {
            "MN_API_MAX_CONSECUTIVE_FAILURES": "9",
            "MN_WEB_UI_RESTART_DELAY_SECONDS": "99",
        },
        "MN_API",
        defaults=(1, 2, 3, 4),
    ) == {
        "restart_delay": 1,
        "max_restart_delay": 2,
        "max_consecutive_failures": "9",
        "min_uptime_seconds": 4,
    }


def test_spawn_failure_closes_log_and_propagates(tmp_path, monkeypatch):
    outputs = []

    def fail(*args, **kwargs):
        outputs.append(kwargs["stdout"])
        raise OSError("spawn failed")

    monkeypatch.setattr(watchdog.subprocess, "Popen", fail)
    log = tmp_path / "watchdog.log"
    log.write_text("old log")
    with pytest.raises(OSError, match="spawn failed"):
        watchdog.launch_watchdog({}, env={}, watchdog_log=log, script="script")
    assert outputs[0].closed
    assert log.read_text() == ""
