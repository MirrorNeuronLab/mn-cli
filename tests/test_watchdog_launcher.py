import json
import os
import subprocess
import sys
from pathlib import Path
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
    assert kwargs["cwd"] == str(tmp_path)
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


@pytest.mark.parametrize("name", ["native_sdk_grpc", "api"])
def test_sidecars_use_runtime_home_after_installer_directory_is_removed(
    tmp_path, monkeypatch, name
):
    runtime_home = tmp_path / "runtime"
    runtime_home.mkdir()
    installer = tmp_path / "installer"
    installer.mkdir()
    monkeypatch.setattr(server, "DIR", runtime_home)
    monkeypatch.chdir(installer)
    prefix = "NATIVE_SDK_GRPC" if name == "native_sdk_grpc" else "API"
    for suffix in ("PID_FILE", "LOG", "WATCHDOG_LOG"):
        monkeypatch.setattr(server, f"{prefix}_{suffix}", runtime_home / suffix.lower())
    monkeypatch.setattr(server, f"_{name}_command", lambda: ["service-bin"])
    observed = {}

    def capture(config, **kwargs):
        observed.update(config=config, env=kwargs["env"])
        return Mock()

    monkeypatch.setattr(server, "launch_watchdog", capture)
    env = {"MN_HOME": str(runtime_home), "RESOLVED_SETTING": "preserved"}
    getattr(server, f"_start_{name}_watchdog")(env)
    monkeypatch.chdir(tmp_path)
    installer.rmdir()

    assert observed["config"]["cwd"] == str(runtime_home)
    assert observed["env"] is env
    import mn_sdk

    script = """
import json
from pathlib import Path
from mn_sdk.native_runtime_service import _resolve_runtime_model_prepare
models = [_resolve_runtime_model_prepare({"model": name, "node": "selected"}).entry["id"]
          for name in ("nemotron-3.5-lightning:latest", "gemma4:e2b")]
print(json.dumps({"cwd": str(Path.cwd()), "models": models}))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=observed["config"]["cwd"],
        env={
            **observed["env"],
            "PYTHONPATH": str(Path(mn_sdk.__file__).resolve().parent.parent),
        },
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    assert json.loads(result.stdout) == {
        "cwd": str(runtime_home.resolve()),
        "models": ["nemotron-3.5-lightning:latest", "gemma4:e2b"],
    }


def test_sidecar_workdir_does_not_read_the_callers_cwd(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "DIR", tmp_path)

    def missing_cwd():
        raise FileNotFoundError("installer was removed")

    monkeypatch.setattr(server.Path, "cwd", missing_cwd)
    assert server._sidecar_workdir() == tmp_path


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows cannot remove an active cwd"
)
def test_watchdog_can_launch_when_the_callers_directory_was_removed(
    tmp_path, monkeypatch
):
    runtime_home = tmp_path / "runtime"
    runtime_home.mkdir()
    installer = tmp_path / "installer"
    installer.mkdir()
    monkeypatch.chdir(installer)
    installer.rmdir()
    log = runtime_home / "watchdog.log"
    try:
        process = watchdog.launch_watchdog(
            {"cwd": str(runtime_home)},
            env={},
            watchdog_log=log,
            script="from pathlib import Path; print(Path.cwd())",
        )
        try:
            assert process.wait(timeout=5) == 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    finally:
        monkeypatch.chdir(tmp_path)
    assert log.read_text().strip() == str(runtime_home.resolve())


@pytest.mark.parametrize("name", ["native_sdk_grpc", "api"])
def test_sidecars_preserve_caller_relative_config_paths(tmp_path, monkeypatch, name):
    runtime_home = tmp_path / "runtime"
    runtime_home.mkdir()
    caller = tmp_path / "caller"
    caller.mkdir()
    monkeypatch.chdir(caller)
    monkeypatch.setattr(server, "DIR", runtime_home)
    prefix = "NATIVE_SDK_GRPC" if name == "native_sdk_grpc" else "API"
    for suffix in ("PID_FILE", "LOG", "WATCHDOG_LOG"):
        monkeypatch.setattr(server, f"{prefix}_{suffix}", runtime_home / suffix.lower())
    monkeypatch.setattr(server, f"_{name}_command", lambda: ["service-bin"])
    launch = Mock()
    monkeypatch.setattr(server, "launch_watchdog", launch)
    env = {
        "MN_HOME": "../runtime",
        "MN_BLUEPRINT_LOCAL": "../blueprints",
        "MN_WORKSPACE_ROOT": "../workspace",
        "MN_MODEL_CATALOG_PATH": "../catalog.json",
        "MN_SDK_LOG_PATH": "",
        "MN_API_BASE_URL": "http://127.0.0.1:55050/api/v1",
    }
    original = env.copy()
    getattr(server, f"_start_{name}_watchdog")(env)
    prepared = launch.call_args.kwargs["env"]
    for key in (
        "MN_HOME",
        "MN_BLUEPRINT_LOCAL",
        "MN_WORKSPACE_ROOT",
        "MN_MODEL_CATALOG_PATH",
    ):
        assert prepared[key] == str((caller / env[key]).resolve())
    assert prepared["MN_SDK_LOG_PATH"] == ""
    assert prepared["MN_API_BASE_URL"] == env["MN_API_BASE_URL"]
    assert env == original


def test_relative_runtime_home_is_resolved_before_sidecar_launch(tmp_path):
    caller = tmp_path / "caller"
    caller.mkdir()
    env = os.environ.copy()
    env["MN_HOME"] = "../runtime"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from mn_cli.runtime.server import DIR, API_PID_FILE; print(DIR); print(API_PID_FILE)",
        ],
        cwd=caller,
        env={**env, "PYTHONPATH": str(Path(server.__file__).resolve().parents[2])},
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    assert result.stdout.splitlines() == [
        str(tmp_path / "runtime"),
        str(tmp_path / "runtime" / "pids" / "api.pid"),
    ]
