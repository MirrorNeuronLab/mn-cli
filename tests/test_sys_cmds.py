import importlib
from io import StringIO
from types import SimpleNamespace
from unittest.mock import call

from rich.console import Console

import mn_cli.libs.sys_cmds as sys_cmds
import mn_cli.server_cmds as server_cmds


def test_leave_does_not_rotate_grpc_tokens(monkeypatch, tmp_path):
    auth_file = tmp_path / "grpc_auth.token"
    admin_file = tmp_path / "grpc_admin.token"
    auth_file.write_text("stable-auth-token\n")
    admin_file.write_text("stable-admin-token\n")
    calls = []
    shared = importlib.import_module("mn_cli.shared")

    monkeypatch.setattr(shared, "client", SimpleNamespace(remove_node=lambda node: "removed"))
    monkeypatch.setattr(shared, "console", Console(file=StringIO(), force_terminal=False, width=160))
    monkeypatch.setattr(sys_cmds, "_detach_local_docker_node_if_matches", lambda node: calls.append(node))

    sys_cmds.leave("mirror_neuron@192.168.4.20")

    assert calls == ["mirror_neuron@192.168.4.20"]
    assert auth_file.read_text().strip() == "stable-auth-token"
    assert admin_file.read_text().strip() == "stable-admin-token"


def test_restart_sidecars_api_only_restarts_api_without_web_ui(mocker, tmp_path):
    api_pid_files = ((tmp_path / "api-watchdog.pid", "REST API watchdog"),)
    mocker.patch.object(sys_cmds, "api_pid_files", return_value=api_pid_files)
    mocker.patch.object(sys_cmds, "runtime_compose_available", return_value=False)
    mocker.patch.object(sys_cmds, "_runtime_base_env", return_value={})
    stop_sidecar = mocker.patch.object(sys_cmds, "_stop_sidecar_processes", return_value=True)
    stop_matching = mocker.patch.object(sys_cmds, "_stop_matching_sidecar_processes", return_value=True)
    start_api = mocker.patch.object(sys_cmds, "_start_api_if_installed", return_value=True)
    start_web_ui = mocker.patch.object(sys_cmds, "_start_web_ui_if_installed", return_value=True)
    mocker.patch.object(sys_cmds, "find_web_ui_dir", return_value=None)
    write_endpoints = mocker.patch.object(sys_cmds, "_write_runtime_endpoints_file", return_value={})

    sys_cmds.restart_sidecars(api=True, web_ui=False)

    stop_sidecar.assert_called_once_with(api_pid_files)
    stop_matching.assert_called_once_with("mn-api", "REST API")
    start_api.assert_called_once()
    env = start_api.call_args.args[0]
    assert env["MN_API_HOST"] == "localhost"
    assert env["MN_API_PORT"] == "54001"
    start_web_ui.assert_not_called()
    write_endpoints.assert_called_once_with(env, web_ui_available=False)


def test_restart_sidecars_web_ui_only_restarts_web_ui_without_api(mocker, tmp_path):
    web_pid_files = ((tmp_path / "web-ui-watchdog.pid", "Web UI watchdog"),)
    web_ui_dir = tmp_path / "web-ui"
    mocker.patch.object(sys_cmds, "web_ui_pid_files", return_value=web_pid_files)
    mocker.patch.object(sys_cmds, "runtime_compose_available", return_value=False)
    mocker.patch.object(sys_cmds, "_runtime_base_env", return_value={})
    stop_sidecar = mocker.patch.object(sys_cmds, "_stop_sidecar_processes", return_value=True)
    stop_matching = mocker.patch.object(sys_cmds, "_stop_matching_sidecar_processes", return_value=True)
    start_api = mocker.patch.object(sys_cmds, "_start_api_if_installed", return_value=True)
    start_web_ui = mocker.patch.object(sys_cmds, "_start_web_ui_if_installed", return_value=True)
    mocker.patch.object(sys_cmds, "find_web_ui_dir", return_value=web_ui_dir)
    write_endpoints = mocker.patch.object(sys_cmds, "_write_runtime_endpoints_file", return_value={})

    sys_cmds.restart_sidecars(api=False, web_ui=True)

    stop_sidecar.assert_called_once_with(web_pid_files)
    stop_matching.assert_called_once_with("mn-web-ui-server", "Web UI")
    start_web_ui.assert_called_once()
    env = start_web_ui.call_args.args[0]
    assert env["MN_WEB_UI_HOST"] == "localhost"
    assert env["MN_WEB_UI_PORT"] == "55173"
    start_api.assert_not_called()
    write_endpoints.assert_called_once_with(env, web_ui_available=True)


def test_sidecar_runtime_env_normalizes_legacy_ports_without_writing_compose(mocker):
    mocker.patch.object(sys_cmds, "runtime_compose_available", return_value=True)
    mocker.patch.object(
        sys_cmds,
        "_runtime_base_env",
        return_value={"MN_API_PORT": "4001", "MN_WEB_UI_PORT": "5173"},
    )

    env = sys_cmds._sidecar_runtime_env()

    assert env["MN_API_PORT"] == "54001"
    assert env["MN_WEB_UI_PORT"] == "55173"


def test_stop_sweeps_orphan_native_sidecars(mocker, tmp_path):
    mocker.patch.object(sys_cmds, "_stop_network_runtime")
    mocker.patch.object(sys_cmds, "runtime_compose_available", return_value=False)
    mocker.patch.object(sys_cmds.subprocess, "run")
    mocker.patch.object(sys_cmds, "web_ui_pid_files", return_value=())
    mocker.patch.object(sys_cmds, "api_pid_files", return_value=())
    mocker.patch.object(sys_cmds, "BEAM_PID_FILE", tmp_path / "beam.pid")
    stop_matching = mocker.patch.object(sys_cmds, "_stop_matching_sidecar_processes", return_value=True)

    sys_cmds.stop()

    assert stop_matching.call_args_list == [
        call("mn-api", "REST API"),
        call("mn-web-ui-server", "Web UI"),
    ]


def test_stop_sidecar_processes_kills_running_processes_and_cleans_pid_files(mocker, tmp_path):
    running_pid_file = tmp_path / "api-watchdog.pid"
    stale_pid_file = tmp_path / "api.pid"
    running_pid_file.write_text("1234")
    stale_pid_file.write_text("not-a-pid")
    mocker.patch.object(sys_cmds.os, "kill")
    kill = mocker.patch.object(sys_cmds, "kill_tree")
    sleep = mocker.patch.object(sys_cmds.time, "sleep")

    stopped = sys_cmds._stop_sidecar_processes(
        (
            (running_pid_file, "REST API watchdog"),
            (stale_pid_file, "REST API"),
        )
    )

    assert stopped is True
    kill.assert_called_once_with(1234)
    sleep.assert_called_once_with(1)
    assert not running_pid_file.exists()
    assert not stale_pid_file.exists()


def test_stop_matching_sidecar_processes_kills_pgrep_matches(mocker):
    mocker.patch.object(
        server_cmds.subprocess,
        "check_output",
        return_value=(
            "111 /Users/homer/.local/share/mn_venv/bin/python3.11 -c\n"
            "script body with mn-api later\n"
            "222 /Users/homer/.local/share/mn_venv/bin/python3.11 /Users/homer/.local/share/mn_venv/bin/mn-api\n"
            "not-a-pid noise mn-api\n"
        ),
    )
    mocker.patch.object(server_cmds.os, "getpid", return_value=999)
    kill = mocker.patch.object(server_cmds, "kill_tree")
    sleep = mocker.patch.object(server_cmds.time, "sleep")

    stopped = sys_cmds._stop_matching_sidecar_processes("mn-api", "REST API")

    assert stopped is True
    assert kill.call_args_list == [call(111), call(222)]
    sleep.assert_called_once_with(1)


def test_stop_matching_sidecar_processes_ignores_missing_pgrep(mocker):
    mocker.patch.object(server_cmds.subprocess, "check_output", side_effect=FileNotFoundError)
    kill = mocker.patch.object(server_cmds, "kill_tree")

    assert sys_cmds._stop_matching_sidecar_processes("mn-api", "REST API") is False
    kill.assert_not_called()
