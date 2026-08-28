import importlib
from unittest.mock import call

import mn_cli.libs.sys_cmds as sys_cmds
import mn_cli.server_cmds as server_cmds


def test_runtime_cleanup_dry_run_reports_confirmed_candidates(monkeypatch):
    registry = importlib.import_module("mn_sdk.native_resource_registry")
    native_service = importlib.import_module("mn_sdk.native_runtime_service")
    calls = []
    rendered = []
    recorded = []
    summary = {
        "removed": [{"kind": "docker_worker", "external_id": "mn-dw-orphan"}],
        "removed_count": 1,
        "preserved": [{"kind": "openshell", "external_id": "mirror-neuron-job-live"}],
        "deferred": [],
        "errors": [],
        "cache": {"removed_count": 1, "reclaimed_bytes": 1024, "errors": []},
    }

    monkeypatch.setattr(native_service, "_native_resource_reference_checker", lambda: object())
    monkeypatch.setattr(
        registry,
        "reconcile_native_resources",
        lambda **kwargs: calls.append(kwargs) or summary,
    )
    monkeypatch.setattr(
        sys_cmds,
        "print_success_confirmation",
        lambda _console, action, **kwargs: rendered.append((action, kwargs)),
    )
    monkeypatch.setattr(sys_cmds, "record_result", recorded.append)

    sys_cmds.cleanup(dry_run=True, yes=False, include_cache=True, json_output=False)

    assert calls[0]["dry_run"] is True
    assert calls[0]["observation_threshold"] == 1
    assert calls[0]["discover_legacy_resources"] is True
    assert recorded[0]["removed_count"] == 1
    assert recorded[0]["cache_removed_count"] == 1
    assert recorded[0]["reclaimed_bytes"] == 1024
    assert recorded[0]["preserved_count"] == 1
    assert rendered[0][0] == "Native resource cleanup"


def test_remove_node_confirms_and_uses_the_sdk_federation_control(monkeypatch):
    calls = []
    rendered = []
    recorded = []

    class Client:
        def remove_federated_peer(self, node_name):
            calls.append(node_name)
            return "removed"

    # Some CLI import-isolation tests reload ``mn_cli.shared``. Resolve the
    # currently registered module rather than traversing a possibly stale
    # package attribute, so this test controls the same client the command
    # imports at call time.
    shared = importlib.import_module("mn_cli.shared")
    monkeypatch.setattr(shared, "client", Client())
    monkeypatch.setattr(
        sys_cmds,
        "require_confirmation",
        lambda _console, **kwargs: rendered.append(kwargs),
    )
    monkeypatch.setattr(
        sys_cmds,
        "print_success_confirmation",
        lambda _console, action, **kwargs: rendered.append((action, kwargs)),
    )
    monkeypatch.setattr(sys_cmds, "record_result", recorded.append)

    sys_cmds.remove_node("mirror_neuron@spark", yes=True)

    assert calls == ["mirror_neuron@spark"]
    assert rendered == [
        {
            "action": "Node removal",
            "prompt": (
                "Remove federated peer mirror_neuron@spark? Existing jobs stay on their "
                "owner node until the peer is joined again."
            ),
            "yes": True,
        },
        (
            "Node removal",
            {
                "status": "removed",
                "details": {"Node": "mirror_neuron@spark"},
                "next_steps": "mn node list",
            },
        ),
    ]
    assert recorded == [{"node_name": "mirror_neuron@spark", "status": "removed"}]


def test_stop_clears_join_metadata_before_teardown(monkeypatch, mocker, tmp_path):
    calls = []

    monkeypatch.setattr(sys_cmds, "_clear_join_owner_metadata", lambda: calls.append("clear-join"))
    monkeypatch.setattr(sys_cmds, "_stop_network_runtime", lambda: calls.append("network-runtime"))
    monkeypatch.setattr(sys_cmds, "runtime_compose_available", lambda: False)
    monkeypatch.setattr(sys_cmds, "web_ui_pid_files", lambda: ())
    monkeypatch.setattr(sys_cmds, "api_pid_files", lambda: ())
    monkeypatch.setattr(sys_cmds, "native_sdk_grpc_pid_files", lambda: ())
    monkeypatch.setattr(sys_cmds, "_stop_matching_sidecar_processes", lambda *_args: None)
    mocker.patch.object(sys_cmds.subprocess, "run")

    sys_cmds.stop()

    assert calls[:2] == ["clear-join", "network-runtime"]


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


def test_sidecar_runtime_env_preserves_current_ports_without_writing_compose(mocker):
    mocker.patch.object(sys_cmds, "runtime_compose_available", return_value=True)
    mocker.patch.object(
        sys_cmds,
        "_runtime_base_env",
        return_value={"MN_API_PORT": "54001", "MN_WEB_UI_PORT": "55173"},
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
    mocker.patch.object(sys_cmds, "native_sdk_grpc_pid_files", return_value=())
    stop_matching = mocker.patch.object(sys_cmds, "_stop_matching_sidecar_processes", return_value=True)

    sys_cmds.stop()

    assert stop_matching.call_args_list == [
        call("mn-api", "REST API"),
        call("mn-native-sdk-grpc", "Native SDK gRPC"),
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
            "111 /tmp/home/.local/share/mn_venv/bin/python3.11 -c\n"
            "script body with mn-api later\n"
            "222 /tmp/home/.local/share/mn_venv/bin/python3.11 /tmp/home/.local/share/mn_venv/bin/mn-api\n"
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
