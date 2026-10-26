import json
from types import SimpleNamespace

import pytest

from mn_cli.runtime import native_sdk_config, server


@pytest.mark.parametrize("change", ["endpoint", "pid", "missing", "malformed", "insecure"])
def test_launch_receipt_rejects_stale_configuration_and_identity(tmp_path, change):
    pid = tmp_path / "native-sdk-grpc-watchdog.pid"
    pid.write_text("10")
    env = {"OPENSHELL_GATEWAY_ENDPOINT": "http://old.test:58080"}
    native_sdk_config.record(pid, 10, env)
    assert native_sdk_config.matches(pid, env)
    if change == "endpoint":
        env["OPENSHELL_GATEWAY_ENDPOINT"] = "http://new.test:58080"
    elif change == "pid":
        pid.write_text("11")
    elif change == "missing":
        native_sdk_config.receipt_path(pid).unlink()
    elif change == "malformed":
        native_sdk_config.receipt_path(pid).write_text("[]")
    else:
        env["OPENSHELL_GATEWAY_INSECURE"] = "true"
    assert not native_sdk_config.matches(pid, env)


def test_receipt_contains_only_version_pid_and_fingerprint(tmp_path):
    pid = tmp_path / "service.pid"
    env = {"OPENSHELL_GATEWAY_ENDPOINT": "https://user:private@gateway.test?token=secret",
           "MN_API_TOKEN": "private-token"}
    native_sdk_config.record(pid, 10, env)
    saved = native_sdk_config.receipt_path(pid).read_text()
    assert set(json.loads(saved)) == {"version", "watchdog_pid", "fingerprint"}
    assert not any(value in saved for value in ("private", "secret", "gateway.test"))


@pytest.mark.parametrize("previous", ["same", "old", "unrecorded"])
@pytest.mark.parametrize("endpoint", ["http://bridge.test:58080", "https://custom.test:8443"])
def test_responsive_native_service_is_reused_only_with_matching_config(tmp_path, monkeypatch, previous, endpoint):
    for key in ("OPENSHELL_GATEWAY", "OPENSHELL_GATEWAY_INSECURE", "OPENSHELL_CONFIG_DIR"):
        monkeypatch.delenv(key, raising=False)
    watchdog = tmp_path / "native-sdk-watchdog.pid"
    child = tmp_path / "native-sdk.pid"
    watchdog.write_text("99")
    child.write_text("100")
    monkeypatch.setattr(server, "NATIVE_SDK_GRPC_WATCHDOG_PID_FILE", watchdog)
    monkeypatch.setattr(server, "NATIVE_SDK_GRPC_PID_FILE", child)
    monkeypatch.setattr(server, "_native_sdk_grpc_command", lambda: ["native-sdk"])
    monkeypatch.setattr(server, "_source_checkout_sdk_dir", lambda: None)
    monkeypatch.setattr(server, "check_status", lambda _path: 0)
    monkeypatch.setattr(server, "_wait_for_tcp", lambda *_a, **_kw: True)
    monkeypatch.setattr(server.time, "sleep", lambda _: None)
    killed, launched = [], []
    monkeypatch.setattr(server, "kill_tree", killed.append)
    monkeypatch.setattr(server, "stop_matching_sidecar_processes", lambda *_: None)
    def launch(env):
        launched.append(env)
        return SimpleNamespace(pid=200)
    monkeypatch.setattr(server, "_start_native_sdk_grpc_watchdog", launch)
    expected = {"OPENSHELL_GATEWAY_ENDPOINT": endpoint}
    if previous != "unrecorded":
        native_sdk_config.record(watchdog, 99, expected if previous == "same" else {
            "OPENSHELL_GATEWAY_ENDPOINT": "http://127.0.0.1:58080"})
    assert server._start_native_sdk_grpc_if_installed(expected)
    if previous == "same":
        assert killed == launched == []
    else:
        assert killed == [99]
        assert launched[0]["OPENSHELL_GATEWAY_ENDPOINT"] == expected["OPENSHELL_GATEWAY_ENDPOINT"]
        assert native_sdk_config.matches(watchdog, launched[0])
