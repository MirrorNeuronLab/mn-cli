import json

import pytest

from mn_cli.runtime import reconnect, server
from mn_cli.libs import runtime_health
from mn_cli import output
from mn_cli.runtime.identity import resolve_identity


@pytest.mark.parametrize("host,offline", [("10.0.4.28", False), ("mini.local", False), ("127.0.0.1", True), ("", True)])
def test_refresh_changes_endpoints_without_renaming_or_restarting(tmp_path, monkeypatch, host, offline):
    name = resolve_identity(tmp_path, configured="mirror_neuron@10.0.4.27")
    env = {"MN_NODE_NAME": name, "MN_NETWORK_ADVERTISE_HOST": "10.0.4.27"}
    monkeypatch.delenv("MN_NETWORK_ADVERTISE_HOST", raising=False)
    monkeypatch.setattr(server, "DIR", tmp_path)
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_ENV", tmp_path / "docker-compose.env")
    monkeypatch.setattr(server, "runtime_compose_available", lambda: True)
    monkeypatch.setattr(server, "_runtime_base_env", lambda _: env)
    monkeypatch.setattr(server, "_detect_lan_ip", lambda: host)
    monkeypatch.setattr(runtime_health, "collect_runtime_status", lambda: {"identity": {"valid": True}})
    result = []
    monkeypatch.setattr(output, "record_result", result.append)
    monkeypatch.setattr(server.subprocess, "run", lambda *a, **k: pytest.fail("reconnect must not run Docker"))
    reconnect.reconnect()
    assert resolve_identity(tmp_path) == name
    if offline:
        assert result[0]["offline"] is True
        assert not (tmp_path / "runtime-network.json").exists()
    else:
        record = json.loads((tmp_path / "runtime-network.json").read_text())
        assert record["node_name"] == name
        assert record["host"] == host


def test_refresh_keeps_explicit_service_endpoints():
    updates = reconnect.endpoint_updates({
        "MN_NETWORK_ADVERTISE_HOST": "10.0.4.27",
        "MN_NATIVE_SDK_GRPC_ADVERTISE_HOST": "10.0.4.27",
        "MN_LITELLM_ADVERTISE_HOST": "custom.local",
        "MN_ARTIFACT_ADVERTISE_URL": "http://10.0.4.27:55660/path",
    }, "10.0.4.28")
    assert updates["MN_NATIVE_SDK_GRPC_ADVERTISE_HOST"] == "10.0.4.28"
    assert "MN_LITELLM_ADVERTISE_HOST" not in updates
    assert updates["MN_ARTIFACT_ADVERTISE_URL"] == "http://10.0.4.28:55660/path"
