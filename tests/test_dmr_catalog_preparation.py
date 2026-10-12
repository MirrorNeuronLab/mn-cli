from mn_cli.libs import model_cmds, model_probe
from mn_sdk import resolve_model_entry


def test_gateway_adopts_sdk_prepared_entry(monkeypatch):
    original = resolve_model_entry("small")
    prepared = {**original, "managed_dmr": {"recipe": "prepared", "target": "darwin-arm64-metal"}}
    calls = []
    endpoint = {"model": original["id"], "api_base": "http://custom-owner:14000/engines/v1"}
    monkeypatch.setattr(model_cmds, "docker_model_runner_endpoint", lambda entry, **kwargs: calls.append(entry) or endpoint)
    monkeypatch.setattr(model_cmds, "sync_litellm_gateway", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(model_cmds, "reconcile_cluster_model_routes", lambda **kwargs: None)
    model_cmds._sync_installed_model_gateway_route(original, result={"entry": prepared}, node=None, strict=True)
    assert calls[0] == prepared
    assert all(value == endpoint for value in calls[1]["runtime_endpoints"].values())


def test_local_custom_probe_uses_sdk_runner_address(monkeypatch):
    entry = resolve_model_entry("small")
    calls = []
    monkeypatch.setattr(model_probe, "model_installed", lambda model: True)
    monkeypatch.setattr("mn_sdk.dmr_managed.managed_endpoint", lambda value, **kwargs: {
        "api_base": "http://127.0.0.1:14001/engines/v1", "api_key": "not-needed"})
    monkeypatch.setattr(model_probe, "docker_model_runner_endpoint", lambda value, **kwargs: calls.append(kwargs) or kwargs)
    model_probe._local_model_probe_endpoint(entry, record=None, local_node="mini")
    assert calls[0]["api_base"] == "http://127.0.0.1:14001/engines/v1"


def test_custom_installation_health_uses_sdk_diagnosis(monkeypatch):
    monkeypatch.setattr(model_cmds, "_local_runtime_node_name", lambda: "mini")
    monkeypatch.setattr("mn_sdk.model_service.doctor_runtime_model", lambda model: {
        "model": {"installed": True}, "docker_model_runner": {"running": True},
        "compatibility": {"ok": True}, "ok": True,
    })
    monkeypatch.setattr(model_cmds, "_docker_status", lambda: (_ for _ in ()).throw(AssertionError("stock DMR inspected")))
    health = model_cmds._dmr_installation_health(resolve_model_entry("small"), "mini")
    assert health["health"] == "healthy" and health["installed"] and health["running"]
