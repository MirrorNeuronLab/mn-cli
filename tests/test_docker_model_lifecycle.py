import json
from types import SimpleNamespace

import pytest
from mn_sdk import add_registered_models, dmr_registration, resolve_model_entry
from typer.testing import CliRunner

from mn_cli.libs import model_cmds, model_lifecycle, model_probe
from mn_cli.main import app


@pytest.fixture(autouse=True)
def isolated_owner(monkeypatch, tmp_path):
    monkeypatch.setenv("MN_HOME", str(tmp_path))
    monkeypatch.delenv("MN_MODEL_CATALOG_PATH", raising=False)
    monkeypatch.setattr(model_cmds, "_local_runtime_node_name", lambda: "local")


@pytest.mark.parametrize(
    "action,status", [("start", "running"), ("stop", "stopped"), ("unload", "unloaded")]
)
@pytest.mark.parametrize("plain,width", [(False, 160), (True, 48)])
def test_local_lifecycle_commands(action, status, plain, width, monkeypatch):
    calls = []
    monkeypatch.setattr(
        model_lifecycle,
        f"{action}_runtime_model",
        lambda model: (
            calls.append(model)
            or {"model": model, "source": "docker", "status": status}
        ),
    )
    env = {"COLUMNS": str(width), "MN_CLI_OUTPUT": "plain" if plain else "rich"}
    result = CliRunner().invoke(app, ["model", action, "cosmos3", "--local"], env=env)
    assert result.exit_code == 0, result.output
    assert calls == ["cosmos3"] and status in result.output


def test_remote_lifecycle_uses_selected_owner_without_local_commands(monkeypatch):
    add_registered_models(
        [dmr_registration(resolve_model_entry("cosmos3"), selected_node="spark")]
    )
    calls = []
    monkeypatch.setattr(
        model_cmds, "_cluster_node_endpoint", lambda node: {"node_name": node}
    )

    def native(endpoint):
        assert endpoint["node_name"] == "spark"
        return SimpleNamespace(
            prepare_runtime_model=lambda request: (
                calls.append(request) or json.dumps({"status": "unloaded"})
            )
        )

    monkeypatch.setattr(model_cmds, "_native_runtime_client_for_node", native)
    monkeypatch.setattr(
        model_lifecycle,
        "unload_runtime_model",
        lambda *_a: pytest.fail("local operation"),
    )
    result = CliRunner().invoke(app, ["model", "unload", "cosmos3", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["status"] == "unloaded"
    assert calls == [{"action": "unload", "model": "cosmos3", "node": "spark"}]


def test_replicated_lifecycle_requires_explicit_owner(monkeypatch):
    add_registered_models(
        [
            dmr_registration(
                resolve_model_entry("cosmos3"),
                installations=[{"node": "spark"}, {"node": "gpu-2"}],
            )
        ]
    )
    monkeypatch.setattr(
        model_lifecycle,
        "stop_runtime_model",
        lambda *_a: pytest.fail("ambiguous operation"),
    )
    result = CliRunner().invoke(app, ["model", "stop", "cosmos3"])
    assert result.exit_code == 2 and "--node" in result.output


def test_docker_discovery_and_probe_use_nim_identity_and_port(monkeypatch):
    entry = resolve_model_entry("cosmos3")
    payload = model_cmds._discovered_dmr_payload(
        entry["model"], routed_names={entry["id"]}
    )
    assert payload["kind"] == payload["source"] == "docker"
    assert payload["api_model"] == "nvidia/cosmos3-nano-reasoner"
    monkeypatch.setattr(model_probe, "model_installed", lambda model: True)
    endpoint = model_probe._local_model_probe_endpoint(
        entry, record=None, local_node="local"
    )
    assert endpoint["api_base"] == "http://127.0.0.1:30082/v1"
    assert endpoint["api_model"] == "nvidia/cosmos3-nano-reasoner"


def test_remote_docker_health_does_not_confuse_node_health_with_container_readiness(
    monkeypatch,
):
    entry = resolve_model_entry("cosmos3")
    monkeypatch.setattr(
        model_cmds,
        "_cluster_runtime_status_endpoints",
        lambda **_kw: [{"node_name": "spark", "node": {"status": "healthy"}}],
    )
    monkeypatch.setattr(
        model_cmds,
        "_runtime_model_inventory_for_node",
        lambda _endpoint: [{**entry, "running": False, "endpoint_ok": False}],
    )
    monkeypatch.setattr(
        model_cmds, "_remote_node_compatibility", lambda *_a, **_kw: {"ok": True}
    )
    health = model_cmds._dmr_installation_health(entry, "spark")
    assert (
        health["installed"]
        and health["health"] == "unavailable"
        and not health["running"]
    )


@pytest.mark.parametrize("lifecycle,running,endpoint_ok,expected", [
    ("idle", False, False, "idle"),
    ("ready", True, True, "healthy"),
    ("unavailable", True, False, "unavailable"),
])
def test_docker_health_distinguishes_cold_installation_from_failed_serving(
    lifecycle, running, endpoint_ok, expected, monkeypatch,
):
    entry = resolve_model_entry("cosmos3")
    monkeypatch.setattr(model_cmds, "_cluster_runtime_status_endpoints", lambda **_kw: [{"node_name": "spark", "node": {"status": "healthy"}}])
    monkeypatch.setattr(model_cmds, "_runtime_model_inventory_for_node", lambda _ep: [{**entry, "running": running, "endpoint_ok": endpoint_ok, "lifecycle_state": lifecycle}])
    monkeypatch.setattr(model_cmds, "_remote_node_compatibility", lambda *_a, **_kw: {"ok": True})
    health = model_cmds._dmr_installation_health(entry, "spark")
    assert health["installed"] and health["health"] == expected


@pytest.mark.parametrize("failure", [False, True])
def test_local_docker_probe_owns_readiness_and_always_releases(failure, monkeypatch):
    from contextlib import contextmanager
    from mn_sdk import ModelCapabilityReport

    entry = resolve_model_entry("cosmos3")
    calls = []

    @contextmanager
    def lease(model):
        assert model == entry["model"]
        calls.append("start")
        try:
            yield
        finally:
            calls.append("release")

    def probe(_model, required, **kwargs):
        if not kwargs["persist"]:
            assert calls == ["start"]
            if failure:
                raise RuntimeError("inference failed")
        else:
            assert calls == ["start", "release"]
        return ModelCapabilityReport(entry["id"], required, {"streaming": True})

    monkeypatch.setattr(model_probe, "model_request", lease)
    monkeypatch.setattr(model_probe, "ensure_model_capabilities", probe)
    monkeypatch.setattr(model_probe, "model_installed", lambda _model: True)
    monkeypatch.setattr(model_probe, "litellm_gateway_health", lambda: {"ok": True, "url": "http://127.0.0.1:4000/v1/models", "models": [entry["id"]]})
    if failure:
        with pytest.raises(RuntimeError, match="inference failed"):
            model_probe.run_model_probe(entry["id"], ["streaming"], local_node="local")
    else:
        assert model_probe.run_model_probe(entry["id"], ["streaming"], local_node="local")["parity"]
    assert calls == ["start", "release"]


@pytest.mark.parametrize("node", ["local", "spark"])
def test_docker_update_uses_new_catalog_context_and_replaces_snapshot(
    node, monkeypatch
):
    from mn_sdk import get_registered_model
    from mn_sdk.model_catalog import update_model_catalog_entry

    entry = resolve_model_entry("cosmos3")
    add_registered_models([dmr_registration(entry, selected_node=node)])
    record = get_registered_model(entry["id"])
    update_model_catalog_entry(
        entry["id"],
        {
            "context_size": 16384,
            "docker": {
                "environment": {
                    "NIM_MAX_MODEL_LEN": "16384",
                    "NIM_GPU_MEMORY_UTILIZATION": "0.4",
                }
            },
        },
    )
    calls = []

    def install(current, **kwargs):
        calls.append((current, kwargs))
        return {
            "entry": current,
            "docker_model": current["model"],
            "compatibility": {"backend": "nim"},
        }

    monkeypatch.setattr(model_cmds, "install_model_entry", install)
    monkeypatch.setattr(model_cmds, "_install_model_on_cluster_node", install)
    monkeypatch.setattr(
        model_cmds, "record_manual_model_install", lambda *_a, **_kw: None
    )
    monkeypatch.setattr(
        model_cmds, "_sync_installed_model_gateway_route", lambda *_a, **_kw: {}
    )
    result = model_cmds._update_dmr_registration(record, force=False, json_output=True)
    assert result["state"] == "ready" and len(calls) == 1
    assert calls[0][0]["docker"]["environment"]["NIM_MAX_MODEL_LEN"] == "16384"
    assert calls[0][1]["context_size"] == 16384
    assert get_registered_model(entry["id"])["definition"]["context_size"] == 16384
