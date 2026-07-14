import json
import subprocess
import urllib.error
from pathlib import Path

import pytest
from typer.testing import CliRunner

from mn_cli.main import app
from mn_cli.libs import model_cmds
from mn_sdk import HostHardwareProfile, load_model_ownership, load_model_proxies, load_model_remotes, upsert_model_remote


runner = CliRunner()


@pytest.fixture(autouse=True)
def isolate_model_ownership(monkeypatch, tmp_path):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    monkeypatch.setenv("MN_MODEL_REMOTES_PATH", str(tmp_path / "model-remotes.json"))
    monkeypatch.setenv("MN_MODEL_PROXIES_PATH", str(tmp_path / "model-proxies.json"))
    monkeypatch.setattr("mn_cli.libs.model_cmds._endpoint_responds", lambda: False)
    monkeypatch.setattr("mn_sdk.model_service.endpoint_responds", lambda: False)
    monkeypatch.setattr("mn_cli.libs.model_cmds._selected_model_install_node", lambda: None)
    monkeypatch.setattr("mn_cli.libs.model_cmds._record_runtime_model_install", lambda _entry: None)


def _completed(command, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(command, returncode, stdout, stderr)


class FakeResponse:
    def __init__(self, body="{}"):
        self.body = body.encode("utf-8")
        self.status = 200
        self._offset = 0

    def __enter__(self):
        self._offset = 0
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self.body

    def readline(self):
        if self._offset >= len(self.body):
            return b""
        newline = self.body.find(b"\n", self._offset)
        if newline == -1:
            chunk = self.body[self._offset :]
            self._offset = len(self.body)
            return chunk
        chunk = self.body[self._offset : newline + 1]
        self._offset = newline + 1
        return chunk


def _cluster_node(name, host, *, self_node=False, grpc_port=55051, native_port=55052):
    return {
        "name": name,
        "grpc_host": host,
        "grpc_port": grpc_port,
        "self": self_node,
        "native_sdk_grpc": {
            "target": f"{host}:{native_port}",
            "host": host,
            "port": native_port,
        },
    }


def _cluster_summary(*nodes):
    return json.dumps({"nodes": list(nodes)})


def test_model_list_prints_builtin_catalog_json(mocker):
    mocker.patch("mn_cli.libs.model_cmds._installed_model_names", return_value=set())

    result = runner.invoke(app, ["model", "list", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    gemma = next(model for model in payload["models"] if model["id"] == "gemma4:e2b")
    assert gemma["model"] == "docker.io/ai/gemma4:E2B"
    assert gemma["default"] is True
    assert gemma["status"] == "default"


def test_model_show_resolves_gemme_alias(mocker):
    mocker.patch("mn_cli.libs.model_cmds._model_installed", return_value=False)

    result = runner.invoke(app, ["model", "show", "gemme4:e2b", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["id"] == "gemma4:e2b"
    assert payload["model"] == "docker.io/ai/gemma4:E2B"
    assert payload["default"] is True
    assert payload["status"] == "default"


def test_model_show_does_not_require_docker_binary(mocker):
    mocker.patch("subprocess.run", side_effect=FileNotFoundError("docker"))
    mocker.patch("mn_cli.libs.model_cmds._model_installed", return_value=False)

    result = runner.invoke(app, ["model", "show", "gemma4:e2b", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["installed"] is False


def test_model_remote_add_list_remove_json(mocker):
    mocker.patch("mn_cli.libs.model_cmds._sync_gateway_best_effort")
    mocker.patch("mn_cli.libs.model_cmds._sync_gateway_runtime_endpoints_across_cluster", return_value=[])

    add = runner.invoke(
        app,
        [
            "model",
            "remote",
            "add",
            "ai/qwen3-coder",
            "--base-url",
            "http://192.168.4.173:12434/v1",
            "--name",
            "spark",
            "--json",
        ],
    )

    assert add.exit_code == 0
    added = json.loads(add.stdout)
    assert added["remote"]["name"] == "spark"
    assert added["remote"]["model"] == "ai/qwen3-coder"
    assert load_model_remotes()["remotes"]["spark"]["base_url"] == "http://192.168.4.173:12434/v1"

    listed = runner.invoke(app, ["model", "remote", "list", "--json"])
    assert listed.exit_code == 0
    assert json.loads(listed.stdout)["remotes"][0]["name"] == "spark"

    removed = runner.invoke(app, ["model", "remote", "remove", "spark", "--json"])
    assert removed.exit_code == 0
    assert json.loads(removed.stdout)["removed"]["name"] == "spark"
    assert load_model_remotes()["remotes"] == {}


def test_cluster_node_is_local_uses_local_host_alias(monkeypatch):
    node_endpoint = {
        "node": {"name": "mirror_neuron@192.168.6.28"},
        "host": "192.168.6.28",
        "port": "55052",
        "self": False,
    }
    monkeypatch.setattr("mn_cli.libs.model_cmds._local_host_addresses", lambda: {"192.168.6.28"})

    assert model_cmds._cluster_node_is_local(node_endpoint) is True


def test_cluster_node_is_not_local_for_remote_host(monkeypatch):
    node_endpoint = {
        "node": {"name": "mirror_neuron@192.168.4.173"},
        "host": "192.168.4.173",
        "port": "55052",
        "self": False,
    }
    monkeypatch.setattr("mn_cli.libs.model_cmds._local_host_addresses", lambda: {"192.168.6.28"})

    assert model_cmds._cluster_node_is_local(node_endpoint) is False


def test_install_model_on_cluster_node_uses_local_runtime_client_for_local_host(mocker):
    model = {"id": "gemma4:e2b", "model": "docker.io/ai/gemma4:E2B", "provider": "docker_model_runner"}
    node = "mirror_neuron@192.168.6.28"

    def fake_cluster_node_endpoint(_node_name: str):
        return {
            "grpc_target": "192.168.6.28:55051",
            "host": "192.168.6.28",
            "port": "55051",
            "node": {"name": node, "grpc_host": "192.168.6.28", "grpc_port": 55051},
            "node_name": node,
        }

    mocker.patch("mn_cli.libs.model_cmds._cluster_node_endpoint", side_effect=fake_cluster_node_endpoint)
    mocker.patch("mn_cli.libs.model_cmds._local_host_addresses", return_value={"192.168.6.28"})
    mocker.patch(
        "mn_cli.libs.model_cmds.Client",
        side_effect=AssertionError("should use local runtime client for local host"),
    )
    fake_runtime_client = mocker.Mock()
    fake_runtime_client.prepare_runtime_model.return_value = json.dumps(
        {
            "status": "installed",
            "install": {"compatibility": {"backend": "llama.cpp"}},
            "endpoint": {"api_base": "http://mn-litellm-proxy:4000/v1"},
        }
    )
    mocker.patch("mn_cli.libs.model_cmds.client", fake_runtime_client)
    mocker.patch("mn_cli.libs.model_cmds._sync_gateway_best_effort")
    mocker.patch("mn_cli.libs.model_cmds._cluster_gateway_endpoint", return_value={"api_base": "http://mn-litellm-proxy:4000/v1"})

    result = model_cmds._install_model_on_cluster_node(model, node=node, backend="llama.cpp", context_size=None, force=False)

    assert result["entry"] == model
    assert result["transport"] == "runtime_node_grpc"
    fake_runtime_client.prepare_runtime_model.assert_called_once()


def test_model_proxy_registers_provider_config_without_start(tmp_path, mocker):
    mocker.patch("mn_cli.libs.model_cmds._installed_model_names", return_value=set())
    gateway_config = tmp_path / "gateway.json"

    def fake_sync_litellm_gateway(**kwargs):
        gateway_config.write_text(json.dumps(kwargs["external_litellm_config"]), encoding="utf-8")
        return {"config_path": str(gateway_config)}

    mocker.patch("mn_cli.libs.model_cmds.sync_litellm_gateway", side_effect=fake_sync_litellm_gateway)
    mocker.patch("mn_cli.libs.model_cmds._sync_external_litellm_config_across_cluster", return_value=[])
    config = tmp_path / "openai-compatible.json"
    config.write_text(
        json.dumps(
            {
                "provider": {
                    "openai-compatible": {
                        "name": "OpenAI-compatible endpoint",
                        "options": {
                            "baseURL": "https://api.openai.com/v1",
                            "apiKeyEnv": "OPENAI_API_KEY",
                        },
                        "models": {
                            "openai/gpt-5.4-mini": {
                                "name": "GPT 5.4 Mini",
                                "model": "openai/gpt-5.4-mini",
                                "rate_limit_rpm": 30,
                                "timeout_seconds": 120,
                            }
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["model", "proxy", "--config", str(config), "--no-start", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "registered"
    assert payload["service"] == "mn-litellm-proxy"
    assert payload["base_url"] == "http://mn-litellm-proxy:4000/v1"
    assert payload["models"][0]["id"] == "openai/gpt-5.4-mini"
    assert payload["models"][0]["backend"] == "proxy"
    generated = json.loads(Path(payload["config"]).read_text(encoding="utf-8"))
    assert generated["model_list"][0]["model_name"] == "openai/gpt-5.4-mini"
    assert generated["model_list"][0]["litellm_params"]["api_key"] == "os.environ/OPENAI_API_KEY"
    assert load_model_proxies()["proxies"]["openai-gpt-5.4-mini"]["backend"] == "proxy"

    listed = runner.invoke(app, ["model", "list", "--installed", "--json"])
    assert listed.exit_code == 0
    proxy_models = [model for model in json.loads(listed.stdout)["models"] if model["id"] == "openai/gpt-5.4-mini"]
    assert len(proxy_models) == 1
    model = proxy_models[0]
    assert model["id"] == "openai/gpt-5.4-mini"
    assert model["backend"] == "proxy"
    assert model["installed"] is True
    assert model["route_source"] == "external-proxy"


def test_model_proxy_syncs_external_provider_config_to_all_cluster_nodes(tmp_path, mocker):
    local_syncs = []
    node_syncs = []
    mocker.patch("mn_cli.libs.model_cmds._installed_model_names", return_value=set())
    mocker.patch(
        "mn_cli.libs.model_cmds.client.get_system_summary",
        return_value=_cluster_summary(
            _cluster_node("local", "10.0.0.1", self_node=True),
            _cluster_node("spark", "10.0.0.2"),
        ),
    )
    mocker.patch(
        "mn_cli.libs.model_cmds.sync_litellm_gateway",
        side_effect=lambda **kwargs: local_syncs.append(kwargs)
        or {"status": "registered", "config_path": str(tmp_path / "gateway.yaml")},
    )

    class FakeClient:
        def __init__(self, **kwargs):
            self.target = kwargs.get("target")

        def sync_litellm_gateway(self, payload):
            node_syncs.append((self.target, payload))
            return json.dumps({"status": "registered"})

    mocker.patch("mn_cli.libs.model_cmds.Client", FakeClient)
    config = tmp_path / "openai-compatible.json"
    config.write_text(
        json.dumps(
            {
                "provider": {
                    "openai-compatible": {
                        "options": {"baseURL": "https://api.openai.com/v1", "apiKeyEnv": "OPENAI_API_KEY"},
                        "models": {"openai/gpt-5.4-mini": {"model": "openai/gpt-5.4-mini"}},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["model", "proxy", "--config", str(config), "--no-start", "--json"])

    assert result.exit_code == 0
    assert local_syncs[0]["external_litellm_config"]["model_list"][0]["litellm_params"]["api_key"] == "os.environ/OPENAI_API_KEY"
    assert {target for target, _payload in node_syncs} == {"10.0.0.2:55052"}
    for _target, payload in node_syncs:
        params = payload["external_litellm_config"]["model_list"][0]["litellm_params"]
        assert params["api_base"] == "https://api.openai.com/v1"
        assert params["api_key"] == "os.environ/OPENAI_API_KEY"
        assert payload["restart"] is False


def test_model_install_pulls_and_runs_compatible_model(mocker):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n      --context-size int\n")
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds._sync_installed_model_gateway_route")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b", "--context-size", "8192"])

    assert result.exit_code == 0
    assert "Model install successful." in result.stdout
    assert "gemma4:e2b" in result.stdout
    assert ["docker", "model", "pull", "docker.io/ai/gemma4:E2B"] in calls
    assert ["docker", "model", "run", "--detach", "--context-size", "8192", "docker.io/ai/gemma4:E2B"] in calls


def test_model_install_syncs_local_dmr_gateway_route(mocker):
    synced = []

    def fake_run(command, **kwargs):
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n")
        return _completed(command)

    def fake_sync(**kwargs):
        synced.append(kwargs)
        return {"status": "running", "config_path": "config.yaml", "models": ["gemma4:e2b"]}

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds.sync_litellm_gateway", side_effect=fake_sync)
    mocker.patch("mn_cli.libs.model_cmds._sync_model_route_across_cluster")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 0
    assert synced
    endpoints = synced[0]["runtime_endpoints"]
    assert endpoints["gemma4:e2b"]["api_base"] == "http://host.docker.internal:12434/engines/v1"
    assert "docker.io/ai/gemma4:E2B" in load_model_ownership()["models"]


def test_model_install_local_dmr_fans_out_gateway_route_to_other_nodes(mocker):
    local_syncs = []
    node_syncs = []

    def fake_run(command, **kwargs):
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n")
        return _completed(command)

    class FakeClient:
        def __init__(self, **kwargs):
            self.target = kwargs.get("target")

        def sync_litellm_gateway(self, payload):
            node_syncs.append((self.target, payload))
            return json.dumps({"status": "running"})

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds.Client", FakeClient)
    mocker.patch(
        "mn_cli.libs.model_cmds.client.get_system_summary",
        return_value=_cluster_summary(
            _cluster_node("local", "10.0.0.1", self_node=True),
            _cluster_node("spark", "10.0.0.2"),
        ),
    )
    mocker.patch("mn_cli.libs.model_cmds.sync_litellm_gateway", side_effect=lambda **kwargs: local_syncs.append(kwargs) or {})
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b", "--local"])

    assert result.exit_code == 0
    assert local_syncs[0]["runtime_endpoints"]["gemma4:e2b"]["api_base"] == "http://host.docker.internal:12434/engines/v1"
    assert len(node_syncs) == 1
    target, payload = node_syncs[0]
    assert target == "10.0.0.2:55052"
    endpoint = payload["runtime_endpoints"]["gemma4:e2b"]
    assert endpoint["api_base"] == "http://10.0.0.1:4000/v1"
    assert endpoint["source"] == "remote-dmr"


def test_model_install_node_uses_prepare_runtime_model_not_ssh(mocker):
    calls = []
    synced = []

    class FakeClient:
        def __init__(self, **kwargs):
            calls.append(("client", kwargs))

        def prepare_runtime_model(self, payload):
            calls.append(("prepare", payload))
            return json.dumps(
                {
                    "status": "installed",
                    "docker_model": "docker.io/ai/gemma4:E2B",
                    "install": {"compatibility": {"backend": "llama.cpp", "warnings": []}},
                    "gateway": {"host_api_base": "http://127.0.0.1:4000/v1"},
                    "endpoint": {
                        "provider": "docker_model_runner",
                        "model": "gemma4:E2B",
                        "runtime_model": "docker.io/ai/gemma4:E2B",
                        "api_model": "gemma4:E2B",
                        "api_base": "http://mn-litellm-proxy:4000/v1",
                        "node": "spark",
                    },
                }
            )

    mocker.patch(
        "mn_cli.libs.model_cmds.client.get_system_summary",
        return_value=json.dumps(
            {
                "nodes": [
                    {
                        "name": "spark",
                        "grpc_host": "192.168.4.173",
                        "grpc_port": 55051,
                        "native_sdk_grpc": {"target": "192.168.4.173:55052", "host": "192.168.4.173", "port": 55052},
                    }
                ]
            }
        ),
    )
    mocker.patch("mn_cli.libs.model_cmds.Client", FakeClient)
    mocker.patch("mn_cli.libs.model_cmds.sync_litellm_gateway", side_effect=lambda **kwargs: synced.append(kwargs) or {})
    mocker.patch("subprocess.run", side_effect=AssertionError("remote install must not shell out"))

    result = runner.invoke(app, ["model", "install", "gemma4:e2b", "--node", "spark"])

    assert result.exit_code == 0
    prepare_payload = [payload for kind, payload in calls if kind == "prepare"][0]
    assert prepare_payload["model"] == "docker.io/ai/gemma4:E2B"
    assert prepare_payload["source"] == "mn-cli"
    assert synced[0]["runtime_endpoints"]["gemma4:e2b"]["api_base"] == "http://192.168.4.173:4000/v1"
    assert load_model_ownership()["models"] == {}
    remotes = load_model_remotes()["remotes"]
    assert remotes["spark-gemma4-e2b"]["base_url"] == "http://192.168.4.173:4000/v1"
    assert remotes["spark-gemma4-e2b"]["node"] == "spark"


def test_model_update_refreshes_local_dmr_gateway_route(mocker):
    calls = []
    synced = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds.sync_litellm_gateway", side_effect=lambda **kwargs: synced.append(kwargs) or {})
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "update", "gemma4:e2b"])

    assert result.exit_code == 0
    assert ["docker", "model", "pull", "docker.io/ai/gemma4:E2B"] in calls
    assert ["docker", "model", "run", "--detach", "docker.io/ai/gemma4:E2B"] in calls
    assert synced[0]["runtime_endpoints"]["gemma4:e2b"]["api_base"] == "http://host.docker.internal:12434/engines/v1"
    assert "docker.io/ai/gemma4:E2B" in load_model_ownership()["models"]


def test_model_install_node_fans_out_gateway_route_to_other_nodes(mocker):
    calls = []
    local_syncs = []
    node_syncs = []

    class FakeClient:
        def __init__(self, **kwargs):
            self.target = kwargs.get("target")

        def prepare_runtime_model(self, payload):
            calls.append(("prepare", self.target, payload))
            return json.dumps(
                {
                    "status": "installed",
                    "docker_model": "docker.io/ai/gemma4:E2B",
                    "install": {"compatibility": {"backend": "llama.cpp", "warnings": []}},
                    "gateway": {"host_api_base": "http://127.0.0.1:4000/v1"},
                    "endpoint": {"api_model": "gemma4:E2B"},
                }
            )

        def sync_litellm_gateway(self, payload):
            node_syncs.append((self.target, payload))
            return json.dumps({"status": "running"})

    mocker.patch(
        "mn_cli.libs.model_cmds.client.get_system_summary",
        return_value=_cluster_summary(
            _cluster_node("spark", "192.168.4.173"),
            _cluster_node("moon", "192.168.4.174"),
        ),
    )
    mocker.patch("mn_cli.libs.model_cmds.Client", FakeClient)
    mocker.patch("mn_cli.libs.model_cmds.sync_litellm_gateway", side_effect=lambda **kwargs: local_syncs.append(kwargs) or {})
    mocker.patch("subprocess.run", side_effect=AssertionError("remote install must not shell out"))

    result = runner.invoke(app, ["model", "install", "gemma4:e2b", "--node", "spark"])

    assert result.exit_code == 0
    assert calls[0][1] == "192.168.4.173:55052"
    assert local_syncs[0]["runtime_endpoints"]["gemma4:e2b"]["api_base"] == "http://192.168.4.173:4000/v1"
    assert len(node_syncs) == 1
    target, payload = node_syncs[0]
    assert target == "192.168.4.174:55052"
    assert payload["runtime_endpoints"]["gemma4:e2b"]["api_base"] == "http://192.168.4.173:4000/v1"


def test_model_install_streams_pull_progress(mocker):
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n")
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds._sync_installed_model_gateway_route")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 0
    pull_kwargs = [
        kwargs
        for command, kwargs in calls
        if command == ["docker", "model", "pull", "docker.io/ai/gemma4:E2B"]
    ][0]
    assert pull_kwargs["capture_output"] is False


def test_model_install_retries_transient_pull_failure(mocker):
    calls = []
    pull_attempts = 0

    def fake_run(command, **kwargs):
        nonlocal pull_attempts
        calls.append(command)
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n")
        if command == ["docker", "model", "pull", "docker.io/ai/gemma4:E2B"]:
            pull_attempts += 1
            if pull_attempts == 1:
                return _completed(command, returncode=1, stderr="writing blob: blob digest mismatch")
        if command == ["docker", "model", "inspect", "docker.io/ai/gemma4:E2B"]:
            return _completed(command, returncode=1)
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds._sync_installed_model_gateway_route")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 0
    assert calls.count(["docker", "model", "pull", "docker.io/ai/gemma4:E2B"]) == 2
    assert ["docker", "model", "run", "--detach", "docker.io/ai/gemma4:E2B"] in calls


def test_model_install_persists_manual_ownership_record(mocker):
    def fake_run(command, **kwargs):
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n      --context-size int\n")
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds._sync_installed_model_gateway_route")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 0
    record = load_model_ownership()["models"]["docker.io/ai/gemma4:E2B"]
    assert record["model_id"] == "gemma4:e2b"
    assert record["docker_model"] == "docker.io/ai/gemma4:E2B"
    assert record["backend"] == "llama.cpp"
    assert record["manual"] is True
    assert record["owners"] == {}


def test_model_install_state_can_be_listed_after_install(mocker):
    def fake_run(command, **kwargs):
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n      --context-size int\n")
        if command[:4] == ["docker", "model", "list", "--format"]:
            return _completed(command, stdout=json.dumps([{"name": "docker.io/ai/gemma4:E2B"}]))
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds._sync_installed_model_gateway_route")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    install_result = runner.invoke(app, ["model", "install", "gemma4:e2b"])
    list_result = runner.invoke(app, ["model", "list", "--installed", "--json"])

    assert install_result.exit_code == 0
    assert list_result.exit_code == 0
    model = json.loads(list_result.stdout)["models"][0]
    assert model["id"] == "gemma4:e2b"
    assert model["docker_model"] == "docker.io/ai/gemma4:E2B"
    assert model["installed"] is True
    assert model["manual"] is True
    assert model["owner_count"] == 0
    assert model["orphaned"] is False


def test_model_install_skips_context_size_when_docker_cli_does_not_support_it(mocker):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n  -d, --detach\n")
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds._sync_installed_model_gateway_route")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b", "--context-size", "8192"])

    assert result.exit_code == 0
    assert ["docker", "model", "run", "--detach", "docker.io/ai/gemma4:E2B"] in calls
    assert ["docker", "model", "run", "--detach", "--context-size", "8192", "docker.io/ai/gemma4:E2B"] not in calls


def test_model_install_falls_back_to_dmr_rest_when_cli_plugin_missing(mocker):
    calls = []
    requests = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[:3] == ["docker", "model", "--help"]:
            return _completed(command, returncode=1, stderr="unknown command")
        return _completed(command)

    def fake_urlopen(request, timeout=0):
        requests.append((request.full_url, request.get_method(), request.data))
        if "model-runner.docker.internal" in request.full_url:
            raise urllib.error.URLError("not in container")
        return FakeResponse("{}")

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("urllib.request.urlopen", side_effect=fake_urlopen)
    mocker.patch("mn_cli.libs.model_cmds._sync_installed_model_gateway_route")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 0
    assert ["docker", "model", "pull", "docker.io/ai/gemma4:E2B"] not in calls
    assert any(url.endswith("/models/create") and method == "POST" for url, method, _data in requests)
    payloads = [json.loads(data.decode("utf-8")) for _url, _method, data in requests if data]
    assert {"from": "docker.io/ai/gemma4:E2B"} in payloads


def test_model_install_prefers_dmr_rest_pull_when_runner_api_reachable(mocker, monkeypatch):
    calls = []
    requests = []
    monkeypatch.setattr("mn_cli.libs.model_cmds._endpoint_responds", lambda: True)
    monkeypatch.setattr("mn_sdk.model_service.endpoint_responds", lambda: True)

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:4] == ["docker", "model", "run", "--help"]:
            return _completed(command, stdout="Options:\n")
        return _completed(command)

    def fake_urlopen(request, timeout=0):
        requests.append((request.full_url, request.get_method(), request.data, timeout))
        return FakeResponse("{}")

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("urllib.request.urlopen", side_effect=fake_urlopen)
    mocker.patch("mn_cli.libs.model_cmds._sync_installed_model_gateway_route")
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 0
    assert ["docker", "model", "pull", "docker.io/ai/gemma4:E2B"] not in calls
    assert ["docker", "model", "run", "--detach", "docker.io/ai/gemma4:E2B"] in calls
    assert any(url.endswith("/models/create") and method == "POST" for url, method, _data, _timeout in requests)
    payloads = [json.loads(data.decode("utf-8")) for _url, _method, data, _timeout in requests if data]
    assert {"from": "docker.io/ai/gemma4:E2B"} in payloads


def test_model_list_reads_dmr_rest_tags_when_cli_plugin_missing(mocker):
    def fake_run(command, **kwargs):
        if command[:3] == ["docker", "model", "--help"] or command[:3] == ["docker", "model", "list"]:
            return _completed(command, returncode=1, stderr="unknown command")
        return _completed(command)

    def fake_urlopen(request, timeout=0):
        return FakeResponse(json.dumps([
            {"id": "sha256:one", "tags": ["docker.io/ai/gemma4:E2B"]}
        ]))

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("urllib.request.urlopen", side_effect=fake_urlopen)

    result = runner.invoke(app, ["model", "list", "--installed", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["models"][0]["id"] == "gemma4:e2b"
    assert payload["models"][0]["installed"] is True


def test_model_doctor_reports_corrupted_gateway_config(tmp_path, monkeypatch, mocker):
    class FakeCompatibility:
        ok = True

        def to_dict(self):
            return {"ok": True, "status": "pass", "message": "ok"}

    config_path = tmp_path / "litellm-config.yaml"
    config_path.write_text("{broken", encoding="utf-8")
    monkeypatch.setenv("MN_LITELLM_GATEWAY_CONFIG_PATH", str(config_path))
    mocker.patch("mn_cli.libs.model_cmds.assess_model_compatibility", return_value=FakeCompatibility())
    mocker.patch("mn_cli.libs.model_cmds._docker_status", return_value={"running": True})
    mocker.patch("mn_cli.libs.model_cmds._model_installed", return_value=True)
    mocker.patch("mn_cli.libs.model_cmds._endpoint_responds", return_value=True)
    mocker.patch("mn_cli.libs.model_cmds.litellm_gateway_health", return_value={"ok": True, "url": "http://127.0.0.1:4000/v1/models", "models": ["gemma4:e2b"]})
    mocker.patch(
        "mn_cli.libs.model_cmds.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "doctor", "gemma4:e2b", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["litellm_gateway"]["config_ok"] is False
    assert payload["litellm_gateway"]["config_path"] == str(config_path)
    assert "Expecting property name" in payload["litellm_gateway"]["config_error"]


def test_model_install_blocks_incompatible_hardware_before_pull(mocker):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=8, unified_memory_gb=8, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 1
    assert ["docker", "model", "pull", "docker.io/ai/gemma4:E2B"] not in calls


def test_model_install_failure_does_not_record_manual_ownership(mocker, tmp_path, monkeypatch):
    ledger_path = tmp_path / "ownership.json"
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(ledger_path))
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[:3] == ["docker", "model", "--help"]:
            return _completed(command)
        if command[:4] == ["docker", "model", "status", "--json"]:
            return _completed(command, stdout=json.dumps({"running": True, "backends": {"llama.cpp": "Running"}}))
        if command[:3] == ["docker", "model", "pull"]:
            return _completed(command, returncode=1, stderr="pull failed")
        if command[:3] == ["docker", "model", "inspect"]:
            return _completed(command, returncode=1)
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_sdk.model_service.model_installed", return_value=False)
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 1
    assert ["docker", "model", "pull", "docker.io/ai/gemma4:E2B"] in calls
    assert load_model_ownership()["models"] == {}


def test_model_install_rest_failure_does_not_record_manual_ownership(
    mocker, tmp_path, monkeypatch
):
    ledger_path = tmp_path / "ownership.json"
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(ledger_path))
    requests = []

    def fake_run(command, **kwargs):
        if command[:3] == ["docker", "model", "--help"]:
            return _completed(command, returncode=1, stderr="unknown command")
        return _completed(command)

    def fake_urlopen(request, timeout=0):
        requests.append((request.full_url, request.get_method(), request.data))
        raise urllib.error.URLError("runner down")

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("urllib.request.urlopen", side_effect=fake_urlopen)
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 1
    assert any(url.endswith("/models/create") and method == "POST" for url, method, _data in requests)
    assert load_model_ownership()["models"] == {}


def test_model_remove_uses_resolved_docker_model_with_force(mocker):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)

    result = runner.invoke(app, ["model", "remove", "gemma4:e2b", "--force"])

    assert result.exit_code == 0
    assert "Model remove successful." in result.stdout
    assert ["docker", "model", "rm", "--force", "docker.io/ai/gemma4:E2B"] in calls


def test_model_remove_local_dmr_removes_gateway_route_from_cluster(mocker):
    calls = []
    local_removed = []
    node_removed = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return _completed(command)

    class FakeClient:
        def __init__(self, **kwargs):
            self.target = kwargs.get("target")

        def remove_litellm_gateway_route(self, payload):
            node_removed.append((self.target, payload))
            return json.dumps({"status": "removed"})

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.model_cmds.Client", FakeClient)
    mocker.patch(
        "mn_cli.libs.model_cmds.client.get_system_summary",
        return_value=_cluster_summary(
            _cluster_node("local", "10.0.0.1", self_node=True),
            _cluster_node("spark", "10.0.0.2"),
        ),
    )
    mocker.patch("mn_cli.libs.model_cmds.remove_litellm_gateway_route", side_effect=lambda model: local_removed.append(model))

    result = runner.invoke(app, ["model", "remove", "gemma4:e2b", "--force"])

    assert result.exit_code == 0
    assert ["docker", "model", "rm", "--force", "docker.io/ai/gemma4:E2B"] in calls
    assert local_removed == ["docker.io/ai/gemma4:E2B", "gemma4:e2b"]
    assert {(target, payload["model"]) for target, payload in node_removed} == {
        ("10.0.0.2:55052", "docker.io/ai/gemma4:E2B"),
        ("10.0.0.2:55052", "gemma4:e2b"),
    }


def test_model_remove_node_removes_route_on_target_and_other_nodes(mocker):
    local_removed = []
    node_removed = []
    local_syncs = []
    upsert_model_remote(
        "spark-gemma4-e2b",
        "docker.io/ai/gemma4:E2B",
        "http://192.168.4.173:4000/v1",
        api_model="gemma4:E2B",
        node="spark",
    )

    class FakeClient:
        def __init__(self, **kwargs):
            self.target = kwargs.get("target")

        def remove_litellm_gateway_route(self, payload):
            node_removed.append((self.target, payload))
            return json.dumps({"status": "removed"})

    mocker.patch("mn_cli.libs.model_cmds.Client", FakeClient)
    mocker.patch(
        "mn_cli.libs.model_cmds.client.get_system_summary",
        return_value=_cluster_summary(
            _cluster_node("spark", "192.168.4.173"),
            _cluster_node("moon", "192.168.4.174"),
        ),
    )
    mocker.patch("mn_cli.libs.model_cmds.remove_litellm_gateway_route", side_effect=lambda model: local_removed.append(model))
    mocker.patch("mn_cli.libs.model_cmds.sync_litellm_gateway", side_effect=lambda **kwargs: local_syncs.append(kwargs) or {})
    mocker.patch("subprocess.run", side_effect=AssertionError("remote route removal must not shell out"))

    result = runner.invoke(app, ["model", "remove", "gemma4:e2b", "--node", "spark"])

    assert result.exit_code == 0
    assert set(local_removed) == {"gemma4:e2b", "spark-gemma4-e2b", "docker.io/ai/gemma4:E2B", "gemma4:E2B"}
    assert load_model_remotes()["remotes"] == {}
    assert local_syncs == [{"runtime_endpoints": {}, "restart": True}]
    assert [(target, payload["model"], payload["source"]) for target, payload in node_removed] == [
        ("192.168.4.173:55052", "gemma4:e2b", "mn-cli-remove-route"),
        ("192.168.4.174:55052", "gemma4:e2b", "mn-cli-remove-route-fanout"),
    ]


def test_model_remote_remove_removes_gateway_route_from_all_nodes(mocker):
    local_removed = []
    node_removed = []
    upsert_model_remote(
        "spark-qwen",
        "ai/qwen3-coder",
        "http://192.168.4.173:4000/v1",
        api_model="ai/qwen3-coder",
        node="spark",
    )

    class FakeClient:
        def __init__(self, **kwargs):
            self.target = kwargs.get("target")

        def remove_litellm_gateway_route(self, payload):
            node_removed.append((self.target, payload))
            return json.dumps({"status": "removed"})

    mocker.patch("mn_cli.libs.model_cmds.Client", FakeClient)
    mocker.patch(
        "mn_cli.libs.model_cmds.client.get_system_summary",
        return_value=_cluster_summary(
            _cluster_node("local", "10.0.0.1", self_node=True),
            _cluster_node("spark", "10.0.0.2"),
        ),
    )
    mocker.patch("mn_cli.libs.model_cmds.remove_litellm_gateway_route", side_effect=lambda model: local_removed.append(model))
    mocker.patch("mn_cli.libs.model_cmds.sync_litellm_gateway", return_value={})

    result = runner.invoke(app, ["model", "remote", "remove", "spark-qwen", "--json"])

    assert result.exit_code == 0
    assert local_removed == ["spark-qwen", "ai/qwen3-coder"]
    assert {(target, payload["model"]) for target, payload in node_removed} == {
        ("10.0.0.2:55052", "spark-qwen"),
        ("10.0.0.2:55052", "ai/qwen3-coder"),
    }
