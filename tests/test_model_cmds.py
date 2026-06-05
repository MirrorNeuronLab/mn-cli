import json
import subprocess
import urllib.error

import pytest
from typer.testing import CliRunner

from mn_cli.main import app
from mn_sdk import HostHardwareProfile, load_model_ownership


runner = CliRunner()


@pytest.fixture(autouse=True)
def isolate_model_ownership(monkeypatch, tmp_path):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))


def _completed(command, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(command, returncode, stdout, stderr)


class FakeResponse:
    def __init__(self, body="{}"):
        self.body = body.encode("utf-8")
        self.status = 200

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self.body


def test_model_list_prints_builtin_catalog_json():
    result = runner.invoke(app, ["model", "list", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["models"][0]["id"] == "gemma4:e2b"
    assert payload["models"][0]["model"] == "ai/gemma4:E2B"


def test_model_show_resolves_gemme_alias():
    result = runner.invoke(app, ["model", "show", "gemme4:e2b", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["id"] == "gemma4:e2b"
    assert payload["model"] == "ai/gemma4:E2B"


def test_model_show_does_not_require_docker_binary(mocker):
    mocker.patch("subprocess.run", side_effect=FileNotFoundError("docker"))

    result = runner.invoke(app, ["model", "show", "gemma4:e2b", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["installed"] is False


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
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b", "--context-size", "8192"])

    assert result.exit_code == 0
    assert ["docker", "model", "pull", "ai/gemma4:E2B"] in calls
    assert ["docker", "model", "run", "--detach", "--context-size", "8192", "ai/gemma4:E2B"] in calls


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
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b", "--context-size", "8192"])

    assert result.exit_code == 0
    assert ["docker", "model", "run", "--detach", "ai/gemma4:E2B"] in calls
    assert ["docker", "model", "run", "--detach", "--context-size", "8192", "ai/gemma4:E2B"] not in calls


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
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 0
    assert ["docker", "model", "pull", "ai/gemma4:E2B"] not in calls
    assert any(url.endswith("/models/create") and method == "POST" for url, method, _data in requests)
    payloads = [json.loads(data.decode("utf-8")) for _url, _method, data in requests if data]
    assert {"from": "ai/gemma4:E2B"} in payloads


def test_model_list_reads_dmr_rest_tags_when_cli_plugin_missing(mocker):
    def fake_run(command, **kwargs):
        if command[:3] == ["docker", "model", "--help"] or command[:3] == ["docker", "model", "list"]:
            return _completed(command, returncode=1, stderr="unknown command")
        return _completed(command)

    def fake_urlopen(request, timeout=0):
        return FakeResponse(json.dumps([
            {"id": "sha256:one", "tags": ["ai/gemma4:E2B"]}
        ]))

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch("urllib.request.urlopen", side_effect=fake_urlopen)

    result = runner.invoke(app, ["model", "list", "--installed", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["models"][0]["id"] == "gemma4:e2b"
    assert payload["models"][0]["installed"] is True


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
    assert ["docker", "model", "pull", "ai/gemma4:E2B"] not in calls


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
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)
    mocker.patch(
        "mn_sdk.model_runtime.detect_host_hardware",
        return_value=HostHardwareProfile("darwin", "arm64", total_memory_gb=16, unified_memory_gb=16, has_apple_silicon=True),
    )

    result = runner.invoke(app, ["model", "install", "gemma4:e2b"])

    assert result.exit_code == 1
    assert ["docker", "model", "pull", "ai/gemma4:E2B"] in calls
    assert load_model_ownership()["models"] == {}


def test_model_remove_uses_resolved_docker_model_with_force(mocker):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return _completed(command)

    mocker.patch("subprocess.run", side_effect=fake_run)

    result = runner.invoke(app, ["model", "remove", "gemma4:e2b", "--force"])

    assert result.exit_code == 0
    assert ["docker", "model", "rm", "--force", "ai/gemma4:E2B"] in calls
