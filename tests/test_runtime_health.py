from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

from rich.console import Console
from typer.testing import CliRunner

from mn_cli.main import app
from mn_cli.libs import runtime_health


runner = CliRunner()


def test_runtime_health_all_passing(mocker, tmp_path):
    _patch_targets(mocker, tmp_path, web_ui_installed=True)
    mocker.patch.object(runtime_health.client, "get_system_summary", return_value='{"nodes":[]}')
    mocker.patch(
        "mn_cli.libs.runtime_health.urllib.request.urlopen",
        side_effect=[
            _HttpResponse({"status": "ok"}),
            _HttpResponse({"status": "ok", "component": "web-ui"}),
        ],
    )

    report = runtime_health.collect_runtime_health(timeout=1)

    assert report["overall"] == "passing"
    assert [component["status"] for component in report["components"]] == ["passing", "passing", "passing"]


def test_runtime_health_api_down_is_critical(mocker, tmp_path):
    _patch_targets(mocker, tmp_path, web_ui_installed=True)
    mocker.patch.object(runtime_health.client, "get_system_summary", return_value='{"nodes":[]}')
    mocker.patch(
        "mn_cli.libs.runtime_health.urllib.request.urlopen",
        side_effect=[
            OSError("connection refused"),
            _HttpResponse({"status": "ok", "component": "web-ui"}),
        ],
    )

    report = runtime_health.collect_runtime_health(timeout=1)

    assert report["overall"] == "critical"
    assert report["components"][1]["name"] == "api"
    assert report["components"][1]["status"] == "critical"


def test_runtime_health_grpc_down_is_critical(mocker, tmp_path):
    _patch_targets(mocker, tmp_path, web_ui_installed=True)
    mocker.patch.object(runtime_health.client, "get_system_summary", side_effect=RuntimeError("grpc unavailable"))
    mocker.patch(
        "mn_cli.libs.runtime_health.urllib.request.urlopen",
        side_effect=[
            _HttpResponse({"status": "ok"}),
            _HttpResponse({"status": "ok", "component": "web-ui"}),
        ],
    )

    report = runtime_health.collect_runtime_health(timeout=1)

    assert report["overall"] == "critical"
    assert report["components"][0]["name"] == "core_grpc"
    assert report["components"][0]["status"] == "critical"


def test_runtime_health_uses_injected_core_client_without_shared_client(mocker, tmp_path):
    _patch_targets(mocker, tmp_path, web_ui_installed=True)
    shared_summary = mocker.patch.object(
        runtime_health.client,
        "get_system_summary",
        side_effect=AssertionError("shared client should not be called"),
    )
    core_client = _CoreClient()
    mocker.patch(
        "mn_cli.libs.runtime_health.urllib.request.urlopen",
        side_effect=[
            _HttpResponse({"status": "ok"}),
            _HttpResponse({"status": "ok", "component": "web-ui"}),
        ],
    )

    report = runtime_health.collect_runtime_health(timeout=1, core_client=core_client)

    assert report["overall"] == "passing"
    assert core_client.calls == 1
    shared_summary.assert_not_called()


def test_runtime_health_web_ui_down_when_advertised_is_critical(mocker, tmp_path):
    endpoints = _patch_targets(mocker, tmp_path, web_ui_installed=False)
    endpoints.write_text(
        json.dumps({"web_ui": {"url": "http://localhost:55173"}, "api": {"base_url": "http://localhost:54001/api/v1"}}),
        encoding="utf-8",
    )
    mocker.patch.object(runtime_health.client, "get_system_summary", return_value='{"nodes":[]}')
    mocker.patch(
        "mn_cli.libs.runtime_health.urllib.request.urlopen",
        side_effect=[
            _HttpResponse({"status": "ok"}),
            OSError("connection refused"),
        ],
    )

    report = runtime_health.collect_runtime_health(timeout=1)

    assert report["overall"] == "critical"
    assert report["components"][2]["name"] == "web_ui"
    assert report["components"][2]["status"] == "critical"


def test_runtime_health_web_ui_not_installed_is_warning(mocker, tmp_path):
    _patch_targets(mocker, tmp_path, web_ui_installed=False)
    mocker.patch.object(runtime_health.client, "get_system_summary", return_value='{"nodes":[]}')
    mocker.patch(
        "mn_cli.libs.runtime_health.urllib.request.urlopen",
        return_value=_HttpResponse({"status": "ok"}),
    )

    report = runtime_health.collect_runtime_health(timeout=1)

    assert report["overall"] == "warning"
    assert report["components"][2]["status"] == "warning"


def test_runtime_health_compose_native_port_env_normalizes_legacy_ports():
    env = runtime_health._compose_native_port_env(
        {"MN_GRPC_PORT": "50051", "MN_API_PORT": "4001", "MN_WEB_UI_PORT": "5173"}
    )

    assert env["MN_GRPC_PORT"] == "55051"
    assert env["MN_API_PORT"] == "54001"
    assert env["MN_WEB_UI_PORT"] == "55173"


def test_runtime_health_command_json_exits_nonzero_for_critical(mocker):
    mocker.patch(
        "mn_cli.libs.runtime_health.collect_runtime_health",
        return_value={
            "overall": "critical",
            "checked_at": "2026-06-03T00:00:00Z",
            "components": [
                {"name": "core_grpc", "status": "critical", "target": "localhost:55051", "duration_ms": 1, "error": "down"}
            ],
        },
    )

    result = runner.invoke(app, ["runtime", "health", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout)["overall"] == "critical"


def test_runtime_status_command_json_emits_sdk_payload(mocker):
    mocker.patch(
        "mn_cli.libs.runtime_health.collect_runtime_status",
        return_value=_status_report(overall="passing"),
    )

    result = runner.invoke(app, ["runtime", "status", "--json"])

    assert result.exit_code == 0
    body = json.loads(result.stdout)
    assert body["overall"] == "passing"
    assert body["endpoints"]["core_grpc"] == "localhost:55051"
    assert body["shared_storage"]["host_root"] == "/tmp/mn-shared"


def test_runtime_status_command_human_output_includes_overview(mocker):
    mocker.patch(
        "mn_cli.libs.runtime_health.collect_runtime_status",
        return_value=_status_report(overall="warning"),
    )

    result = runner.invoke(app, ["runtime", "status"])

    assert result.exit_code == 0
    assert "Runtime status: warning" in result.stdout
    assert "Core gRPC" in result.stdout
    assert "localhost:55051" in result.stdout
    assert "REST API" in result.stdout
    assert "http://localhost:54001/api/v1" in result.stdout
    assert "Web UI" in result.stdout
    assert "http://localhost:55173" in result.stdout
    assert "Nodes" in result.stdout
    assert "healthy: 1" in result.stdout
    assert "Active jobs" in result.stdout
    assert "running: 1" in result.stdout
    assert "Shared storage" in result.stdout
    assert "/tmp/mn-shared" in result.stdout


def test_runtime_status_human_output_wraps_long_details_for_narrow_console(mocker):
    report = _status_report(overall="critical")
    report["components"][0]["status"] = "critical"
    report["components"][0]["error"] = (
        "failed to connect to all addresses; last error: FAILED_PRECONDITION: "
        "ipv4:192.168.4.27:55051: connect failed: addr: "
        "ipv4:192.168.4.27:55051 error: Operation not permitted"
    )
    report["components"][1]["status"] = "critical"
    report["components"][1]["error"] = "<urlopen error [Errno 1] Operation not permitted>"
    stream = StringIO()
    mocker.patch(
        "mn_cli.libs.runtime_health.console",
        Console(file=stream, force_terminal=False, no_color=True, width=80),
    )

    runtime_health.print_status_report(report)

    output = stream.getvalue()
    assert "Runtime status: critical" in output
    assert "detail:" in output
    assert "failed to connect" in output
    assert "┏" not in output
    assert "│" not in output
    assert all(len(line) <= 80 for line in output.splitlines())


def test_runtime_status_command_exits_nonzero_for_critical(mocker):
    mocker.patch(
        "mn_cli.libs.runtime_health.collect_runtime_status",
        return_value=_status_report(overall="critical"),
    )

    result = runner.invoke(app, ["runtime", "status", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout)["overall"] == "critical"


def test_runtime_doctor_detects_stale_litellm_gateway_config(mocker):
    mocker.patch(
        "mn_cli.libs.runtime_health.collect_runtime_status",
        return_value=_status_report(overall="passing"),
    )
    mocker.patch("mn_cli.libs.runtime_health.docker_status", return_value={"running": True})
    mocker.patch("mn_cli.libs.runtime_health.dmr_api_list_models", return_value={"gemma4:e2b"})
    mocker.patch(
        "mn_cli.libs.runtime_health.validate_litellm_gateway_config_file",
        return_value={
            "ok": True,
            "path": "/tmp/.mn/models/litellm-gateway/config.yaml",
            "model_count": 2,
            "models": ["gemma4:e2b", "nemotron3"],
        },
    )
    mocker.patch(
        "mn_cli.libs.runtime_health.litellm_gateway_health",
        return_value={"ok": True, "url": "http://127.0.0.1:4000/v1/models", "models": []},
    )

    report = runtime_health.collect_runtime_doctor(timeout=1)

    gateway = report["foundation"]["litellm_gateway"]
    assert report["overall"] == "critical"
    assert gateway["status"] == "critical"
    assert gateway["missing_models"] == ["gemma4:e2b", "nemotron3"]
    assert "stale config" in gateway["detail"]


def test_runtime_doctor_detects_legacy_compose_model_override(mocker, tmp_path):
    compose_file = tmp_path / ".mn" / "docker-compose.yml"
    compose_file.parent.mkdir(parents=True)
    compose_file.write_text("services: {}\n", encoding="utf-8")
    (compose_file.parent / "docker-compose.models.yml").write_text(
        "services:\n"
        "  mirror-neuron-core:\n"
        "    models:\n"
        "      llm-runtime-model:\n"
        "        endpoint_var: MN_DOCKER_MODEL_RUNNER_API_BASE\n"
        "        model_var: MN_DOCKER_MODEL_RUNNER_MODEL\n"
        "\n"
        "models:\n"
        "  llm-runtime-model:\n"
        "    model: \"${MN_LLM_MODEL_RUNNER_MODEL:-nemotron3}\"\n",
        encoding="utf-8",
    )
    mocker.patch("mn_cli.libs.runtime_health.RUNTIME_COMPOSE_FILE", compose_file)
    mocker.patch(
        "mn_cli.libs.runtime_health.collect_runtime_status",
        return_value=_status_report(overall="passing"),
    )
    mocker.patch("mn_cli.libs.runtime_health.docker_status", return_value={"running": True})
    mocker.patch("mn_cli.libs.runtime_health.dmr_api_list_models", return_value=[])
    mocker.patch(
        "mn_cli.libs.runtime_health.validate_litellm_gateway_config_file",
        return_value={"ok": True, "path": str(tmp_path / "gateway.yaml"), "model_count": 0, "models": []},
    )
    mocker.patch(
        "mn_cli.libs.runtime_health.litellm_gateway_health",
        return_value={"ok": True, "url": "http://127.0.0.1:4000/v1/models", "models": []},
    )

    report = runtime_health.collect_runtime_doctor(timeout=1)

    component = report["foundation"]["runtime_compose_model_override"]
    assert report["overall"] == "critical"
    assert component["status"] == "critical"
    assert "eager Docker Model Runner model pulls" in component["detail"]
    assert component["target"].endswith("docker-compose.models.yml")


def test_runtime_doctor_command_json_exits_nonzero_for_stale_gateway(mocker):
    mocker.patch(
        "mn_cli.libs.runtime_health.collect_runtime_doctor",
        return_value={
            "overall": "critical",
            "checked_at": "2026-06-03T00:00:00Z",
            "runtime": {},
            "endpoints": {},
            "components": [
                {
                    "name": "litellm_gateway",
                    "status": "critical",
                    "target": "http://127.0.0.1:4000/v1",
                    "configured_models": ["nemotron3"],
                    "live_models": [],
                    "missing_models": ["nemotron3"],
                    "detail": "LiteLLM gateway is serving stale config",
                }
            ],
            "foundation": {},
            "nodes": {},
            "jobs": {},
            "shared_storage": {},
        },
    )

    result = runner.invoke(app, ["runtime", "doctor", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["overall"] == "critical"
    assert payload["components"][0]["missing_models"] == ["nemotron3"]


def test_runtime_health_repair_rechecks_after_restart(mocker):
    reports = [
        {
            "overall": "critical",
            "checked_at": "2026-06-03T00:00:00Z",
            "components": [
                {"name": "core_grpc", "status": "passing", "target": "localhost:55051", "duration_ms": 1},
                {"name": "api", "status": "critical", "target": "http://localhost:54001/api/v1/health", "duration_ms": 1},
                {"name": "web_ui", "status": "critical", "target": "http://localhost:55173/health", "duration_ms": 1},
            ],
        },
        {
            "overall": "passing",
            "checked_at": "2026-06-03T00:00:01Z",
            "components": [
                {"name": "core_grpc", "status": "passing", "target": "localhost:55051", "duration_ms": 1},
                {"name": "api", "status": "passing", "target": "http://localhost:54001/api/v1/health", "duration_ms": 1},
                {"name": "web_ui", "status": "passing", "target": "http://localhost:55173/health", "duration_ms": 1},
            ],
        },
    ]
    mock_collect = mocker.patch("mn_cli.libs.runtime_health.collect_runtime_health", side_effect=reports)
    mock_repair = mocker.patch("mn_cli.libs.runtime_health._repair_runtime_sidecars", return_value=True)

    result = runner.invoke(app, ["runtime", "health", "--json", "--repair"])

    assert result.exit_code == 0
    assert json.loads(result.stdout)["overall"] == "passing"
    assert mock_collect.call_count == 2
    mock_repair.assert_called_once_with(reports[0])


def _patch_targets(mocker, tmp_path: Path, *, web_ui_installed: bool) -> Path:
    endpoints = tmp_path / "runtime-endpoints.json"
    mocker.patch("mn_cli.libs.runtime_health.RUNTIME_ENDPOINTS_FILE", endpoints)
    mocker.patch("mn_cli.libs.runtime_health.runtime_compose_available", return_value=False)
    mocker.patch("mn_cli.libs.runtime_health._runtime_base_env", return_value={})
    mocker.patch(
        "mn_cli.libs.runtime_health._runtime_endpoint_snapshot",
        return_value={
            "api": {"base_url": "http://localhost:54001/api/v1"},
            "grpc": {"target": "localhost:55051"},
            **({"web_ui": {"url": "http://localhost:55173"}} if web_ui_installed else {}),
        },
    )
    mocker.patch("mn_cli.libs.runtime_health.find_web_ui_dir", return_value=(tmp_path / "web-ui") if web_ui_installed else None)
    return endpoints


class _CoreClient:
    def __init__(self):
        self.calls = 0

    def get_system_summary(self):
        self.calls += 1
        return '{"nodes":[]}'


class _HttpResponse:
    def __init__(self, payload: dict, status: int = 200):
        self.payload = payload
        self.status = status

    def getcode(self):
        return self.status

    def read(self, *_args):
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _status_report(*, overall: str) -> dict:
    return {
        "overall": overall,
        "checked_at": "2026-06-03T00:00:00Z",
        "runtime": {"mode": "local", "mn_home": "/tmp/.mn"},
        "endpoints": {
            "core_grpc": "localhost:55051",
            "api": "http://localhost:54001/api/v1",
            "web_ui": "http://localhost:55173",
        },
        "components": [
            {"name": "core_grpc", "status": "passing", "target": "localhost:55051", "duration_ms": 1},
            {"name": "api", "status": "passing", "target": "http://localhost:54001/api/v1/health", "duration_ms": 1},
            {"name": "web_ui", "status": "warning", "target": "http://localhost:55173", "duration_ms": 1, "detail": "web ui is not installed"},
        ],
        "nodes": {"available": True, "total": 1, "by_status": {"healthy": 1}, "items": []},
        "jobs": {"available": True, "total": 2, "by_status": {"running": 1, "completed": 1}, "active": 1, "active_by_status": {"running": 1}},
        "shared_storage": {"host_root": "/tmp/mn-shared", "runtime_root": "/runtime/shared", "configured": True},
    }
