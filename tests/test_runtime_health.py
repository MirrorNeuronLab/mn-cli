from __future__ import annotations

import json
from pathlib import Path

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
