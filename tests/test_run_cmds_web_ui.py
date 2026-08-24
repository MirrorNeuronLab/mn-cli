import json
from types import SimpleNamespace

from mn_sdk.submission_preparation import prepare_manifest_for_submission
from v1_manifests import workflow_manifest

from mn_cli.libs.run_cmds.web_ui import (
    _console_web_ui_url,
    _console_web_ui_url_from_manifest,
    _console_web_ui_url_from_job_data,
    _register_manifest_web_ui_handle,
)


def test_console_web_ui_url_comes_from_blueprint_declared_service():
    manifest = {
        "agents": {
            "extra_nodes": [
                {
                    "node_id": "product_ui",
                    "services": [
                        {
                            "name": "product-ui",
                            "port": 61000,
                            "tags": ["web_ui", "json-render"],
                        }
                    ],
                }
            ]
        }
    }

    assert _console_web_ui_url_from_manifest(manifest) == "http://localhost:61000"


def test_console_web_ui_url_reads_job_written_handle(tmp_path, monkeypatch):
    job_dir = tmp_path / "job-data" / "job-1"
    job_dir.mkdir(parents=True)
    monkeypatch.setenv("MN_JOB_DATA_ROOT", str(tmp_path / "job-data"))
    (job_dir / "web_ui.json").write_text(
        json.dumps({"job_id": "job-1", "url": "http://localhost:62000/"}),
        encoding="utf-8",
    )

    assert _console_web_ui_url_from_job_data("job-1") == "http://localhost:62000/"


def test_console_web_ui_url_uses_local_job_route_for_registered_handle(tmp_path, monkeypatch, mocker):
    job_dir = tmp_path / "job-data" / "job-1"
    job_dir.mkdir(parents=True)
    monkeypatch.setenv("MN_JOB_DATA_ROOT", str(tmp_path / "job-data"))
    (job_dir / "web_ui.json").write_text(
        json.dumps({"job_id": "job-1", "url": "http://10.0.4.26:8088/"}),
        encoding="utf-8",
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.web_ui.RuntimeConfig.from_env",
        return_value=SimpleNamespace(
            web_ui_url="http://localhost:55173", web_ui_advertised=True
        ),
    )

    assert _console_web_ui_url({}, "job-1") == "http://localhost:55173/jobs/job-1/ui"


def test_register_manifest_web_ui_handle_uses_resolved_service_configuration(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_JOB_DATA_ROOT", str(tmp_path / "job-data"))
    manifest = {
        "metadata": {"web_ui": {"title": "Warehouse AMR Monitor", "node_id": "warehouse"}},
        "agents": {
            "nodes": [
                {
                    "services": [
                        {
                            "name": "warehouse-ui",
                            "tags": ["web_ui"],
                            "meta": {"url": "http://${config.web_ui.service.advertise_host}:${config.web_ui.service.port}"},
                        },
                        {"name": "video", "port": 8080, "tags": ["video"]},
                        {"name": "rosbridge", "port": 9090, "tags": ["websocket"]},
                        {"name": "mcp", "port": 8090, "tags": ["mcp"]},
                    ]
                }
            ]
        },
    }

    url = _register_manifest_web_ui_handle(
        manifest,
        "job-1",
        configuration={"web_ui": {"enabled": True, "service": {"advertise_host": "10.0.4.26", "port": 8088}}},
    )

    assert url == "http://10.0.4.26:8088"
    handle = json.loads((tmp_path / "job-data" / "job-1" / "web_ui.json").read_text(encoding="utf-8"))
    assert handle["job_id"] == "job-1"
    assert handle["url"] == "http://10.0.4.26:8088"
    assert handle["metadata"]["proxy"] == {
        "schema_version": "mn.web_ui.proxy.v1",
        "http_ports": [8080, 8088],
        "websocket_ports": [9090],
    }


def test_manifest_url_skips_unresolved_templates():
    manifest = {
        "agents": {
            "nodes": [
                {
                    "services": [
                        {
                            "tags": ["web_ui"],
                            "meta": {"url": "http://${config.web_ui.service.advertise_host}:${config.web_ui.service.port}"},
                        }
                    ]
                }
            ]
        }
    }

    assert _console_web_ui_url_from_manifest(manifest) is None


def test_submission_does_not_inject_dashboard_service(tmp_path):
    manifest = workflow_manifest({
        "graph_id": "plain-service",
        "type": "service",
        "nodes": [{"node_id": "worker", "config": {"environment": {}}}],
        "entrypoints": ["worker"],
    })

    prepared = prepare_manifest_for_submission(tmp_path, manifest)

    assert [node["node_id"] for node in prepared["agents"]["nodes"]] == ["worker"]
