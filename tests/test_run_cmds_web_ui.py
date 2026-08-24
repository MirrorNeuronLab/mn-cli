import json

from mn_sdk.submission_preparation import prepare_manifest_for_submission
from v1_manifests import workflow_manifest

from mn_cli.libs.run_cmds.web_ui import (
    _console_web_ui_url_from_manifest,
    _console_web_ui_url_from_job_data,
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


def test_submission_does_not_inject_dashboard_service(tmp_path):
    manifest = workflow_manifest({
        "graph_id": "plain-service",
        "type": "service",
        "nodes": [{"node_id": "worker", "config": {"environment": {}}}],
        "entrypoints": ["worker"],
    })

    prepared = prepare_manifest_for_submission(tmp_path, manifest)

    assert [node["node_id"] for node in prepared["agents"]["nodes"]] == ["worker"]
