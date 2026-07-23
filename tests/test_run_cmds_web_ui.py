import json

from mn_cli.libs.run_cmds.web_ui import (
    _console_web_ui_url_from_manifest,
    _console_web_ui_url_from_run_dir,
)
from mn_sdk.submission_preparation import prepare_manifest_for_submission


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


def test_console_web_ui_url_reads_blueprint_written_handle(tmp_path):
    (tmp_path / "web_ui.json").write_text(
        json.dumps({"url": "http://localhost:62000/"}),
        encoding="utf-8",
    )

    assert _console_web_ui_url_from_run_dir(tmp_path) == "http://localhost:62000/"


def test_submission_does_not_inject_dashboard_service(tmp_path):
    manifest = {
        "graph_id": "plain-service",
        "type": "service",
        "nodes": [{"node_id": "worker", "config": {"environment": {}}}],
        "entrypoints": ["worker"],
    }

    prepared = prepare_manifest_for_submission(tmp_path, manifest)

    assert [node["node_id"] for node in prepared["nodes"]] == ["worker"]
