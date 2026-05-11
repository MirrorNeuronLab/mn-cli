import json

from mn_cli.libs.run_logs import JobLogWriter, materialize_sent_email_copy
from mn_cli.libs.run_manifest import prepare_manifest_for_submission


def test_prepare_manifest_for_submission_merges_runtime_env_and_metadata(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({"identity": {"blueprint_id": "bp"}}))

    manifest = {
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "environment": {
                        "LITELLM_MODEL": "ollama/test",
                        "MN_LLM_API_KEY": "kept",
                    }
                },
            }
        ]
    }

    prepared = prepare_manifest_for_submission(
        bundle_dir,
        manifest,
        env_overrides={"MN_RUN_ID": "run-1"},
        submission_metadata={"blueprint_id": "bp"},
    )

    env = prepared["nodes"][0]["config"]["environment"]
    assert json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])["identity"]["blueprint_id"] == "bp"
    assert env["MN_RUN_ID"] == "run-1"
    assert env["MN_LLM_MODEL"] == "ollama/test"
    assert env["MN_LLM_API_KEY"] == "kept"
    assert prepared["metadata"]["mn_cli"]["blueprint_id"] == "bp"


def test_job_log_writer_deduplicates_events_and_records_web_ui_once():
    writer = JobLogWriter("unit-run-helper")
    event = {
        "timestamp": "2026-05-01T00:00:00Z",
        "type": "custom",
        "payload": {"message_id": "m1", "web_ui": {"url": "http://localhost:1"}},
    }

    assert writer.write_event(event) is True
    assert writer.write_event(event) is False
    assert writer.record_web_ui_url(event) == "http://localhost:1"
    assert writer.record_web_ui_url(event) is None


def test_materialize_sent_email_copy_uses_safe_host_paths(tmp_path):
    materialize_sent_email_copy(
        tmp_path,
        {
            "provider_id": "id/with spaces",
            "sent_email_copy": {
                "html_path": "../unsafe.html",
                "text_content": "plain",
                "html_content": "<p>Hello</p>",
                "metadata": {"provider": "test"},
            },
        },
    )

    email_dir = tmp_path / "sent_emails"
    assert (email_dir / "unsafe.html").read_text() == "<p>Hello</p>"
    metadata = json.loads((email_dir / "id-with-spaces.json").read_text())
    assert metadata["provider"] == "test"
    assert PathLikeName(metadata["host_html_path"]) == "unsafe.html"


def PathLikeName(path: str) -> str:
    return path.rsplit("/", 1)[-1]
