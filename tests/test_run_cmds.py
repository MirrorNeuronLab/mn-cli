import pytest
import json
import logging
import uuid
from logging.handlers import RotatingFileHandler
from typer.testing import CliRunner
from mn_cli.main import app
from mn_cli.libs import run_cmds

runner = CliRunner()

def test_validate_success(tmp_path):
    bundle_dir = tmp_path / "valid_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_data = {
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["e1"],
        "nodes": [{"node_id": "n1"}]
    }
    manifest_file.write_text(json.dumps(manifest_data))
    
    result = runner.invoke(app, ["validate", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Job bundle at" in result.stdout
    assert "is valid" in result.stdout

def test_validate_not_directory(tmp_path):
    not_a_dir = tmp_path / "not_a_dir"
    result = runner.invoke(app, ["validate", str(not_a_dir)])
    assert result.exit_code == 1
    assert "is not a directory" in result.stdout

def test_validate_no_manifest(tmp_path):
    bundle_dir = tmp_path / "no_manifest"
    bundle_dir.mkdir()
    result = runner.invoke(app, ["validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "manifest.json not found in" in result.stdout

def test_validate_bad_json(tmp_path):
    bundle_dir = tmp_path / "bad_json"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text("{bad_json: 1}")
    result = runner.invoke(app, ["validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "is not valid JSON" in result.stdout

def test_validate_missing_keys(tmp_path):
    bundle_dir = tmp_path / "missing_keys"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"manifest_version": "1.0"}')
    result = runner.invoke(app, ["validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "missing required keys" in result.stdout

def test_validate_nodes_not_list(tmp_path):
    bundle_dir = tmp_path / "nodes_not_list"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_data = {
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["e1"],
        "nodes": "not_a_list"
    }
    manifest_file.write_text(json.dumps(manifest_data))
    result = runner.invoke(app, ["validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "'nodes' must be a list" in result.stdout

def test_run_success(mocker, tmp_path):
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mock_stream = mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_pending"}),
        json.dumps({"type": "job_completed"})
    ])
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    payloads_dir = bundle_dir / "payloads"
    payloads_dir.mkdir()
    (payloads_dir / "test.txt").write_text("hello")
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    
    assert result.exit_code == 0
    assert "Job submitted successfully" in result.stdout
    assert "Type" in result.stdout
    assert "Batch" in result.stdout
    assert "Job Status: Success" in result.stdout
    mock_submit.assert_called_once()
    mock_stream.assert_called_once_with("job-123")


def test_run_injects_blueprint_config_scenario_and_run_id(mocker, tmp_path):
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "environment": {
                        "LITELLM_MODEL": "ollama/nemotron3:33b",
                        "LITELLM_API_BASE": "http://old",
                    }
                },
            }
        ]
    }))
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({"identity": {"blueprint_id": "bp-1"}}))
    (bundle_dir / "scenario.json").write_text(json.dumps({"blueprint_id": "bp-1", "metrics": [], "actions": []}))
    (bundle_dir / "product.json").write_text(json.dumps({"title": "bp-1"}))

    result = runner.invoke(app, ["run", str(bundle_dir)])

    assert result.exit_code == 0
    manifest = json.loads(mock_submit.call_args.args[0])
    env = manifest["nodes"][0]["config"]["environment"]
    assert json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])["identity"]["blueprint_id"] == "bp-1"
    assert json.loads(env["MN_BLUEPRINT_SCENARIO_JSON"])["blueprint_id"] == "bp-1"
    assert json.loads(env["MN_BLUEPRINT_PRODUCT_JSON"])["title"] == "bp-1"
    assert env["MN_LLM_MODEL"] == "ollama/nemotron3:33b"
    assert env["MN_LLM_API_BASE"] == "http://old"


def test_run_records_blueprint_run_id_mapping(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-abc")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({"nodes": []}))

    run_cmds.run_bundle(
        str(bundle_dir),
        env_overrides={"MN_RUN_ID": "bp-run"},
        submission_metadata={"blueprint_run_id": "bp-run", "blueprint_revision": "rev-1"},
    )

    mapping = json.loads((tmp_path / "runs" / "bp-run" / "job.json").read_text())
    assert mapping["job_id"] == "job-abc"
    assert mapping["blueprint_revision"] == "rev-1"

def test_run_displays_live_job_type_and_follow_status(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-live")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_pending"}),
        json.dumps({"type": "job_scheduled"}),
    ])
    mocker.patch(
        'mn_cli.libs.run_cmds.client.get_job',
        return_value=json.dumps({
            "summary": {"status": "running"},
            "job": {"status": "running"},
            "recent_events": [],
        }),
    )

    bundle_dir = tmp_path / "live_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "daemon": True,
        "policies": {"stream_mode": "live"},
        "nodes": [],
    }))

    result = runner.invoke(app, ["run", str(bundle_dir), "--follow-seconds", "0"])

    assert result.exit_code == 0
    assert "Live daemon" in result.stdout
    assert "Starting: agents scheduled" in result.stdout
    assert "Following: status running" in result.stdout
    assert "75%" not in result.stdout

def test_run_uses_detach_log_seconds_env(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUN_DETACH_LOG_SECONDS", "4.5")
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-env-follow")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_pending"}),
        json.dumps({"type": "job_scheduled"}),
    ])
    mock_follow = mocker.patch(
        'mn_cli.libs.run_cmds._follow_job_events',
        return_value=("running", {}),
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text('{"nodes": []}')

    result = runner.invoke(app, ["run", str(bundle_dir)])

    assert result.exit_code == 0
    assert "4.5s event tail" in result.stdout
    assert mock_follow.call_args.args[2] == 4.5

def test_run_follow_seconds_option_overrides_env(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUN_DETACH_LOG_SECONDS", "9")
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-option-follow")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_scheduled"}),
    ])
    mock_follow = mocker.patch(
        'mn_cli.libs.run_cmds._follow_job_events',
        return_value=("running", {}),
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text('{"nodes": []}')

    result = runner.invoke(app, ["run", str(bundle_dir), "--follow-seconds", "1.25"])

    assert result.exit_code == 0
    assert "1.25s event tail" in result.stdout
    assert mock_follow.call_args.args[2] == 1.25

def test_job_log_writer_uses_run_logging_env(monkeypatch):
    job_id = f"env-vars-{uuid.uuid4().hex}"
    monkeypatch.setenv("MN_RUN_EVENT_LOG_MAX_BYTES", "123")
    monkeypatch.setenv("MN_RUN_EVENT_LOG_BACKUP_COUNT", "2")
    monkeypatch.setenv("MN_RUN_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("MN_RUN_LOG_MAX_BYTES", "456")
    monkeypatch.setenv("MN_RUN_LOG_BACKUP_COUNT", "3")

    writer = run_cmds.JobLogWriter(job_id)
    handler = next(
        handler
        for handler in writer.run_logger.handlers
        if isinstance(handler, RotatingFileHandler)
    )

    assert writer.max_bytes == 123
    assert writer.backup_count == 2
    assert writer.run_logger.level == logging.DEBUG
    assert handler.maxBytes == 456
    assert handler.backupCount == 3

def test_job_log_writer_rotates_event_log_with_env(monkeypatch):
    job_id = f"rotate-{uuid.uuid4().hex}"
    monkeypatch.setenv("MN_RUN_EVENT_LOG_MAX_BYTES", "1")
    monkeypatch.setenv("MN_RUN_EVENT_LOG_BACKUP_COUNT", "2")

    writer = run_cmds.JobLogWriter(job_id)
    for index in range(4):
        writer.write_event(
            {
                "type": "custom",
                "timestamp": f"2026-04-29T00:00:0{index}Z",
                "payload": {"value": "x" * 20},
            }
        )

    assert writer.events_file.exists()
    assert (writer.log_dir / "events.log.1").exists()
    assert (writer.log_dir / "events.log.2").exists()
    assert not (writer.log_dir / "events.log.3").exists()

def test_job_log_writer_extracts_web_ui_url_once():
    writer = run_cmds.JobLogWriter(f"web-ui-{uuid.uuid4().hex}")
    event = {
        "type": "web_ui_available",
        "payload": {"url": "http://127.0.0.1:7860", "adapter": "gradio"},
    }

    assert writer.record_web_ui_url(event) == "http://127.0.0.1:7860"
    assert writer.record_web_ui_url(event) is None

def test_run_error_submitting(mocker, tmp_path):
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', side_effect=Exception("API failure"))
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    
    assert result.exit_code == 1
    assert "Error running bundle: API failure" in result.stdout

def test_run_keyboard_interrupt(mocker, tmp_path):
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mock_stream = mocker.patch('mn_cli.libs.run_cmds.client.stream_events', side_effect=KeyboardInterrupt)
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    
    assert result.exit_code == 0
    assert "Detached from log stream" in result.stdout

def test_run_not_dir(tmp_path):
    not_a_dir = tmp_path / "not_a_dir"
    result = runner.invoke(app, ["run", str(not_a_dir)])
    assert result.exit_code == 1
    assert "is not a directory" in result.stdout

def test_run_no_manifest(tmp_path):
    bundle_dir = tmp_path / "no_manifest"
    bundle_dir.mkdir()
    result = runner.invoke(app, ["run", str(bundle_dir)])
    assert result.exit_code == 1
    assert "manifest.json not found" in result.stdout

def test_monitor_success(mocker):
    mocker.patch('mn_cli.libs.run_cmds.client.get_job', return_value=json.dumps({"summary": {"status": "completed", "live?": False}, "job": {"job_name": "test"}, "agents": [{"agent_id": "a1", "status": "running", "processed_messages": 10}]}))
    mocker.patch('sys.stdin.isatty', return_value=False)
    
    result = runner.invoke(app, ["monitor", "job-123"])
    
    assert result.exit_code == 0
    assert "Live Job Monitor" in result.stdout
    assert "Job Execution Summary" in result.stdout

def test_monitor_error(mocker):
    mocker.patch('sys.stdin.isatty', return_value=False)
    mocker.patch('mn_cli.libs.run_cmds.client.get_job', side_effect=Exception("Network fail"))
    result = runner.invoke(app, ["monitor", "job-123"])
    assert result.exit_code == 0
    assert "Error fetching job: Network fail" in result.stdout
def test_result_success(mocker, tmp_path):
    mock_get = mocker.patch('mn_cli.libs.run_cmds.client.get_job', return_value=json.dumps({
        "job": {"status": "completed", "result": {"test": "result"}},
        "recent_events": []
    }))
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "custom_event", "payload": "progressive"})
    ])
    
    result = runner.invoke(app, ["result", "job-123"])
    
    assert result.exit_code == 0
    assert "Final result saved to" in result.stdout
    assert "Stream results saved to" in result.stdout

def test_result_not_completed(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.get_job', return_value=json.dumps({
        "job": {"status": "running"},
        "recent_events": []
    }))
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[])
    
    result = runner.invoke(app, ["result", "job-999"])
    
    assert result.exit_code == 0
    assert "No final result found" in result.stdout

def test_result_error(mocker):
    mocker.patch('mn_cli.libs.run_cmds.fetch_and_save_results', side_effect=Exception("DB Error"))
    
    result = runner.invoke(app, ["result", "job-888"])
    
    assert result.exit_code == 0
    assert "Error fetching results: DB Error" in result.stdout

def test_stream_bad_json(mocker, tmp_path):
    mock_stream = mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        "invalid json format",
        json.dumps({"type": "job_failed"})
    ])
    
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Job Status: Failed" in result.stdout

def test_validate_unexpected_error(mocker, tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.touch()
    
    # Mock open to raise Exception
    mocker.patch('builtins.open', side_effect=Exception("Read error"))
    
    result = runner.invoke(app, ["validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "Validation failed: Read error" in result.stdout

def test_stream_all_events(mocker, tmp_path):
    events = [
        json.dumps({"type": "job_validated"}),
        json.dumps({"type": "job_scheduled"}),
        json.dumps({"type": "job_running"}),
        json.dumps({"type": "agent_message_received"}),
        json.dumps({"type": "custom_progressive", "payload": {"foo": "progressive"}}),
        json.dumps({"type": "job_completed", "result": {"foo": "bar"}})
    ]
    mock_stream = mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=events)
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Job Status: Success" in result.stdout
    assert "result.txt" in result.stdout
    assert "result_stream.txt" in result.stdout

def test_stream_keyboard_interrupt(mocker, tmp_path):
    mock_stream = mocker.patch('mn_cli.libs.run_cmds.client.stream_events', side_effect=KeyboardInterrupt)
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Detached from log stream" in result.stdout
