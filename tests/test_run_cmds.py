import pytest
import json
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
    assert "Job Status: Success" in result.stdout
    mock_submit.assert_called_once()
    mock_stream.assert_called_once_with("job-123")

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
