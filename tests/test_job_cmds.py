from typer.testing import CliRunner
import json

from mn_cli.main import app

runner = CliRunner()

def test_submit_success(mocker, tmp_path):
    mock_submit = mocker.patch('mn_cli.libs.job_cmds.client.submit_job', return_value="job-123")
    
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text('{"graph_id": "test"}')
    
    result = runner.invoke(app, ["submit", str(manifest_file)])
    
    assert result.exit_code == 0
    assert "Job submitted successfully. Job ID: job-123" in result.stdout
    mock_submit.assert_called_once_with('{"graph_id": "test"}', {})

def test_submit_error(mocker, tmp_path):
    mocker.patch('mn_cli.libs.job_cmds.client.submit_job', side_effect=Exception("Failed API call"))
    
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text('{"graph_id": "test"}')
    
    result = runner.invoke(app, ["submit", str(manifest_file)])
    
    assert result.exit_code == 0
    assert "Error submitting job: Failed API call" in result.stdout

def test_status_success(mocker):
    mock_get = mocker.patch('mn_cli.libs.job_cmds.client.get_job', return_value='{"status": "running"}')
    
    result = runner.invoke(app, ["status", "job-123"])
    
    assert result.exit_code == 0
    assert "running" in result.stdout
    mock_get.assert_called_once_with("job-123")

def test_status_error(mocker):
    mocker.patch('mn_cli.libs.job_cmds.client.get_job', side_effect=Exception("Job not found"))
    result = runner.invoke(app, ["status", "job-123"])
    assert result.exit_code == 0
    assert "Error fetching job status: Job not found" in result.stdout

def test_list_jobs_success(mocker):
    mock_list = mocker.patch(
        'mn_cli.libs.job_cmds.client.list_jobs',
        return_value=json.dumps({"data": [{"job_id": "job-1", "graph_id": "g-1", "status": "completed", "submitted_at": "2023-10-01", "recovery_status": "normal"}]})
    )
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "job-1" in result.stdout
    assert "normal" in result.stdout
    mock_list.assert_called_once()

def test_list_jobs_running_only_success(mocker):
    mock_list = mocker.patch(
        'mn_cli.libs.job_cmds.client.list_jobs',
        return_value=json.dumps({"data": [
            {"job_id": "job-1", "graph_id": "g-1", "status": "completed", "submitted_at": "2023-10-01"},
            {"job_id": "job-2", "graph_id": "g-2", "status": "running", "submitted_at": "2023-10-01"}
        ]})
    )
    result = runner.invoke(app, ["list", "--running-only"])
    assert result.exit_code == 0
    assert "job-2" in result.stdout
    assert "job-1" not in result.stdout
    mock_list.assert_called_once()

def test_list_jobs_error(mocker):
    mocker.patch('mn_cli.libs.job_cmds.client.list_jobs', side_effect=Exception("Network error"))
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "Error listing jobs: Network error" in result.stdout

def test_clear_success(mocker):
    mock_clear = mocker.patch('mn_cli.libs.job_cmds.client.clear_jobs', return_value=5)
    result = runner.invoke(app, ["clear"])
    assert result.exit_code == 0
    assert "Successfully cleared 5 non-running jobs" in result.stdout
    mock_clear.assert_called_once()

def test_clear_error(mocker):
    mocker.patch('mn_cli.libs.job_cmds.client.clear_jobs', side_effect=Exception("DB Error"))
    result = runner.invoke(app, ["clear"])
    assert result.exit_code == 0
    assert "Error clearing jobs: DB Error" in result.stdout

def test_cancel_success(mocker):
    mock_cancel = mocker.patch('mn_cli.libs.job_cmds.client.cancel_job', return_value="cancelled")
    mock_cleanup = mocker.patch('mn_cli.libs.job_cmds._cleanup_cancelled_job_web_ui')
    result = runner.invoke(app, ["cancel", "job-123"])
    assert result.exit_code == 0
    assert "Job cancelled. Status: cancelled" in result.stdout
    mock_cancel.assert_called_once_with("job-123")
    mock_cleanup.assert_called_once_with("job-123")

def test_cancel_error(mocker):
    mocker.patch('mn_cli.libs.job_cmds.client.cancel_job', side_effect=Exception("Fail"))
    mock_cleanup = mocker.patch('mn_cli.libs.job_cmds._cleanup_cancelled_job_web_ui')
    result = runner.invoke(app, ["cancel", "job-123"])
    assert result.exit_code == 0
    assert "Error cancelling job: Fail" in result.stdout
    mock_cleanup.assert_called_once_with("job-123")

def test_cancel_cleans_blueprint_web_ui_process(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path))
    run_dir = tmp_path / "run-123"
    run_dir.mkdir()
    mocker.patch('mn_cli.libs.job_cmds._blueprint_run_id_for_job', return_value="run-123")
    mock_cleanup = mocker.patch('mn_cli.libs.job_cmds.cleanup_web_ui_process')

    mocker.patch('mn_cli.libs.job_cmds.client.cancel_job', return_value="cancelled")
    result = runner.invoke(app, ["cancel", "job-123"])

    assert result.exit_code == 0
    mock_cleanup.assert_called_once()
    _, kwargs = mock_cleanup.call_args
    assert kwargs["dry_run"] is False
    assert kwargs["reason"] == "job_cancelled"

def test_pause_success(mocker):
    mock_pause = mocker.patch('mn_cli.libs.job_cmds.client.pause_job', return_value="paused")
    result = runner.invoke(app, ["pause", "job-123"])
    assert result.exit_code == 0
    assert "Job paused. Status: paused" in result.stdout
    mock_pause.assert_called_once_with("job-123")

def test_pause_error(mocker):
    mocker.patch('mn_cli.libs.job_cmds.client.pause_job', side_effect=Exception("Fail"))
    result = runner.invoke(app, ["pause", "job-123"])
    assert result.exit_code == 0
    assert "Error pausing job: Fail" in result.stdout

def test_resume_success(mocker):
    mock_resume = mocker.patch('mn_cli.libs.job_cmds.client.resume_job', return_value="running")
    result = runner.invoke(app, ["resume", "job-123"])
    assert result.exit_code == 0
    assert "Job resumed. Status: running" in result.stdout
    mock_resume.assert_called_once_with("job-123")

def test_resume_error(mocker):
    mocker.patch('mn_cli.libs.job_cmds.client.resume_job', side_effect=Exception("Fail"))
    result = runner.invoke(app, ["resume", "job-123"])
    assert result.exit_code == 0
    assert "Error resuming job: Fail" in result.stdout


def test_unfinished_jobs_shows_recovery_review_state(mocker):
    mock_list = mocker.patch(
        'mn_cli.libs.job_cmds.client.list_jobs',
        return_value=json.dumps({"data": [
            {
                "job_id": "job-review",
                "graph_id": "g-review",
                "status": "paused",
                "updated_at": "2026-05-05T00:00:00Z",
                "recovery_status": "paused_for_review",
                "recovery_requires_review": True,
            }
        ]})
    )

    result = runner.invoke(app, ["unfinished"])

    assert result.exit_code == 0
    assert "job-review" in result.stdout
    assert "paused_for_review" in result.stdout
    assert "yes" in result.stdout
    assert "mn resume <job_id>" in result.stdout
    mock_list.assert_called_once_with(include_terminal=False)


def test_unfinished_jobs_empty(mocker):
    mocker.patch(
        'mn_cli.libs.job_cmds.client.list_jobs',
        return_value=json.dumps({"data": []})
    )

    result = runner.invoke(app, ["unfinished"])

    assert result.exit_code == 0
    assert "No unfinished jobs" in result.stdout


def test_unfinished_jobs_accepts_nested_recovery_summary(mocker):
    mocker.patch(
        'mn_cli.libs.job_cmds.client.list_jobs',
        return_value=json.dumps({"data": [
            {
                "job_id": "job-nested",
                "graph_id": "g-nested",
                "status": "paused",
                "submitted_at": "2026-05-05T00:00:00Z",
                "recovery": {
                    "status": "paused_for_review",
                    "requires_review": True,
                },
            }
        ]})
    )

    result = runner.invoke(app, ["unfinished"])

    assert result.exit_code == 0
    assert "job-nested" in result.stdout
    assert "paused_for_review" in result.stdout
    assert "review=yes" in result.stdout

def test_nodes_success(mocker):
    mock_nodes = mocker.patch('mn_cli.libs.job_cmds.client.get_system_summary', return_value='{"nodes": ["node1"]}')
    result = runner.invoke(app, ["nodes"])
    assert result.exit_code == 0
    assert "node1" in result.stdout
    mock_nodes.assert_called_once()

def test_nodes_error(mocker):
    mocker.patch('mn_cli.libs.job_cmds.client.get_system_summary', side_effect=Exception("Fail"))
    result = runner.invoke(app, ["nodes"])
    assert result.exit_code == 0
    assert "Error fetching nodes: Fail" in result.stdout


def test_metrics_success(mocker):
    mocker.patch(
        'mn_cli.libs.job_cmds.client.get_system_summary',
        return_value=json.dumps({"nodes": ["n1"], "jobs": [{"status": "running"}]}),
    )
    result = runner.invoke(app, ["metrics"])
    assert result.exit_code == 0
    assert '"running": 1' in result.stdout


def test_dead_letters_success(mocker):
    mocker.patch(
        'mn_cli.libs.job_cmds.client.stream_events',
        return_value=[
            json.dumps({"type": "agent_started"}),
            json.dumps({"type": "dead_letter", "agent_id": "a1", "reason": "queue full"}),
        ],
    )
    result = runner.invoke(app, ["dead-letters", "job-1"])
    assert result.exit_code == 0
    assert "queue full" in result.stdout
