import json
import logging
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
import pytest
from logging.handlers import RotatingFileHandler
from typer.testing import CliRunner
from rich.console import Console
from mn_cli.main import app
from mn_cli.libs import model_cmds, run_cmds
from mn_cli.libs.ui import JobMonitorState, generate_live_layout
from mn_cli.libs.workflow_progress import BlueprintWorkflowProgress, _agent_progress_detail
from mn_cli.libs.run_manifest import prepare_manifest_for_submission
from mn_sdk import AgentProgress, load_model_ownership, load_model_remotes, upsert_model_remote

runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_mn_home(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_HOME", str(tmp_path / "mn-home"))
    monkeypatch.delenv("MN_SHARED_STORAGE_ROOT", raising=False)
    monkeypatch.delenv("MN_HOST_SHARED_STORAGE_ROOT", raising=False)
    monkeypatch.delenv("MN_RUNTIME_SHARED_STORAGE_ROOT", raising=False)
    monkeypatch.delenv("MN_CONTAINER_SHARED_STORAGE_ROOT", raising=False)
    monkeypatch.setattr(
        run_cmds,
        "sync_litellm_gateway",
        lambda **_kwargs: {"status": "running", "api_base": "http://mn-litellm-proxy:4000/v1"},
    )

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

def test_stream_bad_json(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        "invalid json format",
        json.dumps({"type": "job_failed"})
    ])
    
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Job Status: Failed" in result.stdout

def test_stream_all_events(mocker, tmp_path):
    events = [
        json.dumps({"type": "job_validated"}),
        json.dumps({"type": "job_scheduled"}),
        json.dumps({"type": "job_running"}),
        json.dumps({"type": "agent_message_received"}),
        json.dumps({"type": "custom_progressive", "payload": {"foo": "progressive"}}),
        json.dumps({"type": "job_completed", "result": {"foo": "bar"}})
    ]
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=events)
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Job Status: Success" in result.stdout
    assert "result.txt" in result.stdout
    assert "result_stream.txt" in result.stdout

def test_stream_cancelled_event_is_terminal(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_running"}),
        json.dumps({"type": "job_cancelled"}),
    ])
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mock_get = mocker.patch('mn_cli.libs.run_cmds.client.get_job')

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Job Status: Cancelled" in result.stdout
    mock_get.assert_not_called()

def test_stream_helper_cancelled_event_is_terminal_without_follow(mocker, tmp_path):
    job_id = f"job-cancelled-{uuid.uuid4().hex}"
    log_writer = run_cmds.JobLogWriter(job_id, run_dir=tmp_path / "run")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_running"}),
        json.dumps({"type": "job_cancelled"}),
    ])
    mock_get = mocker.patch('mn_cli.libs.run_cmds.client.get_job')

    status = run_cmds._stream_and_format_events(job_id, log_writer=log_writer, follow_seconds=0)

    assert status == "cancelled"
    assert '"type": "job_cancelled"' in log_writer.events_file.read_text()
    mock_get.assert_not_called()

def test_stream_keyboard_interrupt(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', side_effect=KeyboardInterrupt)
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Detached from workflow UI. Job is still running." in result.stdout

def test_post_submit_keyboard_interrupt_detaches_without_stopping_job(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-started")
    mocker.patch(
        'mn_cli.libs.run_cmds._stream_and_format_events',
        side_effect=KeyboardInterrupt,
    )
    mocker.patch(
        'mn_cli.libs.run_cmds.client.get_job',
        return_value=json.dumps({
            "summary": {"status": "running"},
            "job": {"status": "running"},
            "recent_events": [],
        }),
    )

    bundle_dir = tmp_path / "started_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({"nodes": []}))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Detached from workflow UI. Job is still running." in result.stdout
    assert "Run Detached" in result.stdout
    assert "Running" in result.stdout
