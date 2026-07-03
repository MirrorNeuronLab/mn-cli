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

def test_monitor_success(mocker):
    mocker.patch('mn_cli.libs.run_cmds.client.get_job', return_value=json.dumps({"summary": {"status": "completed", "live?": False}, "job": {"job_name": "test"}, "agents": [{"agent_id": "a1", "status": "running", "processed_messages": 10}]}))
    mocker.patch('sys.stdin.isatty', return_value=False)
    
    result = runner.invoke(app, ["job", "monitor", "job-123"])
    
    assert result.exit_code == 0
    assert "Workflow Job Monitor" in result.stdout
    assert "keys: j/k or arrows select agent" in result.stdout
    assert "Job Execution Summary" in result.stdout

def test_job_monitor_keyboard_state_and_agent_detail():
    state = JobMonitorState()
    assert state.handle_key("j", 2) is True
    assert state.selected_index == 1
    assert state.handle_key("d", 2) is True
    assert state.detail_mode is True

    console = Console(record=True, width=160, force_terminal=False)
    console.print(
        generate_live_layout(
            "job-123",
            {
                "summary": {"status": "running", "live?": True, "nodes": ["worker"]},
                "job": {"job_name": "test", "graph_id": "graph"},
                "agents": [
                    {"agent_id": "a1", "agent_type": "router", "status": "running", "processed_messages": 20},
                    {
                        "agent_id": "a2",
                        "agent_type": "executor",
                        "status": "busy",
                        "current_task": "Inspect document batch",
                        "processed_messages": 10,
                        "mailbox_depth": 3,
                    },
                ],
            },
            state=state,
        )
    )
    output = console.export_text()
    assert "Agent Detail" in output
    assert "a2" in output
    assert "Inspect document batch" in output

    assert state.handle_key("o", 2) is True
    assert state.detail_mode is False
    assert state.handle_key("\x04", 2) is False
    assert state.handle_key("q", 2) is False

def test_monitor_error(mocker):
    mocker.patch('sys.stdin.isatty', return_value=False)
    mocker.patch('mn_cli.libs.run_cmds.client.get_job', side_effect=Exception("Network fail"))
    result = runner.invoke(app, ["job", "monitor", "job-123"])
    assert result.exit_code == 0
    assert "Error fetching job: Network fail" in result.stdout

def test_result_success(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.get_job', return_value=json.dumps({
        "job": {"status": "completed", "result": {"test": "result"}},
        "recent_events": []
    }))
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "custom_event", "payload": "progressive"})
    ])
    
    result = runner.invoke(app, ["job", "result", "job-123"])
    
    assert result.exit_code == 0
    assert "Job result fetch successful." in result.stdout
    assert "Final result:" in result.stdout
    assert "Stream results:" in result.stdout

def test_result_not_completed(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.get_job', return_value=json.dumps({
        "job": {"status": "running"},
        "recent_events": []
    }))
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[])
    
    result = runner.invoke(app, ["job", "result", "job-999"])
    
    assert result.exit_code == 0
    assert "No final result found" in result.stdout

def test_result_error(mocker):
    mocker.patch('mn_cli.libs.run_cmds.fetch_and_save_results', side_effect=Exception("DB Error"))
    
    result = runner.invoke(app, ["job", "result", "job-888"])
    
    assert result.exit_code == 0
    assert "Error fetching results: DB Error" in result.stdout
