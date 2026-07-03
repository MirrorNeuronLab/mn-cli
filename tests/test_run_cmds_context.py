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

def test_run_ensures_context_engine_when_blueprint_memory_enabled(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="context-run")
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-context")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])
    mock_ensure = mocker.patch(
        "mn_cli.libs.run_cmds.ensure_context_engine_runtime",
        return_value={"status": "started", "service": "membrane-context-engine"},
    )

    bundle_dir = tmp_path / "context_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({"nodes": []}))
    (bundle_dir / "payloads").mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "memory_layer": {
                    "enabled": True,
                    "enabled_env": "MN_CONTEXT_MEMORY_ENABLED",
                    "sdk_import_package": "mn_context_engine_sdk",
                }
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--force"])

    assert result.exit_code == 0
    stdout_text = re.sub(r"\s+", " ", result.stdout)
    assert "This blueprint uses context memory" in result.stdout
    assert "First launch may download the context model" in stdout_text
    assert "Context memory ready" in result.stdout
    assert "Launch: Check runtime resources" in result.stdout
    assert "Launch: Package workflow" in result.stdout
    assert "Launch: Submit runtime job" in result.stdout
    mock_ensure.assert_called_once_with(force=True)
    mock_submit.assert_called_once()

def test_runtime_ensure_context_engine_explains_first_launch(mocker):
    mock_ensure = mocker.patch(
        "mn_cli.libs.sys_cmds.ensure_context_engine_runtime",
        return_value={
            "status": "started",
            "service": "membrane-context-engine",
            "model": "hf.co/example/context-model",
            "membrane_dir": "/tmp/Membrane",
        },
    )

    result = runner.invoke(app, ["runtime", "ensure-context-engine"])

    assert result.exit_code == 0
    stdout_text = re.sub(r"\s+", " ", result.stdout)
    assert "This runtime service powers blueprint context memory" in result.stdout
    assert "First launch may download the context model" in stdout_text
    assert "Context engine" in result.stdout
    assert "hf.co/example/context-model" in result.stdout
    assert "/tmp/Membrane" in result.stdout
    mock_ensure.assert_called_once_with(force=False)

def test_runtime_ensure_context_engine_reports_release_image(mocker):
    mock_ensure = mocker.patch(
        "mn_cli.libs.sys_cmds.ensure_context_engine_runtime",
        return_value={
            "status": "started",
            "service": "membrane-context-engine",
            "model": "hf.co/example/context-model",
            "engine_image": "us-central1-docker.pkg.dev/example/runtime/membrane-context-engine:v1.2.14",
        },
    )

    result = runner.invoke(app, ["runtime", "ensure-context-engine"])

    assert result.exit_code == 0
    assert "Context engine" in result.stdout
    assert "hf.co/example/context-model" in result.stdout
    assert "membrane-context-engine:v1.2.14" in result.stdout
    mock_ensure.assert_called_once_with(force=False)

def test_run_does_not_ensure_context_engine_when_memory_disabled_by_env(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("MN_CONTEXT_MEMORY_ENABLED", "0")
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="context-disabled-run")
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-context-disabled")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])
    mock_ensure = mocker.patch("mn_cli.libs.run_cmds.ensure_context_engine_runtime")

    bundle_dir = tmp_path / "context_disabled_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({"nodes": []}))
    (bundle_dir / "payloads").mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "memory_layer": {
                    "enabled": True,
                    "enabled_env": "MN_CONTEXT_MEMORY_ENABLED",
                    "sdk_import_package": "mn_context_engine_sdk",
                }
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--force"])

    assert result.exit_code == 0
    mock_ensure.assert_not_called()
    mock_submit.assert_called_once()
