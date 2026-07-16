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
    assert "→ Check runtime resources" in result.stdout
    assert "→ Package workflow" in result.stdout
    assert "→ Submit runtime job" in result.stdout
    mock_ensure.assert_called_once_with(force=True)
    mock_submit.assert_called_once()


def test_context_engine_prepares_on_selected_workflow_node(mocker, tmp_path):
    bundle_dir = tmp_path / "remote_context_bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps({"memory_layer": {"enabled": True, "sdk_import_package": "mn_context_engine_sdk"}}),
        encoding="utf-8",
    )
    manifest = {"runtime": {"memory": {"enabled": True}}}
    endpoint = {"node": {"name": "mirror_neuron@spark"}}
    runtime_client = object()
    mocker.patch("mn_cli.libs.run_cmds.context._cluster_node_endpoint", return_value=endpoint)
    mocker.patch("mn_cli.libs.run_cmds.context._runtime_model_prepare_client", return_value=runtime_client)
    prepare = mocker.patch(
        "mn_cli.libs.run_cmds.context._prepare_runtime_model_with_retry",
        return_value={
            "status": "started",
            "service": "membrane-context-engine",
            "model": "hf.co/context",
            "node": "mirror_neuron@spark",
        },
    )
    local_ensure = mocker.patch("mn_cli.libs.run_cmds.context.ensure_context_engine_runtime")

    summary = run_cmds._ensure_context_engine_for_run_if_needed(
        bundle_dir,
        manifest,
        env_overrides={"MN_SELECTED_RUNTIME_NODE": "mirror_neuron@spark"},
        force=True,
    )

    assert summary["node"] == "mirror_neuron@spark"
    assert prepare.call_args.args[0] is runtime_client
    assert prepare.call_args.args[1] == {
        "node": "mirror_neuron@spark",
        "purpose": "context_engine",
        "ensure_context_engine": True,
        "force": True,
        "source": "mn-cli-workflow-placement",
    }
    local_ensure.assert_not_called()

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
    assert "Engine image" in result.stdout
    assert "v1.2.14" in result.stdout
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
