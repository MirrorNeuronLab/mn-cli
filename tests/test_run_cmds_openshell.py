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

def test_openshell_env_prefers_active_gateway_metadata(tmp_path, monkeypatch):
    config_dir = tmp_path / "openshell-config"
    gateway_dir = config_dir / "gateways" / "openshell"
    gateway_dir.mkdir(parents=True)
    (config_dir / "active_gateway").write_text("openshell\n")
    (gateway_dir / "metadata.json").write_text(json.dumps({
        "name": "openshell",
        "gateway_endpoint": "https://127.0.0.1:8080",
    }))
    monkeypatch.setenv("OPENSHELL_CONFIG_DIR", str(config_dir))
    monkeypatch.delenv("OPENSHELL_GATEWAY", raising=False)
    monkeypatch.delenv("OPENSHELL_GATEWAY_ENDPOINT", raising=False)

    env = run_cmds._openshell_env()

    assert env["OPENSHELL_GATEWAY"] == "openshell"
    assert "OPENSHELL_GATEWAY_ENDPOINT" not in env
    assert run_cmds._openshell_gateway_endpoint() == "https://127.0.0.1:8080"

def test_run_prebuilds_custom_openshell_image_from_payload_directory(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("OPENSHELL_CONFIG_DIR", str(tmp_path / "openshell-config"))
    monkeypatch.delenv("OPENSHELL_GATEWAY", raising=False)
    monkeypatch.delenv("OPENSHELL_GATEWAY_ENDPOINT", raising=False)
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="openshell-from-run")
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])
    mock_build = mocker.patch(
        'mn_cli.libs.run_cmds.subprocess.run',
        return_value=mocker.Mock(
            returncode=0,
            stdout="Image \x1b[36mopenshell/sandbox-from:123\x1b[39m is available in the gateway.\n",
            stderr="",
        ),
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": "openshell-workflow",
        "flow": {
            "nodes": [
                {
                    "node_id": "detector",
                    "config": {
                        "runner_module": "MirrorNeuron.Runner.OpenShell",
                        "custom_openshell_image": "detector/openshell_sandbox",
                    },
                }
            ]
        },
    }))
    sandbox_dir = bundle_dir / "payloads" / "detector" / "openshell_sandbox"
    sandbox_dir.mkdir(parents=True)
    (sandbox_dir / "Dockerfile").write_text("FROM base\n")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "OpenShell sandbox image build successful." in result.stdout
    mock_build.assert_called_once()
    assert mock_build.call_args.kwargs["env"]["OPENSHELL_GATEWAY_ENDPOINT"] == "http://127.0.0.1:58080"
    manifest = json.loads(mock_submit.call_args.args[0])
    assert manifest["flow"]["nodes"][0]["config"]["custom_openshell_image"] == "detector/openshell_sandbox"
    assert manifest["flow"]["nodes"][0]["config"]["from"] == "openshell/sandbox-from:123"

def test_run_prebuilds_legacy_openshell_from_directory(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("OPENSHELL_CONFIG_DIR", str(tmp_path / "openshell-config"))
    monkeypatch.delenv("OPENSHELL_GATEWAY", raising=False)
    monkeypatch.delenv("OPENSHELL_GATEWAY_ENDPOINT", raising=False)
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="openshell-from-run")
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])
    mocker.patch(
        'mn_cli.libs.run_cmds.subprocess.run',
        return_value=mocker.Mock(
            returncode=0,
            stdout="Image openshell/sandbox-from:456 is available in the gateway.\n",
            stderr="",
        ),
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "detector",
                "config": {
                    "runner_module": "MirrorNeuron.Sandbox.OpenShell",
                    "from": "detector/openshell_sandbox",
                },
            }
        ]
    }))
    sandbox_dir = bundle_dir / "payloads" / "detector" / "openshell_sandbox"
    sandbox_dir.mkdir(parents=True)
    (sandbox_dir / "Dockerfile").write_text("FROM base\n")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    manifest = json.loads(mock_submit.call_args.args[0])
    assert manifest["nodes"][0]["config"]["from"] == "openshell/sandbox-from:456"

def test_openshell_skill_dependency_context_injects_pinned_gar_install(tmp_path):
    sandbox_dir = tmp_path / "openshell_sandbox"
    sandbox_dir.mkdir()
    (sandbox_dir / "Dockerfile").write_text("FROM python:3.11-slim\n", encoding="utf-8")
    manifest = {
        "skill_dependencies": [
            {
                "type": "pip",
                "source": "gar",
                "name": "mirrorneuron-websocket-stream-skill",
                "version": "1.2.7",
            }
        ]
    }

    context = run_cmds._openshell_skill_dependency_context(sandbox_dir, manifest)
    try:
        dockerfile = (context / "Dockerfile").read_text(encoding="utf-8")
        requirements = (context / "requirements.txt").read_text(encoding="utf-8")
    finally:
        if context != sandbox_dir:
            run_cmds.shutil.rmtree(context, ignore_errors=True)

    assert context != sandbox_dir
    assert "mirrorneuron-websocket-stream-skill==1.2.7" in requirements
    assert "https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/" in requirements
    assert "--index-url\n" not in requirements
    assert "--index-url https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/" in requirements
    assert "--extra-index-url https://pypi.org/simple" in requirements
    assert "COPY requirements.txt /tmp/mn-skill-runtime/requirements.txt" in dockerfile
    assert "pip install --timeout 120 --retries 10 --break-system-packages --no-cache-dir -r /tmp/mn-skill-runtime/requirements.txt" in dockerfile


def test_openshell_skill_dependency_context_injects_local_dev_sources(tmp_path):
    sandbox_dir = tmp_path / "openshell_sandbox"
    sandbox_dir.mkdir()
    (sandbox_dir / "Dockerfile").write_text("FROM python:3.11-slim\n", encoding="utf-8")
    local_skill = tmp_path / "example_skill"
    local_skill.mkdir()
    (local_skill / "pyproject.toml").write_text(
        "[project]\nname='mirrorneuron-example-skill'\nversion='1.0.0'\n",
        encoding="utf-8",
    )
    manifest = {
        "metadata": {
            "mn_local_skill_dependencies": {
                "sources": [
                    {
                        "package": "mirrorneuron-example-skill",
                        "source": str(local_skill),
                    }
                ]
            }
        }
    }

    context = run_cmds._openshell_skill_dependency_context(sandbox_dir, manifest)
    try:
        dockerfile = (context / "Dockerfile").read_text(encoding="utf-8")
        local_requirements = (context / "local-requirements.txt").read_text(encoding="utf-8")
        staged_source = (
            context
            / "__mn_skill_dependencies"
            / "local"
            / "example_skill"
            / "pyproject.toml"
        )
        assert staged_source.is_file()
    finally:
        if context != sandbox_dir:
            run_cmds.shutil.rmtree(context, ignore_errors=True)

    assert "/tmp/mn-skill-runtime/local/example_skill" in local_requirements
    assert "COPY __mn_skill_dependencies/local/example_skill" in dockerfile
    assert "-r /tmp/mn-skill-runtime/local-requirements.txt" in dockerfile


def test_local_docker_openshell_build_uses_plain_progress(mocker, tmp_path):
    sandbox_dir = tmp_path / "openshell_sandbox"
    sandbox_dir.mkdir()
    (sandbox_dir / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    mock_build = mocker.patch(
        "mn_cli.libs.run_cmds._run_streaming_local_docker_build",
        return_value=subprocess.CompletedProcess(["docker"], 0, "ok", ""),
    )

    image = run_cmds._build_local_docker_sandbox_image(sandbox_dir)

    assert image.startswith("openshell/sandbox-from:")
    command = mock_build.call_args.args[0]
    assert command[:3] == ["docker", "build", "--progress=plain"]
