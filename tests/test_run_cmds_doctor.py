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

def test_doctor_bundle_prepares_without_submitting_job(mocker, tmp_path):
    bundle_dir = tmp_path / "doctor_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({"nodes": []}))
    mocker.patch(
        "mn_cli.libs.run_cmds._doctor_runtime_foundation",
        return_value={"status": "passing", "detail": "runtime ok"},
    )
    mocker.patch("mn_cli.libs.run_cmds._doctor_validate_hardware", return_value={"ok": True})
    mocker.patch("mn_cli.libs.run_cmds._doctor_validate_services", return_value={"ok": True})
    mocker.patch("mn_cli.libs.run_cmds._doctor_validate_inputs", return_value={"ok": True})
    mocker.patch("mn_cli.libs.run_cmds._doctor_validate_models", return_value={"ok": True})
    mocker.patch("mn_cli.libs.run_cmds._prepare_runtime_models_for_run_or_exit", return_value={"models": []})
    mocker.patch(
        "mn_cli.libs.run_cmds.prepare_manifest_for_submission",
        side_effect=lambda _bundle_dir, manifest, **_kwargs: manifest,
    )
    mocker.patch("mn_cli.libs.run_cmds._prepare_openshell_custom_images")
    mocker.patch(
        "mn_cli.libs.run_cmds._doctor_prepare_hostlocal_python_envs",
        return_value={"status": "skipped", "detail": "none"},
    )
    mocker.patch("mn_cli.libs.run_cmds._stage_bundle_payloads", return_value={})
    mocker.patch("mn_cli.libs.run_cmds.blueprint_requires_context_engine", return_value=False)
    mock_prepare = mocker.patch(
        "mn_cli.libs.run_cmds.prepare_job_submission",
        return_value=SimpleNamespace(
            manifest_json=json.dumps({"metadata": {}}),
            payloads={},
            metadata={"submission_id": "doctor-submission"},
        ),
    )
    mock_submit = mocker.patch("mn_cli.libs.run_cmds.client.submit_job")
    mocker.patch("mn_cli.libs.run_cmds._doctor_print_report")

    report = run_cmds.doctor_bundle(str(bundle_dir), no_llm_call=True)

    assert report["summary"]["status"] == "passing"
    mock_prepare.assert_called_once()
    mock_submit.assert_not_called()

def test_doctor_bundle_check_only_skips_openshell_build(mocker, tmp_path):
    bundle_dir = tmp_path / "doctor_openshell_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "node_id": "shell",
                        "config": {"runner_module": "MirrorNeuron.Sandbox.OpenShell"},
                    }
                ]
            }
        )
    )
    mocker.patch(
        "mn_cli.libs.run_cmds._doctor_runtime_foundation",
        return_value={"status": "passing", "detail": "runtime ok"},
    )
    mocker.patch("mn_cli.libs.run_cmds._doctor_validate_hardware", return_value={"ok": True})
    mocker.patch("mn_cli.libs.run_cmds._doctor_validate_services", return_value={"ok": True})
    mocker.patch("mn_cli.libs.run_cmds._doctor_validate_inputs", return_value={"ok": True})
    mocker.patch("mn_cli.libs.run_cmds._doctor_validate_models", return_value={"ok": True})
    mocker.patch(
        "mn_cli.libs.run_cmds.prepare_manifest_for_submission",
        side_effect=lambda _bundle_dir, manifest, **_kwargs: manifest,
    )
    mock_build = mocker.patch("mn_cli.libs.run_cmds._prepare_openshell_custom_images")
    mocker.patch("mn_cli.libs.run_cmds._stage_bundle_payloads", return_value={})
    mock_prepare_job = mocker.patch("mn_cli.libs.run_cmds.prepare_job_submission")
    mocker.patch("mn_cli.libs.run_cmds._doctor_print_report")

    report = run_cmds.doctor_bundle(str(bundle_dir), check_only=True, no_llm_call=True)

    assert report["summary"]["status"] == "warning"
    assert report["environments"]["openshell"]["status"] == "warning"
    mock_build.assert_not_called()
    mock_prepare_job.assert_not_called()

def test_doctor_summary_redacts_and_marks_critical():
    report = {
        "runtime": {"status": "passing"},
        "models": {"status": "critical"},
        "config": {"api_key": "secret", "nested": {"token": "abc", "plain": "ok"}},
    }

    summary = run_cmds._doctor_summary(report)
    redacted = run_cmds._doctor_redact(report)

    assert summary["status"] == "critical"
    assert redacted["config"]["api_key"] == "[redacted]"
    assert redacted["config"]["nested"]["token"] == "[redacted]"
    assert redacted["config"]["nested"]["plain"] == "ok"

def test_doctor_environment_probe_reports(mocker, tmp_path):
    env_dir = tmp_path / "venv"
    mocker.patch("mn_cli.libs.run_cmds._doctor_prepare_python_env", return_value=env_dir)
    host_manifest = {
        "nodes": [
            {
                "node_id": "native",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "python_environment": {"packages": ["requests"]},
                },
            }
        ]
    }

    host_report = run_cmds._doctor_prepare_hostlocal_python_envs(
        tmp_path,
        host_manifest,
        timeout=1,
        check_only=False,
    )
    openshell_report = run_cmds._doctor_openshell_report(
        {
            "nodes": [
                {
                    "node_id": "shell",
                    "config": {"runner_module": "MirrorNeuron.Sandbox.OpenShell"},
                }
            ]
        }
    )
    docker_report = run_cmds._doctor_docker_worker_report(
        {
            "prepared": True,
            "services": [{"service": "worker", "container_name": "mn-worker", "image": "worker:latest"}],
        }
    )

    assert host_report["status"] == "passing"
    assert host_report["prepared"][0]["path"] == str(env_dir)
    assert openshell_report["status"] == "passing"
    assert openshell_report["nodes"] == ["shell"]
    assert docker_report["status"] == "passing"
    assert docker_report["services"][0]["service"] == "worker"


def test_doctor_prepares_hostlocal_python_on_selected_remote_node(mocker, tmp_path):
    manifest = {
        "metadata": {"mn_workflow_placement": {"selected_node": "mirror_neuron@spark"}},
        "nodes": [
            {
                "node_id": "report_writer",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "python_environment": {"packages": ["requests==2.32.0"]},
                },
            }
        ],
    }
    runtime_client = object()
    mocker.patch("mn_cli.libs.run_cmds.handlers.doctor._local_runtime_node_name", return_value="mirror_neuron@mac")
    mocker.patch("mn_cli.libs.run_cmds.handlers.doctor._cluster_node_endpoint", return_value={"node": {}})
    mocker.patch("mn_cli.libs.run_cmds.handlers.doctor._runtime_model_prepare_client", return_value=runtime_client)
    prepare = mocker.patch(
        "mn_cli.libs.run_cmds.handlers.doctor._prepare_runtime_model_with_retry",
        return_value={"runtime_path": "/runtime/shared/blueprint-python-envs/remote", "host_path": "/host/shared/blueprint-python-envs/remote"},
    )
    local_prepare = mocker.patch("mn_cli.libs.run_cmds.handlers.doctor._doctor_prepare_python_env")

    report = run_cmds._doctor_prepare_hostlocal_python_envs(
        tmp_path,
        manifest,
        timeout=1,
        check_only=False,
    )

    assert report["status"] == "passing"
    assert manifest["nodes"][0]["config"]["python_environment"]["path"] == "/runtime/shared/blueprint-python-envs/remote"
    assert prepare.call_args.args[0] is runtime_client
    assert prepare.call_args.args[1]["node"] == "mirror_neuron@spark"
    assert prepare.call_args.args[1]["ensure_hostlocal_python_environment"] is True
    local_prepare.assert_not_called()


def test_doctor_maps_prepared_python_environment_into_runtime_shared_storage(tmp_path, monkeypatch):
    host_root = tmp_path / "host-shared"
    runtime_root = Path("/runtime/shared")
    monkeypatch.setenv("MN_SHARED_STORAGE_ROOT", str(host_root))
    monkeypatch.setenv("MN_RUNTIME_SHARED_STORAGE_ROOT", str(runtime_root))

    mapped = run_cmds._doctor_runtime_python_env_path(
        host_root / "blueprint-python-envs" / "digest"
    )

    assert mapped == runtime_root / "blueprint-python-envs" / "digest"


def test_doctor_prepares_hostlocal_python_environment_inside_docker_core(tmp_path, monkeypatch, mocker):
    bundle_dir = tmp_path / "bundle"
    requirements = bundle_dir / "payloads" / "worker" / "requirements.txt"
    requirements.parent.mkdir(parents=True)
    requirements.write_text("gradio==5.0.0\n", encoding="utf-8")
    host_root = tmp_path / "host-shared"
    runtime_root = Path("/runtime/shared")
    monkeypatch.setenv("MN_SHARED_STORAGE_ROOT", str(host_root))
    monkeypatch.setenv("MN_RUNTIME_SHARED_STORAGE_ROOT", str(runtime_root))
    mocker.patch(
        "mn_cli.libs.run_cmds._doctor_running_core_container",
        return_value="mirror-neuron-core",
    )
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        if args[-1] == "--version":
            return subprocess.CompletedProcess(args, 0, stdout="Python 3.11.2\n", stderr="")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    mocker.patch("mn_cli.libs.run_cmds.subprocess.run", side_effect=fake_run)

    env_dir = run_cmds._doctor_prepare_python_env(
        bundle_dir,
        blueprint_id="docker-core-env",
        node_id="worker",
        packages=["requests==2.32.0"],
        requirements_path="worker/requirements.txt",
        timeout=1,
    )

    runtime_env_dir = runtime_root / "blueprint-python-envs" / env_dir.name
    assert calls[0][0] == ["docker", "exec", "mirror-neuron-core", "python3", "--version"]
    assert calls[1][0] == [
        "docker",
        "exec",
        "mirror-neuron-core",
        "python3",
        "-m",
        "venv",
        str(runtime_env_dir),
    ]
    assert calls[2][0] == [
        "docker",
        "exec",
        "-e",
        "PIP_DISABLE_PIP_VERSION_CHECK=1",
        "-e",
        "PIP_NO_INPUT=1",
        "mirror-neuron-core",
        str(runtime_env_dir / "bin" / "python"),
        "-m",
        "pip",
        "install",
        "-r",
        str(runtime_env_dir / ".mn-requirements.txt"),
        "requests==2.32.0",
    ]
    assert (env_dir / ".ready").is_file()
    assert (env_dir / ".mn-requirements.txt").read_text(encoding="utf-8") == "gradio==5.0.0\n"


def test_doctor_skill_report_reads_declared_dependencies(tmp_path):
    bundle_dir = tmp_path / "bundle"
    config_dir = bundle_dir / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "input_skills": {
                    "search": {
                        "skill": "web_search",
                        "package": "search-pkg",
                        "import": "json",
                        "executable": sys.executable,
                    }
                },
                "python_dependencies": {"packages": ["numpy"]},
            }
        )
    )

    report = run_cmds._doctor_skill_report(bundle_dir, {})

    assert report["status"] == "passing"
    assert {entry["name"] for entry in report["entries"]} == {"search", "numpy"}

def test_doctor_llm_and_embedding_smoke_uses_host_reachable_urls(mocker):
    calls = []

    def fake_post(name, url, payload, *, timeout):
        calls.append((name, url, payload, timeout))
        return {"name": name, "status": "passing", "url": url}

    mocker.patch("mn_cli.libs.run_cmds._doctor_post_openai_payload", side_effect=fake_post)

    chat = run_cmds._doctor_chat_smoke(
        "primary",
        {
            "provider": "docker_model_runner",
            "model": "gemma4:e2b",
            "api_base": "http://mn-litellm-proxy:4000/v1",
        },
        {},
        timeout=2,
    )
    embedding = run_cmds._doctor_embedding_smoke(
        {
            "embedding_model": "embedding-model",
            "embedding_api_base": "http://host.docker.internal:12434/engines/v1",
        },
        timeout=2,
    )

    assert chat["status"] == "passing"
    assert embedding["status"] == "passing"
    assert calls[0][1] == "http://127.0.0.1:4000/v1/chat/completions"
    assert calls[1][1] == "http://127.0.0.1:12434/engines/v1/embeddings"
