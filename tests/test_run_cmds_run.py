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

def test_run_success(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="run-bundle-auto")
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
    nested_payloads = payloads_dir / "nested"
    nested_payloads.mkdir()
    (nested_payloads / "input.json").write_text("{}")
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--web-ui"])
    
    assert result.exit_code == 0
    assert "Job submit successful" in result.stdout
    assert "run-bundle-auto" in result.stdout
    assert "Type" in result.stdout
    assert "Batch" in result.stdout
    assert "Job Status: Success" in result.stdout
    mapping = json.loads((tmp_path / "runs" / "run-bundle-auto" / "job.json").read_text())
    assert mapping["job_id"] == "job-123"
    mock_submit.assert_called_once()
    submitted_payloads = mock_submit.call_args.args[1]
    assert submitted_payloads["test.txt"] == b"hello"
    assert submitted_payloads["nested/input.json"] == b"{}"
    mock_stream.assert_called_once_with("job-123", follow=True, timeout=None, heartbeat_interval_ms=5000)

def test_run_stream_error_falls_back_to_status_polling(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch("mn_cli.libs.run_cmds._make_blueprint_run_id", return_value="run-stream-fallback")
    mocker.patch("mn_cli.libs.run_cmds.client.submit_job", return_value="job-stream-fallback")
    mocker.patch(
        "mn_cli.libs.run_cmds.client.stream_events",
        side_effect=RuntimeError("resource exhausted by event stream"),
    )
    mock_get = mocker.patch(
        "mn_cli.libs.run_cmds.client.get_job",
        return_value=json.dumps(
            {
                "job": {"status": "completed", "result": {"ok": True}},
                "summary": {"status": "completed"},
                "recent_events": [
                    {
                        "type": "job_completed",
                        "result": {"ok": True},
                    }
                ],
            }
        ),
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text('{"nodes": []}')

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--follow-seconds", "0"])

    assert result.exit_code == 0
    assert "Job submit successful" in result.stdout
    assert "Completed" in result.stdout
    assert "Monitor" in result.stdout
    mock_get.assert_called_once_with("job-stream-fallback")

def test_run_prepares_runtime_models_before_model_validation(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="run-order")
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-order")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"}),
    ])
    order: list[str] = []

    mocker.patch(
        "mn_cli.libs.run_cmds._validate_manifest_services_or_exit",
        side_effect=lambda *args, **kwargs: order.append("services") or {"ok": True},
    )
    mocker.patch(
        "mn_cli.libs.run_cmds._prepare_runtime_models_for_run_or_exit",
        side_effect=lambda *args, **kwargs: order.append("prepare_models") or {"ok": True},
    )
    mocker.patch(
        "mn_cli.libs.run_cmds._validate_manifest_models_or_exit",
        side_effect=lambda *args, **kwargs: order.append("validate_models") or {"ok": True},
    )
    mocker.patch(
        "mn_cli.libs.run_cmds._validate_manifest_inputs_or_exit",
        side_effect=lambda *args, **kwargs: order.append("inputs") or {"ok": True},
    )

    bundle_dir = tmp_path / "run_order_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({"nodes": []}))

    run_cmds.run_bundle(str(bundle_dir), follow_seconds=0)

    assert order == ["services", "prepare_models", "validate_models", "inputs"]

def test_run_auto_schedule_creates_resource_wait_schedule(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="run-scheduled")
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job')
    mock_create_schedule = mocker.patch(
        'mn_cli.libs.run_cmds.client.create_schedule',
        return_value=json.dumps({"schedule_id": "schedule-123", "kind": "resource_wait"}),
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text('{"nodes": []}')
    (bundle_dir / "payloads").mkdir()

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--auto-schedule"])

    assert result.exit_code == 0
    assert "Schedule create successful." in result.stdout
    assert "schedule-123" in result.stdout
    mock_submit.assert_not_called()
    mock_create_schedule.assert_called_once()
    assert mock_create_schedule.call_args.kwargs["schedule"]["kind"] == "resource_wait"

def test_run_force_skips_input_validation(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="forced-run")
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [],
        "input_validation": {
            "rules": [
                {
                    "name": "missing_command",
                    "type": "command",
                    "command": ["definitely-missing-validator"],
                }
            ]
        },
    }))
    (bundle_dir / "payloads").mkdir()

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--force"])

    assert result.exit_code == 0
    assert "Validation skipped because --force was provided" in result.stdout
    manifest = json.loads(mock_submit.call_args.args[0])
    assert manifest["metadata"]["mn_validation"]["force"] is True
    assert manifest["metadata"]["mn_validation"]["status"] == "skipped"
    assert manifest["metadata"]["mn_validation"]["skipped_checks"] == [
        "services",
        "models",
        "input_validation",
        "soft_requirements",
    ]
    assert mock_submit.call_args.kwargs["force"] is True


def test_run_prevalidates_command_rules_before_core_submission(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch("mn_cli.libs.run_cmds._make_blueprint_run_id", return_value="prevalidated-run")
    mock_submit = mocker.patch("mn_cli.libs.run_cmds.client.submit_job", return_value="job-123")
    mocker.patch(
        "mn_cli.libs.run_cmds.client.stream_events",
        return_value=[json.dumps({"type": "job_completed"})],
    )

    bundle_dir = tmp_path / "prevalidated_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "job_name": "prevalidated-bundle",
                "nodes": [],
                "input_validation": {
                    "rules": [
                        {
                            "name": "host_validator",
                            "type": "command",
                            "command": [sys.executable, "-c", "print('validated')"],
                        },
                        {
                            "name": "job_name",
                            "type": "pattern",
                            "source": "manifest",
                            "path": "job_name",
                            "pattern": "^prevalidated-",
                        },
                    ]
                },
            }
        )
    )
    (bundle_dir / "payloads").mkdir()

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    submitted_manifest = json.loads(mock_submit.call_args.args[0])
    assert submitted_manifest["input_validation"]["rules"] == [
        {
            "name": "job_name",
            "type": "pattern",
            "source": "manifest",
            "path": "job_name",
            "pattern": "^prevalidated-",
        }
    ]
    validation = submitted_manifest["metadata"]["mn_validation"]["input_validation"]
    assert validation == {
        "status": "passed",
        "validator": "mn-python-sdk",
        "prevalidated_command_rules": [
            {"name": "host_validator", "type": "command", "index": 0}
        ],
    }

def test_run_submits_python_environment_requirements_payload(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="python-env-run")
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
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "upload_path": "worker",
                    "upload_as": "worker",
                    "python_environment": {
                        "requirements": "worker/requirements.txt",
                    },
                },
            }
        ]
    }))
    payloads_dir = bundle_dir / "payloads" / "worker"
    payloads_dir.mkdir(parents=True)
    (payloads_dir / "requirements.txt").write_text("opencv-python-headless>=4.10,<5\n")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--web-ui"])

    assert result.exit_code == 0
    payloads = mock_submit.call_args.args[1]
    assert payloads["worker/requirements.txt"] == b"opencv-python-headless>=4.10,<5\n"

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
    (config_dir / "default.json").write_text(json.dumps({"identity": {"blueprint_id": "bp-1"}, "video_source": {"uri": "default"}}))
    (config_dir / "overwrite.json").write_text(json.dumps({"video_source": {"uri": "overwrite"}}))
    (bundle_dir / "scenario.json").write_text(json.dumps({"blueprint_id": "bp-1", "metrics": [], "actions": []}))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    manifest = json.loads(mock_submit.call_args.args[0])
    env = manifest["nodes"][0]["config"]["environment"]
    injected_config = json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])
    assert injected_config["identity"]["blueprint_id"] == "bp-1"
    assert injected_config["video_source"]["uri"] == "overwrite"
    assert env["VIDEO_SOURCE_URI"] == "overwrite"
    assert json.loads(env["MN_BLUEPRINT_SCENARIO_JSON"])["blueprint_id"] == "bp-1"
    assert "MN_BLUEPRINT_PRODUCT_JSON" not in env
    assert env["MN_LLM_MODEL"] == "ollama/nemotron3:33b"
    assert env["MN_LLM_API_BASE"] == "http://old"

def test_run_auto_creates_run_store_identity_for_local_blueprint(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-auto")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="bp-1-auto-run")

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "graph_id": "bp_graph",
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "environment": {},
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "upload_paths": [
                        {"source": "worker", "target": "worker"},
                        {"source": "web_ui", "target": "web_ui"},
                    ],
                    "workdir": "/sandbox/job/worker",
                },
            }
        ],
    }))
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "bp-1", "name": "Blueprint One"},
        "outputs": {"adapter": "local_run_store", "run_root": "$MN_HOME/runs", "write_run_store": True},
        "web_ui": {
            "enabled": True,
            "kind": "static_html",
            "dashboard": {"path": "payloads/web_ui/index.html"},
        },
        "manifest_config_bindings": [
            {
                "config_path": "identity.run_id",
                "manifest_path": "nodes.worker.config.environment.MN_RUN_ID",
            },
            {
                "config_path": "outputs.run_root",
                "manifest_path": "nodes.worker.config.environment.MN_RUNS_ROOT",
            },
        ],
    }))
    web_dir = bundle_dir / "payloads" / "web_ui"
    web_dir.mkdir(parents=True)
    (web_dir / "index.html").write_text("<html></html>")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--web-ui"])

    assert result.exit_code == 0
    assert "bp-1-auto-run" in result.stdout
    mapping = json.loads((tmp_path / "runs" / "bp-1-auto-run" / "job.json").read_text())
    assert mapping["job_id"] == "job-auto"
    manifest = json.loads(mock_submit.call_args.args[0])
    env = manifest["nodes"][0]["config"]["environment"]
    injected_config = json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])
    assert env["MN_RUN_ID"] == "bp-1-auto-run"
    assert env["MN_RUNS_ROOT"].startswith(str(tmp_path / "shared" / "submissions" / "bp-1-auto-run-"))
    assert env["MN_RUNS_ROOT"].endswith("/outputs/runs")
    assert injected_config["identity"]["run_id"] == "bp-1-auto-run"
    assert injected_config["outputs"]["run_root"] == env["MN_RUNS_ROOT"]
    web_ui = json.loads((tmp_path / "runs" / "bp-1-auto-run" / "web_ui.json").read_text())
    assert web_ui["adapter"] == "static_html"
    assert web_ui["title"] == "Blueprint One"
    assert web_ui["url"].startswith("file://")
    assert "index.html" in web_ui["url"]
    assert web_ui["metadata"]["registered_by"] == "mn_cli"
    assert web_ui["metadata"]["launch_adapter"] == "blueprint_static_html"
    assert not (tmp_path / "runs" / "bp-1-auto-run" / "ui.json").exists()
    config = manifest["nodes"][0]["config"]
    assert config["upload_path"] == "."
    assert config["upload_as"] == "."
    assert "upload_paths" not in config

def test_run_starts_pre_launch_hook_before_submit(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("OPENSHELL_CONFIG_DIR", str(tmp_path / "openshell-config"))
    monkeypatch.delenv("OPENSHELL_GATEWAY", raising=False)
    monkeypatch.delenv("OPENSHELL_GATEWAY_ENDPOINT", raising=False)
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-pre-launch")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"}),
    ])

    bundle_dir = tmp_path / "pre_launch_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "worker",
                "config": {"environment": {}},
            }
        ]
    }))
    script_path = bundle_dir / "scripts" / "pre-launch.sh"
    script_path.parent.mkdir()
    script_path.write_text("#!/usr/bin/env bash\n")
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "pre-launch"},
        "video_source": {"uri": "rtsp://127.0.0.1:8554/video-watch"},
    }))

    process = mocker.Mock(pid=4242)
    process.poll.return_value = None

    def fake_popen(_command, **kwargs):
        env = kwargs["env"]
        Path(env["MN_PRE_LAUNCH_READY_FILE"]).write_text(json.dumps({
            "status": "ready",
            "env": {
                "RTSP_PORT": "8561",
                "STREAM_URI": "rtsp://127.0.0.1:8561/video-watch",
                "VIDEO_SOURCE_URI": "rtsp://127.0.0.1:8561/video-watch",
            },
            "config": {
                "video_source": {"uri": "rtsp://127.0.0.1:8561/video-watch"},
                "web_ui": {"dashboard": {"default_video_source": "rtsp://127.0.0.1:8561/video-watch"}},
            },
        }))
        return process

    popen = mocker.patch("mn_cli.libs.run_cmds.subprocess.Popen", side_effect=fake_popen)

    run_cmds.run_bundle(str(bundle_dir), follow_seconds=0)

    command = popen.call_args.args[0]
    env = popen.call_args.kwargs["env"]
    assert command == ["bash", str(script_path.resolve())]
    assert env["OPENSHELL_GATEWAY_ENDPOINT"] == "http://127.0.0.1:58080"
    assert env["MN_RUN_ID"].startswith("pre-launch-")
    assert env["MN_BLUEPRINT_BUNDLE_DIR"] == str(bundle_dir)
    assert json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])["video_source"]["uri"].startswith("rtsp://")
    submitted_manifest = json.loads(run_cmds.client.submit_job.call_args.args[0])
    submitted_env = submitted_manifest["nodes"][0]["config"]["environment"]
    assert submitted_env["VIDEO_SOURCE_URI"] == "rtsp://127.0.0.1:8561/video-watch"
    assert submitted_env["STREAM_URI"] == "rtsp://127.0.0.1:8561/video-watch"
    assert submitted_env["RTSP_PORT"] == "8561"
    process_info = json.loads((tmp_path / "runs" / env["MN_RUN_ID"] / "pre_launch_process.json").read_text())
    assert process_info["pid"] == 4242
    assert process_info["process_group_id"] == 4242
    assert process_info["script"] == str(script_path.resolve())

def test_run_cleans_pre_launch_hook_on_validation_failure(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job')
    mock_killpg = mocker.patch("mn_cli.libs.run_cmds.os.killpg")
    mocker.patch("mn_cli.libs.run_cmds.os.kill")

    bundle_dir = tmp_path / "pre_launch_validation_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [],
        "input_validation": {
            "rules": [
                {
                    "name": "model_url",
                    "type": "pattern",
                    "path": "llm.api_base",
                    "pattern": "^https?://",
                }
            ]
        },
    }))
    script_path = bundle_dir / "scripts" / "pre-launch.sh"
    script_path.parent.mkdir()
    script_path.write_text("#!/usr/bin/env bash\n")
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "pre-launch-validation"},
        "llm": {"api_base": "not-a-url"},
    }))
    process = mocker.Mock(pid=4343)
    process.poll.return_value = None

    def fake_popen(_command, **kwargs):
        Path(kwargs["env"]["MN_PRE_LAUNCH_READY_FILE"]).write_text("ready\n")
        return process

    mocker.patch("mn_cli.libs.run_cmds.subprocess.Popen", side_effect=fake_popen)

    with pytest.raises(Exception):
        run_cmds.run_bundle(str(bundle_dir), follow_seconds=0)

    mock_submit.assert_not_called()
    mock_killpg.assert_any_call(4343, 15)

def test_run_executes_post_launch_hook_after_terminal_status(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-post-launch")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"}),
    ])
    mocker.patch("mn_cli.libs.blueprint_resources.process_is_running", return_value=False)
    post_run = mocker.patch(
        "mn_cli.libs.blueprint_resources.subprocess.run",
        return_value=subprocess.CompletedProcess(["bash"], 0),
    )

    bundle_dir = tmp_path / "post_launch_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "worker",
                "config": {"environment": {}},
            }
        ]
    }))
    scripts_dir = bundle_dir / "scripts"
    scripts_dir.mkdir()
    pre_launch_script = scripts_dir / "pre-launch.sh"
    pre_launch_script.write_text("#!/usr/bin/env bash\n")
    post_launch_script = scripts_dir / "post-launch.sh"
    post_launch_script.write_text("#!/usr/bin/env bash\n")

    process = mocker.Mock(pid=4545)
    process.poll.return_value = None

    def fake_popen(_command, **kwargs):
        Path(kwargs["env"]["MN_PRE_LAUNCH_READY_FILE"]).write_text(json.dumps({
            "status": "ready",
            "env": {"RTSP_PORT": "8563"},
        }))
        return process

    mocker.patch("mn_cli.libs.run_cmds.subprocess.Popen", side_effect=fake_popen)

    run_cmds.run_bundle(
        str(bundle_dir),
        follow_seconds=0,
        env_overrides={"MN_RUN_ID": "post-launch-run"},
        submission_metadata={"blueprint_run_id": "post-launch-run"},
    )

    run_dir = tmp_path / "runs" / "post-launch-run"
    hook_info = json.loads((run_dir / "post_launch_hook.json").read_text())
    assert hook_info["script"] == str(post_launch_script.resolve())
    assert hook_info["state_file"] == str(run_dir / "post_launch_state.json")
    post_run.assert_called_once()
    assert post_run.call_args.args[0] == ["bash", str(post_launch_script.resolve())]
    assert post_run.call_args.kwargs["env"]["MN_POST_LAUNCH_REASON"] == "job_completed"
    assert post_run.call_args.kwargs["env"]["RTSP_PORT"] == "8563"

def test_run_records_blueprint_run_id_mapping(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-abc")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"})
    ])

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "worker",
                "config": {"environment": {}},
            }
        ]
    }))
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"run_id": "stale-run"},
        "outputs": {"run_root": str(tmp_path / "blueprints" / "worker" / "runs")},
        "manifest_config_bindings": [
            {
                "config_path": "identity.run_id",
                "manifest_path": "nodes.worker.config.environment.MN_RUN_ID",
            },
            {
                "config_path": "outputs.run_root",
                "manifest_path": "nodes.worker.config.environment.MN_RUNS_ROOT",
            },
        ],
    }))

    run_cmds.run_bundle(
        str(bundle_dir),
        env_overrides={"MN_RUN_ID": "bp-run"},
        submission_metadata={"blueprint_run_id": "bp-run", "blueprint_revision": "rev-1"},
    )

    mapping = json.loads((tmp_path / "runs" / "bp-run" / "job.json").read_text())
    assert mapping["job_id"] == "job-abc"
    assert mapping["blueprint_revision"] == "rev-1"
    assert not (tmp_path / "blueprints" / "worker" / "runs").exists()
    manifest = json.loads(mock_submit.call_args.args[0])
    env = manifest["nodes"][0]["config"]["environment"]
    injected_config = json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])
    assert env["MN_RUN_ID"] == "bp-run"
    assert env["MN_RUNS_ROOT"].startswith(str(tmp_path / "shared" / "submissions" / "bp-run-"))
    assert env["MN_RUNS_ROOT"].endswith("/outputs/runs")
    assert injected_config["identity"]["run_id"] == "bp-run"
    assert injected_config["outputs"]["run_root"] == env["MN_RUNS_ROOT"]

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

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

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

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--follow-seconds", "1.25"])

    assert result.exit_code == 0
    assert "1.25s event tail" in result.stdout
    assert mock_follow.call_args.args[2] == 1.25

@pytest.mark.parametrize("flag", ["-d", "--detached"])
def test_run_detached_starts_without_live_workflow_ui(flag, mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value=f"detached-{flag.strip('-')}")
    mock_submit = mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-detached")
    mock_stream = mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_completed"}),
    ])

    bundle_dir = tmp_path / f"run_bundle_{flag.strip('-')}"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": "detached-workflow",
        "workflow": {
            "workflow_id": "detached-workflow_v1",
            "entrypoint": "step_one",
            "steps": [{"id": "step_one", "label": "Step One", "run": "step_one"}],
        },
        "agents": {
            "schema": "mn.agents.communication_graph/v1",
            "entrypoints": ["worker-one"],
            "nodes": [{"node_id": "worker-one"}],
            "edges": [],
        },
        "runtime": {"bindings": {"step_one": {"worker": {"id": "worker-one"}}}},
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), flag])

    assert result.exit_code == 0
    assert "Detached immediately" in result.stdout
    assert "Run Detached" in result.stdout
    assert "Submitted" in result.stdout
    mock_submit.assert_called_once()
    mock_stream.assert_not_called()

def test_run_error_submitting(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', side_effect=Exception("API failure"))
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    
    assert result.exit_code == 1
    assert "MN_EXECUTION_FAILED" in result.stdout

def test_run_keyboard_interrupt(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', side_effect=KeyboardInterrupt)
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    
    assert result.exit_code == 0
    assert "Detached from workflow UI. Job is still running." in result.stdout

def test_run_not_dir(tmp_path):
    not_a_dir = tmp_path / "not_a_dir"
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(not_a_dir)])
    assert result.exit_code == 1
    assert "is not a directory" in re.sub(r"\s+", " ", result.stdout)

def test_run_no_manifest(tmp_path):
    bundle_dir = tmp_path / "no_manifest"
    bundle_dir.mkdir()
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    assert result.exit_code == 1
    assert "manifest.json not found" in result.stdout
