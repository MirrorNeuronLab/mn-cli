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
from mn_cli.libs import model_cmds, run_cmds, run_manifest
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

def test_run_shows_runtime_web_ui_url_in_submit_and_detach_panels(
    mocker, tmp_path, monkeypatch
):
    web_ui_port = 28910
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("MN_RUN_BACKGROUND_EVENT_RELAY", "0")
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PORT_START", str(web_ui_port))
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PORT_END", str(web_ui_port))
    mocker.patch(
        "mn_sdk.blueprint_support.runtime_web_ui.web_ui_port_available",
        return_value=True,
    )
    mocker.patch(
        'mn_cli.libs.run_cmds._make_blueprint_run_id',
        return_value="web-ui-run",
    )
    mock_submit = mocker.patch(
        'mn_cli.libs.run_cmds.client.submit_job',
        return_value="job-web-ui",
    )
    mocker.patch(
        'mn_cli.libs.run_cmds.client.stream_events',
        return_value=[json.dumps({"type": "job_scheduled"})],
    )
    mocker.patch(
        'mn_cli.libs.run_cmds.client.get_job',
        return_value=json.dumps(
            {
                "summary": {"status": "running"},
                "job": {"status": "running"},
                "recent_events": [],
            }
        ),
    )

    bundle_dir = tmp_path / "web_ui_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "manifest_version": "1.0",
                "type": "service",
                "graph_id": "bp_web_ui_v1",
                "job_name": "bp-web-ui",
                "entrypoints": [],
                "nodes": [],
            }
        )
    )
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "bp_web_ui", "name": "Blueprint Web UI"},
                "outputs": {
                    "adapter": "local_run_store",
                    "run_root": "$MN_HOME/runs",
                    "write_run_store": True,
                },
                "web_ui": {
                    "enabled": True,
                    "output": {
                        "adapter": "gradio",
                        "title": "Blueprint Web UI",
                    },
                },
            }
        )
    )

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--follow-seconds", "0", "--web-ui"])

    assert result.exit_code == 0
    assert "Web UI" in result.stdout
    assert f"http://localhost:{web_ui_port}" in result.stdout
    manifest = json.loads(mock_submit.call_args.args[0])
    assert (
        manifest["metadata"]["blueprint_web_ui_service"]["url"]
        == f"http://localhost:{web_ui_port}"
    )

def test_run_does_not_auto_start_runtime_web_ui(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mock_submit = mocker.patch(
        'mn_cli.libs.run_cmds.client.submit_job',
        return_value="job-no-web-ui",
    )
    mocker.patch(
        'mn_cli.libs.run_cmds.client.stream_events',
        return_value=[json.dumps({"type": "job_completed"})],
    )

    bundle_dir = tmp_path / "no_auto_web_ui_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "manifest_version": "1.0",
        "type": "service",
        "graph_id": "bp_no_auto_web_ui",
        "entrypoints": [],
        "nodes": [],
    }))
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "bp_no_auto_web_ui", "name": "No Auto Web UI"},
        "web_ui": {
            "enabled": True,
            "output": {"adapter": "gradio", "title": "No Auto Web UI"},
        },
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Web UI" not in result.stdout
    manifest = json.loads(mock_submit.call_args.args[0])
    assert "blueprint_web_ui_service" not in manifest.get("metadata", {})
    assert not any(node.get("node_id") == "web_ui_dashboard" for node in manifest.get("nodes", []))

def test_write_local_web_ui_handle_skips_runtime_backed_gradio_script(tmp_path, monkeypatch, mocker):
    explicit_port = 28770
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", "0")
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    script_path = bundle_dir / "payloads" / "web_ui" / "run_dashboard.py"
    script_path.parent.mkdir(parents=True)
    script_path.write_text("print('started')\n")
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "bp-gradio", "name": "Blueprint Gradio"},
                "web_ui": {
                    "enabled": True,
                    "output": {
                        "adapter": "gradio",
                        "title": "Blueprint Gradio",
                        "host": "127.0.0.1",
                        "port": explicit_port,
                        "launch_script": "payloads/web_ui/run_dashboard.py",
                    },
                    "dashboard": {
                        "event_types": ["alert"],
                    },
                },
            }
        )
    )
    popen = mocker.patch("mn_cli.libs.run_cmds.subprocess.Popen")

    run_cmds._write_local_web_ui_handle(bundle_dir, "bp-gradio-run", env_overrides={})

    popen.assert_not_called()
    assert not (tmp_path / "runs" / "bp-gradio-run" / "web_ui_process.json").exists()
    assert not (tmp_path / "runs" / "bp-gradio-run" / "ui.json").exists()

def test_write_local_web_ui_handle_skips_runtime_backed_shared_gradio_module(tmp_path, monkeypatch, mocker):
    explicit_port = 28771
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", "0")
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "bp-shared-gradio", "name": "Shared Gradio"},
                "web_ui": {
                    "enabled": True,
                    "output": {
                        "adapter": "gradio",
                        "title": "Shared Gradio",
                        "host": "127.0.0.1",
                        "port": explicit_port,
                    },
                    "dashboard": {
                        "event_types": ["alert"],
                    },
                },
            }
        )
    )
    popen = mocker.patch("mn_cli.libs.run_cmds.subprocess.Popen")

    run_cmds._write_local_web_ui_handle(bundle_dir, "bp-shared-gradio-run", env_overrides={})

    popen.assert_not_called()
    assert not (tmp_path / "runs" / "bp-shared-gradio-run" / "web_ui_process.json").exists()
    assert not (tmp_path / "runs" / "bp-shared-gradio-run" / "ui.json").exists()

def test_prepare_manifest_injects_runtime_web_ui_service_from_config(tmp_path, monkeypatch, mocker):
    first_port = 28800
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", "0")
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_BIND_HOST", "0.0.0.0")
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PUBLIC_HOST", "localhost")
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PORT_START", str(first_port))
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PORT_END", str(first_port + 2))
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "bp-range", "name": "Range Dashboard"},
                "web_ui": {
                    "enabled": True,
                    "output": {
                        "adapter": "gradio",
                        "title": "Range Dashboard",
                        "constraints": [
                            {
                                "attribute": "node.name",
                                "operator": "==",
                                "value": "mirror_neuron@127.0.0.1",
                            }
                        ],
                    },
                },
            }
        )
    )
    mocker.patch("mn_sdk.blueprint_support.runtime_web_ui.web_ui_port_available", return_value=True)
    manifest = prepare_manifest_for_submission(
        bundle_dir,
        {
            "manifest_version": "1.0",
            "type": "service",
            "graph_id": "bp-range",
            "nodes": [{"node_id": "worker", "agent_type": "executor", "config": {"environment": {}}}],
            "entrypoints": ["worker"],
            "initial_inputs": {"worker": [{}]},
        },
        env_overrides={"MN_RUN_ID": "bp-range-run", "MN_RUNS_ROOT": str(tmp_path / "runs")},
        submission_metadata={"blueprint_id": "bp-range", "blueprint_run_id": "bp-range-run"},
    )

    node = next(node for node in manifest["nodes"] if node["node_id"] == "web_ui_dashboard")
    command = node["config"]["command"]
    assert command[0] == "python3.11"
    assert "--host" in command
    assert command[command.index("--host") + 1] == "0.0.0.0"
    assert "--port" in command
    assert command[command.index("--port") + 1] == str(first_port)
    assert "--base-url" in command
    assert command[command.index("--base-url") + 1] == f"http://localhost:{first_port}"
    env = node["config"]["environment"]
    assert env["MN_BLUEPRINT_WEB_UI_HOST"] == "0.0.0.0"
    assert env["MN_BLUEPRINT_WEB_UI_PORT"] == str(first_port)
    assert env["MN_BLUEPRINT_WEB_UI_BASE_URL"] == f"http://localhost:{first_port}"
    assert "mn_runtime_web_ui/src" in env["PYTHONPATH"].split(os.pathsep)
    assert node["config"]["workdir"] == "/sandbox/job/payloads"
    assert node["config"]["python_environment"]["packages"] == ["gradio>=4.0"]
    assert node["constraints"] == [
        {
            "attribute": "node.name",
            "operator": "==",
            "value": "mirror_neuron@127.0.0.1",
        }
    ]
    assert node["services"][0]["name"] == "blueprint-web-ui"
    assert node["resources"]["ports"][0]["port"] == first_port


def test_runtime_web_ui_maps_host_runs_into_prepublished_docker_core(tmp_path, mocker):
    host_runs = tmp_path / "mn" / "runs"
    mocker.patch("mn_sdk.submission_preparation.running_core_container", return_value="mirror-neuron-core")
    mocker.patch(
        "mn_sdk.submission_preparation.RuntimeConfig.from_env",
        return_value=SimpleNamespace(
            mn_home=tmp_path / "mn",
            runtime_env={
                "MN_HOST_ARTIFACTS_DIR": str(host_runs),
                "MN_CONTAINER_RUNS_ROOT": "/root/.mn/runs",
                "MN_BLUEPRINT_WEB_UI_BIND_HOST": "0.0.0.0",
                "MN_BLUEPRINT_WEB_UI_PUBLIC_HOST": "localhost",
                "MN_BLUEPRINT_WEB_UI_PORT_START": "61000",
                "MN_BLUEPRINT_WEB_UI_PORT_END": "61049",
                "MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE": "prepublished",
            },
        ),
    )

    runtime_runs, overrides = run_manifest._runtime_web_ui_submission_context(
        str(host_runs),
        {"MN_RUN_ID": "run-1"},
    )

    assert runtime_runs == "/root/.mn/runs"
    assert overrides["MN_BLUEPRINT_WEB_UI_BIND_HOST"] == "0.0.0.0"
    assert overrides["MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE"] == "prepublished"

def test_web_ui_port_range_skips_busy_ports(monkeypatch):
    first_port = 28810
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PORT_START", str(first_port))
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PORT_END", str(first_port + 1))
    monkeypatch.setattr(run_cmds, "_web_ui_port_available", lambda host, port: port == first_port + 1)

    assert run_cmds._web_ui_port({}, host="127.0.0.1") == first_port + 1

def test_web_ui_port_uses_ephemeral_fallback_when_default_range_is_busy(monkeypatch):
    monkeypatch.delenv("MN_BLUEPRINT_WEB_UI_PORT_START", raising=False)
    monkeypatch.delenv("MN_BLUEPRINT_WEB_UI_PORT_END", raising=False)
    monkeypatch.setattr(run_cmds, "_web_ui_port_available", lambda host, port: False)
    monkeypatch.setattr(run_cmds, "_ephemeral_web_ui_port", lambda host: 61234)

    assert run_cmds._web_ui_port({}, host="127.0.0.1") == 61234

def test_web_ui_port_range_fails_when_all_ports_are_busy(monkeypatch):
    first_port = 28820
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PORT_START", str(first_port))
    monkeypatch.setenv("MN_BLUEPRINT_WEB_UI_PORT_END", str(first_port + 1))
    monkeypatch.setattr(run_cmds, "_web_ui_port_available", lambda host, port: False)

    with pytest.raises(RuntimeError, match="No available blueprint web UI port"):
        run_cmds._web_ui_port({}, host="127.0.0.1")

def test_web_ui_explicit_port_fails_when_unavailable(monkeypatch):
    monkeypatch.setattr(run_cmds, "_web_ui_port_available", lambda host, port: False)

    with pytest.raises(RuntimeError, match="Blueprint web UI port 28830 is unavailable on 0.0.0.0"):
        run_cmds._web_ui_port({"port": 28830}, host="0.0.0.0")

def test_live_manifest_detection_accepts_scheduler_job_type():
    assert run_cmds._is_live_manifest(
        {"policies": {"scheduler": {"job_type": "service"}}}
    )

def test_live_web_ui_run_starts_background_event_relay(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="live-ui-run")
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-live-ui")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_pending"}),
        json.dumps({"type": "job_running"}),
    ])
    mocker.patch(
        'mn_cli.libs.run_cmds.client.get_job',
        return_value=json.dumps({
            "summary": {"status": "running"},
            "job": {"status": "running"},
            "recent_events": [],
        }),
    )
    mock_process = mocker.Mock(pid=4242)
    mock_popen = mocker.patch('mn_cli.libs.run_cmds.subprocess.Popen', return_value=mock_process)

    bundle_dir = tmp_path / "live_ui_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "type": "service",
        "policies": {"stream_mode": "live"},
        "nodes": [],
    }))
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "live-ui"},
        "budgets": {"max_stream_duration_seconds": 120},
        "web_ui": {
            "enabled": True,
            "output": {
                "adapter": "custom",
                "custom_url": "http://127.0.0.1:9999",
                "refresh_seconds": 0.5,
            },
        },
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--follow-seconds", "0", "--web-ui"])

    assert result.exit_code == 0
    assert "Live event relay" in result.stdout
    mock_popen.assert_called_once()
    command = mock_popen.call_args.args[0]
    assert command[:3] == [sys.executable, "-m", "mn_sdk.blueprint_support.event_relay"]
    assert "--max-seconds" in command
    assert "--shared-storage-json" in command
    pythonpath = mock_popen.call_args.kwargs["env"].get("PYTHONPATH", "")
    assert "mn-skills/blueprint_support_skill/src" not in pythonpath
    relay = json.loads((tmp_path / "runs" / "live-ui-run" / "event_relay.json").read_text())
    assert relay["pid"] == 4242
    storage_path = Path(relay["shared_storage_path"])
    assert storage_path.name == "shared_storage.json"
    assert json.loads(storage_path.read_text())["output_copy_executor"] == "master_host"

def test_job_log_writer_extracts_web_ui_url_once():
    writer = run_cmds.JobLogWriter(f"web-ui-{uuid.uuid4().hex}")
    event = {
        "type": "web_ui_available",
        "payload": {"url": "http://127.0.0.1:7860", "adapter": "gradio"},
    }

    assert writer.record_web_ui_url(event) == "http://127.0.0.1:7860"
    assert writer.web_ui_url == "http://127.0.0.1:7860"
    assert writer.record_web_ui_url(event) is None
