import json
import logging
import re
import sys
import uuid
from logging.handlers import RotatingFileHandler
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
    assert "is not a directory" in re.sub(r"\s+", " ", result.stdout)

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


def test_validate_accepts_host_local_python_environment(tmp_path):
    bundle_dir = tmp_path / "python_env_bundle"
    requirements = bundle_dir / "payloads" / "worker" / "requirements.txt"
    requirements.parent.mkdir(parents=True)
    requirements.write_text("opencv-python-headless>=4.10,<5\n")
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["worker"],
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "python_environment": {
                        "requirements": "worker/requirements.txt",
                        "packages": ["numpy>=1.26"],
                    },
                },
            }
        ],
    }))

    result = runner.invoke(app, ["validate", str(bundle_dir)])

    assert result.exit_code == 0
    assert "is valid" in result.stdout


def test_validate_rejects_invalid_python_environment(tmp_path):
    bundle_dir = tmp_path / "bad_python_env_bundle"
    (bundle_dir / "payloads").mkdir(parents=True)
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["worker"],
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "python_environment": {
                        "requirements": "../requirements.txt",
                        "packages": ["numpy>=1.26", ""],
                    },
                },
            }
        ],
    }))

    result = runner.invoke(app, ["validate", str(bundle_dir)])

    assert result.exit_code == 1
    normalized = re.sub(r"\s+", " ", result.stdout)
    assert "python_environment.requirements must be a relative path inside payloads" in normalized
    assert "python_environment.packages must be a list of non-empty strings" in result.stdout


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
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    
    assert result.exit_code == 0
    assert "Job submitted successfully" in result.stdout
    assert "run-bundle-auto" in result.stdout
    assert "Type" in result.stdout
    assert "Batch" in result.stdout
    assert "Job Status: Success" in result.stdout
    mapping = json.loads((tmp_path / "runs" / "run-bundle-auto" / "job.json").read_text())
    assert mapping["job_id"] == "job-123"
    mock_submit.assert_called_once()
    mock_stream.assert_called_once_with("job-123")


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

    result = runner.invoke(app, ["run", str(bundle_dir)])

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

    result = runner.invoke(app, ["run", str(bundle_dir)])

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
        "outputs": {"adapter": "local_run_store", "run_root": "~/.mn/runs", "write_run_store": True},
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

    result = runner.invoke(app, ["run", str(bundle_dir)])

    assert result.exit_code == 0
    assert "bp-1-auto-run" in result.stdout
    mapping = json.loads((tmp_path / "runs" / "bp-1-auto-run" / "job.json").read_text())
    assert mapping["job_id"] == "job-auto"
    manifest = json.loads(mock_submit.call_args.args[0])
    env = manifest["nodes"][0]["config"]["environment"]
    injected_config = json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])
    assert env["MN_RUN_ID"] == "bp-1-auto-run"
    assert env["MN_RUNS_ROOT"] == str(tmp_path / "runs")
    assert injected_config["identity"]["run_id"] == "bp-1-auto-run"
    assert injected_config["outputs"]["run_root"] == str(tmp_path / "runs")
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


def test_write_local_web_ui_handle_launches_blueprint_owned_web_ui_script(tmp_path, monkeypatch, mocker):
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
                        "port": 7870,
                        "launch_script": "payloads/web_ui/run_dashboard.py",
                    },
                    "dashboard": {
                        "event_types": ["alert"],
                    },
                },
            }
        )
    )
    process = mocker.Mock()
    process.pid = 4242
    process.poll.return_value = None
    popen = mocker.patch("mn_cli.libs.run_cmds.subprocess.Popen", return_value=process)

    run_cmds._write_local_web_ui_handle(bundle_dir, "bp-gradio-run", env_overrides={})

    process_info = json.loads((tmp_path / "runs" / "bp-gradio-run" / "web_ui_process.json").read_text())
    assert process_info["pid"] == 4242
    assert process_info["url"] == "http://localhost:7870"
    command = popen.call_args.args[0]
    assert command[1] == str(script_path)
    assert "--run-id" in command
    assert "bp-gradio-run" in command
    assert "--base-url" in command
    assert "http://localhost:7870" in command
    assert popen.call_args.kwargs["env"]["MN_BLUEPRINT_WEB_UI_PORT"] == "7870"
    assert not (tmp_path / "runs" / "bp-gradio-run" / "ui.json").exists()


def test_write_local_web_ui_handle_launches_shared_gradio_support_module(tmp_path, monkeypatch, mocker):
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
                        "port": 7871,
                    },
                    "dashboard": {
                        "event_types": ["alert"],
                    },
                },
            }
        )
    )
    process = mocker.Mock()
    process.pid = 4243
    process.poll.return_value = None
    popen = mocker.patch("mn_cli.libs.run_cmds.subprocess.Popen", return_value=process)

    run_cmds._write_local_web_ui_handle(bundle_dir, "bp-shared-gradio-run", env_overrides={})

    process_info = json.loads((tmp_path / "runs" / "bp-shared-gradio-run" / "web_ui_process.json").read_text())
    assert process_info["pid"] == 4243
    assert process_info["module"] == "mn_blueprint_support.gradio_dashboard"
    assert process_info["url"] == "http://localhost:7871"
    command = popen.call_args.args[0]
    assert command[:3] == [sys.executable, "-m", "mn_blueprint_support.gradio_dashboard"]
    assert "--run-id" in command
    assert "bp-shared-gradio-run" in command
    assert "--base-url" in command
    assert "http://localhost:7871" in command
    assert popen.call_args.kwargs["env"]["MN_BLUEPRINT_WEB_UI_PORT"] == "7871"
    assert not (tmp_path / "runs" / "bp-shared-gradio-run" / "ui.json").exists()


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
    assert env["MN_RUNS_ROOT"] == str(tmp_path / "runs")
    assert injected_config["identity"]["run_id"] == "bp-run"
    assert injected_config["outputs"]["run_root"] == str(tmp_path / "runs")

def test_run_displays_live_job_type_and_follow_status(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-live")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_pending"}),
        json.dumps({"type": "job_scheduled"}),
    ])
    mocker.patch(
        'mn_cli.libs.run_cmds.client.get_job',
        return_value=json.dumps({
            "summary": {"status": "running"},
            "job": {"status": "running"},
            "recent_events": [],
        }),
    )

    bundle_dir = tmp_path / "live_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "daemon": True,
        "policies": {"stream_mode": "live"},
        "nodes": [],
    }))

    result = runner.invoke(app, ["run", str(bundle_dir), "--follow-seconds", "0"])

    assert result.exit_code == 0
    assert "Live daemon" in result.stdout
    assert "Starting: agents scheduled" in result.stdout
    assert "Following: status running" in result.stdout
    assert "75%" not in result.stdout

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

    result = runner.invoke(app, ["run", str(bundle_dir)])

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

    result = runner.invoke(app, ["run", str(bundle_dir), "--follow-seconds", "1.25"])

    assert result.exit_code == 0
    assert "1.25s event tail" in result.stdout
    assert mock_follow.call_args.args[2] == 1.25

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

def test_job_log_writer_extracts_web_ui_url_once():
    writer = run_cmds.JobLogWriter(f"web-ui-{uuid.uuid4().hex}")
    event = {
        "type": "web_ui_available",
        "payload": {"url": "http://127.0.0.1:7860", "adapter": "gradio"},
    }

    assert writer.record_web_ui_url(event) == "http://127.0.0.1:7860"
    assert writer.record_web_ui_url(event) is None

def test_run_error_submitting(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', side_effect=Exception("API failure"))
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    
    assert result.exit_code == 1
    assert "Error running bundle: API failure" in result.stdout

def test_run_keyboard_interrupt(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', side_effect=KeyboardInterrupt)
    
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
    assert "is not a directory" in re.sub(r"\s+", " ", result.stdout)

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
    mocker.patch('mn_cli.libs.run_cmds.client.get_job', return_value=json.dumps({
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
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
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
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=events)
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
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', side_effect=KeyboardInterrupt)
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["run", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Detached from log stream" in result.stdout
