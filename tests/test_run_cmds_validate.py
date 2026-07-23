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

def _workflow_manifest_fixture():
    return {
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": "tax_flow",
        "name": "Tax Flow",
        "manifest_version": "1.0",
        "job_name": "tax-flow",
        "contract": {
            "inputs": {},
            "outputs": {"primary": {"path": "final_artifact.json"}},
        },
        "workflow": {
            "schema": "mn.workflow.problem_graph/v1",
            "workflow_id": "tax_flow_v1",
            "mode": "static_dag",
            "entrypoint": "intake",
            "source": "intake",
            "sink": "report",
            "edges": [
                {"id": "intake_to_income", "from": "intake", "to": "income", "required": True},
                {"id": "intake_to_property", "from": "intake", "to": "property", "required": False},
                {"id": "income_to_report", "from": "income", "to": "report", "required": True},
                {"id": "property_to_report", "from": "property", "to": "report", "required": False},
            ],
            "steps": [
                {"id": "intake", "label": "Intake"},
                {"id": "income", "label": "Income"},
                {"id": "property", "label": "Property"},
                {"id": "report", "label": "Report"},
            ],
        },
        "agents": {
            "schema": "mn.agents.communication_graph/v1",
            "entrypoints": ["worker"],
            "nodes": [{"node_id": "worker"}],
            "edges": [],
        },
        "runtime": {"bindings": {}},
    }

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
    
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 0, result.stdout
    assert "Job bundle validation confirmed." in result.stdout
    assert "valid" in result.stdout
    assert "Bundle:" in result.stdout

def test_validate_accepts_workflow_manifest_without_legacy_nodes(tmp_path):
    bundle_dir = tmp_path / "workflow_bundle"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    manifest["runtime"]["bindings"] = {
        "income": {
            "type": "team",
            "workers": [
                {"id": "income_worker", "kind": "worker"},
                {"id": "income_validator", "kind": "validator", "depends_on": ["income_worker"]},
            ],
        }
    }
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 0
    assert "4" in result.stdout

def test_validate_accepts_source_manifest_after_expansion(tmp_path):
    bundle_dir = tmp_path / "source_workflow_bundle"
    bundle_dir.mkdir()
    manifest = {
        "apiVersion": "mn.workflow.source/v2",
        "kind": "WorkflowSource",
        "identity": {"id": "source_flow", "name": "Source Flow"},
        "defaults": {"worker": {"with": {"image": "source-flow:test"}}},
        "workflow": {
            "steps": [
                {
                    "id": "prepare",
                    "needs": [],
                    "run": {"handler": "source_flow.steps.prepare"},
                },
                {
                    "id": "publish",
                    "needs": ["prepare"],
                    "run": {"handler": "source_flow.steps.publish"},
                },
            ]
        },
    }
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 0, result.stdout
    assert "Job bundle validation confirmed." in result.stdout
    assert "source_flow_v1" in result.stdout

def test_validate_records_first_use_models_as_deferred(mocker, tmp_path):
    bundle_dir = tmp_path / "lazy_model_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps(_workflow_manifest_fixture()))
    deferred = {
        "ok": True,
        "deferred": True,
        "models": [
            {
                "id": "default",
                "model": "default",
                "runtime_model": "docker.io/ai/nemotron3:latest",
                "status": "deferred_runtime_install",
            }
        ],
    }
    defer_models = mocker.patch(
        "mn_cli.libs.run_cmds._defer_runtime_models_for_run_or_exit",
        return_value=deferred,
    )
    validate_models = mocker.patch(
        "mn_cli.libs.run_cmds._validate_manifest_models_or_exit",
        return_value={"ok": True, "results": []},
    )

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 0
    defer_models.assert_called_once()
    assert defer_models.call_args.kwargs["quiet"] is True
    assert validate_models.call_args.kwargs["model_install_summary"] is deferred

def test_validate_rejects_workflow_manifest_cycles(tmp_path):
    bundle_dir = tmp_path / "workflow_cycle"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    manifest["id"] = "cyclic_flow"
    manifest["workflow"]["workflow_id"] = "cyclic_flow_v1"
    manifest["workflow"]["entrypoint"] = "a"
    manifest["workflow"]["source"] = "a"
    manifest["workflow"]["sink"] = "c"
    manifest["workflow"]["steps"] = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    manifest["workflow"]["edges"] = [
        {"id": "a_to_b", "from": "a", "to": "b"},
        {"id": "b_to_c", "from": "b", "to": "c"},
        {"id": "c_to_b", "from": "c", "to": "b"},
    ]
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--output", "json"])

    assert result.exit_code == 1
    report = json.loads(result.stdout)
    assert any("acyclic" in issue["message"] for issue in report["issues"])

def test_validate_rejects_workflow_manifest_root_graph_id(tmp_path):
    bundle_dir = tmp_path / "workflow_root_graph_id"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    manifest["graph_id"] = "tax_flow_v1"
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--output", "json"])

    assert result.exit_code == 1
    report = json.loads(result.stdout)
    assert any(issue["location"]["path"] == "graph_id" for issue in report["issues"])

def test_validate_rejects_workflow_manifest_missing_workflow_id(tmp_path):
    bundle_dir = tmp_path / "workflow_missing_id"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    del manifest["workflow"]["workflow_id"]
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--output", "json"])

    assert result.exit_code == 1
    report = json.loads(result.stdout)
    assert any("workflow_id" in issue["message"] for issue in report["issues"])

def test_validate_rejects_old_flow_workflow_manifest(tmp_path):
    bundle_dir = tmp_path / "workflow_old_flow"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    manifest["flow"] = {"steps": manifest["workflow"]["steps"]}
    del manifest["workflow"]
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--output", "json"])

    assert result.exit_code == 1
    report = json.loads(result.stdout)
    assert any(issue["location"]["path"] in {"flow", "manifest"} for issue in report["issues"])

def test_validate_not_directory(tmp_path):
    not_a_dir = tmp_path / "not_a_dir"
    result = runner.invoke(app, ["blueprint", "validate", str(not_a_dir)])
    assert result.exit_code == 1
    assert "is not a directory" in re.sub(r"\s+", " ", result.stdout)

def test_validate_no_manifest(tmp_path):
    bundle_dir = tmp_path / "no_manifest"
    bundle_dir.mkdir()
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "manifest.json not found in" in result.stdout

def test_validate_bad_json(tmp_path):
    bundle_dir = tmp_path / "bad_json"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text("{bad_json: 1}")
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "is not valid JSON" in result.stdout

def test_validate_missing_keys(tmp_path):
    bundle_dir = tmp_path / "missing_keys"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"manifest_version": "1.0"}')
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
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
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "'nodes' must be a list" in result.stdout

def test_validate_rejects_bad_resource_specs(tmp_path):
    bundle_dir = tmp_path / "bad_resources"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text(json.dumps({
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["worker"],
        "nodes": [
            {
                "node_id": "worker",
                "resources": {
                    "ports": [{"label": "api", "port": 70000}],
                    "volumes": [{"name": "models", "source": "relative", "target": "models"}],
                },
            }
        ],
    }))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--output", "json"])

    assert result.exit_code == 1
    report = json.loads(result.stdout)
    codes = {issue["code"] for issue in report["issues"]}
    assert "manifest.resources.port_number" in codes
    assert "manifest.resources.volume_source" in codes

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

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Job bundle validation confirmed." in result.stdout
    assert "valid" in result.stdout

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

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 1
    normalized = re.sub(r"\s+", " ", result.stdout)
    assert "python_environment.requirements must be a relative path inside payloads" in normalized
    assert "python_environment.packages must be a list of non-empty strings" in result.stdout

def test_validate_rejects_missing_explicit_skill_runtime_dockerfile(tmp_path):
    bundle_dir = tmp_path / "bad_skill_runtime_bundle"
    (bundle_dir / "payloads").mkdir(parents=True)
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["worker"],
        "metadata": {
            "mn_skill_runtime": {
                "enabled": True,
                "driver": "docker_worker",
                "build_context": "worker/docker_worker",
                "generated": False,
            }
        },
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.DockerWorker",
                    "docker_worker_image": "worker/docker_worker",
                    "image": "example/worker:local",
                },
            }
        ],
    }))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 1
    assert "mn_skill_runtime Dockerfile not found" in result.stdout

def test_validate_runs_manifest_input_validation(tmp_path):
    bundle_dir = tmp_path / "validated_inputs"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    (bundle_dir / "config" / "default.json").write_text(json.dumps({
        "video_source": {"uri": "ftp://camera.local/live"}
    }))
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["worker"],
        "nodes": [{"node_id": "worker"}],
        "input_validation": {
            "rules": [
                {
                    "name": "camera_url",
                    "type": "pattern",
                    "path": "video_source.uri",
                    "pattern": "^https?://",
                }
            ]
        },
    }))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 1
    assert "Input validation failed" in result.stdout
    assert "camera_url" in result.stdout
    assert "Field" in result.stdout
    assert "Fix" in result.stdout

def test_validate_runs_required_service_checks_before_input_validation(tmp_path):
    bundle_dir = tmp_path / "service_validated_inputs"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    (bundle_dir / "config" / "default.json").write_text(json.dumps({
        "video_source": {"uri": "ftp://camera.local/live"}
    }))
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["worker"],
        "nodes": [{"node_id": "worker"}],
        "required_services": [
            {
                "name": "external-probe",
                "origin": "external",
                "checks": [
                    {
                        "name": "probe",
                        "type": "script",
                        "command": [sys.executable, "-c", "import sys; sys.exit(2)"],
                    }
                ],
            }
        ],
        "input_validation": {
            "rules": [
                {
                    "name": "camera_url",
                    "type": "pattern",
                    "path": "video_source.uri",
                    "pattern": "^https?://",
                }
            ]
        },
    }))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 1
    assert "Service validation failed" in result.stdout
    assert "Input validation failed" not in result.stdout

def test_validate_outputs_json_report(tmp_path):
    bundle_dir = tmp_path / "validated_inputs"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    (bundle_dir / "config" / "default.json").write_text(json.dumps({
        "video_source": {"uri": "ftp://camera.local/live"}
    }))
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "manifest_version": "1.0",
        "graph_id": "test_graph",
        "job_name": "test_job",
        "entrypoints": ["worker"],
        "nodes": [{"node_id": "worker"}],
        "input_validation": {
            "rules": [
                {
                    "name": "camera_url",
                    "type": "pattern",
                    "path": "video_source.uri",
                    "pattern": "^https?://",
                    "help": "Use an http:// or https:// URL.",
                }
            ]
        },
    }))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--output", "json"])

    assert result.exit_code == 1
    report = json.loads(result.stdout)
    assert report["ok"] is False
    assert report["issues"][0]["location"]["path"] == "video_source.uri"
    assert report["issues"][0]["rule"]["name"] == "camera_url"
    assert report["issues"][0]["help"] == "Use an http:// or https:// URL."

def test_validate_unexpected_error(mocker, tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.touch()
    
    # Mock open to raise Exception
    mocker.patch('builtins.open', side_effect=Exception("Read error"))
    
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "MN_EXECUTION_FAILED" in result.stdout
