import json
import logging
import os
import re
import subprocess
import sys
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import SimpleNamespace

import pytest
from blueprint_fixtures import write_package_manifest
from mn_sdk import (
    AgentProgress,
    load_model_ownership,
    load_model_remotes,
    upsert_model_remote,
)
from rich.console import Console
from typer.testing import CliRunner

from mn_cli.libs import model_cmds, run_cmds
from mn_cli.libs.run_manifest import prepare_manifest_for_submission
from mn_cli.libs.ui import JobMonitorState, generate_live_layout
from mn_cli.libs.workflow_progress import (
    BlueprintWorkflowProgress,
    _agent_progress_detail,
)
from mn_cli.main import app

runner = CliRunner()


def cli_data(result):
    payload = json.loads(result.stdout)
    assert payload["schema"] == "mn.cli/v1"
    return payload["data"] if payload["ok"] else payload["error"].get("details", {})


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
        lambda **_kwargs: {
            "status": "running",
            "api_base": "http://mn-litellm-proxy:4000/v1",
        },
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
            "workflow_id": "tax_flow_v2",
            "mode": "static_dag",
            "entrypoint": "intake",
            "source": "intake",
            "sink": "report",
            "edges": [
                {
                    "id": "intake_to_income",
                    "from": "intake",
                    "to": "income",
                    "required": True,
                },
                {
                    "id": "intake_to_property",
                    "from": "intake",
                    "to": "property",
                    "required": False,
                },
                {
                    "id": "income_to_report",
                    "from": "income",
                    "to": "report",
                    "required": True,
                },
                {
                    "id": "property_to_report",
                    "from": "property",
                    "to": "report",
                    "required": False,
                },
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
    manifest_data = _workflow_manifest_fixture()
    write_package_manifest(manifest_file, json.dumps(manifest_data))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 0, result.stdout
    assert "Job bundle validation confirmed." in result.stdout
    assert "valid" in result.stdout
    assert "Bundle:" in result.stdout


def test_validate_accepts_current_workflow_manifest(tmp_path):
    bundle_dir = tmp_path / "workflow_bundle"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    manifest["runtime"]["bindings"] = {
        "income": {
            "type": "team",
            "workers": [
                {"id": "income_worker", "kind": "worker"},
                {
                    "id": "income_validator",
                    "kind": "validator",
                    "depends_on": ["income_worker"],
                },
            ],
        }
    }
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 0
    assert "4" in result.stdout


def test_validate_accepts_source_manifest_after_expansion(monkeypatch, tmp_path):
    bundle_dir = tmp_path / "source_workflow_bundle"
    bundle_dir.mkdir()
    agent_root = tmp_path / "mn-agents"
    agent_dir = (
        agent_root
        / "worker_python_host_agent"
        / "src"
        / "mn_worker_python_host_agent"
        / "resources"
    )
    docker_agent_dir = (
        agent_root
        / "worker_python_docker_agent"
        / "src"
        / "mn_worker_python_docker_agent"
        / "resources"
    )
    agent_dir.mkdir(parents=True)
    docker_agent_dir.mkdir(parents=True)
    (agent_root / "index.json").write_text(
        json.dumps(
            {
                "schema_version": "mn-agents.index.v2",
                "agents": [
                    {
                        "agent_id": "mn-agents.worker.python_host",
                        "template_id": "mn-agents.worker.python_host",
                        "version": 1,
                        "distribution": "mn-worker-python-host-agent",
                        "module": "mn_worker_python_host_agent",
                        "package_kind": "runtime_node",
                        "resource_path": "worker_python_host_agent/src/mn_worker_python_host_agent/resources/agent.json",
                        "template_category": "data",
                    },
                    {
                        "agent_id": "mn-agents.worker.python_docker",
                        "template_id": "mn-agents.worker.python_docker",
                        "version": 1,
                        "distribution": "mn-worker-python-docker-agent",
                        "module": "mn_worker_python_docker_agent",
                        "package_kind": "runtime_node",
                        "resource_path": "worker_python_docker_agent/src/mn_worker_python_docker_agent/resources/agent.json",
                        "template_category": "data",
                    },
                ],
            }
        )
    )
    (docker_agent_dir / "agent.json").write_text(
        json.dumps(
            {
                "schema_version": "mn.agent.package.v1",
                "agent_id": "mn-agents.worker.python_docker",
                "template_id": "mn-agents.worker.python_docker",
                "version": 1,
                "package_kind": "runtime_node",
                "defaults": {
                    "type": "map",
                    "agent_type": "executor",
                    "runner_module": "MirrorNeuron.Runner.DockerWorker",
                },
                "stereotypes": {
                    "blueprint_docker_worker": {
                        "with": {
                            "command": ["python3", "-m", "mn_sdk.step_runtime"],
                            "docker_worker_image": "docker_worker",
                            "upload_path": "runtime",
                        }
                    }
                },
                "inputs": {"required": []},
            }
        )
    )
    (agent_dir / "agent.json").write_text(
        json.dumps(
            {
                "schema_version": "mn.agent.package.v1",
                "agent_id": "mn-agents.worker.python_host",
                "template_id": "mn-agents.worker.python_host",
                "version": 1,
                "package_kind": "runtime_node",
                "defaults": {
                    "type": "map",
                    "agent_type": "executor",
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                },
                "inputs": {"required": []},
            }
        )
    )
    monkeypatch.setenv("MN_AGENTS_ROOT", str(agent_root))
    manifest = {
        "apiVersion": "mn.workflow/v1",
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
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 0, result.stdout
    assert "Job bundle validation confirmed." in result.stdout
    assert "Workflow ID" in result.stdout


def test_validate_is_static_and_does_not_probe_runtime_readiness(mocker, tmp_path):
    bundle_dir = tmp_path / "lazy_model_bundle"
    bundle_dir.mkdir()
    write_package_manifest(
        bundle_dir / "manifest.json", json.dumps(_workflow_manifest_fixture())
    )
    defer_models = mocker.patch(
        "mn_cli.libs.run_cmds.handlers.validate._defer_runtime_models_for_run_or_exit",
        side_effect=AssertionError("static validation must not prepare models"),
    )
    validate_models = mocker.patch(
        "mn_cli.libs.run_cmds.handlers.validate._validate_manifest_models_or_exit",
        side_effect=AssertionError("static validation must not probe models"),
    )
    validate_services = mocker.patch(
        "mn_cli.libs.run_cmds.handlers.validate._validate_manifest_services_or_exit",
        side_effect=AssertionError("static validation must not probe services"),
    )
    validate_hardware = mocker.patch(
        "mn_cli.libs.run_cmds.handlers.validate._validate_manifest_hardware_or_exit",
        side_effect=AssertionError("static validation must not probe hardware"),
    )

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 0
    defer_models.assert_not_called()
    validate_models.assert_not_called()
    validate_services.assert_not_called()
    validate_hardware.assert_not_called()


def test_validate_rejects_workflow_manifest_cycles(tmp_path):
    bundle_dir = tmp_path / "workflow_cycle"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    manifest["id"] = "cyclic_flow"
    manifest["workflow"]["workflow_id"] = "cyclic_flow_v2"
    manifest["workflow"]["entrypoint"] = "a"
    manifest["workflow"]["source"] = "a"
    manifest["workflow"]["sink"] = "c"
    manifest["workflow"]["steps"] = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    manifest["workflow"]["edges"] = [
        {"id": "a_to_b", "from": "a", "to": "b"},
        {"id": "b_to_c", "from": "b", "to": "c"},
        {"id": "c_to_b", "from": "c", "to": "b"},
    ]
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    report = cli_data(result)
    assert any(
        issue["code"] == "blueprint.cyclic_workflow" for issue in report["issues"]
    )


def test_validate_rejects_workflow_manifest_root_graph_id(tmp_path):
    bundle_dir = tmp_path / "workflow_root_graph_id"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    manifest["graph_id"] = "tax_flow_v1"
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    report = cli_data(result)
    assert any(issue["location"]["path"] == "graph_id" for issue in report["issues"])


def test_validate_rejects_workflow_manifest_missing_workflow_id(tmp_path):
    bundle_dir = tmp_path / "workflow_missing_id"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    del manifest["workflow"]["workflow_id"]
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    report = cli_data(result)
    assert any("workflow_id" in issue["message"] for issue in report["issues"])


def test_validate_rejects_old_flow_workflow_manifest(tmp_path):
    bundle_dir = tmp_path / "workflow_old_flow"
    bundle_dir.mkdir()
    manifest = _workflow_manifest_fixture()
    manifest["flow"] = {"steps": manifest["workflow"]["steps"]}
    del manifest["workflow"]
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    report = cli_data(result)
    assert any(
        issue["location"]["path"] in {"flow", "manifest"} for issue in report["issues"]
    )


def test_validate_not_directory(tmp_path):
    not_a_dir = tmp_path / "not_a_dir"
    result = runner.invoke(app, ["blueprint", "validate", str(not_a_dir)])
    assert result.exit_code == 2
    assert "is not a directory" in re.sub(r"\s+", " ", result.stderr)


def test_validate_no_manifest(tmp_path):
    bundle_dir = tmp_path / "no_manifest"
    bundle_dir.mkdir()
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 2
    assert "manifest.json not found in" in result.stderr


def test_validate_bad_json(tmp_path):
    bundle_dir = tmp_path / "bad_json"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text("{bad_json: 1}")
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 2
    assert "is not valid JSON" in result.stderr


def test_validate_rejects_unversioned_manifest(tmp_path):
    bundle_dir = tmp_path / "missing_keys"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"manifest_version": "1.0"}')
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 2
    assert "apiVersion must be mn.workflow/v1" in result.stderr


def test_validate_nodes_not_list(tmp_path):
    bundle_dir = tmp_path / "nodes_not_list"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_data = _workflow_manifest_fixture()
    manifest_data["agents"]["nodes"] = "not_a_list"
    write_package_manifest(manifest_file, json.dumps(manifest_data))
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 2
    assert "/agents/nodes" in result.stdout


def test_validate_rejects_bad_resource_specs(tmp_path):
    bundle_dir = tmp_path / "bad_resources"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest = _workflow_manifest_fixture()
    manifest["agents"]["nodes"] = [
        {
            "node_id": "worker",
            "resources": {
                "ports": [{"label": "api", "port": 70000}],
                "volumes": [
                    {"name": "models", "source": "relative", "target": "models"}
                ],
            },
        }
    ]
    write_package_manifest(manifest_file, json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    report = cli_data(result)
    codes = {issue["code"] for issue in report["issues"]}
    assert "manifest.resources.port_number" in codes
    assert "manifest.resources.volume_source" in codes


def test_validate_accepts_host_local_python_environment(tmp_path):
    bundle_dir = tmp_path / "python_env_bundle"
    requirements = bundle_dir / "payloads" / "worker" / "requirements.txt"
    requirements.parent.mkdir(parents=True)
    requirements.write_text("opencv-python-headless>=4.10,<5\n")
    manifest = _workflow_manifest_fixture()
    manifest["agents"]["nodes"] = [
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
    ]
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Job bundle validation confirmed." in result.stdout
    assert "valid" in result.stdout


def test_validate_rejects_invalid_python_environment(tmp_path):
    bundle_dir = tmp_path / "bad_python_env_bundle"
    (bundle_dir / "payloads").mkdir(parents=True)
    manifest = _workflow_manifest_fixture()
    manifest["agents"]["nodes"] = [
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
    ]
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    messages = [issue["message"] for issue in cli_data(result)["issues"]]
    assert any(
        "python_environment.requirements must be a relative path inside payloads"
        in message
        for message in messages
    )
    assert any(
        "python_environment.packages must be a list of non-empty strings" in message
        for message in messages
    )


def test_validate_rejects_missing_explicit_skill_runtime_dockerfile(tmp_path):
    bundle_dir = tmp_path / "bad_skill_runtime_bundle"
    (bundle_dir / "payloads").mkdir(parents=True)
    manifest = _workflow_manifest_fixture()
    manifest["metadata"] = {
        "mn_skill_runtime": {
            "enabled": True,
            "driver": "docker_worker",
            "build_context": "worker/docker_worker",
            "generated": False,
        }
    }
    manifest["agents"]["nodes"] = [
        {
            "node_id": "worker",
            "config": {
                "runner_module": "MirrorNeuron.Runner.DockerWorker",
                "docker_worker_image": "worker/docker_worker",
                "image": "example/worker:local",
            },
        }
    ]
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    assert any(
        "mn_skill_runtime Dockerfile not found" in issue["message"]
        for issue in cli_data(result)["issues"]
    )


def test_validate_runs_manifest_input_validation(tmp_path):
    bundle_dir = tmp_path / "validated_inputs"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir(exist_ok=True)
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"video_source": {"uri": "ftp://camera.local/live"}})
    )
    manifest = _workflow_manifest_fixture()
    manifest["input_validation"] = {
        "rules": [
            {
                "name": "camera_url",
                "type": "pattern",
                "path": "video_source.uri",
                "pattern": "^https?://",
            }
        ]
    }
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 2
    assert "Input validation failed" in result.stdout
    assert "camera_url" in result.stdout
    assert "Field" in result.stdout
    assert "Fix" in result.stdout


def test_validate_reports_missing_generic_required_input(tmp_path):
    bundle_dir = tmp_path / "missing_required_input"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir(exist_ok=True)
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"inputs": {"payload": {"input_folder": ""}}})
    )
    manifest = _workflow_manifest_fixture()
    manifest["input_validation"] = {
        "required": ["input_folder"],
        "rules": [],
    }
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    report = cli_data(result)
    issue = report["issues"][0]
    assert issue["code"] == "config.required"
    assert issue["message"] == "Required input 'input_folder' is missing."
    assert issue["location"]["path"] == "inputs.payload.input_folder"
    assert "--set inputs.payload.input_folder" in issue["help"]


def test_validate_does_not_run_required_service_probes(tmp_path):
    bundle_dir = tmp_path / "service_validated_inputs"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir(exist_ok=True)
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"video_source": {"uri": "ftp://camera.local/live"}})
    )
    manifest = _workflow_manifest_fixture()
    manifest["required_services"] = [
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
    ]
    manifest["input_validation"] = {
        "rules": [
            {
                "name": "camera_url",
                "type": "pattern",
                "path": "video_source.uri",
                "pattern": "^https?://",
            }
        ]
    }
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])

    assert result.exit_code == 2
    assert "Service validation failed" not in result.stdout
    assert "Input validation failed" in result.stdout


def test_validate_outputs_json_report(tmp_path):
    bundle_dir = tmp_path / "validated_inputs"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir(exist_ok=True)
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"video_source": {"uri": "ftp://camera.local/live"}})
    )
    manifest = _workflow_manifest_fixture()
    manifest["input_validation"] = {
        "rules": [
            {
                "name": "camera_url",
                "type": "pattern",
                "path": "video_source.uri",
                "pattern": "^https?://",
                "help": "Use an http:// or https:// URL.",
            }
        ]
    }
    write_package_manifest(bundle_dir / "manifest.json", json.dumps(manifest))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir), "--json"])

    assert result.exit_code == 2
    report = cli_data(result)
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
    mocker.patch("builtins.open", side_effect=Exception("Read error"))

    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
    assert result.exit_code == 1
    assert "MN_EXECUTION_FAILED" in result.stderr
