import json
import logging
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path
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


def test_manifest_for_model_validation_filters_dmr_models_for_fake_llm():
    manifest = {
        "runtime": {
            "models": {
                "primary": {"model": "default"},
                "secondary": {"provider": "docker_model_runner", "model": "gemma4:e2b"},
                "external": {"provider": "nvidia_service", "model": "service-model"},
            }
        }
    }

    filtered = run_cmds._manifest_for_model_validation(manifest, {"llm": {"mode": "fake"}})

    assert set(filtered["runtime"]["models"]) == {"external"}
    assert set(manifest["runtime"]["models"]) == {"primary", "secondary", "external"}


def test_prefer_default_single_node_agent_placement_uses_local_runtime_node(monkeypatch):
    monkeypatch.delenv("MN_BLUEPRINT_SINGLE_NODE_AGENTS", raising=False)
    monkeypatch.setattr(
        run_cmds.client,
        "get_system_summary",
        lambda: json.dumps(
            {
                "nodes": [
                    {"name": "mirror_neuron@local", "self?": True},
                    {"name": "mirror_neuron@spark", "self?": False},
                ]
            }
        ),
    )
    manifest = {
        "nodes": [
            {"node_id": "default_worker", "config": {}},
            {
                "node_id": "remote_worker",
                "policies": {"scheduler": {"preferred_node": "mirror_neuron@spark"}},
                "config": {},
            },
            {
                "node_id": "constrained_worker",
                "constraints": [{"attribute": "node.name", "operator": "==", "value": "mirror_neuron@spark"}],
                "config": {},
            },
        ]
    }

    run_cmds._prefer_default_single_node_agent_placement(manifest)

    assert manifest["nodes"][0]["policies"]["scheduler"]["preferred_node"] == "mirror_neuron@local"
    assert manifest["nodes"][1]["policies"]["scheduler"]["preferred_node"] == "mirror_neuron@spark"
    assert "policies" not in manifest["nodes"][2]


def test_prefer_default_single_node_agent_placement_can_be_disabled(monkeypatch):
    monkeypatch.setenv("MN_BLUEPRINT_SINGLE_NODE_AGENTS", "0")
    monkeypatch.setattr(
        run_cmds.client,
        "get_system_summary",
        lambda: json.dumps({"nodes": [{"name": "mirror_neuron@local", "self?": True}]}),
    )
    manifest = {"nodes": [{"node_id": "worker", "config": {}}]}

    run_cmds._prefer_default_single_node_agent_placement(manifest)

    assert "policies" not in manifest["nodes"][0]


def test_cli_agent_progress_detail_marks_estimates_and_token_budgets():
    estimated = AgentProgress(
        id="worker",
        status="running",
        progress=0.35,
        progress_source="milestone",
        token_budget=12000,
        tools=4,
    )
    explicit = AgentProgress(
        id="worker",
        status="running",
        progress=0.42,
        progress_source="explicit",
        tokens_used=1300,
        token_budget=12000,
    )

    assert "35% est." in _agent_progress_detail(estimated)
    assert "12k tok budget" in _agent_progress_detail(estimated)
    assert "42% est." not in _agent_progress_detail(explicit)
    assert "1.3k/12k tok" in _agent_progress_detail(explicit)


def test_prepare_runtime_models_installs_missing_model_for_run(
    mocker,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    bundle_dir = tmp_path / "vc_assistant"
    bundle_dir.mkdir()
    manifest = {
        "metadata": {"blueprint_id": "vc_assistant", "blueprint_revision": "rev-1"},
        "runtime": {
            "models": {
                "primary": {
                    "provider": "docker_model_runner",
                    "runtime_model": "gemma4:e2b",
                    "backend": "llama.cpp",
                }
            }
        },
    }
    catalog = {
        "gemma4:e2b": {
            "id": "gemma4:e2b",
            "model": "ai/gemma4:E2B",
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
        }
    }
    mocker.patch("mn_cli.libs.run_cmds.load_model_catalog", return_value=catalog)
    mocker.patch("mn_cli.libs.run_cmds.model_installed", return_value=False)
    install_model = mocker.patch(
        "mn_cli.libs.run_cmds.install_model_entry",
        return_value={
            "entry": catalog["gemma4:e2b"],
            "docker_model": "ai/gemma4:E2B",
            "compatibility": {"backend": "llama.cpp"},
        },
    )

    summary = run_cmds._prepare_runtime_models_for_run_or_exit(bundle_dir, manifest)

    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "installed"
    install_model.assert_called_once_with(
        catalog["gemma4:e2b"],
        backend="llama.cpp",
        context_size=None,
        force=False,
    )
    record = load_model_ownership()["models"]["ai/gemma4:E2B"]
    assert record["owners"]["vc_assistant"]["blueprint_revision"] == "rev-1"
    output = capsys.readouterr().out
    assert "Runtime model gemma4:e2b (ai/gemma4:E2B) is not installed." in output
    assert "Installing runtime model gemma4:e2b (ai/gemma4:E2B)" in output
    assert "Docker Model Runner" in output
    assert "Runtime models ready: gemma4:e2b" in output


def test_prepare_runtime_models_uses_cluster_model_endpoint(
    mocker,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    monkeypatch.setenv("MN_MODEL_REMOTES_PATH", str(tmp_path / "remotes.json"))
    bundle_dir = tmp_path / "assistant"
    bundle_dir.mkdir()
    manifest = {
        "metadata": {"blueprint_id": "assistant"},
        "runtime": {
            "models": {
                "primary": {
                    "provider": "docker_model_runner",
                    "runtime_model": "qwen3-coder",
                }
            }
        },
    }
    catalog = {
        "qwen3-coder": {
            "id": "qwen3-coder",
            "model": "ai/qwen3-coder",
            "api_model": "ai/qwen3-coder",
            "provider": "docker_model_runner",
        }
    }
    service = {
        "id": "spark:docker-model-runner:qwen3",
        "name": "docker-model-runner",
        "status": "passing",
        "address": "http://192.168.4.173:12434/v1",
        "tags": ["model:ai/qwen3-coder", "model-id:qwen3-coder"],
        "meta": {"model": "ai/qwen3-coder", "api_model": "ai/qwen3-coder"},
    }
    mocker.patch("mn_cli.libs.run_cmds.load_model_catalog", return_value=catalog)
    mocker.patch(
        "mn_cli.libs.run_cmds.client.resolve_service",
        return_value=json.dumps({"services": [service]}),
    )
    install_model = mocker.patch("mn_cli.libs.run_cmds.install_model_entry")
    env_overrides = {}

    summary = run_cmds._prepare_runtime_models_for_run_or_exit(
        bundle_dir,
        manifest,
        env_overrides=env_overrides,
    )

    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "service_registry"
    install_model.assert_not_called()
    endpoints = json.loads(env_overrides["MN_MODEL_ENDPOINTS_JSON"])
    assert endpoints["qwen3-coder"]["api_base"] == "http://mn-litellm-proxy:4000/v1"
    assert "MN_LLM_API_BASE" not in env_overrides
    assert "MN_LLM_MODEL" not in env_overrides


def test_prepare_runtime_models_prunes_node_owned_remote_and_prepares_on_target_node(
    mocker,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    monkeypatch.setenv("MN_MODEL_REMOTES_PATH", str(tmp_path / "remotes.json"))
    bundle_dir = tmp_path / "assistant"
    bundle_dir.mkdir()
    manifest = {
        "metadata": {"blueprint_id": "assistant"},
        "runtime": {
            "models": {
                "primary": {
                    "provider": "docker_model_runner",
                    "runtime_model": "nemotron3:latest",
                    "backend": "llama.cpp",
                }
            }
        },
    }
    catalog = {
        "nemotron3:latest": {
            "id": "nemotron3:latest",
            "model": "ai/nemotron3:latest",
            "api_model": "ai/nemotron3:latest",
            "aliases": ["nemotron3", "ai/nemotron3:latest"],
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "requirements": {"min_vram_gb": 48},
        }
    }
    upsert_model_remote(
        "spark",
        "ai/nemotron3:latest",
        "http://192.168.4.173:12434/v1",
        api_model="ai/nemotron3:latest",
        node="spark",
    )
    mocker.patch("mn_cli.libs.run_cmds.load_model_catalog", return_value=catalog)
    mocker.patch(
        "mn_cli.libs.run_cmds.client.resolve_service",
        return_value=json.dumps({"services": []}),
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.resolve_cluster_model_placement",
        return_value={"source": "cluster_node", "status": "cluster_node", "node": "spark"},
    )
    cluster_install = mocker.patch(
        "mn_cli.libs.run_cmds._install_runtime_cluster_model",
        return_value={
            "endpoint": {
                "provider": "docker_model_runner",
                "model": "ai/nemotron3:latest",
                "runtime_model": "ai/nemotron3:latest",
                "api_model": "ai/nemotron3:latest",
                "api_base": "http://spark:12434/engines/v1",
                "node": "spark",
                "source": "cluster_node_install",
            },
        },
    )
    install_model = mocker.patch("mn_cli.libs.run_cmds.install_model_entry")
    env_overrides = {}

    summary = run_cmds._prepare_runtime_models_for_run_or_exit(
        bundle_dir,
        manifest,
        env_overrides=env_overrides,
    )

    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "runtime_node_installed"
    install_model.assert_not_called()
    cluster_install.assert_called_once()
    assert load_model_remotes()["remotes"] == {}
    endpoints = json.loads(env_overrides["MN_MODEL_ENDPOINTS_JSON"])
    assert endpoints["nemotron3:latest"]["api_base"] == "http://mn-litellm-proxy:4000/v1"
    assert endpoints["nemotron3:latest"]["node"] == "spark"


def test_runtime_model_ready_label_includes_remote_install_node():
    label = run_cmds._runtime_model_ready_label(
        {
            "id": "nemotron3",
            "status": "runtime_node_installed",
            "endpoint": {"node": "mirror_neuron@192.168.4.173"},
        }
    )

    assert label == "nemotron3 installed on mirror_neuron@192.168.4.173"


def test_model_remove_remote_records_matches_aliases(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_MODEL_REMOTES_PATH", str(tmp_path / "remotes.json"))
    upsert_model_remote(
        "spark-nemotron3",
        "ai/nemotron3:latest",
        "http://192.168.4.173:4000/v1",
        api_model="ai/nemotron3:latest",
        node="mirror_neuron@192.168.4.173",
    )

    removed = model_cmds._remove_remote_model_records("nemotron3")

    assert len(removed) == 1
    assert removed[0]["node"] == "mirror_neuron@192.168.4.173"
    assert load_model_remotes()["remotes"] == {}


def test_prepare_runtime_models_does_not_install_via_core_on_capable_cluster_node(
    mocker,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    bundle_dir = tmp_path / "assistant"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "llm": {
                    "enabled": True,
                    "model": "nemotron3:latest",
                    "runtime_model": "nemotron3:latest",
                    "default_config": "primary",
                    "configs": {
                        "primary": {
                            "provider": "docker_model_runner",
                            "model": "nemotron3:latest",
                            "runtime_model": "nemotron3:latest",
                            "backend": "llama.cpp",
                            "context_size": 8192,
                        }
                    },
                }
            }
        )
    )
    manifest = {
        "metadata": {"blueprint_id": "assistant"},
        "runtime": {
            "models": {
                "primary": {
                    "provider": "docker_model_runner",
                    "runtime_model": "nemotron3:latest",
                    "backend": "llama.cpp",
                }
            }
        },
    }
    catalog = {
        "nemotron3:latest": {
            "id": "nemotron3:latest",
            "model": "ai/nemotron3:latest",
            "api_model": "ai/nemotron3:latest",
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "requirements": {
                "min_vram_gb": 48,
                "min_unified_memory_gb": 48,
            },
        }
    }
    resource_report = {
        "nodes": [
            {
                "name": "spark",
                "status": "healthy",
                "scheduling_eligible": True,
                "devices": [
                    {
                        "kind": "gpu",
                        "type": "integrated_gpu",
                        "vendor": "nvidia",
                        "memory_total_mb": 131072,
                        "capabilities": ["nvidia-gb10", "nvidia-dgx-spark"],
                    }
                ],
            }
        ]
    }
    mocker.patch("mn_cli.libs.run_cmds.load_model_catalog", return_value=catalog)
    mocker.patch(
        "mn_cli.libs.run_cmds.client.resolve_service",
        return_value=json.dumps({"services": []}),
    )
    mocker.patch("mn_cli.libs.run_cmds.client.get_resource", return_value=json.dumps(resource_report))
    mocker.patch("mn_cli.libs.run_cmds.model_installed", return_value=False)
    cluster_install = mocker.patch(
        "mn_cli.libs.run_cmds._install_runtime_cluster_model",
        return_value={
            "endpoint": {
                "provider": "docker_model_runner",
                "model": "ai/nemotron3:latest",
                "runtime_model": "ai/nemotron3:latest",
                "api_model": "ai/nemotron3:latest",
                "api_base": "http://spark:12434/engines/v1",
                "node": "spark",
                "source": "cluster_node_install",
            },
        },
    )
    install_model = mocker.patch(
        "mn_cli.libs.run_cmds.install_model_entry",
        return_value={"compatibility": {"backend": "llama.cpp"}},
    )

    env_overrides = {}
    summary = run_cmds._prepare_runtime_models_for_run_or_exit(bundle_dir, manifest, env_overrides=env_overrides)

    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "runtime_node_installed"
    assert "MN_MODEL_ENDPOINTS_JSON" in env_overrides
    assert "ai/nemotron3:latest" in json.loads(env_overrides["MN_PREPARED_RUNTIME_MODELS_JSON"])
    install_model.assert_not_called()
    cluster_install.assert_called_once()
    assert "Installing runtime model nemotron3:latest on spark" not in capsys.readouterr().out
    resolver = run_cmds._prepared_model_installed_resolver(summary)
    assert resolver("ai/nemotron3:latest", {"model": "nemotron3:latest"}) is True
    validation_manifest, validation_config = run_cmds._model_validation_inputs_with_prepared_models(
        manifest,
        {"llm": {"configs": {"primary": {"provider": "docker_model_runner", "model": "nemotron3:latest"}}}},
        summary,
    )
    assert validation_manifest["runtime"]["models"]["primary"]["install_mode"] == "cluster_provided"
    assert validation_config["llm"]["configs"]["primary"]["install_mode"] == "cluster_provided"


def test_runtime_cluster_model_install_uses_target_node_native_sdk_grpc_not_ssh_or_core(mocker, capsys):
    progress_descriptions: list[str] = []

    class FakeProgress:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def add_task(self, description, total=None):
            progress_descriptions.append(description)
            return 1

    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_system_summary",
        return_value=json.dumps(
            {
                "nodes": [
                    {
                        "name": "mirror_neuron@192.168.4.173",
                        "grpc_host": "192.168.4.173",
                        "grpc_port": 55051,
                        "native_sdk_grpc": {
                            "enabled": True,
                            "host": "192.168.4.173",
                            "port": 55052,
                            "target": "192.168.4.173:55052",
                        },
                    }
                ]
            }
        ),
    )
    mocker.patch("mn_cli.libs.run_cmds.use_progress", return_value=True)
    mocker.patch("mn_cli.libs.run_cmds.Progress", FakeProgress)
    remote_client_class = mocker.patch("mn_cli.libs.run_cmds.Client")
    remote_client = remote_client_class.return_value
    remote_client.prepare_runtime_model.return_value = json.dumps(
        {
            "status": "installed",
            "endpoint": {
                "provider": "docker_model_runner",
                "model": "nemotron3",
                "runtime_model": "nemotron3",
            "api_model": "nemotron3",
            "api_base": "http://mn-litellm-proxy:4000/v1",
            "node": "mirror_neuron@192.168.4.173",
            "source": "litellm_gateway",
        },
        "gateway": {"host_api_base": "http://127.0.0.1:4000/v1"},
    }
    )
    shell = mocker.patch("mn_cli.libs.run_cmds.subprocess.run")

    result = run_cmds._install_runtime_cluster_model(
        requirement={"context_size": 8192},
        entry={"id": "nemotron3", "model": "nemotron3", "provider": "docker_model_runner"},
        model={"id": "nemotron3", "model": "nemotron3"},
        cluster={"node": "mirror_neuron@192.168.4.173"},
        backend="llama.cpp",
        context_size=8192,
        force=False,
    )

    shell.assert_not_called()
    remote_client_class.assert_called_once_with(
        target="192.168.4.173:55052",
        timeout=run_cmds.DEFAULT_RUNTIME_MODEL_PREPARE_TIMEOUT_SECONDS,
        auth_token=run_cmds.config.grpc_auth_token,
        admin_token=run_cmds.config.grpc_admin_token,
    )
    remote_client.prepare_runtime_model.assert_called_once()
    payload = remote_client.prepare_runtime_model.call_args.args[0]
    assert payload["node"] == "mirror_neuron@192.168.4.173"
    assert payload["model"] == "nemotron3"
    assert payload["backend"] == "llama.cpp"
    assert result["endpoint"]["node"] == "mirror_neuron@192.168.4.173"
    assert result["endpoint"]["api_base"] == "http://192.168.4.173:4000/v1"
    assert result["endpoint"]["source"] == "remote-dmr"
    output = " ".join(capsys.readouterr().out.split())
    assert (
        "Installing runtime model nemotron3 on mirror_neuron@192.168.4.173 "
        "with native SDK gRPC"
    ) in output
    assert len(progress_descriptions) == 1
    assert (
        "Pulling and starting nemotron3 on mirror_neuron@192.168.4.173; "
        "waiting for remote Docker Model Runner..."
    ) in progress_descriptions[0]


def test_runtime_cluster_model_install_requires_native_sdk_grpc_metadata(mocker):
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_system_summary",
        return_value=json.dumps(
            {
                "nodes": [
                    {
                        "name": "mirror_neuron@192.168.4.173",
                        "grpc_host": "192.168.4.173",
                        "grpc_port": 55051,
                    }
                ]
            }
        ),
    )
    remote_client_class = mocker.patch("mn_cli.libs.run_cmds.Client")
    shell = mocker.patch("mn_cli.libs.run_cmds.subprocess.run")

    with pytest.raises(RuntimeError, match="does not advertise native SDK gRPC"):
        run_cmds._install_runtime_cluster_model(
            requirement={"context_size": 8192},
            entry={"id": "nemotron3", "model": "nemotron3", "provider": "docker_model_runner"},
            model={"id": "nemotron3", "model": "nemotron3"},
            cluster={"node": "mirror_neuron@192.168.4.173"},
            backend="llama.cpp",
            context_size=8192,
            force=False,
        )

    shell.assert_not_called()
    remote_client_class.assert_not_called()


def test_prepare_runtime_models_uses_default_model_fallback_without_capable_cluster_node(
    mocker,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    bundle_dir = tmp_path / "assistant"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "llm": {
                    "enabled": True,
                    "model": "nemotron3:latest",
                    "runtime_model": "nemotron3:latest",
                    "default_config": "primary",
                    "configs": {
                        "primary": {
                            "provider": "docker_model_runner",
                            "model": "nemotron3:latest",
                            "runtime_model": "nemotron3:latest",
                            "backend": "llama.cpp",
                            "context_size": 8192,
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "metadata": {"blueprint_id": "assistant"},
        "runtime": {
            "models": {
                "primary": {
                    "provider": "docker_model_runner",
                    "runtime_model": "nemotron3:latest",
                    "backend": "llama.cpp",
                }
            }
        },
    }
    catalog = {
        "nemotron3:latest": {
            "id": "nemotron3:latest",
            "model": "ai/nemotron3:latest",
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "fallback_model": "gemma4:e2b",
            "requirements": {"min_vram_gb": 48, "min_unified_memory_gb": 48},
        },
        "gemma4:e2b": {
            "id": "gemma4:e2b",
            "model": "ai/gemma4:E2B",
            "api_model": "ai/gemma4:E2B",
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "context_size": 4096,
        }
    }
    resource_report = {
        "nodes": [
            {
                "name": "small-node",
                "status": "healthy",
                "scheduling_eligible": True,
                "devices": [{"kind": "gpu", "type": "integrated_gpu", "memory_total_mb": 32768}],
            }
        ]
    }
    mocker.patch("mn_cli.libs.run_cmds.load_model_catalog", return_value=catalog)
    mocker.patch("mn_cli.libs.run_cmds.model_installed", return_value=False)
    mocker.patch(
        "mn_cli.libs.run_cmds.client.resolve_service",
        return_value=json.dumps({"services": []}),
    )
    mocker.patch("mn_cli.libs.run_cmds.client.get_resource", return_value=json.dumps(resource_report))
    mocker.patch("mn_cli.libs.run_cmds.model_installed", side_effect=lambda model: model == "ai/gemma4:E2B")
    install_model = mocker.patch(
        "mn_cli.libs.run_cmds.install_model_entry",
    )

    env_overrides = {}
    summary = run_cmds._prepare_runtime_models_for_run_or_exit(bundle_dir, manifest, env_overrides=env_overrides)

    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "fallback_model"
    assert summary["models"][0]["fallback"]["id"] == "gemma4:e2b"
    assert summary["models"][0]["fallback"]["model"] == "ai/gemma4:E2B"
    assert env_overrides["MN_LLM_MODEL"] == "ai/gemma4:E2B"
    assert env_overrides["MN_LLM_RUNTIME_MODEL"] == "ai/gemma4:E2B"
    effective_config = json.loads(env_overrides["MN_BLUEPRINT_CONFIG_JSON"])
    assert effective_config["llm"]["model"] == "gemma4:e2b"
    assert effective_config["llm"]["configs"]["primary"]["runtime_model"] == "gemma4:e2b"
    install_model.assert_not_called()


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
    assert result.exit_code == 0
    assert "Job bundle validation confirmed." in result.stdout
    assert "Status: valid" in result.stdout
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
    assert "Workflow steps: 4" in result.stdout


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
    assert "Status: valid" in result.stdout


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
    assert "result.txt" in result.stdout
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
    assert "Schedule ID: schedule-123" in result.stdout
    mock_submit.assert_not_called()
    mock_create_schedule.assert_called_once()
    assert mock_create_schedule.call_args.kwargs["schedule"]["kind"] == "resource_wait"


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
        "nodes": [
            {
                "node_id": "detector",
                "config": {
                    "runner_module": "MirrorNeuron.Sandbox.OpenShell",
                    "custom_openshell_image": "detector/openshell_sandbox",
                },
            }
        ]
    }))
    sandbox_dir = bundle_dir / "payloads" / "detector" / "openshell_sandbox"
    sandbox_dir.mkdir(parents=True)
    (sandbox_dir / "Dockerfile").write_text("FROM base\n")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "OpenShell sandbox image build successful." in result.stdout
    assert "Status: ready" in result.stdout
    mock_build.assert_called_once()
    assert mock_build.call_args.kwargs["env"]["OPENSHELL_GATEWAY_ENDPOINT"] == "http://127.0.0.1:58080"
    manifest = json.loads(mock_submit.call_args.args[0])
    assert manifest["nodes"][0]["config"]["custom_openshell_image"] == "detector/openshell_sandbox"
    assert manifest["nodes"][0]["config"]["from"] == "openshell/sandbox-from:123"


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
        requirements = (
            context / "__mn_skill_dependencies" / "requirements.txt"
        ).read_text(encoding="utf-8")
    finally:
        if context != sandbox_dir:
            run_cmds.shutil.rmtree(context, ignore_errors=True)

    assert context != sandbox_dir
    assert "mirrorneuron-websocket-stream-skill==1.2.7" in requirements
    assert "https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/" in requirements
    assert "--index-url\n" not in requirements
    assert "--index-url https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/" in requirements
    assert "--extra-index-url https://pypi.org/simple" in requirements
    assert "COPY __mn_skill_dependencies/requirements.txt" in dockerfile
    assert "pip install --break-system-packages --no-cache-dir -r /tmp/mn-skill-dependencies/requirements.txt" in dockerfile


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


def test_run_injects_user_home_output_environment(mocker, tmp_path, monkeypatch):
    home_dir = tmp_path / "home"
    output_home = tmp_path / "outputs-home"
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("MN_OUTPUT_HOME", str(output_home))
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
                    "environment": {}
                },
            }
        ]
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    manifest = json.loads(mock_submit.call_args.args[0])
    env = manifest["nodes"][0]["config"]["environment"]
    assert env["MN_OUTPUT_HOME"] == str(output_home)
    assert env["MN_USER_HOME"] == str(home_dir)
    assert env["OTTERDESK_USER_HOME"] == str(home_dir)


def test_run_materializes_vc_final_artifact_outputs(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    output_dir = tmp_path / "vc-output"
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({
            "type": "job_completed",
            "result": {
                "final_artifact": {
                    "type": "vc_early_heuristic_analysis_reports",
                    "executive_summary": "VC Assistant prepared score-only reports.",
                    "company_reports": [
                        {
                            "company_name": "Aurora AI",
                            "company_slug": "aurora-ai",
                            "composite_score": 71.5,
                            "confidence": 0.74,
                            "method_count": 1,
                            "methods": {
                                "berkus_method": {
                                    "status": "scored",
                                    "score": 70,
                                    "evidence_refs": ["pitch_summary.txt"],
                                    "evidence_summary": {
                                        "status_reason": "Berkus method score is grounded in prototype and team evidence."
                                    },
                                    "missing_evidence": []
                                }
                            }
                        }
                    ],
                    "action_ledger": {"budget": 100, "used": 24, "remaining": 76},
                    "artifact_quality": {"status": "warning", "passes_required_gate": True},
                    "run_health": {"status": "warning", "warning_count": 1, "failure_count": 0},
                }
            }
        })
    ])

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "environment": {}
                },
            }
        ]
    }))
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "vc_assistant", "name": "VC Assistant"},
        "inputs": {"payload": {"output_folder": str(output_dir)}},
        "outputs": {"folder_path": str(output_dir)}
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Materialized blueprint outputs" in result.stdout
    assert json.loads((output_dir / "company_index.json").read_text())["companies"][0]["company_slug"] == "aurora-ai"
    assert "Berkus method score" in (output_dir / "aurora-ai" / "analysis.md").read_text()
    assert json.loads((output_dir / "final_artifact.json").read_text())["action_ledger"]["budget"] == 100
    assert json.loads((output_dir / "action_ledger.json").read_text())["used"] == 24
    assert json.loads((output_dir / "artifact_quality.json").read_text())["status"] == "warning"
    assert json.loads((output_dir / "run_health.json").read_text())["warning_count"] == 1


def test_run_materializes_deeply_nested_hostlocal_vc_artifact(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    output_dir = tmp_path / "vc-output"
    final_artifact = {
        "type": "vc_early_heuristic_analysis_reports",
        "executive_summary": "VC Assistant prepared nested HostLocal reports.",
        "company_reports": [
            {
                "company_name": "Boreal Robotics",
                "company_slug": "boreal-robotics",
                "composite_score": 63,
                "confidence": 0.62,
                "method_count": 1,
                "methods": {
                    "cost_to_duplicate_method": {
                        "status": "scored",
                        "score": 65,
                        "evidence_refs": ["company_brief.txt"],
                        "evidence_summary": {
                            "status_reason": "Replacement cost reflects prototype hardware and sensor dataset evidence."
                        },
                        "missing_evidence": [],
                    }
                },
            }
        ],
        "action_ledger": {"budget": 100, "used": 21, "remaining": 79},
        "artifact_quality": {"status": "ok", "passes_required_gate": True},
        "run_health": {"status": "ok", "warning_count": 0, "failure_count": 0},
    }
    nested_result = {"sandbox": {"logs": json.dumps({"final_artifact": final_artifact})}}
    for index in range(25):
        nested_result = {
            "agent_id": f"agent-{index}",
            "input": nested_result,
            "sandbox": {"logs": json.dumps({"status": "completed"})},
        }

    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({
            "type": "job_completed",
            "result": {"last_message": nested_result},
        })
    ])

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "environment": {}
                },
            }
        ]
    }))
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "vc_assistant", "name": "VC Assistant"},
        "outputs": {"folder_path": str(output_dir)}
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Materialized blueprint outputs" in result.stdout
    assert json.loads((output_dir / "company_index.json").read_text())["companies"][0]["company_slug"] == "boreal-robotics"
    assert "Replacement cost reflects prototype" in (output_dir / "boreal-robotics" / "analysis.md").read_text()
    assert json.loads((output_dir / "final_artifact.json").read_text())["action_ledger"]["used"] == 21
    assert json.loads((output_dir / "action_ledger.json").read_text())["used"] == 21
    assert json.loads((output_dir / "artifact_quality.json").read_text())["status"] == "ok"
    assert json.loads((output_dir / "run_health.json").read_text())["status"] == "ok"


def test_extract_final_artifact_from_prefixed_worker_logs():
    final_artifact = {
        "type": "vc_early_heuristic_analysis_reports",
        "company_reports": [{"company_name": "Aurora AI"}],
    }

    result = {
        "sandbox": {
            "logs": "VC Assistant DockerWorker skill and context imports are available\n"
            + json.dumps({"status": "completed", "final_artifact": final_artifact})
        }
    }

    assert run_cmds._extract_final_artifact(result) == final_artifact


def test_materialize_shared_storage_outputs_copies_host_runtime_path(tmp_path):
    host_root = tmp_path / "shared"
    source = host_root / "submissions" / "sub-1" / "outputs" / "user"
    source.mkdir(parents=True)
    (source / "final_artifact.json").write_text('{"ok": true}\n', encoding="utf-8")
    (source / "company" / "analysis.md").parent.mkdir()
    (source / "company" / "analysis.md").write_text("# Company\n", encoding="utf-8")

    target = tmp_path / "Downloads" / "vc_assistant"
    copied = run_cmds._materialize_shared_storage_outputs(
        {
            "host_root": str(host_root),
            "runtime_root": "/runtime/shared",
            "output_copy": [
                {
                    "source_path": "/runtime/shared/submissions/sub-1/outputs/user",
                    "target_path": str(target),
                    "kind": "directory",
                }
            ],
        }
    )

    assert copied is True
    assert json.loads((target / "final_artifact.json").read_text())["ok"] is True
    assert (target / "company" / "analysis.md").read_text() == "# Company\n"


def test_materialize_shared_storage_outputs_cleans_submission_after_copy(tmp_path):
    host_root = tmp_path / "shared"
    submission = host_root / "submissions" / "sub-clean"
    source = submission / "outputs" / "user"
    source.mkdir(parents=True)
    (source / "result.json").write_text('{"ok": true}\n', encoding="utf-8")

    target = tmp_path / "Downloads" / "vc_assistant"
    copied = run_cmds._materialize_shared_storage_outputs(
        {
            "host_root": str(host_root),
            "host_submission_path": str(submission),
            "runtime_root": "/runtime/shared",
            "cleanup_after_output_copy": True,
            "output_copy": [
                {
                    "source_path": "/runtime/shared/submissions/sub-clean/outputs/user",
                    "target_path": str(target),
                    "kind": "directory",
                }
            ],
        }
    )

    assert copied is True
    assert json.loads((target / "result.json").read_text())["ok"] is True
    assert not submission.exists()


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

def test_run_displays_live_job_type_and_follow_status(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("MN_RUN_BACKGROUND_EVENT_RELAY", "0")
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
        "policies": {"job_type": "service", "stream_mode": "live"},
        "nodes": [],
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--follow-seconds", "0", "--web-ui"])

    assert result.exit_code == 0
    assert "Live service" in result.stdout
    assert "Starting: agents scheduled" in result.stdout
    assert "Following: status running" in result.stdout
    assert "75%" not in result.stdout


def test_run_displays_workflow_steps_and_agents(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-workflow")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_pending"}),
        json.dumps({"type": "job_scheduled"}),
        json.dumps({"type": "workflow_step_started", "payload": {"step": "research"}}),
        json.dumps({"type": "workflow_worker_started", "payload": {"step": "research", "worker": "research:docs"}}),
        json.dumps(
            {
                "type": "workflow_step_attempt_completed",
                "payload": {
                    "step": "research",
                    "worker": "research:docs",
                    "tokens": 1200,
                    "tools": 3,
                },
            }
        ),
        json.dumps(
            {
                "type": "workflow_step_attempt_completed",
                "payload": {
                    "step": "research",
                    "worker": "research:docs",
                    "llm": {"usage": {"input_tokens": 350, "output_tokens": 250}},
                    "tools": 1,
                },
            }
        ),
        json.dumps({"type": "research_done", "payload": {"step": "research"}}),
        json.dumps({"type": "job_completed"}),
    ])

    bundle_dir = tmp_path / "workflow_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": "workflow-blueprint",
        "name": "Workflow Blueprint",
        "description": "Two workers inside one workflow step.",
        "workflow": {
            "workflow_id": "workflow-blueprint_v1",
            "entrypoint": "research",
            "steps": [
                {
                    "id": "research",
                    "label": "Research",
                    "goal": "Collect evidence",
                    "run": "research_team",
                    "emits": "research_done",
                    "on": {"research_done": "completed"},
                }
            ],
        },
        "agents": {
            "schema": "mn.agents.communication_graph/v1",
            "entrypoints": ["research:docs"],
            "nodes": [{"node_id": "research:docs"}, {"node_id": "research:risks"}],
            "edges": [],
        },
        "runtime": {
            "bindings": {
                "research_team": {
                    "type": "team",
                    "workers": [
                        {
                            "id": "research:docs",
                            "role": "Analyze docs",
                            "model": "Opus 4.8",
                            "tokens": 1200,
                            "tools": 3,
                        },
                        {
                            "id": "research:risks",
                            "role": "Summarize risks",
                            "model": "Opus 4.8",
                            "tokens": 900,
                            "tools": 2,
                        },
                    ],
                }
            }
        },
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Phases" in result.stdout
    assert "Research" in result.stdout
    assert "research:docs" in result.stdout
    assert "Analyze docs" in " ".join(result.stdout.split())
    assert "1/1 steps" in result.stdout
    assert "Research  |  2 agents" in result.stdout
    assert "run used 1.8k / budget 2.1k tok" in result.stdout


def test_workflow_monitor_renders_service_idle_and_ready_counts():
    progress = {
        "workflow_id": "video_watch_assistant_v1",
        "workflow_kind": "service",
        "status": "running",
        "elapsed_seconds": 342,
        "agent_count": {"done": 1, "running": 0, "idle": 1, "ready": 2, "failed": 0, "total": 2},
        "current_step_id": "visual_detector",
        "current_step": {
            "id": "visual_detector",
            "label": "Visual Detector",
            "status": "idle",
            "current": True,
            "done_count": 0,
            "running_count": 0,
            "idle_count": 1,
            "ready_count": 1,
            "total_count": 1,
            "agents": [
                {
                    "id": "visual_detector",
                    "status": "idle",
                    "working_on": "Review visual detection",
                    "progress": 0.2,
                    "mailbox_depth": 0,
                }
            ],
        },
        "steps": [
            {"id": "ingress", "label": "Ingress", "status": "done", "done_count": 1, "ready_count": 1, "total_count": 1},
            {"id": "visual_detector", "label": "Visual Detector", "status": "idle", "current": True, "idle_count": 1, "ready_count": 1, "total_count": 1},
        ],
        "messages": ["Observing: latest event video_watch_frame_observed"],
    }

    console = Console(record=True, width=140)
    console.print(generate_live_layout("job-service", {"workflow_progress": progress}, JobMonitorState()))
    rendered = console.export_text()

    assert "2/2 steps" in rendered
    assert "idle" in rendered
    assert "Review visual detection" in rendered
    assert "Visual Detector" in rendered


def test_workflow_monitor_renders_graph_layers_and_multiple_active_steps():
    progress = {
        "workflow_id": "tax_graph",
        "workflow_kind": "batch",
        "status": "running",
        "elapsed_seconds": 42,
        "agent_count": {"done": 1, "running": 2, "idle": 0, "ready": 3, "failed": 0, "total": 4},
        "current_step_id": "income",
        "current_step_ids": ["income", "property"],
        "steps": [
            {"id": "intake", "label": "Intake", "status": "done", "done_count": 1, "total_count": 1, "layer": 0, "children": ["income", "property"]},
            {
                "id": "income",
                "label": "Income",
                "status": "running",
                "current": True,
                "running_count": 1,
                "total_count": 1,
                "layer": 1,
                "parents": ["intake"],
                "agents": [{"id": "income_agent", "status": "running", "working_on": "Prepare income", "progress": 0.4}],
            },
            {
                "id": "property",
                "label": "Property",
                "status": "running",
                "current": True,
                "running_count": 1,
                "total_count": 1,
                "layer": 1,
                "parents": ["intake"],
                "agents": [{"id": "property_agent", "status": "running", "working_on": "Prepare property", "progress": 0.3}],
            },
        ],
        "messages": ["Running: graph branches active"],
    }

    console = Console(record=True, width=150)
    console.print(generate_live_layout("job-graph", {"workflow_progress": progress}, JobMonitorState()))
    rendered = console.export_text()

    assert "L2 2 Income" in rendered
    assert "L2 3 Property" in rendered
    assert "income_agent" in rendered
    assert "property_agent" in rendered


def test_workflow_renderer_shared_between_live_monitor_and_blueprint_run_paths():
    manifest = {
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": "workflow-shared-blueprint",
        "name": "Workflow Shared Blueprint",
        "workflow": {
            "workflow_id": "workflow_shared_v1",
            "entrypoint": "research",
            "steps": [
                {
                    "id": "research",
                    "label": "Research",
                    "run": "research_team",
                    "on": {"research_done": "completed"},
                    "emits": "research_done",
                }
            ],
        },
        "runtime": {
            "bindings": {
                "research_team": {
                    "type": "team",
                    "workers": [
                        {
                            "id": "research:docs",
                            "role": "Analyze docs",
                            "model": "opus",
                            "tokens": 1200,
                        }
                    ],
                }
            },
        },
    }

    view = BlueprintWorkflowProgress(manifest, job_id="job-shared")
    view.record_event_token_usage(
        {"type": "workflow_step_attempt_completed", "payload": {"step": "research", "worker": "research:docs", "llm": {"usage": {"input_tokens": 80, "output_tokens": 20}}}}
    )
    snapshot = view.snapshot()

    monitor_console = Console(record=True, width=140)
    monitor_console.print(view.render())
    workflow_view = monitor_console.export_text()

    job_console = Console(record=True, width=140)
    job_console.print(
        generate_live_layout(
            "job-shared",
            {"workflow_progress": snapshot},
            JobMonitorState(),
        )
    )
    job_monitor_view = job_console.export_text()

    assert "Workflow Job Monitor" in workflow_view
    assert "Workflow Job Monitor" in job_monitor_view
    assert "1/1 steps" in workflow_view
    assert "1/1 steps" in job_monitor_view
    assert "Research" in workflow_view
    assert "Research" in job_monitor_view
    assert "run used 100 tok / budget 1.2k tok" in workflow_view
    assert "run used 100 tok / budget 1.2k tok" in job_monitor_view


def test_blueprint_workflow_monitor_disables_ctrl_d():
    progress = {
        "workflow_id": "blueprint-no-ctrld",
        "workflow_kind": "batch",
        "status": "running",
        "elapsed_seconds": 10,
        "steps": [
            {
                "id": "step-a",
                "label": "Step A",
                "status": "running",
                "current": True,
                "running_count": 1,
                "total_count": 1,
                "agents": [],
            }
        ],
    }

    state = JobMonitorState(allow_ctrl_d=False)
    console = Console(record=True, width=140)
    console.print(
        generate_live_layout(
            "job-blueprint",
            {"workflow_progress": progress},
            state=state,
        )
    )
    output = console.export_text()

    assert state.handle_key("\x04", 0) is True
    assert "q or Ctrl+C detach" in output
    assert "Ctrl+D/Ctrl+C" not in output


def test_workflow_token_tracking_prefers_usage_fields_and_ignores_budget_only_payloads():
    manifest = {
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": "workflow-token-blueprint",
        "name": "Workflow Token Blueprint",
        "workflow": {
            "workflow_id": "workflow_token_v1",
            "entrypoint": "step",
            "steps": [
                {
                    "id": "step",
                    "label": "Step",
                    "run": "team",
                    "on": {"step_done": "completed"},
                    "emits": "step_done",
                }
            ],
        },
        "runtime": {
            "bindings": {
                "team": {
                    "type": "team",
                    "workers": [
                        {
                            "id": "agent-1",
                            "role": "Operator",
                            "model": "opus",
                            "tokens": 500,
                        }
                    ],
                }
            },
        },
    }

    view = BlueprintWorkflowProgress(manifest, job_id="job-token")
    console = Console(record=True, width=120)
    console.print(generate_live_layout("job-token", {"workflow_progress": view.snapshot()}, JobMonitorState()))
    assert "500 tok budget" not in console.export_text()
    assert "run used" not in console.export_text()

    view.record_event_token_usage(
        {"type": "workflow_step_attempt_completed", "payload": {"step": "step", "worker": "agent-1", "token_budget": 500}}
    )
    console = Console(record=True, width=120)
    console.print(generate_live_layout("job-token", {"workflow_progress": view.snapshot()}, JobMonitorState()))
    assert "500 tok budget" not in console.export_text()
    assert "run used" not in console.export_text()

    view.record_event_token_usage(
        {"type": "workflow_step_attempt_completed", "payload": {"step": "step", "worker": "agent-1", "tokens": {"count": 12}, "usage": {"total_tokens": 50}}}
    )
    console.print(generate_live_layout("job-token", {"workflow_progress": view.snapshot()}, JobMonitorState()))
    output = console.export_text()
    assert "run used 50 tok" in output


def test_workflow_monitor_state_controls_with_shared_renderer():
    state = JobMonitorState()
    assert state.handle_key("j", 2) is True
    assert state.selected_index == 1
    assert state.handle_key("d", 2) is True
    assert state.detail_mode is True

    console = Console(record=True, width=180, force_terminal=False)
    console.print(
        generate_live_layout(
            "job-workflow-interactive",
            {
                "workflow_progress": {
                    "workflow_id": "interactive-workflow",
                    "workflow_kind": "batch",
                    "status": "running",
                    "elapsed_seconds": 5,
                    "steps": [
                        {
                            "id": "step-a",
                            "label": "Step A",
                            "status": "running",
                            "current": True,
                            "running_count": 1,
                            "total_count": 1,
                            "agents": [
                                {"id": "agent-a", "status": "running", "working_on": "Analyze A", "progress": 0.7, "tokens": 50},
                            ],
                        },
                        {
                            "id": "step-b",
                            "label": "Step B",
                            "status": "done",
                            "done_count": 1,
                            "total_count": 1,
                            "agents": [
                                {"id": "agent-b", "status": "done", "working_on": "Finish B", "progress": 1.0, "tokens": 30},
                            ],
                        },
                    ],
                    "current_step_ids": ["step-a", "step-b"],
                }
            },
            state=state,
        )
    )
    output = console.export_text()
    assert "Agent Detail" in output
    assert "agent-b" in output
    assert "Finish B" in output
    assert state.handle_key("o", 2) is True
    assert state.detail_mode is False
    assert state.handle_key("\x04", 2) is False


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


def test_detached_batch_run_starts_output_event_relay_for_shared_storage(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("MN_SHARED_STORAGE_ROOT", str(tmp_path / "shared"))
    monkeypatch.setenv("MN_RUNTIME_SHARED_STORAGE_ROOT", "/runtime/shared")
    mocker.patch('mn_cli.libs.run_cmds._make_blueprint_run_id', return_value="batch-output-run")
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-batch-output")
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
    mock_process = mocker.Mock(pid=4343)
    mock_popen = mocker.patch('mn_cli.libs.run_cmds.subprocess.Popen', return_value=mock_process)

    bundle_dir = tmp_path / "batch_bundle"
    bundle_dir.mkdir()
    target_path = tmp_path / "Downloads" / "vc_assistant"
    runtime_config = {
        "document_sources": {"folder_path": ""},
        "outputs": {"folder_path": str(target_path)},
    }
    (bundle_dir / "manifest.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "worker",
                "agent_type": "executor",
                "config": {
                    "environment": {
                        "MN_BLUEPRINT_CONFIG_JSON": json.dumps(runtime_config),
                    }
                },
            }
        ],
        "initial_inputs": {
            "worker": {
                "output_folder": str(target_path),
            }
        },
    }))
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "vc_assistant"},
        "outputs": {"folder_path": str(target_path)},
    }))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir), "--follow-seconds", "0"])

    assert result.exit_code == 0
    assert "Output event relay" in result.stdout
    mock_popen.assert_called_once()
    command = mock_popen.call_args.args[0]
    assert command[:3] == [sys.executable, "-m", "mn_sdk.blueprint_support.event_relay"]
    assert "--shared-storage-json" in command
    relay = json.loads((tmp_path / "runs" / "batch-output-run" / "event_relay.json").read_text())
    storage_path = Path(relay["shared_storage_path"])
    storage = json.loads(storage_path.read_text())
    assert storage["output_copy_executor"] == "master_host"
    assert storage["output_copy"][0]["target_path"] == str(target_path)


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
    assert writer.web_ui_url == "http://127.0.0.1:7860"
    assert writer.record_web_ui_url(event) is None

def test_run_error_submitting(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', side_effect=Exception("API failure"))
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    
    assert result.exit_code == 1
    assert "Error running bundle: API failure" in result.stdout

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
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Job Status: Failed" in result.stdout

def test_validate_unexpected_error(mocker, tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.touch()
    
    # Mock open to raise Exception
    mocker.patch('builtins.open', side_effect=Exception("Read error"))
    
    result = runner.invoke(app, ["blueprint", "validate", str(bundle_dir)])
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
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Job Status: Success" in result.stdout
    assert "result.txt" in result.stdout
    assert "result_stream.txt" in result.stdout


def test_stream_cancelled_event_is_terminal(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_running"}),
        json.dumps({"type": "job_cancelled"}),
    ])
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    mock_get = mocker.patch('mn_cli.libs.run_cmds.client.get_job')

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Job Status: Cancelled" in result.stdout
    mock_get.assert_not_called()


def test_stream_helper_cancelled_event_is_terminal_without_follow(mocker, tmp_path):
    job_id = f"job-cancelled-{uuid.uuid4().hex}"
    log_writer = run_cmds.JobLogWriter(job_id, run_dir=tmp_path / "run")
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', return_value=[
        json.dumps({"type": "job_running"}),
        json.dumps({"type": "job_cancelled"}),
    ])
    mock_get = mocker.patch('mn_cli.libs.run_cmds.client.get_job')

    status = run_cmds._stream_and_format_events(job_id, log_writer=log_writer, follow_seconds=0)

    assert status == "cancelled"
    assert '"type": "job_cancelled"' in log_writer.events_file.read_text()
    mock_get.assert_not_called()


def test_stream_keyboard_interrupt(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.stream_events', side_effect=KeyboardInterrupt)
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-123")
    
    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    manifest_file = bundle_dir / "manifest.json"
    manifest_file.write_text('{"nodes": []}')
    
    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])
    assert result.exit_code == 0
    assert "Detached from workflow UI. Job is still running." in result.stdout


def test_post_submit_keyboard_interrupt_detaches_without_stopping_job(mocker, tmp_path):
    mocker.patch('mn_cli.libs.run_cmds.client.submit_job', return_value="job-started")
    mocker.patch(
        'mn_cli.libs.run_cmds._stream_and_format_events',
        side_effect=KeyboardInterrupt,
    )
    mocker.patch(
        'mn_cli.libs.run_cmds.client.get_job',
        return_value=json.dumps({
            "summary": {"status": "running"},
            "job": {"status": "running"},
            "recent_events": [],
        }),
    )

    bundle_dir = tmp_path / "started_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(json.dumps({"nodes": []}))

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Detached from workflow UI. Job is still running." in result.stdout
    assert "Run Detached" in result.stdout
    assert "Running" in result.stdout
