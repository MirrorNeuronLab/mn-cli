import json
import logging
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
import grpc
import pytest
import typer
from logging.handlers import RotatingFileHandler
from typer.testing import CliRunner
from rich.console import Console
from mn_cli.main import app
from mn_cli.libs import model_cmds, run_cmds
from mn_cli.libs.ui import JobMonitorState, generate_live_layout
from mn_cli.libs.workflow_progress import BlueprintWorkflowProgress, _agent_progress_detail
from mn_cli.libs.run_manifest import prepare_manifest_for_submission
from mn_sdk import (
    AgentProgress,
    ModelPrepareTransportError,
    load_model_ownership,
    load_model_remotes,
    upsert_model_remote,
)

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
            "model": "docker.io/ai/gemma4:E2B",
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
            "docker_model": "docker.io/ai/gemma4:E2B",
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
    record = load_model_ownership()["models"]["docker.io/ai/gemma4:E2B"]
    assert record["owners"]["vc_assistant"]["blueprint_revision"] == "rev-1"
    output = capsys.readouterr().out
    assert "Runtime model gemma4:e2b (docker.io/ai/gemma4:E2B) is not installed." in output
    assert "Installing runtime model gemma4:e2b (docker.io/ai/gemma4:E2B)" in output
    assert re.search(r"Docker\s+Model\s+Runner", output)
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
            "model": "docker.io/docker.io/ai/nemotron3:latest",
            "api_model": "docker.io/docker.io/ai/nemotron3:latest",
            "aliases": ["nemotron3", "docker.io/docker.io/ai/nemotron3:latest"],
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "requirements": {"min_vram_gb": 48},
        }
    }
    upsert_model_remote(
        "spark",
        "docker.io/docker.io/ai/nemotron3:latest",
        "http://192.168.4.173:12434/v1",
        api_model="docker.io/docker.io/ai/nemotron3:latest",
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
            "install": {"status": "already_installed"},
            "endpoint": {
                "provider": "docker_model_runner",
                "model": "docker.io/docker.io/ai/nemotron3:latest",
                "runtime_model": "docker.io/docker.io/ai/nemotron3:latest",
                "api_model": "docker.io/docker.io/ai/nemotron3:latest",
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
    assert summary["models"][0]["status"] == "runtime_node_already_installed"
    install_model.assert_not_called()
    cluster_install.assert_called_once()
    assert load_model_remotes()["remotes"] == {}
    endpoints = json.loads(env_overrides["MN_MODEL_ENDPOINTS_JSON"])
    assert endpoints["nemotron3:latest"]["api_base"] == "http://mn-litellm-proxy:4000/v1"
    assert endpoints["nemotron3:latest"]["node"] == "spark"
    resolver = run_cmds._prepared_model_installed_resolver(summary)
    assert resolver("docker.io/docker.io/ai/nemotron3:latest", {"model": "nemotron3:latest"}) is True
    validation_manifest, _validation_config = run_cmds._model_validation_inputs_with_prepared_models(
        manifest,
        {},
        summary,
    )
    assert validation_manifest["runtime"]["models"]["primary"]["install_mode"] == "cluster_provided"

def test_runtime_model_ready_label_includes_remote_install_node():
    label = run_cmds._runtime_model_ready_label(
        {
            "id": "nemotron3",
            "status": "runtime_node_installed",
            "endpoint": {"node": "mirror_neuron@192.168.4.173"},
        }
    )

    assert label == "nemotron3 installed on mirror_neuron@192.168.4.173"


def test_runtime_model_ready_label_includes_remote_already_installed_node():
    label = run_cmds._runtime_model_ready_label(
        {
            "id": "gemma4:e2b",
            "status": "runtime_node_already_installed",
            "endpoint": {"node": "mirror_neuron@192.168.5.10"},
        }
    )

    assert label == "gemma4:e2b already installed on mirror_neuron@192.168.5.10"


def test_model_remove_remote_records_matches_aliases(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_MODEL_REMOTES_PATH", str(tmp_path / "remotes.json"))
    upsert_model_remote(
        "spark-nemotron3",
        "docker.io/docker.io/ai/nemotron3:latest",
        "http://192.168.4.173:4000/v1",
        api_model="docker.io/docker.io/ai/nemotron3:latest",
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
                    "strict_json": True,
                    "require_live": True,
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
            "model": "docker.io/docker.io/ai/nemotron3:latest",
            "api_model": "docker.io/docker.io/ai/nemotron3:latest",
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
                "model": "docker.io/docker.io/ai/nemotron3:latest",
                "runtime_model": "docker.io/docker.io/ai/nemotron3:latest",
                "api_model": "docker.io/docker.io/ai/nemotron3:latest",
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
    assert "docker.io/docker.io/ai/nemotron3:latest" in json.loads(env_overrides["MN_PREPARED_RUNTIME_MODELS_JSON"])
    install_model.assert_not_called()
    cluster_install.assert_called_once()
    assert "Installing runtime model nemotron3:latest on spark" not in capsys.readouterr().out
    resolver = run_cmds._prepared_model_installed_resolver(summary)
    assert resolver("docker.io/docker.io/ai/nemotron3:latest", {"model": "nemotron3:latest"}) is True
    validation_manifest, validation_config = run_cmds._model_validation_inputs_with_prepared_models(
        manifest,
        {"llm": {"configs": {"primary": {"provider": "docker_model_runner", "model": "nemotron3:latest"}}}},
        summary,
    )
    assert validation_manifest["runtime"]["models"]["primary"]["install_mode"] == "cluster_provided"
    assert validation_config["llm"]["configs"]["primary"]["install_mode"] == "cluster_provided"

def _preferred_large_model_config(default_model: str = "gemma4:e2b") -> dict:
    return {
        "llm": {
            "enabled": True,
            "model": default_model,
            "runtime_model": default_model,
            "fallback_model": "gemma4:e2b",
            "preferred_model": "nemotron3",
            "default_config": "primary",
            "configs": {
                "primary": {
                    "provider": "docker_model_runner",
                    "model": default_model,
                    "runtime_model": default_model,
                    "fallback_model": "gemma4:e2b",
                    "backend": "llama.cpp",
                    "max_tokens": 1800,
                    "num_retries": 2,
                    "strict_json": False,
                    "require_live": False,
                },
                "large": {
                    "provider": "docker_model_runner",
                    "model": "nemotron3",
                    "runtime_model": "nemotron3",
                    "backend": "llama.cpp",
                    "context_size": 8192,
                    "max_tokens": 1800,
                    "num_retries": 1,
                    "strict_json": True,
                    "require_live": True,
                    "required": False,
                    "hardware": {"gpu": {"min_count": 1, "min_memory_mb": 49152, "memory_operator": ">="}},
                },
            },
            "small_model_profile": {
                "provider": "docker_model_runner",
                "model": "gemma4:e2b",
                "runtime_model": "gemma4:e2b",
                "fallback_model": "gemma4:e2b",
                "backend": "llama.cpp",
                "max_tokens": 1800,
                "num_retries": 2,
                "strict_json": False,
                "require_live": False,
            },
            "live_model_profile": {
                "provider": "docker_model_runner",
                "model": "gemma4:e2b",
                "runtime_model": "gemma4:e2b",
                "fallback_model": "gemma4:e2b",
                "backend": "llama.cpp",
                "max_tokens": 1800,
                "num_retries": 2,
                "strict_json": False,
                "require_live": False,
            },
            "large_model_profile": {
                "provider": "docker_model_runner",
                "model": "nemotron3",
                "runtime_model": "nemotron3",
                "fallback_model": "gemma4:e2b",
                "backend": "llama.cpp",
                "context_size": 8192,
                "max_tokens": 1800,
                "num_retries": 1,
                "strict_json": True,
                "require_live": True,
                "hardware": {"gpu": {"min_count": 1, "min_memory_mb": 49152, "memory_operator": ">="}},
            },
        }
    }


def test_default_llm_alias_detection_is_explicit():
    assert run_cmds._blueprint_requests_default_llm({"llm": {"model": "default"}}) is True
    assert run_cmds._blueprint_requests_default_llm({"llm": {"model": "gemma4:e2b"}}) is False


def test_default_manifest_model_is_satisfied_by_prepared_fallback():
    summary = {
        "models": [
            {
                "id": "nemotron3",
                "model": "docker.io/ai/nemotron3:latest",
                "status": "fallback_model",
                "fallback": {
                    "id": "gemma4:e2b",
                    "model": "docker.io/ai/gemma4:E2B",
                },
            }
        ]
    }

    manifest, _ = run_cmds._model_validation_inputs_with_prepared_models(
        {"runtime": {"models": {"primary": {"provider": "docker_model_runner", "model": "default"}}}},
        {},
        summary,
    )

    assert manifest["runtime"]["models"]["primary"]["install_mode"] == "cluster_provided"


def test_prepared_default_model_marks_inheriting_llm_profile_cluster_provided():
    summary = {
        "models": [
            {
                "id": "nemotron3",
                "model": "docker.io/ai/nemotron3:latest",
                "status": "explicit_config",
            }
        ]
    }

    _, config = run_cmds._model_validation_inputs_with_prepared_models(
        {},
        {
            "llm": {
                "model": "default",
                "configs": {
                    "primary": {
                        "provider": "docker_model_runner",
                        "api_base": "http://localhost:12434/engines/v1",
                    }
                },
            }
        },
        summary,
    )

    assert config["llm"]["install_mode"] == "cluster_provided"
    assert config["llm"]["configs"]["primary"]["install_mode"] == "cluster_provided"

def _preferred_large_model_catalog() -> dict:
    return {
        "nemotron3": {
            "id": "nemotron3",
            "model": "docker.io/docker.io/ai/nemotron3:latest",
            "api_model": "nemotron3",
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "context_size": 8192,
            "fallback_model": "gemma4:e2b",
            "requirements": {"min_vram_gb": 48, "min_unified_memory_gb": 48},
        },
        "gemma4:e2b": {
            "id": "gemma4:e2b",
            "model": "docker.io/ai/gemma4:E2B",
            "api_model": "docker.io/ai/gemma4:E2B",
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "context_size": 4096,
        },
    }

def _spark_gb10_resource_report() -> dict:
    return {
        "nodes": [
            {
                "name": "mirror_neuron@192.168.5.12",
                "status": "healthy",
                "scheduling_eligible": True,
                "devices": [
                    {
                        "kind": "gpu",
                        "type": "nvidia/gpu",
                        "vendor": "nvidia",
                        "model": "NVIDIA GB10",
                        "memory_total_mb": 131072,
                        "memory_free_mb": 126000,
                        "capabilities": ["gpu", "nvidia", "cuda", "nvidia-gb10", "nvidia-dgx-spark"],
                    }
                ],
            }
        ]
    }

def test_prepare_runtime_models_promotes_preferred_large_profile_on_capable_cluster_node(
    mocker,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    bundle_dir = tmp_path / "assistant"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps(_preferred_large_model_config()), encoding="utf-8")
    manifest = {"metadata": {"blueprint_id": "assistant"}}
    catalog = _preferred_large_model_catalog()
    mocker.patch("mn_cli.libs.run_cmds.load_model_catalog", return_value=catalog)
    mocker.patch(
        "mn_cli.libs.run_cmds.client.resolve_service",
        return_value=json.dumps({"services": []}),
    )
    mocker.patch("mn_cli.libs.run_cmds.client.get_resource", return_value=json.dumps(_spark_gb10_resource_report()))
    mocker.patch("mn_cli.libs.run_cmds.model_installed", return_value=False)
    cluster_install = mocker.patch(
        "mn_cli.libs.run_cmds._install_runtime_cluster_model",
        return_value={
            "install": {"status": "already_installed", "node": "mirror_neuron@192.168.5.12"},
            "endpoint": {
                "provider": "docker_model_runner",
                "model": "nemotron3",
                "runtime_model": "docker.io/docker.io/ai/nemotron3:latest",
                "api_model": "nemotron3",
                "api_base": "http://192.168.5.12:4000/v1",
                "node": "mirror_neuron@192.168.5.12",
                "source": "remote-dmr",
            },
        },
    )
    install_model = mocker.patch("mn_cli.libs.run_cmds.install_model_entry")

    env_overrides = {}
    summary = run_cmds._prepare_runtime_models_for_run_or_exit(bundle_dir, manifest, env_overrides=env_overrides)

    assert summary["ok"] is True
    assert len(summary["models"]) == 1
    assert summary["models"][0]["id"] == "nemotron3"
    assert summary["models"][0]["status"] == "runtime_node_already_installed"
    assert summary["models"][0]["cluster"]["node"] == "mirror_neuron@192.168.5.12"
    cluster_install.assert_called_once()
    install_model.assert_not_called()
    prepared = json.loads(env_overrides["MN_PREPARED_RUNTIME_MODELS_JSON"])
    assert "nemotron3" in prepared
    assert "docker.io/docker.io/ai/nemotron3:latest" in prepared
    effective_config = json.loads(env_overrides["MN_BLUEPRINT_CONFIG_JSON"])
    assert effective_config["llm"]["active_model_profile"] == "large_model_profile"
    assert effective_config["llm"]["model"] == "nemotron3"
    assert effective_config["llm"]["runtime_model"] == "docker.io/docker.io/ai/nemotron3:latest"
    assert effective_config["llm"]["strict_json"] is True
    assert effective_config["llm"]["configs"]["primary"]["model"] == "nemotron3"
    assert effective_config["llm"]["configs"]["primary"]["runtime_model"] == "docker.io/docker.io/ai/nemotron3:latest"
    resolver = run_cmds._prepared_model_installed_resolver(summary)
    assert resolver("docker.io/docker.io/ai/nemotron3:latest", {"model": "nemotron3"}) is True
    validation_manifest, validation_config = run_cmds._model_validation_inputs_with_prepared_models(
        {"runtime": {"models": {"primary": {"provider": "docker_model_runner", "model": "nemotron3"}}}},
        {"llm": {"configs": {"primary": {"provider": "docker_model_runner", "model": "nemotron3"}}}},
        summary,
    )
    assert validation_manifest["runtime"]["models"]["primary"]["install_mode"] == "cluster_provided"
    assert validation_config["llm"]["configs"]["primary"]["install_mode"] == "cluster_provided"

def test_prepare_runtime_models_keeps_default_model_without_capable_cluster_node(
    mocker,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    bundle_dir = tmp_path / "assistant"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps(_preferred_large_model_config()), encoding="utf-8")
    manifest = {"metadata": {"blueprint_id": "assistant"}}
    catalog = _preferred_large_model_catalog()
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
    mocker.patch(
        "mn_cli.libs.run_cmds.client.resolve_service",
        return_value=json.dumps({"services": []}),
    )
    mocker.patch("mn_cli.libs.run_cmds.client.get_resource", return_value=json.dumps(resource_report))
    mocker.patch("mn_cli.libs.run_cmds.model_installed", side_effect=lambda model: model == "docker.io/ai/gemma4:E2B")
    cluster_install = mocker.patch("mn_cli.libs.run_cmds._install_runtime_cluster_model")
    install_model = mocker.patch("mn_cli.libs.run_cmds.install_model_entry")

    env_overrides = {}
    summary = run_cmds._prepare_runtime_models_for_run_or_exit(bundle_dir, manifest, env_overrides=env_overrides)

    assert summary["ok"] is True
    assert len(summary["models"]) == 1
    assert summary["models"][0]["id"] == "gemma4:e2b"
    assert summary["models"][0]["status"] == "already_installed"
    cluster_install.assert_not_called()
    install_model.assert_not_called()
    effective_config = json.loads(env_overrides["MN_BLUEPRINT_CONFIG_JSON"])
    assert effective_config["llm"]["active_model_profile"] == "small_model_profile"
    assert effective_config["llm"]["model"] == "docker.io/ai/gemma4:E2B"
    assert effective_config["llm"]["runtime_model"] == "docker.io/ai/gemma4:E2B"

def test_prepare_runtime_models_surfaces_large_profile_prepare_failure_without_fallback(
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
    (config_dir / "default.json").write_text(json.dumps(_preferred_large_model_config()), encoding="utf-8")
    manifest = {"metadata": {"blueprint_id": "assistant"}}
    catalog = _preferred_large_model_catalog()
    mocker.patch("mn_cli.libs.run_cmds.load_model_catalog", return_value=catalog)
    mocker.patch(
        "mn_cli.libs.run_cmds.client.resolve_service",
        return_value=json.dumps({"services": []}),
    )
    mocker.patch("mn_cli.libs.run_cmds.client.get_resource", return_value=json.dumps(_spark_gb10_resource_report()))
    mocker.patch("mn_cli.libs.run_cmds.model_installed", return_value=False)
    cluster_install = mocker.patch(
        "mn_cli.libs.run_cmds._install_runtime_cluster_model",
        side_effect=RuntimeError("remote prepare failed"),
    )
    install_model = mocker.patch("mn_cli.libs.run_cmds.install_model_entry")

    with pytest.raises(typer.Exit) as exc:
        run_cmds._prepare_runtime_models_for_run_or_exit(bundle_dir, manifest, env_overrides={})

    assert exc.value.exit_code == 1
    cluster_install.assert_called_once()
    install_model.assert_not_called()
    assert "Runtime model preparation failed unexpectedly" in capsys.readouterr().out

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
        "Preparing runtime model nemotron3 on mirror_neuron@192.168.4.173 "
        "with native SDK gRPC"
    ) in output
    assert len(progress_descriptions) == 1
    assert (
        "Checking and preparing nemotron3 on mirror_neuron@192.168.4.173; "
        "waiting for remote Docker Model Runner..."
    ) in progress_descriptions[0]

def test_runtime_cluster_model_install_uses_long_timeout_for_local_runtime_coordinator(mocker):
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_system_summary",
        return_value=json.dumps(
            {
                "nodes": [
                    {
                        "name": "mirror_neuron@192.168.5.10",
                        "grpc_host": "192.168.5.10",
                        "grpc_port": 55051,
                        "self?": True,
                    }
                ]
            }
        ),
    )
    local_client_class = mocker.patch("mn_cli.libs.run_cmds.Client")
    local_client = local_client_class.return_value
    local_client.prepare_runtime_model.return_value = json.dumps(
        {
            "status": "installed",
            "endpoint": {
                "provider": "docker_model_runner",
                "model": "docker.io/ai/gemma4:E2B",
                "runtime_model": "docker.io/ai/gemma4:E2B",
                "api_model": "docker.io/ai/gemma4:E2B",
            },
        }
    )

    result = run_cmds._install_runtime_cluster_model(
        requirement={"context_size": 4096},
        entry={"id": "gemma4:e2b", "model": "docker.io/ai/gemma4:E2B", "provider": "docker_model_runner"},
        model={"id": "gemma4:e2b", "model": "docker.io/ai/gemma4:E2B"},
        cluster={"node": "mirror_neuron@192.168.5.10"},
        backend="llama.cpp",
        context_size=4096,
        force=False,
    )

    local_client_class.assert_called_once_with(
        target=run_cmds.config.grpc_target,
        timeout=run_cmds.DEFAULT_RUNTIME_MODEL_PREPARE_TIMEOUT_SECONDS,
        auth_token=run_cmds.config.grpc_auth_token,
        admin_token=run_cmds.config.grpc_admin_token,
    )
    local_client.prepare_runtime_model.assert_called_once()
    assert result["endpoint"]["source"] == "local-dmr"
    assert result["endpoint"]["node"] == "mirror_neuron@192.168.5.10"

class FakePrepareRpcError(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def details(self):
        return str(self._code)

def test_prepare_runtime_model_retries_once_on_deadline(mocker, capsys):
    runtime_client = mocker.Mock()
    runtime_client.prepare_runtime_model.side_effect = [
        FakePrepareRpcError(grpc.StatusCode.DEADLINE_EXCEEDED),
        json.dumps({"status": "installed"}),
    ]

    response = run_cmds._prepare_runtime_model_with_retry(runtime_client, {"model": "docker.io/ai/gemma4:E2B"})

    assert response["status"] == "installed"
    assert runtime_client.prepare_runtime_model.call_count == 2
    assert "retrying once" in capsys.readouterr().out

def test_prepare_runtime_model_does_not_retry_non_transient_errors(mocker):
    runtime_client = mocker.Mock()
    runtime_client.prepare_runtime_model.side_effect = FakePrepareRpcError(grpc.StatusCode.INVALID_ARGUMENT)

    with pytest.raises(ModelPrepareTransportError) as exc_info:
        run_cmds._prepare_runtime_model_with_retry(runtime_client, {"model": "docker.io/ai/gemma4:E2B"})

    assert exc_info.value.code == "model.custom_prepare_transport_failed"
    assert exc_info.value.retryable is False
    runtime_client.prepare_runtime_model.assert_called_once()

def test_cluster_node_endpoint_is_local_uses_local_host_alias(monkeypatch):
    node_endpoint = {
        "node": {"name": "mirror_neuron@192.168.6.28"},
        "host": "192.168.6.28",
        "port": "55051",
    }
    monkeypatch.setattr(run_cmds, "_local_host_addresses", lambda: {"192.168.6.28"})

    assert run_cmds._cluster_node_endpoint_is_local(node_endpoint) is True

def test_cluster_node_endpoint_is_not_local_for_remote_host(monkeypatch):
    node_endpoint = {
        "node": {"name": "mirror_neuron@192.168.4.173"},
        "host": "192.168.4.173",
        "port": "55051",
    }
    monkeypatch.setattr(run_cmds, "_local_host_addresses", lambda: {"192.168.6.28"})

    assert run_cmds._cluster_node_endpoint_is_local(node_endpoint) is False

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
                    "small_model_profile": {
                        "provider": "docker_model_runner",
                        "model": "gemma4:e2b",
                        "runtime_model": "gemma4:e2b",
                        "backend": "llama.cpp",
                        "max_tokens": 1800,
                        "num_retries": 2,
                        "strict_json": False,
                        "require_live": False,
                    },
                    "large_model_profile": {
                        "provider": "docker_model_runner",
                        "model": "nemotron3:latest",
                        "runtime_model": "nemotron3:latest",
                        "backend": "llama.cpp",
                        "context_size": 8192,
                        "max_tokens": 1800,
                        "num_retries": 1,
                        "strict_json": True,
                        "require_live": True,
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
            "model": "docker.io/docker.io/ai/nemotron3:latest",
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "fallback_model": "gemma4:e2b",
            "requirements": {"min_vram_gb": 48, "min_unified_memory_gb": 48},
        },
        "gemma4:e2b": {
            "id": "gemma4:e2b",
            "model": "docker.io/ai/gemma4:E2B",
            "api_model": "docker.io/ai/gemma4:E2B",
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
    mocker.patch("mn_cli.libs.run_cmds.model_installed", side_effect=lambda model: model == "docker.io/ai/gemma4:E2B")
    install_model = mocker.patch(
        "mn_cli.libs.run_cmds.install_model_entry",
    )

    env_overrides = {}
    summary = run_cmds._prepare_runtime_models_for_run_or_exit(bundle_dir, manifest, env_overrides=env_overrides)

    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "fallback_model"
    assert summary["models"][0]["fallback"]["id"] == "gemma4:e2b"
    assert summary["models"][0]["fallback"]["model"] == "docker.io/ai/gemma4:E2B"
    assert env_overrides["MN_LLM_MODEL"] == "docker.io/ai/gemma4:E2B"
    assert env_overrides["MN_LLM_RUNTIME_MODEL"] == "docker.io/ai/gemma4:E2B"
    effective_config = json.loads(env_overrides["MN_BLUEPRINT_CONFIG_JSON"])
    assert effective_config["llm"]["model"] == "gemma4:e2b"
    assert effective_config["llm"]["active_model_profile"] == "small_model_profile"
    assert effective_config["llm"]["strict_json"] is False
    assert effective_config["llm"]["require_live"] is False
    assert effective_config["llm"]["max_tokens"] == 1800
    assert effective_config["llm"]["num_retries"] == 2
    assert effective_config["llm"]["configs"]["primary"]["runtime_model"] == "gemma4:e2b"
    assert effective_config["llm"]["configs"]["primary"]["max_tokens"] == 1800
    assert effective_config["llm"]["configs"]["primary"]["num_retries"] == 2
    install_model.assert_not_called()

def test_runtime_model_profile_applies_large_model_strict_contract():
    config = {
        "llm": {
            "enabled": True,
            "model": "nemotron3",
            "runtime_model": "nemotron3",
            "strict_json": False,
            "require_live": False,
            "default_config": "primary",
            "configs": {
                "primary": {
                    "provider": "docker_model_runner",
                    "model": "nemotron3",
                    "runtime_model": "nemotron3",
                    "backend": "llama.cpp",
                    "max_tokens": 900,
                    "num_retries": 2,
                }
            },
            "small_model_profile": {
                "provider": "docker_model_runner",
                "model": "gemma4:e2b",
                "runtime_model": "gemma4:e2b",
                "strict_json": False,
                "require_live": False,
            },
            "large_model_profile": {
                "provider": "docker_model_runner",
                "model": "nemotron3",
                "runtime_model": "nemotron3",
                "backend": "llama.cpp",
                "context_size": 8192,
                "max_tokens": 1800,
                "num_retries": 1,
                "strict_json": True,
                "require_live": True,
            },
        }
    }

    materialized = run_cmds._config_with_runtime_model_profile(config)

    assert materialized["llm"]["active_model_profile"] == "large_model_profile"
    assert materialized["llm"]["strict_json"] is True
    assert materialized["llm"]["require_live"] is True
    assert materialized["llm"]["context_size"] == 8192
    assert materialized["llm"]["configs"]["primary"]["context_size"] == 8192
    assert materialized["llm"]["configs"]["primary"]["max_tokens"] == 1800
    assert materialized["llm"]["configs"]["primary"]["num_retries"] == 1


def test_custom_runtime_model_selects_most_powerful_capable_node(mocker):
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_resource",
        return_value=json.dumps(
            {
                "nodes": [
                    {
                        "name": "gpu-64",
                        "status": "healthy",
                        "scheduling_eligible": True,
                        "gpu_memory_total_mb": 65536,
                        "gpu_memory_free_mb": 60000,
                    },
                    {
                        "name": "gpu-128",
                        "status": "healthy",
                        "scheduling_eligible": True,
                        "gpu_memory_total_mb": 131072,
                        "gpu_memory_free_mb": 120000,
                    },
                ]
            }
        ),
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_system_summary",
        return_value=json.dumps(
            {
                "nodes": [
                    {
                        "name": name,
                        "status": "healthy",
                        "scheduling_eligible": True,
                        "native_sdk_grpc": {
                            "enabled": True,
                            "host": f"{name}.local",
                            "port": 55052,
                            "target": f"{name}.local:55052",
                            "capabilities": ["custom_hf_model_v1"],
                        },
                    }
                    for name in ("gpu-64", "gpu-128")
                ]
            }
        ),
    )

    placement = run_cmds._resolve_runtime_cluster_model(
        requirement={"customize_mode": True},
        entry={"id": "custom", "model": "huggingface.co/acme/custom:Q4_K_M"},
    )

    assert placement["node"] == "gpu-128"
    assert placement["selection"]["gpu_memory_total_mb"] == 131072


def test_custom_runtime_model_prepare_payload_carries_risk_contract(mocker):
    mocker.patch(
        "mn_cli.libs.run_cmds._cluster_node_endpoint",
        return_value={
            "grpc_target": "gpu-128.local:55051",
            "host": "gpu-128.local",
            "port": "55051",
            "node": {"name": "gpu-128", "self?": True},
        },
    )
    runtime_client = mocker.Mock()
    runtime_client.prepare_runtime_model.return_value = json.dumps(
        {
            "status": "installed",
            "docker_model": "huggingface.co/acme/custom:Q4_K_M",
            "endpoint": {
                "model": "huggingface.co/acme/custom:Q4_K_M",
                "runtime_model": "huggingface.co/acme/custom:Q4_K_M",
            },
        }
    )
    mocker.patch("mn_cli.libs.run_cmds._runtime_model_prepare_client", return_value=runtime_client)

    run_cmds._install_runtime_cluster_model(
        requirement={"model": "hf.co/acme/custom:Q4_K_M", "context_size": 4096},
        entry={
            "id": "huggingface.co/acme/custom:Q4_K_M",
            "model": "huggingface.co/acme/custom:Q4_K_M",
            "api_model": "huggingface.co/acme/custom:Q4_K_M",
            "provider": "docker_model_runner",
            "backend": "llama.cpp",
            "customize_mode": True,
            "verification": "unverified",
            "source_model": "hf.co/acme/custom:Q4_K_M",
        },
        model={
            "id": "huggingface.co/acme/custom:Q4_K_M",
            "model": "huggingface.co/acme/custom:Q4_K_M",
        },
        cluster={"node": "gpu-128"},
        backend="llama.cpp",
        context_size=4096,
        force=False,
    )

    payload = runtime_client.prepare_runtime_model.call_args.args[0]
    assert payload["customize_mode"] is True
    assert payload["verification"] == "unverified"
    assert payload["source_model"] == "hf.co/acme/custom:Q4_K_M"
