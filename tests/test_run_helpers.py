import json
import uuid
from pathlib import Path

import pytest

from mn_cli.libs.run_logs import JobLogWriter, materialize_sent_email_copy
from mn_cli.libs.artifacts import promote_large_payloads_to_blob_refs
from mn_cli.libs.run_manifest import (
    apply_manifest_config_bindings,
    ensure_blueprint_support_sdk_build_context_uploads,
    load_blueprint_config,
    prepare_manifest_for_submission,
    stage_blueprint_support_payloads_for_manifest,
    stage_skill_dependency_payloads_for_manifest,
    stage_skill_runtime_support_payloads_for_manifest,
    stage_local_input_payloads_for_manifest,
    stage_upload_path_payloads_for_manifest,
    workspace_root,
)

def _write_skill_pyproject(
    skills_root: Path,
    folder: str,
    package: str,
    *,
    runtime: bool = False,
) -> None:
    skill_dir = skills_root / folder
    skill_dir.mkdir(parents=True)
    runtime_block = ""
    if runtime:
        runtime_block = """
[tool.mirrorneuron.skill]
id = "w3m_browser_skill"
package = "mirrorneuron-w3m-browser-skill"

[tool.mirrorneuron.skill.runtime]
driver = "docker_worker"
install_scope = "shared_job_container"
base_image = "debian:bookworm-slim"
verify_commands = ["command -v w3m", "python3 -c 'import mn_w3m_browser_skill'"]

[[tool.mirrorneuron.skill.runtime.system_packages]]
manager = "apt"
packages = ["ca-certificates", "python3", "python3-pip", "python3-venv", "w3m"]
"""
    skill_dir.joinpath("pyproject.toml").write_text(
        f"""
[project]
name = "{package}"
version = "0.0.0"
dependencies = []
{runtime_block}
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"
""",
        encoding="utf-8",
    )


def test_prepare_manifest_for_submission_merges_runtime_env_and_metadata(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({
        "identity": {"blueprint_id": "bp"},
        "vl_model": {"model": "default"},
        "manifest_config_bindings": [
            {
                "config_path": "vl_model.model",
                "manifest_path": "nodes.worker.config.environment.CUSTOM_MODEL",
            }
        ],
    }))
    (config_dir / "overwrite.json").write_text(json.dumps({"vl_model": {"model": "overwrite"}}))

    manifest = {
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "environment": {
                        "LITELLM_MODEL": "ollama/test",
                        "MN_LLM_API_KEY": "kept",
                        "MN_BLUEPRINT_CONFIG_JSON": json.dumps({"identity": {"blueprint_id": "stale"}}),
                    }
                },
            }
        ]
    }

    prepared = prepare_manifest_for_submission(
        bundle_dir,
        manifest,
        env_overrides={"MN_RUN_ID": "run-1"},
        submission_metadata={"blueprint_id": "bp"},
        config_overrides={"vl_model": {"base_url": "http://local"}},
    )

    env = prepared["nodes"][0]["config"]["environment"]
    injected_config = json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])
    assert injected_config["identity"]["blueprint_id"] == "bp"
    assert injected_config["vl_model"] == {"model": "overwrite", "base_url": "http://local"}
    assert env["VL_MODEL_NAME"] == "overwrite"
    assert env["OLLAMA_MODEL"] == "overwrite"
    assert env["VL_MODEL_BASE_URL"] == "http://local"
    assert env["CUSTOM_MODEL"] == "overwrite"
    assert env["MN_RUN_ID"] == "run-1"
    assert env["MN_LLM_MODEL"] == "ollama/test"
    assert env["MN_LLM_API_KEY"] == "kept"
    assert prepared["metadata"]["mn_cli"]["blueprint_id"] == "bp"


def test_prepare_manifest_auto_patches_skill_binary_deps_to_dockerworker(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    skills_root = tmp_path / "mn-skills"
    sdk_root = tmp_path / "mn-python-sdk"
    bundle_dir.mkdir()
    sdk_root.mkdir()
    (sdk_root / "pyproject.toml").write_text("[project]\nname='mirrorneuron-python-sdk'\n", encoding="utf-8")
    (bundle_dir / "config").mkdir()
    worker_dir = bundle_dir / "payloads" / "worker"
    worker_dir.mkdir(parents=True)
    (worker_dir / "requirements.txt").write_text(
        "--index-url https://packages.example/simple/\n"
        "mirrorneuron-w3m-browser-skill\n"
        "example-external>=1\n",
        encoding="utf-8",
    )
    _write_skill_pyproject(
        skills_root,
        "w3m_browser_skill",
        "mirrorneuron-w3m-browser-skill",
        runtime=True,
    )
    _write_skill_pyproject(
        skills_root,
        "blueprint_support_skill",
        "mirrorneuron-blueprint-support-skill",
    )
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))
    monkeypatch.setenv("MN_WORKSPACE_ROOT", str(tmp_path))

    (bundle_dir / "config" / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "binary_bp"},
                "python_dependencies": {
                    "requirements": "worker/requirements.txt",
                    "packages": [
                        "mirrorneuron-blueprint-support-skill",
                        "mirrorneuron-w3m-browser-skill",
                        "example-external>=1",
                    ],
                },
                "input_skills": {
                    "w3m_browser": {
                        "skill": "w3m_browser_skill",
                        "package": "mirrorneuron-w3m-browser-skill",
                        "install_policy": "python_environment_pip",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    manifest = {
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "upload_path": "worker",
                    "upload_as": "worker",
                    "workdir": "/sandbox/job/worker",
                    "command": ["python3.11", "run.py"],
                    "python_environment": {"requirements": "worker/requirements.txt"},
                    "environment": {},
                },
            }
        ]
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    node_config = prepared["nodes"][0]["config"]
    runtime = prepared["metadata"]["mn_skill_runtime"]
    assert node_config["runner_module"] == "MirrorNeuron.Runner.DockerWorker"
    assert node_config["workdir"] == "/mn/job/worker"
    assert node_config["docker_worker_image"] == "__mn_skill_runtime/docker_worker"
    assert node_config["shared_container"] is True
    assert node_config["reuse_shared_container"] is True
    assert "python_environment" not in node_config
    assert {"source": "__mn_skill_runtime", "target": "__mn_skill_runtime"} in node_config["upload_paths"]
    assert node_config.get("build_context_upload_paths") in (None, [])
    assert runtime["local_packages"] == []
    assert runtime["generated"] is True
    assert "w3m" in runtime["system_packages"]["apt"]
    assert runtime["patched_nodes"] == ["worker"]

    payloads: dict[str, bytes] = {}
    staged = stage_skill_runtime_support_payloads_for_manifest(
        prepared,
        payloads,
        bundle_dir=bundle_dir,
    )

    assert staged["staged"] is True
    dockerfile = payloads["__mn_skill_runtime/docker_worker/Dockerfile"].decode()
    requirements = payloads["__mn_skill_runtime/docker_worker/requirements.txt"].decode()
    assert "apt-get install" in dockerfile
    assert "w3m" in dockerfile
    assert "COPY build_context/mn-python-sdk" not in dockerfile
    assert "COPY build_context/w3m_browser_skill" not in dockerfile
    assert "/tmp/mn-local-packages" not in dockerfile
    assert "command -v w3m" in dockerfile
    assert "mirrorneuron-w3m-browser-skill==1.2.7" in requirements
    assert "mirrorneuron-blueprint-support-skill==1.2.7" in requirements
    assert "example-external>=1" in requirements


def test_prepare_manifest_injects_gar_skill_dependencies_for_hostlocal(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"identity": {"blueprint_id": "gar_hostlocal"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("MN_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MN_SKILLS_ROOT", str(tmp_path / "mn-skills"))

    manifest = {
        "skill_dependencies": [
            {
                "type": "pip",
                "source": "gar",
                "name": "mirrorneuron-llm-ocr-skill",
                "version": "v1.2.7",
            }
        ],
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "command": ["python3", "run.py"],
                    "environment": {},
                },
            }
        ],
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    node_config = prepared["nodes"][0]["config"]
    packages = node_config["python_environment"]["packages"]
    assert packages[:5] == [
        "--index-url",
        "https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/",
        "--extra-index-url",
        "https://pypi.org/simple",
        "mirrorneuron-llm-ocr-skill==1.2.7",
    ]
    env = node_config["environment"]
    assert "MN_SKILLS_ROOT" not in env
    assert "MN_WORKSPACE_ROOT" not in env
    assert str(tmp_path / "mn-skills") not in env.get("PYTHONPATH", "")


def test_prepare_manifest_stages_local_skill_dependencies_in_dev(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    skills_root = tmp_path / "mn-skills"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"identity": {"blueprint_id": "local_skill_dev"}}),
        encoding="utf-8",
    )
    _write_skill_pyproject(
        skills_root,
        "evidence_engine_skill",
        "mirrorneuron-evidence-engine-skill",
    )
    skill_module = skills_root / "evidence_engine_skill" / "src" / "mn_evidence_engine_skill"
    skill_module.mkdir(parents=True)
    (skill_module / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setenv("MN_ENV", "dev")
    monkeypatch.setenv("MN_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))

    manifest = {
        "skill_dependencies": [
            {
                "type": "pip",
                "source": "gar",
                "name": "mirrorneuron-evidence-engine-skill",
                "version": "1.2.7",
            },
            {
                "type": "pip",
                "source": "gar",
                "name": "mirrorneuron-rag-skill",
                "version": "1.2.14",
            },
        ],
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.DockerWorker",
                    "docker_worker_image": "worker/docker_worker",
                    "environment": {},
                },
            }
        ],
    }
    payloads = {"worker/docker_worker/Dockerfile": b"FROM python:3.11-slim\n"}

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)
    remaining = [item["name"] for item in prepared["skill_dependencies"]]
    assert remaining == ["mirrorneuron-rag-skill"]
    assert prepared["metadata"]["mn_local_skill_dependencies"]["packages"] == [
        "mirrorneuron-evidence-engine-skill"
    ]

    staged = stage_skill_dependency_payloads_for_manifest(
        prepared,
        payloads,
        bundle_dir=bundle_dir,
    )

    assert ".mn-local-skills/evidence_engine_skill" in staged["sources"]
    assert (
        ".mn-local-skills/evidence_engine_skill/src/mn_evidence_engine_skill/__init__.py"
        in payloads
    )
    requirements = payloads["worker/docker_worker/__mn_skill_dependencies/requirements.txt"].decode()
    assert "mirrorneuron-rag-skill==1.2.14" in requirements
    assert "mirrorneuron-evidence-engine-skill" not in requirements


def test_prepare_manifest_stages_local_skill_dependencies_from_runtime_env(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    skills_root = tmp_path / "mn-skills"
    runtime_home = tmp_path / ".mn"
    bundle_dir.mkdir()
    runtime_home.mkdir()
    (runtime_home / "docker-compose.env").write_text("MN_ENV=dev\n", encoding="utf-8")
    monkeypatch.delenv("MN_ENV", raising=False)
    monkeypatch.setenv("MN_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))
    monkeypatch.setattr("mn_cli.libs.run_manifest.mn_home", lambda: runtime_home)
    _write_skill_pyproject(
        skills_root,
        "evidence_engine_skill",
        "mirrorneuron-evidence-engine-skill",
    )
    skill_module = skills_root / "evidence_engine_skill" / "src" / "mn_evidence_engine_skill"
    skill_module.mkdir(parents=True)
    (skill_module / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")

    manifest = {
        "skill_dependencies": [
            {
                "type": "pip",
                "source": "gar",
                "name": "mirrorneuron-evidence-engine-skill",
                "version": "1.2.7",
            }
        ],
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.DockerWorker",
                    "docker_worker_image": "worker/docker_worker",
                    "environment": {},
                },
            }
        ],
    }
    payloads = {"worker/docker_worker/Dockerfile": b"FROM python:3.11-slim\n"}

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)
    assert prepared["skill_dependencies"] == []

    staged = stage_skill_dependency_payloads_for_manifest(
        prepared,
        payloads,
        bundle_dir=bundle_dir,
    )

    assert ".mn-local-skills/evidence_engine_skill" in staged["sources"]
    assert (
        ".mn-local-skills/evidence_engine_skill/src/mn_evidence_engine_skill/__init__.py"
        in payloads
    )


def test_prepare_manifest_keeps_gar_skill_dependencies_for_hostlocal_dev(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    skills_root = tmp_path / "mn-skills"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"identity": {"blueprint_id": "hostlocal_dev"}}),
        encoding="utf-8",
    )
    _write_skill_pyproject(
        skills_root,
        "evidence_engine_skill",
        "mirrorneuron-evidence-engine-skill",
    )
    monkeypatch.setenv("MN_ENV", "dev")
    monkeypatch.setenv("MN_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))

    manifest = {
        "skill_dependencies": [
            {
                "type": "pip",
                "source": "gar",
                "name": "mirrorneuron-evidence-engine-skill",
                "version": "1.2.7",
            }
        ],
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "command": ["python3.11", "run.py"],
                    "environment": {},
                },
            }
        ],
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    assert prepared["skill_dependencies"] == manifest["skill_dependencies"]
    assert "mn_local_skill_dependencies" not in prepared.get("metadata", {})
    packages = prepared["nodes"][0]["config"]["python_environment"]["packages"]
    assert "mirrorneuron-evidence-engine-skill==1.2.7" in packages


def test_prepare_manifest_gar_skill_runtime_uses_pinned_requirements_not_local_sources(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    skills_root = tmp_path / "mn-skills"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    _write_skill_pyproject(
        skills_root,
        "w3m_browser_skill",
        "mirrorneuron-w3m-browser-skill",
        runtime=True,
    )
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))

    (bundle_dir / "config" / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "gar_runtime_bp"},
                "python_dependencies": {
                    "packages": [
                        "mirrorneuron-w3m-browser-skill",
                        "example-external>=1",
                    ],
                },
                "input_skills": {
                    "w3m_browser": {
                        "skill": "w3m_browser_skill",
                        "package": "mirrorneuron-w3m-browser-skill",
                        "install_policy": "python_environment_pip",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    manifest = {
        "skill_dependencies": [
            {
                "type": "pip",
                "source": "gar",
                "name": "mirrorneuron-w3m-browser-skill",
                "version": "1.2.7",
            }
        ],
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "upload_path": "worker",
                    "workdir": "/sandbox/job/worker",
                    "command": ["python3", "run.py"],
                    "environment": {},
                },
            }
        ],
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)
    node_config = prepared["nodes"][0]["config"]
    runtime = prepared["metadata"]["mn_skill_runtime"]

    assert node_config["runner_module"] == "MirrorNeuron.Runner.DockerWorker"
    assert node_config.get("build_context_upload_paths") in (None, [])
    assert runtime["local_packages"] == []
    assert "mirrorneuron-w3m-browser-skill==1.2.7" in runtime["requirements_text"]
    assert "example-external>=1" in runtime["requirements_text"]

    payloads: dict[str, bytes] = {}
    stage_skill_runtime_support_payloads_for_manifest(prepared, payloads, bundle_dir=bundle_dir)
    dockerfile = payloads["__mn_skill_runtime/docker_worker/Dockerfile"].decode()
    requirements = payloads["__mn_skill_runtime/docker_worker/requirements.txt"].decode()
    assert "COPY build_context/w3m_browser_skill" not in dockerfile
    assert "/tmp/mn-local-packages" not in dockerfile
    assert "mirrorneuron-w3m-browser-skill==1.2.7" in requirements


def test_stage_skill_dependency_payloads_injects_pinned_gar_requirements_for_dockerworker(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    manifest = {
        "skill_dependencies": [
            {
                "type": "pip",
                "source": "gar",
                "name": "mirrorneuron-rag-skill",
                "version": "1.2.7",
            }
        ],
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.DockerWorker",
                    "docker_worker_image": "worker/docker_worker",
                },
            }
        ],
    }
    payloads = {"worker/docker_worker/Dockerfile": b"FROM python:3.11-slim\n"}

    staged = stage_skill_dependency_payloads_for_manifest(
        manifest,
        payloads,
        bundle_dir=bundle_dir,
    )

    assert staged["staged"] is True
    requirements = payloads["worker/docker_worker/__mn_skill_dependencies/requirements.txt"].decode()
    dockerfile = payloads["worker/docker_worker/Dockerfile"].decode()
    assert "mirrorneuron-rag-skill==1.2.7" in requirements
    assert "https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/" in requirements
    assert "--index-url\n" not in requirements
    assert "--index-url https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/" in requirements
    assert "--extra-index-url https://pypi.org/simple" in requirements
    assert "COPY __mn_skill_dependencies/requirements.txt" in dockerfile
    assert "pip install --break-system-packages --no-cache-dir -r /tmp/mn-skill-dependencies/requirements.txt" in dockerfile


def test_prepare_manifest_adds_sdk_upload_for_manual_blueprint_support_worker(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    skills_root = tmp_path / "mn-skills"
    sdk_root = tmp_path / "mn-python-sdk"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    sdk_root.mkdir()
    (sdk_root / "pyproject.toml").write_text("[project]\nname='mirrorneuron-python-sdk'\n", encoding="utf-8")
    _write_skill_pyproject(
        skills_root,
        "blueprint_support_skill",
        "mirrorneuron-blueprint-support-skill",
    )
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))
    monkeypatch.setenv("MN_WORKSPACE_ROOT", str(tmp_path))
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"identity": {"blueprint_id": "manual_support"}}),
        encoding="utf-8",
    )
    manifest = {
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.DockerWorker",
                    "build_context_upload_paths": [
                        {
                            "base": "skills_root",
                            "source": "blueprint_support_skill",
                            "target": "document_workflow/docker_worker/build_context/blueprint_support_skill",
                        }
                    ],
                },
            }
        ]
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)
    uploads = prepared["nodes"][0]["config"]["build_context_upload_paths"]

    assert {
        "base": "workspace_root",
        "source": "mn-python-sdk",
        "target": "document_workflow/docker_worker/build_context/mn-python-sdk",
    } in uploads
    assert ensure_blueprint_support_sdk_build_context_uploads(prepared)["added"] == 0


def test_prepare_manifest_skill_runtime_node_scope_patches_only_selected_nodes(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    skills_root = tmp_path / "mn-skills"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    _write_skill_pyproject(
        skills_root,
        "w3m_browser_skill",
        "mirrorneuron-w3m-browser-skill",
        runtime=True,
    )
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "scoped_bp"},
                "skill_runtime": {
                    "auto_patch": True,
                    "node_scope": "public_research_workers",
                    "node_scopes": {"public_research_workers": ["research_worker"]},
                },
                "input_skills": {
                    "w3m_browser": {
                        "skill": "w3m_browser_skill",
                        "package": "mirrorneuron-w3m-browser-skill",
                        "install_policy": "python_environment_pip",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    manifest = {
        "agents": {
            "nodes": [
                {
                    "node_id": "research_worker",
                    "config": {
                        "runner_module": "MirrorNeuron.Runner.HostLocal",
                        "command": ["python3", "research.py"],
                        "environment": {},
                    },
                },
                {
                    "node_id": "local_worker",
                    "config": {
                        "runner_module": "MirrorNeuron.Runner.HostLocal",
                        "command": ["python3", "local.py"],
                        "environment": {},
                    },
                },
            ]
        }
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)
    nodes = {node["node_id"]: node for node in prepared["agents"]["nodes"]}

    assert nodes["research_worker"]["config"]["runner_module"] == "MirrorNeuron.Runner.DockerWorker"
    assert nodes["local_worker"]["config"]["runner_module"] == "MirrorNeuron.Runner.HostLocal"
    assert prepared["metadata"]["mn_skill_runtime"]["patched_nodes"] == ["research_worker"]


def test_stage_upload_path_payloads_stages_top_level_blueprint_sources(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    payload_dir = bundle_dir / "payloads" / "worker"
    payload_dir.mkdir(parents=True)
    (payload_dir / "run.py").write_text("print('hi')\n", encoding="utf-8")
    sample_dir = bundle_dir / "examples" / "sample_inputs" / "company"
    sample_dir.mkdir(parents=True)
    (sample_dir / "brief.txt").write_text("sample evidence\n", encoding="utf-8")
    cache_dir = sample_dir / "__pycache__"
    cache_dir.mkdir()
    (cache_dir / "brief.pyc").write_bytes(b"cache")

    manifest = {
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "upload_paths": [
                        {"source": "worker", "target": "worker"},
                        {"source": "examples/sample_inputs", "target": "vc_assistant/examples/sample_inputs"},
                    ]
                },
            }
        ]
    }
    payloads = {"worker/run.py": b"print('hi')\n"}

    staged = stage_upload_path_payloads_for_manifest(manifest, payloads, bundle_dir=bundle_dir)

    assert staged == {"staged": True, "sources": ["examples/sample_inputs"]}
    assert payloads["examples/sample_inputs/company/brief.txt"] == b"sample evidence\n"
    assert "examples/sample_inputs/company/__pycache__/brief.pyc" not in payloads


def test_prepare_manifest_leaves_manual_docker_worker_skill_policy_alone(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    skills_root = tmp_path / "mn-skills"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    _write_skill_pyproject(
        skills_root,
        "w3m_browser_skill",
        "mirrorneuron-w3m-browser-skill",
        runtime=True,
    )
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "manual_bp"},
                "input_skills": {
                    "w3m_browser": {
                        "skill": "w3m_browser_skill",
                        "package": "mirrorneuron-w3m-browser-skill",
                        "install_policy": "docker_worker_image",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "nodes": [
            {
                "node_id": "worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "command": ["python3.11", "run.py"],
                    "environment": {},
                },
            }
        ]
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    assert prepared["nodes"][0]["config"]["runner_module"] == "MirrorNeuron.Runner.HostLocal"
    assert "mn_skill_runtime" not in prepared.get("metadata", {})


def test_prepare_manifest_for_submission_lowers_workflow_manifest_for_core_runtime(tmp_path):
    bundle_dir = tmp_path / "workflow_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    (bundle_dir / "config" / "default.json").write_text(
        json.dumps({"identity": {"blueprint_id": "workflow_bp"}})
    )

    manifest = {
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": "workflow_bp",
        "name": "Workflow BP",
        "manifest_version": "1.0",
        "job_name": "workflow-bp",
        "workflow": {
            "schema": "mn.workflow.problem_graph/v1",
            "workflow_id": "workflow_bp_v1",
            "entrypoint": "load_inputs",
            "source": "load_inputs",
            "sink": "finish",
            "steps": [
                {"id": "load_inputs", "run": "load_inputs"},
                {"id": "finish", "run": "finish"},
            ],
            "edges": [{"id": "load_to_finish", "from": "load_inputs", "to": "finish"}],
        },
        "agents": {
            "schema": "mn.agents.communication_graph/v1",
            "entrypoints": ["ingress"],
            "nodes": [
                {"node_id": "ingress", "agent_type": "router", "type": "map", "config": {}},
                {"node_id": "worker", "agent_type": "executor", "type": "generic", "config": {}},
            ],
            "edges": [
                {
                    "edge_id": "ingress_to_worker",
                    "from_node": "ingress",
                    "to_node": "worker",
                    "message_type": "start",
                }
            ],
        },
        "runtime": {
            "bindings": {
                "load_inputs": {
                    "workers": [{"id": "ingress", "role": "Ingress"}],
                    "seed_inputs": {"ingress": [{"hello": "world"}]},
                },
                "finish": {"workers": [{"id": "worker", "role": "Worker"}]},
            }
        },
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    assert "graph_id" not in manifest
    assert prepared["graph_id"] == "workflow_bp_v1"
    assert prepared["flow"]["nodes"] == prepared["agents"]["nodes"]
    assert prepared["flow"]["edges"] == prepared["agents"]["edges"]
    assert prepared["entrypoints"] == ["ingress"]
    assert prepared["initial_inputs"]["ingress"] == [{"hello": "world"}]
    assert "nodes" not in prepared
    assert "edges" not in prepared


def test_prepare_manifest_for_submission_lowers_legacy_agent_graph_workflow_id(tmp_path):
    bundle_dir = tmp_path / "legacy_agent_graph"
    bundle_dir.mkdir()

    manifest = {
        "manifest_version": "1.0",
        "workflow_id": "legacy_agent_graph_v1",
        "job_name": "legacy-agent-graph",
        "agents": {
            "entrypoints": ["video_understanding_agent"],
            "nodes": [
                {
                    "node_id": "video_understanding_agent",
                    "agent_type": "executor",
                    "type": "generic",
                    "config": {},
                }
            ],
            "edges": [],
        },
        "runtime": {"models": {}},
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    assert prepared["graph_id"] == "legacy_agent_graph_v1"
    assert prepared["flow"]["nodes"] == prepared["agents"]["nodes"]
    assert prepared["flow"]["edges"] == []
    assert prepared["entrypoints"] == ["video_understanding_agent"]


def test_stage_blueprint_support_payloads_for_support_dependent_hostlocal_worker(tmp_path, monkeypatch):
    skills_root = workspace_root() / "mn-skills"
    if not (skills_root / "blueprint_support_skill" / "src").is_dir():
        pytest.skip("blueprint support skill source is not checked out")

    bundle_dir = tmp_path / "bundle"
    script_dir = bundle_dir / "payloads" / "simulation_loop" / "scripts"
    config_dir = bundle_dir / "config"
    script_dir.mkdir(parents=True)
    config_dir.mkdir()
    (script_dir / "run_blueprint.py").write_text(
        "from mn_blueprint_support import run_blueprint_cli\n",
        encoding="utf-8",
    )
    (config_dir / "default.json").write_text('{"identity": {"blueprint_id": "bp"}}\n')
    manifest = {
        "agents": {
            "nodes": [
                {
                    "node_id": "worker",
                    "config": {
                        "runner_module": "MirrorNeuron.Runner.HostLocal",
                        "upload_path": "simulation_loop",
                    },
                }
            ]
        }
    }
    payloads = {
        "simulation_loop/scripts/run_blueprint.py": b"from mn_blueprint_support import run_blueprint_cli\n"
    }
    monkeypatch.setenv("MN_SKILLS_ROOT", str(skills_root))
    monkeypatch.chdir(tmp_path)

    summary = stage_blueprint_support_payloads_for_manifest(
        manifest,
        payloads,
        bundle_dir=Path("bundle"),
    )

    assert summary == {"staged": True, "sources": ["simulation_loop"]}
    assert "simulation_loop/mn_blueprint_support/__init__.py" in payloads
    assert "simulation_loop/scripts/mn_blueprint_support/__init__.py" in payloads
    assert (
        "simulation_loop/scripts/litellm_communicate_skill/src/"
        "mn_litellm_communicate_skill/__init__.py"
    ) in payloads
    assert payloads["simulation_loop/config/default.json"] == b'{"identity": {"blueprint_id": "bp"}}\n'


def test_promote_large_payloads_to_blob_refs(tmp_path, monkeypatch):
    blob_root = tmp_path / "blobs"
    monkeypatch.setenv("MN_HOST_BLOB_STORE_DIR", str(blob_root))
    monkeypatch.setenv("MN_INLINE_PAYLOAD_MAX_BYTES", "5")
    monkeypatch.setenv("MN_ARTIFACT_ADVERTISE_URL", "http://node-a:55660")
    monkeypatch.setenv("MN_NODE_NAME", "node-a@lab")

    manifest = {"metadata": {}}
    payloads = {
        "small.txt": b"12345",
        "media/demo.mp4": b"123456",
    }

    promoted = promote_large_payloads_to_blob_refs(manifest, payloads)

    assert "small.txt" in payloads
    assert "media/demo.mp4" not in payloads
    assert len(promoted) == 1
    ref = manifest["metadata"]["mn_artifacts"]["blob_refs"][0]
    assert ref["type"] == "blob_ref"
    assert ref["payload_path"] == "media/demo.mp4"
    assert ref["size_bytes"] == 6
    assert ref["locations"][0]["node"] == "node-a@lab"
    assert ref["locations"][0]["url"].endswith(f"/blobs/{ref['sha256']}")
    assert (blob_root / ref["sha256"][:2] / ref["sha256"]).read_bytes() == b"123456"


def test_promote_large_payloads_can_be_disabled(monkeypatch):
    monkeypatch.setenv("MN_INLINE_PAYLOAD_MAX_BYTES", "-1")
    manifest = {}
    payloads = {"media/demo.mp4": b"123456"}

    promoted = promote_large_payloads_to_blob_refs(manifest, payloads)

    assert promoted == []
    assert payloads == {"media/demo.mp4": b"123456"}
    assert "metadata" not in manifest


def test_promote_large_payloads_prefers_injected_runtime_env(tmp_path, monkeypatch):
    home = tmp_path / "mn-home"
    home.mkdir()
    (home / "docker-compose.env").write_text(
        "MN_ARTIFACT_ADVERTISE_URL=http://from-file:55660\n"
        "MN_NODE_NAME=file-node\n"
        f"MN_HOST_BLOB_STORE_DIR={tmp_path / 'file-blobs'}\n",
        encoding="utf-8",
    )
    blob_root = tmp_path / "runtime-blobs"
    monkeypatch.setenv("MN_HOME", str(home))
    monkeypatch.setenv("MN_INLINE_PAYLOAD_MAX_BYTES", "2")
    monkeypatch.setenv("MN_ARTIFACT_ADVERTISE_URL", "http://from-env:55660")
    monkeypatch.setenv("MN_NODE_NAME", "env-node")
    monkeypatch.setenv("MN_HOST_BLOB_STORE_DIR", str(tmp_path / "env-blobs"))

    manifest = {}
    payloads = {"docs/report.txt": b"large"}

    promoted = promote_large_payloads_to_blob_refs(
        manifest,
        payloads,
        runtime_env={
            "MN_ARTIFACT_ADVERTISE_URL": "http://from-runtime:55660",
            "MN_NODE_NAME": "runtime-node",
            "MN_HOST_BLOB_STORE_DIR": str(blob_root),
        },
    )

    assert payloads == {}
    assert len(promoted) == 1
    ref = manifest["metadata"]["mn_artifacts"]["blob_refs"][0]
    assert ref["locations"][0]["url"].startswith("http://from-runtime:55660/blobs/")
    assert ref["locations"][0]["node"] == "runtime-node"
    assert (blob_root / ref["sha256"][:2] / ref["sha256"]).read_bytes() == b"large"


def test_prepare_manifest_injects_docker_model_runner_llm_env_by_node_runtime(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "llm": {
                    "enabled": True,
                    "default_config": "primary",
                    "configs": {
                        "primary": {
                            "provider": "docker_model_runner",
                            "mode": "openai_compatible",
                            "runtime_model": "gemma4:e2b",
                            "model": "gemma4:e2b",
                            "api_base": "auto",
                            "backend": "llama.cpp",
                            "context_size": 4096,
                            "timeout_seconds": 60,
                            "max_tokens": 800,
                        }
                    },
                }
            }
        )
    )
    manifest = {
        "nodes": [
            {
                "node_id": "host_worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "environment": {},
                },
            },
            {
                "node_id": "sandbox_worker",
                "config": {"environment": {}},
            },
        ]
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    host_env = prepared["nodes"][0]["config"]["environment"]
    sandbox_env = prepared["nodes"][1]["config"]["environment"]
    assert host_env["MN_LLM_PROVIDER"] == "docker_model_runner"
    assert host_env["MN_LLM_MODEL"] == "ai/gemma4:E2B"
    assert host_env["MN_LLM_RUNTIME_MODEL"] == "ai/gemma4:E2B"
    assert host_env["MN_LLM_API_BASE"] == "http://model-runner.docker.internal/engines/v1"
    assert sandbox_env["MN_LLM_API_BASE"] == "http://model-runner.docker.internal/engines/v1"
    assert host_env["MN_LLM_CONTEXT_SIZE"] == "4096"
    assert host_env["MN_LLM_MAX_TOKENS"] == "800"


def test_prepare_manifest_strips_docker_model_runner_scheduler_requirement_for_http_llm(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "llm": {
                    "enabled": True,
                    "default_config": "primary",
                    "configs": {
                        "primary": {
                            "provider": "docker_model_runner",
                            "mode": "openai_compatible",
                            "runtime_model": "gemma4:e2b",
                            "model": "gemma4:e2b",
                            "api_base": "http://host.docker.internal:12434/engines/v1",
                            "backend": "llama.cpp",
                        }
                    },
                }
            }
        )
    )
    manifest = {
        "required_services": [
            {"name": "docker-model-runner", "model": "gemma4:e2b"},
            {"name": "vector-db"},
        ],
        "runtime": {
            "models": {
                "primary": {"model": "default"},
                "service_model": {"provider": "nvidia_service", "model": "remote/model"},
            },
            "bindings": {
                "startup_folder_watcher": {
                    "workers": [
                        {"id": "startup_folder_watcher", "model": "default"},
                    ]
                }
            },
        },
        "nodes": [
            {
                "node_id": "startup_folder_watcher",
                "requires_services": [
                    {
                        "name": "docker-model-runner",
                        "model": "gemma4:e2b",
                        "resources": {"gpu": {"min_vram_mb": 8192}},
                    },
                    {"name": "redis"},
                ],
                "config": {"environment": {}},
            }
        ],
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    assert prepared["required_services"] == [{"name": "vector-db"}]
    assert prepared["runtime"]["models"] == {"service_model": {"provider": "nvidia_service", "model": "remote/model"}}
    assert "model" not in prepared["runtime"]["bindings"]["startup_folder_watcher"]["workers"][0]
    assert prepared["nodes"][0]["requires_services"] == [{"name": "redis"}]
    env = prepared["nodes"][0]["config"]["environment"]
    assert env["MN_LLM_PROVIDER"] == "docker_model_runner"
    assert env["MN_LLM_API_BASE"] == "http://host.docker.internal:12434/engines/v1"


def test_prepare_manifest_strips_docker_model_runner_scheduler_requirement_for_fake_llm(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "llm": {
                    "enabled": True,
                    "default_config": "primary",
                    "configs": {
                        "primary": {
                            "provider": "docker_model_runner",
                            "runtime_model": "gemma4:e2b",
                            "model": "gemma4:e2b",
                        }
                    },
                }
            }
        )
    )
    manifest = {
        "required_services": [
            {"name": "docker-model-runner", "model": "gemma4:e2b"},
            {"name": "redis"},
        ],
        "runtime": {
            "models": {"primary": {"provider": "docker_model_runner", "model": "gemma4:e2b"}},
            "bindings": {"worker": {"workers": [{"id": "worker", "model": "gemma4:e2b"}]}},
        },
        "nodes": [
            {
                "node_id": "worker",
                "requires_services": [{"name": "docker-model-runner", "model": "gemma4:e2b"}],
                "config": {"environment": {}},
            }
        ],
    }

    prepared = prepare_manifest_for_submission(
        bundle_dir,
        manifest,
        env_overrides={"MN_LLM_PROVIDER": "fake", "MN_LLM_MODEL": "fake-deterministic-blueprint-agent"},
        submission_metadata={"fake_llm": True},
    )

    assert prepared["required_services"] == [{"name": "redis"}]
    assert "models" not in prepared["runtime"]
    assert "model" not in prepared["runtime"]["bindings"]["worker"]["workers"][0]
    assert "requires_services" not in prepared["nodes"][0]
    assert prepared["nodes"][0]["config"]["environment"]["MN_LLM_PROVIDER"] == "fake"


def test_prepare_manifest_model_only_llm_config_does_not_request_scheduler_model(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps({"llm": {"enabled": True, "model": "default"}})
    )
    manifest = {
        "nodes": [
            {
                "node_id": "support_worker",
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "environment": {},
                },
            }
        ]
    }

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)

    env = prepared["nodes"][0]["config"]["environment"]
    assert env["MN_LLM_PROVIDER"] == "docker_model_runner"
    assert env["MN_LLM_MODEL"] == "ai/gemma4:E2B"
    assert "MN_LLM_RUNTIME_MODEL" not in env


def test_prepare_manifest_for_submission_injects_flow_nodes_and_scheduler_binding(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "safety_video_analyser"},
                "llm": {"provider": "docker_model_runner", "model": "gemma4:e2b"},
                "node_allocation": {
                    "report_generator_preferred_node": "mirror_neuron@192.168.4.173"
                },
                "manifest_config_bindings": [
                    {
                        "config_path": "node_allocation.report_generator_preferred_node",
                        "manifest_path": "agents.nodes.report_generator.policies.scheduler.preferred_node",
                    }
                ],
            }
        )
    )
    manifest = {
        "manifest_version": "1.0",
        "workflow_id": "agent-node-env",
        "agents": {
            "nodes": [
                {
                    "node_id": "video_understanding_agent",
                    "config": {
                        "environment": {
                            "MN_LLM_MODEL": "hf.co/nvidia/Nemotron-3-Nano-Omni-30B-A3B-Reasoning-BF16"
                        }
                    },
                },
                {
                    "node_id": "report_generator",
                    "config": {"environment": {}},
                    "policies": {"scheduler": {}},
                },
            ],
            "edges": [],
        },
    }

    prepared = prepare_manifest_for_submission(
        bundle_dir,
        manifest,
        env_overrides={"MN_RUN_ID": "safety-run"},
    )

    nodes = {node["node_id"]: node for node in prepared["agents"]["nodes"]}
    video_env = nodes["video_understanding_agent"]["config"]["environment"]
    report_env = nodes["report_generator"]["config"]["environment"]

    assert video_env["MN_RUN_ID"] == "safety-run"
    assert video_env["MN_LLM_MODEL"].startswith("hf.co/nvidia/")
    assert report_env["MN_LLM_MODEL"] == "ai/gemma4:E2B"
    assert (
        nodes["report_generator"]["policies"]["scheduler"]["preferred_node"]
        == "mirror_neuron@192.168.4.173"
    )
    assert "nodes" not in prepared


def test_flow_node_local_video_inputs_promote_to_blob_refs(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    videos = tmp_path / "videos"
    videos.mkdir()
    (videos / "work_safety_1.mp4").write_bytes(b"0123456789abcdef")
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "safety_video_analyser"},
                "video_inputs": {"folder_path": str(videos)},
                "inputs": {"payload": {"video_folder": str(videos)}},
                "local_inputs": {
                    "folders": [
                        {
                            "config_path": "video_inputs.folder_path",
                            "payload_path": "safety_video_analyser/mn_local_inputs/videos",
                            "runtime_path": "mn_local_inputs/videos",
                            "allowed_extensions": [".mp4"],
                            "linked_config_paths": ["inputs.payload.video_folder"],
                        }
                    ]
                },
            }
        )
    )
    manifest = {
        "manifest_version": "1.0",
        "workflow_id": "agent-video-blobs",
        "agents": {
            "nodes": [
                {
                    "node_id": "video_understanding_agent",
                    "config": {"environment": {}},
                }
            ],
            "edges": [],
        },
    }
    monkeypatch.setenv("MN_INLINE_PAYLOAD_MAX_BYTES", "5")
    monkeypatch.setenv("MN_HOST_BLOB_STORE_DIR", str(tmp_path / "blobs"))
    monkeypatch.setenv("MN_ARTIFACT_ADVERTISE_URL", "http://node-a:55660")
    monkeypatch.setenv("MN_NODE_NAME", "mirror_neuron@node-a")

    prepared = prepare_manifest_for_submission(bundle_dir, manifest)
    payloads = {}
    summary = stage_local_input_payloads_for_manifest(prepared, payloads, bundle_dir=bundle_dir)

    assert summary["folders"][0]["file_count"] == 1
    assert "safety_video_analyser/mn_local_inputs/videos/work_safety_1.mp4" in payloads

    promoted = promote_large_payloads_to_blob_refs(prepared, payloads)

    assert len(promoted) == 1
    assert payloads == {}
    ref = prepared["metadata"]["mn_artifacts"]["blob_refs"][0]
    assert ref["type"] == "blob_ref"
    assert ref["payload_path"] == "safety_video_analyser/mn_local_inputs/videos/work_safety_1.mp4"
    assert ref["locations"][0]["node"] == "mirror_neuron@node-a"


def test_stage_local_input_payloads_after_manifest_preparation(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    docs = tmp_path / "tax_docs"
    docs.mkdir()
    (docs / "w2.txt").write_text("wages 100\n", encoding="utf-8")
    (docs / "ignore.csv").write_text("skip\n", encoding="utf-8")
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "tax_documents": {"folder_path": ""},
                "inputs": {"payload": {"document_folder": ""}},
                "local_inputs": {
                    "folders": [
                        {
                            "config_path": "tax_documents.folder_path",
                            "payload_path": "tax_workflow/mn_local_inputs/tax_documents",
                            "runtime_path": "mn_local_inputs/tax_documents",
                            "allowed_extensions": [".txt"],
                            "linked_config_paths": ["inputs.payload.document_folder"],
                        }
                    ]
                },
            }
        )
    )
    manifest = {
        "nodes": [
            {
                "node_id": "document_intake_agent",
                "config": {"environment": {}},
            }
        ]
    }
    prepared = prepare_manifest_for_submission(
        bundle_dir,
        manifest,
        env_overrides={"MN_RUN_ID": "tax-run-cli"},
        config_overrides={"tax_documents": {"folder_path": str(docs)}},
    )
    payloads = {}

    summary = stage_local_input_payloads_for_manifest(prepared, payloads, bundle_dir=bundle_dir)

    env = prepared["nodes"][0]["config"]["environment"]
    injected_config = json.loads(env["MN_BLUEPRINT_CONFIG_JSON"])
    assert injected_config["tax_documents"]["folder_path"] == "mn_local_inputs/tax_documents"
    assert injected_config["inputs"]["payload"]["document_folder"] == "mn_local_inputs/tax_documents"
    assert payloads["tax_workflow/mn_local_inputs/tax_documents/w2.txt"] == b"wages 100\n"
    assert "tax_workflow/mn_local_inputs/tax_documents/ignore.csv" not in payloads
    assert summary["folders"][0]["file_count"] == 1


def test_prepare_manifest_for_submission_renders_agent_templates(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "config").mkdir()
    (bundle_dir / "config" / "default.json").write_text(json.dumps({"identity": {"blueprint_id": "bp"}}))

    agent_root = tmp_path / "mn-agents"
    agent_dir = agent_root / "control_router"
    agent_dir.mkdir(parents=True)
    (agent_root / "index.json").write_text(json.dumps({
        "agents": [
            {
                "template_id": "mn-agents.control_router",
                "version": 1,
                "path": "control_router",
                "template_category": "control",
            }
        ]
    }))
    (agent_dir / "agent.json").write_text(json.dumps({
        "template_id": "mn-agents.control_router",
        "version": 1,
        "defaults": {
            "agent_type": "router",
            "type": "map",
            "role": "coordinator",
            "emit_type": "start",
        },
        "inputs": {"required": []},
    }))
    monkeypatch.setenv("MN_AGENTS_ROOT", str(agent_root))

    prepared = prepare_manifest_for_submission(
        bundle_dir,
        {
            "nodes": [
                {
                    "node_id": "ingress",
                    "uses": "mn-agents.control_router@1",
                    "with": {"emit_type": "video_monitor_start"},
                }
            ]
        },
        env_overrides={"MN_RUN_ID": "run-template"},
    )

    node = prepared["nodes"][0]
    assert node["agent_type"] == "router"
    assert node["type"] == "map"
    assert "uses" not in node
    assert "with" not in node
    assert node["config"]["emit_type"] == "video_monitor_start"
    assert node["config"]["environment"]["MN_RUN_ID"] == "run-template"


def test_blueprint_config_ignores_misnamed_overwrite_file(tmp_path):
    bundle_dir = tmp_path / "bundle"
    config_dir = bundle_dir / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "default.json").write_text(json.dumps({"vl_model": {"model": "default"}}))
    (config_dir / "overwrites.json").write_text(json.dumps({"vl_model": {"model": "wrong-name"}}))

    config = load_blueprint_config(bundle_dir)

    assert config == {"vl_model": {"model": "default"}}


@pytest.mark.parametrize("payload", ["[]", "{bad json"])
def test_blueprint_config_rejects_invalid_overwrite_data_format(tmp_path, payload):
    bundle_dir = tmp_path / "bundle"
    config_dir = bundle_dir / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "default.json").write_text(json.dumps({"vl_model": {"model": "default"}}))
    (config_dir / "overwrite.json").write_text(payload)

    with pytest.raises((json.JSONDecodeError, ValueError)):
        load_blueprint_config(bundle_dir)


def test_manifest_config_bindings_ignore_wrong_names():
    manifest = {
        "nodes": [
            {
                "node_id": "worker",
                "config": {"environment": {"CUSTOM_MODEL": "keep"}},
            }
        ]
    }
    config = {
        "vl_model": {"model": "overwrite"},
        "manifest_config_bindings": [
            {
                "config_path": "vl_model.wrong_name",
                "manifest_path": "nodes.worker.config.environment.CUSTOM_MODEL",
            },
            {
                "config_path": "vl_model.model",
                "manifest_path": "nodes.missing_worker.config.environment.NEW_MODEL",
            },
        ],
    }

    apply_manifest_config_bindings(manifest, config)

    env = manifest["nodes"][0]["config"]["environment"]
    assert env == {"CUSTOM_MODEL": "keep"}


def test_job_log_writer_deduplicates_events_and_records_web_ui_once():
    writer = JobLogWriter(f"unit-run-helper-{uuid.uuid4().hex}")
    event = {
        "timestamp": "2026-05-01T00:00:00Z",
        "type": "custom",
        "payload": {"message_id": "m1", "web_ui": {"url": "http://localhost:1"}},
    }

    assert writer.write_event(event) is True
    assert writer.write_event(event) is False
    assert writer.record_web_ui_url(event) == "http://localhost:1"
    assert writer.record_web_ui_url(event) is None


def test_job_log_writer_loads_existing_run_events(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    event = {
        "timestamp": "2026-05-14T00:00:00Z",
        "type": "door_camera_frame_tick_generated",
        "agent_id": "door_camera_tick_source",
        "payload": {"tick_seq": 1},
    }
    (run_dir / "events.jsonl").write_text(json.dumps(event) + "\n")

    writer = JobLogWriter("unit-existing-events", run_dir=run_dir)

    assert writer.write_event(event) is False


def test_materialize_sent_email_copy_uses_safe_host_paths(tmp_path):
    materialize_sent_email_copy(
        tmp_path,
        {
            "provider_id": "id/with spaces",
            "sent_email_copy": {
                "html_path": "../unsafe.html",
                "text_content": "plain",
                "html_content": "<p>Hello</p>",
                "metadata": {"provider": "test"},
            },
        },
    )

    email_dir = tmp_path / "sent_emails"
    assert (email_dir / "unsafe.html").read_text() == "<p>Hello</p>"
    metadata = json.loads((email_dir / "id-with-spaces.json").read_text())
    assert metadata["provider"] == "test"
    assert PathLikeName(metadata["host_html_path"]) == "unsafe.html"


def PathLikeName(path: str) -> str:
    return path.rsplit("/", 1)[-1]
