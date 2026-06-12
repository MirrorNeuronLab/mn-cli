import json
import importlib.util
import uuid
from pathlib import Path

import pytest

from mn_cli.libs.run_logs import JobLogWriter, materialize_sent_email_copy
from mn_cli.libs.artifacts import promote_large_payloads_to_blob_refs
from mn_cli.libs.run_manifest import (
    apply_manifest_config_bindings,
    load_blueprint_config,
    prepare_manifest_for_submission,
    stage_blueprint_support_payloads_for_manifest,
    stage_local_input_payloads_for_manifest,
)

requires_blueprint_support = pytest.mark.skipif(
    importlib.util.find_spec("mn_blueprint_support") is None,
    reason="mn_blueprint_support is not installed",
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
    bundle_dir = tmp_path / "bundle"
    script_dir = bundle_dir / "payloads" / "simulation_loop" / "scripts"
    script_dir.mkdir(parents=True)
    (script_dir / "run_blueprint.py").write_text(
        "from mn_blueprint_support import run_blueprint_cli\n",
        encoding="utf-8",
    )
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
    monkeypatch.chdir(tmp_path)

    summary = stage_blueprint_support_payloads_for_manifest(
        manifest,
        payloads,
        bundle_dir=Path("bundle"),
    )

    assert summary == {"staged": True, "sources": ["simulation_loop"]}
    assert "simulation_loop/mn_blueprint_support/__init__.py" in payloads
    assert "simulation_loop/scripts/mn_blueprint_support/__init__.py" in payloads


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
    assert host_env["MN_LLM_API_BASE"] == "http://localhost:12434/engines/v1"
    assert sandbox_env["MN_LLM_API_BASE"] == "http://model-runner.docker.internal/engines/v1"
    assert host_env["MN_LLM_CONTEXT_SIZE"] == "4096"
    assert host_env["MN_LLM_MAX_TOKENS"] == "800"


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


@requires_blueprint_support
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


@requires_blueprint_support
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


@requires_blueprint_support
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


@requires_blueprint_support
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
                "version": "1.0.0",
                "path": "control_router",
                "template_category": "control",
            }
        ]
    }))
    (agent_dir / "agent.json").write_text(json.dumps({
        "template_id": "mn-agents.control_router",
        "version": "1.0.0",
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
                    "uses": "mn-agents.control_router@1.0.0",
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
