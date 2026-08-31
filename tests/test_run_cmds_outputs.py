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
from mn_sdk import (
    AgentProgress,
    load_model_ownership,
    load_model_remotes,
    upsert_model_remote,
)
from rich.console import Console
from typer.testing import CliRunner
from v1_manifests import workflow_manifest

from mn_cli.libs import model_cmds, run_cmds
from mn_cli.libs.run_manifest import prepare_manifest_for_submission
from mn_cli.libs.ui import JobMonitorState, generate_live_layout
from mn_cli.libs.workflow_progress import (
    BlueprintWorkflowProgress,
    _agent_progress_detail,
)
from mn_cli.main import app

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
        lambda **_kwargs: {
            "status": "running",
            "api_base": "http://mn-litellm-proxy:4000/v1",
        },
    )


def test_background_event_relay_is_unbounded_unless_explicitly_configured(
    monkeypatch,
):
    from mn_cli.libs.run_cmds import web_ui

    monkeypatch.delenv("MN_RUN_EVENT_RELAY_MAX_SECONDS", raising=False)
    assert (
        web_ui._background_event_relay_max_seconds(
            {"budgets": {"max_stream_duration_seconds": 10}}
        )
        is None
    )

    monkeypatch.setenv("MN_RUN_EVENT_RELAY_MAX_SECONDS", "45")
    assert web_ui._background_event_relay_max_seconds({}) == 45.0


def test_run_injects_user_home_output_environment(mocker, tmp_path, monkeypatch):
    home_dir = tmp_path / "home"
    output_home = tmp_path / "outputs-home"
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("MN_OUTPUT_HOME", str(output_home))
    mock_submit = mocker.patch(
        "mn_cli.libs.run_cmds.client.create_job", return_value=json.dumps({"job_id": "job-123"})
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.client.stream_events",
        return_value=[json.dumps({"type": "job_completed"})],
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            workflow_manifest({
                "nodes": [
                    {
                        "node_id": "worker",
                        "config": {"environment": {}},
                    }
                ]
            })
        )
    )

    result = runner.invoke(app, ["blueprint", "run", str(bundle_dir)])

    assert result.exit_code == 0
    manifest = json.loads(mock_submit.call_args.args[0])
    env = manifest["agents"]["nodes"][0]["config"]["environment"]
    assert env["MN_OUTPUT_HOME"] == str(output_home)
    assert env["MN_USER_HOME"] == str(home_dir)
    assert env["OTTERDESK_USER_HOME"] == str(home_dir)


def test_run_materializes_vc_final_artifact_outputs(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    output_dir = tmp_path / "vc-output"
    mocker.patch("mn_cli.libs.run_cmds.client.create_job", return_value=json.dumps({"job_id": "job-123"}))
    mocker.patch(
        "mn_cli.libs.run_cmds.client.stream_events",
        return_value=[
            json.dumps(
                {
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
                                            "missing_evidence": [],
                                        }
                                    },
                                }
                            ],
                            "action_ledger": {
                                "budget": 100,
                                "used": 24,
                                "remaining": 76,
                            },
                            "artifact_quality": {
                                "status": "warning",
                                "passes_required_gate": True,
                            },
                            "run_health": {
                                "status": "warning",
                                "warning_count": 1,
                                "failure_count": 0,
                            },
                        }
                    },
                }
            )
        ],
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            workflow_manifest({
                "nodes": [
                    {
                        "node_id": "worker",
                        "config": {"environment": {}},
                    }
                ]
            })
        )
    )
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            workflow_manifest({
                "identity": {"blueprint_id": "vc_assistant", "name": "VC Assistant"},
                "inputs": {"payload": {"output_folder": str(output_dir)}},
                "outputs": {"folder_path": str(output_dir)},
            })
        )
    )

    result = runner.invoke(app, ["blueprint", "run", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Materialized blueprint outputs" in result.stdout
    assert (
        json.loads((output_dir / "company_index.json").read_text())["companies"][0][
            "company_slug"
        ]
        == "aurora-ai"
    )
    assert (
        "Berkus method score" in (output_dir / "aurora-ai" / "analysis.md").read_text()
    )
    assert (
        json.loads((output_dir / "final_artifact.json").read_text())["action_ledger"][
            "budget"
        ]
        == 100
    )
    assert json.loads((output_dir / "action_ledger.json").read_text())["used"] == 24
    assert (
        json.loads((output_dir / "artifact_quality.json").read_text())["status"]
        == "warning"
    )
    assert (
        json.loads((output_dir / "run_health.json").read_text())["warning_count"] == 1
    )


def test_run_materializes_deeply_nested_hostlocal_vc_artifact(
    mocker, tmp_path, monkeypatch
):
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
    nested_result = {
        "sandbox": {"logs": json.dumps({"final_artifact": final_artifact})}
    }
    for index in range(25):
        nested_result = {
            "agent_id": f"agent-{index}",
            "input": nested_result,
            "sandbox": {"logs": json.dumps({"status": "completed"})},
        }

    mocker.patch("mn_cli.libs.run_cmds.client.create_job", return_value=json.dumps({"job_id": "job-123"}))
    mocker.patch(
        "mn_cli.libs.run_cmds.client.stream_events",
        return_value=[
            json.dumps(
                {
                    "type": "job_completed",
                    "result": {"last_message": nested_result},
                }
            )
        ],
    )

    bundle_dir = tmp_path / "run_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            workflow_manifest({
                "nodes": [
                    {
                        "node_id": "worker",
                        "config": {"environment": {}},
                    }
                ]
            })
        )
    )
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "vc_assistant", "name": "VC Assistant"},
                "outputs": {"folder_path": str(output_dir)},
            }
        )
    )

    result = runner.invoke(app, ["blueprint", "run", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Materialized blueprint outputs" in result.stdout
    assert (
        json.loads((output_dir / "company_index.json").read_text())["companies"][0][
            "company_slug"
        ]
        == "boreal-robotics"
    )
    assert (
        "Replacement cost reflects prototype"
        in (output_dir / "boreal-robotics" / "analysis.md").read_text()
    )
    assert (
        json.loads((output_dir / "final_artifact.json").read_text())["action_ledger"][
            "used"
        ]
        == 21
    )
    assert json.loads((output_dir / "action_ledger.json").read_text())["used"] == 21
    assert (
        json.loads((output_dir / "artifact_quality.json").read_text())["status"] == "ok"
    )
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


def test_resolve_job_result_reads_staged_snapshot(monkeypatch):
    reference = {
        "type": "artifact_ref",
        "version": "mn.staged_artifact/v1",
        "storage": "syncthing",
        "kind": "job_result",
        "submission_id": "submission-1",
        "run_id": "run-1",
        "relative_path": "outputs/runs/run-1/artifacts/aa/result.json",
        "content_type": "application/json",
        "size_bytes": 11,
        "sha256": "a" * 64,
    }
    captured = {}

    class RuntimeConfig:
        shared_storage_root = "/tmp/mn-shared"

    def resolve(selected, **kwargs):
        captured.update(kwargs)
        return {"final_artifact": {"ok": selected == reference}}

    monkeypatch.delenv("MN_HOST_SHARED_STORAGE_ROOT", raising=False)
    monkeypatch.setattr(run_cmds.RuntimeConfig, "from_env", lambda: RuntimeConfig())
    monkeypatch.setattr(run_cmds, "resolve_json_reference", resolve)

    assert run_cmds._resolve_job_result({"result_ref": reference}) == {
        "final_artifact": {"ok": True}
    }
    assert captured["env"]["MN_HOST_SHARED_STORAGE_ROOT"] == "/tmp/mn-shared"


def test_fetch_and_save_results_preserves_failed_worker_payload(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        run_cmds,
        "_resolve_job_result",
        lambda _job: {"status": "failed", "error": "worker failed"},
    )
    monkeypatch.setattr(run_cmds.client, "stream_events", lambda *_args, **_kwargs: [])
    output = tmp_path / "result"

    run_cmds.fetch_and_save_results(
        "job-failed",
        data={"status": "failed", "result_ref": {"kind": "job_result"}},
        output_dir=output,
    )

    assert json.loads((output / "result.txt").read_text(encoding="utf-8")) == {
        "status": "failed",
        "error": "worker failed",
    }


def test_copy_shared_submission_outputs_materializes_worker_result_files(monkeypatch, tmp_path):
    shared_root = tmp_path / "shared"
    source = shared_root / "submissions" / "submission-1" / "outputs" / "user"
    source.mkdir(parents=True)
    (source / "final_artifact.json").write_text('{"ok": true}\n', encoding="utf-8")
    (source / "company" / "analysis.md").parent.mkdir()
    (source / "company" / "analysis.md").write_text("# Analysis\n", encoding="utf-8")

    class RuntimeConfig:
        shared_storage_root = str(shared_root)

    monkeypatch.setattr(run_cmds.RuntimeConfig, "from_env", lambda: RuntimeConfig())
    target = tmp_path / "result"

    assert run_cmds._copy_shared_submission_outputs(
        {
            "result_ref": {
                "type": "artifact_ref",
                "version": "mn.staged_artifact/v1",
                "storage": "syncthing",
                "kind": "job_result",
                "submission_id": "submission-1",
                "run_id": "run-1",
                "relative_path": "outputs/runs/run-1/artifacts/aa/result.json",
                "content_type": "application/json",
                "size_bytes": 11,
                "sha256": "a" * 64,
            }
        },
        target,
    ) is True
    assert json.loads((target / "final_artifact.json").read_text()) == {"ok": True}
    assert (target / "company" / "analysis.md").read_text() == "# Analysis\n"


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


def test_materialize_shared_storage_outputs_retains_submission_after_copy(tmp_path):
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
    assert submission.exists()


def test_materialize_shared_storage_outputs_waits_for_delayed_user_output(
    monkeypatch, tmp_path
):
    host_root = tmp_path / "shared"
    submission = host_root / "submissions" / "sub-delayed"
    run_source = submission / "outputs" / "runs" / "run-1"
    user_source = submission / "outputs" / "user"
    run_target = tmp_path / "runs" / "run-1"
    user_target = tmp_path / "Downloads" / "software_architecture_advisor"
    run_source.mkdir(parents=True)
    user_source.mkdir(parents=True)
    (run_source / "events.jsonl").write_text('{"type": "job_completed"}\n')
    sleep_calls = 0

    def complete_user_source(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            (user_source / "architecture_report.md").write_text("# Report\n")

    monkeypatch.setenv("MN_OUTPUT_COPY_TIMEOUT_SECONDS", "1")
    monkeypatch.setattr(
        "mn_sdk.blueprint_support.shared_outputs.time.sleep",
        complete_user_source,
    )

    copied = run_cmds._materialize_shared_storage_outputs(
        {
            "host_root": str(host_root),
            "runtime_root": "/runtime/shared",
            "output_copy": [
                {
                    "source_path": "/runtime/shared/submissions/sub-delayed/outputs/runs/run-1",
                    "target_path": str(run_target),
                    "kind": "directory",
                },
                {
                    "source_path": "/runtime/shared/submissions/sub-delayed/outputs/user",
                    "target_path": str(user_target),
                    "kind": "directory",
                },
            ],
        }
    )

    assert copied is True
    assert sleep_calls >= 2
    assert (run_target / "events.jsonl").exists()
    assert (user_target / "architecture_report.md").read_text() == "# Report\n"


def test_detached_batch_run_starts_output_event_relay_for_shared_storage(
    mocker, tmp_path, monkeypatch
):
    monkeypatch.setenv("MN_RUN_BACKGROUND_EVENT_RELAY", "1")
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("MN_SHARED_STORAGE_ROOT", str(tmp_path / "shared"))
    monkeypatch.setenv("MN_RUNTIME_SHARED_STORAGE_ROOT", "/runtime/shared")
    mocker.patch(
        "mn_cli.libs.run_cmds._make_blueprint_run_id", return_value="batch-output-run"
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.client.create_job", return_value=json.dumps({"job_id": "job-batch-output"})
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.client.stream_events",
        return_value=[
            json.dumps({"type": "job_pending"}),
            json.dumps({"type": "job_running"}),
        ],
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_run",
        return_value=json.dumps(
            {
                "run_id": "batch-output-run",
                "status": "running",
            }
        ),
    )
    mock_process = mocker.Mock(pid=4343)
    mock_popen = mocker.patch(
        "mn_cli.libs.run_cmds.subprocess.Popen", return_value=mock_process
    )

    bundle_dir = tmp_path / "batch_bundle"
    bundle_dir.mkdir()
    target_path = tmp_path / "Downloads" / "vc_assistant"
    runtime_config = {
        "document_sources": {"folder_path": ""},
        "outputs": {"folder_path": str(target_path)},
    }
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            workflow_manifest({
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
            })
        )
    )
    config_dir = bundle_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "identity": {"blueprint_id": "vc_assistant"},
                "outputs": {"folder_path": str(target_path)},
            }
        )
    )

    result = runner.invoke(
        app, ["blueprint", "run", str(bundle_dir), "--follow-seconds", "0"]
    )

    assert result.exit_code == 0
    assert "Output event relay" in result.stdout
    mock_popen.assert_called_once()
    command = mock_popen.call_args.args[0]
    assert command[:3] == [sys.executable, "-m", "mn_sdk.blueprint_support.event_relay"]
    assert command[command.index("--run-id") + 1] == "batch-output-run"
    assert "--shared-storage-json" in command
    relay = json.loads(
        (tmp_path / "runs" / "batch-output-run" / "event_relay.json").read_text()
    )
    assert relay["run_id"] == "batch-output-run"
    storage_path = Path(relay["shared_storage_path"])
    storage = json.loads(storage_path.read_text())
    assert storage["output_copy_executor"] == "master_host"
    assert {
        item["target_path"] for item in storage["output_copy"]
    } == {
        str(tmp_path / "runs" / "batch-output-run"),
        str(target_path),
    }
