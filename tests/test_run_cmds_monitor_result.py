import io
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
from mn_cli.libs.ui import (
    JobMonitorState,
    _agent_table,
    _workflow_agent_table,
    generate_live_layout,
)
from mn_cli.libs.run_cmds.handlers.monitor import (
    _get_job_for_monitor,
    _monitor_api_stream_timeout_seconds,
    _public_progress_from_api_snapshot,
    _workflow_progress_for_monitor,
)
from mn_cli.libs.run_cmds.live import _read_monitor_key
from mn_cli.libs.workflow_progress import (
    BlueprintWorkflowProgress,
    _agent_progress_detail,
)
from mn_cli.libs.run_manifest import prepare_manifest_for_submission
from mn_sdk import (
    AgentProgress,
    load_model_ownership,
    load_model_remotes,
    upsert_model_remote,
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_mn_home(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_HOME", str(tmp_path / "mn-home"))
    monkeypatch.setenv("MN_JOB_MONITOR_DISABLE_API_STREAM", "1")
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
    monkeypatch.setattr(run_cmds.client, "stream_events", lambda *_args, **_kwargs: [])


def test_monitor_success(mocker):
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_job",
        return_value=json.dumps(
            {
                "summary": {"status": "completed", "live?": False},
                "job": {"job_name": "test"},
                "agents": [
                    {"agent_id": "a1", "status": "running", "processed_messages": 10}
                ],
            }
        ),
    )
    mocker.patch("sys.stdin.isatty", return_value=False)

    result = runner.invoke(app, ["job", "monitor", "job-123"])

    assert result.exit_code == 0
    assert "Workflow Job Monitor" in result.stdout
    assert "keys: ↑/↓ select agent" in result.stdout
    assert "Job summary" in result.stdout


def test_job_monitor_keyboard_state_and_agent_detail():
    state = JobMonitorState()
    assert state.handle_key("\x1b[B", 2) is True
    assert state.selected_index == 1
    assert state.handle_key("\r", 2) is True
    assert state.detail_mode is True

    console = Console(record=True, width=160, force_terminal=False)
    console.print(
        generate_live_layout(
            "job-123",
            {
                "summary": {"status": "running", "live?": True, "nodes": ["worker"]},
                "job": {"job_name": "test", "graph_id": "graph"},
                "agents": [
                    {
                        "agent_id": "a1",
                        "agent_type": "router",
                        "status": "running",
                        "processed_messages": 20,
                    },
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

    assert state.handle_key("\x7f", 2) is True
    assert state.detail_mode is False
    assert state.handle_key("\x04", 2) is True
    assert state.handle_key("q", 2) is False


def test_job_monitor_ignores_legacy_shortcuts_and_accepts_only_shared_keys():
    state = JobMonitorState()

    for key in ("j", "k", "d", "1", "o", "\t", "\x04"):
        assert state.handle_key(key, 3) is True
    assert state.selected_index == 0
    assert state.detail_mode is False

    assert state.handle_key("\x1b[B", 3) is True
    assert state.selected_index == 1
    assert state.handle_key("\x1b[A", 3) is True
    assert state.selected_index == 0
    assert state.handle_key("\n", 3) is True
    assert state.detail_mode is True
    assert state.handle_key("\x08", 3) is True
    assert state.detail_mode is False
    assert state.handle_key("\x03", 3) is False


def test_monitor_key_reader_preserves_arrow_escape_sequences():
    class ReadyWhenData:
        def select(self, readers, _writers, _errors, _timeout):
            stream = readers[0]
            return (readers, [], []) if stream.tell() < len(payload) else ([], [], [])

    payload = "\x1b[A"
    assert _read_monitor_key(io.StringIO(payload), ReadyWhenData()) == payload


def test_monitor_uses_public_workflow_contract_and_hides_runtime_nodes(mocker):
    data = {
        "summary": {
            "status": "running",
            "job_id": "job-public",
            "graph_id": "public-workflow",
        },
        "job": {
            "job_id": "job-public",
            "graph_id": "public-workflow",
            "job_name": "Public Workflow",
            "status": "running",
            "submitted_at": "2026-07-16T10:00:00Z",
            "workflow_state": {
                "enabled": True,
                "step_order": ["detect", "research"],
                "edges": [
                    {
                        "id": "detect_to_research",
                        "from": "detect",
                        "to": "research",
                        "event": "detect_completed",
                    }
                ],
                "steps": {
                    "detect": {
                        "id": "detect",
                        "label": "Detect Sources",
                        "run": "detect__start",
                        "status": "completed",
                    },
                    "research": {
                        "id": "research",
                        "label": "Research Sources",
                        "run": "research__start",
                        "status": "running",
                    },
                },
            },
            "runtime_topology": {
                "nodes": [
                    {"node_id": "detect__start", "agent_type": "step_source"},
                    {"node_id": "detect__worker", "agent_type": "executor"},
                    {"node_id": "detect__end", "agent_type": "step_sink"},
                    {"node_id": "research__start", "agent_type": "step_source"},
                    {"node_id": "research__worker", "agent_type": "executor"},
                    {"node_id": "research__end", "agent_type": "step_sink"},
                ]
            },
        },
        "agents": [
            {"agent_id": "detect__start", "status": "completed"},
            {"agent_id": "detect__worker", "status": "completed"},
            {"agent_id": "detect__end", "status": "ready"},
            {"agent_id": "research__start", "status": "ready"},
            {"agent_id": "research__worker", "status": "running"},
            {"agent_id": "research__end", "status": "ready"},
        ],
    }
    mocker.patch(
        "mn_cli.libs.run_cmds.handlers.monitor.client.stream_events",
        return_value=[
            json.dumps(
                {
                    "type": "workflow_step_attempt_started",
                    "agent_id": "research__start",
                    "step": "research",
                }
            )
        ],
    )

    progress = _workflow_progress_for_monitor("job-public", data)

    assert progress is not None
    assert [step["id"] for step in progress["steps"]] == ["detect", "research"]
    assert progress["current_step"]["id"] == "research"
    assert [agent["id"] for agent in progress["current_step"]["agents"]] == ["research"]

    console = Console(record=True, width=140)
    console.print(
        generate_live_layout(
            "job-public", {"workflow_progress": progress}, JobMonitorState()
        )
    )
    rendered = console.export_text()
    assert "Detect Sources" in rendered
    assert "Research Sources" in rendered
    assert "detect__start" not in rendered
    assert "research__end" not in rendered


def test_monitor_normalizes_flat_grpc_job_payload_and_hides_runtime_nodes(mocker):
    data = {
        "job_id": "job-flat",
        "graph_id": "public-workflow",
        "job_name": "Public Workflow",
        "status": "running",
        "runtime_topology": {
            "nodes": [
                {"node_id": "detect__start", "agent_type": "step_source"},
                {"node_id": "detect__worker", "agent_type": "executor"},
                {"node_id": "detect__end", "agent_type": "step_sink"},
                {"node_id": "research__start", "agent_type": "step_source"},
                {"node_id": "research__worker", "agent_type": "executor"},
                {"node_id": "research__end", "agent_type": "step_sink"},
            ]
        },
        "agents": [
            {"agent_id": "detect__start", "status": "completed"},
            {"agent_id": "detect__worker", "status": "completed"},
            {"agent_id": "research__worker", "status": "running"},
        ],
    }
    mocker.patch(
        "mn_cli.libs.run_cmds.handlers.monitor.client.stream_events",
        return_value=[
            json.dumps(
                {
                    "type": "workflow_step_attempt_started",
                    "payload": {"step": "research", "worker": "research"},
                }
            )
        ],
    )

    progress = _workflow_progress_for_monitor("job-flat", data)

    assert progress is not None
    assert [step["id"] for step in progress["steps"]] == ["detect", "research"]
    assert progress["current_step"]["id"] == "research"
    assert [agent["id"] for agent in progress["current_step"]["agents"]] == ["research"]


def test_monitor_prefers_source_manifest_from_local_run_store(mocker, tmp_path):
    run_dir = tmp_path / "runs" / "va-f381a1a2"
    run_dir.mkdir(parents=True)
    (run_dir / "job.json").write_text(
        json.dumps({"job_id": "job-source", "run_id": "va-f381a1a2"})
    )
    (run_dir / "config.json").write_text(
        json.dumps(
            {
                "workflow": {
                    "workflow_id": "source-workflow",
                    "steps": [
                        {"id": "detect", "label": "Detect", "run": "detect"},
                        {"id": "research", "label": "Research", "run": "research_team"},
                    ],
                },
                "runtime": {
                    "bindings": {
                        "detect": {"workers": [{"id": "detect", "role": "Detect"}]},
                        "research_team": {
                            "workers": [{"id": "research:docs", "role": "Analyze docs"}]
                        },
                    }
                },
            }
        )
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.handlers.monitor.default_runs_root",
        return_value=tmp_path / "runs",
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.handlers.monitor.client.stream_events",
        return_value=[
            json.dumps(
                {
                    "type": "workflow_step_attempt_started",
                    "payload": {"step": "research", "worker": "research:docs"},
                }
            )
        ],
    )

    data = {
        "job": {
            "job_id": "job-source",
            "run_id": "va-f381a1a2",
            "manifest": {
                "workflow": {
                    "steps": [{"id": "detect__start"}, {"id": "research__start"}]
                },
                "nodes": [],
            },
            "runtime_topology": {
                "nodes": [
                    {"node_id": "detect__start", "agent_type": "step_source"},
                    {"node_id": "research__start", "agent_type": "step_source"},
                ]
            },
        },
        "summary": {"status": "running"},
    }

    progress = _workflow_progress_for_monitor("job-source", data)

    assert progress is not None
    assert [step["id"] for step in progress["steps"]] == ["detect", "research"]
    assert [agent["id"] for agent in progress["current_step"]["agents"]] == [
        "research:docs"
    ]


def test_monitor_retries_transient_deadline_fetch(mocker, monkeypatch):
    class DeadlineError(Exception):
        def code(self):
            return SimpleNamespace(name="DEADLINE_EXCEEDED")

    payload = json.dumps({"job_id": "job-retry", "status": "running"})
    get_job = mocker.patch(
        "mn_cli.libs.run_cmds.handlers.monitor.client.get_job",
        side_effect=[DeadlineError("Deadline Exceeded"), payload],
    )
    monkeypatch.setenv("MN_JOB_MONITOR_RETRY_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("MN_JOB_MONITOR_GRPC_TIMEOUT_SECONDS", "30")

    assert _get_job_for_monitor("job-retry") == payload
    assert get_job.call_count == 2


def test_monitor_projects_existing_run_from_blueprint_source_without_get_job(
    mocker, tmp_path
):
    runs_root = tmp_path / "runs"
    run_dir = runs_root / "vc-run"
    run_dir.mkdir(parents=True)
    blueprint_root = tmp_path / "blueprints"
    blueprint_dir = blueprint_root / "vc_assistant"
    blueprint_dir.mkdir(parents=True)
    manifest = {
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "workflow": {
            "workflow_id": "vc-workflow",
            "steps": [
                {
                    "id": "audit",
                    "label": "Audit",
                    "action": "audit",
                    "run": "audit__start",
                }
            ],
        },
        "runtime": {
            "bindings": {
                "audit": {
                    "worker": {
                        "id": "audit__score_consistency_auditor",
                        "role": "Score Consistency Auditor",
                    }
                }
            }
        },
    }
    (blueprint_dir / "manifest.json").write_text(json.dumps(manifest))
    (blueprint_root / "index.json").write_text(
        json.dumps([{"id": "vc_assistant", "path": "vc_assistant"}])
    )
    (run_dir / "job.json").write_text(
        json.dumps(
            {
                "job_id": "job-large",
                "run_id": "vc-run",
                "blueprint_id": "vc_assistant",
                "blueprint_source": str(blueprint_root),
            }
        )
    )
    (run_dir / "events.jsonl").write_text(
        "\n".join(
            json.dumps(event)
            for event in (
                {
                    "type": "workflow_step_attempt_started",
                    "step_id": "audit",
                    "agent_id": "audit__start",
                },
                {
                    "type": "workflow_worker_started",
                    "payload": {
                        "step_id": "audit",
                        "worker": "audit__score_consistency_auditor",
                    },
                },
            )
        )
        + "\n"
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.handlers.monitor.default_runs_root",
        return_value=runs_root,
    )
    get_job = mocker.patch(
        "mn_cli.libs.run_cmds.handlers.monitor.client.get_job",
        side_effect=AssertionError("large GetJob response should not be fetched"),
    )

    progress = _public_progress_from_api_snapshot(
        "job-large", {"job_id": "job-large", "status": "running"}
    )

    assert [step["id"] for step in progress["steps"]] == ["audit"]
    assert progress["current_step"]["id"] == "audit"
    assert [agent["id"] for agent in progress["current_step"]["agents"]] == [
        "audit__score_consistency_auditor"
    ]
    get_job.assert_not_called()


def test_monitor_stream_timeout_covers_transient_deadlines(monkeypatch):
    monkeypatch.delenv("MN_JOB_MONITOR_API_STREAM_TIMEOUT", raising=False)

    assert _monitor_api_stream_timeout_seconds() >= 30


def test_agent_selection_has_no_reverse_background():
    workflow_table = _workflow_agent_table(
        [{"id": "worker", "status": "running", "progress": 0.1}], 0
    )
    legacy_table = _agent_table(
        [{"agent_id": "worker", "status": "running", "progress": 0.1}], 0
    )

    assert all("reverse" not in str(row.style) for row in workflow_table.rows)
    assert all("reverse" not in str(row.style) for row in legacy_table.rows)


def test_monitor_error(mocker):
    mocker.patch("sys.stdin.isatty", return_value=False)
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_job", side_effect=Exception("Network fail")
    )
    result = runner.invoke(app, ["job", "monitor", "job-123"])
    assert result.exit_code == 0
    assert "Error fetching job: Network fail" in result.stdout


def test_result_success(mocker, tmp_path):
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_job",
        return_value=json.dumps(
            {
                "job": {"status": "completed", "result": {"test": "result"}},
                "recent_events": [],
            }
        ),
    )
    mocker.patch(
        "mn_cli.libs.run_cmds.client.stream_events",
        return_value=[json.dumps({"type": "custom_event", "payload": "progressive"})],
    )

    result = runner.invoke(app, ["job", "result", "job-123"])

    assert result.exit_code == 0
    assert "Job result fetch successful." in result.stdout
    assert "Final result:" in result.stdout
    assert "Stream results:" in result.stdout


def test_result_not_completed(mocker, tmp_path):
    mocker.patch(
        "mn_cli.libs.run_cmds.client.get_job",
        return_value=json.dumps({"job": {"status": "running"}, "recent_events": []}),
    )
    mocker.patch("mn_cli.libs.run_cmds.client.stream_events", return_value=[])

    result = runner.invoke(app, ["job", "result", "job-999"])

    assert result.exit_code == 0
    assert "No final result found" in result.stdout


def test_result_error(mocker):
    mocker.patch(
        "mn_cli.libs.run_cmds.fetch_and_save_results", side_effect=Exception("DB Error")
    )

    result = runner.invoke(app, ["job", "result", "job-888"])

    assert result.exit_code == 1
    assert "MN_EXECUTION_FAILED" in result.stdout
