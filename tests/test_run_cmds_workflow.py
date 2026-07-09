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
    assert "Monitor" in result.stdout
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
    assert "Research" in result.stdout
    assert "Research" in result.stdout
    assert "Research  |  2 agents" in result.stdout
    assert "1.8k" in result.stdout

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
    assert "0/1" in workflow_view
    assert "0/1" in job_monitor_view
    assert "Research" in workflow_view
    assert "Research" in job_monitor_view
    assert "run used 100 tok" in workflow_view
    assert "run used 100 tok" in job_monitor_view

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
