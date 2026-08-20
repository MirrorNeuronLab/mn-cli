from copy import deepcopy

from mn_cli.libs import workflow_validation


def _workflow_manifest() -> dict:
    return {
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": "sample_flow",
        "name": "Sample Flow",
        "manifest_version": "1.0",
        "job_name": "sample-flow",
        "workflow": {
            "schema": "mn.workflow.problem_graph/v1",
            "workflow_id": "sample_flow_v2",
            "mode": "static_dag",
            "entrypoint": "start",
            "source": "start",
            "sink": "finish",
            "steps": [{"id": "start"}, {"id": "finish"}],
            "edges": [{"id": "start_to_finish", "from": "start", "to": "finish"}],
        },
        "agents": {
            "schema": "mn.agents.communication_graph/v1",
            "entrypoints": ["worker"],
            "nodes": [{"node_id": "worker"}],
            "edges": [],
        },
        "runtime": {"bindings": {}},
    }


def _issue_paths(issues: list[dict]) -> set[str]:
    return {issue["location"]["path"] for issue in issues}


def test_workflow_schema_reports_each_deprecated_root_field():
    manifest = _workflow_manifest()
    manifest["graph_id"] = "legacy"
    manifest["nodes"] = []

    issues = workflow_validation._validate_workflow_schema_issues(manifest)

    assert _issue_paths(issues) == {"graph_id", "nodes"}
    assert {issue["code"] for issue in issues} == {"workflow_manifest.schema_failed"}


def test_workflow_schema_requires_literal_true_for_customize_mode():
    manifest = _workflow_manifest()
    manifest["runtime"]["models"] = {
        "primary": {
            "provider": "docker_model_runner",
            "runtime_model": "hf.co/acme/custom:Q4_K_M",
            "backend": "llama.cpp",
            "customize_mode": True,
        }
    }

    assert "runtime.models.primary.customize_mode" not in _issue_paths(
        workflow_validation._validate_workflow_schema_issues(manifest)
    )

    manifest["runtime"]["models"]["primary"]["customize_mode"] = "true"
    issues = workflow_validation._validate_workflow_schema_issues(manifest)

    assert "runtime.models.primary.customize_mode" in _issue_paths(issues)


def test_workflow_manifest_reports_nested_step_and_runtime_binding_issues():
    manifest = _workflow_manifest()
    manifest["workflow"]["steps"][0] = {
        "id": "start",
        "control": {
            "retry": {"max_attempts": 0},
            "timeout_seconds": -1,
        },
        "join": {"mode": "min_success", "min_success": 0},
    }
    manifest["runtime"]["bindings"] = {"missing_step": {}}

    issues = workflow_validation._validate_workflow_manifest_issues(manifest)

    assert {
        "workflow.steps[0].control.retry.max_attempts",
        "workflow.steps[0].control.timeout_seconds",
        "workflow.steps[0].join.min_success",
        "runtime.bindings.missing_step",
    }.issubset(_issue_paths(issues))


def test_dynamic_workflow_accepts_admitted_template_binding_and_replace_path_region():
    manifest = _workflow_manifest()
    manifest["workflow"]["mode"] = "dynamic_dag"
    manifest["workflow"]["dynamic"] = {
        "enabled": True,
        "apply_at": "between_steps",
        "templates": {
            "followup_research": {
                "id": "followup_research",
                "run": "followup_research",
                "agent_id": "followup_worker",
            }
        },
        "regions": [
            {
                "id": "research_followups",
                "strategy": "replace_path",
                "controller": "start",
                "exit": "finish",
                "templates": ["followup_research"],
                "mutable_edges": ["start_to_finish"],
            }
        ],
    }
    manifest["agents"]["nodes"].append({"node_id": "followup_worker"})
    manifest["runtime"]["bindings"]["followup_research"] = {
        "worker": {"id": "followup_worker"}
    }

    issues = workflow_validation._validate_workflow_manifest_issues(manifest)

    assert issues == []


def test_dynamic_workflow_rejects_template_collision_and_hard_limit_overflow():
    manifest = _workflow_manifest()
    manifest["workflow"]["mode"] = "dynamic_dag"
    manifest["workflow"]["dynamic"] = {
        "enabled": True,
        "apply_at": "between_steps",
        "limits": {"max_active_steps": 1001},
        "templates": {
            "start": {
                "id": "start",
                "run": "start_template",
                "agent_id": "worker",
            }
        },
        "regions": [
            {
                "id": "region",
                "strategy": "replace_path",
                "controller": "start",
                "exit": "finish",
                "templates": ["start"],
                "mutable_edges": ["start_to_finish"],
            }
        ],
    }

    paths = _issue_paths(
        workflow_validation._validate_workflow_manifest_issues(manifest)
    )

    assert "workflow.dynamic.templates.start" in paths
    assert "workflow.dynamic.limits.max_active_steps" in paths


def test_workflow_graph_skips_reachability_checks_when_edges_are_invalid():
    workflow = deepcopy(_workflow_manifest()["workflow"])
    workflow["steps"].append({"id": "orphan"})
    workflow["edges"] = [
        {
            "id": "bad_edge",
            "from": "start",
            "to": "missing",
            "required": "yes",
            "accepts": [],
        }
    ]

    issues = workflow_validation._validate_workflow_manifest_issues(
        {
            **_workflow_manifest(),
            "workflow": workflow,
        }
    )
    paths = _issue_paths(issues)

    assert "workflow.edges[0].to" in paths
    assert "workflow.edges[0].required" in paths
    assert "workflow.edges[0].accepts" in paths
    assert "workflow.source" not in paths
