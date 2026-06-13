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
            "workflow_id": "sample_flow_v1",
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
