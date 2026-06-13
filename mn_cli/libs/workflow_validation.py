from __future__ import annotations

import importlib.resources
import json
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError


DEPRECATED_WORKFLOW_ROOT_FIELDS = ("flow", "graph_id", "nodes", "edges", "entrypoints")
WORKFLOW_SCHEMA = "mn.workflow.problem_graph/v1"
WORKFLOW_MODE = "static_dag"
AGENT_GRAPH_SCHEMA = "mn.agents.communication_graph/v1"


def _is_workflow_manifest(manifest: dict[str, Any]) -> bool:
    return (
        manifest.get("apiVersion") == "mn.workflow/v1"
        or manifest.get("kind") == "Workflow"
        or isinstance(manifest.get("workflow"), dict)
    )


def _manifest_workflow_id(manifest: dict[str, Any]) -> str | None:
    workflow = manifest.get("workflow") if isinstance(manifest.get("workflow"), dict) else {}
    workflow_id = workflow.get("workflow_id") if isinstance(workflow, dict) else None
    return (
        str(workflow_id)
        if isinstance(workflow_id, str) and workflow_id.strip()
        else None
    )


def _workflow_schema_validator() -> Draft202012Validator:
    schema_path = importlib.resources.files("mn_cli").joinpath(
        "schemas/workflow_manifest.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def _validate_workflow_schema_issues(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    deprecated_fields = [
        field for field in DEPRECATED_WORKFLOW_ROOT_FIELDS if field in manifest
    ]
    if deprecated_fields:
        return [
            _workflow_validation_issue(
                field,
                f"{field} is not allowed in mn.workflow/v1 manifests",
                code="workflow_manifest.schema_failed",
            )
            for field in deprecated_fields
        ]

    validator = _workflow_schema_validator()
    return [
        _workflow_schema_issue(error)
        for error in sorted(validator.iter_errors(manifest), key=_schema_error_sort_key)
    ]


def _schema_error_sort_key(error: ValidationError) -> tuple[str, str]:
    return (_schema_error_path(error), str(error.message))


def _schema_error_path(error: ValidationError) -> str:
    parts = list(error.path)
    schema_parts = list(error.absolute_schema_path)
    if not parts and len(schema_parts) >= 2 and schema_parts[-2] == "properties":
        return str(schema_parts[-1])
    if not parts:
        return "manifest"

    rendered: list[str] = []
    for part in parts:
        if isinstance(part, int) and rendered:
            rendered[-1] = f"{rendered[-1]}[{part}]"
        else:
            rendered.append(str(part))
    return ".".join(rendered)


def _workflow_schema_issue(error: ValidationError) -> dict[str, Any]:
    path = _schema_error_path(error)
    message = _workflow_schema_message(error, path)
    return _workflow_validation_issue(
        path, message, code="workflow_manifest.schema_failed"
    )


def _workflow_schema_message(error: ValidationError, path: str) -> str:
    if path in DEPRECATED_WORKFLOW_ROOT_FIELDS:
        return f"{path} is not allowed in mn.workflow/v1 manifests"
    if error.validator == "required":
        instance = error.instance if isinstance(error.instance, dict) else {}
        missing = ", ".join(
            str(item) for item in error.validator_value if item not in instance
        )
        if missing:
            return f"missing required field: {missing}"
    return str(error.message)


def _validate_workflow_manifest_issues(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    workflow = manifest.get("workflow")
    if not isinstance(workflow, dict):
        return [_workflow_validation_issue("workflow", "workflow must be an object")]

    issues: list[dict[str, Any]] = []
    agents = manifest.get("agents")
    runtime = manifest.get("runtime")

    if not isinstance(agents, dict):
        issues.append(_workflow_validation_issue("agents", "agents must be an object"))
    if not isinstance(runtime, dict):
        issues.append(_workflow_validation_issue("runtime", "runtime must be an object"))

    issues.extend(_validate_workflow_id_issues(workflow))
    step_issues, step_ids = _validate_workflow_steps(workflow)
    issues.extend(step_issues)
    issues.extend(_validate_workflow_graph_issues(workflow, step_ids))

    if isinstance(agents, dict):
        issues.extend(_validate_agent_graph_issues(agents))
    if isinstance(runtime, dict):
        issues.extend(_validate_runtime_binding_issues(runtime, step_ids))

    return issues


def _validate_workflow_id_issues(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    workflow_id = workflow.get("workflow_id")
    if isinstance(workflow_id, str) and workflow_id.strip():
        return []
    return [
        _workflow_validation_issue(
            "workflow.workflow_id",
            "workflow.workflow_id must be a non-empty string",
        )
    ]


def _validate_workflow_steps(
    workflow: dict[str, Any],
) -> tuple[list[dict[str, Any]], set[str]]:
    issues: list[dict[str, Any]] = []
    steps = workflow.get("steps")
    if not isinstance(steps, list) or not steps:
        return [
            _workflow_validation_issue(
                "workflow.steps", "workflow.steps must be a non-empty list"
            )
        ], set()

    step_ids: set[str] = set()
    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            issues.append(
                _workflow_validation_issue(
                    f"workflow.steps[{index}]", "workflow step must be an object"
                )
            )
            continue

        step_id = step.get("id")
        if not isinstance(step_id, str) or not step_id.strip():
            issues.append(
                _workflow_validation_issue(
                    f"workflow.steps[{index}].id", "workflow step id is required"
                )
            )
            continue
        if step_id in step_ids:
            issues.append(
                _workflow_validation_issue(
                    f"workflow.steps[{index}].id",
                    f"duplicate workflow step id: {step_id}",
                )
            )
        step_ids.add(step_id)
        issues.extend(_validate_workflow_step_control(step, index))
        issues.extend(_validate_workflow_step_join(step, index))

    return issues, step_ids


def _validate_workflow_step_control(
    step: dict[str, Any], index: int
) -> list[dict[str, Any]]:
    control = step.get("control")
    if not isinstance(control, dict):
        return []

    issues: list[dict[str, Any]] = []
    retry = control.get("retry")
    if isinstance(retry, dict):
        attempts = retry.get("max_attempts")
        if attempts is not None and (not isinstance(attempts, int) or attempts < 1):
            issues.append(
                _workflow_validation_issue(
                    f"workflow.steps[{index}].control.retry.max_attempts",
                    "retry max_attempts must be a positive integer",
                )
            )

    timeout = control.get("timeout_seconds")
    if timeout is not None and (not isinstance(timeout, (int, float)) or timeout < 0):
        issues.append(
            _workflow_validation_issue(
                f"workflow.steps[{index}].control.timeout_seconds",
                "timeout_seconds must be zero or greater",
            )
        )
    return issues


def _validate_workflow_step_join(
    step: dict[str, Any], index: int
) -> list[dict[str, Any]]:
    join = step.get("join")
    if join is None:
        return []
    if not isinstance(join, dict):
        return [
            _workflow_validation_issue(
                f"workflow.steps[{index}].join", "join must be an object"
            )
        ]

    issues: list[dict[str, Any]] = []
    mode = join.get("mode") or "all_required"
    if mode not in {"all_required", "min_success"}:
        issues.append(
            _workflow_validation_issue(
                f"workflow.steps[{index}].join.mode",
                "join.mode must be all_required or min_success",
            )
        )
    if mode == "min_success":
        min_success = join.get("min_success")
        if not isinstance(min_success, int) or min_success < 1:
            issues.append(
                _workflow_validation_issue(
                    f"workflow.steps[{index}].join.min_success",
                    "join.min_success must be a positive integer",
                )
            )
    return issues


def _validate_workflow_graph_issues(
    workflow: dict[str, Any], step_ids: set[str]
) -> list[dict[str, Any]]:
    issues = _validate_workflow_graph_settings(workflow, step_ids)

    edges = workflow.get("edges") or []
    if not isinstance(edges, list):
        return [
            _workflow_validation_issue("workflow.edges", "workflow.edges must be a list")
        ]
    if not edges:
        issues.append(
            _workflow_validation_issue(
                "workflow.edges", "workflow.edges must be a non-empty list"
            )
        )

    edge_issues, adjacency = _validate_workflow_edges(edges, step_ids)
    issues.extend(edge_issues)
    if issues:
        return issues

    source = workflow.get("source")
    sink = workflow.get("sink")
    if isinstance(source, str) and isinstance(sink, str):
        issues.extend(_validate_workflow_reachability(adjacency, source, sink, step_ids))
    return issues


def _validate_workflow_graph_settings(
    workflow: dict[str, Any], step_ids: set[str]
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    schema = workflow.get("schema")
    if schema != WORKFLOW_SCHEMA:
        issues.append(
            _workflow_validation_issue(
                "workflow.schema", f"workflow.schema must be {WORKFLOW_SCHEMA}"
            )
        )

    mode = workflow.get("mode") or WORKFLOW_MODE
    if mode != WORKFLOW_MODE:
        issues.append(
            _workflow_validation_issue(
                "workflow.mode", f"workflow.mode must be {WORKFLOW_MODE}"
            )
        )

    source = workflow.get("source")
    sink = workflow.get("sink")
    if source != workflow.get("entrypoint"):
        issues.append(
            _workflow_validation_issue(
                "workflow.source", "workflow.source must match workflow.entrypoint"
            )
        )
    if source not in step_ids:
        issues.append(
            _workflow_validation_issue(
                "workflow.source", "workflow.source must reference a workflow step id"
            )
        )
    if sink not in step_ids:
        issues.append(
            _workflow_validation_issue(
                "workflow.sink", "workflow.sink must reference a workflow step id"
            )
        )
    return issues


def _validate_workflow_edges(
    edges: list[Any], step_ids: set[str]
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    issues: list[dict[str, Any]] = []
    edge_ids: set[str] = set()
    adjacency: dict[str, list[str]] = {step_id: [] for step_id in step_ids}

    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            issues.append(
                _workflow_validation_issue(
                    f"workflow.edges[{index}]", "workflow edge must be an object"
                )
            )
            continue
        issues.extend(_validate_workflow_edge(edge, index, step_ids, edge_ids))
        upstream = edge.get("from")
        downstream = edge.get("to")
        if upstream in step_ids and downstream in step_ids:
            adjacency.setdefault(upstream, []).append(downstream)

    return issues, adjacency


def _validate_workflow_edge(
    edge: dict[str, Any], index: int, step_ids: set[str], edge_ids: set[str]
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    edge_id = edge.get("id")
    if not isinstance(edge_id, str) or not edge_id.strip():
        issues.append(
            _workflow_validation_issue(
                f"workflow.edges[{index}].id",
                "workflow edge id must be a non-empty string",
            )
        )
    elif edge_id in edge_ids:
        issues.append(
            _workflow_validation_issue(
                f"workflow.edges[{index}].id", f"duplicate workflow edge id: {edge_id}"
            )
        )
    else:
        edge_ids.add(edge_id)

    upstream = edge.get("from")
    downstream = edge.get("to")
    if upstream not in step_ids:
        issues.append(
            _workflow_validation_issue(
                f"workflow.edges[{index}].from",
                "edge from must reference a workflow step id",
            )
        )
    if downstream not in step_ids:
        issues.append(
            _workflow_validation_issue(
                f"workflow.edges[{index}].to",
                "edge to must reference a workflow step id",
            )
        )
    if upstream == downstream and upstream in step_ids:
        issues.append(
            _workflow_validation_issue(
                f"workflow.edges[{index}].to",
                "workflow edge cannot point a step to itself",
            )
        )

    required = edge.get("required", True)
    if not isinstance(required, bool):
        issues.append(
            _workflow_validation_issue(
                f"workflow.edges[{index}].required",
                "workflow edge required must be true or false",
            )
        )

    accepts = edge.get("accepts")
    if accepts is not None and not _valid_accepts_list(accepts):
        issues.append(
            _workflow_validation_issue(
                f"workflow.edges[{index}].accepts",
                "workflow edge accepts must be a non-empty string list",
            )
        )
    return issues


def _valid_accepts_list(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, str) and bool(item) for item in value)
    )


def _validate_workflow_reachability(
    adjacency: dict[str, list[str]],
    source: str,
    sink: str,
    step_ids: set[str],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    cycle = _workflow_graph_cycle(adjacency)
    if cycle:
        issues.append(
            _workflow_validation_issue(
                "workflow.edges",
                f"workflow graph must be acyclic: {' -> '.join(cycle)}",
            )
        )

    reachable = _workflow_reachable(adjacency, source)
    missing = sorted(step_ids - reachable)
    if missing:
        issues.append(
            _workflow_validation_issue(
                "workflow.source",
                f"workflow steps are unreachable from source: {', '.join(missing)}",
            )
        )
    if sink not in reachable:
        issues.append(
            _workflow_validation_issue(
                "workflow.sink", "workflow sink is not reachable from source"
            )
        )
    return issues


def _validate_agent_graph_issues(agents: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if agents.get("schema") != AGENT_GRAPH_SCHEMA:
        issues.append(
            _workflow_validation_issue(
                "agents.schema", f"agents.schema must be {AGENT_GRAPH_SCHEMA}"
            )
        )

    node_issues, node_ids, can_validate_references = _validate_agent_nodes(agents)
    issues.extend(node_issues)
    if not can_validate_references:
        return issues

    issues.extend(_validate_agent_entrypoints(agents, node_ids))
    issues.extend(_validate_agent_edges(agents, node_ids))
    return issues


def _validate_agent_nodes(
    agents: dict[str, Any],
) -> tuple[list[dict[str, Any]], set[str], bool]:
    nodes = agents.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        return [
            _workflow_validation_issue(
                "agents.nodes", "agents.nodes must be a non-empty list"
            )
        ], set(), False

    issues: list[dict[str, Any]] = []
    node_ids: set[str] = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            issues.append(
                _workflow_validation_issue(
                    f"agents.nodes[{index}]", "agent node must be an object"
                )
            )
            continue

        node_id = node.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            issues.append(
                _workflow_validation_issue(
                    f"agents.nodes[{index}].node_id", "agent node_id is required"
                )
            )
        elif node_id in node_ids:
            issues.append(
                _workflow_validation_issue(
                    f"agents.nodes[{index}].node_id",
                    f"duplicate agent node id: {node_id}",
                )
            )
        else:
            node_ids.add(node_id)

    return issues, node_ids, True


def _validate_agent_entrypoints(
    agents: dict[str, Any], node_ids: set[str]
) -> list[dict[str, Any]]:
    entrypoints = agents.get("entrypoints")
    if not isinstance(entrypoints, list) or not entrypoints:
        return [
            _workflow_validation_issue(
                "agents.entrypoints", "agents.entrypoints must be a non-empty list"
            )
        ]

    issues: list[dict[str, Any]] = []
    for index, entrypoint in enumerate(entrypoints):
        if entrypoint not in node_ids:
            issues.append(
                _workflow_validation_issue(
                    f"agents.entrypoints[{index}]",
                    "agent entrypoint must reference an agent node id",
                )
            )
    return issues


def _validate_agent_edges(
    agents: dict[str, Any], node_ids: set[str]
) -> list[dict[str, Any]]:
    edges = agents.get("edges")
    if not isinstance(edges, list):
        return [
            _workflow_validation_issue("agents.edges", "agents.edges must be a list")
        ]

    issues: list[dict[str, Any]] = []
    edge_ids: set[str] = set()
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            issues.append(
                _workflow_validation_issue(
                    f"agents.edges[{index}]", "agent edge must be an object"
                )
            )
            continue

        edge_id = edge.get("edge_id")
        if not isinstance(edge_id, str) or not edge_id.strip():
            issues.append(
                _workflow_validation_issue(
                    f"agents.edges[{index}].edge_id", "agent edge_id is required"
                )
            )
        elif edge_id in edge_ids:
            issues.append(
                _workflow_validation_issue(
                    f"agents.edges[{index}].edge_id",
                    f"duplicate agent edge id: {edge_id}",
                )
            )
        else:
            edge_ids.add(edge_id)

        if edge.get("from_node") not in node_ids:
            issues.append(
                _workflow_validation_issue(
                    f"agents.edges[{index}].from_node",
                    "agent edge from_node must reference an agent node id",
                )
            )
        if edge.get("to_node") not in node_ids:
            issues.append(
                _workflow_validation_issue(
                    f"agents.edges[{index}].to_node",
                    "agent edge to_node must reference an agent node id",
                )
            )
    return issues


def _validate_runtime_binding_issues(
    runtime: dict[str, Any], step_ids: set[str]
) -> list[dict[str, Any]]:
    bindings = runtime.get("bindings")
    if bindings is not None and not isinstance(bindings, dict):
        return [
            _workflow_validation_issue(
                "runtime.bindings", "runtime.bindings must be an object"
            )
        ]
    if not isinstance(bindings, dict):
        return []

    return [
        _workflow_validation_issue(
            f"runtime.bindings.{step_id}",
            "runtime binding must reference a workflow step id",
        )
        for step_id in bindings
        if step_ids and step_id not in step_ids
    ]


def _workflow_reachable(adjacency: dict[str, list[str]], source: str) -> set[str]:
    seen: set[str] = set()
    stack = [source]
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(adjacency.get(node, []))
    return seen


def _workflow_graph_cycle(adjacency: dict[str, list[str]]) -> list[str]:
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(node: str) -> list[str]:
        if node in visiting:
            if node in path:
                return path[path.index(node) :] + [node]
            return [node, node]
        if node in visited:
            return []

        visiting.add(node)
        path.append(node)
        for child in adjacency.get(node, []):
            cycle = visit(child)
            if cycle:
                return cycle
        path.pop()
        visiting.remove(node)
        visited.add(node)
        return []

    for node in adjacency:
        cycle = visit(node)
        if cycle:
            return cycle
    return []


def _workflow_validation_issue(
    path: str,
    message: str,
    *,
    code: str = "workflow_manifest.validation_failed",
) -> dict[str, Any]:
    pointer = "/manifest" if path == "manifest" else "/manifest/" + path.replace(".", "/")
    return {
        "code": code,
        "message": message,
        "help": "Fix this workflow manifest field and run validation again.",
        "severity": "error",
        "location": {
            "source": "manifest",
            "path": path,
            "pointer": pointer,
        },
    }
