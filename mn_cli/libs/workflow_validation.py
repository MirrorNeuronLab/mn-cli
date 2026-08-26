from __future__ import annotations

import importlib.resources
import json
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

RETIRED_WORKFLOW_ROOT_FIELDS = ("flow", "graph_id", "nodes", "edges", "entrypoints")
WORKFLOW_SCHEMA = "mn.workflow.problem_graph/v1"
WORKFLOW_MODE = "static_dag"
WORKFLOW_MODES = {"static_dag", "dynamic_dag"}
AGENT_GRAPH_SCHEMA = "mn.agents.communication_graph/v1"


def _is_workflow_manifest(manifest: dict[str, Any]) -> bool:
    return (
        manifest.get("apiVersion") == "mn.workflow/v1"
        and manifest.get("kind") == "Workflow"
        and isinstance(manifest.get("workflow"), dict)
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
    retired_fields = [
        field for field in RETIRED_WORKFLOW_ROOT_FIELDS if field in manifest
    ]
    if retired_fields:
        return [
            _workflow_validation_issue(
                field,
                f"{field} is not allowed in mn.workflow/v1 manifests",
                code="workflow_manifest.schema_failed",
            )
            for field in retired_fields
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
    if path in RETIRED_WORKFLOW_ROOT_FIELDS:
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
    dynamic_issues, template_ids = _validate_dynamic_workflow_issues(
        workflow, step_ids
    )
    issues.extend(dynamic_issues)

    if isinstance(agents, dict):
        issues.extend(_validate_agent_graph_issues(agents))
    if isinstance(runtime, dict):
        issues.extend(
            _validate_runtime_binding_issues(runtime, step_ids | template_ids)
        )

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
    if not edges and not _dynamic_checkpoint_only(workflow):
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
    if mode not in WORKFLOW_MODES:
        issues.append(
            _workflow_validation_issue(
                "workflow.mode",
                "workflow.mode must be static_dag or dynamic_dag",
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


def _validate_dynamic_workflow_issues(
    workflow: dict[str, Any], step_ids: set[str]
) -> tuple[list[dict[str, Any]], set[str]]:
    mode = workflow.get("mode") or WORKFLOW_MODE
    dynamic = workflow.get("dynamic")
    if mode == WORKFLOW_MODE:
        if isinstance(dynamic, dict) and dynamic.get("enabled") is True:
            return [
                _workflow_validation_issue(
                    "workflow.dynamic.enabled",
                    "dynamic.enabled requires workflow.mode dynamic_dag",
                )
            ], set()
        return [], set()
    if mode != "dynamic_dag":
        return [], set()
    if not isinstance(dynamic, dict):
        return [
            _workflow_validation_issue(
                "workflow.dynamic",
                "dynamic_dag workflows must declare workflow.dynamic",
            )
        ], set()

    issues: list[dict[str, Any]] = []
    if dynamic.get("enabled") is not True:
        issues.append(
            _workflow_validation_issue(
                "workflow.dynamic.enabled",
                "dynamic_dag workflows require dynamic.enabled true",
            )
        )
    if dynamic.get("apply_at") != "between_steps":
        issues.append(
            _workflow_validation_issue(
                "workflow.dynamic.apply_at",
                "dynamic.apply_at must be between_steps",
            )
        )

    templates = dynamic.get("templates")
    template_ids: set[str] = set()
    if not isinstance(templates, dict) or not templates:
        issues.append(
            _workflow_validation_issue(
                "workflow.dynamic.templates",
                "dynamic.templates must be a non-empty object",
            )
        )
    else:
        for template_id, template in templates.items():
            path = f"workflow.dynamic.templates.{template_id}"
            if not isinstance(template_id, str) or not template_id.strip():
                issues.append(
                    _workflow_validation_issue(path, "dynamic template id is required")
                )
                continue
            template_ids.add(template_id)
            if template_id in step_ids:
                issues.append(
                    _workflow_validation_issue(
                        path,
                        f"dynamic template id collides with fixed step: {template_id}",
                    )
                )
            if not isinstance(template, dict):
                issues.append(
                    _workflow_validation_issue(path, "dynamic template must be an object")
                )
                continue
            run = template.get("run")
            agent_id = template.get("agent_id")
            if not isinstance(run, str) or not run.strip():
                issues.append(
                    _workflow_validation_issue(
                        f"{path}.run", "dynamic template run is required"
                    )
                )
            if agent_id is not None and (
                not isinstance(agent_id, str) or not agent_id.strip()
            ):
                issues.append(
                    _workflow_validation_issue(
                        f"{path}.agent_id",
                        "dynamic template agent_id must be a non-empty string when provided",
                    )
                )

    edges = workflow.get("edges") if isinstance(workflow.get("edges"), list) else []
    edge_ids = {
        str(edge.get("id"))
        for edge in edges
        if isinstance(edge, dict) and edge.get("id")
    }
    edges_by_id = {
        str(edge.get("id")): edge
        for edge in edges
        if isinstance(edge, dict) and edge.get("id")
    }
    regions = dynamic.get("regions")
    if not isinstance(regions, list) or not regions:
        issues.append(
            _workflow_validation_issue(
                "workflow.dynamic.regions",
                "dynamic.regions must be a non-empty list",
            )
        )
    else:
        seen_regions: set[str] = set()
        edge_owners: dict[str, str] = {}
        for index, region in enumerate(regions):
            path = f"workflow.dynamic.regions[{index}]"
            if not isinstance(region, dict):
                issues.append(
                    _workflow_validation_issue(path, "dynamic region must be an object")
                )
                continue
            region_id = region.get("id")
            strategy = region.get("strategy")
            controller = region.get("controller")
            allowed = region.get("templates")
            mutable_edges = region.get("mutable_edges") or []
            if not isinstance(region_id, str) or not region_id.strip():
                issues.append(
                    _workflow_validation_issue(f"{path}.id", "dynamic region id is required")
                )
            elif region_id in seen_regions:
                issues.append(
                    _workflow_validation_issue(
                        f"{path}.id", f"duplicate dynamic region id: {region_id}"
                    )
                )
            else:
                seen_regions.add(region_id)
            if strategy not in {"replace_path", "checkpoint_fanout"}:
                issues.append(
                    _workflow_validation_issue(
                        f"{path}.strategy",
                        "region strategy must be replace_path or checkpoint_fanout",
                    )
                )
            if controller not in step_ids:
                issues.append(
                    _workflow_validation_issue(
                        f"{path}.controller",
                        "region controller must reference a fixed workflow step",
                    )
                )
            if (
                not isinstance(allowed, list)
                or not allowed
                or any(template not in template_ids for template in allowed)
            ):
                issues.append(
                    _workflow_validation_issue(
                        f"{path}.templates",
                        "region templates must reference admitted dynamic templates",
                    )
                )
            if strategy == "replace_path":
                if region.get("exit") not in step_ids:
                    issues.append(
                        _workflow_validation_issue(
                            f"{path}.exit",
                            "replace_path exit must reference a fixed workflow step",
                        )
                    )
                if not isinstance(mutable_edges, list) or not mutable_edges:
                    issues.append(
                        _workflow_validation_issue(
                            f"{path}.mutable_edges",
                            "replace_path must declare mutable_edges",
                        )
                    )
            elif mutable_edges:
                issues.append(
                    _workflow_validation_issue(
                        f"{path}.mutable_edges",
                        "checkpoint_fanout cannot declare mutable_edges",
                    )
                )
            for edge_id in mutable_edges if isinstance(mutable_edges, list) else []:
                if edge_id not in edge_ids:
                    issues.append(
                        _workflow_validation_issue(
                            f"{path}.mutable_edges",
                            f"unknown mutable workflow edge: {edge_id}",
                        )
                    )
                elif (
                    strategy == "replace_path"
                    and (
                        edges_by_id[edge_id].get("from") != controller
                        or edges_by_id[edge_id].get("to") != region.get("exit")
                    )
                ):
                    issues.append(
                        _workflow_validation_issue(
                            f"{path}.mutable_edges",
                            f"mutable workflow edge {edge_id} must connect the region controller to its exit",
                        )
                    )
                elif edge_id in edge_owners:
                    issues.append(
                        _workflow_validation_issue(
                            f"{path}.mutable_edges",
                            f"mutable workflow edge {edge_id} already belongs to region {edge_owners[edge_id]}",
                        )
                    )
                else:
                    edge_owners[edge_id] = str(region_id)

    limits = dynamic.get("limits")
    if limits is not None:
        issues.extend(_validate_dynamic_limits(limits))
    return issues, template_ids


def _validate_dynamic_limits(limits: Any) -> list[dict[str, Any]]:
    if not isinstance(limits, dict):
        return [
            _workflow_validation_issue(
                "workflow.dynamic.limits", "dynamic.limits must be an object"
            )
        ]
    caps = {
        "max_patches": 100_000,
        "max_active_steps": 1_000,
        "max_operations_per_patch": 256,
    }
    return [
        _workflow_validation_issue(
            f"workflow.dynamic.limits.{key}",
            f"{key} must be a positive integer no greater than {cap}",
        )
        for key, cap in caps.items()
        if key in limits
        and (
            not isinstance(limits[key], int)
            or isinstance(limits[key], bool)
            or limits[key] < 1
            or limits[key] > cap
        )
    ] + (
        [
            _workflow_validation_issue(
                "workflow.dynamic.limits.max_instance_input_bytes",
                "max_instance_input_bytes must be a positive integer",
            )
        ]
        if "max_instance_input_bytes" in limits
        and (
            not isinstance(limits["max_instance_input_bytes"], int)
            or isinstance(limits["max_instance_input_bytes"], bool)
            or limits["max_instance_input_bytes"] < 1
        )
        else []
    )


def _dynamic_checkpoint_only(workflow: dict[str, Any]) -> bool:
    if workflow.get("mode") != "dynamic_dag":
        return False
    dynamic = workflow.get("dynamic")
    regions = dynamic.get("regions") if isinstance(dynamic, dict) else None
    return (
        isinstance(regions, list)
        and bool(regions)
        and all(
            isinstance(region, dict)
            and region.get("strategy") == "checkpoint_fanout"
            for region in regions
        )
    )


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
