from .common import *
from contextlib import contextmanager
from contextvars import ContextVar


_WORKFLOW_PLACEMENT_NODE: ContextVar[str] = ContextVar(
    "mn_workflow_placement_node", default=""
)
_WORKFLOW_PLACEMENT_MODEL_FALLBACKS: ContextVar[tuple[dict[str, Any], ...]] = (
    ContextVar("mn_workflow_placement_model_fallbacks", default=())
)


@contextmanager
def _runtime_model_placement_scope(env_overrides: Optional[dict[str, str]] = None):
    """Make the workflow node available to model preparation callbacks.

    ``BlueprintModelOps`` only passes a model requirement and catalog entry to
    its placement callback.  A context variable keeps that callback coupled to
    the placement selected for this launch without mutating process-wide
    environment variables (or model catalog entries).
    """

    selected_node = str(
        (env_overrides or {}).get("MN_SELECTED_RUNTIME_NODE")
        or os.environ.get("MN_SELECTED_RUNTIME_NODE")
        or ""
    ).strip()
    fallback_records = _workflow_placement_model_fallback_records(env_overrides)
    token = _WORKFLOW_PLACEMENT_NODE.set(selected_node)
    fallback_token = _WORKFLOW_PLACEMENT_MODEL_FALLBACKS.set(fallback_records)
    try:
        yield
    finally:
        _WORKFLOW_PLACEMENT_MODEL_FALLBACKS.reset(fallback_token)
        _WORKFLOW_PLACEMENT_NODE.reset(token)


def _resolve_runtime_cluster_model(
    *, requirement: dict[str, Any], entry: dict[str, Any]
) -> dict[str, Any] | None:
    selected_node = _WORKFLOW_PLACEMENT_NODE.get().strip()
    if selected_node:
        fallback_entry = _workflow_placement_fallback_for_entry(entry)
        if fallback_entry is not None:
            return {
                "source": "workflow_placement",
                "status": "fallback_model",
                "node": selected_node,
                "requirement": _workflow_model_gpu_requirement(fallback_entry),
                "fallback_entry": fallback_entry,
                "fallback_reason": "no_capable_node_for_preferred_model",
            }
        return {
            "source": "workflow_placement",
            "status": "cluster_node",
            "node": selected_node,
            "requirement": _workflow_model_gpu_requirement(entry),
        }
    if is_custom_model_requirement(requirement):
        resource_report = _runtime_resource_report()
        try:
            system_summary = json.loads(client.get_system_summary())
        except Exception as exc:
            raise ModelPrepareError(
                "model.custom_cluster_inspection_failed",
                f"could not inspect cluster nodes for custom model placement: {exc}",
                stage="placement",
                safe_message="Could not inspect runtime nodes for custom model placement.",
            ) from exc
        if not isinstance(system_summary, dict):
            raise ModelPrepareError(
                "model.custom_cluster_inspection_failed",
                "runtime system summary is not a JSON object",
                stage="placement",
                safe_message="Runtime node metadata is invalid for custom model placement.",
            )
        return resolve_custom_model_placement(
            resource_report=resource_report,
            system_summary=system_summary,
        )
    try:
        return resolve_cluster_model_placement(
            entry, resource_report=_runtime_resource_report
        )
    except Exception:
        logger.exception(
            "Failed to resolve cluster model placement for %s",
            entry.get("id") or entry.get("model"),
        )
        return None


def _cluster_node_grpc_target(node_name: str) -> str:
    return _cluster_node_endpoint(node_name)["grpc_target"]


def _cluster_node_endpoint(node_name: str) -> dict[str, Any]:
    node_name = str(node_name or "").strip()
    if not node_name:
        raise RuntimeError("cluster model placement did not return a target node")
    try:
        summary = json.loads(client.get_system_summary())
    except Exception as exc:
        raise RuntimeError(
            f"could not inspect cluster nodes for {node_name}: {exc}"
        ) from exc
    nodes = summary.get("nodes") if isinstance(summary, dict) else None
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        if str(node.get("name") or node.get("node") or "").strip() != node_name:
            continue
        host = str(node.get("grpc_host") or node.get("address") or "").strip()
        port = str(node.get("grpc_port") or "").strip()
        if not host or not port:
            raise RuntimeError(
                f"cluster node {node_name} does not advertise grpc_host/grpc_port"
            )
        return {
            "grpc_target": f"{host}:{port}",
            "host": host,
            "port": port,
            "node": node,
        }
    raise RuntimeError(f"cluster node {node_name} was not found in runtime summary")


def _node_native_sdk_grpc_info(node: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[Any] = [node.get("native_sdk_grpc")]
    hardware = node.get("hardware")
    if isinstance(hardware, dict):
        candidates.append(hardware.get("native_sdk_grpc"))
    node_info = node.get("node_info")
    if isinstance(node_info, dict):
        candidates.append(node_info.get("native_sdk_grpc"))
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return candidate
    return None


def _cluster_node_native_sdk_endpoint(
    node_name: str, node: dict[str, Any]
) -> dict[str, str]:
    native = _node_native_sdk_grpc_info(node)
    if not native:
        raise RuntimeError(
            f"cluster node {node_name} does not advertise native SDK gRPC; "
            "restart that worker with an updated `mn runtime start --worker-node` "
            "so runtime model preparation can run outside Core"
        )
    if native.get("enabled") is False:
        raise RuntimeError(
            f"cluster node {node_name} advertises native SDK gRPC as disabled; "
            "start the node-local mn-python-sdk native runtime service before preparing models"
        )

    target = str(native.get("target") or "").strip()
    host = str(native.get("host") or "").strip()
    port = str(native.get("port") or "").strip()
    if target and (not host or not port) and ":" in target:
        parsed_host, parsed_port = target.rsplit(":", 1)
        host = host or parsed_host.strip()
        port = port or parsed_port.strip()
    if not target and host and port:
        target = f"{host}:{port}"
    if not target or not host or not port:
        raise RuntimeError(
            f"cluster node {node_name} advertises incomplete native SDK gRPC metadata"
        )
    return {"target": target, "host": host, "port": port}


def _runtime_model_prepare_client(node: str, node_endpoint: dict[str, Any]) -> Client:
    timeout = _runtime_model_prepare_timeout_seconds()
    if _cluster_node_endpoint_is_local(node_endpoint):
        return Client(
            target=config.grpc_target,
            timeout=timeout,
            auth_token=config.grpc_auth_token,
            admin_token=config.grpc_admin_token,
        )

    native_endpoint = _cluster_node_native_sdk_endpoint(node, node_endpoint["node"])
    return Client(
        target=native_endpoint["target"],
        timeout=timeout,
        auth_token=config.grpc_auth_token,
        admin_token=config.grpc_admin_token,
    )


def _prepare_runtime_model_with_retry(
    runtime_client: Client, prepare_payload: dict[str, Any]
) -> dict[str, Any]:
    def notify_retry(_attempt: int, _error: BaseException) -> None:
        console.print(
            "[yellow]Runtime model prepare timed out or became unavailable; "
            "retrying once with the same request...[/yellow]"
        )

    return call_prepare_runtime_model(
        runtime_client,
        prepare_payload,
        on_retry=notify_retry,
        logger=logger,
    )


def _resolve_and_apply_workflow_placement(
    manifest: dict[str, Any],
    *,
    runtime_model_requirements: Optional[list[dict[str, Any]]] = None,
    resource_report: Optional[dict[str, Any]] = None,
    system_summary: Optional[dict[str, Any]] = None,
    env: Optional[dict[str, str]] = None,
) -> dict[str, Any] | None:
    """Select one node that can satisfy the complete native workflow.

    Placement is intentionally resolved before model, context, HostLocal, or
    DockerWorker preparation.  A workflow that uses node-local resources must
    not have its lightweight nodes left on the submitter while a GPU worker is
    sent to another machine: that produces unreachable gateways, context
    services, and container networks.
    """

    mode = _workflow_placement_mode(manifest, env=env)
    if mode == "distributed":
        return None
    if (
        mode is None
        and not _workflow_requires_single_node(manifest)
        and not runtime_model_requirements
    ):
        return None

    resources = (
        resource_report
        if isinstance(resource_report, dict)
        else _runtime_resource_report()
    )
    if system_summary is None:
        try:
            decoded = json.loads(client.get_system_summary())
        except Exception as exc:
            raise RuntimeError(
                f"could not inspect runtime nodes for workflow placement: {exc}"
            ) from exc
        system_summary = decoded if isinstance(decoded, dict) else {}

    resource_nodes = _workflow_nodes_by_name(resources)
    system_nodes = _workflow_nodes_by_name(system_summary)
    names = sorted(set(resource_nodes) | set(system_nodes))
    if not names:
        raise RuntimeError(
            "No runtime nodes were reported while resolving single-node workflow placement."
        )

    explicit = _workflow_explicit_node_placements(manifest)
    distinct_explicit = sorted(set(explicit.values()))
    if len(distinct_explicit) > 1:
        details = ", ".join(
            f"{node_id}={node_name}" for node_id, node_name in sorted(explicit.items())
        )
        raise RuntimeError(
            "single_node workflow placement conflicts with explicit per-agent placements: "
            + details
        )

    requested_requirements = _workflow_node_requirements(
        manifest,
        runtime_model_requirements=runtime_model_requirements,
    )
    requirements = requested_requirements
    candidates, rejections = _workflow_placement_candidates(
        names,
        resource_nodes=resource_nodes,
        system_nodes=system_nodes,
        requirements=requirements,
        explicit_node=distinct_explicit[0] if distinct_explicit else "",
    )
    model_fallbacks: list[dict[str, Any]] = []
    if not candidates and runtime_model_requirements:
        fallback_model_requirements, model_fallbacks = (
            _workflow_model_fallback_requirements(runtime_model_requirements)
        )
        if model_fallbacks:
            requirements = _workflow_node_requirements(
                manifest,
                runtime_model_requirements=fallback_model_requirements,
            )
            candidates, fallback_rejections = _workflow_placement_candidates(
                names,
                resource_nodes=resource_nodes,
                system_nodes=system_nodes,
                requirements=requirements,
                explicit_node=distinct_explicit[0] if distinct_explicit else "",
            )
            if candidates:
                rejections = fallback_rejections
            else:
                rejections = {
                    name: list(
                        dict.fromkeys(
                            rejections.get(name, [])
                            + [
                                "fallback placement: " + reason
                                for reason in fallback_rejections.get(name, [])
                            ]
                        )
                    )
                    for name in sorted(set(rejections) | set(fallback_rejections))
                }

    if not candidates:
        diagnostics = (
            "; ".join(
                f"{name}: {', '.join(reasons)}"
                for name, reasons in sorted(rejections.items())
            )
            or "no eligible runtime nodes"
        )
        raise RuntimeError(
            "No single runtime node can run this workflow. Per-node rejection reasons: "
            + diagnostics
        )

    _, selected_node, capacity = sorted(
        candidates,
        key=lambda item: (-item[0][0], -item[0][1], item[0][2]),
    )[0]
    _apply_workflow_node_constraint(manifest, selected_node)
    placement = {
        "mode": "single_node",
        "selected_node": selected_node,
        "selection": "best_fit_accelerator_headroom",
        "capacity": capacity,
        "requirements": requirements,
        "rejections": rejections,
    }
    if model_fallbacks:
        placement["requested_requirements"] = requested_requirements
        placement["model_fallbacks"] = model_fallbacks
    metadata = manifest.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        manifest["metadata"] = metadata
    metadata["mn_workflow_placement"] = placement
    runtime = (
        manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
    )
    if runtime:
        runtime_placement = (
            runtime.get("placement")
            if isinstance(runtime.get("placement"), dict)
            else {}
        )
        runtime_placement["mode"] = "single_node"
        runtime_placement["selected_node"] = selected_node
        runtime["placement"] = runtime_placement
        manifest["runtime"] = runtime
    return placement


def _workflow_placement_candidates(
    names: list[str],
    *,
    resource_nodes: dict[str, dict[str, Any]],
    system_nodes: dict[str, dict[str, Any]],
    requirements: dict[str, Any],
    explicit_node: str,
) -> tuple[
    list[tuple[tuple[float, float, str], str, dict[str, Any]]],
    dict[str, list[str]],
]:
    candidates: list[tuple[tuple[float, float, str], str, dict[str, Any]]] = []
    rejections: dict[str, list[str]] = {}
    for name in names:
        resource = resource_nodes.get(name) or system_nodes.get(name) or {}
        system = system_nodes.get(name) or resource
        reasons = _workflow_node_rejections(
            name,
            resource=resource,
            system=system,
            requirements=requirements,
            explicit_node=explicit_node,
        )
        if reasons:
            rejections[name] = reasons
            continue
        capacity = _workflow_node_capacity(resource, system)
        # Higher free GPU-memory headroom wins; after that prefer lower load,
        # then the node name for deterministic placement.
        headroom = capacity["gpu_memory_free_mb"] - requirements["min_gpu_memory_mb"]
        score = (headroom, -_workflow_node_load(resource, system), name)
        candidates.append((score, name, capacity))
    return candidates, rejections


def _workflow_model_fallback_requirements(
    runtime_model_requirements: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Substitute catalog fallbacks only when preferred placement cannot fit.

    Runtime model preparation already knows how to use a catalog fallback.  This
    keeps single-node placement aligned with that policy instead of rejecting a
    workflow before LiteLLM can route ``default`` to its portable model.
    """

    effective: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    for requirement in runtime_model_requirements:
        if not isinstance(requirement, dict):
            continue
        entry = requirement.get("entry") if isinstance(requirement.get("entry"), dict) else requirement
        fallback_ref = str(entry.get("fallback_model") or "").strip()
        if not fallback_ref:
            effective.append(requirement)
            continue
        try:
            fallback_entry = resolve_model_entry(fallback_ref)
        except Exception:
            effective.append(requirement)
            continue
        fallback_requirement = dict(requirement)
        fallback_requirement["entry"] = fallback_entry
        fallback_requirement["model"] = str(
            fallback_entry.get("id")
            or fallback_entry.get("model")
            or fallback_ref
        )
        fallback_requirement["label"] = str(
            fallback_entry.get("id")
            or fallback_requirement.get("label")
            or fallback_ref
        )
        effective.append(fallback_requirement)
        records.append(
            {
                "preferred": {
                    "id": str(entry.get("id") or ""),
                    "model": str(entry.get("model") or ""),
                    "dmr_model": str(entry.get("dmr_model") or ""),
                    "aliases": list(entry.get("aliases") or []),
                },
                "fallback": fallback_entry,
            }
        )
    return effective, records


def _workflow_placement_model_fallback_records(
    env_overrides: Optional[dict[str, str]],
) -> tuple[dict[str, Any], ...]:
    raw = str(
        (env_overrides or {}).get("MN_SELECTED_RUNTIME_MODEL_FALLBACKS_JSON")
        or os.environ.get("MN_SELECTED_RUNTIME_MODEL_FALLBACKS_JSON")
        or ""
    ).strip()
    if not raw:
        return ()
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return ()
    if not isinstance(decoded, list):
        return ()
    return tuple(item for item in decoded if isinstance(item, dict))


def _workflow_placement_fallback_for_entry(
    entry: dict[str, Any],
) -> dict[str, Any] | None:
    candidate_keys = _workflow_model_match_keys(entry)
    if not candidate_keys:
        return None
    for record in _WORKFLOW_PLACEMENT_MODEL_FALLBACKS.get():
        preferred = record.get("preferred")
        fallback = record.get("fallback")
        if not isinstance(preferred, dict) or not isinstance(fallback, dict):
            continue
        if candidate_keys & _workflow_model_match_keys(preferred):
            return fallback
    return None


def _workflow_model_match_keys(entry: dict[str, Any]) -> set[str]:
    values = [
        entry.get("id"),
        entry.get("model"),
        entry.get("dmr_model"),
        entry.get("api_model"),
        *(entry.get("aliases") or []),
    ]
    return {str(value).strip().lower() for value in values if str(value or "").strip()}


def _preflight_and_apply_runtime_model_placement(
    manifest: dict[str, Any],
    *,
    runtime_model_requirements: Optional[list[dict[str, Any]]] = None,
    resource_report: Optional[dict[str, Any]] = None,
    system_summary: Optional[dict[str, Any]] = None,
    env: Optional[dict[str, str]] = None,
) -> dict[str, Any] | None:
    """Verify every required runtime model before any prepare request is sent."""

    if not runtime_model_requirements:
        if _workflow_placement_mode(
            manifest, env=env
        ) is None and not _workflow_uses_docker_worker(manifest):
            return None
        return _resolve_and_apply_workflow_placement(
            manifest,
            resource_report=resource_report,
            system_summary=system_summary,
            env=env,
        )

    if _workflow_placement_mode(manifest, env=env) == "distributed":
        _validate_distributed_runtime_model_feasibility(
            runtime_model_requirements,
            resource_report=resource_report,
            system_summary=system_summary,
        )
        return None

    return _resolve_and_apply_workflow_placement(
        manifest,
        runtime_model_requirements=runtime_model_requirements,
        resource_report=resource_report,
        system_summary=system_summary,
        env=env,
    )


def _workflow_uses_docker_worker(manifest: dict[str, Any]) -> bool:
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        runner = str(config.get("runner_module") or node.get("runner_module") or "")
        if runner == "MirrorNeuron.Runner.DockerWorker":
            return True
    return False


def _validate_distributed_runtime_model_feasibility(
    runtime_model_requirements: list[dict[str, Any]],
    *,
    resource_report: Optional[dict[str, Any]] = None,
    system_summary: Optional[dict[str, Any]] = None,
) -> None:
    resources = (
        resource_report
        if isinstance(resource_report, dict)
        else _runtime_resource_report()
    )
    if system_summary is None:
        try:
            decoded = json.loads(client.get_system_summary())
        except Exception as exc:
            raise RuntimeError(
                f"could not inspect runtime nodes for model feasibility: {exc}"
            ) from exc
        system_summary = decoded if isinstance(decoded, dict) else {}

    resource_nodes = _workflow_nodes_by_name(resources)
    system_nodes = _workflow_nodes_by_name(system_summary)
    names = sorted(set(resource_nodes) | set(system_nodes))
    if not names:
        raise RuntimeError(
            "No runtime nodes were reported while checking model feasibility."
        )

    for model_requirement in runtime_model_requirements:
        requirements = _workflow_node_requirements(
            {"nodes": []},
            runtime_model_requirements=[model_requirement],
        )
        rejections: dict[str, list[str]] = {}
        for name in names:
            resource = resource_nodes.get(name) or system_nodes.get(name) or {}
            system = system_nodes.get(name) or resource
            reasons = _workflow_node_rejections(
                name,
                resource=resource,
                system=system,
                requirements=requirements,
                explicit_node="",
            )
            if reasons:
                rejections[name] = reasons
        if len(rejections) != len(names):
            continue
        label = str(
            model_requirement.get("label")
            or model_requirement.get("model")
            or "runtime model"
        )
        diagnostics = "; ".join(
            f"{name}: {', '.join(reasons)}"
            for name, reasons in sorted(rejections.items())
        )
        raise RuntimeError(
            f"No runtime node can prepare required model {label}. Per-node rejection reasons: {diagnostics}"
        )


def _workflow_placement_mode(
    manifest: dict[str, Any], *, env: Optional[dict[str, str]]
) -> str | None:
    runtime = (
        manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
    )
    declaration = (
        runtime.get("placement") if isinstance(runtime.get("placement"), dict) else {}
    )
    raw_mode = str(declaration.get("mode") or "").strip().lower().replace("-", "_")
    if raw_mode and raw_mode not in {"single_node", "distributed"}:
        raise RuntimeError(
            "runtime.placement.mode must be either 'single_node' or 'distributed'."
        )
    if raw_mode:
        return raw_mode
    values = os.environ if env is None else env
    # Compatibility only: older callers can explicitly opt into their former
    # distributed scheduling behavior while they migrate to runtime.placement.
    if (
        str(values.get("MN_BLUEPRINT_SINGLE_NODE_AGENTS", "")).strip().lower()
        in FALSE_VALUES
    ):
        return "distributed"
    return None


def _workflow_requires_single_node(manifest: dict[str, Any]) -> bool:
    runtime = (
        manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
    )
    if isinstance(runtime.get("models"), dict) and runtime["models"]:
        return True
    if isinstance(runtime.get("memory"), dict) and runtime["memory"]:
        return True
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        runner = str(config.get("runner_module") or "").strip()
        if runner in {
            "MirrorNeuron.Runner.DockerWorker",
            "MirrorNeuron.Runner.HostLocal",
        }:
            return True
        if config.get("gpus") not in (None, "", "none", "None"):
            return True
    return False


def _workflow_explicit_node_placements(manifest: dict[str, Any]) -> dict[str, str]:
    placements: dict[str, str] = {}
    for index, node in enumerate(manifest_nodes(manifest)):
        node_id = str(node.get("node_id") or node.get("id") or f"node-{index}")
        explicit = _node_explicit_node_name(node)
        if explicit:
            placements[node_id] = explicit
    return placements


def _node_explicit_node_name(node: dict[str, Any]) -> str:
    policies = node.get("policies") if isinstance(node.get("policies"), dict) else {}
    scheduler = (
        policies.get("scheduler") if isinstance(policies.get("scheduler"), dict) else {}
    )
    for value in (
        scheduler.get("preferred_node"),
        scheduler.get("preferredNode"),
        policies.get("preferred_node"),
        policies.get("preferredNode"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    constraints = (
        node.get("constraints") if isinstance(node.get("constraints"), list) else []
    )
    for constraint in constraints:
        if not isinstance(constraint, dict) or not _is_node_name_constraint(constraint):
            continue
        if str(constraint.get("operator") or "==").strip().lower() not in {
            "==",
            "=",
            "eq",
            "equals",
        }:
            continue
        value = str(constraint.get("value") or "").strip()
        if value:
            return value
    return ""


def _workflow_node_requirements(
    manifest: dict[str, Any],
    *,
    runtime_model_requirements: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    requirements: dict[str, Any] = {
        "constraints": [],
        "resources": [],
        "min_gpu_count": 0.0,
        "min_gpu_memory_mb": 0.0,
        "requires_nvidia": False,
        "requires_native_model_prepare": False,
        "requires_native_docker_worker_prepare": False,
        "model_requirements": [],
        "sources": [],
    }
    for index, node in enumerate(manifest_nodes(manifest)):
        node_id = str(node.get("node_id") or node.get("id") or f"node-{index}")
        constraints = (
            node.get("constraints") if isinstance(node.get("constraints"), list) else []
        )
        if constraints:
            requirements["constraints"].extend(
                (node_id, constraint)
                for constraint in constraints
                if isinstance(constraint, dict)
            )
        resources = (
            node.get("resources") if isinstance(node.get("resources"), dict) else {}
        )
        if resources:
            requirements["resources"].append((node_id, resources))
            _merge_workflow_resource_requirement(
                requirements, resources, source=node_id
            )
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if str(config.get("runner_module") or "") == "MirrorNeuron.Runner.DockerWorker":
            requirements["sources"].append(f"{node_id}:DockerWorker")
            requirements["requires_native_docker_worker_prepare"] = True
            if str(config.get("gpus") or "").strip().lower() not in {
                "",
                "none",
                "false",
                "0",
            }:
                requirements["requires_nvidia"] = True
                requirements["min_gpu_count"] = max(requirements["min_gpu_count"], 1.0)
        if str(config.get("gpus") or "").strip().lower() not in {
            "",
            "none",
            "false",
            "0",
        }:
            requirements["requires_nvidia"] = True
            requirements["min_gpu_count"] = max(requirements["min_gpu_count"], 1.0)

    top_level = (
        manifest.get("requirements")
        if isinstance(manifest.get("requirements"), dict)
        else {}
    )
    gpu = top_level.get("gpu") if isinstance(top_level.get("gpu"), dict) else {}
    if gpu:
        requirements["min_gpu_count"] = max(
            requirements["min_gpu_count"],
            _workflow_number(gpu.get("min_count") or gpu.get("count") or 1),
        )
        requirements["min_gpu_memory_mb"] = max(
            requirements["min_gpu_memory_mb"],
            _workflow_number(gpu.get("min_memory_mb")),
        )
        requirements["requires_nvidia"] = (
            requirements["requires_nvidia"]
            or str(gpu.get("vendor") or "").lower() == "nvidia"
        )
        requirements["sources"].append("manifest.requirements.gpu")

    if runtime_model_requirements is None:
        runtime = (
            manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
        )
        models = (
            runtime.get("models") if isinstance(runtime.get("models"), dict) else {}
        )
        runtime_model_requirements = []
        for name, model in models.items():
            if not isinstance(model, dict):
                continue
            model_ref = str(
                model.get("runtime_model") or model.get("model") or ""
            ).strip()
            if not model_ref:
                continue
            try:
                entry = resolve_model_entry(model_ref)
            except Exception:
                entry = model
            runtime_model_requirements.append(
                {
                    "label": str(name),
                    "model": model_ref,
                    "entry": entry,
                    "source": f"runtime.models.{name}",
                }
            )

    for model in runtime_model_requirements:
        if not isinstance(model, dict):
            continue
        entry = model.get("entry") if isinstance(model.get("entry"), dict) else model
        _merge_workflow_model_requirement(
            requirements,
            entry,
            label=str(
                model.get("label")
                or model.get("id")
                or model.get("model")
                or "runtime model"
            ),
            source=str(model.get("source") or "runtime model"),
        )
    return requirements


def _merge_workflow_model_requirement(
    requirements: dict[str, Any],
    entry: dict[str, Any],
    *,
    label: str,
    source: str,
) -> None:
    requirements["requires_native_model_prepare"] = True
    requirement = _workflow_model_gpu_requirement(entry)
    model_requirement = {
        "label": label,
        "source": source,
        "min_count": requirement["min_count"] if requirement else 0.0,
        "min_memory_mb": requirement["min_memory_mb"] if requirement else 0.0,
    }
    if model_requirement not in requirements["model_requirements"]:
        requirements["model_requirements"].append(model_requirement)
    if requirement:
        requirements["min_gpu_count"] = max(
            requirements["min_gpu_count"], requirement["min_count"]
        )
        requirements["min_gpu_memory_mb"] = max(
            requirements["min_gpu_memory_mb"], requirement["min_memory_mb"]
        )
    requirements["sources"].append(source)


def _workflow_model_gpu_requirement(entry: dict[str, Any]) -> dict[str, float] | None:
    raw = (
        entry.get("requirements") if isinstance(entry.get("requirements"), dict) else {}
    )
    values = [
        _workflow_number(raw.get("min_vram_gb")) * 1024,
        _workflow_number(raw.get("min_unified_memory_gb")) * 1024,
        _workflow_number(raw.get("min_memory_mb")),
    ]
    required_memory = max(values, default=0.0)
    if required_memory <= 0:
        return None
    return {"min_count": 1.0, "min_memory_mb": required_memory}


def _merge_workflow_resource_requirement(
    requirements: dict[str, Any], resource: dict[str, Any], *, source: str
) -> None:
    gpu_count = _workflow_number(resource.get("gpu_count"))
    requirements["min_gpu_count"] = max(requirements["min_gpu_count"], gpu_count)
    devices = (
        resource.get("devices") if isinstance(resource.get("devices"), list) else []
    )
    for device in devices:
        if not isinstance(device, dict):
            continue
        kind = str(device.get("kind") or "").lower()
        if kind != "gpu" and "gpu" not in str(device.get("type") or "").lower():
            continue
        requirements["min_gpu_count"] = max(
            requirements["min_gpu_count"], _workflow_number(device.get("count") or 1)
        )
        requirements["min_gpu_memory_mb"] = max(
            requirements["min_gpu_memory_mb"],
            _workflow_number(device.get("min_memory_mb")),
        )
        if (
            str(device.get("vendor") or "").strip().lower() == "nvidia"
            or str(device.get("driver") or "").strip().lower() == "cuda"
        ):
            requirements["requires_nvidia"] = True
    if gpu_count or devices:
        requirements["sources"].append(f"{source}:resources")


def _workflow_node_rejections(
    name: str,
    *,
    resource: dict[str, Any],
    system: dict[str, Any],
    requirements: dict[str, Any],
    explicit_node: str,
) -> list[str]:
    reasons: list[str] = []
    for source, facts in (("resource", resource), ("system", system)):
        status = str(facts.get("status") or "healthy").strip().lower()
        if status not in {"healthy", "joining"}:
            reasons.append(f"{source}_status={status}")
        if facts.get("scheduling_eligible") is False:
            reasons.append(f"{source}_scheduling_ineligible")
        if _workflow_truthy(facts.get("drain")):
            reasons.append(f"{source}_draining")
        if _workflow_truthy(facts.get("maintenance")):
            reasons.append(f"{source}_maintenance")
    if explicit_node and name != explicit_node:
        reasons.append(f"explicit_placement_requires={explicit_node}")
    for node_id, constraint in requirements["constraints"]:
        if not _workflow_constraint_matches(constraint, name, resource, system):
            reasons.append(f"{node_id}:constraint_unsatisfied")
    for node_id, requested in requirements["resources"]:
        if not _workflow_resources_match(requested, resource, system):
            reasons.append(f"{node_id}:resources_unsatisfied")
    capacity = _workflow_node_capacity(resource, system)
    if capacity["gpu_count"] < requirements["min_gpu_count"]:
        reasons.append(
            f"gpu_count={int(capacity['gpu_count'])} < required={int(requirements['min_gpu_count'])}"
        )
    if capacity["gpu_memory_free_mb"] < requirements["min_gpu_memory_mb"]:
        reasons.append(
            f"gpu_memory_free_mb={int(capacity['gpu_memory_free_mb'])} < required={int(requirements['min_gpu_memory_mb'])}"
        )
    for model in requirements.get("model_requirements") or []:
        if not isinstance(model, dict):
            continue
        label = str(model.get("label") or "runtime model")
        min_count = _workflow_number(model.get("min_count"))
        min_memory_mb = _workflow_number(model.get("min_memory_mb"))
        if capacity["gpu_count"] < min_count:
            reasons.append(
                f"model {label}: gpu_count={int(capacity['gpu_count'])} < required={int(min_count)}"
            )
        if capacity["gpu_memory_free_mb"] < min_memory_mb:
            reasons.append(
                f"model {label}: gpu_memory_free_mb={int(capacity['gpu_memory_free_mb'])} < required={int(min_memory_mb)}"
            )
    if requirements["requires_nvidia"] and "nvidia" not in _workflow_node_capabilities(
        resource, system
    ):
        reasons.append("nvidia_cuda_required")
    if not _workflow_node_is_local(system) and (
        requirements["requires_native_model_prepare"]
        or requirements["requires_native_docker_worker_prepare"]
    ):
        native = _node_native_sdk_grpc_info(system)
        if not native:
            reasons.append("native_sdk_grpc_missing")
        elif native.get("enabled") is False:
            reasons.append("native_sdk_grpc_disabled")
        elif (
            not str(native.get("target") or native.get("host") or "").strip()
            or not str(native.get("port") or "").strip()
        ):
            reasons.append("native_sdk_grpc_incomplete")
        elif requirements["requires_native_docker_worker_prepare"]:
            capabilities = native.get("capabilities")
            if isinstance(capabilities, str):
                capabilities = [capabilities]
            if (
                isinstance(capabilities, list)
                and capabilities
                and "docker_worker_prepare_v1"
                not in {str(item) for item in capabilities}
            ):
                reasons.append("native_sdk_capability_missing:docker_worker_prepare_v1")
    return list(dict.fromkeys(reasons))


def _workflow_constraint_matches(
    constraint: dict[str, Any],
    name: str,
    resource: dict[str, Any],
    system: dict[str, Any],
) -> bool:
    attribute = str(
        constraint.get("attribute")
        or constraint.get("target")
        or constraint.get("l_target")
        or ""
    ).strip("${}")
    operator = str(constraint.get("operator") or "==").strip().lower()
    expected = constraint.get("value")
    values = _workflow_placement_values(attribute, name, resource, system)
    expected_values = expected if isinstance(expected, list) else [expected]
    wanted = {
        str(value).strip().lower()
        for value in expected_values
        if str(value or "").strip()
    }
    if operator in {"==", "=", "eq", "equals"}:
        return bool(wanted) and next(iter(wanted)) in values
    if operator in {"contains", "in"}:
        return bool(wanted & values)
    if operator in {"contains_all", "all"}:
        return wanted.issubset(values)
    return False


def _workflow_resources_match(
    requested: dict[str, Any], resource: dict[str, Any], system: dict[str, Any]
) -> bool:
    capacity = _workflow_node_capacity(resource, system)
    if capacity["gpu_count"] < _workflow_number(requested.get("gpu_count")):
        return False
    devices = _workflow_node_devices(resource, system)
    for needed in requested.get("devices") or []:
        if not isinstance(needed, dict):
            continue
        count = int(_workflow_number(needed.get("count") or 1))
        matches = [
            device for device in devices if _workflow_device_matches(needed, device)
        ]
        if len(matches) < count:
            return False
    return True


def _workflow_device_matches(needed: dict[str, Any], actual: dict[str, Any]) -> bool:
    for key in ("kind", "type", "vendor", "driver"):
        expected = str(needed.get(key) or "").strip().lower()
        if expected and str(actual.get(key) or "").strip().lower() != expected:
            return False
    minimum = _workflow_number(needed.get("min_memory_mb"))
    memory = _workflow_number(
        actual.get("memory_free_mb") or actual.get("memory_total_mb")
    )
    return memory >= minimum


def _workflow_node_capacity(
    resource: dict[str, Any], system: dict[str, Any]
) -> dict[str, float]:
    devices = [
        device
        for device in _workflow_node_devices(resource, system)
        if _workflow_is_gpu_device(device)
    ]
    return {
        "gpu_count": max(
            _workflow_number(resource.get("gpu_count")), float(len(devices))
        ),
        "gpu_memory_free_mb": max(
            _workflow_number(resource.get("gpu_memory_free_mb")),
            sum(
                _workflow_number(
                    device.get("memory_free_mb") or device.get("memory_total_mb")
                )
                for device in devices
            ),
        ),
    }


def _workflow_node_devices(
    resource: dict[str, Any], system: dict[str, Any]
) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    for facts in (resource, system):
        for container in (
            facts,
            facts.get("hardware") if isinstance(facts.get("hardware"), dict) else {},
        ):
            raw = container.get("devices") or container.get("gpu")
            if isinstance(raw, list):
                devices.extend(item for item in raw if isinstance(item, dict))
            elif isinstance(raw, dict):
                devices.append(raw)
    unique: dict[str, dict[str, Any]] = {}
    for index, device in enumerate(devices):
        unique[
            str(
                device.get("id")
                or device.get("uuid")
                or f"{device.get('name')}:{index}"
            )
        ] = device
    return list(unique.values())


def _workflow_node_capabilities(
    resource: dict[str, Any], system: dict[str, Any]
) -> set[str]:
    capabilities: set[str] = set()
    for facts in (resource, system):
        for container in (
            facts,
            facts.get("hardware") if isinstance(facts.get("hardware"), dict) else {},
        ):
            raw = container.get("capabilities")
            if isinstance(raw, str):
                capabilities.add(raw.lower())
            elif isinstance(raw, list):
                capabilities.update(str(value).lower() for value in raw)
    for device in _workflow_node_devices(resource, system):
        capabilities.update(
            str(value).lower() for value in device.get("capabilities") or []
        )
        for key in ("kind", "type", "vendor", "driver"):
            value = str(device.get(key) or "").strip().lower()
            if value:
                capabilities.add(value)
    return capabilities


def _workflow_placement_values(
    attribute: str, name: str, resource: dict[str, Any], system: dict[str, Any]
) -> set[str]:
    if attribute in {"node", "node.name", "node.unique.name"}:
        return {name.lower()}
    if attribute in {"capability", "capabilities"}:
        return _workflow_node_capabilities(resource, system)
    values: set[str] = set()
    for facts in (resource, system):
        for container in (
            facts,
            facts.get("hardware") if isinstance(facts.get("hardware"), dict) else {},
        ):
            value = container.get(attribute)
            if isinstance(value, list):
                values.update(str(item).strip().lower() for item in value)
            elif value is not None:
                values.add(str(value).strip().lower())
    return values


def _apply_workflow_node_constraint(
    manifest: dict[str, Any], selected_node: str
) -> None:
    for index, node in enumerate(manifest_nodes(manifest)):
        node_id = str(node.get("node_id") or node.get("id") or f"node-{index}")
        explicit = _node_explicit_node_name(node)
        if explicit and explicit != selected_node:
            raise RuntimeError(
                f"single_node workflow placement selected {selected_node}, but {node_id} explicitly requires {explicit}"
            )
        policies = (
            node.get("policies") if isinstance(node.get("policies"), dict) else {}
        )
        scheduler = (
            policies.get("scheduler")
            if isinstance(policies.get("scheduler"), dict)
            else {}
        )
        scheduler["preferred_node"] = selected_node
        policies["scheduler"] = scheduler
        node["policies"] = policies
        constraints = (
            node.get("constraints") if isinstance(node.get("constraints"), list) else []
        )
        existing = [
            constraint for constraint in constraints if isinstance(constraint, dict)
        ]
        if not any(
            _is_node_name_constraint(constraint)
            and str(constraint.get("operator") or "==").strip().lower()
            in {"==", "=", "eq", "equals"}
            and str(constraint.get("value") or "").strip() == selected_node
            for constraint in existing
        ):
            existing.append(
                {
                    "attribute": "node.name",
                    "operator": "==",
                    "value": selected_node,
                    "source": "mn-cli-workflow-placement",
                }
            )
        node["constraints"] = existing


def _workflow_nodes_by_name(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    nodes = report.get("nodes") if isinstance(report, dict) else None
    return {
        str(node.get("name") or node.get("node") or "").strip(): node
        for node in nodes or []
        if isinstance(node, dict)
        and str(node.get("name") or node.get("node") or "").strip()
    }


def _workflow_node_load(resource: dict[str, Any], system: dict[str, Any]) -> float:
    values: list[float] = []
    for facts in (resource, system):
        for key in ("load", "cpu_load", "active_jobs", "running_jobs", "allocations"):
            values.append(_workflow_number(facts.get(key)))
    return max(values, default=0.0)


def _workflow_is_gpu_device(device: dict[str, Any]) -> bool:
    return (
        str(device.get("kind") or "").lower() == "gpu"
        or "gpu" in str(device.get("type") or "").lower()
        or "gpu" in {str(value).lower() for value in device.get("capabilities") or []}
    )


def _workflow_number(value: Any) -> float:
    try:
        return max(float(value or 0), 0.0)
    except (TypeError, ValueError):
        return 0.0


def _workflow_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "draining",
        "maintenance",
    }


def _workflow_node_is_local(node: dict[str, Any]) -> bool:
    return node.get("self") is True or node.get("self?") is True


def _local_runtime_node_name() -> str:
    try:
        summary = json.loads(client.get_system_summary())
    except Exception:
        return ""
    nodes = summary.get("nodes") if isinstance(summary, dict) else None
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        if node.get("self?") is True or node.get("self") is True:
            return str(node.get("name") or node.get("node") or "").strip()
    return ""


def _node_has_explicit_node_placement(node: dict[str, Any]) -> bool:
    policies = node.get("policies") if isinstance(node.get("policies"), dict) else {}
    scheduler = (
        policies.get("scheduler") if isinstance(policies.get("scheduler"), dict) else {}
    )
    if str(
        scheduler.get("preferred_node") or scheduler.get("preferredNode") or ""
    ).strip():
        return True
    if str(
        policies.get("preferred_node") or policies.get("preferredNode") or ""
    ).strip():
        return True
    constraints = (
        node.get("constraints") if isinstance(node.get("constraints"), list) else []
    )
    return any(
        _is_node_name_constraint(constraint)
        for constraint in constraints
        if isinstance(constraint, dict)
    )


def _is_node_name_constraint(constraint: dict[str, Any]) -> bool:
    attribute = str(
        constraint.get("attribute")
        or constraint.get("target")
        or constraint.get("l_target")
        or ""
    ).strip("${}")
    return attribute in {"node", "node.name", "node.unique.name"}


def _install_runtime_cluster_model(
    *,
    requirement: dict[str, Any],
    entry: dict[str, Any],
    model: dict[str, Any],
    cluster: dict[str, Any],
    backend: str,
    context_size: Any,
    force: bool,
) -> dict[str, Any]:
    node = str(cluster.get("node") or "").strip()
    model_ref = str(model.get("model") or docker_model_name(entry))
    model_label = str(model.get("id") or model_ref)
    node_label = node or "selected runtime node"
    node_endpoint = _cluster_node_endpoint(node)
    local_target = _cluster_node_endpoint_is_local(node_endpoint)
    transport = "local runtime coordinator" if local_target else "native SDK gRPC"
    print_info(console, f"Preparing runtime model {model_label} on {node_label} with {transport}…")
    runtime_client = _runtime_model_prepare_client(node, node_endpoint)
    prepare_payload = build_prepare_runtime_model_request(
        requirement=requirement,
        entry=entry,
        model={**model, "model": model_ref},
        node=node,
        backend=backend,
        context_size=context_size,
        force=force,
        source="mn-cli",
    )
    if entry.get("customize_mode") is True:
        print_warning(console, CUSTOM_MODEL_WARNING)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        disable=not use_progress(),
    ) as progress:
        progress.add_task(
            (
                f"[cyan]Checking and preparing {model_label} on {node_label}; "
                f"waiting for {'local' if local_target else 'remote'} Docker Model Runner..."
            ),
            total=None,
        )
        payload = _prepare_runtime_model_with_retry(runtime_client, prepare_payload)
    endpoint = (
        payload.get("endpoint") if isinstance(payload.get("endpoint"), dict) else {}
    )
    upstream_endpoint = _cluster_runtime_model_upstream_endpoint(
        entry=entry,
        node=node,
        node_endpoint=node_endpoint,
        payload=payload,
        endpoint=endpoint,
    )
    return {
        "install": payload,
        "endpoint": upstream_endpoint,
    }


def _cluster_runtime_model_upstream_endpoint(
    *,
    entry: dict[str, Any],
    node: str,
    node_endpoint: dict[str, Any],
    payload: dict[str, Any],
    endpoint: dict[str, Any],
) -> dict[str, Any]:
    if _cluster_node_endpoint_is_local(node_endpoint):
        local_endpoint = docker_model_runner_endpoint(
            entry, node=node, source="local-dmr"
        )
        local_endpoint["node"] = node
        return local_endpoint

    return remote_runtime_model_endpoint(
        entry=entry,
        node=node,
        node_host=str(node_endpoint.get("host") or ""),
        payload=payload,
    )


def _cluster_node_endpoint_is_local(node_endpoint: dict[str, Any]) -> bool:
    node = (
        node_endpoint.get("node") if isinstance(node_endpoint.get("node"), dict) else {}
    )
    if node.get("self?") is True or node.get("self") is True:
        return True
    host = str(node_endpoint.get("host") or "").strip().lower()
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True
    if host in _local_host_addresses():
        return True

    node_name = str(node.get("name") or node.get("node") or "").strip()
    return bool(node_name and node_name == _local_runtime_node_name())


@lru_cache(maxsize=1)
def _local_host_addresses() -> set[str]:
    hostnames = {"localhost", "127.0.0.1", "::1", "::", "0.0.0.0"}
    candidates: set[str] = {address.lower() for address in hostnames}
    try:
        candidates.update(_resolved_local_hostnames())
    except Exception:
        pass
    try:
        parsed = urllib.parse.urlparse(f"//{config.grpc_target}")
        if parsed.hostname:
            candidates.add(parsed.hostname.lower())
    except Exception:
        pass
    for env_key in ("MN_API_HOST", "MN_GRPC_TARGET", "MN_API_BASE_URL"):
        env_value = os.getenv(env_key, "")
        if env_value:
            candidates.update(_extract_host_candidates_from_text(env_value))
    return candidates


def _extract_host_candidates_from_text(value: str) -> set[str]:
    candidates: set[str] = set()
    text = str(value or "").strip()
    if not text:
        return candidates
    for part in (
        text,
        f"//{text}",
    ):
        parsed = urllib.parse.urlparse(part)
        if parsed.hostname:
            candidates.add(parsed.hostname.lower())
    return candidates


def _resolved_local_hostnames() -> set[str]:
    addresses: set[str] = set()
    try:
        addresses.add(socket.gethostbyname(socket.gethostname()).lower())
    except Exception:
        pass
    try:
        addresses.update(
            addr.lower() for addr in socket.gethostbyname_ex(socket.gethostname())[2]
        )
    except Exception:
        pass
    try:
        for info in socket.getaddrinfo(
            socket.gethostname(), None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
        ):
            if len(info) >= 5:
                entry = info[4][0]
                if isinstance(entry, str):
                    addresses.add(entry.lower().split("%", 1)[0])
    except Exception:
        pass
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("10.255.255.255", 1))
            addresses.add(probe.getsockname()[0].lower())
        finally:
            probe.close()
    except Exception:
        pass
    return addresses


def _runtime_model_prepare_timeout_seconds() -> float:
    return runtime_model_prepare_timeout_seconds()


def _print_runtime_model_install_summary(summary: dict[str, Any]) -> None:
    models = summary.get("models") or []
    if not models:
        return

    failed = [item for item in models if str(item.get("status") or "") == "failed"]
    skipped = [item for item in models if str(item.get("status") or "") == "skipped"]
    for item in failed:
        console.print(
            f"[red]Runtime model preparation failed: {_runtime_model_failure_label(item)}[/red]"
        )
    for item in skipped:
        console.print(
            f"[yellow]Runtime model preparation skipped: {_runtime_model_failure_label(item)}[/yellow]"
        )

    prepared = [
        item
        for item in models
        if str(item.get("status") or "")
        in {
            "installed",
            "already_installed",
            "service_required",
            "cluster_provided",
            "runtime_node_install",
            "runtime_node_already_installed",
            "runtime_node_installed",
            "fallback_model",
            "service_registry",
            "model_remote",
            "explicit_config",
        }
    ]
    if prepared:
        labels = ", ".join(_runtime_model_ready_label(item) for item in prepared[:4])
        if len(prepared) > 4:
            labels = f"{labels}, +{len(prepared) - 4} more"
        console.print(f"[green]Runtime models ready:[/green] {labels}")


def _runtime_model_failure_label(item: dict[str, Any]) -> str:
    label = str(item.get("id") or item.get("model") or "runtime model")
    node = _runtime_model_ready_node(item) or "the selected runtime node"
    stage = str(item.get("prepare_stage") or "prepare")
    error = str(item.get("error") or "runtime model preparation failed")
    code = str(item.get("error_code") or "model.prepare_failed")
    return f"{label} on {node} ({stage}): {error} (code={code})"


def _runtime_model_ready_label(item: dict[str, Any]) -> str:
    label = str(item.get("id") or item.get("model") or "runtime model")
    fallback = item.get("fallback") if isinstance(item.get("fallback"), dict) else {}
    if str(item.get("status") or "") == "fallback_model" and fallback:
        fallback_label = str(
            fallback.get("id") or fallback.get("model") or "fallback model"
        )
        return f"{label} -> {fallback_label}"
    status = str(item.get("status") or "")
    if status in {"runtime_node_installed", "runtime_node_already_installed"}:
        node = _runtime_model_ready_node(item)
        if node:
            if status == "runtime_node_already_installed":
                return f"{label} already installed on {node}"
            return f"{label} installed on {node}"
    return label


def _runtime_model_ready_node(item: dict[str, Any]) -> str:
    for value in (
        item.get("node"),
        (item.get("endpoint") or {}).get("node")
        if isinstance(item.get("endpoint"), dict)
        else None,
        (item.get("cluster") or {}).get("node")
        if isinstance(item.get("cluster"), dict)
        else None,
        (item.get("install") or {}).get("node")
        if isinstance(item.get("install"), dict)
        else None,
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


__all__ = [name for name in globals() if not name.startswith("__")]
