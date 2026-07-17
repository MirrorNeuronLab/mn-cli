from ..common import *
from ..live import *
from ..outputs import *
from contextlib import contextmanager
from mn_cli.libs.workflow_progress import build_workflow_progress_snapshot


_MONITOR_JOB_FIELDS = {
    "agents",
    "created",
    "created_at",
    "description",
    "graph_id",
    "id",
    "job_id",
    "job_name",
    "job_type",
    "manifest",
    "manifest_ref",
    "name",
    "result",
    "result_ref",
    "run_id",
    "runId",
    "runtime_topology",
    "started_at",
    "status",
    "submitted_at",
    "type",
    "workflow_id",
    "workflow_state",
    "workflow_state_ref",
}
_MONITOR_MANIFEST_CACHE: dict[str, tuple[int, int, dict[str, Any]]] = {}
_MONITOR_EVENTS_CACHE: dict[str, tuple[int, int, list[dict[str, Any]]]] = {}


def _job_and_summary_from_data(
    data: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Normalize both flat gRPC and wrapped API job payloads.

    ``Client.get_job`` returns the runtime's flat job JSON, while the HTTP API
    wraps that object under ``job`` and adds a ``summary``.  The monitor must
    preserve the runtime topology and workflow ledger in either shape.
    """

    raw_job = data.get("job") if isinstance(data.get("job"), dict) else None
    if raw_job is None:
        job = {key: value for key, value in data.items() if key != "summary"}
    else:
        job = dict(raw_job)
        for key in _MONITOR_JOB_FIELDS:
            if key not in job and key in data:
                job[key] = data[key]

    summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
    return job, summary


def _monitor_rpc_timeout_seconds() -> float:
    configured = getattr(config, "grpc_timeout_seconds", None)
    try:
        default = max(float(configured or 0), 30.0)
    except (TypeError, ValueError):
        default = 30.0
    raw = os.getenv("MN_JOB_MONITOR_GRPC_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return default
    try:
        return max(float(raw), 1.0)
    except ValueError:
        return default


@contextmanager
def _temporary_monitor_rpc_timeout():
    """Give monitor-only RPCs enough time to read large running job ledgers."""

    missing = object()
    original = getattr(client, "timeout", missing)
    if original is missing or original is None:
        yield
        return

    try:
        should_extend = float(original) < _monitor_rpc_timeout_seconds()
    except (TypeError, ValueError):
        should_extend = False
    if not should_extend:
        yield
        return

    client.timeout = _monitor_rpc_timeout_seconds()
    try:
        yield
    finally:
        client.timeout = original


def _monitor_rpc_code_name(exc: Exception) -> str:
    code_reader = getattr(exc, "code", None)
    if callable(code_reader):
        try:
            code = code_reader()
        except Exception:
            code = None
        return str(getattr(code, "name", code) or "").upper()
    return ""


def _is_transient_monitor_fetch_error(exc: Exception) -> bool:
    if _monitor_rpc_code_name(exc) in {"DEADLINE_EXCEEDED", "UNAVAILABLE"}:
        return True

    text = str(exc).upper()
    return "DEADLINE_EXCEEDED" in text or "UNAVAILABLE" in text


def _monitor_fetch_warning(exc: Exception) -> str:
    code_name = _monitor_rpc_code_name(exc)
    if code_name == "DEADLINE_EXCEEDED":
        return "Temporary job status timeout; retrying."
    if code_name == "UNAVAILABLE":
        return "Runtime temporarily unavailable; retrying."
    return "Temporary job status fetch failure; retrying."


def _monitor_retry_attempts() -> int:
    try:
        return max(int(os.getenv("MN_JOB_MONITOR_RETRY_ATTEMPTS", "3")), 1)
    except ValueError:
        return 3


def _monitor_retry_backoff_seconds() -> float:
    try:
        return max(
            float(os.getenv("MN_JOB_MONITOR_RETRY_BACKOFF_SECONDS", "0.25")), 0.0
        )
    except ValueError:
        return 0.25


def _get_job_for_monitor(job_id: str) -> str:
    """Fetch a job with bounded retries for transient gRPC read failures."""

    attempts = _monitor_retry_attempts()
    backoff = _monitor_retry_backoff_seconds()
    for attempt in range(attempts):
        try:
            with _temporary_monitor_rpc_timeout():
                return client.get_job(job_id)
        except Exception as exc:
            if not _is_transient_monitor_fetch_error(exc) or attempt == attempts - 1:
                raise
            delay = backoff * (attempt + 1)
            logger.warning(
                "Transient monitor job fetch failure for job_id=%s; retrying in %.2fs: %s",
                job_id,
                delay,
                exc,
            )
            if delay:
                time.sleep(delay)
    raise RuntimeError(f"Unable to fetch job {job_id}")


def _read_monitor_json_value(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _read_monitor_json_object(path: Path) -> dict[str, Any] | None:
    loaded = _read_monitor_json_value(path)
    return loaded if isinstance(loaded, dict) else None


def _local_run_store_entries(
    data: dict[str, Any], job: dict[str, Any], summary: dict[str, Any]
) -> list[tuple[Path, dict[str, Any]]]:
    run_ids: list[str] = []
    for mapping in (job, summary, data):
        if not isinstance(mapping, dict):
            continue
        for key in ("run_id", "runId", "blueprint_run_id", "blueprintRunId"):
            value = str(mapping.get(key) or "").strip()
            if value and value not in run_ids:
                run_ids.append(value)
    job_ids = {
        str(mapping.get(key) or "").strip()
        for mapping in (job, summary, data)
        if isinstance(mapping, dict)
        for key in ("job_id", "id")
        if str(mapping.get(key) or "").strip()
    }

    try:
        root = Path(default_runs_root()).expanduser()
    except Exception:
        return []
    if not root.is_dir():
        return []

    entries: list[tuple[Path, dict[str, Any]]] = []
    for run_id in run_ids:
        candidate = root / run_id
        if candidate.is_dir():
            entries.append(
                (candidate, _read_monitor_json_object(candidate / "job.json") or {})
            )

    # A CLI-created run has a small job mapping even when the GetJob response
    # does not carry run_id.  Resolve that mapping without scanning artifacts.
    for mapping_path in root.glob("*/job.json"):
        mapping = _read_monitor_json_object(mapping_path)
        if not mapping:
            continue
        mapped_job_id = str(mapping.get("job_id") or mapping.get("id") or "").strip()
        if mapped_job_id in job_ids and all(
            mapping_path.parent != run_dir for run_dir, _mapping in entries
        ):
            entries.append((mapping_path.parent, mapping))
    return entries


def _read_monitor_manifest(path: Path) -> dict[str, Any] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    cache_key = str(path)
    cached = _MONITOR_MANIFEST_CACHE.get(cache_key)
    if cached and cached[:2] == (stat.st_mtime_ns, stat.st_size):
        return cached[2]

    candidate = _read_monitor_json_object(path)
    if not candidate:
        return None
    try:
        if is_manifest_source(candidate):
            candidate = expand_manifest_source(candidate, root_dir=path.parent)
    except Exception:
        logger.exception("Failed to expand monitor manifest at %s", path)
        return None
    _MONITOR_MANIFEST_CACHE[cache_key] = (
        stat.st_mtime_ns,
        stat.st_size,
        candidate,
    )
    return candidate


def _blueprint_manifest_from_mapping(
    mapping: dict[str, Any],
) -> dict[str, Any] | None:
    blueprint_id = str(mapping.get("blueprint_id") or "").strip()
    source_values: list[str] = []
    explicit_source = str(mapping.get("blueprint_source") or "").strip()
    if explicit_source:
        source_values.append(explicit_source)

    try:
        from mn_cli.libs.blueprint_repository import (
            blueprint_storage_dir_for_source,
            resolved_blueprint_source,
        )

        source, uses_default_cache = resolved_blueprint_source(
            source=None, blueprint_repo=None
        )
        storage = str(
            blueprint_storage_dir_for_source(
                source, use_default_cache=uses_default_cache
            )
        )
        if storage and storage not in source_values:
            source_values.append(storage)
    except Exception:
        logger.debug("Default blueprint source is unavailable", exc_info=True)

    for raw_source in source_values:
        source_path = Path(raw_source).expanduser()
        candidates: list[Path] = []
        if source_path.is_file():
            candidates.append(source_path)
        else:
            candidates.append(source_path / "manifest.json")
            if blueprint_id:
                candidates.append(source_path / blueprint_id / "manifest.json")
            index = _read_monitor_json_value(source_path / "index.json")
            raw_blueprints = (
                index.get("blueprints") if isinstance(index, dict) else None
            )
            if not isinstance(raw_blueprints, list) and isinstance(index, list):
                raw_blueprints = index
            for entry in raw_blueprints if isinstance(raw_blueprints, list) else []:
                if not isinstance(entry, dict):
                    continue
                if blueprint_id and str(entry.get("id") or "") != blueprint_id:
                    continue
                relative = str(entry.get("path") or "").strip()
                if relative:
                    candidates.append(source_path / relative / "manifest.json")

        for candidate_path in candidates:
            manifest = _read_monitor_manifest(candidate_path)
            if manifest and _workflow_step_ids(manifest):
                return manifest
    return None


def _local_run_store_manifest(
    data: dict[str, Any], job: dict[str, Any], summary: dict[str, Any]
) -> dict[str, Any] | None:
    """Load the source workflow contract used by ``blueprint run``."""

    entries = _local_run_store_entries(data, job, summary)
    for run_dir, _mapping in entries:
        for filename in ("manifest.json", "config.json"):
            candidate = _read_monitor_manifest(run_dir / filename)
            if candidate and _workflow_step_ids(candidate):
                return candidate
    for _run_dir, mapping in entries:
        candidate = _blueprint_manifest_from_mapping(mapping)
        if candidate:
            return candidate
    return None


def _local_run_store_events(job_id: str) -> list[dict[str, Any]]:
    data = {"job_id": job_id}
    entries = _local_run_store_entries(data, data, {})
    if not entries:
        return []
    path = entries[0][0] / "events.jsonl"
    try:
        stat = path.stat()
    except OSError:
        return []
    cache_key = str(path)
    cached = _MONITOR_EVENTS_CACHE.get(cache_key)
    if cached and cached[:2] == (stat.st_mtime_ns, stat.st_size):
        return list(cached[2])

    events: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
    except (OSError, UnicodeDecodeError):
        return []
    _MONITOR_EVENTS_CACHE[cache_key] = (stat.st_mtime_ns, stat.st_size, events)
    return list(events)


def _workflow_progress_for_monitor(
    job_id: str, data: dict[str, Any]
) -> dict[str, Any] | None:
    job, summary = _job_and_summary_from_data(data)
    manifest = _manifest_from_job_data(data)
    events: list[dict[str, Any]] = []
    try:
        for event_json in client.stream_events(job_id, follow=False):
            try:
                event = json.loads(event_json)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
    except Exception:
        logger.exception("Failed to load workflow events for monitor")
    try:
        return build_workflow_progress_snapshot(
            manifest,
            events,
            job=job,
            summary=summary,
            job_id=job_id,
        )
    except Exception:
        logger.exception("Failed to build workflow progress for monitor")
        return None


def _manifest_from_job_data(data: dict[str, Any]) -> dict[str, Any]:
    job, summary = _job_and_summary_from_data(data)
    workflow_manifest = _public_workflow_manifest_from_job(job, summary)

    run_manifest = _local_run_store_manifest(data, job, summary)
    if run_manifest and _matches_public_workflow_contract(
        run_manifest, workflow_manifest
    ):
        return run_manifest

    for candidate in (
        data.get("manifest"),
        job.get("manifest"),
        summary.get("manifest"),
    ):
        if _matches_public_workflow_contract(candidate, workflow_manifest):
            return candidate
    manifest_ref = (
        job.get("manifest_ref")
        if isinstance(job.get("manifest_ref"), dict)
        else summary.get("manifest_ref")
    )
    if isinstance(manifest_ref, dict):
        for raw_path in (
            manifest_ref.get("manifest_path"),
            Path(str(manifest_ref.get("job_path") or "")) / "manifest.json"
            if manifest_ref.get("job_path")
            else None,
        ):
            if not raw_path:
                continue
            try:
                path = Path(str(raw_path)).expanduser()
                if path.is_file():
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                    if _matches_public_workflow_contract(loaded, workflow_manifest):
                        return loaded
            except (OSError, json.JSONDecodeError):
                continue

    if workflow_manifest:
        return workflow_manifest

    if run_manifest:
        return run_manifest

    return _legacy_manifest_from_job_data(data, job=job, summary=summary)


def _matches_public_workflow_contract(
    candidate: Any, public_manifest: dict[str, Any] | None
) -> bool:
    if not isinstance(candidate, dict) or not candidate:
        return False
    if public_manifest is None:
        return True
    candidate_steps = _workflow_step_ids(candidate)
    return bool(candidate_steps) and candidate_steps == _workflow_step_ids(
        public_manifest
    )


def _workflow_step_ids(manifest: dict[str, Any]) -> list[str]:
    workflow = (
        manifest.get("workflow") if isinstance(manifest.get("workflow"), dict) else {}
    )
    steps = workflow.get("steps") if isinstance(workflow.get("steps"), list) else []
    return [
        str(step.get("id"))
        for step in steps
        if isinstance(step, dict) and step.get("id")
    ]


def _public_workflow_manifest_from_job(
    job: dict[str, Any], summary: dict[str, Any]
) -> dict[str, Any] | None:
    """Rebuild the source-facing workflow contract from the runtime ledger.

    The runtime lowers every public step into start/end/router nodes.  Those
    nodes are execution details, not the agents that a person should see while
    monitoring a blueprint.
    """

    workflow_state = _workflow_state_from_job(job, summary)
    steps_by_id = (
        (
            workflow_state.get("steps")
            if isinstance(workflow_state.get("steps"), dict)
            else {}
        )
        if isinstance(workflow_state, dict)
        else {}
    )
    step_order = (
        workflow_state.get("step_order")
        if isinstance(workflow_state, dict)
        and isinstance(workflow_state.get("step_order"), list)
        else []
    )
    step_ids = [str(step_id) for step_id in step_order if str(step_id) in steps_by_id]
    if not step_ids:
        step_ids = [str(step_id) for step_id in steps_by_id if str(step_id).strip()]

    if not step_ids:
        step_ids = _public_step_ids_from_topology(job)
    if not step_ids:
        return None

    edges = _public_workflow_edges(workflow_state, job, step_ids)
    outgoing: dict[str, list[tuple[str, str]]] = {}
    for edge in edges:
        source = str(edge.get("from") or "")
        target = str(edge.get("to") or "")
        event_name = str(edge.get("event") or edge.get("message_type") or "")
        if source and target and event_name:
            outgoing.setdefault(source, []).append((event_name, target))

    steps: list[dict[str, Any]] = []
    for index, step_id in enumerate(step_ids):
        record = steps_by_id.get(step_id)
        record = record if isinstance(record, dict) else {}
        transitions = {
            event_name: target for event_name, target in outgoing.get(step_id, [])
        }
        steps.append(
            {
                "id": step_id,
                "label": str(record.get("label") or _humanize_identifier(step_id)),
                "goal": str(record.get("goal") or ""),
                # Source manifests run a public step through this generated
                # start node.  The shared run renderer intentionally presents
                # the public step agent, rather than the internal node.
                "run": str(record.get("run") or f"{step_id}__start"),
                "emits": _step_emit_name(step_id, outgoing.get(step_id, [])),
                "on": transitions,
                "needs": [
                    str(edge.get("from"))
                    for edge in edges
                    if str(edge.get("to") or "") == step_id
                ],
                "kind": "source"
                if index == 0
                else "sink"
                if index == len(step_ids) - 1
                else "stage",
            }
        )

    workflow_id = str(
        workflow_state.get("workflow_id")
        if isinstance(workflow_state, dict) and workflow_state.get("workflow_id")
        else job.get("workflow_id")
        or summary.get("workflow_id")
        or job.get("graph_id")
        or summary.get("graph_id")
        or job.get("job_id")
        or "workflow"
    )
    job_type = str(
        job.get("job_type")
        or job.get("type")
        or summary.get("job_type")
        or summary.get("type")
        or ""
    )
    policies = {"stream_mode": "live"} if job_type.lower() == "service" else {}
    return {
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": str(job.get("graph_id") or summary.get("graph_id") or workflow_id),
        "name": str(job.get("job_name") or summary.get("job_name") or workflow_id),
        "description": str(summary.get("description") or job.get("description") or ""),
        "policies": policies,
        "workflow": {
            "workflow_id": workflow_id,
            "entrypoint": step_ids[0],
            "source": step_ids[0],
            "sink": step_ids[-1],
            "steps": steps,
            "edges": edges,
        },
        # There are deliberately no bindings here: the source contract's
        # public agent is the step, while runtime-only workers stay hidden.
        "runtime": {"bindings": {}},
    }


def _workflow_state_from_job(
    job: dict[str, Any], summary: dict[str, Any]
) -> dict[str, Any] | None:
    for mapping in (job, summary):
        state = mapping.get("workflow_state") if isinstance(mapping, dict) else None
        if isinstance(state, dict):
            return state
    return None


def _public_step_ids_from_topology(job: dict[str, Any]) -> list[str]:
    topology = (
        job.get("runtime_topology")
        if isinstance(job.get("runtime_topology"), dict)
        else {}
    )
    nodes = topology.get("nodes") if isinstance(topology.get("nodes"), list) else []
    step_ids: list[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("node_id") or node.get("id") or "")
        node_types = {
            str(node.get(key) or "").strip().lower()
            for key in ("agent_type", "node_type", "type")
        }
        if "step_source" not in node_types or not node_id.endswith("__start"):
            continue
        step_id = node_id.removesuffix("__start")
        if step_id and step_id not in step_ids:
            step_ids.append(step_id)
    return step_ids


def _public_workflow_edges(
    workflow_state: dict[str, Any] | None,
    job: dict[str, Any],
    step_ids: list[str],
) -> list[dict[str, Any]]:
    raw_edges = (
        workflow_state.get("edges") if isinstance(workflow_state, dict) else None
    )
    if not isinstance(raw_edges, list):
        topology = (
            job.get("runtime_topology")
            if isinstance(job.get("runtime_topology"), dict)
            else {}
        )
        raw_edges = (
            topology.get("edges") if isinstance(topology.get("edges"), list) else []
        )

    known_steps = set(step_ids)
    edges: list[dict[str, Any]] = []
    for raw_edge in raw_edges:
        if not isinstance(raw_edge, dict):
            continue
        source = str(raw_edge.get("from") or raw_edge.get("from_node") or "")
        target = str(raw_edge.get("to") or raw_edge.get("to_node") or "")
        if source.endswith("__end"):
            source = source.removesuffix("__end")
        if target.endswith("__start"):
            target = target.removesuffix("__start")
        if source not in known_steps or target not in known_steps:
            continue
        edges.append(
            {
                "id": str(
                    raw_edge.get("id")
                    or raw_edge.get("edge_id")
                    or f"{source}_to_{target}"
                ),
                "from": source,
                "to": target,
                "event": str(
                    raw_edge.get("event")
                    or raw_edge.get("message_type")
                    or f"{source}_completed"
                ),
            }
        )
    return edges


def _step_emit_name(step_id: str, transitions: list[tuple[str, str]]) -> str:
    return transitions[0][0] if transitions else f"{step_id}_completed"


def _humanize_identifier(value: str) -> str:
    return " ".join(
        part.capitalize() for part in value.replace("-", "_").split("_") if part
    )


def _legacy_manifest_from_job_data(
    data: dict[str, Any], *, job: dict[str, Any], summary: dict[str, Any]
) -> dict[str, Any]:
    topology = (
        job.get("runtime_topology")
        if isinstance(job.get("runtime_topology"), dict)
        else {}
    )
    topology_nodes = (
        topology.get("nodes") if isinstance(topology.get("nodes"), list) else []
    )
    agents = topology_nodes or (
        data.get("agents") if isinstance(data.get("agents"), list) else []
    )
    nodes = []
    for index, agent in enumerate(agents):
        if not isinstance(agent, dict):
            continue
        agent_id = str(
            agent.get("agent_id")
            or agent.get("id")
            or agent.get("node_id")
            or f"agent_{index + 1}"
        )
        nodes.append(
            {
                "node_id": agent_id,
                "agent_type": str(
                    agent.get("agent_type") or agent.get("type") or "worker"
                ),
                "role": str(
                    agent.get("role")
                    or agent.get("current_task")
                    or agent.get("agent_type")
                    or "worker"
                ),
                "type": str(agent.get("node_type") or agent.get("type") or ""),
                "live": agent.get("live?", agent.get("live", False)),
                "config": {
                    "llm_config": str(
                        agent.get("model") or agent.get("llm_config") or "runtime"
                    )
                },
            }
        )
    job_type = str(
        job.get("job_type")
        or job.get("type")
        or summary.get("job_type")
        or summary.get("type")
        or ""
    )
    policies = {"stream_mode": "live"} if job_type.lower() == "service" else {}
    return {
        "id": str(
            job.get("graph_id") or summary.get("graph_id") or job.get("job_id") or "job"
        ),
        "name": str(
            job.get("job_name") or summary.get("job_name") or job.get("job_id") or "Job"
        ),
        "description": str(summary.get("description") or job.get("description") or ""),
        "graph_id": str(job.get("graph_id") or summary.get("graph_id") or ""),
        "type": job_type,
        "job_type": job_type,
        "policies": policies,
        "nodes": nodes,
    }


def _public_progress_from_api_snapshot(
    job_id: str, snapshot: dict[str, Any]
) -> dict[str, Any]:
    """Project API stream updates onto the source-facing workflow contract.

    Prefer the source manifest and event relay persisted by ``blueprint run``.
    This avoids downloading an ever-growing runtime ledger for every stream
    update and keeps attached monitoring identical to the launch-time view.
    """

    local_progress = _local_progress_from_run_store(job_id, snapshot)
    if local_progress:
        return local_progress

    local_data = {"job_id": job_id}
    local_manifest = _local_run_store_manifest(local_data, local_data, {})
    if local_manifest:
        public_ids = _workflow_step_ids(local_manifest)
        snapshot_ids = [
            str(step.get("id"))
            for step in snapshot.get("steps", [])
            if isinstance(step, dict) and step.get("id")
        ]
        if snapshot_ids == public_ids:
            return snapshot

    try:
        data = json.loads(_get_job_for_monitor(job_id))
    except Exception:
        return snapshot
    if not isinstance(data, dict):
        return snapshot
    job, summary = _job_and_summary_from_data(data)
    if not _public_workflow_manifest_from_job(job, summary):
        return snapshot
    try:
        progress = build_workflow_progress_snapshot(
            _manifest_from_job_data(data),
            [],
            job=job,
            summary=summary,
            job_id=job_id,
        )
    except Exception:
        logger.exception("Failed to project API workflow progress onto source contract")
        return snapshot

    return _merge_api_progress_metadata(progress, snapshot)


def _local_progress_from_run_store(
    job_id: str, snapshot: dict[str, Any]
) -> dict[str, Any] | None:
    local_data = {"job_id": job_id}
    manifest = _local_run_store_manifest(local_data, local_data, {})
    events = _local_run_store_events(job_id) if manifest else []
    if not manifest or not events:
        return None
    try:
        progress = build_workflow_progress_snapshot(
            manifest,
            events,
            job={
                "job_id": job_id,
                "status": snapshot.get("status"),
                "submitted_at": snapshot.get("submitted_at"),
            },
            summary={"status": snapshot.get("status")},
            job_id=job_id,
        )
        return _merge_api_progress_metadata(progress, snapshot)
    except Exception:
        logger.exception("Failed to build progress from the local run store")
        return None


def _merge_api_progress_metadata(
    progress: dict[str, Any], snapshot: dict[str, Any]
) -> dict[str, Any]:
    for key in ("messages", "resource_tokens", "observability_summary", "trace_id"):
        if key not in progress and key in snapshot:
            progress[key] = snapshot[key]
    stream_status = str(snapshot.get("status") or "").lower()
    if (
        stream_status in FINAL_STATUSES
        and str(progress.get("status") or "").lower() not in FINAL_STATUSES
    ):
        progress["status"] = stream_status
    return progress


def _monitor_api_stream_timeout_seconds() -> float:
    raw = os.getenv("MN_JOB_MONITOR_API_STREAM_TIMEOUT", "").strip()
    if not raw:
        return _monitor_rpc_timeout_seconds()
    try:
        return max(float(raw), 1.0)
    except ValueError:
        return _monitor_rpc_timeout_seconds()


def _live_monitor_api_stream(job_id: str) -> bool:
    api_base_url = str(getattr(config, "api_base_url", "") or "").strip()
    if not api_base_url or os.getenv(
        "MN_JOB_MONITOR_DISABLE_API_STREAM", ""
    ).strip().lower() in {"1", "true", "yes", "on"}:
        return False

    import queue
    import threading
    import select
    import sys

    event_queue: queue.Queue[tuple[str, Any]] = queue.Queue()

    def reader() -> None:
        try:
            for snapshot in stream_api_workflow_progress(
                api_base_url,
                job_id,
                api_token=str(getattr(config, "api_token", "") or ""),
                timeout=_monitor_api_stream_timeout_seconds(),
            ):
                event_queue.put(("snapshot", snapshot))
                if str(snapshot.get("status") or "").lower() in FINAL_STATUSES:
                    break
        except Exception as exc:
            event_queue.put(("error", exc))
        finally:
            event_queue.put(("done", None))

    class MonitorView:
        def __init__(
            self,
            state: JobMonitorState,
            initial_progress: dict[str, Any] | None = None,
        ):
            self.data: dict[str, Any] | None = (
                {
                    "workflow_progress": initial_progress,
                    "summary": {"status": initial_progress.get("status")},
                    "job": {"job_id": job_id, "status": initial_progress.get("status")},
                }
                if initial_progress
                else None
            )
            self.state = state

        def __rich__(self):
            if not self.data:
                from rich.panel import Panel

                return Panel("Connecting to workflow progress stream...", style="cyan")
            return generate_live_layout(job_id, self.data, state=self.state)

    initial_progress = _local_progress_from_run_store(
        job_id, {"job_id": job_id, "status": "running"}
    )
    monitor_state = JobMonitorState()
    view = MonitorView(monitor_state, initial_progress)
    worker = threading.Thread(target=reader, daemon=True)
    worker.start()
    is_tty = _interactive_live_output()
    old_settings = None
    if is_tty:
        import tty
        import termios

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setcbreak(fd)

    saw_snapshot = initial_progress is not None
    try:
        with Live(
            view,
            refresh_per_second=12,
            console=console,
            screen=bool(is_tty and getattr(console, "is_terminal", False)),
            transient=bool(is_tty and getattr(console, "is_terminal", False)),
        ):
            while True:
                if not _handle_live_workflow_key(
                    monitor_state,
                    view.data,
                    is_tty=is_tty,
                    select_module=select,
                    block_seconds=0.05 if is_tty else 0.0,
                ):
                    # A handled detach must not fall through to the polling
                    # monitor in _live_monitor.
                    return True
                try:
                    kind, payload = event_queue.get(timeout=0.05 if is_tty else 0.5)
                except queue.Empty:
                    continue
                if kind == "error":
                    if not saw_snapshot:
                        return False
                    logger.warning("Workflow progress stream ended: %s", payload)
                    break
                if kind == "done":
                    break
                if kind == "snapshot" and isinstance(payload, dict):
                    saw_snapshot = True
                    progress = _public_progress_from_api_snapshot(job_id, payload)
                    view.data = {
                        "workflow_progress": progress,
                        "summary": {"status": progress.get("status")},
                        "job": {"job_id": job_id, "status": progress.get("status")},
                    }
                    if str(progress.get("status") or "").lower() in FINAL_STATUSES:
                        break
    except KeyboardInterrupt:
        return True
    finally:
        if is_tty and old_settings:
            import termios

            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_settings)
    return saw_snapshot


def _live_monitor(job_id: str):
    if _live_monitor_api_stream(job_id):
        return

    import sys
    import select
    import time
    from rich.live import Live

    is_tty = _interactive_live_output()
    old_settings = None
    if is_tty:
        import tty
        import termios

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setcbreak(fd)

    class MonitorView:
        def __init__(self, state: JobMonitorState):
            self.data = None
            self.state = state

        def __rich__(self):
            if not self.data:
                from rich.panel import Panel

                return Panel("Connecting...", style="cyan")
            if self.data.get("retrying"):
                from rich.panel import Panel

                return Panel(
                    f"! Warning: {self.data.get('monitor_warning', 'Temporary status fetch failure; retrying...')}",
                    style="yellow",
                )
            if "error" in self.data:
                from rich.panel import Panel

                return Panel(f"Error fetching job: {self.data['error']}", style="red")
            return generate_live_layout(job_id, self.data, state=self.state)

    final_status = "unknown"
    data = None
    last_good_data: dict[str, Any] | None = None
    monitor_state = JobMonitorState()
    view = MonitorView(monitor_state)

    try:
        with Live(
            view,
            refresh_per_second=12,
            console=console,
            screen=bool(is_tty and getattr(console, "is_terminal", False)),
            transient=bool(is_tty and getattr(console, "is_terminal", False)),
        ):
            while True:
                try:
                    with _temporary_monitor_rpc_timeout():
                        job_json = _get_job_for_monitor(job_id)
                        data = json.loads(job_json)
                        data["workflow_progress"] = _workflow_progress_for_monitor(
                            job_id, data
                        )
                    last_good_data = data
                except Exception as exc:
                    if _is_transient_monitor_fetch_error(exc):
                        warning = _monitor_fetch_warning(exc)
                        if last_good_data is None:
                            data = {"retrying": True, "monitor_warning": warning}
                        else:
                            data = dict(last_good_data)
                            data["monitor_warning"] = warning
                            progress = data.get("workflow_progress")
                            if isinstance(progress, dict):
                                progress = dict(progress)
                                progress["monitor_warning"] = warning
                                data["workflow_progress"] = progress
                        logger.warning("%s", warning)
                    else:
                        data = {"error": str(exc)}

                view.data = data

                if data and "error" not in data:
                    job, summary = _job_and_summary_from_data(data)
                    status = summary.get("status") or job.get("status") or "unknown"
                    if status in ["completed", "failed", "cancelled"]:
                        final_status = status
                        break

                if not _handle_live_workflow_key(
                    monitor_state,
                    data,
                    is_tty=is_tty,
                    select_module=select,
                    block_seconds=0.5,
                ):
                    break
                if not is_tty:
                    time.sleep(0.5)
                    break

    except KeyboardInterrupt:
        pass
    finally:
        if is_tty and old_settings:
            import termios

            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    if final_status in ["completed", "failed", "cancelled"]:
        # Save results and print final summary
        fetch_and_save_results(job_id, data)
        log_dir = Path(f"/tmp/mn_{job_id}")
        panel = generate_summary_panel(job_id, final_status, log_dir)
        console.print(panel)
    else:
        console.print(f"\n[yellow]Exited live monitor for {job_id}[/yellow]")
        fetch_and_save_results(job_id, data)


def monitor(job_id: str):
    """Stream live events for a job"""
    try:
        _live_monitor(job_id)
    except Exception as e:
        handle_cli_error(e, console, "monitor stream")


__all__ = [name for name in globals() if not name.startswith("__")]
