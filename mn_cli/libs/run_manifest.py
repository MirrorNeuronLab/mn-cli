from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from mn_sdk import DOCKER_MODEL_RUNNER_CONTAINER_API_BASE, resolve_llm_environment

DEFAULT_RUNS_ROOT = "~/.mn/runs"
USER_HOME_ENV_KEYS = ("MN_OUTPUT_HOME", "MN_USER_HOME", "OTTERDESK_USER_HOME")


def _inject_local_blueprint_support_path() -> None:
    import sys

    repo_root = workspace_root()
    candidate = repo_root / "mn-skills" / "blueprint_support_skill" / "src"
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


def workspace_root() -> Path:
    for name in (
        "MN_WORKSPACE_ROOT",
    ):
        value = os.getenv(name)
        if value:
            return Path(value).expanduser().resolve()
    return Path(__file__).resolve().parents[3]


def runtime_path_environment() -> dict[str, str]:
    root = workspace_root()
    membrane_project_path = Path(
        os.getenv("MN_MEMBRANE_PROJECT_PATH") or root / "Membrane"
    ).expanduser()
    membrane_sdk_path = Path(
        os.getenv("MN_MEMBRANE_SDK_PATH")
        or membrane_project_path / "mn-context-engine-python-sdk" / "src"
    ).expanduser()
    skills_root = Path(os.getenv("MN_SKILLS_ROOT") or root / "mn-skills").expanduser()
    env = {
        "MN_WORKSPACE_ROOT": str(root),
        "MN_MEMBRANE_PROJECT_PATH": str(membrane_project_path),
        "MN_MEMBRANE_SDK_PATH": str(membrane_sdk_path),
        "MN_SKILLS_ROOT": str(skills_root),
    }
    python_paths = [
        skills_root / "blueprint_support_skill" / "src",
        skills_root / "llm_ocr_skill" / "src",
        skills_root / "pdf_extract_skill" / "src",
    ]
    existing_pythonpath = os.getenv("PYTHONPATH")
    resolved_python_paths = [str(path) for path in python_paths if path.exists()]
    if existing_pythonpath:
        resolved_python_paths.append(existing_pythonpath)
    if resolved_python_paths:
        env["PYTHONPATH"] = os.pathsep.join(resolved_python_paths)
    env.update(user_home_environment())
    return env


def user_home_environment() -> dict[str, str]:
    home = str(Path.home())
    env: dict[str, str] = {}
    for key in USER_HOME_ENV_KEYS:
        value = os.getenv(key)
        if value:
            env[key] = str(Path(value).expanduser())
    if home:
        env.setdefault("MN_USER_HOME", home)
        env.setdefault("MN_OUTPUT_HOME", home)
        env.setdefault("OTTERDESK_USER_HOME", home)
    return env


def prepare_manifest_for_submission(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    enable_runtime_web_ui: bool = True,
) -> dict[str, Any]:
    prepared = json.loads(json.dumps(manifest_dict))
    render_agent_templates_for_submission(prepared)
    metadata = dict(submission_metadata or {})
    run_id = (
        metadata.get("blueprint_run_id")
        or metadata.get("run_id")
        or (env_overrides or {}).get("MN_RUN_ID")
    )
    runs_root = _shared_runs_root(env_overrides)
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    if run_id:
        config = with_shared_run_store_config(config, str(run_id), runs_root)
    if config is not None:
        apply_manifest_config_bindings(prepared, config)
        refresh_embedded_blueprint_config(prepared, config)
    if enable_runtime_web_ui and run_id and config is not None and blueprint_web_ui_enabled(config):
        inject_runtime_web_ui_service_for_submission(
            prepared,
            bundle_dir=bundle_dir,
            config=config,
            run_id=str(run_id),
            runs_root=runs_root,
            env_overrides=env_overrides,
        )
    runtime_env = blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    if run_id:
        runtime_env.setdefault("MN_RUN_ID", str(run_id))
        runtime_env["MN_RUNS_ROOT"] = runs_root
    runtime_env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    if runtime_env:
        inject_node_environment(prepared, runtime_env)
        strip_docker_model_runner_placement_requirements(prepared)
    normalize_host_local_uploads(prepared)
    lower_manifest_topology_for_runtime_submission(prepared)
    if metadata:
        prepared.setdefault("metadata", {}).setdefault("mn_cli", {}).update(metadata)
    return prepared


def blueprint_web_ui_enabled(config: dict[str, Any] | None) -> bool:
    web_ui = config.get("web_ui") if isinstance(config, dict) else None
    output = web_ui.get("output") if isinstance(web_ui, dict) else None
    return (
        isinstance(web_ui, dict)
        and web_ui.get("enabled") is True
        and isinstance(output, dict)
        and output.get("adapter") == "gradio"
    )


def manifest_nodes(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    agents = manifest.get("agents") if isinstance(manifest.get("agents"), dict) else {}
    agent_nodes = agents.get("nodes") if isinstance(agents, dict) else None
    if isinstance(agent_nodes, list):
        return [node for node in agent_nodes if isinstance(node, dict)]
    nodes = manifest.get("nodes")
    if isinstance(nodes, list):
        return [node for node in nodes if isinstance(node, dict)]
    return []


def lower_manifest_topology_for_runtime_submission(manifest: dict[str, Any]) -> None:
    """Lower catalog workflow manifests into the core runtime topology shape."""

    workflow_id = _submission_workflow_id(manifest)
    agents = manifest.get("agents") if isinstance(manifest.get("agents"), dict) else {}
    has_agent_graph = isinstance(agents.get("nodes"), list) or isinstance(agents.get("edges"), list)
    if not (_is_workflow_submission_manifest(manifest) or (workflow_id and has_agent_graph)):
        return

    if workflow_id and not manifest.get("graph_id"):
        manifest["graph_id"] = workflow_id
    if not manifest.get("job_name"):
        manifest["job_name"] = str(manifest.get("id") or workflow_id or "workflow")

    flow = manifest.get("flow")
    if not isinstance(flow, dict):
        flow = {}
        manifest["flow"] = flow

    flow_nodes_source = flow.get("nodes")
    agent_nodes_source = agents.get("nodes") if isinstance(agents, dict) else None
    root_nodes_source = manifest.get("nodes")
    nodes = _dedupe_topology_items(
        _list_dicts(flow_nodes_source),
        _list_dicts(agent_nodes_source),
        _list_dicts(root_nodes_source),
        key="node_id",
    )
    if nodes:
        flow["nodes"] = nodes

    flow_edges_source = flow.get("edges")
    agent_edges_source = agents.get("edges") if isinstance(agents, dict) else None
    root_edges_source = manifest.get("edges")
    edges = _dedupe_topology_items(
        _list_dicts(flow_edges_source),
        _list_dicts(agent_edges_source),
        _list_dicts(root_edges_source),
        key="edge_id",
    )
    if edges or any(
        isinstance(source, list)
        for source in (flow_edges_source, agent_edges_source, root_edges_source)
    ):
        flow["edges"] = edges

    entrypoints = _dedupe_strings(
        _list_strings(agents.get("entrypoints")) if isinstance(agents, dict) else [],
        _list_strings(manifest.get("entrypoints")),
    )
    if entrypoints:
        manifest["entrypoints"] = entrypoints

    initial_inputs = _runtime_binding_seed_inputs(manifest)
    if initial_inputs:
        current_inputs = manifest.get("initial_inputs")
        if not isinstance(current_inputs, dict):
            current_inputs = {}
            manifest["initial_inputs"] = current_inputs
        for node_id, inputs in initial_inputs.items():
            current_inputs.setdefault(node_id, inputs)

    manifest.pop("nodes", None)
    manifest.pop("edges", None)


def _is_workflow_submission_manifest(manifest: dict[str, Any]) -> bool:
    return (
        manifest.get("apiVersion") == "mn.workflow/v1"
        or manifest.get("kind") == "Workflow"
        or isinstance(manifest.get("workflow"), dict)
    )


def _submission_workflow_id(manifest: dict[str, Any]) -> str | None:
    workflow = manifest.get("workflow") if isinstance(manifest.get("workflow"), dict) else {}
    workflow_id = workflow.get("workflow_id") if isinstance(workflow, dict) else None
    if isinstance(workflow_id, str) and workflow_id.strip():
        return workflow_id.strip()
    legacy_id = manifest.get("workflow_id")
    if isinstance(legacy_id, str) and legacy_id.strip():
        return legacy_id.strip()
    return None


def _list_dicts(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _list_strings(value: Any) -> list[str]:
    return [item for item in value if isinstance(item, str) and item.strip()] if isinstance(value, list) else []


def _dedupe_topology_items(*groups: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    items_by_id: dict[str, dict[str, Any]] = {}
    anonymous: list[dict[str, Any]] = []
    for group in groups:
        for item in group:
            item_id = item.get(key)
            if isinstance(item_id, str) and item_id:
                items_by_id[item_id] = item
            else:
                anonymous.append(item)
    return [*items_by_id.values(), *anonymous]


def _dedupe_strings(*groups: list[str]) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for group in groups:
        for value in group:
            if value in seen:
                continue
            seen.add(value)
            values.append(value)
    return values


def _runtime_binding_seed_inputs(manifest: dict[str, Any]) -> dict[str, Any]:
    runtime = manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
    bindings = runtime.get("bindings") if isinstance(runtime.get("bindings"), dict) else {}
    initial_inputs: dict[str, Any] = {}
    for binding in bindings.values():
        if not isinstance(binding, dict):
            continue
        seed_inputs = binding.get("seed_inputs", binding.get("initial_inputs"))
        if isinstance(seed_inputs, dict):
            initial_inputs.update(seed_inputs)
    return initial_inputs


def manifest_local_inputs_enabled(manifest: dict[str, Any]) -> bool:
    for node in manifest_nodes(manifest):
        environment = (node.get("config") or {}).get("environment") or {}
        config_json = environment.get("MN_BLUEPRINT_CONFIG_JSON")
        if not config_json:
            continue
        try:
            config = json.loads(config_json)
        except (TypeError, ValueError):
            continue
        local_inputs = config.get("local_inputs") if isinstance(config, dict) else None
        if isinstance(local_inputs, dict) and (
            local_inputs.get("folders") or local_inputs.get("files")
        ):
            return True
    return False


def inject_runtime_web_ui_service_for_submission(
    manifest: dict[str, Any],
    *,
    bundle_dir: Path,
    config: dict[str, Any],
    run_id: str,
    runs_root: str,
    env_overrides: Optional[dict[str, str]] = None,
) -> dict[str, Any] | None:
    _inject_local_blueprint_support_path()
    try:
        from mn_blueprint_support import inject_runtime_web_ui_service
    except ImportError as exc:
        raise RuntimeError(
            "Blueprint web UI service injection requires mn_blueprint_support."
        ) from exc
    return inject_runtime_web_ui_service(
        manifest,
        bundle_dir=bundle_dir,
        config=config,
        run_id=run_id,
        runs_root=runs_root,
        env_overrides=env_overrides,
    )


def runtime_web_ui_support_payloads_for_manifest(manifest: dict[str, Any]) -> dict[str, bytes]:
    _inject_local_blueprint_support_path()
    try:
        from mn_blueprint_support import runtime_web_ui_service_from_manifest, runtime_web_ui_support_payloads
    except ImportError:
        return {}
    if not runtime_web_ui_service_from_manifest(manifest):
        return {}
    return runtime_web_ui_support_payloads()


def stage_local_input_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    _inject_local_blueprint_support_path()
    try:
        from mn_blueprint_support import stage_local_input_payloads_for_manifest as stage_payloads
    except ImportError as exc:
        if not manifest_local_inputs_enabled(manifest):
            return {}
        raise RuntimeError("Local blueprint input staging requires mn_blueprint_support.") from exc
    return stage_payloads(manifest, payloads, bundle_dir=bundle_dir)


def stage_blueprint_support_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    skills_root = workspace_root() / "mn-skills"
    support_root = skills_root / "blueprint_support_skill" / "src" / "mn_blueprint_support"
    if not support_root.is_dir():
        return {"staged": False, "sources": []}
    litellm_skill_src = skills_root / "litellm_communicate_skill" / "src"

    payload_root = bundle_dir / "payloads"
    staged_sources: list[str] = []
    config_payloads = _blueprint_config_payloads(bundle_dir)
    for source in _support_dependent_upload_sources(manifest, payload_root):
        for target_prefix in _support_staging_prefixes(payload_root, source):
            _stage_tree_payloads(
                payloads,
                source_root=support_root,
                target_prefix=_payload_join(target_prefix, "mn_blueprint_support"),
            )
            if litellm_skill_src.is_dir():
                _stage_tree_payloads(
                    payloads,
                    source_root=litellm_skill_src,
                    target_prefix=_payload_join(target_prefix, "litellm_communicate_skill", "src"),
                )
        for relative, contents in config_payloads.items():
            payloads.setdefault(_payload_join(source, "config", relative), contents)
        staged_sources.append(source)

    return {"staged": bool(staged_sources), "sources": staged_sources}


def render_agent_templates_for_submission(manifest: dict[str, Any]) -> None:
    nodes = manifest_nodes(manifest)
    if not nodes or not any("uses" in node for node in nodes):
        return
    _inject_local_blueprint_support_path()
    try:
        from mn_blueprint_support import render_manifest_agent_templates
    except ImportError as exc:
        raise RuntimeError(
            "Manifest uses agent templates, but mn_blueprint_support is not installed."
        ) from exc
    rendered = render_manifest_agent_templates(manifest)
    manifest.clear()
    manifest.update(rendered)


def _support_dependent_upload_sources(manifest: dict[str, Any], payload_root: Path) -> list[str]:
    sources: list[str] = []
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") != "MirrorNeuron.Runner.HostLocal":
            continue
        for source in _node_upload_sources(config):
            if source in sources:
                continue
            if _payload_source_imports_blueprint_support(payload_root, source):
                sources.append(source)
    return sources


def _node_upload_sources(config: dict[str, Any]) -> list[str]:
    upload_paths = config.get("upload_paths")
    if isinstance(upload_paths, list) and upload_paths:
        return [
            str(entry.get("source")).strip()
            for entry in upload_paths
            if isinstance(entry, dict) and str(entry.get("source") or "").strip()
        ]
    source = str(config.get("upload_path") or "").strip()
    return [source] if source else []


def _payload_source_imports_blueprint_support(payload_root: Path, source: str) -> bool:
    source_path = (payload_root / source).resolve()
    try:
        source_path.relative_to(payload_root.resolve())
    except ValueError:
        return False
    if not source_path.exists():
        return False

    files = [source_path] if source_path.is_file() else source_path.rglob("*.py")
    for file_path in files:
        try:
            if "mn_blueprint_support" in file_path.read_text(encoding="utf-8", errors="ignore"):
                return True
        except OSError:
            continue
    return False


def _support_staging_prefixes(payload_root: Path, source: str) -> list[str]:
    payload_root_resolved = payload_root.resolve()
    source_path = (payload_root_resolved / source).resolve()
    try:
        source_path.relative_to(payload_root_resolved)
    except ValueError:
        return []
    if not source_path.exists():
        return []

    prefixes: list[str] = []
    if source_path.is_dir():
        prefixes.append(source.strip("/"))

    files = [source_path] if source_path.is_file() else source_path.rglob("*.py")
    for file_path in files:
        try:
            if "mn_blueprint_support" not in file_path.read_text(encoding="utf-8", errors="ignore"):
                continue
            rel_parent = file_path.parent.relative_to(payload_root_resolved).as_posix()
        except (OSError, ValueError):
            continue
        if rel_parent == ".":
            rel_parent = ""
        if rel_parent not in prefixes:
            prefixes.append(rel_parent)
    return prefixes


def _blueprint_config_payloads(bundle_dir: Path) -> dict[str, bytes]:
    config_dir = bundle_dir / "config"
    if not config_dir.is_dir():
        return {}

    payloads: dict[str, bytes] = {}
    for file_path in config_dir.rglob("*"):
        if not file_path.is_file():
            continue
        payloads[file_path.relative_to(config_dir).as_posix()] = file_path.read_bytes()
    return payloads


def _stage_tree_payloads(
    payloads: dict[str, bytes],
    *,
    source_root: Path,
    target_prefix: str,
) -> None:
    for file_path in source_root.rglob("*"):
        if not file_path.is_file() or "__pycache__" in file_path.parts:
            continue
        relative = file_path.relative_to(source_root).as_posix()
        payloads.setdefault(
            _payload_join(target_prefix, relative),
            file_path.read_bytes(),
        )


def _payload_join(*parts: str) -> str:
    cleaned = []
    for part in parts:
        value = part.strip("/")
        if not value or value == ".":
            continue
        cleaned.append(value)
    return "/".join(cleaned)


def _shared_runs_root(env_overrides: Optional[dict[str, str]] = None) -> str:
    return str(
        Path(
            (env_overrides or {}).get("MN_RUNS_ROOT")
            or os.getenv("MN_RUNS_ROOT")
            or DEFAULT_RUNS_ROOT
        ).expanduser()
    )


def with_shared_run_store_config(
    config: Optional[dict[str, Any]],
    run_id: str,
    runs_root: str,
) -> dict[str, Any]:
    resolved = json.loads(json.dumps(config or {}))
    identity = resolved.setdefault("identity", {})
    if isinstance(identity, dict):
        identity["run_id"] = run_id
    outputs = resolved.setdefault("outputs", {})
    if isinstance(outputs, dict):
        outputs["run_root"] = runs_root
        outputs.setdefault("write_run_store", True)
    return resolved


def blueprint_runtime_environment(
    bundle_dir: Path,
    *,
    config: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, str]:
    env: dict[str, str] = runtime_path_environment()
    if config is None:
        config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    if config is not None:
        env["MN_BLUEPRINT_CONFIG_JSON"] = json.dumps(config, sort_keys=True)
        projected_config = load_blueprint_config_overwrites(
            bundle_dir, config_overrides=config_overrides
        )
        if projected_config is not None:
            env.update(config_to_environment(projected_config))
        docker_model_env = resolve_llm_environment(config)
        if docker_model_env:
            env.update(docker_model_env)

    scenario_path = bundle_dir / "scenario.json"
    if scenario_path.exists():
        env["MN_BLUEPRINT_SCENARIO_JSON"] = scenario_path.read_text(encoding="utf-8")
    return env


def apply_manifest_config_bindings(
    manifest: dict[str, Any], config: dict[str, Any]
) -> None:
    bindings = config.get("manifest_config_bindings") or []
    if not isinstance(bindings, list):
        return
    for binding in bindings:
        if not isinstance(binding, dict):
            continue
        config_path = binding.get("config_path") or binding.get("from")
        manifest_path = binding.get("manifest_path") or binding.get("to")
        if not isinstance(config_path, str) or not isinstance(manifest_path, str):
            continue
        value = config_path_get(config, config_path)
        if value is None and not binding.get("allow_null", False):
            continue
        if binding.get("stringify") is True:
            value = str(value).lower() if isinstance(value, bool) else str(value)
        set_manifest_path(manifest, manifest_path, value)


def refresh_embedded_blueprint_config(manifest: dict[str, Any], config: dict[str, Any]) -> None:
    encoded = json.dumps(config, sort_keys=True)
    for node in manifest_nodes(manifest):
        node_config = node.get("config")
        if not isinstance(node_config, dict):
            continue
        environment = node_config.get("environment")
        if not isinstance(environment, dict):
            continue
        if "MN_BLUEPRINT_CONFIG_JSON" in environment:
            environment["MN_BLUEPRINT_CONFIG_JSON"] = encoded


def config_to_environment(config: dict[str, Any]) -> dict[str, str]:
    env: dict[str, str] = {}
    docker_model_env = resolve_llm_environment(config)
    for path, names in (
        ("video_source.uri", ("VIDEO_SOURCE_URI",)),
        ("video_source.transport", ("VIDEO_SOURCE_TRANSPORT",)),
        ("video_source.codec", ("VIDEO_SOURCE_CODEC",)),
        ("video_source.camera_id", ("VIDEO_SOURCE_CAMERA_ID",)),
        ("video_source.frame_sample_seconds", ("FRAME_SAMPLE_SECONDS",)),
        ("video_source.frame_jpeg_max_width", ("FRAME_JPEG_MAX_WIDTH",)),
        ("vl_model.base_url", ("VL_MODEL_BASE_URL", "OLLAMA_BASE_URL")),
        ("vl_model.model", ("VL_MODEL_NAME", "OLLAMA_MODEL")),
        (
            "vl_model.timeout_seconds",
            ("VL_MODEL_TIMEOUT_SECONDS", "OLLAMA_TIMEOUT_SECONDS"),
        ),
        ("vl_model.temperature", ("VL_MODEL_TEMPERATURE", "OLLAMA_TEMPERATURE")),
    ):
        value = config_path_get(config, path)
        if value is None:
            continue
        for name in names:
            env[name] = str(value)

    if docker_model_env:
        env.update(docker_model_env)
        return env

    for path, names in (
        ("llm.api_base", ("MN_LLM_API_BASE", "LITELLM_API_BASE")),
        ("llm.model", ("MN_LLM_MODEL", "LITELLM_MODEL")),
        ("llm.timeout_seconds", ("MN_LLM_TIMEOUT_SECONDS", "LITELLM_TIMEOUT_SECONDS")),
        ("llm.max_tokens", ("MN_LLM_MAX_TOKENS", "LITELLM_MAX_TOKENS")),
        ("llm.num_retries", ("MN_LLM_NUM_RETRIES", "LITELLM_NUM_RETRIES")),
    ):
        value = config_path_get(config, path)
        if value is None:
            continue
        for name in names:
            env[name] = str(value)
    return env


def set_manifest_path(target: Any, dotted_path: str, value: Any) -> None:
    parts = [part for part in dotted_path.split(".") if part]
    _set_path(target, parts, value)


def _set_path(cursor: Any, parts: list[str], value: Any) -> None:
    if not parts:
        return
    part = parts[0]
    rest = parts[1:]

    if isinstance(cursor, list):
        for item in _list_targets(cursor, part):
            _set_path(item, rest, value)
        return

    if not isinstance(cursor, dict):
        return

    if len(parts) == 1:
        cursor[part] = value
        return

    next_value = cursor.get(part)
    if isinstance(next_value, list):
        _set_path(next_value, rest, value)
        return
    if not isinstance(next_value, dict):
        next_value = {}
        cursor[part] = next_value
    _set_path(next_value, rest, value)


def _list_targets(items: list[Any], selector: str) -> list[Any]:
    if selector == "*":
        return [item for item in items if isinstance(item, dict)]
    if selector.isdigit():
        index = int(selector)
        if 0 <= index < len(items):
            return [items[index]]
        return []
    if selector.endswith("*"):
        prefix = selector[:-1]
        return [
            item
            for item in items
            if isinstance(item, dict)
            and str(item.get("node_id") or item.get("id") or "").startswith(prefix)
        ]
    return [
        item
        for item in items
        if isinstance(item, dict)
        and (
            item.get("node_id") == selector
            or item.get("id") == selector
            or item.get("edge_id") == selector
        )
    ]


def config_path_get(config: dict[str, Any], dotted_path: str) -> Any:
    cursor: Any = config
    for part in dotted_path.split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            return None
        cursor = cursor[part]
    return cursor


def load_blueprint_config(
    bundle_dir: Path,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any] | None:
    config: dict[str, Any] = {}
    loaded = False
    for path in (
        bundle_dir / "config" / "default.json",
        bundle_dir / "config" / "overwrite.json",
    ):
        if path.exists():
            config = deep_merge(config, read_json_object(path))
            loaded = True
    if config_overrides:
        config = deep_merge(config, config_overrides)
        loaded = True
    return config if loaded else None


def load_blueprint_config_overwrites(
    bundle_dir: Path,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any] | None:
    config: dict[str, Any] = {}
    loaded = False
    overwrite_path = bundle_dir / "config" / "overwrite.json"
    if overwrite_path.exists():
        config = deep_merge(config, read_json_object(overwrite_path))
        loaded = True
    if config_overrides:
        config = deep_merge(config, config_overrides)
        loaded = True
    return config if loaded else None


def inject_node_environment(manifest: dict[str, Any], env: dict[str, str]) -> None:
    for node in manifest_nodes(manifest):
        config = node.setdefault("config", {})
        if not isinstance(config, dict):
            continue
        environment = config.setdefault("environment", {})
        if not isinstance(environment, dict):
            continue
        existing_env = dict(environment)
        node_env = dict(env)
        if existing_env.get("PYTHONPATH") and node_env.get("PYTHONPATH"):
            existing_env["PYTHONPATH"] = merge_path_values(
                str(existing_env["PYTHONPATH"]),
                str(node_env["PYTHONPATH"]),
            )
        adjust_llm_environment_for_node(node_env, node)
        environment.clear()
        environment.update(node_env)
        environment.update(existing_env)
        add_mn_llm_aliases(environment)


def merge_path_values(*values: str) -> str:
    merged: list[str] = []
    for value in values:
        for item in value.split(os.pathsep):
            item = item.strip()
            if item and item not in merged:
                merged.append(item)
    return os.pathsep.join(merged)


def add_mn_llm_aliases(environment: dict[str, Any]) -> None:
    for legacy, primary in (
        ("LITELLM_MODEL", "MN_LLM_MODEL"),
        ("LITELLM_API_BASE", "MN_LLM_API_BASE"),
        ("LITELLM_API_KEY", "MN_LLM_API_KEY"),
        ("LITELLM_TIMEOUT_SECONDS", "MN_LLM_TIMEOUT_SECONDS"),
        ("LITELLM_MAX_TOKENS", "MN_LLM_MAX_TOKENS"),
        ("LITELLM_NUM_RETRIES", "MN_LLM_NUM_RETRIES"),
        ("LITELLM_RETRY_BACKOFF_SECONDS", "MN_LLM_RETRY_BACKOFF_SECONDS"),
    ):
        if primary not in environment and legacy in environment:
            environment[primary] = environment[legacy]


def adjust_llm_environment_for_node(environment: dict[str, Any], node: dict[str, Any]) -> None:
    if environment.get("MN_LLM_PROVIDER") != "docker_model_runner":
        return
    api_base = str(environment.get("MN_LLM_API_BASE") or "")
    if "localhost:12434" in api_base or "127.0.0.1:12434" in api_base:
        environment["MN_LLM_API_BASE"] = DOCKER_MODEL_RUNNER_CONTAINER_API_BASE


def strip_docker_model_runner_placement_requirements(manifest: dict[str, Any]) -> None:
    if not _manifest_uses_docker_model_runner_http_endpoint(manifest):
        return

    _strip_service_list_field(manifest, "required_services")
    _strip_docker_model_runner_runtime_models(manifest)
    _strip_runtime_binding_worker_models(manifest)
    for node in _service_requirement_nodes(manifest):
        _strip_service_list_field(node, "requires_services")
        _strip_service_list_field(node, "services")


def _manifest_uses_docker_model_runner_http_endpoint(manifest: dict[str, Any]) -> bool:
    for node in _service_requirement_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        environment = config.get("environment") if isinstance(config.get("environment"), dict) else {}
        provider = str(environment.get("MN_LLM_PROVIDER") or "").strip().lower()
        api_base = str(environment.get("MN_LLM_API_BASE") or "").strip().lower()
        if provider == "docker_model_runner" and (
            api_base.startswith("http://") or api_base.startswith("https://")
        ):
            return True
    return False


def _service_requirement_nodes(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    groups: list[Any] = [manifest.get("nodes")]
    for section_name in ("agents", "flow"):
        section = manifest.get(section_name)
        if isinstance(section, dict):
            groups.append(section.get("nodes"))

    nodes: list[dict[str, Any]] = []
    seen: set[int] = set()
    for group in groups:
        if not isinstance(group, list):
            continue
        for item in group:
            if not isinstance(item, dict):
                continue
            marker = id(item)
            if marker in seen:
                continue
            seen.add(marker)
            nodes.append(item)
    return nodes


def _strip_service_list_field(target: dict[str, Any], field: str) -> None:
    value = target.get(field)
    if isinstance(value, list):
        retained = [service for service in value if not _is_docker_model_runner_service(service)]
        if retained:
            target[field] = retained
        else:
            target.pop(field, None)
        return
    if _is_docker_model_runner_service(value):
        target.pop(field, None)


def _strip_docker_model_runner_runtime_models(manifest: dict[str, Any]) -> None:
    runtime = manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else None
    models = runtime.get("models") if isinstance(runtime, dict) and isinstance(runtime.get("models"), dict) else None
    if not isinstance(models, dict):
        return
    retained = {
        name: model
        for name, model in models.items()
        if not isinstance(model, dict)
        or str(model.get("provider") or model.get("mode") or "").strip().lower()
        not in {"", "docker_model_runner", "docker-model-runner", "dmr"}
    }
    if retained:
        runtime["models"] = retained
    else:
        runtime.pop("models", None)


def _strip_runtime_binding_worker_models(manifest: dict[str, Any]) -> None:
    runtime = manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else None
    bindings = runtime.get("bindings") if isinstance(runtime, dict) and isinstance(runtime.get("bindings"), dict) else None
    if not isinstance(bindings, dict):
        return
    for binding in bindings.values():
        if not isinstance(binding, dict):
            continue
        workers = binding.get("workers")
        if not isinstance(workers, list):
            continue
        for worker in workers:
            if isinstance(worker, dict):
                worker.pop("model", None)


def _is_docker_model_runner_service(service: Any) -> bool:
    if not isinstance(service, dict):
        return False
    name = str(service.get("name") or service.get("service") or "").strip().lower()
    return name in {"docker-model-runner", "docker_model_runner"}


def normalize_host_local_uploads(manifest: dict[str, Any]) -> None:
    for node in manifest_nodes(manifest):
        config = node.get("config")
        if not isinstance(config, dict):
            continue
        if config.get("runner_module") != "MirrorNeuron.Runner.HostLocal":
            continue
        upload_paths = config.get("upload_paths")
        if not isinstance(upload_paths, list) or len(upload_paths) <= 1:
            continue
        config["upload_path"] = "."
        config["upload_as"] = "."
        config.pop("upload_paths", None)


def run_mode_label(manifest: dict) -> str:
    policies = manifest.get("policies", {}) if isinstance(manifest.get("policies"), dict) else {}
    scheduler = policies.get("scheduler", {}) if isinstance(policies.get("scheduler"), dict) else {}
    manifest_type = str(
        policies.get("job_type")
        or scheduler.get("job_type")
        or manifest.get("job_type")
        or manifest.get("type")
        or "batch"
    ).lower()
    is_live = manifest_type == "service" or policies.get("stream_mode") == "live"
    if manifest_type == "service":
        return "Live service"
    if is_live:
        return "Live"
    return "Batch"


def read_json_object(path: Path) -> dict[str, Any]:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return decoded


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
