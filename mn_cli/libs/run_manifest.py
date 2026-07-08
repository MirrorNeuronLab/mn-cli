from __future__ import annotations

import json
import os
import re
import tomllib
from pathlib import Path
from typing import Any, Optional

from mn_sdk import (
    expand_manifest_source,
    expand_manifest_model_service_requirements,
    is_manifest_source,
)
from mn_sdk.blueprint_runtime import (
    add_mn_llm_aliases as sdk_add_mn_llm_aliases,
    adjust_llm_environment_for_node as sdk_adjust_llm_environment_for_node,
    apply_manifest_config_bindings as sdk_apply_manifest_config_bindings,
    blueprint_runtime_environment as sdk_blueprint_runtime_environment,
    config_path_get as sdk_config_path_get,
    config_to_environment as sdk_config_to_environment,
    config_uses_docker_worker_skill_runtime as sdk_config_uses_docker_worker_skill_runtime,
    deep_merge as sdk_deep_merge,
    docker_worker_runtime_service_environment as sdk_docker_worker_runtime_service_environment,
    inject_node_environment as sdk_inject_node_environment,
    load_blueprint_config as sdk_load_blueprint_config,
    load_blueprint_config_overwrites as sdk_load_blueprint_config_overwrites,
    merge_path_values as sdk_merge_path_values,
    set_manifest_path as sdk_set_manifest_path,
    shared_runs_root as sdk_shared_runs_root,
    with_shared_run_store_config as sdk_with_shared_run_store_config,
)
from mn_sdk.runtime_modules import (
    ensure_runtime_modules_for_manifest,
    local_skill_source_roots as sdk_local_skill_source_roots,
    local_skill_sources_enabled as sdk_local_skill_sources_enabled,
    runtime_path_environment as sdk_runtime_path_environment,
)
from mn_sdk.blueprint_support import (
    inject_runtime_web_ui_service,
    render_manifest_agent_templates,
    runtime_web_ui_service_from_manifest,
    runtime_web_ui_support_payloads,
    stage_local_input_payloads_for_manifest as stage_sdk_local_input_payloads,
)
from mn_cli.libs.skill_runtime import (
    prepare_skill_runtime_for_manifest,
    stage_skill_runtime_payloads_for_manifest,
)
from mn_cli.libs.skill_dependencies import (
    DEFAULT_SKILL_PACKAGE_VERSION,
    GAR_PIP_INDEX_URL,
    PYPI_PIP_INDEX_URL,
    gar_requirement_lines,
    normalize_package_name,
    pinned_skill_dependency_requirements,
    requirement_package_name,
    skill_dependency_records,
    skill_dependency_package_names,
    without_requirements_for_packages,
)
from mn_cli.runtime_state import mn_home
USER_HOME_ENV_KEYS = ("MN_OUTPUT_HOME", "MN_USER_HOME", "OTTERDESK_USER_HOME")
UPLOAD_SOURCE_EXCLUDED_DIRS = {
    ".git",
    ".hg",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "venv",
}
BLUEPRINT_SUPPORT_SOURCE = "blueprint_support_skill"
SDK_SOURCE = "mn-python-sdk"
WORKSPACE_BASES = {"workspace_root", "workspace", "repo_root"}
HOST_LOCAL_RUNNER = "MirrorNeuron.Runner.HostLocal"
DOCKER_WORKER_RUNNER = "MirrorNeuron.Runner.DockerWorker"
SKILL_DEPENDENCY_CONTEXT_ROOT = "__mn_skill_dependencies"
SKILL_DEPENDENCY_MARKER = "# mirrorneuron: skill-dependencies"
SKILL_DEPENDENCY_END_MARKER = "# mirrorneuron: skill-dependencies-end"
LOCAL_SKILL_CONTEXT_ROOT = ".mn-local-skills"
RUNTIME_REQUIREMENTS_COPY_LINE = "COPY requirements.txt /tmp/mn-skill-runtime/requirements.txt"
LOCAL_REQUIREMENTS_COPY_LINE = "COPY local-requirements.txt /tmp/mn-skill-runtime/local-requirements.txt"


def workspace_root() -> Path:
    for name in (
        "MN_WORKSPACE_ROOT",
    ):
        value = os.getenv(name)
        if value:
            return Path(value).expanduser().resolve()
    for candidate in (Path.cwd(), *Path.cwd().parents):
        if (candidate / "mn-skills").is_dir():
            return candidate.resolve()
    return Path(__file__).resolve().parents[3]


def runtime_path_environment() -> dict[str, str]:
    env = sdk_runtime_path_environment(env=_sdk_runtime_env(), workspace_root=workspace_root())
    env.update(user_home_environment())
    return env


def local_skill_sources_enabled() -> bool:
    return sdk_local_skill_sources_enabled(env=_sdk_runtime_env(), default_for_dev_env=True)


def _local_skill_source_roots() -> list[Path]:
    """Return candidate local skill roots, ordered by explicit env then fallbacks."""

    return sdk_local_skill_source_roots(env=_sdk_runtime_env(), workspace_root=workspace_root())


def _sdk_runtime_env() -> dict[str, str]:
    values = dict(os.environ)
    values["MN_HOME"] = str(mn_home())
    return values


def localize_skill_dependencies_for_dev(manifest: dict[str, Any]) -> dict[str, Any]:
    """Stage local skill sources for dev runs and leave prod GAR dependencies intact."""

    if not local_skill_sources_enabled():
        return {"localized": 0, "packages": []}

    records = skill_dependency_records(manifest)
    if not records:
        return {"localized": 0, "packages": []}

    local_sources: dict[str, Path] = {}
    for skills_root in _local_skill_source_roots():
        for package, source in _local_skill_sources_by_package(skills_root).items():
            local_sources.setdefault(package, source)
    selected: dict[str, tuple[dict[str, str], Path]] = {}
    for record in records:
        package_key = normalize_package_name(record["name"])
        source = local_sources.get(package_key)
        if source:
            selected[package_key] = (record, source)

    if not selected:
        return {"localized": 0, "packages": []}

    upload_roots = _docker_worker_upload_roots(manifest)
    if not upload_roots:
        return {"localized": 0, "packages": []}

    raw_dependencies = manifest.get("skill_dependencies")
    if isinstance(raw_dependencies, list):
        manifest["skill_dependencies"] = [
            item
            for item in raw_dependencies
            if not (
                isinstance(item, dict)
                and normalize_package_name(str(item.get("name") or "")) in selected
            )
        ]

    packages: list[str] = []
    sources: list[dict[str, str]] = []
    for _package_key, (record, source) in selected.items():
        if record["name"] not in packages:
            packages.append(record["name"])
        for upload_root in upload_roots:
            target = "/".join(
                part
                for part in (
                    upload_root.strip("/"),
                    LOCAL_SKILL_CONTEXT_ROOT,
                    source.name,
                )
                if part
            )
            sources.append(
                {
                    "package": record["name"],
                    "source": str(source),
                    "target": target,
                }
            )

    if packages:
        metadata = manifest.setdefault("metadata", {})
        if isinstance(metadata, dict):
            metadata["mn_local_skill_dependencies"] = {
                "context_root": LOCAL_SKILL_CONTEXT_ROOT,
                "packages": packages,
                "sources": sources,
            }
    return {"localized": len(packages), "packages": packages}


def _local_skill_sources_by_package(skills_root: Path) -> dict[str, Path]:
    if not skills_root.is_dir():
        return {}
    sources: dict[str, Path] = {}
    for pyproject in skills_root.glob("*/pyproject.toml"):
        try:
            data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        project = data.get("project") if isinstance(data, dict) else {}
        name = project.get("name") if isinstance(project, dict) else None
        if isinstance(name, str) and name.strip():
            sources[normalize_package_name(name)] = pyproject.parent
    return sources


def _docker_worker_upload_roots(manifest: dict[str, Any]) -> list[str]:
    roots: list[str] = []
    has_docker_worker = False
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") != DOCKER_WORKER_RUNNER:
            continue
        has_docker_worker = True
        sources = _node_upload_sources_for_workdir(config) or _node_upload_sources(config)
        for source in sources:
            cleaned = source.strip("/")
            if cleaned and cleaned not in roots:
                roots.append(cleaned)
    if not has_docker_worker:
        return []
    return roots or [""]


def _node_upload_sources_for_workdir(config: dict[str, Any]) -> list[str]:
    workdir = str(config.get("workdir") or "").strip().strip("/")
    if not workdir.startswith("mn/job/"):
        return []
    workdir_target = workdir.removeprefix("mn/job/").strip("/")
    if not workdir_target:
        return []

    upload_paths = config.get("upload_paths")
    if not isinstance(upload_paths, list):
        return []

    sources: list[str] = []
    for entry in upload_paths:
        if not isinstance(entry, dict):
            continue
        source = str(entry.get("source") or "").strip().strip("/")
        target = str(entry.get("target") or Path(source).name).strip().strip("/")
        if source and target == workdir_target:
            sources.append(source)
    return sources


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
    if is_manifest_source(prepared):
        prepared = expand_manifest_source(prepared, root_dir=bundle_dir)
    ensure_runtime_modules_for_manifest(prepared, workspace_root=workspace_root())
    render_agent_templates_for_submission(prepared)
    metadata = dict(submission_metadata or {})
    fake_llm = bool(metadata.get("fake_llm")) or str((env_overrides or {}).get("MN_LLM_PROVIDER") or "").strip().lower() == "fake"
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
        ensure_runtime_modules_for_manifest(prepared, config, workspace_root=workspace_root())
        prepare_skill_runtime_for_manifest(
            prepared,
            config,
            bundle_dir=bundle_dir,
            workspace_root=workspace_root(),
        )
        ensure_blueprint_support_sdk_build_context_uploads(prepared)
        refresh_embedded_blueprint_config(prepared, config)
    localize_skill_dependencies_for_dev(prepared)
    inject_skill_dependency_python_environments(prepared)
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
    if skill_dependency_package_names(prepared):
        runtime_env = release_skill_dependency_runtime_environment(runtime_env)
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
    expand_manifest_model_service_requirements(prepared, config or {}, env=runtime_env)
    if runtime_env:
        inject_node_environment(prepared, runtime_env)
        strip_docker_model_runner_placement_requirements(prepared, force=fake_llm)
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
    ensure_runtime_modules_for_manifest(manifest, config, workspace_root=workspace_root())
    return inject_runtime_web_ui_service(
        manifest,
        bundle_dir=bundle_dir,
        config=config,
        run_id=run_id,
        runs_root=runs_root,
        env_overrides=env_overrides,
    )


def runtime_web_ui_support_payloads_for_manifest(manifest: dict[str, Any]) -> dict[str, bytes]:
    ensure_runtime_modules_for_manifest(manifest, workspace_root=workspace_root())
    if not runtime_web_ui_service_from_manifest(manifest):
        return {}
    return runtime_web_ui_support_payloads()


def stage_local_input_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    ensure_runtime_modules_for_manifest(manifest, workspace_root=workspace_root())
    try:
        return stage_sdk_local_input_payloads(manifest, payloads, bundle_dir=bundle_dir)
    except RuntimeError:
        if not manifest_local_inputs_enabled(manifest):
            return {}
        raise


def stage_blueprint_support_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    if "mirrorneuron-blueprint-support-skill" in skill_dependency_package_names(manifest):
        return {"staged": False, "sources": []}

    skills_root = Path(runtime_path_environment()["MN_SKILLS_ROOT"])
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


def ensure_blueprint_support_sdk_build_context_uploads(manifest: dict[str, Any]) -> dict[str, Any]:
    """Stage the SDK beside blueprint-support shims in DockerWorker build contexts."""

    if "mirrorneuron-blueprint-support-skill" in skill_dependency_package_names(manifest):
        return {"added": 0, "sources": []}

    sdk_root = workspace_root() / SDK_SOURCE
    if not sdk_root.joinpath("pyproject.toml").is_file():
        return {"added": 0, "sources": []}

    added: list[str] = []
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        uploads = config.get("build_context_upload_paths")
        if not isinstance(uploads, list):
            continue
        for entry in list(uploads):
            if not isinstance(entry, dict):
                continue
            base = str(entry.get("base") or "payloads").strip()
            source = str(entry.get("source") or "").strip().strip("/")
            if base not in {"skills_root", "mn_skills", "skills"} or source != BLUEPRINT_SUPPORT_SOURCE:
                continue
            target = str(entry.get("target") or "").strip().strip("/")
            sdk_target = _sdk_build_context_target(target)
            if not sdk_target:
                continue
            if _has_build_context_upload(uploads, base="workspace_root", source=SDK_SOURCE, target=sdk_target):
                continue
            uploads.append({"base": "workspace_root", "source": SDK_SOURCE, "target": sdk_target})
            added.append(sdk_target)
    return {"added": len(added), "sources": added}


def stage_skill_runtime_support_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    return stage_skill_runtime_payloads_for_manifest(
        manifest,
        payloads,
        bundle_dir=bundle_dir,
    )


def stage_skill_dependency_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    staged_sources: list[str] = list(_stage_local_skill_dependency_payloads(manifest, payloads))
    config = _manifest_blueprint_config(manifest)
    local_sources = _local_skill_dependency_source_records(manifest)
    for context_path in _docker_worker_context_paths(manifest):
        dockerfile_key = _payload_join(context_path, "Dockerfile")
        if dockerfile_key not in payloads:
            dockerfile_path = bundle_dir / "payloads" / dockerfile_key
            if dockerfile_path.is_file():
                payloads[dockerfile_key] = dockerfile_path.read_bytes()
        dockerfile_bytes = payloads.get(dockerfile_key)
        if dockerfile_bytes is None:
            continue

        local_context_sources = _stage_local_skill_dependency_context_sources(
            context_path,
            local_sources,
            payloads,
        )
        requirements_text = _docker_worker_requirements_text(
            manifest,
            payloads,
            bundle_dir=bundle_dir,
            context_path=context_path,
            config=config,
        )
        local_requirements_text = _local_skill_requirements_text(local_context_sources)
        if not requirements_text and not local_requirements_text:
            staged_sources.extend(local_context_sources)
            continue

        requirements_key = _payload_join(context_path, "requirements.txt")
        payloads[requirements_key] = requirements_text.encode("utf-8")
        staged_payload_keys = [requirements_key]
        if local_requirements_text:
            local_requirements_key = _payload_join(context_path, "local-requirements.txt")
            payloads[local_requirements_key] = local_requirements_text.encode("utf-8")
            staged_payload_keys.append(local_requirements_key)

        dockerfile_text = dockerfile_bytes.decode("utf-8", errors="ignore")
        next_dockerfile = _ensure_docker_worker_requirements_install(
            dockerfile_text,
            local_context_sources=local_context_sources,
        )
        if next_dockerfile != dockerfile_text:
            payloads[dockerfile_key] = next_dockerfile.encode("utf-8")
            staged_sources.append(dockerfile_key)
        staged_sources.extend([*staged_payload_keys, *local_context_sources])

    return {"staged": bool(staged_sources), "sources": _dedupe_strings(staged_sources)}


def _manifest_blueprint_config(manifest: dict[str, Any]) -> dict[str, Any]:
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        environment = config.get("environment") if isinstance(config.get("environment"), dict) else {}
        raw = environment.get("MN_BLUEPRINT_CONFIG_JSON")
        if not isinstance(raw, str) or not raw.strip():
            continue
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict):
            return decoded
    return {}


def _docker_worker_requirements_text(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
    context_path: str,
    config: dict[str, Any],
) -> str:
    local_package_names = {
        normalize_package_name(str(record.get("package") or ""))
        for record in _local_skill_dependency_source_records(manifest)
        if str(record.get("package") or "").strip()
    }
    manifest_skill_packages = skill_dependency_package_names(manifest)
    configured_skill_packages = _configured_skill_package_names(config)
    skill_layer_packages = manifest_skill_packages | local_package_names | configured_skill_packages

    lines: list[str] = []
    foundation = _foundation_requirement_lines(config, payloads, bundle_dir, context_path)
    lines.extend(without_requirements_for_packages(foundation, skill_layer_packages))
    lines.extend(_configured_non_skill_package_lines(config, skill_layer_packages))

    skill_lines = pinned_skill_dependency_requirements(manifest)
    skill_lines.extend(
        _configured_skill_requirement_lines(
            config,
            manifest_skill_packages | local_package_names,
        )
    )
    if skill_lines:
        lines = _ensure_pip_index_lines(lines, config)
    lines.extend(skill_lines)
    return _requirements_text(lines)


def _local_skill_requirements_text(local_context_sources: list[str]) -> str:
    return _requirements_text(_local_skill_install_requirement_lines(local_context_sources))


def _foundation_requirement_lines(
    config: dict[str, Any],
    payloads: dict[str, bytes],
    bundle_dir: Path,
    context_path: str,
) -> list[str]:
    paths: list[str] = []
    python_dependencies = config.get("python_dependencies") if isinstance(config.get("python_dependencies"), dict) else {}
    configured = python_dependencies.get("requirements")
    if isinstance(configured, str) and configured.strip():
        paths.append(configured.strip().strip("/"))
    context_requirements = _payload_join(context_path, "requirements.txt")
    if context_requirements not in paths:
        paths.append(context_requirements)

    lines: list[str] = []
    for path in paths:
        text = _payload_or_file_text(payloads, bundle_dir, path)
        if text:
            lines.extend(text.splitlines())
    return lines


def _payload_or_file_text(payloads: dict[str, bytes], bundle_dir: Path, path: str) -> str:
    payload = payloads.get(path)
    if payload is not None:
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError:
            return ""
    candidate = bundle_dir / "payloads" / path
    try:
        return candidate.read_text(encoding="utf-8") if candidate.is_file() else ""
    except OSError:
        return ""


def _configured_non_skill_package_lines(config: dict[str, Any], excluded_packages: set[str]) -> list[str]:
    python_dependencies = config.get("python_dependencies") if isinstance(config.get("python_dependencies"), dict) else {}
    lines: list[str] = []
    for package in python_dependencies.get("packages", []):
        if not isinstance(package, str) or not package.strip():
            continue
        name = requirement_package_name(package)
        if not name or name in excluded_packages or _is_skill_requirement(package):
            continue
        lines.append(package.strip())
    return lines


def _configured_skill_package_names(config: dict[str, Any]) -> set[str]:
    python_dependencies = config.get("python_dependencies") if isinstance(config.get("python_dependencies"), dict) else {}
    return {
        package_name
        for package in python_dependencies.get("packages", [])
        if isinstance(package, str)
        and _is_skill_requirement(package)
        and (package_name := requirement_package_name(package))
    }


def _configured_skill_requirement_lines(config: dict[str, Any], known_packages: set[str]) -> list[str]:
    python_dependencies = config.get("python_dependencies") if isinstance(config.get("python_dependencies"), dict) else {}
    lines: list[str] = []
    for package in python_dependencies.get("packages", []):
        if not isinstance(package, str) or not _is_skill_requirement(package):
            continue
        package_name = requirement_package_name(package)
        if not package_name or package_name in known_packages:
            continue
        text = package.strip()
        if re.search(r"(==|~=|!=|<=|>=|<|>)", text):
            lines.append(text)
            continue
        lines.append(f"{_requirement_display_name(text)}=={DEFAULT_SKILL_PACKAGE_VERSION}")
    return lines


def _is_skill_requirement(requirement: str) -> bool:
    package = requirement_package_name(requirement)
    return bool(package and package.startswith("mirrorneuron-") and package.endswith("-skill"))


def _requirement_display_name(requirement: str) -> str:
    match = re.match(r"([A-Za-z0-9_.-]+(?:\[[^\]]+\])?)", requirement.strip())
    return match.group(1) if match else requirement.strip()


def _ensure_pip_index_lines(lines: list[str], config: dict[str, Any]) -> list[str]:
    if any(line.strip().startswith("--index-url") for line in lines):
        return lines
    python_dependencies = config.get("python_dependencies") if isinstance(config.get("python_dependencies"), dict) else {}
    index_url = str(python_dependencies.get("index_url") or GAR_PIP_INDEX_URL).strip()
    extra_index_url = str(python_dependencies.get("extra_index_url") or PYPI_PIP_INDEX_URL).strip()
    index_lines = [f"--index-url {index_url}"] if index_url else []
    if extra_index_url:
        index_lines.append(f"--extra-index-url {extra_index_url}")
    return [*index_lines, *lines]


def _requirements_text(lines: list[str]) -> str:
    output: list[str] = []
    seen: set[str] = set()
    for line in lines:
        text = str(line).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return "\n".join(output) + ("\n" if output else "")


def _local_skill_dependency_source_records(manifest: dict[str, Any]) -> list[dict[str, str]]:
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    local = metadata.get("mn_local_skill_dependencies") if isinstance(metadata, dict) else {}
    sources = local.get("sources") if isinstance(local, dict) else None
    if not isinstance(sources, list):
        return []

    records: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for entry in sources:
        if not isinstance(entry, dict):
            continue
        source = str(entry.get("source") or "").strip()
        package = str(entry.get("package") or "").strip()
        if not source or not package:
            continue
        key = (normalize_package_name(package), str(Path(source).expanduser()))
        if key in seen:
            continue
        seen.add(key)
        records.append({"package": package, "source": source})
    return records


def _stage_local_skill_dependency_context_sources(
    context_path: str,
    local_sources: list[dict[str, str]],
    payloads: dict[str, bytes],
) -> list[str]:
    staged: list[str] = []
    for record in local_sources:
        source = Path(str(record.get("source") or "")).expanduser()
        if not source.exists():
            continue
        target = _payload_join(
            context_path,
            SKILL_DEPENDENCY_CONTEXT_ROOT,
            "local",
            _safe_dependency_source_name(source),
        )
        if _add_upload_source_to_payloads(source, target, payloads):
            staged.append(target)
    return _dedupe_strings(staged)


def _safe_dependency_source_name(source: Path) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", source.name).strip(".-")
    return safe or "skill"


def _local_skill_install_requirement_lines(local_context_sources: list[str]) -> list[str]:
    return [
        f"/tmp/mn-skill-runtime/local/{Path(source).name}"
        for source in local_context_sources
        if Path(source).name
    ]


def _ensure_docker_worker_requirements_install(
    dockerfile_text: str,
    *,
    local_context_sources: list[str],
) -> str:
    copy_block = _local_skill_copy_block(local_context_sources)
    next_text = _replace_skill_dependency_marker_block(dockerfile_text, copy_block)
    if local_context_sources and LOCAL_REQUIREMENTS_COPY_LINE not in next_text:
        next_text = _insert_after_runtime_requirements_copy(next_text, LOCAL_REQUIREMENTS_COPY_LINE)
    if next_text == dockerfile_text and local_context_sources:
        next_text = _insert_after_runtime_requirements_copy(next_text, copy_block)
    installs_runtime = _dockerfile_installs_runtime_requirements(next_text)
    installs_local = _dockerfile_installs_local_requirements(next_text)
    has_local_copy_block = not local_context_sources or copy_block in next_text
    if installs_runtime and (not local_context_sources or (installs_local and has_local_copy_block)):
        return next_text

    lines: list[str] = []
    if RUNTIME_REQUIREMENTS_COPY_LINE not in next_text:
        lines.append(RUNTIME_REQUIREMENTS_COPY_LINE)
    if local_context_sources and LOCAL_REQUIREMENTS_COPY_LINE not in next_text:
        lines.append(LOCAL_REQUIREMENTS_COPY_LINE)
    if local_context_sources and copy_block not in next_text:
        lines.append(copy_block)
    if not installs_runtime:
        lines.extend(_runtime_requirements_install_lines())
    if local_context_sources and not installs_local:
        lines.extend(_local_requirements_install_lines())
    return next_text.rstrip() + "\n\n" + "\n".join(lines).rstrip() + "\n"


def _local_skill_copy_block(local_context_sources: list[str]) -> str:
    lines = [SKILL_DEPENDENCY_MARKER]
    if local_context_sources:
        for source in local_context_sources:
            name = Path(source).name
            lines.append(
                f"COPY {SKILL_DEPENDENCY_CONTEXT_ROOT}/local/{name} "
                f"/tmp/mn-skill-runtime/local/{name}"
            )
    else:
        lines.append("# No local skill sources staged for this image.")
    lines.append(SKILL_DEPENDENCY_END_MARKER)
    return "\n".join(lines)


def _replace_skill_dependency_marker_block(dockerfile_text: str, block: str) -> str:
    start = dockerfile_text.find(SKILL_DEPENDENCY_MARKER)
    if start < 0:
        return dockerfile_text
    end = dockerfile_text.find(SKILL_DEPENDENCY_END_MARKER, start)
    if end < 0:
        return dockerfile_text
    end += len(SKILL_DEPENDENCY_END_MARKER)
    return dockerfile_text[:start].rstrip() + "\n" + block + "\n" + dockerfile_text[end:].lstrip("\n")


def _insert_after_runtime_requirements_copy(dockerfile_text: str, block: str) -> str:
    lines = dockerfile_text.splitlines()
    for index, line in enumerate(lines):
        if line.strip() != RUNTIME_REQUIREMENTS_COPY_LINE:
            continue
        return "\n".join([*lines[: index + 1], "", block, *lines[index + 1 :]]) + "\n"
    return dockerfile_text


def _dockerfile_installs_runtime_requirements(dockerfile_text: str) -> bool:
    return (
        "/tmp/mn-skill-runtime/requirements.txt" in dockerfile_text
        and "pip install" in dockerfile_text
        and "-r /tmp/mn-skill-runtime/requirements.txt" in dockerfile_text
    )


def _dockerfile_installs_local_requirements(dockerfile_text: str) -> bool:
    return (
        "/tmp/mn-skill-runtime/local-requirements.txt" in dockerfile_text
        and "pip install" in dockerfile_text
        and "-r /tmp/mn-skill-runtime/local-requirements.txt" in dockerfile_text
    )


def _runtime_requirements_install_lines() -> list[str]:
    return [
        "RUN if [ -s /tmp/mn-skill-runtime/requirements.txt ]; then \\",
        "      python3 -m pip install --timeout 120 --retries 10 --break-system-packages --no-cache-dir -r /tmp/mn-skill-runtime/requirements.txt; \\",
        "    fi",
    ]


def _local_requirements_install_lines() -> list[str]:
    return [
        "RUN if [ -s /tmp/mn-skill-runtime/local-requirements.txt ]; then \\",
        "      python3 -m pip install --timeout 120 --retries 10 --break-system-packages --no-cache-dir -r /tmp/mn-skill-runtime/local-requirements.txt; \\",
        "    fi",
    ]


def _stage_local_skill_dependency_payloads(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
) -> list[str]:
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    local = metadata.get("mn_local_skill_dependencies") if isinstance(metadata, dict) else {}
    sources = local.get("sources") if isinstance(local, dict) else None
    if not isinstance(sources, list):
        return []

    staged: list[str] = []
    for entry in sources:
        if not isinstance(entry, dict):
            continue
        source = Path(str(entry.get("source") or "")).expanduser()
        target = str(entry.get("target") or "").strip().strip("/")
        if (
            not source.exists()
            or not target
            or Path(target).is_absolute()
            or ".." in Path(target).parts
        ):
            continue
        if _add_upload_source_to_payloads(source, target, payloads):
            staged.append(target)
    return staged


def inject_skill_dependency_python_environments(manifest: dict[str, Any]) -> dict[str, Any]:
    gar_args = gar_requirement_lines(manifest)
    if not gar_args:
        return {"patched": 0, "nodes": []}

    package_names = skill_dependency_package_names(manifest)
    patched_nodes: list[str] = []
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") != HOST_LOCAL_RUNNER or not _python_hostlocal_node(config):
            continue
        python_environment = config.get("python_environment")
        if not isinstance(python_environment, dict):
            python_environment = {}
            config["python_environment"] = python_environment
        existing = [
            str(package).strip()
            for package in python_environment.get("packages", [])
            if isinstance(package, str) and package.strip()
        ]
        merged: list[str] = []
        seen: set[str] = set()
        for package in [
            *gar_args,
            *without_requirements_for_packages(existing, package_names),
        ]:
            if package in seen:
                continue
            seen.add(package)
            merged.append(package)
        python_environment["packages"] = merged
        patched_nodes.append(str(node.get("node_id") or node.get("id") or ""))

    return {"patched": len(patched_nodes), "nodes": patched_nodes}


def release_skill_dependency_runtime_environment(env: dict[str, str]) -> dict[str, str]:
    cleaned = dict(env)
    workspace = cleaned.pop("MN_WORKSPACE_ROOT", None)
    skills_root = cleaned.pop("MN_SKILLS_ROOT", None)
    pythonpath = cleaned.get("PYTHONPATH")
    if pythonpath:
        excluded_roots = []
        if workspace:
            excluded_roots.append(Path(workspace).expanduser() / "mn-skills")
        if skills_root:
            excluded_roots.append(Path(skills_root).expanduser())
        kept = [
            entry
            for entry in pythonpath.split(os.pathsep)
            if entry and not _path_is_under_any(entry, excluded_roots)
        ]
        if kept:
            cleaned["PYTHONPATH"] = os.pathsep.join(kept)
        else:
            cleaned.pop("PYTHONPATH", None)
    return cleaned


def stage_upload_path_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    staged_sources: list[str] = []
    seen: set[str] = set()
    for source in _manifest_upload_sources(manifest):
        if source in seen:
            continue
        seen.add(source)
        if not _safe_upload_source(source):
            continue
        if _payload_source_present(payloads, source):
            continue
        source_path = (bundle_dir / source).resolve()
        try:
            source_path.relative_to(bundle_dir.resolve())
        except ValueError:
            continue
        if not source_path.exists():
            continue
        file_count = _add_upload_source_to_payloads(source_path, source, payloads)
        if file_count:
            staged_sources.append(source)
    return {"staged": bool(staged_sources), "sources": staged_sources}


def render_agent_templates_for_submission(manifest: dict[str, Any]) -> None:
    nodes = manifest_nodes(manifest)
    if not nodes or not any("uses" in node for node in nodes):
        return
    ensure_runtime_modules_for_manifest(manifest, workspace_root=workspace_root())
    rendered = render_manifest_agent_templates(manifest)
    manifest.clear()
    manifest.update(rendered)


def _manifest_upload_sources(manifest: dict[str, Any]) -> list[str]:
    sources: list[str] = []
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        for source in _node_upload_sources(config):
            if source and source not in sources:
                sources.append(source)
    return sources


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


def _sdk_build_context_target(blueprint_support_target: str) -> str:
    if not blueprint_support_target:
        return ""
    target = blueprint_support_target.strip("/")
    parts = target.split("/")
    if not parts or parts[-1] != BLUEPRINT_SUPPORT_SOURCE:
        return ""
    return "/".join([*parts[:-1], SDK_SOURCE])


def _has_build_context_upload(uploads: list[Any], *, base: str, source: str, target: str) -> bool:
    for item in uploads:
        if not isinstance(item, dict):
            continue
        item_base = str(item.get("base") or "payloads").strip()
        item_source = str(item.get("source") or "").strip().strip("/")
        item_target = str(item.get("target") or "").strip().strip("/")
        if item_base in WORKSPACE_BASES and base in WORKSPACE_BASES and item_source == source and item_target == target:
            return True
        if item_base == base and item_source == source and item_target == target:
            return True
    return False


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


def _safe_upload_source(source: str) -> bool:
    candidate = Path(source)
    return (
        source not in {"", "."}
        and not candidate.is_absolute()
        and ".." not in candidate.parts
        and candidate.parts[:1] != ("payloads",)
    )


def _payload_source_present(payloads: dict[str, bytes], source: str) -> bool:
    prefix = source.rstrip("/") + "/"
    return source in payloads or any(key.startswith(prefix) for key in payloads)


def _add_upload_source_to_payloads(source_path: Path, payload_prefix: str, payloads: dict[str, bytes]) -> int:
    if source_path.is_file():
        payloads[payload_prefix] = source_path.read_bytes()
        return 1
    if not source_path.is_dir():
        return 0
    count = 0
    source_root = source_path.resolve()
    for file_path in sorted(source_root.rglob("*")):
        if _excluded_upload_source_path(file_path, source_root) or file_path.is_symlink():
            continue
        if not file_path.is_file():
            continue
        resolved = file_path.resolve()
        try:
            resolved.relative_to(source_root)
        except ValueError:
            continue
        relative = file_path.relative_to(source_root).as_posix()
        payloads[f"{payload_prefix.rstrip('/')}/{relative}"] = file_path.read_bytes()
        count += 1
    return count


def _excluded_upload_source_path(path: Path, source_root: Path) -> bool:
    try:
        relative = path.relative_to(source_root)
    except ValueError:
        return False
    for part in relative.parts:
        if part in UPLOAD_SOURCE_EXCLUDED_DIRS or part.endswith(".egg-info"):
            return True
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


def _python_hostlocal_node(config: dict[str, Any]) -> bool:
    if "python_environment" in config:
        return True
    command = config.get("command")
    if isinstance(command, list) and command:
        return "python" in Path(str(command[0])).name
    if isinstance(command, str) and command.strip():
        return "python" in command.split(maxsplit=1)[0]
    return False


def _docker_worker_context_paths(manifest: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") != DOCKER_WORKER_RUNNER:
            continue
        for key in ("docker_worker_image", "build"):
            value = config.get(key)
            if isinstance(value, str) and value.strip() and "://" not in value:
                cleaned = _safe_context_path(value)
                if cleaned and cleaned not in paths:
                    paths.append(cleaned)
    return paths


def _safe_context_path(value: str) -> str:
    cleaned = value.strip().strip("/")
    candidate = Path(cleaned)
    if not cleaned or candidate.is_absolute() or ".." in candidate.parts:
        return ""
    if candidate.name == "Dockerfile":
        return candidate.parent.as_posix()
    return cleaned


def _path_is_under_any(value: str, roots: list[Path]) -> bool:
    if not roots:
        return False
    try:
        path = Path(value).expanduser().resolve()
    except OSError:
        return False
    for root in roots:
        try:
            path.relative_to(root.expanduser().resolve())
            return True
        except (OSError, ValueError):
            continue
    return False


def _shared_runs_root(env_overrides: Optional[dict[str, str]] = None) -> str:
    return sdk_shared_runs_root(env_overrides)


def with_shared_run_store_config(
    config: Optional[dict[str, Any]],
    run_id: str,
    runs_root: str,
) -> dict[str, Any]:
    return sdk_with_shared_run_store_config(config, run_id, runs_root)


def blueprint_runtime_environment(
    bundle_dir: Path,
    *,
    config: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, str]:
    return sdk_blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
        runtime_env=runtime_path_environment(),
        include_docker_worker_runtime_env=True,
        read_json_object_fn=read_json_object,
    )


def _config_uses_docker_worker_skill_runtime(config: dict[str, Any]) -> bool:
    return sdk_config_uses_docker_worker_skill_runtime(config)


def _docker_worker_runtime_service_environment() -> dict[str, str]:
    return sdk_docker_worker_runtime_service_environment()


def apply_manifest_config_bindings(
    manifest: dict[str, Any], config: dict[str, Any]
) -> None:
    sdk_apply_manifest_config_bindings(manifest, config)


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
    return sdk_config_to_environment(config)


def set_manifest_path(target: Any, dotted_path: str, value: Any) -> None:
    sdk_set_manifest_path(target, dotted_path, value)


def config_path_get(config: dict[str, Any], dotted_path: str) -> Any:
    return sdk_config_path_get(config, dotted_path)


def load_blueprint_config(
    bundle_dir: Path,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any] | None:
    return sdk_load_blueprint_config(
        bundle_dir,
        config_overrides=config_overrides,
        read_json_object_fn=read_json_object,
    )


def load_blueprint_config_overwrites(
    bundle_dir: Path,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any] | None:
    return sdk_load_blueprint_config_overwrites(
        bundle_dir,
        config_overrides=config_overrides,
        read_json_object_fn=read_json_object,
    )


def inject_node_environment(manifest: dict[str, Any], env: dict[str, str]) -> None:
    sdk_inject_node_environment(
        manifest,
        env,
        nodes=manifest_nodes(manifest),
        preserve_existing=True,
        force_env_keys=("MN_BLUEPRINT_CONFIG_JSON",),
        skip_host_local_dmr_rewrite=False,
    )


def merge_path_values(*values: str) -> str:
    return sdk_merge_path_values(*values)


def add_mn_llm_aliases(environment: dict[str, Any]) -> None:
    sdk_add_mn_llm_aliases(environment)


def adjust_llm_environment_for_node(environment: dict[str, Any], node: dict[str, Any]) -> None:
    sdk_adjust_llm_environment_for_node(environment, node, skip_host_local=False)


def strip_docker_model_runner_placement_requirements(manifest: dict[str, Any], *, force: bool = False) -> None:
    if not force and not _manifest_uses_docker_model_runner_http_endpoint(manifest):
        return

    _strip_service_list_field(manifest, "required_services")
    _strip_docker_model_runner_model_placement_requirements(manifest)
    _strip_docker_model_runner_runtime_models(manifest)
    _strip_runtime_binding_worker_models(manifest)
    for node in _service_requirement_nodes(manifest):
        _strip_service_list_field(node, "requires_services")
        _strip_service_list_field(node, "services")
        _strip_docker_model_runner_model_placement_requirements(node)
        config = node.get("config") if isinstance(node.get("config"), dict) else None
        if config is not None:
            _strip_docker_model_runner_model_placement_requirements(config)


def _manifest_uses_docker_model_runner_http_endpoint(manifest: dict[str, Any]) -> bool:
    for node in _service_requirement_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        environment = config.get("environment") if isinstance(config.get("environment"), dict) else {}
        if str(environment.get("MN_MODEL_ENDPOINTS_JSON") or "").strip():
            return True
        provider = str(environment.get("MN_LLM_PROVIDER") or "").strip().lower()
        api_base = str(environment.get("MN_LLM_API_BASE") or "").strip().lower()
        if provider in {"docker_model_runner", "docker-model-runner", "dmr"} and api_base:
            return True
        if "host.docker.internal:12434" in api_base or "127.0.0.1:12434" in api_base:
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


def _strip_docker_model_runner_model_placement_requirements(target: dict[str, Any]) -> None:
    placement = target.get("placement_requirements")
    if not isinstance(placement, dict):
        return

    models = placement.get("models")
    if isinstance(models, list):
        retained = [model for model in models if not _is_docker_model_runner_model_placement(model)]
        if retained:
            placement["models"] = retained
        else:
            placement.pop("models", None)
    elif _is_docker_model_runner_model_placement(models):
        placement.pop("models", None)

    if not placement:
        target.pop("placement_requirements", None)


def _is_docker_model_runner_model_placement(model: Any) -> bool:
    if not isinstance(model, dict):
        return False
    provider = str(model.get("provider") or "").strip().lower()
    if provider in {"docker_model_runner", "docker-model-runner", "dmr"}:
        return True
    return _is_docker_model_runner_service(model.get("service"))


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
    return sdk_deep_merge(base, override)
