from __future__ import annotations

import hashlib
import json
import os
import re
import tomllib
from pathlib import Path
from typing import Any

from mn_sdk.runtime_modules import default_registered_modules_root


HOST_LOCAL_RUNNER = "MirrorNeuron.Runner.HostLocal"
DOCKER_WORKER_RUNNER = "MirrorNeuron.Runner.DockerWorker"
DEFAULT_CONTEXT_ROOT = "__mn_skill_runtime"
DEFAULT_CONTEXT_PATH = f"{DEFAULT_CONTEXT_ROOT}/docker_worker"
DEFAULT_BASE_IMAGE = "debian:bookworm-slim"
BLUEPRINT_SUPPORT_PACKAGE = "mirrorneuron-blueprint-support-skill"
SDK_PACKAGE = "mirrorneuron-python-sdk"
SDK_SOURCE = "mn-python-sdk"


def prepare_skill_runtime_for_manifest(
    manifest: dict[str, Any],
    config: dict[str, Any] | None,
    *,
    bundle_dir: Path,
    workspace_root: Path,
) -> dict[str, Any] | None:
    """Patch Python workers to a shared DockerWorker when enabled skills need binaries."""

    if not isinstance(config, dict):
        return None

    spec = resolve_skill_runtime_spec(config, bundle_dir=bundle_dir, workspace_root=workspace_root)
    if not spec:
        return None

    target_nodes = _target_node_ids(spec, manifest)
    patched_nodes: list[str] = []
    for node in _manifest_nodes(manifest):
        node_id = str(node.get("node_id") or node.get("id") or "")
        if target_nodes is not None and node_id not in target_nodes:
            continue
        node_config = node.get("config")
        if not isinstance(node_config, dict):
            continue
        if not _auto_patchable_python_node(node_config):
            continue
        _patch_node_config_for_docker_worker(node_config, spec)
        patched_nodes.append(node_id)

    if not patched_nodes:
        return None

    summary = _metadata_from_spec(spec, patched_nodes)
    manifest.setdefault("metadata", {})["mn_skill_runtime"] = summary
    return summary


def resolve_skill_runtime_spec(
    config: dict[str, Any],
    *,
    bundle_dir: Path,
    workspace_root: Path,
) -> dict[str, Any] | None:
    runtime_config = config.get("skill_runtime") if isinstance(config.get("skill_runtime"), dict) else {}
    if runtime_config.get("enabled") is False:
        return None

    skills_root = _skills_root(runtime_config, workspace_root)
    local_skills = _local_skill_index(skills_root)
    enabled_skills = _enabled_skill_entries(config)
    runtime_skills = _runtime_skill_records(enabled_skills, local_skills)
    if not runtime_skills:
        return None

    manual_policy = any(
        str(record["entry"].get("install_policy") or "").strip() == "docker_worker_image"
        for record in runtime_skills
    )
    auto_patch = bool(runtime_config.get("auto_patch", not manual_policy))
    if not auto_patch:
        return None

    base_image = str(runtime_config.get("base_image") or _first_runtime_value(runtime_skills, "base_image") or DEFAULT_BASE_IMAGE)
    context_path = _clean_payload_path(str(runtime_config.get("docker_worker_image") or runtime_config.get("build_context") or DEFAULT_CONTEXT_PATH))
    generated = context_path == DEFAULT_CONTEXT_PATH or bool(runtime_config.get("generate_context", context_path == DEFAULT_CONTEXT_PATH))
    local_packages = _local_python_packages(config, enabled_skills, local_skills, workspace_root=workspace_root)
    package_names = {record["package"] for record in local_packages if record.get("package")}
    requirements_text = _generated_requirements(config, bundle_dir, package_names)
    apt_packages = _system_packages(runtime_skills, "apt")
    verify_commands = _verify_commands(runtime_skills)

    spec = {
        "driver": "docker_worker",
        "install_scope": "shared_job_container",
        "base_image": base_image,
        "context_path": context_path,
        "context_root": context_path.split("/", 1)[0],
        "generated": generated,
        "network": str(runtime_config.get("network") or os.getenv("MN_DOCKER_WORKER_NETWORK") or "").strip(),
        "apt_packages": apt_packages,
        "verify_commands": verify_commands,
        "runtime_skills": [
            {
                "skill": record["skill"],
                "package": record["package"],
                "source": record["source"],
            }
            for record in runtime_skills
        ],
        "local_packages": local_packages,
        "requirements_text": requirements_text,
        "target_node_ids": _configured_node_ids(runtime_config, enabled_skills),
        "target_node_scopes": _configured_node_scopes(runtime_config, enabled_skills),
        "node_scope_definitions": _configured_node_scope_definitions(runtime_config),
    }
    spec["image"] = str(runtime_config.get("image") or _image_ref(config, spec))
    return spec


def stage_skill_runtime_payloads_for_manifest(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    bundle_dir: Path,
) -> dict[str, Any]:
    runtime = _runtime_metadata(manifest)
    if not runtime or not runtime.get("generated"):
        return {"staged": False, "sources": []}

    context_path = _clean_payload_path(str(runtime.get("build_context") or DEFAULT_CONTEXT_PATH))
    dockerfile = _render_dockerfile(runtime)
    requirements = str(runtime.get("requirements_text") or "")
    payloads.setdefault(_payload_join(context_path, "Dockerfile"), dockerfile.encode("utf-8"))
    payloads.setdefault(_payload_join(context_path, "requirements.txt"), requirements.encode("utf-8"))
    return {
        "staged": True,
        "sources": [
            _payload_join(context_path, "Dockerfile"),
            _payload_join(context_path, "requirements.txt"),
        ],
    }


def validate_skill_runtime_requirements(bundle_dir: Path, manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    runtime = _runtime_metadata(manifest)
    if not runtime:
        return errors

    context_path = _clean_payload_path(str(runtime.get("build_context") or DEFAULT_CONTEXT_PATH))
    if not context_path:
        errors.append("mn_skill_runtime.build_context must be a safe relative payload path")

    if runtime.get("generated") is not True:
        dockerfile = bundle_dir / "payloads" / context_path / "Dockerfile"
        if not dockerfile.is_file():
            errors.append(f"mn_skill_runtime Dockerfile not found: payloads/{context_path}/Dockerfile")

    for node in _manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") == DOCKER_WORKER_RUNNER and "python_environment" in config:
            node_id = str(node.get("node_id") or node.get("id") or "unknown")
            errors.append(f"{node_id}: python_environment must be installed in the DockerWorker image, not kept on the node")

    return errors


def _patch_node_config_for_docker_worker(config: dict[str, Any], spec: dict[str, Any]) -> None:
    old_workdir = str(config.get("workdir") or "")
    context_path = str(spec["context_path"])

    config["runner_module"] = DOCKER_WORKER_RUNNER
    config["docker_worker_image"] = context_path
    config["image"] = spec["image"]
    config["shared_container"] = True
    config["reuse_shared_container"] = True
    config.setdefault("cleanup_remote_dir", True)
    if spec.get("network"):
        config["network"] = spec["network"]
    if old_workdir.startswith("/sandbox/job"):
        config["workdir"] = "/mn/job" + old_workdir.removeprefix("/sandbox/job")
    elif not old_workdir:
        upload_path = str(config.get("upload_path") or "").strip("/")
        config["workdir"] = f"/mn/job/{upload_path}" if upload_path else "/mn/job"

    python_environment = config.pop("python_environment", None)
    if python_environment is not None:
        config.setdefault("metadata", {})["python_environment_moved_to_docker_worker"] = python_environment

    _ensure_upload_path(config, spec["context_root"], spec["context_root"])
    for package in spec.get("local_packages", []):
        _ensure_build_context_upload(
            config,
        base=str(package.get("base") or "skills_root"),
        source=str(package["source"]),
        target=_payload_join(context_path, "build_context", str(package["source"])),
    )


def _render_dockerfile(runtime: dict[str, Any]) -> str:
    base_image = str(runtime.get("base_image") or DEFAULT_BASE_IMAGE)
    system_packages = runtime.get("system_packages") if isinstance(runtime.get("system_packages"), dict) else {}
    apt_packages = _sorted_strings(runtime.get("apt_packages") or system_packages.get("apt") or [])
    local_packages = [
        package
        for package in runtime.get("local_packages", [])
        if isinstance(package, dict) and package.get("source")
    ]
    verify_commands = _sorted_strings(runtime.get("verify_commands") or [])

    lines = [
        f"FROM {base_image}",
        "",
        "ENV PIP_ROOT_USER_ACTION=ignore",
    ]

    if apt_packages:
        lines.extend(
            [
                "RUN apt-get update && apt-get install -y --no-install-recommends \\",
                *[f"    {package} \\" for package in apt_packages[:-1]],
                f"    {apt_packages[-1]} \\",
                "    && rm -rf /var/lib/apt/lists/*",
                "",
            ]
        )

    lines.extend(
        [
            "COPY requirements.txt /tmp/mn-skill-runtime/requirements.txt",
        ]
    )

    for package in local_packages:
        source = str(package["source"])
        lines.append(f"COPY build_context/{source} /tmp/mn-local-packages/{source}")

    if local_packages:
        install_targets = " ".join(f"/tmp/mn-local-packages/{package['source']}" for package in local_packages)
        lines.extend(
            [
                f"RUN python3 -m pip install --break-system-packages --no-cache-dir {install_targets}",
            ]
        )

    lines.extend(
        [
            "RUN if [ -s /tmp/mn-skill-runtime/requirements.txt ]; then \\",
            "      python3 -m pip install --break-system-packages --no-cache-dir -r /tmp/mn-skill-runtime/requirements.txt; \\",
            "    fi",
        ]
    )

    for command in verify_commands:
        lines.append(f"RUN {command}")

    lines.extend(["", "WORKDIR /mn/job", "ENTRYPOINT []", ""])
    return "\n".join(lines)


def _metadata_from_spec(spec: dict[str, Any], patched_nodes: list[str]) -> dict[str, Any]:
    return {
        "enabled": True,
        "driver": spec["driver"],
        "install_scope": spec["install_scope"],
        "base_image": spec["base_image"],
        "build_context": spec["context_path"],
        "generated": spec["generated"],
        "image": spec["image"],
        "network": spec.get("network") or "",
        "system_packages": {"apt": spec["apt_packages"]},
        "verify_commands": spec["verify_commands"],
        "runtime_skills": spec["runtime_skills"],
        "local_packages": spec["local_packages"],
        "requirements_text": spec["requirements_text"],
        "patched_nodes": patched_nodes,
    }


def _local_skill_index(skills_root: Path) -> dict[str, dict[str, Any]]:
    by_package: dict[str, dict[str, Any]] = {}
    by_skill: dict[str, dict[str, Any]] = {}
    if not skills_root.is_dir():
        return {"by_package": by_package, "by_skill": by_skill}

    for pyproject in sorted(skills_root.glob("*/pyproject.toml")):
        try:
            data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        project = data.get("project") if isinstance(data.get("project"), dict) else {}
        package = str(project.get("name") or "").strip()
        tool = data.get("tool") if isinstance(data.get("tool"), dict) else {}
        mirrorneuron = tool.get("mirrorneuron") if isinstance(tool.get("mirrorneuron"), dict) else {}
        skill = mirrorneuron.get("skill") if isinstance(mirrorneuron.get("skill"), dict) else {}
        skill_id = str(skill.get("id") or pyproject.parent.name).strip()
        record = {
            "package": package,
            "skill": skill_id,
            "source": pyproject.parent.name,
            "path": pyproject.parent,
            "metadata": skill,
        }
        if package:
            by_package[_normalize_package_name(package)] = record
        if skill_id:
            by_skill[skill_id] = record
    return {"by_package": by_package, "by_skill": by_skill}


def _enabled_skill_entries(config: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for section_name in ("input_skills", "output_skills"):
        section = config.get(section_name) if isinstance(config.get(section_name), dict) else {}
        for name, raw in section.items():
            if not isinstance(raw, dict) or raw.get("enabled", True) is False:
                continue
            entry = dict(raw)
            entry["_name"] = str(name)
            entry["_section"] = section_name
            entries.append(entry)
    return entries


def _runtime_skill_records(
    enabled_skills: list[dict[str, Any]],
    local_skills: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for entry in enabled_skills:
        local = _find_local_skill(entry, local_skills)
        if not local:
            continue
        runtime = local.get("metadata", {}).get("runtime")
        if not isinstance(runtime, dict):
            continue
        if runtime.get("driver") != "docker_worker":
            continue
        records.append(
            {
                "skill": local.get("skill") or entry.get("skill") or entry["_name"],
                "package": local.get("package") or entry.get("package"),
                "source": local.get("source"),
                "entry": entry,
                "runtime": runtime,
                "local": local,
            }
        )
    return records


def _local_python_packages(
    config: dict[str, Any],
    enabled_skills: list[dict[str, Any]],
    local_skills: dict[str, dict[str, dict[str, Any]]],
    *,
    workspace_root: Path,
) -> list[dict[str, str]]:
    packages: list[str] = []
    python_dependencies = config.get("python_dependencies") if isinstance(config.get("python_dependencies"), dict) else {}
    packages.extend(str(package) for package in python_dependencies.get("packages", []) if isinstance(package, str))
    packages.extend(str(entry.get("package")) for entry in enabled_skills if isinstance(entry.get("package"), str))

    seen_sources: set[str] = set()
    records: list[dict[str, str]] = []
    include_sdk = False
    for package in packages:
        if _normalize_package_name(package) == _normalize_package_name(BLUEPRINT_SUPPORT_PACKAGE):
            include_sdk = True
        local = local_skills["by_package"].get(_normalize_package_name(package))
        if not local:
            continue
        source = str(local.get("source") or "")
        if not source or source in seen_sources:
            continue
        seen_sources.add(source)
        records.append(
            {
                "package": str(local.get("package") or package),
                "skill": str(local.get("skill") or source),
                "source": source,
                "base": "skills_root",
            }
        )
    if include_sdk:
        sdk_root = workspace_root / SDK_SOURCE
        if sdk_root.joinpath("pyproject.toml").is_file() and SDK_SOURCE not in seen_sources:
            records.insert(
                0,
                {
                    "package": SDK_PACKAGE,
                    "skill": SDK_SOURCE,
                    "source": SDK_SOURCE,
                    "base": "workspace_root",
                },
            )
    return records


def _generated_requirements(
    config: dict[str, Any],
    bundle_dir: Path,
    local_package_names: set[str],
) -> str:
    lines: list[str] = []
    python_dependencies = config.get("python_dependencies") if isinstance(config.get("python_dependencies"), dict) else {}
    requirements_path = python_dependencies.get("requirements")
    if isinstance(requirements_path, str) and requirements_path.strip():
        candidate = bundle_dir / "payloads" / requirements_path
        try:
            lines.extend(candidate.read_text(encoding="utf-8").splitlines())
        except OSError:
            pass

    for package in python_dependencies.get("packages", []):
        if isinstance(package, str) and package.strip():
            lines.append(package.strip())

    output: list[str] = []
    seen: set[str] = set()
    normalized_local = {_normalize_package_name(name) for name in local_package_names}
    for line in lines:
        package = _requirement_package_name(line)
        if package and package in normalized_local:
            continue
        key = line.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(line)
    return "\n".join(output).strip() + ("\n" if output else "")


def _system_packages(records: list[dict[str, Any]], manager: str) -> list[str]:
    packages: list[str] = []
    for record in records:
        for item in record["runtime"].get("system_packages", []):
            if not isinstance(item, dict) or item.get("manager") != manager:
                continue
            packages.extend(str(package) for package in item.get("packages", []) if isinstance(package, str) and package.strip())
    return _sorted_strings(packages)


def _verify_commands(records: list[dict[str, Any]]) -> list[str]:
    commands: list[str] = []
    for record in records:
        commands.extend(str(command) for command in record["runtime"].get("verify_commands", []) if isinstance(command, str) and command.strip())
    return _sorted_strings(commands)


def _find_local_skill(
    entry: dict[str, Any],
    local_skills: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, Any] | None:
    skill = entry.get("skill")
    if isinstance(skill, str) and skill in local_skills["by_skill"]:
        return local_skills["by_skill"][skill]
    package = entry.get("package")
    if isinstance(package, str):
        return local_skills["by_package"].get(_normalize_package_name(package))
    return None


def _target_node_ids(spec: dict[str, Any], manifest: dict[str, Any]) -> set[str] | None:
    explicit = {str(node_id) for node_id in spec.get("target_node_ids", []) if str(node_id).strip()}
    scope_names = _sorted_strings(spec.get("target_node_scopes", []))
    if not scope_names:
        return explicit or None

    definitions = _node_scope_definitions(spec, manifest)
    scoped: set[str] = set()
    for scope_name in scope_names:
        if scope_name in definitions:
            scoped.update(definitions[scope_name])
            continue
        scoped.update(_builtin_node_scope(scope_name, manifest))
    return explicit | scoped


def _configured_node_ids(runtime_config: dict[str, Any], enabled_skills: list[dict[str, Any]]) -> list[str]:
    values: list[Any] = []
    values.extend(_list_value(runtime_config.get("node_ids")))
    values.extend(_list_value(runtime_config.get("nodes")))
    for entry in enabled_skills:
        values.extend(_list_value(entry.get("node_ids")))
        values.extend(_list_value(entry.get("runtime_nodes")))
        scope = entry.get("scope") if isinstance(entry.get("scope"), dict) else {}
        values.extend(_list_value(scope.get("node_ids")))
    return [str(value) for value in values if str(value).strip()]


def _configured_node_scopes(runtime_config: dict[str, Any], enabled_skills: list[dict[str, Any]]) -> list[str]:
    values: list[Any] = []
    values.extend(_list_value(runtime_config.get("node_scope")))
    raw_scopes = runtime_config.get("node_scopes")
    if not isinstance(raw_scopes, dict):
        values.extend(_list_value(raw_scopes))
    for entry in enabled_skills:
        values.extend(_list_value(entry.get("node_scope")))
        runtime = entry.get("runtime") if isinstance(entry.get("runtime"), dict) else {}
        values.extend(_list_value(runtime.get("node_scope")))
        raw_runtime_scopes = runtime.get("node_scopes")
        if not isinstance(raw_runtime_scopes, dict):
            values.extend(_list_value(raw_runtime_scopes))
        scope = entry.get("scope") if isinstance(entry.get("scope"), dict) else {}
        values.extend(_list_value(scope.get("node_scope")))
    return _sorted_strings(values)


def _configured_node_scope_definitions(runtime_config: dict[str, Any]) -> dict[str, list[str]]:
    raw_scopes = runtime_config.get("node_scopes")
    return _normalise_node_scope_definitions(raw_scopes)


def _node_scope_definitions(spec: dict[str, Any], manifest: dict[str, Any]) -> dict[str, set[str]]:
    definitions: dict[str, set[str]] = {}
    for source in (
        spec.get("node_scope_definitions"),
        (manifest.get("metadata") or {}).get("node_scopes") if isinstance(manifest.get("metadata"), dict) else None,
        (manifest.get("metadata") or {}).get("mn_node_scopes") if isinstance(manifest.get("metadata"), dict) else None,
    ):
        for name, nodes in _normalise_node_scope_definitions(source).items():
            definitions.setdefault(name, set()).update(nodes)
    return definitions


def _normalise_node_scope_definitions(raw_scopes: Any) -> dict[str, list[str]]:
    if not isinstance(raw_scopes, dict):
        return {}
    definitions: dict[str, list[str]] = {}
    for name, raw_value in raw_scopes.items():
        if not str(name).strip():
            continue
        if isinstance(raw_value, dict):
            values = []
            values.extend(_list_value(raw_value.get("node_ids")))
            values.extend(_list_value(raw_value.get("nodes")))
        else:
            values = _list_value(raw_value)
        node_ids = [str(value) for value in values if str(value).strip()]
        if node_ids:
            definitions[str(name)] = node_ids
    return definitions


def _builtin_node_scope(scope_name: str, manifest: dict[str, Any]) -> set[str]:
    if scope_name in {"all_python_nodes", "python_executor_nodes", "vc_python_executor_nodes"}:
        return {
            str(node.get("node_id") or node.get("id") or "")
            for node in _manifest_nodes(manifest)
            if _auto_patchable_python_node(node.get("config") if isinstance(node.get("config"), dict) else {})
        } - {""}
    if scope_name == "all_nodes":
        return {
            str(node.get("node_id") or node.get("id") or "")
            for node in _manifest_nodes(manifest)
        } - {""}
    return set()


def _auto_patchable_python_node(config: dict[str, Any]) -> bool:
    if config.get("runner_module") != HOST_LOCAL_RUNNER:
        return False
    if "python_environment" in config:
        return True
    command = config.get("command")
    if isinstance(command, list) and command:
        return "python" in Path(str(command[0])).name
    if isinstance(command, str):
        return "python" in command.split(maxsplit=1)[0]
    return False


def _ensure_upload_path(config: dict[str, Any], source: str, target: str) -> None:
    upload_paths = config.get("upload_paths")
    if isinstance(upload_paths, list):
        paths = upload_paths
    else:
        paths = []
        upload_path = config.get("upload_path")
        if isinstance(upload_path, str) and upload_path.strip():
            paths.append({"source": upload_path, "target": config.get("upload_as") or upload_path})
        config["upload_paths"] = paths
    if not any(isinstance(item, dict) and item.get("source") == source for item in paths):
        paths.append({"source": source, "target": target})


def _ensure_build_context_upload(config: dict[str, Any], *, base: str = "skills_root", source: str, target: str) -> None:
    uploads = config.setdefault("build_context_upload_paths", [])
    if not isinstance(uploads, list):
        uploads = []
        config["build_context_upload_paths"] = uploads
    if not any(
        isinstance(item, dict)
        and item.get("base", "payloads") == base
        and item.get("source") == source
        and item.get("target") == target
        for item in uploads
    ):
        uploads.append({"base": base, "source": source, "target": target})


def _first_runtime_value(records: list[dict[str, Any]], key: str) -> Any:
    for record in records:
        value = record["runtime"].get(key)
        if value not in (None, ""):
            return value
    return None


def _image_ref(config: dict[str, Any], spec: dict[str, Any]) -> str:
    identity = config.get("identity") if isinstance(config.get("identity"), dict) else {}
    blueprint_id = str(identity.get("blueprint_id") or "blueprint")
    safe_id = re.sub(r"[^a-z0-9_.-]+", "-", blueprint_id.lower()).strip("-") or "blueprint"
    digest_source = {
        "base_image": spec["base_image"],
        "apt_packages": spec["apt_packages"],
        "verify_commands": spec["verify_commands"],
        "local_packages": spec["local_packages"],
        "requirements_text": spec["requirements_text"],
    }
    digest = hashlib.sha256(json.dumps(digest_source, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    return f"mirror-neuron/{safe_id}-skill-runtime:{digest}"


def _skills_root(runtime_config: dict[str, Any], workspace_root: Path) -> Path:
    configured = runtime_config.get("skills_root") or os.getenv("MN_SKILLS_ROOT")
    if configured:
        return Path(str(configured)).expanduser()
    return default_registered_modules_root(workspace_root=workspace_root)


def _runtime_metadata(manifest: dict[str, Any]) -> dict[str, Any]:
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    runtime = metadata.get("mn_skill_runtime") if isinstance(metadata.get("mn_skill_runtime"), dict) else {}
    return runtime


def _manifest_nodes(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    agents = manifest.get("agents") if isinstance(manifest.get("agents"), dict) else {}
    agent_nodes = agents.get("nodes") if isinstance(agents, dict) else None
    if isinstance(agent_nodes, list):
        return [node for node in agent_nodes if isinstance(node, dict)]
    nodes = manifest.get("nodes")
    if isinstance(nodes, list):
        return [node for node in nodes if isinstance(node, dict)]
    flow = manifest.get("flow") if isinstance(manifest.get("flow"), dict) else {}
    flow_nodes = flow.get("nodes") if isinstance(flow, dict) else None
    if isinstance(flow_nodes, list):
        return [node for node in flow_nodes if isinstance(node, dict)]
    return []


def _requirement_package_name(line: str) -> str | None:
    value = line.strip()
    if not value or value.startswith("#") or value.startswith("-"):
        return None
    value = value.split("#", 1)[0].strip()
    name = re.split(r"\s*(?:===|==|~=|!=|<=|>=|<|>|@)\s*", value, maxsplit=1)[0]
    name = name.split("[", 1)[0].strip()
    return _normalize_package_name(name) if name else None


def _normalize_package_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _clean_payload_path(value: str) -> str:
    path = value.strip().strip("/")
    candidate = Path(path)
    if not path or candidate.is_absolute() or ".." in candidate.parts:
        return ""
    return path.replace("\\", "/")


def _payload_join(*parts: str) -> str:
    cleaned = []
    for part in parts:
        value = str(part).strip("/")
        if value and value != ".":
            cleaned.append(value)
    return "/".join(cleaned)


def _list_value(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _sorted_strings(values: Any) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})
