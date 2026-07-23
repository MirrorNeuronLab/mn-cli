from ..common import *
from ..context import *
from ..models import *
from ..model_cluster import _cluster_node_endpoint, _local_runtime_node_name, _prepare_runtime_model_with_retry, _runtime_model_prepare_client
from ..openshell import *
from ..run_state import *
from .validate import *
from mn_cli.runtime_mode import running_core_container
from mn_sdk.runtime_config import RuntimeConfig

def doctor_bundle(
    bundle_path: str,
    *,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    force: bool = False,
    json_output: bool = False,
    timeout: float = 3.0,
    check_only: bool = False,
    no_llm_call: bool = False,
    cleanup: bool = False,
    debug: bool = False,
) -> dict[str, Any]:
    """Prepare and smoke-check a blueprint without submitting a runtime job."""
    report = _doctor_base_report(bundle_path, check_only=check_only, no_llm_call=no_llm_call)
    env_overrides = dict(env_overrides or {})
    config_overrides = dict(config_overrides or {})
    submission_metadata = dict(submission_metadata or {})
    if debug:
        env_overrides.setdefault("MN_DEBUG", "1")

    prepared_submission_id = ""
    try:
        bundle_dir, manifest_file, manifest_dict = _load_bundle_manifest(bundle_path)
        manifest_dict = _configure_bundle_if_required(bundle_dir, manifest_file, manifest_dict)
        blueprint_id = _doctor_blueprint_id(bundle_dir, manifest_dict, config_overrides)
        report["blueprint"].update(
            {
                "id": blueprint_id,
                "bundle": str(bundle_dir),
                "manifest": str(manifest_file),
            }
        )

        report["runtime"] = _doctor_runtime_foundation(timeout)
        _doctor_record_validation(
            report,
            "resources",
            lambda: _doctor_validate_hardware(
                manifest_dict,
                force=force,
                allow_local_fallback=False,
            ),
        )
        placement = _resolve_and_apply_workflow_placement(
            manifest_dict,
            env={**os.environ, **env_overrides},
        )
        if placement:
            selected_node = str(placement["selected_node"])
            env_overrides["MN_SELECTED_RUNTIME_NODE"] = selected_node
            submission_metadata["selected_node"] = selected_node
            submission_metadata["workflow_placement"] = {
                "mode": placement["mode"],
                "selected_node": selected_node,
                "selection": placement["selection"],
            }
            report["placement"] = _doctor_component(
                "placement",
                "passing",
                f"Pinned the complete workflow to {selected_node}.",
                placement=placement,
            )
        else:
            report["placement"] = _doctor_component(
                "placement",
                "skipped",
                "Workflow uses distributed placement or has no node-local runtime requirements.",
            )

        if not force:
            _doctor_record_validation(
                report,
                "services",
                lambda: _doctor_validate_services(
                    bundle_dir,
                    manifest_dict,
                    env_overrides=env_overrides,
                    config_overrides=config_overrides,
                ),
            )
            _doctor_record_validation(
                report,
                "inputs",
                lambda: _doctor_validate_inputs(
                    bundle_dir,
                    manifest_dict,
                    env_overrides=env_overrides,
                    config_overrides=config_overrides,
                ),
            )
        else:
            report["inputs"] = _doctor_component("inputs", "skipped", "Skipped because --force was provided.")
            report["services"] = _doctor_component("services", "skipped", "Skipped because --force was provided.")

        model_install_summary: dict[str, Any] | None = None
        if check_only:
            _doctor_record_validation(
                report,
                "models",
                lambda: _doctor_validate_models(
                    bundle_dir,
                    manifest_dict,
                    env_overrides=env_overrides,
                    config_overrides=config_overrides,
                ),
            )
        else:
            model_install_summary = _prepare_runtime_models_for_run_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
                force=force,
                quiet=json_output,
            )
            _merge_runtime_model_config_overrides(config_overrides, model_install_summary)
            _doctor_record_validation(
                report,
                "models",
                lambda: _doctor_validate_models(
                    bundle_dir,
                    manifest_dict,
                    env_overrides=env_overrides,
                    config_overrides=config_overrides,
                    model_install_summary=model_install_summary,
                ),
            )

        manifest_dict = prepare_manifest_for_submission(
            bundle_dir,
            manifest_dict,
            env_overrides=env_overrides,
            submission_metadata=submission_metadata,
            config_overrides=config_overrides,
        )
        if not check_only:
            _prepare_openshell_custom_images(bundle_dir, manifest_dict)
        host_env_report = _doctor_prepare_hostlocal_python_envs(
            bundle_dir,
            manifest_dict,
            timeout=timeout,
            check_only=check_only,
        )
        payloads = _stage_bundle_payloads(bundle_dir, manifest_dict)

        context_report = _doctor_component("context_memory", "skipped", "Blueprint does not require context memory.")
        if blueprint_requires_context_engine(manifest_dict, load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}):
            if check_only:
                context_report = _doctor_component("context_memory", "warning", "Context memory is required but was not started in --check-only mode.")
            else:
                _ensure_context_engine_for_run_if_needed(
                    bundle_dir,
                    manifest_dict,
                    env_overrides=env_overrides,
                    config_overrides=config_overrides,
                    force=force,
                )
                context_report = _doctor_component("context_memory", "passing", "Context engine is ready.")

        docker_report = _doctor_component("docker_worker", "skipped", "No DockerWorker nodes declared.")
        if _doctor_has_runner(manifest_dict, "MirrorNeuron.Runner.DockerWorker"):
            if check_only:
                docker_report = _doctor_component("docker_worker", "warning", "DockerWorker nodes declared but not prepared in --check-only mode.")
            else:
                prepared = prepare_job_submission(
                    manifest_dict,
                    payloads,
                    bundle_dir=bundle_dir,
                    run_id=env_overrides.get("MN_RUN_ID"),
                    submission_id=_doctor_submission_id(blueprint_id, manifest_dict),
                    cluster_client=client,
                    env={**os.environ, **env_overrides},
                )
                prepared_submission_id = str(prepared.metadata.get("submission_id") or "")
                prepared_manifest = json.loads(prepared.manifest_json)
                docker_summary = (
                    prepared_manifest.get("metadata", {}).get("mn_docker_workers")
                    if isinstance(prepared_manifest.get("metadata"), dict)
                    else None
                )
                docker_report = _doctor_docker_worker_report(docker_summary)
        elif not check_only:
            prepared = prepare_job_submission(
                manifest_dict,
                payloads,
                bundle_dir=bundle_dir,
                run_id=env_overrides.get("MN_RUN_ID"),
                submission_id=_doctor_submission_id(blueprint_id, manifest_dict),
                cluster_client=client,
                env={**os.environ, **env_overrides},
            )
            prepared_submission_id = str(prepared.metadata.get("submission_id") or "")

        report["environments"] = {
            "docker_worker": docker_report,
            "openshell": _doctor_openshell_report(manifest_dict, prepared=not check_only),
            "host_local_python": host_env_report,
        }
        report["context_memory"] = context_report
        report["skills"] = _doctor_skill_report(bundle_dir, config_overrides, manifest_dict)
        report["llm_smoke"] = (
            _doctor_component("llm_smoke", "skipped", "Disabled by --no-llm-call.")
            if no_llm_call
            else _doctor_llm_smoke_report(bundle_dir, config_overrides, env_overrides, timeout=timeout)
        )
        if cleanup and prepared_submission_id:
            report["cleanup"] = _doctor_cleanup(prepared_submission_id, timeout=timeout)
        elif cleanup:
            report["cleanup"] = _doctor_component("cleanup", "skipped", "No doctor submission resources were created.")
    except typer.Exit:
        if not report.get("summary", {}).get("status"):
            report["summary"] = _doctor_summary(report)
        _doctor_print_report(report, json_output=json_output)
        raise
    except Exception as exc:
        logger.exception("Blueprint doctor failed")
        report.setdefault("errors", []).append(str(exc))
        report["summary"] = _doctor_summary(report, extra_status="critical")
        if "preparation" not in report:
            report["preparation"] = _doctor_component("preparation", "critical", str(exc))
        _doctor_print_report(report, json_output=json_output)
        raise typer.Exit(1) from exc

    report["summary"] = _doctor_summary(report)
    _doctor_print_report(report, json_output=json_output)
    if report["summary"]["status"] == "critical":
        raise typer.Exit(1)
    return report

def _doctor_base_report(bundle_path: str, *, check_only: bool, no_llm_call: bool) -> dict[str, Any]:
    return {
        "doctor_version": 1,
        "blueprint": {"bundle": str(bundle_path)},
        "options": {
            "check_only": check_only,
            "llm_call": not no_llm_call,
        },
        "errors": [],
    }

def _doctor_blueprint_id(
    bundle_dir: Path,
    manifest: dict[str, Any],
    config_overrides: dict[str, Any],
) -> str:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    identity = config.get("identity") if isinstance(config.get("identity"), dict) else {}
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    workflow = manifest.get("workflow") if isinstance(manifest.get("workflow"), dict) else {}
    return str(
        identity.get("blueprint_id")
        or metadata.get("blueprint_id")
        or manifest.get("blueprint_id")
        or manifest.get("id")
        or workflow.get("workflow_id")
        or bundle_dir.name
    )

def _doctor_submission_id(blueprint_id: str, manifest: dict[str, Any]) -> str:
    digest = hashlib.sha256(json.dumps(manifest, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    return f"doctor-{_safe_doctor_name(blueprint_id)}-{digest}"

def _safe_doctor_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", str(value or "blueprint")).strip("-")[:48] or "blueprint"

def _doctor_runtime_foundation(timeout: float) -> dict[str, Any]:
    try:
        from mn_cli.libs.runtime_health import collect_runtime_doctor

        report = collect_runtime_doctor(timeout)
        status = str(report.get("overall") or "unknown")
        return {
            "status": "passing" if status == "passing" else status,
            "detail": f"Runtime doctor overall: {status}",
            "report": report,
        }
    except Exception as exc:
        return _doctor_component("runtime", "critical", str(exc))

def _doctor_validate_hardware(
    manifest: dict[str, Any],
    *,
    force: bool = False,
    allow_local_fallback: bool = False,
) -> dict[str, Any]:
    return run_hardware_requirements_validation(
        manifest,
        resource_report=lambda: _runtime_resource_report(allow_local_fallback=allow_local_fallback),
        force=force,
    )

def _doctor_validate_services(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )

    def resolver(name: str, requirement: dict[str, Any]) -> list[dict[str, Any]]:
        response = client.resolve_service(
            name,
            tags=requirement.get("tags") or [],
            passing_only=True,
        )
        decoded = json.loads(response)
        services = decoded.get("services") if isinstance(decoded, dict) else []
        return services if isinstance(services, list) else []

    return run_service_validation(
        bundle_dir,
        manifest,
        config=config,
        env=env,
        resolver=resolver,
    )

def _doctor_validate_inputs(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    return run_input_validation(bundle_dir, manifest, config=config, env=env)

def _doctor_validate_models(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    model_install_summary: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    if env_overrides and str(env_overrides.get("MN_BLUEPRINT_CONFIG_JSON") or "").strip():
        try:
            decoded_config = json.loads(str(env_overrides["MN_BLUEPRINT_CONFIG_JSON"]))
        except json.JSONDecodeError:
            decoded_config = None
        if isinstance(decoded_config, dict):
            config = decoded_config
    validation_manifest = _manifest_for_model_validation(manifest, config)
    validation_config = config
    if model_install_summary:
        validation_manifest, validation_config = _model_validation_inputs_with_prepared_models(
            validation_manifest,
            config,
            model_install_summary,
        )
    return run_model_validation(
        bundle_dir,
        validation_manifest,
        config=validation_config,
        env=env,
        installed_resolver=_prepared_model_installed_resolver(model_install_summary),
    )

def _doctor_record_validation(
    report: dict[str, Any],
    key: str,
    func: Any,
) -> None:
    try:
        result = func()
        report[key] = {
            "status": "passing" if result.get("ok", True) else "critical",
            "report": result,
        }
    except typer.Exit as exc:
        report[key] = _doctor_component(
            key,
            "critical",
            f"Validation failed with exit code {exc.exit_code}.",
        )
    except Exception as exc:
        report[key] = _doctor_component(key, "critical", str(exc))

def _doctor_component(name: str, status: str, detail: str = "", **extra: Any) -> dict[str, Any]:
    result = {"name": name, "status": status}
    if detail:
        result["detail"] = detail
    result.update(extra)
    return result

def _doctor_has_runner(manifest: dict[str, Any], runner_module: str) -> bool:
    return any(
        isinstance(node.get("config"), dict)
        and node["config"].get("runner_module") == runner_module
        for node in manifest_nodes(manifest)
    )

def _doctor_docker_worker_report(summary: Any) -> dict[str, Any]:
    if not isinstance(summary, dict) or not summary.get("prepared"):
        return _doctor_component("docker_worker", "critical", "DockerWorker nodes were not prepared.")
    services = summary.get("services") if isinstance(summary.get("services"), list) else []
    return _doctor_component(
        "docker_worker",
        "passing",
        f"Prepared {len(services)} DockerWorker service(s).",
        services=[
            {
                "service": service.get("service"),
                "container_name": service.get("container_name"),
                "image": service.get("image"),
                "built_by_sdk": service.get("built_by_sdk"),
            }
            for service in services
            if isinstance(service, dict)
        ],
    )

def _doctor_openshell_report(manifest: dict[str, Any], *, prepared: bool = True) -> dict[str, Any]:
    nodes = [
        node
        for node in manifest_nodes(manifest)
        if isinstance(node.get("config"), dict)
        and node["config"].get("runner_module") == "MirrorNeuron.Sandbox.OpenShell"
    ]
    if not nodes:
        return _doctor_component("openshell", "skipped", "No OpenShell nodes declared.")
    if not prepared:
        return _doctor_component(
            "openshell",
            "warning",
            "OpenShell nodes declared but not prepared in --check-only mode.",
            nodes=[str(node.get("node_id") or node.get("id") or "openshell") for node in nodes],
        )
    return _doctor_component(
        "openshell",
        "passing",
        f"Prepared {len(nodes)} OpenShell node(s).",
        nodes=[str(node.get("node_id") or node.get("id") or "openshell") for node in nodes],
    )

def _doctor_prepare_hostlocal_python_envs(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    timeout: float,
    check_only: bool,
) -> dict[str, Any]:
    prepared: list[dict[str, Any]] = []
    skipped = 0
    failures: list[dict[str, Any]] = []
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    placement = metadata.get("mn_workflow_placement") if isinstance(metadata.get("mn_workflow_placement"), dict) else {}
    selected_node = str(placement.get("selected_node") or "").strip()
    remote_selected_node = selected_node and selected_node != _local_runtime_node_name()
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") != "MirrorNeuron.Runner.HostLocal":
            continue
        python_env = config.get("python_environment") if isinstance(config.get("python_environment"), dict) else {}
        packages = [
            str(package).strip()
            for package in python_env.get("packages", [])
            if isinstance(package, str) and package.strip()
        ]
        requirements_path = str(python_env.get("requirements") or "").strip()
        if not packages and not requirements_path:
            skipped += 1
            continue
        node_id = str(node.get("node_id") or node.get("id") or "host_local")
        if check_only:
            failures.append({"node_id": node_id, "status": "skipped", "detail": "Not prepared in --check-only mode."})
            continue
        try:
            if remote_selected_node:
                requirements_content = _doctor_requirements_content(
                    bundle_dir,
                    node_id=node_id,
                    requirements_path=requirements_path,
                )
                endpoint = _cluster_node_endpoint(selected_node)
                runtime_client = _runtime_model_prepare_client(selected_node, endpoint)
                remote_result = _prepare_runtime_model_with_retry(
                    runtime_client,
                    {
                        "node": selected_node,
                        "ensure_hostlocal_python_environment": True,
                        "blueprint_id": _doctor_blueprint_id(bundle_dir, manifest, {}),
                        "node_id": node_id,
                        "packages": packages,
                        "requirements_content": requirements_content,
                        "timeout": timeout,
                        "source": "mn-cli-workflow-placement",
                    },
                )
                runtime_env_dir = Path(str(remote_result["runtime_path"]))
                env_dir = Path(str(remote_result.get("host_path") or runtime_env_dir))
            else:
                env_dir = _doctor_prepare_python_env(
                    bundle_dir,
                    blueprint_id=_doctor_blueprint_id(bundle_dir, manifest, {}),
                    node_id=node_id,
                    packages=packages,
                    requirements_path=requirements_path,
                    timeout=timeout,
                )
                runtime_env_dir = _doctor_runtime_python_env_path(env_dir)
            python_env["path"] = str(runtime_env_dir)
            config["python_environment"] = python_env
            prepared.append(
                {
                    "node_id": node_id,
                    "path": str(runtime_env_dir),
                    "host_path": str(env_dir),
                }
            )
        except Exception as exc:
            failures.append({"node_id": node_id, "status": "critical", "detail": str(exc)})

    if failures:
        hard_failures = [item for item in failures if item.get("status") == "critical"]
        return _doctor_component(
            "host_local_python",
            "critical" if hard_failures else "warning",
            "Some HostLocal Python environments were not prepared.",
            prepared=prepared,
            failures=failures,
            skipped=skipped,
        )
    if prepared:
        return _doctor_component("host_local_python", "passing", f"Prepared {len(prepared)} Python environment(s).", prepared=prepared)
    return _doctor_component("host_local_python", "skipped", "No HostLocal Python environments declared.", skipped=skipped)

def _doctor_prepare_python_env(
    bundle_dir: Path,
    *,
    blueprint_id: str,
    node_id: str,
    packages: list[str],
    requirements_path: str,
    timeout: float,
) -> Path:
    requirements_content = _doctor_requirements_content(
        bundle_dir,
        node_id=node_id,
        requirements_path=requirements_path,
    )
    return _doctor_prepare_python_env_from_content(
        blueprint_id=blueprint_id,
        node_id=node_id,
        packages=packages,
        requirements_content=requirements_content,
        timeout=timeout,
    )


def _doctor_requirements_content(
    bundle_dir: Path,
    *,
    node_id: str,
    requirements_path: str,
) -> str:
    if not requirements_path:
        return ""
    if not _is_safe_payload_relative_path(requirements_path):
        raise RuntimeError(f"{node_id}: python_environment.requirements must be relative inside payloads/")
    return (bundle_dir / "payloads" / requirements_path).read_text(encoding="utf-8")


def _doctor_prepare_python_env_from_content(
    *,
    blueprint_id: str,
    node_id: str,
    packages: list[str],
    requirements_content: str,
    timeout: float,
) -> Path:

    core_container = _doctor_running_core_container(timeout)
    runtime_python = ["docker", "exec", core_container, "python3"] if core_container else [sys.executable]
    version = subprocess.run(
        [*runtime_python, "--version"],
        capture_output=True,
        text=True,
        timeout=max(timeout, 1.0),
    )
    if version.returncode != 0:
        raise RuntimeError((version.stdout + version.stderr).strip() or "python --version failed")
    digest = hashlib.sha256(
        json.dumps(
            {
                "blueprint_id": blueprint_id,
                "python": version.stdout.strip() or version.stderr.strip(),
                "runtime": f"docker:{core_container}" if core_container else "native",
                "requirements": requirements_content,
                "packages": packages,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    runtime_config = RuntimeConfig.from_env()
    env_root = Path(
        os.getenv(
            "MN_BLUEPRINT_PYTHON_ENVS_DIR",
            str(Path(runtime_config.shared_storage_root) / "blueprint-python-envs"),
        )
    ).expanduser()
    env_dir = env_root / digest
    runtime_env_dir = _doctor_runtime_python_env_path(env_dir)
    if core_container and not _doctor_path_is_in_host_shared_storage(env_dir):
        raise RuntimeError(
            f"{node_id}: Docker Core HostLocal Python environments must be under "
            f"{RuntimeConfig.from_env().shared_storage_root}"
        )
    ready = env_dir / ".ready"
    if ready.is_file() and (env_dir / "bin" / "python").is_file():
        return env_dir

    if env_dir.exists():
        shutil.rmtree(env_dir)
    env_dir.mkdir(parents=True, exist_ok=True)
    install_requirement_file: Path | None = None
    if requirements_content:
        staged_requirements = env_dir / ".mn-requirements.txt"
        staged_requirements.write_text(requirements_content, encoding="utf-8")
        install_requirement_file = (runtime_env_dir if core_container else env_dir) / staged_requirements.name
    create_target = runtime_env_dir if core_container else env_dir
    create_args = [*runtime_python, "-m", "venv"]
    if not core_container:
        create_args.append("--copies")
    create_args.append(str(create_target))
    create = subprocess.run(
        create_args,
        capture_output=True,
        text=True,
        timeout=max(timeout, 1.0),
    )
    if create.returncode != 0:
        raise RuntimeError((create.stdout + create.stderr).strip() or "venv creation failed")
    python_executable = (runtime_env_dir if core_container else env_dir) / "bin" / "python"
    pip_args = [str(python_executable), "-m", "pip", "install"]
    if install_requirement_file is not None:
        pip_args.extend(["-r", str(install_requirement_file)])
    pip_args.extend(packages)
    install_args = (
        [
            "docker",
            "exec",
            "-e",
            "PIP_DISABLE_PIP_VERSION_CHECK=1",
            "-e",
            "PIP_NO_INPUT=1",
            core_container,
            *pip_args,
        ]
        if core_container
        else pip_args
    )
    install = subprocess.run(
        install_args,
        capture_output=True,
        text=True,
        timeout=max(timeout, 1.0) * 60,
        env={**os.environ, "PIP_DISABLE_PIP_VERSION_CHECK": "1", "PIP_NO_INPUT": "1"},
    )
    if install.returncode != 0:
        raise RuntimeError(_truncate_doctor_detail((install.stdout + install.stderr).strip() or "pip install failed"))
    ready.write_text(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + "\n", encoding="utf-8")
    return env_dir


def _doctor_running_core_container(timeout: float) -> str:
    return running_core_container(timeout_seconds=max(timeout, 1.0)) or ""


def _doctor_path_is_in_host_shared_storage(env_dir: Path) -> bool:
    host_root = Path(RuntimeConfig.from_env().shared_storage_root).expanduser().resolve()
    try:
        env_dir.expanduser().resolve().relative_to(host_root)
    except ValueError:
        return False
    return True


def _doctor_runtime_python_env_path(env_dir: Path) -> Path:
    runtime_config = RuntimeConfig.from_env()
    host_root = Path(runtime_config.shared_storage_root).expanduser().resolve()
    try:
        relative = env_dir.expanduser().resolve().relative_to(host_root)
    except ValueError:
        return env_dir
    return Path(runtime_config.runtime_shared_storage_root) / relative

def _doctor_skill_report(
    bundle_dir: Path,
    config_overrides: dict[str, Any],
    manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    entries: list[dict[str, Any]] = []
    for section_name in ("input_skills", "output_skills"):
        section = config.get(section_name) if isinstance(config.get(section_name), dict) else {}
        for name, raw in section.items():
            if not isinstance(raw, dict):
                continue
            entries.append(_doctor_skill_config_entry(section_name, str(name), raw))
    python_dependencies = config.get("python_dependencies") if isinstance(config.get("python_dependencies"), dict) else {}
    for package in python_dependencies.get("packages") or []:
        if isinstance(package, str) and package.strip():
            entries.append({"name": package.strip(), "section": "python_dependencies", "status": "passing", "detail": "declared for runtime install"})
    try:
        for record in skill_dependency_records(manifest):
            entries.append(
                {
                    "name": record["name"],
                    "section": "skill_dependencies",
                    "status": "passing",
                    "source": record["source"],
                    "type": record["type"],
                    "version": record["version"],
                    "detail": "declared GAR skill dependency",
                }
            )
    except Exception as exc:
        entries.append(
            {
                "name": "skill_dependencies",
                "section": "skill_dependencies",
                "status": "critical",
                "detail": str(exc),
            }
        )

    if not entries:
        return _doctor_component("skills", "skipped", "No blueprint skill dependencies declared.", entries=[])
    if any(entry.get("status") == "critical" for entry in entries):
        status = "critical"
    elif any(entry.get("status") == "warning" for entry in entries):
        status = "warning"
    else:
        status = "passing"
    return _doctor_component("skills", status, f"Found {len(entries)} declared skill/dependency item(s).", entries=entries)

def _doctor_skill_config_entry(section_name: str, name: str, raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("enabled") is False:
        return {
            "name": name,
            "section": section_name,
            "status": "skipped",
            "skill": raw.get("skill"),
            "package": raw.get("package"),
            "import": raw.get("import"),
            "detail": "disabled",
        }
    checks: list[dict[str, Any]] = []
    import_name = str(raw.get("import") or raw.get("module") or "").strip()
    if import_name:
        found = importlib.util.find_spec(import_name) is not None
        checks.append(
            {
                "name": "import",
                "target": import_name,
                "status": "passing" if found else "warning",
                "detail": "import is available" if found else "import is not available in the current CLI Python environment",
            }
        )
    executable = str(raw.get("executable") or raw.get("command") or "").strip()
    if executable:
        found_path = shutil.which(executable)
        checks.append(
            {
                "name": "executable",
                "target": executable,
                "status": "passing" if found_path else "warning",
                "detail": found_path or "executable is not available on PATH",
            }
        )
    status = "warning" if any(check.get("status") == "warning" for check in checks) else "passing"
    return {
        "name": name,
        "section": section_name,
        "status": status,
        "skill": raw.get("skill"),
        "package": raw.get("package"),
        "import": import_name or None,
        "executable": executable or None,
        "detail": "declared" if not checks else f"ran {len(checks)} smoke check(s)",
        "checks": checks,
    }

def _doctor_llm_smoke_report(
    bundle_dir: Path,
    config_overrides: dict[str, Any],
    env_overrides: dict[str, str],
    *,
    timeout: float,
) -> dict[str, Any]:
    config = _doctor_effective_config(bundle_dir, config_overrides, env_overrides)
    checks: list[dict[str, Any]] = []
    llm = config.get("llm") if isinstance(config.get("llm"), dict) else {}
    configs = llm.get("configs") if isinstance(llm.get("configs"), dict) else {}
    if configs:
        for name, entry in configs.items():
            if isinstance(entry, dict):
                checks.append(_doctor_chat_smoke(str(name), entry, llm, timeout=timeout))
    elif llm:
        checks.append(_doctor_chat_smoke(str(llm.get("default_config") or "primary"), llm, {}, timeout=timeout))

    rag = config.get("knowledge_rag") if isinstance(config.get("knowledge_rag"), dict) else {}
    if rag.get("enabled") is not False and str(rag.get("embedding_provider") or "").strip().lower() in {"docker_model_runner", "docker-model-runner", "dmr"}:
        checks.append(_doctor_embedding_smoke(rag, timeout=timeout))

    if not checks:
        return _doctor_component("llm_smoke", "skipped", "No live LLM or embedding checks declared.", checks=[])
    status = "critical" if any(check.get("status") == "critical" for check in checks) else "passing"
    return _doctor_component("llm_smoke", status, f"Ran {len(checks)} LLM smoke check(s).", checks=checks)

def _doctor_effective_config(
    bundle_dir: Path,
    config_overrides: dict[str, Any],
    env_overrides: dict[str, str],
) -> dict[str, Any]:
    raw = str(env_overrides.get("MN_BLUEPRINT_CONFIG_JSON") or "").strip()
    if raw:
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, dict):
                return decoded
        except json.JSONDecodeError:
            pass
    return load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}

def _doctor_chat_smoke(name: str, entry: dict[str, Any], llm: dict[str, Any], *, timeout: float) -> dict[str, Any]:
    provider = str(entry.get("provider") or llm.get("provider") or "docker_model_runner").strip().lower()
    if provider in {"fake", "mock"} or str(entry.get("mode") or llm.get("mode") or "").strip().lower() in {"fake", "mock"}:
        return {"name": name, "status": "skipped", "detail": "fake LLM config"}
    model = str(entry.get("model") or entry.get("runtime_model") or llm.get("model") or llm.get("runtime_model") or "").strip()
    api_base = _doctor_host_api_base(str(entry.get("api_base") or llm.get("api_base") or "").strip())
    if not model or not api_base or api_base == "auto":
        return {"name": name, "status": "warning", "detail": "model or api_base is not resolved"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "Reply with the word ok."}],
        "max_tokens": 8,
        "temperature": 0,
    }
    return _doctor_post_openai_payload(name, api_base.rstrip("/") + "/chat/completions", payload, timeout=timeout)

def _doctor_embedding_smoke(rag: dict[str, Any], *, timeout: float) -> dict[str, Any]:
    model = str(rag.get("embedding_model") or "").strip()
    api_base = _doctor_host_api_base(str(rag.get("embedding_api_base") or "").strip())
    if not model or not api_base:
        return {"name": "knowledge_rag_embedding", "status": "warning", "detail": "embedding model or api_base is not resolved"}
    payload = {"model": model, "input": "ok"}
    return _doctor_post_openai_payload("knowledge_rag_embedding", api_base.rstrip("/") + "/embeddings", payload, timeout=timeout)

def _doctor_host_api_base(api_base: str) -> str:
    if not api_base:
        return api_base
    parsed = urllib.parse.urlparse(api_base)
    host = parsed.hostname or ""
    if host == "mn-litellm-proxy":
        return "http://127.0.0.1:4000/v1"
    if host == "host.docker.internal":
        port = parsed.port or (12434 if "12434" in api_base else None)
        netloc = "127.0.0.1" + (f":{port}" if port else "")
        return urllib.parse.urlunparse(parsed._replace(netloc=netloc))
    return api_base

def _doctor_post_openai_payload(name: str, url: str, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]:
    try:
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=max(timeout, 1.0)) as response:
            decoded = json.loads(response.read().decode("utf-8") or "{}")
        ok = bool(decoded.get("choices") or decoded.get("data"))
        return {
            "name": name,
            "status": "passing" if ok else "warning",
            "url": url,
            "detail": "smoke request completed" if ok else "smoke request returned an unexpected response shape",
        }
    except Exception as exc:
        return {"name": name, "status": "critical", "url": url, "detail": _truncate_doctor_detail(str(exc))}

def _doctor_cleanup(submission_id: str, *, timeout: float) -> dict[str, Any]:
    try:
        result = cleanup_docker_worker_services(submission_id=submission_id, timeout=max(timeout, 1.0))
        return _doctor_component("cleanup", "passing", f"Removed {result.get('removed', 0)} DockerWorker ledger entrie(s).", report=result)
    except Exception as exc:
        return _doctor_component("cleanup", "warning", str(exc))

def _doctor_summary(report: dict[str, Any], *, extra_status: str | None = None) -> dict[str, Any]:
    statuses = _doctor_statuses(report)
    if extra_status:
        statuses.append(extra_status)
    if "critical" in statuses:
        status = "critical"
    elif "warning" in statuses:
        status = "warning"
    else:
        status = "passing"
    return {
        "status": status,
        "passing": statuses.count("passing"),
        "warning": statuses.count("warning"),
        "critical": statuses.count("critical"),
        "skipped": statuses.count("skipped"),
    }

def _doctor_statuses(value: Any) -> list[str]:
    statuses: list[str] = []
    if isinstance(value, dict):
        status = value.get("status")
        if isinstance(status, str) and status in {"passing", "warning", "critical", "skipped"}:
            statuses.append(status)
        for key, child in value.items():
            if key == "summary":
                continue
            statuses.extend(_doctor_statuses(child))
    elif isinstance(value, list):
        for child in value:
            statuses.extend(_doctor_statuses(child))
    return statuses

def _doctor_print_report(report: dict[str, Any], *, json_output: bool) -> None:
    redacted = _doctor_redact(report)
    if json_output:
        console.print_json(data=redacted)
        return

    summary = redacted.get("summary", {})
    table = Table(title=f"Blueprint doctor: {summary.get('status', 'unknown')}", show_header=True, header_style="bold")
    table.add_column("Section")
    table.add_column("Status")
    table.add_column("Detail")
    for section in ("runtime", "resources", "services", "inputs", "models", "context_memory", "skills", "llm_smoke", "cleanup"):
        item = redacted.get(section)
        if isinstance(item, dict):
            table.add_row(section, str(item.get("status") or "-"), str(item.get("detail") or ""))
    envs = redacted.get("environments") if isinstance(redacted.get("environments"), dict) else {}
    for name, item in envs.items():
        if isinstance(item, dict):
            table.add_row(f"environment.{name}", str(item.get("status") or "-"), str(item.get("detail") or ""))
    console.print(table)

def _doctor_redact(value: Any) -> Any:
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            key_text = str(key).lower()
            if any(secret in key_text for secret in ("api_key", "token", "authorization", "cookie", "password", "secret")):
                result[key] = "[redacted]"
            else:
                result[key] = _doctor_redact(child)
        return result
    if isinstance(value, list):
        return [_doctor_redact(item) for item in value]
    return value

def _truncate_doctor_detail(value: str, limit: int = 600) -> str:
    text = str(value or "").strip()
    return text if len(text) <= limit else text[:limit].rstrip() + "..."


__all__ = [name for name in globals() if not name.startswith("__")]
