from ..common import *
from ..context import *
from ..events import *
from ..models import *
from ..openshell import *
from ..outputs import *
from ..run_state import *
from ..web_ui import *
from ..web_ui import (
    _console_web_ui_url,
    _start_background_event_relay_if_needed,
)
from .validate import *
from .doctor import _doctor_prepare_hostlocal_python_envs


def _record_prevalidated_command_rules(
    manifest: dict[str, Any],
    validation_report: dict[str, Any],
) -> None:
    """Remove host-executed validators before handing the manifest to Core.

    Command validators run at the trusted CLI/API boundary because Core does not
    execute arbitrary validation commands. Pattern validators remain in the
    manifest so Core can independently re-check them at admission time.
    """

    if validation_report.get("ok") is not True:
        return

    passed_commands: dict[int, dict[str, Any]] = {}
    for result in validation_report.get("results") or []:
        if (
            not isinstance(result, dict)
            or result.get("type") != "command"
            or result.get("ok") is not True
        ):
            continue
        rule_ref = result.get("rule") if isinstance(result.get("rule"), dict) else {}
        index = rule_ref.get("index")
        if isinstance(index, int):
            passed_commands[index] = {
                key: rule_ref[key]
                for key in ("name", "id", "type", "index")
                if key in rule_ref
            }

    if not passed_commands:
        return

    metadata = (
        manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    )
    validation_candidates = (
        (manifest, "input_validation"),
        (manifest, "inputValidation"),
        (metadata, "input_validation"),
        (metadata, "inputValidation"),
    )
    for container, key in validation_candidates:
        raw_validation = container.get(key)
        if isinstance(raw_validation, list):
            rules = raw_validation
            container[key] = [
                rule
                for index, rule in enumerate(rules)
                if not (
                    index in passed_commands
                    and isinstance(rule, dict)
                    and rule.get("type") == "command"
                )
            ]
            break
        if isinstance(raw_validation, dict) and isinstance(
            raw_validation.get("rules"), list
        ):
            rules = raw_validation["rules"]
            raw_validation["rules"] = [
                rule
                for index, rule in enumerate(rules)
                if not (
                    index in passed_commands
                    and isinstance(rule, dict)
                    and rule.get("type") == "command"
                )
            ]
            break
    else:
        return

    manifest_metadata = manifest.setdefault("metadata", {})
    if not isinstance(manifest_metadata, dict):
        manifest_metadata = {}
        manifest["metadata"] = manifest_metadata
    validation_metadata = manifest_metadata.setdefault("mn_validation", {})
    if not isinstance(validation_metadata, dict):
        validation_metadata = {}
        manifest_metadata["mn_validation"] = validation_metadata
    validation_metadata["input_validation"] = {
        "status": "passed",
        "validator": "mn-python-sdk",
        "prevalidated_command_rules": [
            passed_commands[index] for index in sorted(passed_commands)
        ],
    }


def _docker_worker_node_ids(manifest: dict[str, Any]) -> list[str]:
    node_ids: list[str] = []
    for index, node in enumerate(manifest_nodes(manifest)):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        runner = str(
            config.get("runner_module") or node.get("runner_module") or ""
        ).strip()
        if runner == "MirrorNeuron.Runner.DockerWorker":
            node_ids.append(
                str(node.get("node_id") or node.get("id") or f"worker-{index}")
            )
    return node_ids


def _print_docker_worker_ready(
    manifest: dict[str, Any], *, debug: bool = False
) -> None:
    metadata = (
        manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    )
    summary = (
        metadata.get("mn_docker_workers")
        if isinstance(metadata.get("mn_docker_workers"), dict)
        else {}
    )
    services = (
        summary.get("services") if isinstance(summary.get("services"), list) else []
    )
    labels = [
        f"{service.get('node_id', 'DockerWorker')} on {service.get('node', 'the selected node')}"
        for service in services
        if isinstance(service, dict)
    ]
    if labels:
        console.print(f"[green]DockerWorker ready:[/green] {', '.join(labels)}")
    if not debug:
        return
    for service in services:
        if not isinstance(service, dict):
            continue
        build = service.get("build") if isinstance(service.get("build"), dict) else {}
        if not build:
            continue
        node = str(service.get("node") or "the selected runtime node")
        image = str(build.get("image") or service.get("image") or "")
        action = str(build.get("action") or "unknown")
        console.print(
            f"Docker build on {node}: action={action}, image={image}",
            markup=False,
        )
        command = build.get("command")
        if isinstance(command, list) and command:
            console.print(
                "Docker build command: " + " ".join(str(part) for part in command),
                markup=False,
            )
        output = str(build.get("output") or "").strip()
        if output:
            console.print("Docker build output:", markup=False)
            console.print(output, markup=False)


def _strip_docker_worker_debug_details(manifest: dict[str, Any]) -> None:
    metadata = (
        manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    )
    summary = (
        metadata.get("mn_docker_workers")
        if isinstance(metadata.get("mn_docker_workers"), dict)
        else {}
    )
    services = summary.get("services") if isinstance(summary.get("services"), list) else []
    for service in services:
        if isinstance(service, dict):
            service.pop("build", None)


def run_bundle(
    bundle_path: str,
    *,
    follow_seconds: Optional[float] = None,
    env_overrides: Optional[dict[str, str]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    force: bool = False,
    detached: bool = False,
    web_ui: bool = False,
    auto_schedule: bool = False,
    schedule: Optional[str] = None,
    debug: bool = False,
    runtime_model_dependencies: RuntimeModelDependencies | None = None,
    job_id: str | None = None,
):
    """Run a bundle after applying optional runtime metadata and environment."""
    pre_launch_process: subprocess.Popen[Any] | None = None
    pre_launch_run_dir: Path | None = None
    submitted_job_id: str | None = None
    submitted_log_writer: JobLogWriter | None = None
    submitted_bundle_dir: Path | None = None
    submitted_manifest: dict[str, Any] | None = None
    submitted_run_dir: Path | None = None
    submitted_web_ui_url: str | None = None
    submitted_config_overrides: dict[str, Any] | None = None
    prepared_submission: Any | None = None
    try:
        env_overrides = dict(env_overrides or {})
        config_overrides = dict(config_overrides or {})
        submitted_config_overrides = config_overrides
        submission_metadata = dict(submission_metadata or {})
        bundle_dir, manifest_file, manifest_dict = _load_bundle_manifest(bundle_path)
        submitted_bundle_dir = bundle_dir
        manifest_dict = _configure_bundle_if_required(
            bundle_dir,
            manifest_file,
            manifest_dict,
        )

        _ensure_local_run_store_identity(
            bundle_dir,
            manifest_dict,
            env_overrides,
            submission_metadata,
            config_overrides=config_overrides,
        )
        _print_launch_progress(
            "Check runtime resources",
            "confirming the runtime can satisfy this blueprint before submission.",
        )
        runtime_model_plan = _build_runtime_model_prepare_plan(
            bundle_dir,
            manifest_dict,
            config_overrides=config_overrides,
            dependencies=runtime_model_dependencies,
            lazy=True,
        )
        runtime_resource_report = (
            runtime_model_dependencies.resource_report()
            if runtime_model_dependencies is not None
            and runtime_model_dependencies.resource_report is not None
            else None
        )
        runtime_system_summary = (
            runtime_model_dependencies.system_summary()
            if runtime_model_dependencies is not None
            and runtime_model_dependencies.system_summary is not None
            else None
        )
        _validate_manifest_hardware_or_exit(
            manifest_dict,
            force=force,
            allow_local_fallback=False,
            resource_report=runtime_resource_report,
        )
        _validate_distributed_runtime_model_feasibility(
            runtime_model_plan["placement_models"],
            resource_report=runtime_resource_report,
            system_summary=runtime_system_summary,
        )
        placement = _preflight_and_apply_runtime_model_placement(
            manifest_dict,
            runtime_model_requirements=[],
            resource_report=runtime_resource_report,
            system_summary=runtime_system_summary,
            env={**os.environ, **env_overrides},
        )
        if placement:
            selected_node = str(placement["selected_node"])
            env_overrides["MN_SELECTED_RUNTIME_NODE"] = selected_node
            model_fallbacks = placement.get("model_fallbacks")
            if isinstance(model_fallbacks, list) and model_fallbacks:
                # The placement preflight selected the portable catalog model.
                # Keep that decision through preparation so the native runtime
                # installs it on the same pinned node while workers continue to
                # call LiteLLM's logical ``default`` alias.
                env_overrides["MN_SELECTED_RUNTIME_MODEL_FALLBACKS_JSON"] = (
                    json.dumps(model_fallbacks, sort_keys=True)
                )
            submission_metadata["selected_node"] = selected_node
            submission_metadata["workflow_placement"] = {
                "mode": placement["mode"],
                "selected_node": selected_node,
                "selection": placement["selection"],
            }
            _print_launch_progress(
                "Resolve workflow placement",
                f"selected {selected_node}; all agents and node-local runtime services are pinned there.",
            )
        blueprint_run_id = submission_metadata.get(
            "blueprint_run_id"
        ) or env_overrides.get("MN_RUN_ID")
        if blueprint_run_id:
            pre_launch_run_dir = _blueprint_run_dir(
                str(blueprint_run_id), env_overrides
            )
            _register_post_launch_hook(
                bundle_dir, str(blueprint_run_id), env_overrides=env_overrides
            )
            pre_launch_process = _start_pre_launch_hook(
                bundle_dir,
                str(blueprint_run_id),
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
        if not force:
            _print_launch_progress(
                "Validate inputs and dependencies",
                "checking services, models, local inputs, and non-hard requirements.",
            )
            _validate_manifest_services_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
            _print_launch_progress(
                "Prepare runtime models",
                "validating lazy model policies; installation is deferred until first use.",
            )
            model_install_summary = _defer_runtime_models_for_run_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
                force=force,
                debug=debug,
                runtime_model_plan=runtime_model_plan,
                dependencies=runtime_model_dependencies,
            )
            _merge_runtime_model_config_overrides(
                config_overrides, model_install_summary
            )
            _validate_manifest_models_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
                model_install_summary=model_install_summary,
            )
            input_validation_report = _validate_manifest_inputs_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
            )
            _record_prevalidated_command_rules(manifest_dict, input_validation_report)
        else:
            console.print(
                "[yellow]Validation skipped because --force was provided; runtime models will still be selected and prepared lazily on first use.[/yellow]"
            )
            _print_launch_progress(
                "Prepare runtime models",
                "recording lazy model policies; installation is deferred until first use.",
            )
            model_install_summary = _defer_runtime_models_for_run_or_exit(
                bundle_dir,
                manifest_dict,
                env_overrides=env_overrides,
                config_overrides=config_overrides,
                force=force,
                debug=debug,
                runtime_model_plan=runtime_model_plan,
                dependencies=runtime_model_dependencies,
            )
            _merge_runtime_model_config_overrides(
                config_overrides, model_install_summary
            )
        _print_launch_progress(
            "Package workflow",
            "staging workflow files, local inputs, runtime helpers, and output wiring.",
        )
        manifest_dict = prepare_manifest_for_submission(
            bundle_dir,
            manifest_dict,
            env_overrides=env_overrides,
            submission_metadata=submission_metadata,
            config_overrides=config_overrides,
        )
        host_python_report = _doctor_prepare_hostlocal_python_envs(
            bundle_dir,
            manifest_dict,
            timeout=float(os.getenv("MN_BLUEPRINT_PYTHON_ENV_TIMEOUT_SECONDS", "30")),
            check_only=False,
        )
        if host_python_report.get("status") == "critical":
            failures = host_python_report.get("failures") or []
            detail = "; ".join(
                f"{item.get('node_id', 'host_local')}: {item.get('detail', 'environment preparation failed')}"
                for item in failures
                if isinstance(item, dict)
            )
            raise RuntimeError(
                "HostLocal Python environment preparation failed"
                + (f": {detail}" if detail else ".")
            )
        if force:
            _mark_manifest_force(manifest_dict)
        _prepare_openshell_custom_images(bundle_dir, manifest_dict)

        payloads = _stage_bundle_payloads(bundle_dir, manifest_dict)

        schedule_attrs = _run_schedule_attrs(
            auto_schedule=auto_schedule, schedule=schedule
        )
        if schedule_attrs is not None:
            submitted_manifest = manifest_dict
            stable_job_id = job_id
            if not stable_job_id:
                created = json.loads(
                    client.create_stable_job(
                        json.dumps(manifest_dict),
                        payloads,
                        resolved_configuration=config_overrides,
                    )
                )
                stable_job_id = str(created["job_id"])
            elif config_overrides:
                client.update_stable_job(
                    stable_job_id,
                    {"resolved_configuration": config_overrides},
                )
            result = json.loads(
                client.create_job_schedule(
                    stable_job_id,
                    schedule=schedule_attrs,
                    source={"cli": "blueprint run --schedule"},
                )
            )
            console.print_json(data=result)
            return

        _ensure_context_engine_for_run_if_needed(
            bundle_dir,
            manifest_dict,
            env_overrides=env_overrides,
            config_overrides=config_overrides,
            force=force,
        )

        docker_worker_nodes = _docker_worker_node_ids(manifest_dict)
        if docker_worker_nodes:
            selected_node = str(
                env_overrides.get("MN_SELECTED_RUNTIME_NODE") or ""
            ).strip()
            target = selected_node or "the selected runtime node"
            _print_launch_progress(
                f"Prepare DockerWorker on {target}",
                "building and starting the shared worker container through that node's native SDK.",
            )
        prepared_submission = prepare_job_submission(
            manifest_dict,
            payloads,
            bundle_dir=bundle_dir,
            run_id=blueprint_run_id,
            cluster_client=client,
            env={**os.environ, **env_overrides},
        )
        manifest = prepared_submission.manifest_json
        payloads = prepared_submission.payloads
        submitted_manifest = json.loads(manifest)
        if docker_worker_nodes:
            _print_docker_worker_ready(submitted_manifest, debug=debug)
            if debug:
                # Build logs are diagnostic terminal output. Do not persist
                # them in the submitted workflow manifest or Core job record.
                _strip_docker_worker_debug_details(submitted_manifest)
                manifest = json.dumps(submitted_manifest, sort_keys=True)

        blueprint_run_dir = (
            _blueprint_run_dir(blueprint_run_id, env_overrides)
            if blueprint_run_id
            else None
        )
        submitted_run_dir = blueprint_run_dir
        _print_launch_progress(
            "Submit runtime job",
            "handing the prepared bundle to MirrorNeuron core.",
        )
        stable_job_id = job_id
        if not stable_job_id:
            created = json.loads(
                client.create_stable_job(
                    manifest,
                    payloads,
                    resolved_configuration=config_overrides,
                )
            )
            stable_job_id = str(created["job_id"])
        elif config_overrides:
            client.update_stable_job(
                stable_job_id,
                {"resolved_configuration": config_overrides},
            )
        started = json.loads(
            client.start_run(
                stable_job_id,
                run_id=str(blueprint_run_id or ""),
                inputs=config_overrides,
            )
        )
        execution_id = str(started["run_id"])
        submitted_job_id = execution_id
        log_writer = JobLogWriter(execution_id, run_dir=blueprint_run_dir)
        submitted_log_writer = log_writer
        if blueprint_run_id:
            _write_blueprint_job_mapping(
                blueprint_run_id,
                stable_job_id,
                execution_id,
                submission_metadata,
                env_overrides,
                monitor_manifest=manifest_dict,
            )
        web_ui_url = (
            _console_web_ui_url(manifest_dict, blueprint_run_dir) if web_ui else None
        )
        submitted_web_ui_url = web_ui_url
        resolved_follow_seconds = (
            float(os.getenv("MN_RUN_DETACH_LOG_SECONDS", "30"))
            if follow_seconds is None
            else follow_seconds
        )

        console.print(
            generate_run_submitted_panel(
                bundle_name=bundle_dir.name,
                job_id=execution_id,
                payload_count=len(payloads),
                log_dir=log_writer.log_dir,
                follow_seconds=resolved_follow_seconds,
                run_mode=_run_mode_label(manifest_dict),
                blueprint_run_id=blueprint_run_id,
                blueprint_revision=submission_metadata.get("blueprint_revision"),
                web_ui_url=web_ui_url,
                detached=detached,
            )
        )
        if detached:
            if blueprint_run_dir is not None:
                _start_background_event_relay_if_needed(
                    bundle_dir,
                    submitted_manifest or manifest_dict,
                    execution_id,
                    blueprint_run_dir,
                    "submitted",
                    config_overrides=config_overrides,
                    submission_metadata=prepared_submission.metadata,
                )
            console.print(
                generate_detached_panel(
                    execution_id,
                    log_writer.log_dir,
                    "submitted",
                    log_writer.event_count,
                    web_ui_url=log_writer.web_ui_url or web_ui_url,
                )
            )
            return

        final_status = _stream_and_format_events(
            execution_id,
            log_writer,
            resolved_follow_seconds,
            web_ui_url=web_ui_url,
            manifest=manifest_dict,
        )
        if final_status in FINAL_STATUSES:
            materialized_shared = _materialize_shared_storage_outputs(
                prepared_submission.metadata
            )
            if not materialized_shared:
                _materialize_completed_blueprint_outputs(
                    log_writer.log_dir, manifest_dict
                )
        if blueprint_run_dir is not None:
            _start_background_event_relay_if_needed(
                bundle_dir,
                submitted_manifest or manifest_dict,
                execution_id,
                blueprint_run_dir,
                final_status,
                config_overrides=config_overrides,
                submission_metadata=prepared_submission.metadata,
            )
            if final_status in FINAL_STATUSES:
                cleanup_blueprint_host_hooks(
                    blueprint_run_dir,
                    dry_run=False,
                    summary={
                        "process_removed": [],
                        "process_skipped": [],
                        "errors": [],
                    },
                    reason=f"job_{final_status}",
                )
    except typer.Exit:
        _cleanup_pre_launch_artifacts(
            pre_launch_process,
            pre_launch_run_dir,
            reason="launch_failed",
        )
        raise
    except (KeyboardInterrupt, EOFError):
        if submitted_job_id:
            log_writer = submitted_log_writer or JobLogWriter(
                submitted_job_id, run_dir=submitted_run_dir
            )
            status = "running"
            try:
                status, _data = _follow_job_events(submitted_job_id, log_writer, 0)
                if status == "unknown":
                    status = "running"
            except Exception:
                log_writer.run_logger.exception("Failed to poll detached job status")
            console.print(f"[yellow]{DETACHED_AFTER_INTERRUPT_MESSAGE}[/yellow]")
            if (
                submitted_run_dir is not None
                and submitted_bundle_dir is not None
                and submitted_manifest is not None
            ):
                _start_background_event_relay_if_needed(
                    submitted_bundle_dir,
                    submitted_manifest,
                    submitted_job_id,
                    submitted_run_dir,
                    status,
                    config_overrides=submitted_config_overrides,
                    submission_metadata=prepared_submission.metadata,
                )
            console.print(
                generate_detached_panel(
                    submitted_job_id,
                    log_writer.log_dir,
                    status,
                    log_writer.event_count,
                    web_ui_url=log_writer.web_ui_url or submitted_web_ui_url,
                )
            )
            return
        _cleanup_pre_launch_artifacts(
            pre_launch_process,
            pre_launch_run_dir,
            reason="launch_interrupted",
        )
        raise typer.Exit(130)
    except Exception as e:
        if prepared_submission is not None and submitted_job_id is None:
            submission_id = str(
                prepared_submission.metadata.get("submission_id") or ""
            ).strip()
            if submission_id:
                try:
                    cleanup_docker_worker_services(submission_id=submission_id)
                except Exception:
                    logger.exception(
                        "Failed to clean DockerWorker services after submission failure",
                        extra={"submission_id": submission_id},
                    )
        _cleanup_pre_launch_artifacts(
            pre_launch_process,
            pre_launch_run_dir,
            reason="launch_failed",
        )
        # ``blueprint run --debug`` is a command-local option, so it cannot
        # update the root callback's debug state. Pass it explicitly here;
        # otherwise preparation failures only show the generic error even
        # though the run has already enabled debug settings in its payload.
        handle_cli_error(e, console, "run bundle", debug=debug or debug_enabled())
        raise typer.Exit(1)


__all__ = [name for name in globals() if not name.startswith("__")]
