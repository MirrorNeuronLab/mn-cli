from .common import *
from .model_cluster import *
from .model_config import *
from mn_cli.libs.model_cmds import _sync_gateway_runtime_endpoints_across_cluster


def _build_runtime_model_prepare_plan(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    base_config = (
        load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    )
    catalog = load_model_catalog()
    config = _config_with_auto_runtime_model_profile(
        base_config,
        catalog=catalog,
        resolve_cluster_model=_resolve_runtime_cluster_model,
    )
    validation_manifest = _manifest_for_model_validation(manifest, config)
    requirements = required_blueprint_models(
        validation_manifest, config, catalog=catalog
    )
    placement_models: list[dict[str, Any]] = []
    seen: set[str] = set()
    for requirement in requirements:
        if cluster_provided_model(requirement):
            continue
        try:
            entry = resolve_requirement_entry(
                requirement,
                catalog=catalog,
                catalog_resolver=resolve_model_entry,
            )
        except Exception:
            continue
        if str(entry.get("provider") or "docker_model_runner") != "docker_model_runner":
            continue
        docker_model = docker_model_name(entry)
        if docker_model in seen:
            continue
        seen.add(docker_model)
        placement_models.append(
            {
                "id": str(entry.get("id") or requirement.get("name") or docker_model),
                "label": str(
                    entry.get("id") or requirement.get("name") or docker_model
                ),
                "model": docker_model,
                "entry": entry,
                "source": str(
                    requirement.get("path")
                    or requirement.get("manifest_path")
                    or "config"
                ),
            }
        )
    return {
        "base_config": base_config,
        "catalog": catalog,
        "config": config,
        "validation_manifest": validation_manifest,
        "requirements": requirements,
        "placement_models": placement_models,
    }


def _prepare_runtime_models_for_run_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    force: bool = False,
    quiet: bool = False,
    runtime_model_plan: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    plan = runtime_model_plan or _build_runtime_model_prepare_plan(
        bundle_dir,
        manifest,
        config_overrides=config_overrides,
    )
    base_config = plan["base_config"]
    catalog = plan["catalog"]
    config = plan["config"]
    validation_manifest = plan["validation_manifest"]
    with _runtime_model_placement_scope(env_overrides):
        summary = blueprint_model_dependency_summary(
            blueprint_id=_runtime_model_blueprint_id(bundle_dir, manifest, config),
            blueprint_revision=_runtime_model_blueprint_revision(manifest, config),
            bundle_root=bundle_dir,
            manifest=validation_manifest,
            config=config,
            install_source=str(bundle_dir),
            force=force,
            ops=BlueprintModelOps(
                load_model_catalog=lambda: catalog,
                required_blueprint_models=required_blueprint_models,
                load_model_ownership=load_model_ownership,
                resolve_model_entry=resolve_model_entry,
                docker_model_name=docker_model_name,
                cluster_provided_model=cluster_provided_model,
                record_model_owner=record_model_owner,
                model_installed=model_installed,
                install_model_entry=install_model_entry,
                resolve_model_endpoint=_resolve_runtime_model_endpoint,
                notify_model_install_start=None
                if quiet
                else _print_runtime_model_install_start,
                install_model_with_progress=None
                if quiet
                else _install_runtime_model_with_progress,
                resolve_cluster_model=_resolve_runtime_cluster_model,
                install_cluster_model=_install_runtime_cluster_model,
            ),
        )
    if summary["errors"]:
        if not quiet:
            _print_runtime_model_install_summary(summary)
        raise typer.Exit(1)
    endpoints = _sync_litellm_gateway_for_runtime_models(
        summary, env_overrides=env_overrides
    )
    if endpoints and env_overrides is not None:
        env_overrides.update(ModelEndpointMap(endpoints).to_env_overrides())
    prepared_json = _prepared_runtime_models_json(summary)
    if prepared_json and env_overrides is not None:
        env_overrides["MN_PREPARED_RUNTIME_MODELS_JSON"] = prepared_json
    materialized_config = _config_with_runtime_model_endpoints(config, summary)
    materialized_config = _config_with_runtime_model_fallbacks(
        materialized_config, summary
    )
    materialized_config = _config_with_runtime_model_profile(materialized_config)
    materialized_config = _config_with_runtime_model_endpoints(
        materialized_config, summary
    )
    if (
        config is not base_config or materialized_config is not config
    ) and env_overrides is not None:
        summary["config_overrides"] = materialized_config
        env_overrides["MN_BLUEPRINT_CONFIG_JSON"] = json.dumps(
            materialized_config, sort_keys=True
        )
        env_overrides.update(_runtime_model_fallback_llm_env(materialized_config))
    if env_overrides is not None and _blueprint_requests_default_llm(base_config):
        env_overrides["MN_LLM_MODEL"] = "default"
        env_overrides["LITELLM_MODEL"] = "default"
    if not quiet:
        _print_runtime_model_install_summary(summary)
    return summary


def _blueprint_requests_default_llm(config: dict[str, Any]) -> bool:
    llm = config.get("llm") if isinstance(config.get("llm"), dict) else {}
    return str(llm.get("model") or "").strip().lower() == "default"


def _sync_litellm_gateway_for_runtime_models(
    summary: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
) -> dict[str, dict[str, Any]]:
    endpoints = (
        summary.get("endpoints") if isinstance(summary.get("endpoints"), dict) else {}
    )
    upstream_endpoints = dict(endpoints)
    upstream_endpoints.update(_local_runtime_model_endpoints(summary))
    if not upstream_endpoints:
        return {}
    reconcile_cluster_model_remotes(
        upstream_endpoints,
        local_installed_models=installed_model_names(),
        local_node=_local_runtime_node_name(),
        replace=False,
    )
    restart = _runtime_litellm_gateway_restart_enabled()
    gateway = sync_litellm_gateway(
        runtime_endpoints=upstream_endpoints,
        restart=restart,
    )
    # The submitter's proxy may be the one that ultimately runs the workflow.
    # Publish each remote route to every other live proxy as well.  The owner
    # is excluded by the shared helper because it already has a node-local DMR
    # route from PrepareRuntimeModel; pointing it at itself would create a
    # gateway loop.
    gateway["cluster_sync"] = _sync_gateway_runtime_endpoints_across_cluster(
        upstream_endpoints,
        restart=restart,
        quiet=True,
        skip_local=True,
    )
    gateway_endpoints = gateway_endpoint_map(upstream_endpoints)
    summary["gateway"] = gateway
    summary["endpoints"] = gateway_endpoints
    return gateway_endpoints


def _runtime_litellm_gateway_restart_enabled() -> bool:
    return (
        str(os.environ.get("MN_LITELLM_GATEWAY_RESTART", "true")).strip().lower()
        not in FALSE_VALUES
    )


def _local_runtime_model_endpoints(
    summary: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    endpoints: dict[str, dict[str, Any]] = {}
    prepared_statuses = {"installed", "already_installed"}
    for item in summary.get("models") or []:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "")
        if status not in prepared_statuses:
            continue
        if str(item.get("provider") or "docker_model_runner") != "docker_model_runner":
            continue
        prepared_endpoint = (
            item.get("endpoint") if isinstance(item.get("endpoint"), dict) else {}
        )
        if prepared_endpoint and str(prepared_endpoint.get("source") or "") != "local-dmr":
            continue
        model_ref = str(item.get("id") or item.get("model") or "").strip()
        try:
            entry = resolve_model_entry(model_ref)
        except Exception:
            entry = {
                "id": model_ref,
                "provider": "docker_model_runner",
                "model": str(item.get("model") or model_ref),
                "api_model": str(item.get("model") or model_ref),
            }
        endpoint = docker_model_runner_endpoint(entry, source="local-dmr")
        for key in {
            model_ref,
            str(item.get("model") or "").strip(),
            str(entry.get("id") or "").strip(),
            str(entry.get("api_model") or "").strip(),
        }:
            if key:
                endpoints[key] = endpoint
    return endpoints


def _resolve_runtime_model_endpoint(
    *, requirement: dict[str, Any], entry: dict[str, Any]
) -> dict[str, Any] | None:
    model = str(requirement.get("model") or entry.get("id") or "").strip()
    config = (
        requirement.get("config") if isinstance(requirement.get("config"), dict) else {}
    )
    services = _resolve_model_services_for_requirement(entry)
    try:
        endpoint = resolve_model_endpoint(
            model,
            config=config,
            entry=entry,
            services=services,
            remotes=load_model_remotes(),
        )
    except Exception:
        return None
    if _node_owned_dmr_endpoint_requires_prepare(endpoint):
        return None
    return endpoint


def _node_owned_dmr_endpoint_requires_prepare(endpoint: dict[str, Any] | None) -> bool:
    if not isinstance(endpoint, dict):
        return False
    if str(endpoint.get("source") or "") not in {"model_remote", "service_registry"}:
        return False
    if not str(endpoint.get("node") or "").strip():
        return False
    provider = str(endpoint.get("provider") or "docker_model_runner").strip().lower()
    return provider in {"docker_model_runner", "docker-model-runner", "dmr"}


def _resolve_model_services_for_requirement(
    entry: dict[str, Any],
) -> list[dict[str, Any]]:
    tags = _model_service_tags(entry)
    services: list[dict[str, Any]] = []
    for tag in tags:
        try:
            response = client.resolve_service(
                "docker-model-runner",
                tags=[tag],
                passing_only=True,
            )
            decoded = json.loads(response)
            for service in decoded.get("services") or []:
                if isinstance(service, dict) and service not in services:
                    services.append(service)
        except Exception:
            continue
    return services


def _model_service_tags(entry: dict[str, Any]) -> list[str]:
    return model_service_tags(entry)


def _runtime_model_blueprint_id(
    bundle_dir: Path,
    manifest: dict[str, Any],
    config: dict[str, Any],
) -> str:
    metadata = (
        manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    )
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    for value in (
        metadata.get("blueprint_id"),
        metadata.get("blueprintId"),
        identity.get("blueprint_id"),
        identity.get("blueprintId"),
        manifest.get("id"),
        manifest.get("graph_id"),
        manifest.get("job_name"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return bundle_dir.name


def _runtime_model_blueprint_revision(
    manifest: dict[str, Any],
    config: dict[str, Any],
) -> str | None:
    metadata = (
        manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    )
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    for value in (
        metadata.get("blueprint_revision"),
        metadata.get("blueprintRevision"),
        identity.get("blueprint_revision"),
        identity.get("blueprintRevision"),
        manifest.get("revision"),
        manifest.get("version"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return None


def _print_runtime_model_install_start(model: dict[str, Any]) -> None:
    label = str(model.get("id") or model.get("model") or "runtime model")
    docker_model = str(model.get("model") or "")
    backend = str(model.get("backend") or "auto")
    detail = (
        f"{label} ({docker_model})" if docker_model and docker_model != label else label
    )
    console.print(
        f"[yellow]Runtime model {detail} is not installed. "
        f"Installing with backend {backend}; this may take a few minutes the first time.[/yellow]"
    )


def _install_runtime_model_with_progress(
    entry: dict[str, Any],
    *,
    model: dict[str, Any],
    backend: str,
    context_size: Any,
    force: bool,
) -> dict[str, Any]:
    label = str(
        model.get("id")
        or model.get("model")
        or entry.get("id")
        or entry.get("model")
        or "runtime model"
    )
    docker_model = str(model.get("model") or entry.get("model") or "")
    detail = (
        f"{label} ({docker_model})" if docker_model and docker_model != label else label
    )
    console.print(
        f"[cyan]Installing runtime model {detail} with Docker Model Runner...[/cyan]"
    )
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        disable=not use_progress(),
    ) as progress:
        progress.add_task(
            f"[cyan]Pulling and starting {detail} with backend {backend}...",
            total=None,
        )
        return install_model_entry(
            entry,
            backend=backend,
            context_size=context_size,
            force=force,
        )


__all__ = [name for name in globals() if not name.startswith("__")]
