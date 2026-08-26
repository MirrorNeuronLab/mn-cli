from mn_cli.libs.run_cmds.common import (
    _blueprint_runtime_environment,
    _is_safe_payload_relative_path,
    _runtime_resource_report,
)
from mn_cli.libs.run_cmds.model_config import (
    _manifest_for_model_validation,
    _model_validation_inputs_with_prepared_models,
    _prepared_model_installed_resolver,
)
from mn_cli.libs.workflow_validation import (
    _is_workflow_manifest,
    _manifest_workflow_id,
    _validate_workflow_manifest_issues,
    _validate_workflow_schema_issues,
)
from mn_cli.output import json_enabled, record_result

from ..common import *
from ..models import *


def validate(
    bundle_path: Annotated[
        str,
        typer.Argument(help="Path to the local job bundle folder."),
    ],
):
    """Validate a local job bundle before submitting it.

    Examples:
      mn blueprint validate ./bundle
      mn blueprint validate ./bundle --json
    """
    try:
        output_format = "json" if json_enabled() else "table"
        bundle_dir = Path(bundle_path)
        if not bundle_dir.is_dir():
            print_error(console, f"'{bundle_path}' is not a directory. Expected a bundle folder.")
            raise typer.Exit(2)

        manifest_file = bundle_dir / "manifest.json"
        if not manifest_file.exists():
            print_error(console, f"manifest.json not found in '{bundle_path}'.")
            raise typer.Exit(2)

        with open(manifest_file, "r") as f:
            try:
                manifest = json.load(f)
            except json.JSONDecodeError as e:
                print_error(console, f"manifest.json is not valid JSON: {e}")
                raise typer.Exit(2)

        if is_manifest_source(manifest):
            manifest = expand_manifest_source(manifest, root_dir=bundle_dir)

        schema_issues = _validate_workflow_schema_issues(manifest)
        if schema_issues:
            report = make_validation_report(schema_issues)
            _emit_validation_report(
                report, output_format, title="Workflow manifest schema validation failed"
            )
            raise typer.Exit(2)

        workflow_issues = _validate_workflow_manifest_issues(manifest)
        if workflow_issues:
            report = make_validation_report(workflow_issues)
            _emit_validation_report(
                report, output_format, title="Workflow manifest validation failed"
            )
            raise typer.Exit(2)

        python_environment_errors = validate_python_environments(bundle_dir, manifest)
        if python_environment_errors:
            report = make_validation_report(
                [
                    _validation_issue(error, source="manifest")
                    for error in python_environment_errors
                ]
            )
            _emit_validation_report(
                report, output_format, title="Manifest validation failed"
            )
            raise typer.Exit(2)

        skill_runtime_errors = validate_skill_runtime_requirements(bundle_dir, manifest)
        if skill_runtime_errors:
            report = make_validation_report(
                [
                    _validation_issue(error, source="manifest")
                    for error in skill_runtime_errors
                ]
            )
            _emit_validation_report(
                report, output_format, title="Manifest validation failed"
            )
            raise typer.Exit(2)

        manifest_spec_issues = (
            validate_service_spec_issues(manifest)
            + validate_requirements_spec_issues(manifest)
            + validate_resource_spec_issues(manifest)
            + validate_input_validation_spec_issues(manifest)
        )
        if manifest_spec_issues:
            report = make_validation_report(manifest_spec_issues)
            _emit_validation_report(
                report, output_format, title="Manifest validation failed"
            )
            raise typer.Exit(2)

        validation_result = _validate_manifest_inputs_or_exit(
            bundle_dir,
            manifest,
            output_format=output_format,
            exit_code=2,
        )

        if json_enabled():
            record_result(validation_result)
            return

        details: list[tuple[str, Any]] = [
            ("Bundle", bundle_path),
            ("Job Name", manifest.get("job_name")),
            ("Workflow ID", _manifest_workflow_id(manifest)),
        ]
        workflow = manifest.get("workflow", {}) if isinstance(manifest.get("workflow"), dict) else {}
        steps = workflow.get("steps") if isinstance(workflow.get("steps"), list) else []
        details.append(("Workflow steps", len(steps)))
        details.append(("Input validation rules", len(validation_result.get("results") or [])))
        print_confirmed(
            console,
            "Job bundle validation",
            status="valid",
            details=details,
        )

    except typer.Exit:
        raise
    except Exception as e:
        handle_cli_error(e, console, "validate")
        raise typer.Exit(1)

def validate_python_environments(
    bundle_dir: Path, manifest: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    nodes = _manifest_agent_nodes(manifest)
    if not nodes:
        return errors

    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        config = node.get("config")
        if not isinstance(config, dict) or "python_environment" not in config:
            continue

        node_id = str(node.get("node_id") or f"nodes[{index}]")
        runner_module = config.get("runner_module")
        python_environment = config.get("python_environment")
        if runner_module != "MirrorNeuron.Runner.HostLocal":
            errors.append(
                f"{node_id}: python_environment is only supported with MirrorNeuron.Runner.HostLocal"
            )
            continue
        if not isinstance(python_environment, dict):
            errors.append(f"{node_id}: python_environment must be an object")
            continue

        requirements = python_environment.get("requirements")
        if requirements not in (None, ""):
            if not isinstance(requirements, str):
                errors.append(
                    f"{node_id}: python_environment.requirements must be a string"
                )
            elif not _is_safe_payload_relative_path(requirements):
                errors.append(
                    f"{node_id}: python_environment.requirements must be a relative path inside payloads/"
                )
            elif not (bundle_dir / "payloads" / requirements).is_file():
                errors.append(
                    f"{node_id}: python_environment requirements file not found: payloads/{requirements}"
                )

        packages = python_environment.get("packages")
        if packages is not None and (
            not isinstance(packages, list)
            or not all(
                isinstance(package, str) and package.strip() for package in packages
            )
        ):
            errors.append(
                f"{node_id}: python_environment.packages must be a list of non-empty strings"
            )

    return errors

def _manifest_agent_nodes(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    agents = manifest.get("agents") if isinstance(manifest.get("agents"), dict) else {}
    agent_nodes = agents.get("nodes") if isinstance(agents, dict) else None
    if isinstance(agent_nodes, list):
        return [node for node in agent_nodes if isinstance(node, dict)]
    return []

def _validate_manifest_inputs_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    output_format: str = "table",
    exit_code: int = 1,
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
    result = run_input_validation(bundle_dir, manifest, config=config, env=env)
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Input validation failed")
    raise typer.Exit(exit_code)

def _validate_manifest_hardware_or_exit(
    manifest: dict[str, Any],
    *,
    force: bool = False,
    output_format: str = "table",
    allow_local_fallback: bool = False,
    resource_report: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    result = run_hardware_requirements_validation(
        manifest,
        resource_report=(
            (lambda: resource_report)
            if isinstance(resource_report, dict)
            else lambda: _runtime_resource_report(
                allow_local_fallback=allow_local_fallback
            )
        ),
        force=force,
    )
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Runtime requirements need attention")
    raise typer.Exit(1)

def _validate_manifest_services_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    output_format: str = "table",
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

    result = run_service_validation(
        bundle_dir,
        manifest,
        config=config,
        env=env,
        resolver=resolver,
    )
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Service validation failed")
    raise typer.Exit(1)

def _validate_manifest_models_or_exit(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    env_overrides: Optional[dict[str, str]] = None,
    config_overrides: Optional[dict[str, Any]] = None,
    model_install_summary: Optional[dict[str, Any]] = None,
    output_format: str = "table",
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
    result = run_model_validation(
        bundle_dir,
        validation_manifest,
        config=validation_config,
        env=env,
        installed_resolver=_prepared_model_installed_resolver(model_install_summary),
    )
    if result.get("ok"):
        return result

    _emit_validation_report(result, output_format, title="Model validation failed")
    raise typer.Exit(1)

def _normalize_validation_output(output: str) -> str:
    normalized = str(output or "table").strip().lower()
    if normalized in {"table", "rich", "pretty"}:
        return "table"
    if normalized == "json":
        return "json"
    console.print("[red]Unsupported output format. Use 'table' or 'json'.[/red]")
    raise typer.Exit(1)

def _emit_validation_report(
    report: dict[str, Any], output_format: str, *, title: str
) -> None:
    if output_format == "json":
        record_result(report)
        return

    issues = report.get("issues") if isinstance(report.get("issues"), list) else []
    if not issues:
        for error in report.get("errors") or []:
            console.print(f"[red]{title}: {error}[/red]")
        return

    print_validation_issues(console, title, issues)

def _model_capacity_summary(report: dict[str, Any]) -> str:
    summaries: list[str] = []
    for result in report.get("results") or []:
        if not isinstance(result, dict):
            continue
        requirements = result.get("requirements")
        if not isinstance(requirements, dict) or not requirements:
            continue
        parts = [str(result.get("model_id") or result.get("model") or result.get("name") or "model")]
        provider = result.get("provider")
        if provider:
            parts.append(f"provider {provider}")
        min_vram = requirements.get("min_vram_gb")
        if min_vram is not None:
            parts.append(f"GPU >= {min_vram}GB")
        capabilities = requirements.get("required_capabilities")
        if capabilities:
            parts.append("capability any of " + ",".join(str(item) for item in capabilities))
        summaries.append(" ".join(parts))
    return "; ".join(summaries[:3])

def _validation_issue(error: str, *, source: str) -> dict[str, Any]:
    path = ""
    if ":" in error:
        path = error.split(":", 1)[0].strip()
    return {
        "code": "manifest.validation_failed",
        "message": error,
        "help": "Fix this manifest field and run validation again.",
        "severity": "error",
        "location": {
            "source": source,
            "path": path,
            "pointer": "/" + source + ("/" + path.replace(".", "/") if path else ""),
        },
    }


__all__ = [name for name in globals() if not name.startswith("__")]
