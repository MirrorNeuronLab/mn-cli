from mn_sdk.native_resource_registry import register_native_resource
from mn_sdk.submission_preparation import (
    _ensure_docker_worker_requirements_install,
    _local_skill_dependency_source_records,
    _local_skill_requirements_text,
    _requirements_text,
    _safe_dependency_source_name,
)

from mn_cli.runtime_state import mn_home, read_env_file

from .common import *

OPENSHELL_RUNNER_MODULES = {
    "MirrorNeuron.Runner.OpenShell",
    "MirrorNeuron.Sandbox.OpenShell",
}

def _openshell_executable() -> str:
    user_executable = Path.home() / ".local" / "bin" / "openshell"
    if user_executable.is_file() and os.access(user_executable, os.X_OK):
        return str(user_executable)
    return "openshell"

def _prepare_openshell_custom_images(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    *,
    shared_sandbox_job_id: str | None = None,
) -> None:
    nodes = manifest_nodes(manifest_dict)
    flow = manifest_dict.get("flow") if isinstance(manifest_dict.get("flow"), dict) else {}
    if not nodes and isinstance(flow.get("nodes"), list):
        nodes = [node for node in flow["nodes"] if isinstance(node, dict)]

    for node in nodes:
        if not isinstance(node, dict):
            continue
        config = node.get("config")
        if not isinstance(config, dict):
            continue
        if config.get("runner_module") not in OPENSHELL_RUNNER_MODULES:
            continue

        custom_image = config.get("custom_openshell_image")
        if custom_image is not None:
            source_path = _openshell_local_from_path(bundle_dir, custom_image)
            if source_path is None:
                console.print(
                    f"[red]custom_openshell_image for {node.get('node_id') or 'OpenShell node'} "
                    f"must point to a payload directory or Dockerfile: {custom_image}[/red]"
                )
                raise typer.Exit(1)
        else:
            source_path = _openshell_local_from_path(bundle_dir, config.get("from"))
            if source_path is None:
                source_path = _openshell_conventional_local_image_context(
                    bundle_dir, config.get("from")
                )

        if source_path is not None:
            build_source = _openshell_skill_dependency_context(source_path, manifest_dict)
            try:
                config["from"] = _build_openshell_from_image(
                    build_source, node.get("node_id") or "openshell"
                )
            finally:
                if build_source != source_path:
                    shutil.rmtree(build_source, ignore_errors=True)

        if shared_sandbox_job_id and config.get("reuse_shared_sandbox") is True:
            _prepare_openshell_shared_sandbox(
                bundle_dir,
                config,
                job_id=shared_sandbox_job_id,
                node_id=node.get("node_id") or "openshell",
            )
            _record_openshell_native_resource(
                manifest_dict,
                sandbox_name=str(config.get("sandbox_name") or ""),
                job_id=shared_sandbox_job_id,
            )

def _openshell_gateway_endpoint() -> str:
    configured_endpoint = os.getenv("OPENSHELL_GATEWAY_ENDPOINT")
    if configured_endpoint:
        return configured_endpoint

    configured_gateway = os.getenv("OPENSHELL_GATEWAY", "").strip()
    if configured_gateway:
        metadata = _openshell_gateway_metadata(configured_gateway)
        endpoint = metadata.get("gateway_endpoint")
        if isinstance(endpoint, str) and endpoint.strip():
            return endpoint.strip()

    runtime_endpoint = _openshell_runtime_gateway_endpoint()
    if runtime_endpoint:
        return runtime_endpoint

    gateway_name = _openshell_gateway_name()
    if gateway_name:
        metadata = _openshell_gateway_metadata(gateway_name)
        endpoint = metadata.get("gateway_endpoint")
        if isinstance(endpoint, str) and endpoint.strip():
            return endpoint.strip()

    return f"http://127.0.0.1:{os.getenv('OPENSHELL_GATEWAY_PORT', '58080')}"

def _openshell_env() -> dict[str, str]:
    env = os.environ.copy()
    if env.get("OPENSHELL_GATEWAY_ENDPOINT"):
        return env

    if env.get("OPENSHELL_GATEWAY", "").strip():
        return env

    runtime_endpoint = _openshell_runtime_gateway_endpoint()
    if runtime_endpoint:
        env["OPENSHELL_GATEWAY_ENDPOINT"] = runtime_endpoint
        env.pop("OPENSHELL_GATEWAY", None)
        return env

    gateway_name = _openshell_gateway_name(env=env)
    if gateway_name:
        env.setdefault("OPENSHELL_GATEWAY", gateway_name)
    else:
        env.setdefault("OPENSHELL_GATEWAY_ENDPOINT", _openshell_gateway_endpoint())
    return env

def _openshell_runtime_gateway_endpoint() -> str:
    runtime_env = read_env_file(mn_home() / "docker-compose.env")
    endpoint = runtime_env.get("OPENSHELL_GATEWAY_ENDPOINT", "").strip()
    return endpoint

def _openshell_config_dir() -> Path:
    return Path(
        os.getenv("OPENSHELL_CONFIG_DIR", str(Path.home() / ".config" / "openshell"))
    ).expanduser()

def _openshell_gateway_name(*, env: dict[str, str] | None = None) -> str:
    source_env = env or os.environ
    configured_gateway = source_env.get("OPENSHELL_GATEWAY", "").strip()
    if configured_gateway:
        return configured_gateway

    config_dir = _openshell_config_dir()
    try:
        active_gateway = (
            (config_dir / "active_gateway").read_text(encoding="utf-8").strip()
        )
        if active_gateway:
            return active_gateway
    except OSError:
        pass

    if (config_dir / "gateways" / "openshell" / "metadata.json").is_file():
        return "openshell"
    return ""

def _openshell_gateway_metadata(gateway_name: str) -> dict[str, Any]:
    if not gateway_name:
        return {}

    metadata_path = (
        _openshell_config_dir() / "gateways" / gateway_name / "metadata.json"
    )
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return metadata if isinstance(metadata, dict) else {}

def _openshell_local_from_path(bundle_dir: Path, source: Any) -> Path | None:
    if not isinstance(source, str) or not source.strip():
        return None

    source = source.strip()
    if "://" in source:
        return None

    raw = Path(source).expanduser()
    candidates = (
        [raw]
        if raw.is_absolute()
        else [bundle_dir / "payloads" / source, bundle_dir / source]
    )

    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate.is_dir() and (candidate / "Dockerfile").is_file():
            return candidate
        if candidate.is_file() and candidate.name == "Dockerfile":
            return candidate
    return None


def _openshell_conventional_local_image_context(
    bundle_dir: Path, image: Any
) -> Path | None:
    """Resolve the portable fallback context for a local-only image tag."""

    if not isinstance(image, str) or not image.strip().endswith(":local"):
        return None
    context = (bundle_dir / "payloads" / "openshell_image").resolve()
    if context.is_dir() and (context / "Dockerfile").is_file():
        return context
    return None

def _prepare_openshell_shared_sandbox(
    bundle_dir: Path,
    config: dict[str, Any],
    *,
    job_id: str,
    node_id: Any,
) -> None:
    sandbox_name = _openshell_shared_sandbox_name(job_id)
    env = _openshell_env()
    openshell_executable = _openshell_executable()
    register_native_resource(
        kind="openshell",
        scope="definition",
        external_id=sandbox_name,
        job_id=job_id,
        owner_node=str(env.get("MN_NODE_NAME") or ""),
        cleanup={"command": openshell_executable},
        state="preparing",
        mn_home=mn_home(),
        env=env,
    )
    existing = subprocess.run(
        [openshell_executable, "sandbox", "get", sandbox_name],
        capture_output=True,
        text=True,
        env=env,
    )
    if existing.returncode != 0:
        command = [
            openshell_executable,
            "sandbox",
            "create",
            "--name",
            sandbox_name,
        ]
        if config.get("gpu") is True:
            command.append("--gpu")
        for option, value in (
            ("--from", config.get("from")),
            ("--remote", config.get("remote")),
            ("--ssh-key", config.get("ssh_key")),
        ):
            if isinstance(value, str) and value.strip():
                command.extend([option, value.strip()])
        policy = _openshell_policy_path(bundle_dir, config.get("policy"))
        if policy is not None:
            command.extend(["--policy", str(policy)])
        for provider in config.get("providers") or []:
            if isinstance(provider, str) and provider.strip():
                command.extend(["--provider", provider.strip()])
        if config.get("no_auto_providers", True) is True:
            command.append("--no-auto-providers")
        command.extend(
            [
                "--no-tty",
                "--",
                "bash",
                "-lc",
                "mkdir -p /sandbox/job && true",
            ]
        )
        created = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=env,
        )
        if created.returncode != 0:
            detail = (created.stderr or created.stdout).strip()
            console.print(
                f"[red]Failed to prepare shared OpenShell sandbox for {node_id}.[/red]"
            )
            if detail:
                console.print(detail)
            raise typer.Exit(1)

    config["sandbox_name"] = sandbox_name
    config["ssh_host"] = f"openshell-{sandbox_name}"

def _openshell_shared_sandbox_name(job_id: str) -> str:
    raw_job_id = str(job_id).strip() or "job"
    base = re.sub(
        r"[^a-z0-9-]",
        "-",
        f"mirror-neuron-job-{raw_job_id}".lower(),
    ).strip("-")
    digest = hashlib.sha256(raw_job_id.encode("utf-8")).hexdigest()[:10]
    suffix = f"-{digest}"
    return f"{base[: max(63 - len(suffix), 1)].rstrip('-')}{suffix}"


def _record_openshell_native_resource(
    manifest: dict[str, Any], *, sandbox_name: str, job_id: str
) -> None:
    if not sandbox_name:
        return
    metadata = manifest.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        manifest["metadata"] = metadata
    native = metadata.setdefault(
        "mn_native_resources", {"version": 1, "submission_id": "", "resources": []}
    )
    if not isinstance(native, dict):
        native = {"version": 1, "submission_id": "", "resources": []}
        metadata["mn_native_resources"] = native
    resources = native.setdefault("resources", [])
    if not isinstance(resources, list):
        resources = []
        native["resources"] = resources
    descriptor = {
        "kind": "openshell",
        "scope": "definition",
        "external_id": sandbox_name,
        "owner_node": str(os.getenv("MN_NODE_NAME") or ""),
        "job_id": job_id,
    }
    if not any(
        isinstance(item, dict)
        and item.get("kind") == "openshell"
        and item.get("external_id") == sandbox_name
        for item in resources
    ):
        resources.append(descriptor)

def _openshell_policy_path(bundle_dir: Path, source: Any) -> Path | None:
    if not isinstance(source, str) or not source.strip():
        return None
    raw = Path(source.strip()).expanduser()
    candidates = (
        [raw]
        if raw.is_absolute()
        else [bundle_dir / "payloads" / raw, bundle_dir / raw]
    )
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file():
            return resolved
    print_error(console, f"OpenShell policy file was not found: {source}", code="MN_NOT_FOUND")
    raise typer.Exit(2)

def _openshell_skill_dependency_context(source_path: Path, manifest: dict[str, Any]) -> Path:
    requirements_text = gar_requirements_text(manifest)
    local_sources = _local_skill_dependency_source_records(manifest)
    if not requirements_text and not local_sources:
        return source_path

    source_root = source_path.parent if source_path.is_file() else source_path
    temp_context = Path(tempfile.mkdtemp(prefix=f"mn-openshell-skill-deps-{source_root.name}."))
    shutil.copytree(source_root, temp_context, dirs_exist_ok=True)
    dockerfile = temp_context / "Dockerfile"
    requirements = temp_context / "requirements.txt"
    existing_requirements = requirements.read_text(encoding="utf-8") if requirements.is_file() else ""
    requirements.write_text(
        _requirements_text([*existing_requirements.splitlines(), *requirements_text.splitlines()]),
        encoding="utf-8",
    )
    local_context_sources: list[str] = []
    for record in local_sources:
        local_source = Path(record["source"]).expanduser()
        if not local_source.exists():
            continue
        name = _safe_dependency_source_name(local_source)
        relative_target = Path("__mn_skill_dependencies") / "local" / name
        target = temp_context / relative_target
        if local_source.is_dir():
            shutil.copytree(local_source, target, dirs_exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_source, target)
        local_context_sources.append(relative_target.as_posix())
    if local_context_sources:
        (temp_context / "local-requirements.txt").write_text(
            _local_skill_requirements_text(local_context_sources),
            encoding="utf-8",
        )
    dockerfile.write_text(
        _ensure_docker_worker_requirements_install(
            dockerfile.read_text(encoding="utf-8"),
            local_context_sources=local_context_sources,
        ),
        encoding="utf-8",
    )
    return temp_context

def _build_openshell_from_image(source_path: Path, node_id: Any) -> str:
    console.print(
        f"[yellow]Building OpenShell sandbox image for {node_id} from {source_path}...[/yellow]"
    )
    if _openshell_gateway_uses_local_docker():
        image_ref = _build_local_docker_sandbox_image(source_path)
        print_success_confirmation(
            console,
            "OpenShell sandbox image build",
            status="ready",
            details={"Image": image_ref},
        )
        return image_ref

    result = subprocess.run(
        [
            _openshell_executable(),
            "sandbox",
            "create",
            "--from",
            str(source_path),
            "--no-tty",
            "--no-keep",
            "--",
            "true",
        ],
        capture_output=True,
        text=True,
        env=_openshell_env(),
    )
    output = f"{result.stdout}\n{result.stderr}"
    if result.returncode != 0:
        console.print(
            f"[red]Failed to build OpenShell sandbox image for {node_id}.[/red]"
        )
        if output.strip():
            console.print(output.strip())
        raise typer.Exit(1)

    matches = re.findall(r"Image\s+([^\s]+)\s+is available in the gateway", output)
    if not matches:
        console.print(
            f"[red]OpenShell did not report an image reference for {node_id}.[/red]"
        )
        if output.strip():
            console.print(output.strip())
        raise typer.Exit(1)

    image_ref = ANSI_ESCAPE_RE.sub("", matches[-1])
    print_success_confirmation(
        console,
        "OpenShell sandbox image build",
        status="ready",
        details={"Image": image_ref},
    )
    return image_ref

def _openshell_gateway_uses_local_docker() -> bool:
    gateway_name = _openshell_gateway_name()
    if not gateway_name:
        return False

    metadata = _openshell_gateway_metadata(gateway_name)
    if metadata.get("is_remote") is True:
        return False

    endpoint = metadata.get("gateway_endpoint")
    if not isinstance(endpoint, str):
        return False
    parsed = urllib.parse.urlparse(endpoint)
    return parsed.hostname in {"127.0.0.1", "localhost", "::1"}

def _build_local_docker_sandbox_image(source_path: Path) -> str:
    source_path = source_path.resolve()
    digest = hashlib.sha256(str(source_path).encode("utf-8")).hexdigest()[:12]
    image_ref = f"openshell/sandbox-from:{digest}"
    result = _run_streaming_local_docker_build(
        ["docker", "build", "--progress=plain", "-t", image_ref, str(source_path)]
    )
    if result.returncode != 0:
        output = f"{result.stdout}\n{result.stderr}".strip()
        if output:
            console.print(output)
        raise typer.Exit(1)
    return image_ref


def _run_streaming_local_docker_build(command: list[str]) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    recent_output: list[str] = []
    if process.stdout is not None:
        for line in process.stdout:
            recent_output.append(line)
            del recent_output[:-400]
            console.print(line.rstrip("\n"), highlight=False)
    return subprocess.CompletedProcess(command, process.wait(), "".join(recent_output), "")


__all__ = [name for name in globals() if not name.startswith("__")]
