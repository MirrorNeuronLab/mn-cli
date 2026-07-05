from .common import *
from mn_cli.libs.run_manifest import (
    _ensure_docker_worker_requirements_install,
    _requirements_text,
)

def _prepare_openshell_custom_images(
    bundle_dir: Path, manifest_dict: dict[str, Any]
) -> None:
    nodes = manifest_dict.get("nodes")
    if not isinstance(nodes, list):
        return

    for node in nodes:
        if not isinstance(node, dict):
            continue
        config = node.get("config")
        if not isinstance(config, dict):
            continue
        if config.get("runner_module") != "MirrorNeuron.Sandbox.OpenShell":
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
            continue

        build_source = _openshell_skill_dependency_context(source_path, manifest_dict)
        try:
            config["from"] = _build_openshell_from_image(
                build_source, node.get("node_id") or "openshell"
            )
        finally:
            if build_source != source_path:
                shutil.rmtree(build_source, ignore_errors=True)

def _openshell_gateway_endpoint() -> str:
    configured_endpoint = os.getenv("OPENSHELL_GATEWAY_ENDPOINT")
    if configured_endpoint:
        return configured_endpoint

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

    gateway_name = _openshell_gateway_name(env=env)
    if gateway_name:
        env.setdefault("OPENSHELL_GATEWAY", gateway_name)
    else:
        env.setdefault("OPENSHELL_GATEWAY_ENDPOINT", _openshell_gateway_endpoint())
    return env

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

def _openshell_skill_dependency_context(source_path: Path, manifest: dict[str, Any]) -> Path:
    requirements_text = gar_requirements_text(manifest)
    if not requirements_text:
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
    dockerfile.write_text(
        _ensure_docker_worker_requirements_install(
            dockerfile.read_text(encoding="utf-8"),
            local_context_sources=[],
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
            "openshell",
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
    result = subprocess.run(
        ["docker", "build", "-t", image_ref, str(source_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        output = f"{result.stdout}\n{result.stderr}".strip()
        if output:
            console.print(output)
        raise typer.Exit(1)
    return image_ref


__all__ = [name for name in globals() if not name.startswith("__")]
