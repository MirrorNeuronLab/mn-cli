from .common import *
from .run_state import *
from mn_sdk import job_data_dir_from_id
from urllib.parse import quote, urlparse


def _console_web_ui_url(
    manifest_dict: dict[str, Any],
    job_id: str,
) -> Optional[str]:
    registered_url = _console_web_ui_url_from_job_data(job_id)
    if registered_url:
        return _local_job_web_ui_url(job_id) or registered_url
    declared_url = _console_web_ui_url_from_manifest(manifest_dict)
    if declared_url:
        return declared_url
    if _declares_job_scoped_web_ui(manifest_dict):
        return _local_job_web_ui_url(job_id)
    return None


def _register_manifest_web_ui_handle(
    manifest_dict: dict[str, Any],
    job_id: str,
    *,
    configuration: dict[str, Any] | None = None,
) -> Optional[str]:
    """Persist a job-scoped UI handle for a manifest-declared service UI.

    Docker Compose services are launched by the selected node and do not run a
    Python worker that can write ``web_ui.json`` itself.  Register their public
    endpoint here so the local MirrorNeuron Web UI can use its standard
    ``/jobs/<job_id>/ui`` entry point for both local and remote Compose jobs.
    """

    service = _manifest_web_ui_service(manifest_dict)
    if service is None:
        return None
    url = _web_ui_url_from_configuration(configuration) or _web_ui_url_from_service(service)
    if not url:
        return None

    job_data_dir = job_data_dir_from_id(job_id, must_exist=False)
    if job_data_dir is None:
        return None
    job_data_dir.mkdir(parents=True, exist_ok=True)

    metadata = manifest_dict.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    web_ui_metadata = metadata.get("web_ui")
    web_ui_metadata = web_ui_metadata if isinstance(web_ui_metadata, dict) else {}
    title = str(
        web_ui_metadata.get("title")
        or (service.get("meta") or {}).get("title")
        or service.get("name")
        or "Blueprint Web UI"
    ).strip()
    service_name = str(service.get("name") or "").strip()
    node_id = str(web_ui_metadata.get("node_id") or "").strip()
    handle_metadata = {
        "source": "manifest_service",
        **({"service_name": service_name} if service_name else {}),
        **({"node_id": node_id} if node_id else {}),
        "proxy": _web_ui_proxy_policy(manifest_dict, url),
    }
    ui = {
        "schema_version": "mn.web_ui.external.v1",
        "renderer": "external-url",
        "job_id": job_id,
        "title": title,
        "metadata": handle_metadata,
    }
    web_ui = {
        "kind": "service",
        "adapter": "external-url",
        "status": "running",
        "title": title,
        "url": url,
        "job_id": job_id,
        "metadata": handle_metadata,
    }
    _write_json_atomically(job_data_dir / "ui.json", ui)
    _write_json_atomically(job_data_dir / "web_ui.json", web_ui)
    return url


def _console_web_ui_url_from_manifest(
    manifest_dict: dict[str, Any],
) -> Optional[str]:
    service = _manifest_web_ui_service(manifest_dict)
    return _web_ui_url_from_service(service) if service is not None else None


def _declares_job_scoped_web_ui(manifest_dict: dict[str, Any]) -> bool:
    """Whether a worker will register a deferred job-scoped UI handle."""

    metadata = manifest_dict.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    web_ui = metadata.get("web_ui")
    return isinstance(web_ui, dict) and web_ui.get("enabled") is True


def _manifest_web_ui_service(manifest_dict: dict[str, Any]) -> Optional[dict[str, Any]]:
    for service in _manifest_services(manifest_dict):
        tags = {
            str(tag).strip().lower()
            for tag in service.get("tags", [])
            if isinstance(tag, str)
        }
        if "web_ui" in tags:
            return service
    return None


def _manifest_services(manifest_dict: dict[str, Any]) -> list[dict[str, Any]]:
    groups: list[Any] = [
        (manifest_dict.get("agents") or {}).get("nodes"),
        (manifest_dict.get("agents") or {}).get("extra_nodes"),
        (manifest_dict.get("flow") or {}).get("nodes"),
    ]
    services: list[dict[str, Any]] = []
    for nodes in groups:
        if not isinstance(nodes, list):
            continue
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_services = node.get("services")
            if not isinstance(node_services, list):
                continue
            for service in node_services:
                if not isinstance(service, dict):
                    continue
                services.append(service)
    return services


def _console_web_ui_url_from_job_data(job_id: str) -> Optional[str]:
    job_data_dir = job_data_dir_from_id(job_id)
    if job_data_dir is None:
        return None
    try:
        handle = json.loads((job_data_dir / "web_ui.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if handle.get("job_id") != job_id:
        return None
    return _web_ui_url_from_mapping(handle)


def _web_ui_url_from_mapping(value: Any) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    for key in ("url", "web_ui_url", "local_url"):
        candidate = value.get(key)
        if isinstance(candidate, str) and _usable_http_url(candidate):
            return candidate.strip()
    return None


def _web_ui_url_from_service(service: dict[str, Any]) -> Optional[str]:
    url = _web_ui_url_from_mapping(service.get("meta"))
    if url:
        return url
    host = str(service.get("address") or "").strip()
    port = service.get("port")
    if host and _usable_host(host) and _usable_port(port):
        return _http_url(host, int(str(port).strip()))
    if _usable_port(port):
        return f"http://localhost:{int(str(port).strip())}"
    return None


def _web_ui_url_from_configuration(configuration: dict[str, Any] | None) -> Optional[str]:
    web_ui = configuration.get("web_ui") if isinstance(configuration, dict) else None
    if not isinstance(web_ui, dict) or web_ui.get("enabled") is False:
        return None
    service = web_ui.get("service")
    if not isinstance(service, dict):
        return None
    host = str(
        service.get("advertise_host") or service.get("address") or service.get("host") or ""
    ).strip()
    port = service.get("port")
    if not _usable_host(host) or not _usable_port(port):
        return None
    return _http_url(host, int(str(port).strip()))


def _local_job_web_ui_url(job_id: str) -> Optional[str]:
    runtime = RuntimeConfig.from_env()
    base_url = str(runtime.web_ui_url or "").strip().rstrip("/")
    if not runtime.web_ui_advertised or not _usable_http_url(base_url):
        return None
    return f"{base_url}/jobs/{quote(job_id, safe='-._')}/ui"


def _usable_http_url(value: str) -> bool:
    candidate = str(value or "").strip()
    if not candidate or "${" in candidate or any(char in candidate for char in "<>{}"):
        return False
    parsed = urlparse(candidate)
    try:
        parsed.port
        return parsed.scheme in {"http", "https"} and bool(parsed.hostname)
    except ValueError:
        return False


def _usable_host(value: str) -> bool:
    candidate = str(value or "").strip()
    return (
        bool(candidate)
        and "${" not in candidate
        and not any(char in candidate for char in "<>{}")
        and candidate not in {"0.0.0.0", "::"}
    )


def _usable_port(value: Any) -> bool:
    try:
        return 1 <= int(str(value).strip()) <= 65535
    except (TypeError, ValueError):
        return False


def _http_url(host: str, port: int) -> str:
    display_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return f"http://{display_host}:{port}"


def _write_json_atomically(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _web_ui_proxy_policy(manifest_dict: dict[str, Any], url: str) -> dict[str, Any]:
    """Return the explicitly declared ports that the local dashboard may proxy.

    The browser never supplies a remote host.  The local Web UI server uses
    this policy with the service URL above to proxy only the dashboard itself
    and its explicitly tagged HTTP/WebSocket companions.
    """

    parsed = urlparse(url)
    try:
        primary_port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError:
        return {
            "schema_version": "mn.web_ui.proxy.v1",
            "http_ports": [],
            "websocket_ports": [],
        }
    http_ports = {primary_port}
    websocket_ports: set[int] = set()
    for service in _manifest_services(manifest_dict):
        port = service.get("port")
        if not _usable_port(port) or not _service_can_share_web_ui_host(service, parsed.hostname or ""):
            continue
        tags = {
            str(tag).strip().lower()
            for tag in service.get("tags", [])
            if isinstance(tag, str)
        }
        if "websocket" in tags:
            websocket_ports.add(int(str(port).strip()))
        if tags & {"web_ui", "web_ui_proxy", "video"}:
            http_ports.add(int(str(port).strip()))
    return {
        "schema_version": "mn.web_ui.proxy.v1",
        "http_ports": sorted(http_ports),
        "websocket_ports": sorted(websocket_ports),
    }


def _service_can_share_web_ui_host(service: dict[str, Any], web_ui_host: str) -> bool:
    address = str(service.get("address") or "").strip()
    if not address or "${" in address:
        return True
    return address.strip("[]").lower() == web_ui_host.strip("[]").lower()


def _start_background_event_relay_if_needed(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    run_id: str,
    run_dir: Path,
    final_status: str,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
    submission_metadata: Optional[dict[str, Any]] = None,
) -> None:
    if final_status in FINAL_STATUSES:
        return
    storage = (
        dict(submission_metadata)
        if isinstance(submission_metadata, dict)
        and submission_metadata.get("output_copy")
        else _shared_storage_metadata(manifest_dict)
    )
    has_output_copy = bool(storage.get("output_copy")) if isinstance(storage, dict) else False
    if not _is_live_manifest(manifest_dict) and not has_output_copy:
        return
    if os.getenv("MN_RUN_BACKGROUND_EVENT_RELAY", "1").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    max_seconds = _background_event_relay_max_seconds(config)
    poll_seconds = _background_event_relay_poll_seconds()
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "event_relay.log"
    command = [
        sys.executable,
        "-m",
        "mn_sdk.blueprint_support.event_relay",
        "--run-id",
        run_id,
        "--run-dir",
        str(run_dir),
        "--poll-seconds",
        f"{poll_seconds:g}",
    ]
    storage_path: Path | None = None
    if storage:
        storage_path = run_dir / "shared_storage.json"
        storage_path.write_text(
            json.dumps(storage, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        command.extend(["--shared-storage-json", str(storage_path)])
    if max_seconds is not None:
        command.extend(["--max-seconds", f"{max_seconds:g}"])

    with log_path.open("a", encoding="utf-8") as relay_log:
        process = subprocess.Popen(
            command,
            stdout=relay_log,
            stderr=relay_log,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
            env=os.environ.copy(),
        )
    relay_info = {
        "run_id": run_id,
        "pid": process.pid,
        "poll_seconds": poll_seconds,
        "max_seconds": max_seconds,
        "log_path": str(log_path),
        "shared_storage_path": str(storage_path) if storage_path is not None else None,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "event_relay.json").write_text(
        json.dumps(relay_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    console.print(
        f"[green]Output event relay started[/green] (pid {process.pid})"
    )


def _shared_storage_metadata(manifest_dict: dict[str, Any]) -> dict[str, Any]:
    metadata = (
        manifest_dict.get("metadata")
        if isinstance(manifest_dict.get("metadata"), dict)
        else {}
    )
    storage = metadata.get("mn_storage") if isinstance(metadata, dict) else None
    return storage if isinstance(storage, dict) else {}


def _is_live_manifest(manifest_dict: dict[str, Any]) -> bool:
    policies = (
        manifest_dict.get("policies")
        if isinstance(manifest_dict.get("policies"), dict)
        else {}
    )
    scheduler = (
        policies.get("scheduler")
        if isinstance(policies.get("scheduler"), dict)
        else {}
    )
    job_type = str(
        policies.get("job_type")
        or scheduler.get("job_type")
        or manifest_dict.get("job_type")
        or manifest_dict.get("type")
        or "batch"
    ).lower()
    return job_type == "service" or policies.get("stream_mode") == "live"


def _background_event_relay_poll_seconds() -> float:
    raw = os.getenv("MN_RUN_EVENT_RELAY_POLL_SECONDS")
    if raw is None:
        return 1.0
    try:
        return max(float(raw), 0.1)
    except ValueError:
        return 1.0


def _background_event_relay_max_seconds(
    _config: dict[str, Any],
) -> float | None:
    raw = os.getenv("MN_RUN_EVENT_RELAY_MAX_SECONDS")
    if raw is not None:
        if raw.strip().lower() in {"", "0", "none", "infinity"}:
            return None
        try:
            return max(float(raw), 0.0)
        except ValueError:
            return None
    return None
