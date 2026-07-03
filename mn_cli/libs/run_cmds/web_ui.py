from .common import *
from .run_state import *

def _console_web_ui_url(
    manifest_dict: dict[str, Any],
    run_dir: Optional[Path],
) -> Optional[str]:
    return (
        _console_web_ui_url_from_manifest(manifest_dict)
        or _console_web_ui_url_from_run_dir(run_dir)
    )

def _console_web_ui_url_from_manifest(manifest_dict: dict[str, Any]) -> Optional[str]:
    metadata = (
        manifest_dict.get("metadata")
        if isinstance(manifest_dict.get("metadata"), dict)
        else {}
    )
    for candidate in (
        manifest_dict.get("web_ui_service"),
        manifest_dict.get("blueprint_web_ui_service"),
        metadata.get("blueprint_web_ui_service"),
        metadata.get("web_ui_service"),
    ):
        url = _web_ui_url_from_mapping(candidate)
        if url:
            return url

    nodes = manifest_dict.get("nodes")
    if not isinstance(nodes, list):
        return None
    for node in nodes:
        if not isinstance(node, dict):
            continue
        services = node.get("services")
        if not isinstance(services, list):
            continue
        for service in services:
            if not isinstance(service, dict):
                continue
            tags = service.get("tags") if isinstance(service.get("tags"), list) else []
            if service.get("name") != "blueprint-web-ui" and "web_ui" not in tags:
                continue
            url = _web_ui_url_from_mapping(service.get("meta"))
            if url:
                return url
            port = service.get("port")
            if port:
                return f"http://localhost:{port}"
    return None

def _console_web_ui_url_from_run_dir(run_dir: Optional[Path]) -> Optional[str]:
    if run_dir is None:
        return None
    try:
        handle = json.loads((run_dir / "web_ui.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return _web_ui_url_from_mapping(handle)

def _web_ui_url_from_mapping(value: Any) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    for key in ("url", "web_ui_url", "local_url"):
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None

def _write_local_web_ui_handle(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    if not isinstance(config, dict):
        return
    web_ui = config.get("web_ui")
    if not isinstance(web_ui, dict) or web_ui.get("enabled") is False:
        return

    dashboard = (
        web_ui.get("dashboard") if isinstance(web_ui.get("dashboard"), dict) else {}
    )
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    configured = env_overrides.get("MN_RUNS_ROOT")
    runs_root = Path(configured).expanduser() if configured else default_runs_root()
    run_dir = runs_root / blueprint_run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter == "gradio":
        return

    script_path = _web_ui_registration_script(bundle_dir, web_ui, output, dashboard)
    if script_path is not None:
        _launch_blueprint_web_ui_script(
            bundle_dir,
            script_path,
            blueprint_run_id,
            run_dir,
            runs_root,
            config,
            env_overrides=env_overrides,
        )
        return

    module_name = _web_ui_registration_module(web_ui, output, dashboard)
    if module_name is not None:
        _launch_blueprint_web_ui_module(
            bundle_dir,
            module_name,
            blueprint_run_id,
            run_dir,
            runs_root,
            config,
            env_overrides=env_overrides,
        )
        return

    custom_url = _web_ui_custom_url(output)
    if custom_url:
        _write_web_ui_handle(
            run_dir,
            {
                "adapter": str(output.get("adapter") or "custom"),
                "kind": "output",
                "url": custom_url,
                "title": str(
                    output.get("title") or identity.get("name") or bundle_dir.name
                ),
                "status": "external",
                "metadata": {
                    "blueprint_id": identity.get("blueprint_id"),
                    "run_id": blueprint_run_id,
                    "registered_by": "mn_cli",
                    "customer_managed": True,
                },
            },
        )
        return

    _write_static_web_ui_handle(bundle_dir, run_dir, blueprint_run_id, config)

def _start_background_event_relay_if_needed(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    job_id: str,
    run_dir: Path,
    final_status: str,
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    if final_status in FINAL_STATUSES:
        return
    storage = _shared_storage_metadata(manifest_dict)
    has_output_copy = bool(storage.get("output_copy")) if isinstance(storage, dict) else False
    is_live_manifest = _is_live_manifest(manifest_dict)
    if not is_live_manifest and not has_output_copy:
        return
    if os.getenv("MN_RUN_BACKGROUND_EVENT_RELAY", "1").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    if not has_output_copy and (not isinstance(web_ui, dict) or web_ui.get("enabled") is False):
        return

    max_seconds = _background_event_relay_max_seconds(config)
    poll_seconds = _background_event_relay_poll_seconds(config)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "event_relay.log"
    command = [
        sys.executable,
        "-m",
        "mn_sdk.blueprint_support.event_relay",
        "--job-id",
        job_id,
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

    env = os.environ.copy()
    env["MN_RUN_EVENT_RELAY_CHILD"] = "1"
    with open(log_path, "a", encoding="utf-8") as relay_log:
        process = subprocess.Popen(
            command,
            stdout=relay_log,
            stderr=relay_log,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
            env=env,
        )

    relay_info = {
        "job_id": job_id,
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
    if is_live_manifest:
        console.print(
            "[green]Live event relay:[/green] keeping the local dashboard stream updated in the background."
        )
    elif storage_path is not None:
        console.print(
            "[green]Output event relay:[/green] will copy shared outputs when the job completes."
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
    scheduler = policies.get("scheduler", {}) if isinstance(policies.get("scheduler"), dict) else {}
    job_type = str(
        policies.get("job_type")
        or scheduler.get("job_type")
        or manifest_dict.get("job_type")
        or manifest_dict.get("type")
        or "batch"
    ).lower()
    return job_type == "service" or policies.get("stream_mode") == "live"

def _background_event_relay_poll_seconds(config: dict[str, Any]) -> float:
    raw = os.getenv("MN_RUN_EVENT_RELAY_POLL_SECONDS")
    if raw is not None:
        try:
            return max(float(raw), 0.1)
        except ValueError:
            return 1.0

    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    try:
        return max(float(output.get("refresh_seconds", 1.0)), 0.1)
    except (TypeError, ValueError):
        return 1.0

def _background_event_relay_max_seconds(config: dict[str, Any]) -> float | None:
    raw = os.getenv("MN_RUN_EVENT_RELAY_MAX_SECONDS")
    if raw is not None:
        if raw.strip().lower() in {"", "0", "none", "infinity"}:
            return None
        try:
            return max(float(raw), 0.0)
        except ValueError:
            return None

    budgets = config.get("budgets") if isinstance(config.get("budgets"), dict) else {}
    try:
        return max(float(budgets.get("max_stream_duration_seconds", 3600)), 0.0)
    except (TypeError, ValueError):
        return 3600.0

def _web_ui_registration_script(
    bundle_dir: Path,
    web_ui: dict[str, Any],
    output: dict[str, Any],
    dashboard: dict[str, Any],
) -> Path | None:
    registration = (
        web_ui.get("registration")
        if isinstance(web_ui.get("registration"), dict)
        else {}
    )
    dashboard_registration = (
        dashboard.get("registration")
        if isinstance(dashboard.get("registration"), dict)
        else {}
    )
    for raw_value in (
        output.get("launch_script"),
        output.get("registration_script"),
        registration.get("script"),
        dashboard_registration.get("script"),
    ):
        script_path = _safe_bundle_file(bundle_dir, raw_value)
        if script_path is not None:
            return script_path
    return None

def _web_ui_registration_module(
    web_ui: dict[str, Any],
    output: dict[str, Any],
    dashboard: dict[str, Any],
) -> str | None:
    registration = (
        web_ui.get("registration")
        if isinstance(web_ui.get("registration"), dict)
        else {}
    )
    dashboard_registration = (
        dashboard.get("registration")
        if isinstance(dashboard.get("registration"), dict)
        else {}
    )
    for raw_value in (
        output.get("launch_module"),
        output.get("registration_module"),
        registration.get("module"),
        dashboard_registration.get("module"),
    ):
        module_name = _safe_python_module(raw_value)
        if module_name is not None:
            return module_name

    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter == "gradio" and output.get("auto_generate", True) is not False:
        return "mn_sdk.blueprint_support.gradio_dashboard"
    return None

def _safe_python_module(raw_value: Any) -> str | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    module_name = raw_value.strip()
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", module_name):
        return module_name
    return None

def _safe_bundle_file(bundle_dir: Path, raw_value: Any) -> Path | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    relative = Path(raw_value)
    if relative.is_absolute() or ".." in relative.parts:
        return None
    root = bundle_dir.resolve()
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root) or not candidate.is_file():
        return None
    return candidate

def _launch_blueprint_web_ui_script(
    bundle_dir: Path,
    script_path: Path,
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
) -> None:
    command = [
        sys.executable,
        str(script_path),
    ]
    _launch_blueprint_web_ui_command(
        bundle_dir,
        command,
        blueprint_run_id,
        run_dir,
        runs_root,
        config,
        env_overrides=env_overrides,
        script=str(script_path),
    )

def _launch_blueprint_web_ui_module(
    bundle_dir: Path,
    module_name: str,
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
) -> None:
    command = [
        sys.executable,
        "-m",
        module_name,
    ]
    _launch_blueprint_web_ui_command(
        bundle_dir,
        command,
        blueprint_run_id,
        run_dir,
        runs_root,
        config,
        env_overrides=env_overrides,
        module=module_name,
    )

def _launch_blueprint_web_ui_command(
    bundle_dir: Path,
    command_prefix: list[str],
    blueprint_run_id: str,
    run_dir: Path,
    runs_root: Path,
    config: dict[str, Any],
    *,
    env_overrides: dict[str, str],
    script: str | None = None,
    module: str | None = None,
) -> None:
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    host = _web_ui_bind_host(output, env_overrides)
    port = _web_ui_port(output, host=host)
    base_url = _web_ui_base_url(output, host, port)

    env = os.environ.copy()
    env.update(env_overrides)
    env.update(
        {
            "MN_RUN_ID": blueprint_run_id,
            "MN_RUN_DIR": str(run_dir),
            "MN_RUNS_ROOT": str(runs_root),
            "MN_BLUEPRINT_BUNDLE_DIR": str(bundle_dir),
            "MN_BLUEPRINT_CONFIG_JSON": json.dumps(config, sort_keys=True),
            "MN_BLUEPRINT_ID": str(identity.get("blueprint_id") or bundle_dir.name),
            "MN_BLUEPRINT_WEB_UI_HOST": host,
            "MN_BLUEPRINT_WEB_UI_PORT": str(port),
            "MN_BLUEPRINT_WEB_UI_BASE_URL": base_url,
        }
    )
    command = command_prefix + [
        "--run-id",
        blueprint_run_id,
        "--run-dir",
        str(run_dir),
        "--runs-root",
        str(runs_root),
        "--bundle-dir",
        str(bundle_dir),
        "--host",
        host,
        "--port",
        str(port),
        "--base-url",
        base_url,
    ]
    log_path = run_dir / "web_ui.log"
    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                cwd=bundle_dir,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
        process_info = {
            "pid": process.pid,
            "command": command,
            "log": str(log_path),
            "blueprint_id": identity.get("blueprint_id"),
            "run_id": blueprint_run_id,
            "url": base_url,
        }
        if script:
            process_info["script"] = script
        if module:
            process_info["module"] = module
        (run_dir / "web_ui_process.json").write_text(
            json.dumps(process_info, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        _wait_for_blueprint_web_ui(run_dir, process)
    except OSError:
        logger.exception(
            "Failed to launch blueprint web UI for run_id=%s", blueprint_run_id
        )

def _web_ui_bind_host(output: dict[str, Any], env_overrides: dict[str, str]) -> str:
    for value in (
        env_overrides.get("MN_BLUEPRINT_WEB_UI_BIND_HOST"),
        env_overrides.get("MN_BLUEPRINT_WEB_UI_HOST"),
        os.getenv("MN_BLUEPRINT_WEB_UI_BIND_HOST"),
        os.getenv("MN_BLUEPRINT_WEB_UI_HOST"),
        output.get("host"),
    ):
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "127.0.0.1"

def _parse_web_ui_port(raw_value: Any, *, name: str) -> int | None:
    if raw_value in (None, ""):
        return None
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        raise RuntimeError(f"{name} must be a positive integer.") from None
    if parsed <= 0 or parsed > 65535:
        raise RuntimeError(f"{name} must be between 1 and 65535.")
    return parsed

def _web_ui_port_available(host: str, port: int) -> bool:
    bind_host = host if host and host not in {"::"} else "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((bind_host, port))
        except OSError:
            return False
    return True

def _web_ui_port_range_configured() -> bool:
    return (
        os.getenv("MN_BLUEPRINT_WEB_UI_PORT_START") not in (None, "")
        or os.getenv("MN_BLUEPRINT_WEB_UI_PORT_END") not in (None, "")
    )

def _web_ui_port_range() -> tuple[int, int]:
    start = _parse_web_ui_port(
        os.getenv(
            "MN_BLUEPRINT_WEB_UI_PORT_START", str(DEFAULT_BLUEPRINT_WEB_UI_PORT_START)
        ),
        name="MN_BLUEPRINT_WEB_UI_PORT_START",
    )
    end = _parse_web_ui_port(
        os.getenv(
            "MN_BLUEPRINT_WEB_UI_PORT_END", str(DEFAULT_BLUEPRINT_WEB_UI_PORT_END)
        ),
        name="MN_BLUEPRINT_WEB_UI_PORT_END",
    )
    assert start is not None and end is not None
    if end < start:
        raise RuntimeError(
            "MN_BLUEPRINT_WEB_UI_PORT_END must be greater than or equal to MN_BLUEPRINT_WEB_UI_PORT_START."
        )
    return start, end

def _ephemeral_web_ui_port(host: str) -> int:
    bind_host = host if host and host not in {"::"} else "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((bind_host, 0))
        return int(sock.getsockname()[1])

def _web_ui_port(output: dict[str, Any], *, host: str = "127.0.0.1") -> int:
    raw_port = output.get("port")
    explicit_port = _parse_web_ui_port(raw_port, name="web_ui.output.port")
    if explicit_port is not None:
        if not _web_ui_port_available(host, explicit_port):
            raise RuntimeError(
                f"Blueprint web UI port {explicit_port} is unavailable on {host}."
            )
        return explicit_port

    start, end = _web_ui_port_range()
    for port in range(start, end + 1):
        if _web_ui_port_available(host, port):
            return port
    if not _web_ui_port_range_configured():
        return _ephemeral_web_ui_port(host)
    raise RuntimeError(f"No available blueprint web UI port found in {start}-{end}.")

def _web_ui_base_url(output: dict[str, Any], host: str, port: int) -> str:
    configured_url = output.get("base_url") or os.getenv("MN_BLUEPRINT_WEB_UI_BASE_URL")
    if isinstance(configured_url, str) and configured_url.strip():
        return configured_url.rstrip("/")
    public_host = output.get("public_host") or os.getenv(
        "MN_BLUEPRINT_WEB_UI_PUBLIC_HOST"
    )
    if not isinstance(public_host, str) or not public_host.strip():
        public_host = "localhost" if host in {"127.0.0.1", "0.0.0.0", "::"} else host
    scheme = str(output.get("scheme") or "http")
    return f"{scheme}://{public_host}:{port}"

def _wait_for_blueprint_web_ui(run_dir: Path, process: subprocess.Popen[Any]) -> None:
    try:
        timeout = max(
            float(os.getenv("MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", "5")), 0
        )
    except ValueError:
        timeout = 5.0
    deadline = time.monotonic() + timeout
    handle_path = run_dir / "web_ui.json"
    while time.monotonic() < deadline:
        if handle_path.exists():
            return
        poll = getattr(process, "poll", None)
        if callable(poll) and poll() is not None:
            return
        time.sleep(0.1)

def _web_ui_custom_url(output: dict[str, Any]) -> str:
    adapter = str(output.get("adapter") or "").lower()
    if adapter != "custom":
        return ""
    url = output.get("custom_url") or output.get("url")
    return str(url).strip() if isinstance(url, str) else ""

def _write_static_web_ui_handle(
    bundle_dir: Path,
    run_dir: Path,
    blueprint_run_id: str,
    config: dict[str, Any],
) -> None:
    web_ui = config.get("web_ui") if isinstance(config.get("web_ui"), dict) else {}
    dashboard = (
        web_ui.get("dashboard") if isinstance(web_ui.get("dashboard"), dict) else {}
    )
    output = web_ui.get("output") if isinstance(web_ui.get("output"), dict) else {}
    identity = (
        config.get("identity") if isinstance(config.get("identity"), dict) else {}
    )
    adapter = str(output.get("adapter") or web_ui.get("kind") or "").lower()
    if adapter not in {"static_html", "html"} and not (
        output.get("path") or dashboard.get("path") or web_ui.get("path")
    ):
        return

    html_path = _safe_bundle_file(
        bundle_dir, output.get("path") or dashboard.get("path") or web_ui.get("path")
    )
    if html_path is None:
        return

    events_path = run_dir / "events.jsonl"
    query: dict[str, str] = {}
    video_source = _web_ui_video_source(config, bundle_dir)
    video_query_param = str(output.get("video_source_query_param") or "video")
    events_query_param = str(output.get("events_query_param") or "events")
    if video_source:
        query[video_query_param] = video_source
    query[events_query_param] = events_path.resolve().as_uri()
    url = html_path.resolve().as_uri()
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"

    _write_web_ui_handle(
        run_dir,
        {
            "adapter": "static_html",
            "kind": "output",
            "url": url,
            "title": str(
                output.get("title") or identity.get("name") or bundle_dir.name
            ),
            "path": str(html_path),
            "status": "available",
            "metadata": {
                "blueprint_id": identity.get("blueprint_id"),
                "run_id": blueprint_run_id,
                "events_path": str(events_path),
                "registered_by": "mn_cli",
                "launch_adapter": "blueprint_static_html",
            },
        },
    )

def _write_web_ui_handle(run_dir: Path, handle: dict[str, Any]) -> None:
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "web_ui.json").write_text(
            json.dumps(handle, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    except OSError:
        logger.exception(
            "Failed to write blueprint web UI handle for run_dir=%s", run_dir
        )

def _web_ui_video_source(config: dict[str, Any], bundle_dir: Path) -> str:
    video_source = (
        (config.get("video_source") or {})
        if isinstance(config.get("video_source"), dict)
        else {}
    ).get("uri")
    if not isinstance(video_source, str) or not video_source:
        dashboard = (
            (config.get("web_ui") or {}).get("dashboard")
            if isinstance(config.get("web_ui"), dict)
            else {}
        )
        video_source = (dashboard or {}).get("default_video_source") or ""
    if not isinstance(video_source, str) or not video_source:
        return ""
    if "://" in video_source:
        return video_source
    for candidate in (
        bundle_dir / "payloads" / "person_detector" / video_source,
        bundle_dir / "payloads" / "web_ui" / video_source,
        bundle_dir / video_source,
    ):
        if candidate.is_file():
            return candidate.resolve().as_uri()
    return video_source


__all__ = [name for name in globals() if not name.startswith("__")]
