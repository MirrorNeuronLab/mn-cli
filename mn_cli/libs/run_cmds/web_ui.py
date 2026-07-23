from .common import *
from .run_state import *


def _console_web_ui_url(
    manifest_dict: dict[str, Any],
    run_dir: Optional[Path],
) -> Optional[str]:
    return _console_web_ui_url_from_run_dir(run_dir) or _console_web_ui_url_from_manifest(
        manifest_dict
    )


def _console_web_ui_url_from_manifest(
    manifest_dict: dict[str, Any],
) -> Optional[str]:
    groups: list[Any] = [
        manifest_dict.get("nodes"),
        (manifest_dict.get("agents") or {}).get("nodes"),
        (manifest_dict.get("agents") or {}).get("extra_nodes"),
        (manifest_dict.get("flow") or {}).get("nodes"),
    ]
    for nodes in groups:
        if not isinstance(nodes, list):
            continue
        for node in nodes:
            if not isinstance(node, dict):
                continue
            services = node.get("services")
            if not isinstance(services, list):
                continue
            for service in services:
                if not isinstance(service, dict):
                    continue
                tags = {
                    str(tag).strip().lower()
                    for tag in service.get("tags", [])
                    if isinstance(tag, str)
                }
                if "web_ui" not in tags:
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


def _start_background_event_relay_if_needed(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    job_id: str,
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
    config: dict[str, Any],
) -> float | None:
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
