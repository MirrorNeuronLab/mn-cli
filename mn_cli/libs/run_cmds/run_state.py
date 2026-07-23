from .common import *
from .openshell import *

def _ensure_local_run_store_identity(
    bundle_dir: Path,
    manifest_dict: dict[str, Any],
    env_overrides: dict[str, str],
    submission_metadata: dict[str, Any],
    *,
    config_overrides: Optional[dict[str, Any]] = None,
) -> None:
    if submission_metadata.get("blueprint_run_id") or env_overrides.get("MN_RUN_ID"):
        return

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides) or {}
    identity = config.get("identity") if isinstance(config, dict) else {}
    identity = identity if isinstance(identity, dict) else {}
    metadata = (
        manifest_dict.get("metadata")
        if isinstance(manifest_dict.get("metadata"), dict)
        else {}
    )
    workflow_manifest = _is_workflow_manifest(manifest_dict)
    blueprint_id = (
        identity.get("blueprint_id")
        or metadata.get("blueprint_id")
        or manifest_dict.get("id")
        or (None if workflow_manifest else manifest_dict.get("graph_id"))
        or bundle_dir.name
    )
    run_id = identity.get("run_id") or _make_blueprint_run_id(str(blueprint_id))
    env_overrides["MN_RUN_ID"] = str(run_id)
    submission_metadata.setdefault("blueprint_id", str(blueprint_id))
    submission_metadata["blueprint_run_id"] = str(run_id)

def _write_blueprint_job_mapping(
    blueprint_run_id: str,
    job_id: str,
    run_id: str,
    metadata: dict[str, Any],
    env_overrides: dict[str, str],
    *,
    monitor_manifest: dict[str, Any] | None = None,
) -> None:
    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": run_id,
            "job_id": job_id,
            "blueprint_run_id": blueprint_run_id,
            "blueprint_id": metadata.get("blueprint_id"),
            "blueprint_revision": metadata.get("blueprint_revision"),
            "blueprint_source": metadata.get("blueprint_source"),
            "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        tmp = run_dir / f".job.json.{os.getpid()}.tmp"
        tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        tmp.replace(run_dir / "job.json")
        if isinstance(monitor_manifest, dict) and monitor_manifest:
            monitor_contract = _monitor_manifest_contract(monitor_manifest)
            manifest_tmp = run_dir / f".manifest.json.{os.getpid()}.tmp"
            manifest_tmp.write_text(
                json.dumps(monitor_contract, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            manifest_tmp.replace(run_dir / "manifest.json")
    except OSError:
        logger.exception(
            "Failed to write blueprint job mapping for run_id=%s job_id=%s",
            run_id,
            job_id,
        )


def _monitor_manifest_contract(manifest: dict[str, Any]) -> dict[str, Any]:
    """Persist only source-facing monitor fields, never runtime environment."""

    contract = {
        key: manifest[key]
        for key in (
            "apiVersion",
            "kind",
            "id",
            "name",
            "description",
            "graph_id",
            "job_name",
            "job_type",
            "type",
            "policies",
        )
        if key in manifest
    }
    metadata = manifest.get("metadata")
    if isinstance(metadata, dict):
        safe_metadata = {
            key: metadata[key]
            for key in ("blueprint_id", "name", "description")
            if key in metadata
        }
        if safe_metadata:
            contract["metadata"] = safe_metadata

    workflow = manifest.get("workflow")
    if isinstance(workflow, dict):
        safe_workflow = {
            key: workflow[key]
            for key in (
                "workflow_id",
                "name",
                "description",
                "entrypoint",
                "kind",
                "execution",
            )
            if key in workflow
        }
        steps = workflow.get("steps")
        if isinstance(steps, list):
            safe_workflow["steps"] = [
                {
                    key: step[key]
                    for key in (
                        "id",
                        "label",
                        "goal",
                        "action",
                        "run",
                        "emits",
                        "on",
                        "needs",
                        "kind",
                        "live",
                        "requires",
                        "provides",
                        "agent_id",
                        "start_agent_id",
                        "end_agent_id",
                        "agent_ids",
                    )
                    if key in step
                }
                for step in steps
                if isinstance(step, dict)
            ]
        contract["workflow"] = safe_workflow

    runtime = manifest.get("runtime")
    bindings = runtime.get("bindings") if isinstance(runtime, dict) else None
    if isinstance(bindings, dict):
        contract["runtime"] = {
            "bindings": {
                str(binding_id): _monitor_binding_contract(binding)
                for binding_id, binding in bindings.items()
                if isinstance(binding, dict)
            }
        }

    nodes = manifest.get("nodes")
    if isinstance(nodes, list):
        contract["nodes"] = [
            _monitor_worker_contract(node) for node in nodes if isinstance(node, dict)
        ]
    return contract


def _monitor_binding_contract(binding: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: binding[key]
        for key in (
            "id",
            "node_id",
            "type",
            "kind",
            "strategy",
            "role",
            "working_on",
            "model",
            "uses",
            "live",
            "alias",
            "display_name",
            "label",
            "name",
            "tools",
            "tokens",
            "token_budget",
        )
        if key in binding
    }
    worker = binding.get("worker")
    if isinstance(worker, dict):
        result["worker"] = _monitor_worker_contract(worker)
    workers = binding.get("workers")
    if isinstance(workers, list):
        result["workers"] = [
            _monitor_worker_contract(item)
            if isinstance(item, dict)
            else {"id": str(item)}
            for item in workers
        ]
    return result


def _monitor_worker_contract(worker: dict[str, Any]) -> dict[str, Any]:
    return {
        key: worker[key]
        for key in (
            "id",
            "node_id",
            "agent_type",
            "type",
            "role",
            "working_on",
            "model",
            "uses",
            "live",
            "alias",
            "display_name",
            "label",
            "name",
            "tools",
            "tokens",
            "token_budget",
        )
        if key in worker
    }


def _blueprint_run_dir(blueprint_run_id: str, env_overrides: dict[str, str]) -> Path:
    configured = env_overrides.get("MN_RUNS_ROOT")
    runs_root = Path(configured).expanduser() if configured else default_runs_root()
    return runs_root / blueprint_run_id

def _register_post_launch_hook(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
) -> None:
    script_path = (bundle_dir / POST_LAUNCH_SCRIPT).resolve()
    if not script_path.is_file():
        return

    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "post_launch.log"
    ready_file = run_dir / "pre_launch.ready"
    state_file = run_dir / "post_launch_state.json"
    hook_info = {
        "command": ["bash", str(script_path)],
        "script": str(script_path),
        "cwd": str(bundle_dir),
        "log": str(log_path),
        "run_id": blueprint_run_id,
        "bundle_dir": str(bundle_dir),
        "state_file": str(state_file),
        "pre_launch_ready_file": str(ready_file),
        "pre_launch_process_file": str(run_dir / "pre_launch_process.json"),
        "registered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "post_launch_hook.json").write_text(
        json.dumps(hook_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

def _start_pre_launch_hook(
    bundle_dir: Path,
    blueprint_run_id: str,
    *,
    env_overrides: dict[str, str],
    config_overrides: Optional[dict[str, Any]] = None,
) -> subprocess.Popen[Any] | None:
    script_path = (bundle_dir / PRE_LAUNCH_SCRIPT).resolve()
    if not script_path.is_file():
        return None

    run_dir = _blueprint_run_dir(blueprint_run_id, env_overrides)
    runs_root = run_dir.parent
    run_dir.mkdir(parents=True, exist_ok=True)
    ready_file = run_dir / "pre_launch.ready"
    try:
        ready_file.unlink()
    except FileNotFoundError:
        pass

    config = load_blueprint_config(bundle_dir, config_overrides=config_overrides)
    config = _with_shared_run_store_config(config, blueprint_run_id, str(runs_root))
    runtime_env = _blueprint_runtime_environment(
        bundle_dir,
        config=config,
        config_overrides=config_overrides,
    )

    env = _openshell_env()
    env.update(runtime_env)
    env.update(
        {
            key: str(value)
            for key, value in (env_overrides or {}).items()
            if value is not None
        }
    )
    env.update(
        {
            "MN_RUN_ID": blueprint_run_id,
            "MN_RUN_DIR": str(run_dir),
            "MN_RUNS_ROOT": str(runs_root),
            "MN_BLUEPRINT_BUNDLE_DIR": str(bundle_dir),
            "MN_BLUEPRINT_CONFIG_JSON": json.dumps(config, sort_keys=True),
            "MN_PRE_LAUNCH_READY_FILE": str(ready_file),
            "MN_POST_LAUNCH_STATE_FILE": str(run_dir / "post_launch_state.json"),
        }
    )

    command = ["bash", str(script_path)]
    log_path = run_dir / "pre_launch.log"
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
    except OSError as exc:
        raise RuntimeError(f"Failed to start blueprint pre-launch hook: {exc}") from exc

    process_info = {
        "pid": process.pid,
        "process_group_id": process.pid,
        "command": command,
        "script": str(script_path),
        "log": str(log_path),
        "ready_file": str(ready_file),
        "run_id": blueprint_run_id,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (run_dir / "pre_launch_process.json").write_text(
        json.dumps(process_info, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    try:
        _wait_for_pre_launch_ready(run_dir, process, ready_file)
        _apply_pre_launch_ready_metadata(
            ready_file,
            env_overrides=env_overrides,
            config_overrides=config_overrides,
        )
    except Exception:
        _terminate_pre_launch_process(process, reason="pre_launch_failed")
        raise

    console.print(
        "[green]Blueprint pre-launch hook ready:[/green] scripts/pre-launch.sh"
    )
    return process

def _wait_for_pre_launch_ready(
    run_dir: Path, process: subprocess.Popen[Any], ready_file: Path
) -> None:
    try:
        timeout = max(float(os.getenv("MN_PRE_LAUNCH_TIMEOUT_SECONDS", "30")), 0)
    except ValueError:
        timeout = 30.0
    deadline = time.monotonic() + timeout
    while time.monotonic() <= deadline:
        if ready_file.exists():
            return
        poll = getattr(process, "poll", None)
        if callable(poll) and poll() is not None:
            raise RuntimeError(
                f"Blueprint pre-launch hook exited before becoming ready. See {run_dir / 'pre_launch.log'}."
            )
        time.sleep(0.1)
    raise RuntimeError(
        f"Blueprint pre-launch hook timed out after {timeout:g}s. See {run_dir / 'pre_launch.log'}."
    )

def _apply_pre_launch_ready_metadata(
    ready_file: Path,
    *,
    env_overrides: dict[str, str],
    config_overrides: dict[str, Any],
) -> None:
    raw = ready_file.read_text(encoding="utf-8").strip()
    if not raw or raw == "ready":
        return
    try:
        metadata = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(
            "Ignoring non-JSON pre-launch ready metadata from %s", ready_file
        )
        return
    if not isinstance(metadata, dict):
        return
    env_patch = metadata.get("env")
    if isinstance(env_patch, dict):
        env_overrides.update(
            {
                str(key): str(value)
                for key, value in env_patch.items()
                if value is not None
            }
        )
    config_patch = metadata.get("config") or metadata.get("config_overrides")
    if isinstance(config_patch, dict):
        merged = _deep_merge_dict(config_overrides, config_patch)
        config_overrides.clear()
        config_overrides.update(merged)

def _terminate_pre_launch_process(
    process: subprocess.Popen[Any] | None, *, reason: str
) -> None:
    if process is None:
        return
    poll = getattr(process, "poll", None)
    if callable(poll) and poll() is not None:
        return
    pid = getattr(process, "pid", None)
    if not isinstance(pid, int):
        try:
            process.terminate()
        except OSError:
            pass
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except OSError:
        try:
            process.terminate()
        except OSError:
            pass
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pid, signal.SIGKILL)
        except OSError:
            try:
                process.kill()
            except OSError:
                pass
    logger.info("Stopped blueprint pre-launch hook pid=%s reason=%s", pid, reason)

def _cleanup_pre_launch_artifacts(
    process: subprocess.Popen[Any] | None,
    run_dir: Path | None,
    *,
    reason: str,
) -> None:
    _terminate_pre_launch_process(process, reason=reason)
    if run_dir is not None:
        cleanup_blueprint_host_hooks(
            run_dir,
            dry_run=False,
            summary={"process_removed": [], "process_skipped": [], "errors": []},
            reason=reason,
        )


__all__ = [name for name in globals() if not name.startswith("__")]
