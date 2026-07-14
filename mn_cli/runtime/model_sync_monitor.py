from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

FALSE_VALUES = {"0", "false", "no", "off"}
DEFAULT_INTERVAL_SECONDS = 15.0
DEFAULT_RETRY_MIN_SECONDS = 1.0
DEFAULT_RETRY_MAX_SECONDS = 30.0
DEFAULT_NODE_MISSING_GRACE_SECONDS = 90.0


def cluster_model_monitor_enabled(env: Mapping[str, str] | None = None) -> bool:
    values = os.environ if env is None else env
    return (
        str(values.get("MN_CLUSTER_MODEL_MONITOR_ENABLED", "true")).strip().lower()
        not in FALSE_VALUES
    )


def cluster_model_monitor_state_path(
    env: Mapping[str, str] | None = None,
) -> Path:
    values = os.environ if env is None else env
    configured = str(values.get("MN_CLUSTER_MODEL_MONITOR_STATE_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    mn_home = Path(values.get("MN_HOME") or Path.home() / ".mn").expanduser()
    return mn_home / "models" / "cluster-model-monitor.json"


def run_cluster_model_monitor(
    stop_event: threading.Event,
    *,
    reconcile: Callable[..., dict[str, Any]] | None = None,
    env: Mapping[str, str] | None = None,
    now: Callable[[], float] = time.time,
) -> None:
    values = dict(os.environ if env is None else env)
    if not cluster_model_monitor_enabled(values):
        return
    if reconcile is None:
        from mn_cli.libs.model_cmds import reconcile_cluster_model_routes

        reconcile = reconcile_cluster_model_routes

    interval = _positive_float(
        values.get("MN_CLUSTER_MODEL_MONITOR_INTERVAL_SECONDS"),
        DEFAULT_INTERVAL_SECONDS,
    )
    retry_min = _positive_float(
        values.get("MN_CLUSTER_MODEL_MONITOR_RETRY_MIN_SECONDS"),
        DEFAULT_RETRY_MIN_SECONDS,
    )
    retry_max = max(
        retry_min,
        _positive_float(
            values.get("MN_CLUSTER_MODEL_MONITOR_RETRY_MAX_SECONDS"),
            DEFAULT_RETRY_MAX_SECONDS,
        ),
    )
    missing_grace = _positive_float(
        values.get("MN_CLUSTER_MODEL_MONITOR_NODE_MISSING_GRACE_SECONDS"),
        DEFAULT_NODE_MISSING_GRACE_SECONDS,
    )
    state_path = cluster_model_monitor_state_path(values)
    state = _load_monitor_state(state_path)

    while not stop_event.is_set():
        attempted_at = now()
        known_nodes = state.setdefault("nodes", {})
        expected_nodes = {
            str(node)
            for node, last_seen in known_nodes.items()
            if attempted_at - _float_value(last_seen) <= missing_grace
        }
        try:
            result = reconcile(
                restart=True,
                quiet=True,
                expected_nodes=expected_nodes,
            )
            observed_nodes = {
                str(item.get("node") or "").strip()
                for item in result.get("nodes") or []
                if isinstance(item, dict) and str(item.get("node") or "").strip()
            }
            for node in observed_nodes:
                known_nodes[node] = attempted_at
            for node, last_seen in list(known_nodes.items()):
                if attempted_at - _float_value(last_seen) > missing_grace:
                    known_nodes.pop(node, None)

            success = (
                result.get("status") == "ok"
                and not result.get("errors")
                and all(
                    isinstance(item, dict) and item.get("status") == "ok"
                    for item in result.get("nodes") or []
                )
            )
            state["last_result"] = result
            state["last_attempt_at"] = attempted_at
            if success:
                state["consecutive_failures"] = 0
                state["last_success_at"] = attempted_at
                state["last_error"] = ""
                delay = interval
            else:
                failures = int(state.get("consecutive_failures") or 0) + 1
                state["consecutive_failures"] = failures
                state["last_error"] = _result_error(result)
                delay = _retry_delay(failures, retry_min, retry_max)
        except Exception as exc:
            logger.exception("cluster model monitor reconciliation failed")
            failures = int(state.get("consecutive_failures") or 0) + 1
            state["consecutive_failures"] = failures
            state["last_attempt_at"] = attempted_at
            state["last_error"] = str(exc)
            delay = _retry_delay(failures, retry_min, retry_max)

        state["version"] = 1
        state["next_retry_seconds"] = delay
        _save_monitor_state(state_path, state)
        stop_event.wait(delay)


def start_cluster_model_monitor(
    stop_event: threading.Event,
) -> threading.Thread | None:
    if not cluster_model_monitor_enabled():
        return None
    thread = threading.Thread(
        target=run_cluster_model_monitor,
        args=(stop_event,),
        name="mn-cluster-model-monitor",
        daemon=True,
    )
    thread.start()
    return thread


def _load_monitor_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "nodes": {}, "consecutive_failures": 0}
    if not isinstance(payload, dict):
        return {"version": 1, "nodes": {}, "consecutive_failures": 0}
    if not isinstance(payload.get("nodes"), dict):
        payload["nodes"] = {}
    return payload


def _save_monitor_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _positive_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _float_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _retry_delay(failures: int, minimum: float, maximum: float) -> float:
    exponent = min(max(failures - 1, 0), 16)
    return min(maximum, minimum * (2**exponent))


def _result_error(result: dict[str, Any]) -> str:
    errors = result.get("errors") or []
    if errors:
        return "; ".join(
            str(item.get("error") or item)
            for item in errors
            if isinstance(item, dict)
        ) or "cluster model reconciliation was not acknowledged"
    return f"cluster model reconciliation status={result.get('status') or 'unknown'}"
