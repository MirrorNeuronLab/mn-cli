"""Supervised reconciliation of storage for already-authorized federation peers."""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable

logger = logging.getLogger(__name__)
INTERVAL_SECONDS = 30.0
RETRY_SECONDS = 5.0


def reconcile_runtime_storage() -> dict[str, int]:
    from mn_cli.runtime import server

    # A watchdog can outlive a runtime restart. Never retain its old sidecar
    # credentials when the runtime has published new settings.
    env = dict(os.environ)
    env.update(server._read_env_file(server.RUNTIME_COMPOSE_ENV))
    return server._reconcile_syncthing_federated_peers(
        env, advertised_host=env.get("MN_SYNCTHING_ADVERTISE_HOST", "")
    )


def run_storage_sync_monitor(
    stop_event: threading.Event,
    *,
    reconcile: Callable[[], dict[str, int]] = reconcile_runtime_storage,
) -> None:
    previous = None
    while not stop_event.is_set():
        delay = INTERVAL_SECONDS
        try:
            result = reconcile()
            if result["connected"] < result["discovered"]:
                delay = RETRY_SECONDS
            status = (result["discovered"], result["connected"])
            if status != previous:
                logger.info("Storage peers discovered=%s registered=%s", *status)
                previous = status
        except Exception:  # noqa: BLE001 - isolate failures at the supervisor boundary
            # Never include credentials or untrusted remote errors in logs.
            if previous != "unavailable":
                logger.warning("Shared-storage reconciliation unavailable; will retry")
            previous = "unavailable"
            delay = RETRY_SECONDS
        stop_event.wait(delay)


def start_storage_sync_monitor(stop_event: threading.Event) -> threading.Thread:
    # Independent of model preparation, which can take minutes.
    thread = threading.Thread(
        target=run_storage_sync_monitor,
        args=(stop_event,),
        name="mn-storage-sync-monitor",
        daemon=True,
    )
    thread.start()
    return thread
