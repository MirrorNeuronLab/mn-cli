import json

from mn_cli.runtime.model_sync_monitor import (
    cluster_model_monitor_state_path,
    run_cluster_model_monitor,
)


class FakeStopEvent:
    def __init__(self, cycles: int):
        self.cycles = cycles
        self.delays: list[float] = []
        self.stopped = False

    def is_set(self) -> bool:
        return self.stopped

    def wait(self, delay: float) -> bool:
        self.delays.append(delay)
        if len(self.delays) >= self.cycles:
            self.stopped = True
        return self.stopped


def test_cluster_model_monitor_retries_until_every_node_acknowledges(tmp_path):
    stop = FakeStopEvent(cycles=2)
    calls = []
    results = [
        {
            "status": "warning",
            "nodes": [
                {
                    "node": "mirror_neuron@local",
                    "status": "error",
                    "error": "ack timeout",
                }
            ],
            "errors": [
                {
                    "node": "mirror_neuron@local",
                    "stage": "gateway",
                    "error": "ack timeout",
                }
            ],
        },
        {
            "status": "ok",
            "nodes": [
                {
                    "node": "mirror_neuron@local",
                    "status": "ok",
                    "ack": {"sync_id": "sync-2"},
                },
                {
                    "node": "mirror_neuron@spark",
                    "status": "ok",
                    "ack": {"sync_id": "sync-2"},
                },
            ],
            "errors": [],
        },
    ]

    def reconcile(**kwargs):
        calls.append(kwargs)
        return results[len(calls) - 1]

    env = {
        "MN_HOME": str(tmp_path / "mn-home"),
        "MN_CLUSTER_MODEL_MONITOR_INTERVAL_SECONDS": "12",
        "MN_CLUSTER_MODEL_MONITOR_RETRY_MIN_SECONDS": "2",
        "MN_CLUSTER_MODEL_MONITOR_RETRY_MAX_SECONDS": "8",
    }
    timestamps = iter([100.0, 101.0])

    run_cluster_model_monitor(
        stop,
        reconcile=reconcile,
        env=env,
        now=lambda: next(timestamps),
    )

    assert stop.delays == [2.0, 12.0]
    assert calls[0]["expected_nodes"] == set()
    assert calls[1]["expected_nodes"] == {"mirror_neuron@local"}
    state = json.loads(cluster_model_monitor_state_path(env).read_text())
    assert state["consecutive_failures"] == 0
    assert state["last_success_at"] == 101.0
    assert set(state["nodes"]) == {
        "mirror_neuron@local",
        "mirror_neuron@spark",
    }


def test_cluster_model_monitor_preserves_recently_missing_peer(tmp_path):
    env = {
        "MN_HOME": str(tmp_path / "mn-home"),
        "MN_CLUSTER_MODEL_MONITOR_NODE_MISSING_GRACE_SECONDS": "90",
    }
    state_path = cluster_model_monitor_state_path(env)
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": {"mirror_neuron@spark": 100.0},
                "consecutive_failures": 0,
            }
        )
    )
    stop = FakeStopEvent(cycles=1)
    calls = []

    def reconcile(**kwargs):
        calls.append(kwargs)
        return {
            "status": "warning",
            "nodes": [{"node": "mirror_neuron@local", "status": "ok"}],
            "errors": [
                {
                    "node": "mirror_neuron@spark",
                    "stage": "membership",
                    "error": "temporarily absent",
                }
            ],
        }

    run_cluster_model_monitor(
        stop,
        reconcile=reconcile,
        env=env,
        now=lambda: 150.0,
    )

    assert calls[0]["expected_nodes"] == {"mirror_neuron@spark"}
    state = json.loads(state_path.read_text())
    assert "mirror_neuron@spark" in state["nodes"]
    assert state["consecutive_failures"] == 1
