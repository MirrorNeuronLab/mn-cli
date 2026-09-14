import copy
from unittest.mock import Mock

import pytest

from mn_cli.runtime import server
from mn_cli.runtime import storage_sync_monitor as monitor


class StopAfter:
    def __init__(self, cycles):
        self.cycles = cycles
        self.delays = []

    def is_set(self):
        return len(self.delays) >= self.cycles

    def wait(self, delay):
        self.delays.append(delay)
        return self.is_set()


def test_monitor_recovers_peer_that_appears_after_startup(monkeypatch):
    """Exercise real pairing against two in-memory authenticated sidecars."""
    env = {
        "MN_SYNCTHING_ENABLED": "auto",
        "MN_SYNCTHING_DEVICE_ID": "OLDLOCAL",
        "MN_SYNCTHING_API_KEY": "local-key",
        "MN_SYNCTHING_ADVERTISE_HOST": "local.example",
    }
    remote = {
        "enabled": True,
        "device_id": "OLDREMOTE",
        "api_key": "remote-key",
        "host": "remote.example",
        "gui_port": 58384,
        "sync_port": 22000,
        "folder_id": server.SYNCTHING_FOLDER_ID,
        "folder_path": server.SYNCTHING_FOLDER_PATH,
    }
    configs = {
        host: {"devices": [], "folders": []} for host in ("127.0.0.1", "remote.example")
    }
    writes = []
    cycle = 0

    class Core:
        def get_system_summary(self):
            return {
                "nodes": [
                    {
                        "name": "remote",
                        "connection_mode": "federated",
                        "peer_available": cycle > 0,
                    }
                ]
            }

        def get_federated_peer(self, name):
            assert name == "remote"
            return {"syncthing": remote}

    def request(host, port, key, method, path, body=None, **kwargs):
        assert key == ("local-key" if host == "127.0.0.1" else "remote-key")
        if path == "/rest/system/status":
            if cycle == 1 and host == "remote.example":
                raise OSError("sidecar still starting")
            return {"myID": "LOCAL" if host == "127.0.0.1" else "REMOTE"}
        if path == "/rest/config":
            if method == "PUT":
                configs[host] = copy.deepcopy(body)
                writes.append(host)
            return copy.deepcopy(configs[host])
        if path.startswith("/rest/db/ignores"):
            return {"ignore": list(server.SYNCTHING_MANAGED_IGNORE_PATTERNS)}
        return {}

    monkeypatch.setattr(server, "_syncthing_request", request)

    def reconcile():
        nonlocal cycle
        result = server._reconcile_syncthing_federated_peers(
            env, advertised_host="local.example", core_client=Core()
        )
        cycle += 1
        return result

    stop = StopAfter(4)
    monitor.run_storage_sync_monitor(stop, reconcile=reconcile)
    assert stop.delays == [30, 5, 30, 30]
    assert writes == ["127.0.0.1", "remote.example"]  # no steady-state rewrite
    for config in configs.values():
        assert {d["deviceID"] for d in config["folders"][0]["devices"]} == {
            "LOCAL",
            "REMOTE",
        }
    assert remote["device_id"] == "OLDREMOTE"  # never mutate the Core descriptor


def test_monitor_survives_errors_without_logging_secrets(caplog):
    reconcile = Mock(
        side_effect=[RuntimeError("secret-api-key"), {"discovered": 0, "connected": 0}]
    )
    stop = StopAfter(2)
    monitor.run_storage_sync_monitor(stop, reconcile=reconcile)
    assert stop.delays == [5, 30]
    assert "secret-api-key" not in caplog.text


def test_monitor_reloads_persisted_settings_each_pass(monkeypatch):
    monkeypatch.setenv("MN_SYNCTHING_API_KEY", "old-inherited-key")
    configs = iter(
        [
            {
                "MN_SYNCTHING_API_KEY": "first",
                "MN_SYNCTHING_ADVERTISE_HOST": "first-host",
            },
            {
                "MN_SYNCTHING_API_KEY": "second",
                "MN_SYNCTHING_ADVERTISE_HOST": "second-host",
            },
        ]
    )
    monkeypatch.setattr(server, "_read_env_file", lambda _: next(configs))
    reconcile = Mock(return_value={"discovered": 0, "connected": 0})
    monkeypatch.setattr(server, "_reconcile_syncthing_federated_peers", reconcile)
    monitor.reconcile_runtime_storage()
    monitor.reconcile_runtime_storage()
    assert [c.args[0]["MN_SYNCTHING_API_KEY"] for c in reconcile.call_args_list] == [
        "first",
        "second",
    ]


def test_disabled_storage_does_not_contact_core():
    core = Mock()
    result = server._reconcile_syncthing_federated_peers(
        {"MN_SYNCTHING_ENABLED": "false"}, advertised_host="local", core_client=core
    )
    assert result == {"discovered": 0, "connected": 0}
    core.get_system_summary.assert_not_called()


@pytest.mark.parametrize("status", [{}, {"myID": ""}])
def test_missing_live_identity_does_not_change_pairing(monkeypatch, status):
    core = Mock()
    core.get_system_summary.return_value = {
        "nodes": [{"name": "peer", "connection_mode": "federated"}]
    }
    core.get_federated_peer.return_value = {
        "syncthing": {"enabled": True, "host": "peer"}
    }
    monkeypatch.setattr(server, "_syncthing_status", lambda *args: status)
    connect = Mock()
    monkeypatch.setattr(server, "_connect_syncthing_peers", connect)
    assert server._reconcile_syncthing_federated_peers(
        {"MN_SYNCTHING_ENABLED": "auto"}, advertised_host="local", core_client=core
    ) == {"discovered": 1, "connected": 0}
    connect.assert_not_called()
