import json

import grpc
import pytest

from mn_cli.libs import model_cmds


class RpcFailure(grpc.RpcError):
    def __init__(self, code):
        self.status = code

    def code(self):
        return self.status

    def details(self):
        return "Bearer private-credential"


@pytest.fixture
def cluster(mocker, monkeypatch, tmp_path):
    monkeypatch.setenv("MN_HOME", str(tmp_path))
    local = {"name": "mini", "grpc_host": "127.0.0.1", "grpc_port": 55051, "self": True}
    peer = {"name": "spark", "grpc_host": "127.0.0.2", "grpc_port": 55051}
    mocker.patch.object(
        model_cmds.client,
        "get_system_summary",
        return_value=json.dumps({"nodes": [local, peer]}),
    )
    shared = mocker.patch.object(
        model_cmds.client,
        "get_runtime_statuses",
        return_value=json.dumps(
            {
                "nodes": [local, peer],
                "events": [{"id": "1-0", "domain": "models"}],
            }
        ),
    )
    read = mocker.patch.object(model_cmds.client, "get_node_runtime_status")
    ack = mocker.patch.object(
        model_cmds.client,
        "ack_runtime_status_events",
        return_value=json.dumps(
            {"status": "acked", "event_ids": ["1-0"], "acked_count": 1}
        ),
    )
    mocker.patch.object(model_cmds, "_installed_model_names", return_value=set())
    mocker.patch.object(
        model_cmds, "_reconcile_stale_local_model_registrations", return_value=[]
    )
    mocker.patch.object(
        model_cmds,
        "_publish_local_runtime_model_inventory",
        return_value={"status": "accepted"},
    )
    persist = mocker.patch.object(
        model_cmds, "reconcile_cluster_model_remotes", return_value={"remotes": {}}
    )
    mocker.patch.object(
        model_cmds, "sync_litellm_gateway", return_value={"status": "running"}
    )
    warning = mocker.patch.object(model_cmds, "print_warning")
    return read, shared, ack, persist, warning


@pytest.mark.parametrize(
    "failure",
    [
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNAVAILABLE,
    ],
)
@pytest.mark.parametrize("stale", [False, True])
def test_failed_peer_refresh_is_one_warning_and_never_prunes_or_acks(
    cluster, failure, stale
):
    read, shared, ack, persist, warning = cluster
    if stale:
        payload = json.loads(shared.return_value)
        payload["nodes"][1]["runtime_status"] = {
            "models": {
                "revision": "old",
                "reported_at": "2000-01-01T00:00:00Z",
                "status": {"models": []},
            }
        }
        shared.return_value = json.dumps(payload)
    read.side_effect = RpcFailure(failure)
    result = model_cmds.reconcile_cluster_model_routes(restart=False)
    assert result["status"] == "warning" and not result["inventory_complete"]
    assert len(result["errors"]) == 1
    assert persist.call_args.kwargs["replace"] is False
    ack.assert_not_called()
    warning.assert_called_once()
    assert "private-credential" not in str(warning.call_args)
    assert "not yet been published" not in str(warning.call_args)

    # The next successful owner read repairs the inventory using the same loop.
    read.side_effect = None
    read.return_value = json.dumps(
        {
            "name": "spark",
            "runtime_status": {
                "models": {"revision": "empty-v1", "status": {"models": []}}
            },
        }
    )
    result = model_cmds.reconcile_cluster_model_routes(restart=False)
    assert result["status"] == "ok" and result["inventory_complete"]
    assert persist.call_args.kwargs["replace"] is True
    ack.assert_called_once_with(["1-0"])


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"name": "spark", "runtime_status": {}}, "not yet been published"),
        ({"name": "wrong", "runtime_status": {}}, "Invalid runtime status"),
        ({"name": "spark", "runtime_status": {"models": {}}}, "no revision"),
        ({"name": "spark", "runtime_status": {"models": []}}, "Invalid model status"),
        (
            {
                "name": "spark",
                "runtime_status": {
                    "models": {"revision": "v1", "status": {"models": {}}}
                },
            },
            "must be a list",
        ),
    ],
)
def test_unpublished_and_invalid_snapshots_are_distinct(cluster, payload, expected):
    read, _, ack, persist, warning = cluster
    read.return_value = json.dumps(payload)
    result = model_cmds.reconcile_cluster_model_routes(restart=False)
    assert expected in result["errors"][0]["error"]
    assert not result["inventory_complete"]
    assert persist.call_args.kwargs["replace"] is False
    warning.assert_called_once()
    ack.assert_not_called()


def test_owner_routes_preserve_core_advertised_gateway_port():
    node = {
        "node_name": "spark",
        "host": "10.0.0.2",
        "node": {
            "litellm": {"host": "10.0.0.2", "port": 4100, "url": "http://10.0.0.2:4100"}
        },
    }
    assert model_cmds._node_litellm_gateway_api_base(node) == "http://10.0.0.2:4100/v1"


def test_distinct_owners_on_one_host_keep_each_others_gateway_routes():
    routes = {
        "peer-model": {
            "node": "spark",
            "source": "remote_litellm_gateway",
            "api_base": "http://127.0.0.1:4100/v1",
        }
    }
    assert (
        model_cmds._runtime_endpoints_for_gateway_node(
            routes, {"node_name": "mini", "host": "127.0.0.1"}
        )
        == routes
    )
    assert (
        model_cmds._runtime_endpoints_for_gateway_node(
            routes, {"node_name": "spark", "host": "127.0.0.1"}
        )
        == {}
    )
