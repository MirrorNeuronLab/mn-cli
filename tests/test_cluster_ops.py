from mn_cli.runtime.cluster_ops import (
    ClusterCredentials,
    ClusterOperationPlan,
    ClusterTopology,
    NodeIdentity,
    network_node_name,
    node_host,
)


def test_cluster_credentials_compare_fingerprints_without_exposing_values():
    local = ClusterCredentials.from_values(token="join-token", cookie="cookie")
    remote = ClusterCredentials.from_values(token="join-token", cookie="cookie")
    mismatch = ClusterCredentials.from_values(token="other-token", cookie="cookie")

    assert local.matches(remote)
    assert not local.matches(mismatch)
    assert "join-token" not in repr(local)
    assert "raw-cookie-secret" not in repr(ClusterCredentials.from_values(token="join-token", cookie="raw-cookie-secret"))


def test_node_identity_normalizes_network_node_name():
    identity = NodeIdentity.from_host("192.168.4.173", display_name="spark")

    assert identity.name == "mirror_neuron@192.168.4.173"
    assert identity.host == "192.168.4.173"
    assert identity.display_name == "spark"
    assert network_node_name("spark") == "mirror_neuron@spark"
    assert node_host("mirror_neuron@spark") == "spark"


def test_cluster_topology_detects_reciprocal_membership_and_remote_connection():
    local = ClusterTopology.from_system_summary(
        {
            "nodes": [
                {
                    "name": "mirror_neuron@local",
                    "self?": True,
                    "connected_nodes": ["mirror_neuron@local", "mirror_neuron@spark"],
                },
                {"name": "mirror_neuron@spark", "connected_nodes": ["mirror_neuron@spark"]},
            ]
        }
    )
    remote = ClusterTopology.from_system_summary(
        {
            "nodes": [
                {"name": "mirror_neuron@local", "connected_nodes": ["mirror_neuron@local"]},
                {
                    "name": "mirror_neuron@spark",
                    "self?": True,
                    "connected_nodes": ["mirror_neuron@spark", "mirror_neuron@local"],
                },
            ]
        }
    )

    assert local.reciprocal_membership_ok(remote)
    assert local.remote_connected_to_local(remote)


def test_cluster_operation_plan_marks_remote_actions():
    plan = ClusterOperationPlan(
        action="join",
        local=NodeIdentity.from_host("192.168.6.28"),
        remote=NodeIdentity.from_host("192.168.4.173"),
    )

    assert plan.requires_remote
    assert plan.docker_network_mode == "disabled"
