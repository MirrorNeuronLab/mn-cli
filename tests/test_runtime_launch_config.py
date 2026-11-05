"""Characterize pure launch configuration through the established server imports."""

import pytest

from mn_cli.runtime import server


def test_docker_environment_is_filtered_and_sorted_without_mutating_input():
    env = {"UNRELATED": "skip", "MN_Z": "", "ERL_AFLAGS": "flags", "MN_A": "a=b"}
    before = env.copy()
    assert server._docker_env_args(env) == [
        "-e", "ERL_AFLAGS=flags", "-e", "MN_A=a=b", "-e", "MN_Z=",
    ]
    assert env == before


@pytest.mark.parametrize(("target", "expected"), [
    ("", ("localhost", "50051")),
    ("host:", ("host", "50051")),
    (":1234", ("localhost", "1234")),
    ("[::1]:1234", ("::1", "1234")),
    ("grpc://host:1234/path", ("host", "1234")),
])
def test_endpoint_target_parsing(target, expected):
    assert server._host_port_from_target(target, "localhost", "50051") == expected


def test_endpoint_host_projection_keeps_bind_and_advertised_addresses_distinct():
    env = {"MN_NETWORK_ADVERTISE_HOST": "192.168.1.20"}
    assert server._native_endpoint_host(" :: ") == "127.0.0.1"
    assert server._cluster_endpoint_host(env, "0.0.0.0") == "192.168.1.20"
    assert server._cluster_endpoint_host(env, "core.internal") == "core.internal"
    assert server._docker_publish_host("localhost") == "127.0.0.1"
    assert server._docker_publish_host("::1") == "::1"


def test_redis_url_projections_preserve_existing_credentials_and_database_rules():
    url = "rediss://user:p%40ss@internal:6379/4?ignored=yes"
    assert server._redis_url_with_database(url, "7") == "rediss://user:p%40ss@internal:6379/7"
    assert server._redis_url_with_public_endpoint(url, "public", 6380) == "rediss://user:p%40ss@public:6380/4"


def test_compose_profile_order_and_network_disabled_behavior():
    assert server._compose_profiles_with("gpu, CPU,gpu,,cpu", "cpu") == "gpu,CPU"
    assert server._compose_profiles_with("gpu", "cpu") == "gpu,cpu"
    assert server._docker_network_run_args("disabled", "net", "alias") == []
    assert server._docker_network_run_args("bridge", "net", "alias") == [
        "--network", "net", "--network-alias", "alias",
    ]
