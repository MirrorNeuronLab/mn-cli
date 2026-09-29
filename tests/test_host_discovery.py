from unittest.mock import Mock

import pytest

from mn_cli.libs import model_cmds, run_cmds
from mn_cli.runtime import host_discovery


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", set()),
        ("HOST.Example:55051", {"host.example"}),
        ("https://HOST.Example:8080/path", {"host.example", "https"}),
        ("[fe80::1]:55051", {"fe80::1"}),
    ],
)
def test_host_text_preserves_existing_parsing(value, expected):
    assert host_discovery.extract_host_candidates_from_text(value) == expected


def test_resolves_addresses_and_closes_probe(monkeypatch):
    network = host_discovery.socket
    monkeypatch.setattr(network, "gethostname", lambda: "host")
    monkeypatch.setattr(network, "gethostbyname", lambda _: "192.0.2.1")
    monkeypatch.setattr(
        network, "gethostbyname_ex", lambda _: ("host", [], ["192.0.2.2"])
    )
    monkeypatch.setattr(
        network,
        "getaddrinfo",
        lambda *a, **k: [
            (0, 0, 0, "", ("FE80::1%en0", 0, 0, 0)),
            (0,),
            (0, 0, 0, "", (123,)),
        ],
    )
    probe = Mock()
    probe.getsockname.return_value = ("192.0.2.3", 12)
    monkeypatch.setattr(network, "socket", lambda *a: probe)
    assert host_discovery.resolved_local_hostnames() == {
        "192.0.2.1",
        "192.0.2.2",
        "fe80::1",
        "192.0.2.3",
    }
    probe.connect.assert_called_once_with(("10.255.255.255", 1))
    probe.close.assert_called_once()


def test_discovery_failures_are_independent_and_close_probe(monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("unavailable")

    for name in ("gethostbyname", "gethostbyname_ex", "getaddrinfo"):
        monkeypatch.setattr(host_discovery.socket, name, fail)
    probe = Mock()
    probe.connect.side_effect = OSError("offline")
    monkeypatch.setattr(host_discovery.socket, "socket", lambda *a: probe)
    assert host_discovery.resolved_local_hostnames() == set()
    probe.close.assert_called_once()
    monkeypatch.setattr(host_discovery.socket, "socket", fail)
    assert host_discovery.resolved_local_hostnames() == set()


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        ({}, None),
        (
            {
                "native_sdk_grpc": {"target": "top"},
                "hardware": {"native_sdk_grpc": {"target": "nested"}},
            },
            {"target": "top"},
        ),
        (
            {
                "native_sdk_grpc": {},
                "hardware": {"native_sdk_grpc": {"enabled": False}},
            },
            {"enabled": False},
        ),
        (
            {
                "hardware": "invalid",
                "node_info": {"native_sdk_grpc": {"target": "last"}},
            },
            {"target": "last"},
        ),
        ({"native_sdk_grpc": [], "node_info": {"native_sdk_grpc": "invalid"}}, None),
    ],
)
def test_native_metadata_precedence(node, expected):
    assert host_discovery.node_native_sdk_grpc_info(node) == expected


@pytest.mark.parametrize("commands", [model_cmds, run_cmds])
def test_cached_callers_preserve_injected_resolver_and_environment(
    monkeypatch, commands
):
    commands._local_host_addresses.cache_clear()
    calls = Mock(return_value={"192.0.2.7"})
    monkeypatch.setattr(commands, "_resolved_local_hostnames", calls)
    monkeypatch.setenv("MN_API_HOST", "alias.example")
    try:
        addresses = commands._local_host_addresses()
        assert {"192.0.2.7", "alias.example", "localhost", "::1"} <= addresses
        assert commands._local_host_addresses() is addresses
        calls.assert_called_once()
    finally:
        commands._local_host_addresses.cache_clear()


def test_invalid_config_target_does_not_discard_other_sources():
    addresses = host_discovery.local_host_candidates(
        lambda: "[invalid",
        {"MN_API_HOST": "alias.example"},
        resolve_hostnames=Mock(side_effect=OSError("offline")),
        extract_hosts=host_discovery.extract_host_candidates_from_text,
    )
    assert {"localhost", "alias.example"} <= addresses


def test_authoritative_remote_policy_remains_in_model_commands(monkeypatch):
    monkeypatch.setattr(model_cmds, "_local_host_addresses", lambda: {"localhost"})
    assert not model_cmds._cluster_node_is_local(
        {
            "self_authoritative": True,
            "self": False,
            "host": "localhost",
        }
    )


def test_missing_target_and_failed_config_lookup_preserve_loopbacks():
    for target in (lambda: "", Mock(side_effect=AttributeError("missing config"))):
        assert host_discovery.local_host_candidates(
            target,
            {},
            resolve_hostnames=lambda: set(),
            extract_hosts=host_discovery.extract_host_candidates_from_text,
        ) == {"localhost", "127.0.0.1", "::1", "::", "0.0.0.0"}
