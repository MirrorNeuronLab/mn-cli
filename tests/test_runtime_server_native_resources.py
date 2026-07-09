from __future__ import annotations

import json
from pathlib import Path

import pytest

from mn_cli.runtime import server


def test_compose_env_includes_sdk_node_resource_advertisement(monkeypatch):
    hardware = {
        "platform": {"display_name": "sdk-node", "hostname": "sdk-host"},
        "cpu": {"logical_processors": 8, "model": "SDK CPU"},
        "memory": {"total_mb": 32768, "available_mb": 30000},
        "disk": {"total_mb": 100000, "available_mb": 90000},
        "gpu": [],
        "devices": [],
        "capabilities": ["sdk-advertised"],
        "host_paths": ["/srv/mn"],
        "runtime_drivers": ["host_local"],
    }

    monkeypatch.setattr(
        server,
        "node_resource_environment",
        lambda env: {
            "MN_NODE_HARDWARE_JSON": json.dumps(hardware, sort_keys=True, separators=(",", ":")),
            "MN_NODE_DISPLAY_NAME": "sdk-node",
            "MN_NODE_CPU_MODEL": "SDK CPU",
            "MN_NODE_RUNTIME_DRIVERS": "host_local",
        },
    )
    monkeypatch.setattr(server, "_detect_host_gpu_count", lambda: 0)

    env = server._ensure_node_advertisement_settings({})

    assert json.loads(env["MN_NODE_HARDWARE_JSON"]) == hardware
    assert env["MN_NODE_DISPLAY_NAME"] == "sdk-node"
    assert env["MN_NODE_CPU_MODEL"] == "SDK CPU"
    assert env["MN_NODE_RUNTIME_DRIVERS"] == "host_local"


def test_runtime_compose_template_passes_node_advertisement_contract():
    root = Path(__file__).resolve().parents[2]
    if not (root / "mn-deploy").is_dir():
        pytest.skip("mn-deploy sibling repository is not available")
    compose_text = (root / "mn-deploy" / "docker-compose.yml").read_text(encoding="utf-8")

    for key in (
        "MN_NODE_HARDWARE_JSON",
        "MN_NODE_CPU_CORES",
        "MN_NODE_MEMORY_TOTAL_MB",
        "MN_NODE_MEMORY_AVAILABLE_MB",
        "MN_NODE_DISK_TOTAL_MB",
        "MN_NODE_DISK_AVAILABLE_MB",
        "MN_NODE_HOST_PATHS",
        "MN_NODE_RUNTIME_DRIVERS",
        "MN_NODE_GPU_UNIFIED_MEMORY_MB",
    ):
        assert f"{key}: ${{{key}:-}}" in compose_text
        assert key in server.NODE_ADVERTISEMENT_ENV_KEYS


def test_compose_env_includes_native_sdk_grpc_forwarding_target(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_ENV", tmp_path / "docker-compose.env")

    env = server._ensure_compose_native_port_settings({})

    assert env["MN_GRPC_ADVERTISE_PORT"] == "55051"
    assert env["MN_NATIVE_SDK_GRPC_HOST"] == "127.0.0.1"
    assert env["MN_NATIVE_SDK_GRPC_PORT"] == "55052"
    assert env["MN_NATIVE_SDK_GRPC_TARGET"] == "mn-native-sdk-grpc:55052"
    assert env["MN_NATIVE_SDK_GRPC_PROXY_PORT"] == "55052"
    assert env["MN_NATIVE_SDK_GRPC_PROXY_TARGET_HOST"] == "host.docker.internal"
    assert env["MN_NATIVE_SDK_GRPC_PROXY_TARGET_PORT"] == "55052"


def test_compose_env_migrates_legacy_native_sdk_grpc_target(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_ENV", tmp_path / "docker-compose.env")

    env = server._ensure_compose_native_port_settings(
        {"MN_NATIVE_SDK_GRPC_TARGET": "host.docker.internal:55052"}
    )

    assert env["MN_NATIVE_SDK_GRPC_TARGET"] == "mn-native-sdk-grpc:55052"


def test_network_core_env_includes_native_sdk_target_for_worker_core(monkeypatch):
    monkeypatch.setattr(server.os, "uname", lambda: type("Uname", (), {"sysname": "Linux"})())
    monkeypatch.setattr(server, "_ensure_redis_ha_settings", lambda env, **_kwargs: env)
    monkeypatch.setattr(server, "_ensure_node_advertisement_settings", lambda env: env)

    env = server._network_core_env(
        token="join-token",
        host="192.168.4.173",
        docker_network_mode="disabled",
        docker_network_name="mirror-neuron-runtime",
        node_alias="spark",
        node_name="mirror_neuron@192.168.4.173",
        cluster_nodes="mirror_neuron@192.168.4.173",
        grpc_port=55051,
        epmd_port=54369,
        dist_port=54370,
        redis_url="redis://127.0.0.1:6379/0",
        redis_public_host="192.168.4.173",
        redis_public_port=56379,
    )

    assert env["MN_NATIVE_SDK_GRPC_HOST"] == "0.0.0.0"
    assert env["MN_NATIVE_SDK_GRPC_PORT"] == "55052"
    assert env["MN_NATIVE_SDK_GRPC_ADVERTISE_HOST"] == "192.168.4.173"
    assert env["MN_NATIVE_SDK_GRPC_ADVERTISE_PORT"] == "55052"
    assert env["MN_NATIVE_SDK_GRPC_TARGET"] == "host.docker.internal:55052"


def test_network_core_env_advertises_worker_native_sdk_grpc_in_hardware(monkeypatch):
    monkeypatch.setattr(server.os, "uname", lambda: type("Uname", (), {"sysname": "Linux"})())
    monkeypatch.setattr(server, "_ensure_redis_ha_settings", lambda env, **_kwargs: env)

    def fake_node_resource_environment(*, env, **_kwargs):
        native = {
            "enabled": True,
            "host": env["MN_NATIVE_SDK_GRPC_ADVERTISE_HOST"],
            "port": int(env["MN_NATIVE_SDK_GRPC_ADVERTISE_PORT"]),
            "target": f"{env['MN_NATIVE_SDK_GRPC_ADVERTISE_HOST']}:{env['MN_NATIVE_SDK_GRPC_ADVERTISE_PORT']}",
            "bind_host": env["MN_NATIVE_SDK_GRPC_HOST"],
        }
        return {"MN_NODE_HARDWARE_JSON": json.dumps({"native_sdk_grpc": native})}

    monkeypatch.setattr(server, "node_resource_environment", fake_node_resource_environment)
    monkeypatch.setattr(server, "_detect_host_gpu_count", lambda: 0)

    env = server._network_core_env(
        token="join-token",
        host="192.168.4.173",
        docker_network_mode="disabled",
        docker_network_name="mirror-neuron-runtime",
        node_alias="spark",
        node_name="mirror_neuron@192.168.4.173",
        cluster_nodes="mirror_neuron@192.168.4.173",
        grpc_port=55051,
        epmd_port=54369,
        dist_port=54370,
        redis_url="redis://127.0.0.1:6379/0",
        redis_public_host="192.168.4.173",
        redis_public_port=56379,
    )

    hardware = json.loads(env["MN_NODE_HARDWARE_JSON"])
    assert hardware["native_sdk_grpc"]["target"] == "192.168.4.173:55052"
    assert hardware["native_sdk_grpc"]["bind_host"] == "0.0.0.0"


def test_network_core_env_uses_host_docker_internal_for_networked_container(monkeypatch):
    monkeypatch.setattr(server.os, "uname", lambda: type("Uname", (), {"sysname": "Linux"})())
    monkeypatch.setattr(server, "_ensure_redis_ha_settings", lambda env, **_kwargs: env)
    monkeypatch.setattr(server, "_ensure_node_advertisement_settings", lambda env: env)

    env = server._network_core_env(
        token="join-token",
        host="192.168.4.173",
        docker_network_mode="overlay",
        docker_network_name="mirror-neuron-runtime",
        node_alias="spark",
        node_name="mirror_neuron@spark",
        cluster_nodes="mirror_neuron@spark",
        grpc_port=55051,
        epmd_port=54369,
        dist_port=54370,
        redis_url="redis://redis:6379/0",
        redis_public_host="redis",
        redis_public_port=6379,
    )

    assert env["MN_NATIVE_SDK_GRPC_TARGET"] == "host.docker.internal:55052"


def test_native_sdk_grpc_command_falls_back_to_importable_source_module(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "VENV_DIR", tmp_path / "venv")
    monkeypatch.delenv("MN_NATIVE_SDK_GRPC_SOURCE", raising=False)

    command = server._native_sdk_grpc_command()

    assert command is not None
    assert command[-2:] == ["-m", "mn_sdk.native_runtime_service"]


def test_worker_compose_foundation_services_start_gateway_only(monkeypatch, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text(
        "name: mirror-neuron\nservices:\n  mn-native-sdk-grpc:\n    image: mirror-neuron-core:latest\n  mn-litellm-proxy:\n    image: mirror-neuron-core:latest\n",
        encoding="utf-8",
    )
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n", encoding="utf-8")
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_FILE", compose_file)
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_ENV", compose_env)
    monkeypatch.setattr(
        server,
        "runtime_compose_cmd",
        lambda *args: ["docker", "compose", *args],
    )
    commands = []

    def fake_run(command, **kwargs):
        commands.append((command, kwargs))
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr(server.subprocess, "run", fake_run)

    server._start_worker_compose_foundation_services(
        {
            "MN_NETWORK_ADVERTISE_HOST": "192.168.4.173",
            "MN_NATIVE_SDK_GRPC_HOST": "0.0.0.0",
            "MN_NATIVE_SDK_GRPC_PORT": "55052",
            "MN_NATIVE_SDK_GRPC_ADVERTISE_HOST": "192.168.4.173",
            "MN_NATIVE_SDK_GRPC_ADVERTISE_PORT": "55052",
            "MN_NATIVE_SDK_GRPC_TARGET": "host.docker.internal:55052",
            "MN_NODE_NAME": "mirror_neuron@192.168.4.173",
            "MN_NODE_ROLE": "runtime",
            "MN_DOCKER_NETWORK_MODE": "disabled",
            "MN_DOCKER_NETWORK_NAME": "mirror-neuron-runtime",
        }
    )

    assert commands[0][0] == [
        "docker",
        "compose",
        "up",
        "-d",
        "mn-native-sdk-grpc",
        "mn-litellm-proxy",
    ]
    written = server._read_env_file(compose_env)
    assert written["MN_NATIVE_SDK_GRPC_ADVERTISE_HOST"] == "192.168.4.173"
    assert written["MN_NATIVE_SDK_GRPC_TARGET"] == "host.docker.internal:55052"
