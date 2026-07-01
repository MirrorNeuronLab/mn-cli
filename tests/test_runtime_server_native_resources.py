from __future__ import annotations

import json

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
