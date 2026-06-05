import json
import os
import subprocess
import uuid

import pytest

from mn_cli.server_cmds import _docker_network_run_args, _ensure_docker_network


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_MN_DOCKER_NETWORK_TESTS") != "1",
    reason="set RUN_MN_DOCKER_NETWORK_TESTS=1 to run Docker network integration tests",
)


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        check=check,
        text=True,
    )


def require_docker() -> None:
    try:
        docker("info")
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        pytest.skip(f"Docker is not available: {exc}")


def ensure_redis_image() -> None:
    if docker("image", "inspect", "redis:7", check=False).returncode != 0:
        docker("pull", "redis:7")


def test_bridge_network_aliases_resolve_and_leave_keeps_network() -> None:
    require_docker()
    ensure_redis_image()

    suffix = uuid.uuid4().hex[:10]
    network = f"mn-docker-it-{suffix}"
    core = f"mn-docker-it-core-{suffix}"
    redis = f"mn-docker-it-redis-{suffix}"
    node_alias = f"mn-it-{suffix}"
    redis_alias = f"{node_alias}-redis"

    try:
        _ensure_docker_network("bridge", network)
        docker(
            "run",
            "-d",
            "--name",
            redis,
            *_docker_network_run_args("bridge", network, redis_alias),
            "redis:7",
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "no",
        )
        docker(
            "run",
            "-d",
            "--name",
            core,
            *_docker_network_run_args("bridge", network, node_alias),
            "redis:7",
            "sleep",
            "120",
        )

        resolver = docker(
            "run",
            "--rm",
            "--network",
            network,
            "redis:7",
            "sh",
            "-c",
            f"getent hosts {node_alias} && getent hosts {redis_alias} && redis-cli -h {redis_alias} ping",
        )

        assert node_alias in resolver.stdout
        assert redis_alias in resolver.stdout
        assert "PONG" in resolver.stdout

        docker("network", "disconnect", network, core)
        docker("stop", core)
        docker("rm", core)

        network_after_leave = docker("network", "inspect", network)
        inspected = json.loads(network_after_leave.stdout)[0]
        assert inspected["Name"] == network
        assert core not in json.dumps(inspected.get("Containers", {}))
    finally:
        docker("rm", "-f", core, check=False)
        docker("rm", "-f", redis, check=False)
        docker("network", "rm", network, check=False)
