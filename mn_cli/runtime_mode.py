from __future__ import annotations

import subprocess

WORKER_MODE = "worker"
CORE_CONTAINERS = ("mirror-neuron-network-core", "mirror-neuron-core")
DOCKER_TIMEOUT_SECONDS = 1.0


def local_runtime_mode() -> str | None:
    """Return a local runtime mode hint without contacting the runtime."""
    for container_name in CORE_CONTAINERS:
        env = _running_container_env(container_name)
        if _truthy(env.get("MN_NETWORK_ONLY")):
            return WORKER_MODE
    return None


def _running_container_env(container_name: str) -> dict[str, str]:
    if not _container_running(container_name):
        return {}

    try:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "-f",
                "{{range .Config.Env}}{{println .}}{{end}}",
                container_name,
            ],
            capture_output=True,
            text=True,
            timeout=DOCKER_TIMEOUT_SECONDS,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return {}

    if result.returncode != 0:
        return {}

    values: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def _container_running(container_name: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
            capture_output=True,
            text=True,
            timeout=DOCKER_TIMEOUT_SECONDS,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return False

    return result.returncode == 0 and result.stdout.strip().lower() == "true"


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
