import json

import pytest

from mn_cli.runtime import server


def test_docker_container_probe_is_safe_when_docker_is_unavailable(monkeypatch):
    def missing_docker(*_args, **_kwargs):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(server.subprocess, "run", missing_docker)

    assert server._docker_container_running("mirror-neuron-core") is False


@pytest.mark.parametrize("args", [
    ("up", "-d"),
    ("restart", "mn-litellm-proxy"),
    ("stop", "mn-litellm-proxy"),
    ("down",),
])
def test_runtime_compose_preserves_managed_model_credentials(monkeypatch, tmp_path, args):
    home = tmp_path / "runtime home"
    gateway = home / "models" / "litellm-gateway"
    gateway.mkdir(parents=True)
    credential = gateway / "managed-dmr.env"
    credential.write_text("MN_DMR_TEST_TOKEN=saved-model-credential\n")
    override = gateway / "managed-dmr.compose.json"
    override.write_text(json.dumps({"services": {"mn-litellm-proxy": {
        "env_file": [str(credential)]
    }}}))
    cluster = home / "docker-compose.cluster.yml"
    cluster.write_text("services: {}\n")
    monkeypatch.setattr(server, "DIR", home)
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_FILE", home / "docker-compose.yml")
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_ENV", home / "docker-compose.env")

    command = server.runtime_compose_cmd(*args)

    assert command == [
        "docker", "compose", "--env-file", str(home / "docker-compose.env"),
        "-f", str(home / "docker-compose.yml"),
        "-f", str(cluster), "-f", str(override), *args,
    ]
    assert "saved-model-credential" not in " ".join(command)
    assert credential.read_text() == "MN_DMR_TEST_TOKEN=saved-model-credential\n"


@pytest.mark.parametrize("directory_at_override", [False, True])
def test_runtime_compose_without_managed_model_override(monkeypatch, tmp_path, directory_at_override):
    override = tmp_path / "models" / "litellm-gateway" / "managed-dmr.compose.json"
    if directory_at_override:
        override.mkdir(parents=True)
    monkeypatch.setattr(server, "DIR", tmp_path)
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_FILE", tmp_path / "docker-compose.yml")
    monkeypatch.setattr(server, "RUNTIME_COMPOSE_ENV", tmp_path / "docker-compose.env")

    assert server.runtime_compose_cmd("up", "-d") == [
        "docker", "compose", "--env-file", str(tmp_path / "docker-compose.env"),
        "-f", str(tmp_path / "docker-compose.yml"), "up", "-d",
    ]
