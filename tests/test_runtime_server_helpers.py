from mn_cli.runtime import server


def test_docker_container_probe_is_safe_when_docker_is_unavailable(monkeypatch):
    def missing_docker(*_args, **_kwargs):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(server.subprocess, "run", missing_docker)

    assert server._docker_container_running("mirror-neuron-core") is False
