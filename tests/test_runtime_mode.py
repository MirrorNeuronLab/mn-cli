import subprocess

from mn_cli.runtime_mode import local_runtime_mode


def test_local_runtime_mode_detects_worker_container(mocker):
    def run(cmd, **kwargs):
        if cmd[:4] == ["docker", "inspect", "-f", "{{.State.Running}}"]:
            return mocker.Mock(returncode=0, stdout="true\n")
        if cmd[:4] == [
            "docker",
            "inspect",
            "-f",
            "{{range .Config.Env}}{{println .}}{{end}}",
        ]:
            return mocker.Mock(returncode=0, stdout="MN_NETWORK_ONLY=true\n")
        raise AssertionError(f"unexpected command: {cmd}")

    mocker.patch("mn_cli.runtime_mode.subprocess.run", side_effect=run)

    assert local_runtime_mode() == "worker"


def test_local_runtime_mode_ignores_primary_container(mocker):
    def run(cmd, **kwargs):
        if cmd[:4] == ["docker", "inspect", "-f", "{{.State.Running}}"]:
            return mocker.Mock(returncode=0, stdout="true\n")
        if cmd[:4] == [
            "docker",
            "inspect",
            "-f",
            "{{range .Config.Env}}{{println .}}{{end}}",
        ]:
            return mocker.Mock(returncode=0, stdout="MN_NETWORK_ONLY=false\n")
        raise AssertionError(f"unexpected command: {cmd}")

    mocker.patch("mn_cli.runtime_mode.subprocess.run", side_effect=run)

    assert local_runtime_mode() is None


def test_local_runtime_mode_fails_quietly_when_docker_is_unavailable(mocker):
    mocker.patch(
        "mn_cli.runtime_mode.subprocess.run",
        side_effect=FileNotFoundError("docker"),
    )

    assert local_runtime_mode() is None


def test_local_runtime_mode_fails_quietly_when_docker_times_out(mocker):
    mocker.patch(
        "mn_cli.runtime_mode.subprocess.run",
        side_effect=subprocess.TimeoutExpired(["docker"], timeout=1),
    )

    assert local_runtime_mode() is None
