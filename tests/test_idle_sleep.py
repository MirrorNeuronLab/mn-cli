import pytest
from mn_cli.runtime import idle_sleep


def test_macos_assertion_released_on_error(mocker):
    mocker.patch.object(idle_sleep.sys, "platform", "darwin")
    process = mocker.Mock()
    popen = mocker.patch.object(idle_sleep.subprocess, "Popen", return_value=process)
    with pytest.raises(RuntimeError):
        with idle_sleep.prevent_idle_sleep():
            raise RuntimeError("interrupted")
    assert popen.call_args.args[0][:3] == ["/usr/bin/caffeinate", "-i", "-w"]
    process.terminate.assert_called_once()
    process.wait.assert_called_once()
    assert idle_sleep.awake_command(["python", "relay"]) == [
        "/usr/bin/caffeinate", "-i", "python", "relay"]


def test_other_platforms_do_not_launch_assertion(mocker):
    mocker.patch.object(idle_sleep.sys, "platform", "linux")
    popen = mocker.patch.object(idle_sleep.subprocess, "Popen")
    with idle_sleep.prevent_idle_sleep():
        pass
    popen.assert_not_called()
    assert idle_sleep.awake_command(["python", "relay"]) == ["python", "relay"]
