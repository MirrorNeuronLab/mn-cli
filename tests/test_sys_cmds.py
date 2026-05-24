import subprocess

from typer.testing import CliRunner
from mn_cli.banner import format_banner
from mn_cli.main import app

runner = CliRunner()

def test_start_success(mocker):
    mock_start_server = mocker.patch('mn_cli.libs.sys_cmds._start_server')
    result = runner.invoke(app, ["start"])
    assert result.exit_code == 0
    assert format_banner("MirrorNeuron Local Runtime") in result.stdout
    mock_start_server.assert_called_once_with()

def test_expose_node_success(mocker):
    mock_expose_node = mocker.patch('mn_cli.libs.sys_cmds._start_network_seed')
    result = runner.invoke(
        app,
        [
            "expose-node",
            "--host",
            "192.168.4.10",
            "--grpc-port",
            "50055",
            "--dist-port",
            "4500",
            "--redis-port",
            "6380",
            "--force-new-token",
        ],
    )
    assert result.exit_code == 0
    mock_expose_node.assert_called_once_with(
        host="192.168.4.10",
        grpc_port=50055,
        dist_port=4500,
        redis_port=6380,
        force_new_token=True,
    )

def test_add_node_success(mocker):
    mock_add_node = mocker.patch('mn_cli.libs.sys_cmds._join_network')
    result = runner.invoke(
        app,
        [
            "add-node",
            "192.168.4.10",
            "--token",
            "join-token",
            "--grpc-port",
            "50055",
        ],
    )
    assert result.exit_code == 0
    mock_add_node.assert_called_once_with(
        seed_host="192.168.4.10",
        token="join-token",
        grpc_port=50055,
    )

def test_join_success(mocker):
    mock_start_server = mocker.patch('mn_cli.libs.sys_cmds._start_server')
    result = runner.invoke(app, ["join", "192.168.1.1", "--token", "join-token"])
    assert result.exit_code == 0
    mock_start_server.assert_called_once_with(
        "192.168.1.1",
        token="join-token",
        host=None,
        grpc_port=55051,
        dist_port=54370,
        redis_port=None,
    )

def test_leave_success(mocker):
    import mn_cli.shared
    mock_remove = mocker.patch.object(mn_cli.shared.client, 'remove_node', return_value="disconnected")
    result = runner.invoke(app, ["leave", "mirror_neuron@1.2.3.4"])
    assert result.exit_code == 0
    assert "Successfully requested mirror_neuron@1.2.3.4 to leave" in result.stdout
    mock_remove.assert_called_once_with("mirror_neuron@1.2.3.4")

def test_leave_error(mocker):
    import mn_cli.shared
    mocker.patch.object(mn_cli.shared.client, 'remove_node', side_effect=Exception("Timeout"))
    result = runner.invoke(app, ["leave", "mirror_neuron@1.2.3.4"])
    assert result.exit_code == 0
    assert "Error removing node: Timeout" in result.stdout

def test_stop(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill')
    
    # Mock PID files
    mocker.patch('mn_cli.libs.sys_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch('mn_cli.libs.sys_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    
    (tmp_path / "api.pid").write_text("12345")
    (tmp_path / "beam.pid").write_text("67890")
    (tmp_path / "web-ui.pid").write_text("24680")
    
    result = runner.invoke(app, ["stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    assert mock_kill_tree.call_count == 3
    
    assert not (tmp_path / "api.pid").exists()
    assert not (tmp_path / "beam.pid").exists()
    assert not (tmp_path / "web-ui.pid").exists()

def test_stop_uses_compose_runtime_when_available(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mock_run = mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mocker.patch('mn_cli.libs.sys_cmds.runtime_compose_available', return_value=True)
    mocker.patch('mn_cli.libs.sys_cmds.runtime_compose_cmd', return_value=["docker", "compose", "down"])
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill')

    mocker.patch('mn_cli.libs.sys_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch('mn_cli.libs.sys_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    (tmp_path / "api.pid").write_text("12345")

    result = runner.invoke(app, ["stop"])

    assert result.exit_code == 0
    mock_run.assert_any_call(["docker", "compose", "down"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    assert mock_kill_tree.call_count == 1

def test_stop_pid_file_invalid(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    
    mocker.patch('mn_cli.libs.sys_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch('mn_cli.libs.sys_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    
    (tmp_path / "api.pid").write_text("invalid")
    
    result = runner.invoke(app, ["stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    assert mock_kill_tree.call_count == 0

def test_stop_kill_oserror(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill', side_effect=OSError("Process not found"))
    
    mocker.patch('mn_cli.libs.sys_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch('mn_cli.libs.sys_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    
    (tmp_path / "api.pid").write_text("12345")
    
    result = runner.invoke(app, ["stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    # kill_tree shouldn't be called because os.kill raised OSError
    mock_kill_tree.assert_not_called()
