import subprocess

import pytest
from typer.testing import CliRunner
from mn_cli.banner import format_banner
from mn_cli.main import app

runner = CliRunner()

def test_start_success(mocker):
    mock_start_server = mocker.patch('mn_cli.libs.sys_cmds._start_server')
    result = runner.invoke(app, ["runtime", "start"])
    assert result.exit_code == 0
    assert format_banner("MirrorNeuron Local Runtime") in result.stdout
    mock_start_server.assert_called_once_with()

def test_start_worker_node_success(mocker):
    mock_start_worker = mocker.patch('mn_cli.libs.sys_cmds._start_worker_node')
    result = runner.invoke(app, ["runtime", "start", "--worker-node", "--host", "192.168.4.20"])
    assert result.exit_code == 0
    mock_start_worker.assert_called_once_with(host="192.168.4.20", grpc_port=55051)

def test_expose_node_success(mocker):
    mock_expose_node = mocker.patch('mn_cli.libs.sys_cmds._start_network_seed')
    result = runner.invoke(
        app,
        [
            "node",
            "expose",
            "--host",
            "192.168.4.10",
            "--grpc-port",
            "50055",
            "--force-new-token",
            "--network",
            "overlay",
            "--docker-network",
            "mn-overlay",
        ],
    )
    assert result.exit_code == 0
    mock_expose_node.assert_called_once_with(
        host="192.168.4.10",
        grpc_port=50055,
        dist_port=54370,
        redis_port=None,
        force_new_token=True,
        docker_network_mode="overlay",
        docker_network_name="mn-overlay",
    )

def test_expose_node_defaults_to_bridge_network(mocker):
    mock_expose_node = mocker.patch('mn_cli.libs.sys_cmds._start_network_seed')
    result = runner.invoke(
        app,
        [
            "node",
            "expose",
            "--host",
            "192.168.4.10",
        ],
    )
    assert result.exit_code == 0
    mock_expose_node.assert_called_once_with(
        host="192.168.4.10",
        grpc_port=55051,
        dist_port=54370,
        redis_port=None,
        force_new_token=False,
        docker_network_mode=None,
        docker_network_name="mirror-neuron-runtime",
    )

def test_node_docker_network_help_hides_redis_and_erlang_ports():
    expose = runner.invoke(app, ["node", "expose", "--help"])
    join = runner.invoke(app, ["node", "join", "--help"])

    assert expose.exit_code == 0
    assert join.exit_code == 0
    assert "--redis-port" not in expose.stdout
    assert "--dist-port" not in expose.stdout
    assert "--redis-port" not in join.stdout
    assert "--dist-port" not in join.stdout

def test_add_node_success(mocker):
    mock_add_node = mocker.patch('mn_cli.libs.sys_cmds._join_network')
    result = runner.invoke(
        app,
        [
            "node",
            "add",
            "192.168.4.10",
            "--token",
            "join-token",
            "--grpc-port",
            "50055",
            "--network",
            "overlay",
            "--docker-network",
            "mn-overlay",
        ],
    )
    assert result.exit_code == 0
    mock_add_node.assert_called_once_with(
        seed_host="192.168.4.10",
        token="join-token",
        grpc_port=50055,
        docker_network_mode="overlay",
        docker_network_name="mn-overlay",
    )

def test_add_node_defaults_to_bridge_network(mocker):
    mock_add_node = mocker.patch('mn_cli.libs.sys_cmds._join_network')
    result = runner.invoke(
        app,
        [
            "node",
            "add",
            "192.168.4.10",
            "--token",
            "join-token",
        ],
    )
    assert result.exit_code == 0
    mock_add_node.assert_called_once_with(
        seed_host="192.168.4.10",
        token="join-token",
        grpc_port=55051,
        docker_network_mode=None,
        docker_network_name="mirror-neuron-runtime",
    )

def test_join_success(mocker):
    mock_join = mocker.patch('mn_cli.libs.sys_cmds._join_network')
    result = runner.invoke(
        app,
        [
            "node",
            "join",
            "192.168.1.1",
            "--token",
            "join-token",
            "--network",
            "overlay",
            "--docker-network",
            "mn-overlay",
        ],
    )
    assert result.exit_code == 0
    mock_join.assert_called_once_with(
        seed_host="192.168.1.1",
        token="join-token",
        grpc_port=55051,
        docker_network_mode="overlay",
        docker_network_name="mn-overlay",
    )

def test_join_defaults_to_bridge_network(mocker):
    mock_join = mocker.patch('mn_cli.libs.sys_cmds._join_network')
    result = runner.invoke(
        app,
        [
            "node",
            "join",
            "192.168.1.1",
            "--token",
            "join-token",
        ],
    )
    assert result.exit_code == 0
    mock_join.assert_called_once_with(
        seed_host="192.168.1.1",
        token="join-token",
        grpc_port=55051,
        docker_network_mode=None,
        docker_network_name="mirror-neuron-runtime",
    )

@pytest.mark.parametrize(
    ("args", "backend_path"),
    [
        (["node", "join", "192.168.1.1"], "mn_cli.libs.sys_cmds._join_network"),
        (["node", "add", "192.168.1.1"], "mn_cli.libs.sys_cmds._join_network"),
    ],
)
def test_node_cluster_commands_require_join_token(mocker, args, backend_path):
    backend = mocker.patch(backend_path)

    result = runner.invoke(app, args)

    assert result.exit_code == 2
    assert "Missing option '--token'." in result.stderr
    backend.assert_not_called()

def test_leave_success(mocker):
    import mn_cli.shared
    mock_remove = mocker.patch.object(mn_cli.shared.client, 'remove_node', return_value="disconnected")
    mock_detach = mocker.patch("mn_cli.libs.sys_cmds._detach_local_docker_node_if_matches")
    result = runner.invoke(app, ["node", "leave", "mirror_neuron@1.2.3.4"])
    assert result.exit_code == 0
    assert "Successfully requested mirror_neuron@1.2.3.4 to leave" in result.stdout
    mock_remove.assert_called_once_with("mirror_neuron@1.2.3.4")
    mock_detach.assert_called_once_with("mirror_neuron@1.2.3.4")

def test_leave_error(mocker):
    import mn_cli.shared
    mocker.patch.object(mn_cli.shared.client, 'remove_node', side_effect=Exception("Timeout"))
    mock_detach = mocker.patch("mn_cli.libs.sys_cmds._detach_local_docker_node_if_matches")
    result = runner.invoke(app, ["node", "leave", "mirror_neuron@1.2.3.4"])
    assert result.exit_code == 0
    assert "Error removing node: Timeout" in result.stdout
    mock_detach.assert_not_called()

def test_refresh_token_success(mocker):
    mock_refresh = mocker.patch('mn_cli.libs.sys_cmds._refresh_network_token', return_value="new-token")
    result = runner.invoke(app, ["node", "refresh-token"])
    assert result.exit_code == 0
    assert "network join token refreshed" in result.stdout
    assert "new-token" in result.stdout
    mock_refresh.assert_called_once_with()

def test_stop(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill')
    
    # Mock PID files
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch(
        'mn_cli.libs.sys_cmds.web_ui_pid_files',
        return_value=(
            (tmp_path / "web-ui-watchdog.pid", "Web UI watchdog"),
            (tmp_path / "web-ui.pid", "Web UI"),
        ),
    )
    mocker.patch(
        'mn_cli.libs.sys_cmds.api_pid_files',
        return_value=((tmp_path / "api.pid", "REST API"),),
    )
    
    (tmp_path / "api.pid").write_text("12345")
    (tmp_path / "beam.pid").write_text("67890")
    (tmp_path / "web-ui.pid").write_text("24680")
    (tmp_path / "web-ui-watchdog.pid").write_text("24681")
    
    result = runner.invoke(app, ["runtime", "stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    assert mock_kill_tree.call_count == 4
    
    assert not (tmp_path / "api.pid").exists()
    assert not (tmp_path / "beam.pid").exists()
    assert not (tmp_path / "web-ui.pid").exists()
    assert not (tmp_path / "web-ui-watchdog.pid").exists()

def test_stop_cleans_web_ui_pid_files_from_default_runtime_home(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill')

    active_dir = tmp_path / "active"
    default_dir = tmp_path / "default"
    active_dir.mkdir()
    default_dir.mkdir()
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch(
        'mn_cli.libs.sys_cmds.web_ui_pid_files',
        return_value=(
            (active_dir / "web-ui-watchdog.pid", "Web UI watchdog"),
            (active_dir / "web-ui.pid", "Web UI"),
            (default_dir / "web-ui-watchdog.pid", "Web UI watchdog"),
            (default_dir / "web-ui.pid", "Web UI"),
        ),
    )
    mocker.patch(
        'mn_cli.libs.sys_cmds.api_pid_files',
        return_value=((tmp_path / "api.pid", "REST API"),),
    )

    (default_dir / "web-ui-watchdog.pid").write_text("24681")
    (default_dir / "web-ui.pid").write_text("24680")

    result = runner.invoke(app, ["runtime", "stop"])

    assert result.exit_code == 0
    assert mock_kill_tree.call_count == 2
    assert not (default_dir / "web-ui-watchdog.pid").exists()
    assert not (default_dir / "web-ui.pid").exists()

def test_stop_uses_compose_runtime_when_available(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mock_run = mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mocker.patch('mn_cli.libs.sys_cmds.runtime_compose_available', return_value=True)
    mocker.patch('mn_cli.libs.sys_cmds.runtime_compose_cmd', return_value=["docker", "compose", "down"])
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill')

    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch(
        'mn_cli.libs.sys_cmds.web_ui_pid_files',
        return_value=(
            (tmp_path / "web-ui-watchdog.pid", "Web UI watchdog"),
            (tmp_path / "web-ui.pid", "Web UI"),
        ),
    )
    mocker.patch(
        'mn_cli.libs.sys_cmds.api_pid_files',
        return_value=((tmp_path / "api.pid", "REST API"),),
    )
    (tmp_path / "api.pid").write_text("12345")

    result = runner.invoke(app, ["runtime", "stop"])

    assert result.exit_code == 0
    mock_run.assert_any_call(["docker", "compose", "down"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    assert mock_kill_tree.call_count == 1

def test_stop_pid_file_invalid(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch(
        'mn_cli.libs.sys_cmds.web_ui_pid_files',
        return_value=(
            (tmp_path / "web-ui-watchdog.pid", "Web UI watchdog"),
            (tmp_path / "web-ui.pid", "Web UI"),
        ),
    )
    mocker.patch(
        'mn_cli.libs.sys_cmds.api_pid_files',
        return_value=((tmp_path / "api.pid", "REST API"),),
    )

    (tmp_path / "api.pid").write_text("invalid")
    
    result = runner.invoke(app, ["runtime", "stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    assert mock_kill_tree.call_count == 0

def test_stop_kill_oserror(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds._stop_network_runtime')
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill', side_effect=OSError("Process not found"))
    
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    mocker.patch(
        'mn_cli.libs.sys_cmds.web_ui_pid_files',
        return_value=(
            (tmp_path / "web-ui-watchdog.pid", "Web UI watchdog"),
            (tmp_path / "web-ui.pid", "Web UI"),
        ),
    )
    mocker.patch(
        'mn_cli.libs.sys_cmds.api_pid_files',
        return_value=((tmp_path / "api.pid", "REST API"),),
    )
    
    (tmp_path / "api.pid").write_text("12345")
    
    result = runner.invoke(app, ["runtime", "stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    # kill_tree shouldn't be called because os.kill raised OSError
    mock_kill_tree.assert_not_called()
