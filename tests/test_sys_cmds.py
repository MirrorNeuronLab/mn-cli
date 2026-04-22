import pytest
from typer.testing import CliRunner
from mn_cli.main import app
import os
from pathlib import Path

runner = CliRunner()

def test_start_success(mocker):
    mock_start_server = mocker.patch('mn_cli.libs.sys_cmds._start_server')
    result = runner.invoke(app, ["start"])
    assert result.exit_code == 0
    mock_start_server.assert_called_once_with()

def test_join_success(mocker):
    mock_start_server = mocker.patch('mn_cli.libs.sys_cmds._start_server')
    result = runner.invoke(app, ["join", "192.168.1.1"])
    assert result.exit_code == 0
    mock_start_server.assert_called_once_with("192.168.1.1")

def test_leave_success(mocker):
    import mn_cli.shared
    mock_remove = mocker.patch.object(mn_cli.shared.client, 'remove_node', return_value="disconnected")
    result = runner.invoke(app, ["leave", "mirror_neuron@1.2.3.4"])
    assert result.exit_code == 0
    assert "Successfully requested mirror_neuron@1.2.3.4 to leave" in result.stdout
    mock_remove.assert_called_once_with("mirror_neuron@1.2.3.4")

def test_leave_error(mocker):
    import mn_cli.shared
    mock_remove = mocker.patch.object(mn_cli.shared.client, 'remove_node', side_effect=Exception("Timeout"))
    result = runner.invoke(app, ["leave", "mirror_neuron@1.2.3.4"])
    assert result.exit_code == 0
    assert "Error removing node: Timeout" in result.stdout

def test_stop(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill')
    
    # Mock PID files
    mocker.patch('mn_cli.libs.sys_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    
    (tmp_path / "api.pid").write_text("12345")
    (tmp_path / "beam.pid").write_text("67890")
    
    result = runner.invoke(app, ["stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    assert mock_kill_tree.call_count == 2
    
    assert not (tmp_path / "api.pid").exists()
    assert not (tmp_path / "beam.pid").exists()

def test_stop_pid_file_invalid(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    
    mocker.patch('mn_cli.libs.sys_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    
    (tmp_path / "api.pid").write_text("invalid")
    
    result = runner.invoke(app, ["stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    assert mock_kill_tree.call_count == 0

def test_stop_kill_oserror(mocker, tmp_path):
    mocker.patch('mn_cli.libs.sys_cmds.subprocess.run')
    mock_kill_tree = mocker.patch('mn_cli.libs.sys_cmds.kill_tree')
    mocker.patch('mn_cli.libs.sys_cmds.os.kill', side_effect=OSError("Process not found"))
    
    mocker.patch('mn_cli.libs.sys_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.libs.sys_cmds.BEAM_PID_FILE', tmp_path / "beam.pid")
    
    (tmp_path / "api.pid").write_text("12345")
    
    result = runner.invoke(app, ["stop"])
    
    assert result.exit_code == 0
    assert "All services stopped." in result.stdout
    # kill_tree shouldn't be called because os.kill raised OSError
    mock_kill_tree.assert_not_called()
