import pytest
import os
import signal
import subprocess
from pathlib import Path
from mn_cli.server_cmds import check_status, kill_tree, _start_server
import typer

def test_check_status_running(mocker, tmp_path):
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill')
    assert check_status(pid_file) == 0

def test_check_status_stale(mocker, tmp_path):
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill', side_effect=OSError)
    assert check_status(pid_file) == 1

def test_check_status_invalid(tmp_path):
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("abc")
    assert check_status(pid_file) == 1

def test_check_status_missing(tmp_path):
    pid_file = tmp_path / "test.pid"
    assert check_status(pid_file) == 2

def test_kill_tree(mocker):
    # Mock os.kill to succeed for existence check
    mock_kill = mocker.patch('mn_cli.server_cmds.os.kill')
    
    # Mock pgrep to return children
    mock_check_output = mocker.patch('mn_cli.server_cmds.subprocess.check_output', side_effect=[
        b" 1235 \n 1236 \n", # First call for parent 1234
        subprocess.CalledProcessError(1, "pgrep"), # Second call for child 1235
        subprocess.CalledProcessError(1, "pgrep")  # Third call for child 1236
    ])
    
    kill_tree(1234)
    
    # Should call kill for existence, then pgrep, then recurse, then kill with SIGTERM
    assert mock_kill.call_count == 3 * 2 # (exists + term) for 1234, 1235, 1236
    
def test_kill_tree_not_exist(mocker):
    mocker.patch('mn_cli.server_cmds.os.kill', side_effect=OSError)
    mock_check_output = mocker.patch('mn_cli.server_cmds.subprocess.check_output')
    kill_tree(1234)
    mock_check_output.assert_not_called()

def test_kill_tree_term_fails(mocker):
    # Succeed first check, fail SIGTERM
    mock_kill = mocker.patch('mn_cli.server_cmds.os.kill', side_effect=[None, OSError])
    mocker.patch('mn_cli.server_cmds.subprocess.check_output', side_effect=subprocess.CalledProcessError(1, "pgrep"))
    kill_tree(1234)
    assert mock_kill.call_count == 2

def test_start_server_already_running(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    (tmp_path / "api.pid").write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill') # check_status returns 0
    
    with pytest.raises(typer.Exit) as exc:
        _start_server()
    assert exc.value.exit_code == 1

def test_start_server_docker_running(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid") # Missing, check_status -> 2
    
    mock_run = mocker.patch('mn_cli.server_cmds.subprocess.run')
    mock_run.return_value.stdout = "true\n"
    
    with pytest.raises(typer.Exit) as exc:
        _start_server()
    assert exc.value.exit_code == 1

def test_start_server_no_docker(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=FileNotFoundError)
    
    with pytest.raises(typer.Exit) as exc:
        _start_server()
    assert exc.value.exit_code == 1

def test_start_server_docker_start_fails(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    
    def mock_run(cmd, **kwargs):
        if cmd[1] == "inspect":
            m = mocker.Mock()
            m.stdout = "false\n"
            return m
        elif cmd[1] == "run":
            raise subprocess.CalledProcessError(1, "docker run")
        return mocker.Mock()
        
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    
    with pytest.raises(typer.Exit) as exc:
        _start_server()
    assert exc.value.exit_code == 1

def test_start_server_success(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    
    def mock_run(cmd, **kwargs):
        m = mocker.Mock()
        m.stdout = "false\n"
        return m
        
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    api_bin = bin_dir / "mn-api"
    api_bin.touch()
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)
    
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 9999
    
    class UnameMock:
        sysname = "Linux"
    mocker.patch('mn_cli.server_cmds.os.uname', return_value=UnameMock())
    
    _start_server(ip="127.0.0.1")
    
    assert (tmp_path / "api.pid").exists()
    assert (tmp_path / "api.pid").read_text() == "9999"

def test_start_server_darwin(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    
    def mock_run(cmd, **kwargs):
        m = mocker.Mock()
        m.stdout = "false\n"
        return m
        
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path) # no mn-api to skip api
    
    class UnameMock:
        sysname = "Darwin"
    mocker.patch('mn_cli.server_cmds.os.uname', return_value=UnameMock())
    
    _start_server()
