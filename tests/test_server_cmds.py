import pytest
import subprocess
from io import StringIO
from pathlib import Path
from rich.console import Console
import mn_cli.server_cmds as server_cmds
from mn_cli.server_cmds import (
    check_status,
    kill_tree,
    _resolve_grpc_admin_token,
    _resolve_grpc_auth_token,
    _resolve_mn_cookie,
    _start_server,
    find_web_ui_dir,
    _start_web_ui_if_installed,
    _compose_runtime_env,
    _print_service_endpoints,
    runtime_compose_cmd,
)
import typer

ORIGINAL_WEB_UI_DIRS = server_cmds.WEB_UI_DIRS

@pytest.fixture(autouse=True)
def isolated_mn_cookie_home(mocker, tmp_path, monkeypatch):
    monkeypatch.delenv("MN_COOKIE", raising=False)
    monkeypatch.delenv("MN_GRPC_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", raising=False)
    state_dir = tmp_path / ".mirror_neuron"
    log_dir = state_dir / ".logs"
    pid_dir = state_dir / ".pids"
    mocker.patch('mn_cli.server_cmds.DIR', state_dir)
    mocker.patch('mn_cli.server_cmds.PID_DIR', pid_dir)
    mocker.patch('mn_cli.server_cmds.LOG_DIR', log_dir)
    mocker.patch('mn_cli.server_cmds.BEAM_PID_FILE', pid_dir / "beam.pid")
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', pid_dir / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', pid_dir / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', log_dir / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', log_dir / "api.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', log_dir / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path / "mn_venv")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', state_dir / "docker-compose.yml")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', state_dir / "docker-compose.env")
    mocker.patch(
        'mn_cli.server_cmds.WEB_UI_DIRS',
        (state_dir / "webui", state_dir / "web-ui-source"),
    )

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
    mocker.patch('mn_cli.server_cmds.subprocess.check_output', side_effect=[
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

def test_resolve_mn_cookie_generates_persistent_non_default_cookie(tmp_path, mocker):
    cookie_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', cookie_dir)

    cookie = _resolve_mn_cookie()

    assert cookie
    assert cookie != "mirrorneuron"
    assert (cookie_dir / "erlang.cookie").read_text().strip() == cookie
    assert (cookie_dir / "erlang.cookie").stat().st_mode & 0o777 == 0o600
    assert _resolve_mn_cookie() == cookie

def test_resolve_mn_cookie_prefers_non_default_env(monkeypatch):
    monkeypatch.setenv("MN_COOKIE", "operator-provided-cookie")

    assert _resolve_mn_cookie() == "operator-provided-cookie"

def test_resolve_grpc_auth_token_generates_persistent_token(tmp_path, mocker):
    token_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', token_dir)

    token = _resolve_grpc_auth_token()

    assert token
    assert (token_dir / "grpc_auth.token").read_text().strip() == token
    assert (token_dir / "grpc_auth.token").stat().st_mode & 0o777 == 0o600
    assert _resolve_grpc_auth_token() == token

def test_resolve_grpc_auth_token_prefers_env(monkeypatch):
    monkeypatch.setenv("MN_GRPC_AUTH_TOKEN", "auth-token")

    assert _resolve_grpc_auth_token() == "auth-token"

def test_resolve_grpc_admin_token_generates_persistent_token(tmp_path, mocker):
    token_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', token_dir)

    token = _resolve_grpc_admin_token()

    assert token
    assert (token_dir / "grpc_admin.token").read_text().strip() == token
    assert (token_dir / "grpc_admin.token").stat().st_mode & 0o777 == 0o600
    assert _resolve_grpc_admin_token() == token

def test_resolve_grpc_admin_token_prefers_env(monkeypatch):
    monkeypatch.setenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", "admin-token")

    assert _resolve_grpc_admin_token() == "admin-token"

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

def test_runtime_compose_cmd_uses_installed_runtime_files(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    assert runtime_compose_cmd("up", "-d") == [
        "docker",
        "compose",
        "--env-file",
        str(compose_env),
        "-f",
        str(compose_file),
        "up",
        "-d",
    ]

def test_start_server_uses_compose_runtime_when_available(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")

    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)

    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)

    _start_server()

    assert runtime_compose_cmd("up", "-d") in commands
    assert all(cmd[:2] != ["docker", "inspect"] for cmd in commands)
    assert all(cmd[:3] != ["docker", "run", "-d"] for cmd in commands)

def test_start_server_passes_cluster_env_to_compose_runtime(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")

    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.99")

    calls = []

    def mock_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)

    _start_server(ip="192.168.4.173")

    compose_call = next(item for item in calls if item[0] == runtime_compose_cmd("up", "-d"))
    env = compose_call[1]["env"]
    assert env["MN_NODE_NAME"] == "mirror_neuron@192.168.4.99"
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.173"
    assert env["MN_REDIS_URL"] == "redis://192.168.4.173:6379/0"
    assert env["MN_DIST_PORT"] == "4370"
    assert env["ERL_AFLAGS"] == "-kernel inet_dist_listen_min 4370 inet_dist_listen_max 4370"

def test_compose_runtime_env_respects_explicit_cluster_names(monkeypatch):
    env = {
        "MN_NODE_NAME": "mn2@192.168.4.173",
        "MN_CLUSTER_NODES": "mn1@192.168.4.10,mn2@192.168.4.173",
        "MN_REDIS_URL": "redis://192.168.4.10:6379/0",
        "MN_DIST_PORT": "4500",
    }

    resolved = _compose_runtime_env(env, ip="192.168.4.10")

    assert resolved["MN_NODE_NAME"] == "mn2@192.168.4.173"
    assert resolved["MN_CLUSTER_NODES"] == "mn1@192.168.4.10,mn2@192.168.4.173"
    assert resolved["MN_REDIS_URL"] == "redis://192.168.4.10:6379/0"
    assert resolved["ERL_AFLAGS"] == "-kernel inet_dist_listen_min 4500 inet_dist_listen_max 4500"

def test_start_server_success(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    
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
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    
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


def test_start_server_passes_slack_env_to_docker(mocker, tmp_path, monkeypatch):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setenv("SLACK_DEFAULT_CHANNEL", "#claw")

    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)

    class UnameMock:
        sysname = "Darwin"

    mocker.patch('mn_cli.server_cmds.os.uname', return_value=UnameMock())

    _start_server()

    docker_run = next(cmd for cmd in commands if cmd[:3] == ["docker", "run", "-d"])
    cookie_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_COOKIE="))
    auth_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_GRPC_AUTH_TOKEN="))
    admin_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN="))
    assert cookie_env != "MN_COOKIE=mirrorneuron"
    assert auth_env != "MN_GRPC_AUTH_TOKEN="
    assert admin_env != "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN="
    assert ["-e", "SLACK_BOT_TOKEN"] == docker_run[
        docker_run.index("SLACK_BOT_TOKEN") - 1 : docker_run.index("SLACK_BOT_TOKEN") + 1
    ]
    assert ["-e", "SLACK_DEFAULT_CHANNEL"] == docker_run[
        docker_run.index("SLACK_DEFAULT_CHANNEL") - 1 : docker_run.index("SLACK_DEFAULT_CHANNEL") + 1
    ]

def test_default_web_ui_dirs_use_nested_install_path():
    assert ORIGINAL_WEB_UI_DIRS == (
        Path.home() / ".mn" / "webui",
        Path.home() / ".mn" / "web-ui-source",
    )

def test_find_web_ui_dir_installed(tmp_path, mocker):
    missing = tmp_path / "missing"
    installed = tmp_path / "web-ui"
    installed.mkdir()
    (installed / "package.json").write_text("{}")
    (installed / "node_modules").mkdir()
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (missing, installed))

    assert find_web_ui_dir() == installed

def test_start_web_ui_if_installed(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    web_ui_dir.mkdir()
    (web_ui_dir / "package.json").write_text("{}")
    (web_ui_dir / "node_modules").mkdir()

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")

    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 5173

    _start_web_ui_if_installed()

    assert (tmp_path / "web-ui.pid").read_text() == "5173"
    mock_popen.assert_called_once()
    assert mock_popen.call_args.args[0][:3] == ["npm", "run", "dev"]
    assert mock_popen.call_args.args[0][-1] == "localhost"
    assert mock_popen.call_args.kwargs["cwd"] == web_ui_dir

def test_start_web_ui_missing_noop(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (tmp_path / "web-ui",))
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')

    _start_web_ui_if_installed()

    mock_popen.assert_not_called()

def test_print_service_endpoints(mocker, monkeypatch):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    monkeypatch.setenv("MN_GRPC_TARGET", "core.local:55555")
    monkeypatch.setenv("MN_API_HOST", "127.0.0.1")
    monkeypatch.setenv("MN_API_PORT", "4401")
    monkeypatch.setenv("MN_REDIS_URL", "redis://redis.local:6380/0")
    monkeypatch.setenv("MN_DIST_PORT", "4370")

    _print_service_endpoints(ip=None, web_ui_available=True)

    rendered = output.getvalue()
    assert "Service endpoints" in rendered
    assert "Core gRPC" in rendered
    assert "core.local" in rendered
    assert "55555" in rendered
    assert "REST API" in rendered
    assert "4401" in rendered
    assert "Redis" in rendered
    assert "redis.local" in rendered
    assert "6380" in rendered
    assert "Erlang EPMD" in rendered
    assert "Erlang dist" in rendered
    assert "4370" in rendered
    assert "Web UI" in rendered
    assert "5173" in rendered

def test_print_service_endpoints_defaults_to_localhost(mocker, monkeypatch):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    for name in (
        "MN_GRPC_TARGET",
        "MN_CORE_GRPC_TARGET",
        "MN_CORE_HOST",
        "MN_API_HOST",
        "MN_REDIS_HOST",
        "MN_REDIS_URL",
        "MN_EPMD_HOST",
        "MN_DIST_HOST",
        "MN_WEB_UI_HOST",
    ):
        monkeypatch.delenv(name, raising=False)

    _print_service_endpoints(ip=None, web_ui_available=True)

    rendered = output.getvalue()
    assert "Core gRPC" in rendered
    assert "REST API" in rendered
    assert "Redis" in rendered
    assert "Web UI" in rendered
    assert "localhost" in rendered
    assert "0.0.0.0" not in rendered
    assert "127.0.0.1" not in rendered

def test_print_service_endpoints_marks_compose_internal_services(mocker, monkeypatch, tmp_path):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    _print_service_endpoints(ip=None, web_ui_available=False)

    rendered = output.getvalue()
    assert "Core gRPC" in rendered
    assert "REST API" in rendered
    assert "Redis" in rendered
    assert "Redis host" not in rendered
    assert "redis://redis:6379/0 (internal)" in rendered
    assert "Context engine" in rendered
    assert "membrane-context-engine:50052 (internal)" in rendered
    assert "OpenShell" in rendered
    assert "https://127.0.0.1:8080" in rendered
    assert "Erlang EPMD" in rendered
    assert "Erlang dist" in rendered
    assert "4370" in rendered
