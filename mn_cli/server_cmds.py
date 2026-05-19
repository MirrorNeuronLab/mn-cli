import os
import signal
import secrets
import subprocess
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import typer
from rich.console import Console
from rich.table import Table
from mn_cli.config import CliConfig
from mn_cli.logging_config import configure_logging

console = Console()
logger = configure_logging("mn-cli", CliConfig.from_env().log_path)

DIR = Path.home() / ".mirror_neuron"
PID_DIR = DIR / ".pids"
LOG_DIR = DIR / ".logs"
BEAM_PID_FILE = PID_DIR / "beam.pid"
API_PID_FILE = PID_DIR / "api.pid"
WEB_UI_PID_FILE = PID_DIR / "web-ui.pid"
BEAM_LOG = LOG_DIR / "beam.log"
API_LOG = LOG_DIR / "api.log"
WEB_UI_LOG = LOG_DIR / "web-ui.log"
VENV_DIR = Path.home() / ".local" / "share" / "mn_venv"
WEB_UI_DIRS = (
    DIR / "web-ui-source",
    Path(f"{DIR}_ui"),
)
WEB_UI_PORT = "5173"
DEFAULT_HOST = "localhost"

def _env_host(name: str, default: str = DEFAULT_HOST) -> str:
    return os.getenv(name, default).strip() or default

def _core_host() -> str:
    return _env_host("MN_CORE_HOST")

def _api_host() -> str:
    return _env_host("MN_API_HOST")

def _redis_host() -> str:
    return _env_host("MN_REDIS_HOST")

def _epmd_host() -> str:
    return _env_host("MN_EPMD_HOST")

def _dist_host() -> str:
    return _env_host("MN_DIST_HOST")

def _web_ui_host() -> str:
    return _env_host("MN_WEB_UI_HOST")

def _docker_publish_host(host: str) -> str:
    return "127.0.0.1" if host == "localhost" else host

def _resolve_mn_cookie() -> str:
    env_cookie = os.getenv("MN_COOKIE", "").strip()
    if env_cookie and env_cookie != "mirrorneuron":
        return env_cookie

    cookie_file = DIR / "erlang.cookie"
    try:
        existing_cookie = cookie_file.read_text().strip()
        if existing_cookie and existing_cookie != "mirrorneuron":
            return existing_cookie
    except FileNotFoundError:
        pass

    DIR.mkdir(parents=True, exist_ok=True)
    generated_cookie = secrets.token_hex(32)
    fd = os.open(cookie_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{generated_cookie}\n")
    try:
        cookie_file.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod Erlang cookie file %s", cookie_file, exc_info=True)
    return generated_cookie

def _resolve_grpc_auth_token() -> str:
    env_token = os.getenv("MN_GRPC_AUTH_TOKEN", "").strip()
    if env_token:
        return env_token

    token_file = DIR / "grpc_auth.token"
    try:
        existing_token = token_file.read_text().strip()
        if existing_token:
            return existing_token
    except FileNotFoundError:
        pass

    DIR.mkdir(parents=True, exist_ok=True)
    generated_token = secrets.token_hex(32)
    fd = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{generated_token}\n")
    try:
        token_file.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod gRPC auth token file %s", token_file, exc_info=True)
    return generated_token

def check_status(pid_file: Path) -> int:
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)
            return 0 # Running
        except (ValueError, OSError):
            return 1 # Stale
    return 2 # Not running

def kill_tree(parent_pid: int):
    try:
        os.kill(parent_pid, 0)
    except OSError:
        logger.debug("Process %s is not running", parent_pid)
        return
    
    try:
        children = subprocess.check_output(['pgrep', '-P', str(parent_pid)], stderr=subprocess.DEVNULL)
        for child_pid in children.decode().split():
            if child_pid.strip():
                kill_tree(int(child_pid.strip()))
    except subprocess.CalledProcessError:
        pass
    
    try:
        logger.info("Stopping process %s", parent_pid)
        os.kill(parent_pid, signal.SIGTERM)
    except OSError:
        logger.exception("Failed to stop process %s", parent_pid)
        pass

def find_web_ui_dir() -> Optional[Path]:
    for web_ui_dir in WEB_UI_DIRS:
        if (web_ui_dir / "package.json").exists() and (web_ui_dir / "node_modules").exists():
            return web_ui_dir
    return None

def _host_port_from_target(target: str, default_host: str, default_port: str) -> tuple[str, str]:
    if "://" in target:
        parsed = urlparse(target)
        return parsed.hostname or default_host, str(parsed.port or default_port)

    if target.startswith("[") and "]:" in target:
        host, port = target.rsplit("]:", 1)
        return host.lstrip("["), port

    if ":" in target:
        host, port = target.rsplit(":", 1)
        return host or default_host, port or default_port

    return target or default_host, default_port

def _redis_host_port(ip: Optional[str]) -> tuple[str, str]:
    default_host = ip or _redis_host()
    default_url = f"redis://{default_host}:6379/0"
    return _host_port_from_target(
        os.getenv("MN_REDIS_URL", default_url),
        default_host,
        "6379",
    )

def _print_service_endpoints(ip: Optional[str], web_ui_available: bool):
    core_host = _core_host()
    grpc_host, grpc_port = _host_port_from_target(
        os.getenv(
            "MN_GRPC_TARGET",
            os.getenv("MN_CORE_GRPC_TARGET", f"{core_host}:50051"),
        ),
        core_host,
        os.getenv("MN_GRPC_PORT", "50051"),
    )
    api_host = _api_host()
    api_port = os.getenv("MN_API_PORT", "4001")
    redis_host, redis_port = _redis_host_port(ip)
    epmd_host = _epmd_host()
    dist_host = _dist_host()
    dist_port = os.getenv("MN_DIST_PORT", "9000-9010" if os.uname().sysname == "Darwin" else "dynamic")
    web_ui_host = _web_ui_host()

    table = Table(title="Service endpoints", show_header=True, header_style="bold")
    table.add_column("Service")
    table.add_column("Host")
    table.add_column("Port")
    table.add_column("URL / target")

    table.add_row("Core gRPC", grpc_host, grpc_port, f"{grpc_host}:{grpc_port}")
    table.add_row("REST API", api_host, api_port, f"http://{api_host}:{api_port}/api/v1")
    table.add_row("Redis", redis_host, redis_port, f"redis://{redis_host}:{redis_port}/0")
    table.add_row("Erlang EPMD", epmd_host, "4369", f"{epmd_host}:4369")
    table.add_row("Erlang dist", dist_host, dist_port, f"{dist_host}:{dist_port}")
    if web_ui_available:
        table.add_row("Web UI", web_ui_host, WEB_UI_PORT, f"http://{web_ui_host}:{WEB_UI_PORT}")

    console.print(table)

def _start_web_ui_if_installed() -> bool:
    web_ui_dir = find_web_ui_dir()
    if not web_ui_dir:
        return False

    status = check_status(WEB_UI_PID_FILE)
    if status == 0:
        console.print("[yellow]=> Web UI is already running, skipping.[/yellow]")
        return True
    if status == 1:
        WEB_UI_PID_FILE.unlink(missing_ok=True)

    web_ui_host = _web_ui_host()
    env = os.environ.copy()
    env.setdefault("MN_WEB_UI_HOST", web_ui_host)
    env.setdefault("MN_API_HOST", _api_host())
    env.setdefault("MN_API_PORT", os.getenv("MN_API_PORT", "4001"))
    console.print(f"=> Starting mn-web-ui (Vite on {web_ui_host}:5173)...")
    with open(WEB_UI_LOG, "w") as out:
        p_web = subprocess.Popen(
            ["npm", "run", "dev", "--", "--host", web_ui_host],
            cwd=web_ui_dir,
            stdout=out,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True
        )
    WEB_UI_PID_FILE.write_text(str(p_web.pid))
    console.print(f"   [green][Started][/green] Web UI (PID: {p_web.pid})")
    return True

def _start_server(ip: str = None):
    if check_status(API_PID_FILE) == 0:
        console.print("[red]Error: MirrorNeuron API is already running.[/red]")
        console.print("Use 'mn stop' to stop it first.")
        raise typer.Exit(1)
        
    try:
        docker_status = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", "mirror-neuron-core"], capture_output=True, text=True)
        if docker_status.stdout.strip() == "true":
            console.print("[red]Error: MirrorNeuron Core (Docker) is already running.[/red]")
            console.print("Use 'mn stop' to stop it first.")
            raise typer.Exit(1)
    except FileNotFoundError:
        console.print("[red]Error: Docker is not installed or not in PATH.[/red]")
        raise typer.Exit(1)

    PID_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    console.print("===========================================")
    if ip:
        console.print(f"Joining Cluster at {ip} in Detached Mode...")
    else:
        console.print("Starting Services in Detached Mode...")
    console.print("===========================================")

    env = os.environ.copy()
    env.setdefault("MN_CORE_HOST", _core_host())
    env.setdefault("MN_API_HOST", _api_host())
    env.setdefault("MN_REDIS_HOST", _redis_host())
    env.setdefault("MN_EPMD_HOST", _epmd_host())
    env.setdefault("MN_DIST_HOST", _dist_host())
    env.setdefault("MN_WEB_UI_HOST", _web_ui_host())
    env.setdefault("MN_CORE_GRPC_TARGET", f"{env['MN_CORE_HOST']}:{os.getenv('MN_GRPC_PORT', '50051')}")
    if not env.get("MN_GRPC_AUTH_TOKEN"):
        env["MN_GRPC_AUTH_TOKEN"] = _resolve_grpc_auth_token()
    if ip:
        env["MN_CLUSTER_NODES"] = ip

    console.print("=> Starting MirrorNeuron Core Service (Docker)...")
    logger.info("Starting MirrorNeuron Core Docker container")
    subprocess.run(["docker", "rm", "-f", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    
    cmd = ["docker", "run", "-d", "--name", "mirror-neuron-core"]
    
    # We want clustering to work, so we need to set the node name.
    import socket
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except socket.gaierror:
        local_ip = "127.0.0.1"
    # As a fallback or override, you could prompt the user, but we'll try to guess it.
    # To be safe for this specific test, we know local is 192.168.4.25 and remote is 192.168.4.173.
    # Let's see if we can get the actual external IP:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        local_ip = s.getsockname()[0]
    except Exception:
        local_ip = '127.0.0.1'
    finally:
        s.close()
        
    cmd.extend(["-e", f"MN_NODE_NAME=mirror_neuron@{local_ip}"])
    cmd.extend(["-e", f"MN_COOKIE={_resolve_mn_cookie()}"])
    cmd.extend(["-e", f"MN_GRPC_AUTH_TOKEN={env['MN_GRPC_AUTH_TOKEN']}"])
    
    core_publish_host = _docker_publish_host(env["MN_CORE_HOST"])
    epmd_publish_host = _docker_publish_host(env["MN_EPMD_HOST"])
    dist_publish_host = _docker_publish_host(env["MN_DIST_HOST"])

    system_name = os.uname().sysname

    if system_name == "Darwin":
        cmd.extend(["-p", f"{core_publish_host}:50051:50051", "-p", f"{epmd_publish_host}:4369:4369"])
        # Publish the distribution ports too
        for port in range(9000, 9011):
            cmd.extend(["-p", f"{dist_publish_host}:{port}:{port}"])
        cmd.extend(["-e", "MN_REDIS_URL=redis://host.docker.internal:6379/0"])
        cmd.extend(["-e", "MN_EXECUTOR_MAX_CONCURRENCY=50"])
    else:
        cmd.extend(["--network", "host"])
        cmd.extend(["-e", "MN_EXECUTOR_MAX_CONCURRENCY=50"])

    if system_name == "Darwin":
        cmd.extend(["-e", "MN_CORE_HOST=0.0.0.0"])
    else:
        cmd.extend(["-e", f"MN_CORE_HOST={env['MN_CORE_HOST']}"])
        cmd.extend(["-e", f"MN_REDIS_HOST={env['MN_REDIS_HOST']}"])
        cmd.extend(["-e", f"ERL_EPMD_ADDRESS={env['MN_EPMD_HOST']}"])

    if ip:
        cmd.extend(["-e", f"MN_CLUSTER_NODES=mirror_neuron@{ip}"])
        # A node joining another should also point its redis to the main cluster leader if not specified
        cmd.extend(["-e", f"MN_REDIS_URL=redis://{ip}:6379/0"])

    for env_name in [
        "SLACK_BOT_TOKEN",
        "SLACK_DEFAULT_CHANNEL",
        "SLACK_API_BASE_URL",
        "MN_SLACK_BOT_TOKEN",
        "MN_SLACK_DEFAULT_CHANNEL",
        "MN_SLACK_API_BASE_URL",
    ]:
        if os.getenv(env_name):
            cmd.extend(["-e", env_name])

    openshell_container_config_dir = Path(
        os.getenv(
            "OPENSHELL_CONTAINER_CONFIG_DIR",
            str(Path.home() / ".config" / "openshell-mirror-neuron"),
        )
    )
    openshell_config_dir = openshell_container_config_dir
    if not (openshell_config_dir / "gateways" / "openshell").is_dir():
        openshell_config_dir = Path.home() / ".config" / "openshell"
    if (openshell_config_dir / "gateways" / "openshell").is_dir():
        cmd.extend(["-v", f"{openshell_config_dir}:/root/.config/openshell:ro"])
        cmd.extend(["-v", f"{openshell_config_dir}:/opt/mirror_neuron/.config/openshell:ro"])
    
    cmd.append("mirror-neuron-core:latest")
    
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
        console.print("   [green][Started][/green] Core Service (Docker: mirror-neuron-core)")
    except subprocess.CalledProcessError:
        console.print("[red]Failed to start Core Service Docker container.[/red]")
        raise typer.Exit(1)

    console.print("=> Waiting for Elixir to boot...")
    time.sleep(3)

    api_bin = VENV_DIR / "bin" / "mn-api"
    if api_bin.exists():
        console.print("=> Starting mn-api (REST on port 4001)...")
        with open(API_LOG, "w") as out:
            p_api = subprocess.Popen(
                [str(api_bin)],
                stdout=out,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True
            )
        API_PID_FILE.write_text(str(p_api.pid))
        console.print(f"   [green][Started][/green] REST API (PID: {p_api.pid})")
    else:
        console.print("[yellow]=> Warning: mn-api not found, skipping.[/yellow]")

    web_ui_available = _start_web_ui_if_installed()

    console.print("\n===========================================")
    if ip:
        console.print(f"MirrorNeuron is running and attempting to join cluster at {ip}!")
    else:
        console.print("MirrorNeuron is running in the background!")
    _print_service_endpoints(ip, web_ui_available)
    console.print("Logs are available at:")
    console.print(f"  Core: {BEAM_LOG}")
    console.print(f"  API:  {API_LOG}")
    if WEB_UI_LOG.exists():
        console.print(f"  Web:  {WEB_UI_LOG}")
    console.print("\nRun 'mn stop' to shut down the services.")
    console.print("===========================================")
