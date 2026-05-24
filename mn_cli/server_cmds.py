import os
import json
import hashlib
import signal
import secrets
import socket
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

def _mn_home() -> Path:
    configured_home = os.getenv("MN_HOME") or os.getenv("MIRROR_NEURON_HOME")
    return Path(configured_home).expanduser() if configured_home else Path.home() / ".mn"


DIR = _mn_home()
PID_DIR = DIR / "pids"
LOG_DIR = DIR / "logs"
BEAM_PID_FILE = PID_DIR / "beam.pid"
API_PID_FILE = PID_DIR / "api.pid"
WEB_UI_PID_FILE = PID_DIR / "web-ui.pid"
BEAM_LOG = LOG_DIR / "beam.log"
API_LOG = LOG_DIR / "api.log"
WEB_UI_LOG = LOG_DIR / "web-ui.log"
VENV_DIR = Path.home() / ".local" / "share" / "mn_venv"
RUNTIME_COMPOSE_FILE = DIR / "docker-compose.yml"
RUNTIME_COMPOSE_ENV = DIR / "docker-compose.env"
WEB_UI_DIRS = (
    DIR / "webui",
    DIR / "web-ui-source",
)
WEB_UI_PORT = "5173"
DEFAULT_HOST = "localhost"
DEFAULT_DIST_PORT = "4370"
NETWORK_TOKEN_FILE = DIR / "network.token"
NETWORK_DOCKER_NETWORK = "mirror-neuron-network"
NETWORK_CORE_CONTAINER = "mirror-neuron-network-core"
NETWORK_REDIS_CONTAINER = "mirror-neuron-network-redis"


def _openshell_config_dir() -> Path:
    return Path(os.getenv("OPENSHELL_CONFIG_DIR", str(Path.home() / ".config" / "openshell"))).expanduser()


def _openshell_gateway_endpoint() -> str:
    configured_endpoint = os.getenv("OPENSHELL_GATEWAY_ENDPOINT")
    if configured_endpoint:
        return configured_endpoint

    gateway_name = os.getenv("OPENSHELL_GATEWAY", "").strip()
    if not gateway_name:
        try:
            gateway_name = (_openshell_config_dir() / "active_gateway").read_text(encoding="utf-8").strip()
        except OSError:
            gateway_name = ""
    if gateway_name:
        try:
            metadata = json.loads(
                (_openshell_config_dir() / "gateways" / gateway_name / "metadata.json").read_text(
                    encoding="utf-8"
                )
            )
        except (OSError, json.JSONDecodeError):
            metadata = {}
        endpoint = metadata.get("gateway_endpoint") if isinstance(metadata, dict) else None
        if isinstance(endpoint, str) and endpoint.strip():
            return endpoint.strip()

    return f"https://127.0.0.1:{os.getenv('OPENSHELL_GATEWAY_PORT', '8080')}"

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

def _network_publish_host(host: str) -> str:
    normalized = (host or "").strip().lower()
    if normalized in {"", "localhost", "127.0.0.1", "::1"}:
        return "127.0.0.1"
    return "0.0.0.0"

def _detect_lan_ip() -> str:
    try:
        return socket.gethostbyname(socket.gethostname())
    except socket.gaierror:
        pass

    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("10.255.255.255", 1))
        return probe.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        probe.close()

def _compose_runtime_env(env: dict[str, str], ip: Optional[str]) -> dict[str, str]:
    compose_env = dict(env)
    compose_env.setdefault("MN_DIST_PORT", DEFAULT_DIST_PORT)
    compose_env.setdefault("MN_NODE_ROLE", "runtime")

    if ip:
        local_ip = _detect_lan_ip()
        compose_env.setdefault("MN_NODE_NAME", f"mirror_neuron@{local_ip}")
        if not compose_env.get("MN_CLUSTER_NODES") or compose_env.get("MN_CLUSTER_NODES") == ip:
            compose_env["MN_CLUSTER_NODES"] = f"mirror_neuron@{ip}"
        compose_env["MN_REDIS_URL"] = compose_env.get("MN_REDIS_URL") or f"redis://{ip}:6379/0"

    if compose_env.get("MN_NODE_NAME"):
        dist_port = compose_env.get("MN_DIST_PORT", DEFAULT_DIST_PORT)
        compose_env.setdefault(
            "ERL_AFLAGS",
            f"-kernel inet_dist_listen_min {dist_port} inet_dist_listen_max {dist_port}",
        )

    return compose_env

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

def _resolve_grpc_admin_token() -> str:
    env_token = os.getenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", "").strip()
    if env_token:
        return env_token

    token_file = DIR / "grpc_admin.token"
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
        logger.debug("Failed to chmod gRPC admin token file %s", token_file, exc_info=True)
    return generated_token

def _resolve_network_token(force_new: bool = False) -> str:
    env_token = os.getenv("MN_NETWORK_JOIN_TOKEN", "").strip()
    if env_token and not force_new:
        _write_network_token(env_token)
        return env_token

    if not force_new:
        try:
            existing_token = NETWORK_TOKEN_FILE.read_text().strip()
            if existing_token:
                return existing_token
        except FileNotFoundError:
            pass

    DIR.mkdir(parents=True, exist_ok=True)
    generated_token = secrets.token_urlsafe(32)
    _write_network_token(generated_token)
    return generated_token

def _write_network_token(token: str) -> None:
    DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(NETWORK_TOKEN_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{token}\n")
    try:
        NETWORK_TOKEN_FILE.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod network token file %s", NETWORK_TOKEN_FILE, exc_info=True)

def _derive_network_secret(token: str, label: str) -> str:
    material = f"mirror-neuron:{label}:{token}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()

def _network_node_name(host: str) -> str:
    return f"mirror_neuron@{host}"

def _advertised_network_host(host: Optional[str] = None) -> str:
    configured = host or os.getenv("MN_NETWORK_ADVERTISE_HOST", "").strip()
    return (configured or _detect_lan_ip()).strip() or "127.0.0.1"

def _parse_port(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _compose_supports_redis_password() -> bool:
    try:
        return "MN_REDIS_PASSWORD" in RUNTIME_COMPOSE_FILE.read_text(encoding="utf-8")
    except OSError:
        return False

def _redis_url_with_public_endpoint(redis_url: str, host: str, port: int) -> str:
    parsed = urlparse(redis_url)
    scheme = parsed.scheme or "redis"
    path = parsed.path or "/0"
    userinfo = ""
    if "@" in parsed.netloc:
        userinfo = f"{parsed.netloc.rsplit('@', 1)[0]}@"
    return f"{scheme}://{userinfo}{host}:{port}{path}"

def _handshake_with_main_node(seed_host: str, token: str, grpc_port: int) -> dict:
    from mn_sdk import Client

    target = f"{seed_host}:{grpc_port}"
    try:
        handshake = Client(target=target, auth_token="", timeout=10).network_handshake(token)
    except Exception as exc:
        console.print(f"[red]Error: Could not join MirrorNeuron node at {target}.[/red]")
        console.print("Check the host, gRPC port, and token printed by 'mn start' on the main box.")
        console.print(f"[dim]{exc}[/dim]")
        raise typer.Exit(1) from exc

    if not handshake.get("node_name"):
        handshake["node_name"] = _network_node_name(seed_host)
    if not handshake.get("redis_host"):
        handshake["redis_host"] = seed_host
    if not handshake.get("redis_port"):
        handshake["redis_port"] = 6379
    if not handshake.get("redis_url"):
        handshake["redis_url"] = f"redis://{handshake['redis_host']}:{handshake['redis_port']}/0"
    return handshake

def _docker_container_running(name: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", name],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        console.print("[red]Error: Docker is not installed or not in PATH.[/red]")
        raise typer.Exit(1)
    return result.returncode == 0 and result.stdout.strip() == "true"

def _ensure_network_docker_network() -> None:
    result = subprocess.run(
        ["docker", "network", "inspect", NETWORK_DOCKER_NETWORK],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode != 0:
        subprocess.run(["docker", "network", "create", NETWORK_DOCKER_NETWORK], check=True, stdout=subprocess.DEVNULL)

def _network_redis_url(token: str, redis_host: str, redis_port: int) -> str:
    password = _derive_network_secret(token, "redis")
    return f"redis://:{password}@{redis_host}:{redis_port}/0"

def _network_core_env(
    *,
    token: str,
    host: str,
    node_name: str,
    cluster_nodes: str,
    grpc_port: int,
    dist_port: int,
    redis_url: str,
    redis_public_host: str,
    redis_public_port: int,
) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "MN_NETWORK_ONLY": "true",
            "MN_NETWORK_JOIN_TOKEN": token,
            "MN_NETWORK_ADVERTISE_HOST": host,
            "MN_NETWORK_REDIS_HOST": redis_public_host,
            "MN_NETWORK_REDIS_PORT": str(redis_public_port),
            "MN_CORE_HOST": "0.0.0.0",
            "MN_GRPC_PORT": str(grpc_port),
            "MN_NODE_NAME": node_name,
            "MN_NODE_ROLE": "runtime",
            "MN_CLUSTER_NODES": cluster_nodes,
            "MN_REDIS_URL": redis_url,
            "MN_DIST_PORT": str(dist_port),
            "MN_COOKIE": _derive_network_secret(token, "cookie"),
            "MN_GRPC_AUTH_TOKEN": _derive_network_secret(token, "grpc-auth"),
            "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN": _derive_network_secret(token, "grpc-admin"),
            "ERL_EPMD_ADDRESS": "0.0.0.0",
            "ERL_AFLAGS": f"-kernel inet_dist_listen_min {dist_port} inet_dist_listen_max {dist_port}",
        }
    )
    return env

def _docker_env_args(env: dict[str, str]) -> list[str]:
    args: list[str] = []
    for key in sorted(env):
        if key.startswith("MN_") or key in {"ERL_AFLAGS", "ERL_EPMD_ADDRESS"}:
            args.extend(["-e", f"{key}={env[key]}"])
    return args

def _start_network_redis(host: str, redis_port: int, token: str) -> None:
    subprocess.run(["docker", "rm", "-f", NETWORK_REDIS_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    publish_host = _network_publish_host(host)
    password = _derive_network_secret(token, "redis")
    data_dir = DIR / "network-redis"
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        NETWORK_REDIS_CONTAINER,
        "--network",
        NETWORK_DOCKER_NETWORK,
        "-p",
        f"{publish_host}:{redis_port}:6379",
        "-v",
        f"{data_dir}:/data",
        "redis:7",
        "redis-server",
        "--appendonly",
        "yes",
        "--requirepass",
        password,
        "--masterauth",
        password,
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)

def _start_network_core(env: dict[str, str], host: str, grpc_port: int, dist_port: int) -> None:
    subprocess.run(["docker", "rm", "-f", NETWORK_CORE_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    publish_host = _network_publish_host(host)
    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        NETWORK_CORE_CONTAINER,
        "--network",
        NETWORK_DOCKER_NETWORK,
        "-p",
        f"{publish_host}:{grpc_port}:{grpc_port}",
        "-p",
        f"{publish_host}:4369:4369",
        "-p",
        f"{publish_host}:{dist_port}:{dist_port}",
        *_docker_env_args(env),
        "mirror-neuron-core:latest",
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)

def _start_network_seed(
    host: Optional[str] = None,
    grpc_port: int = 50051,
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: int = 6379,
    force_new_token: bool = False,
) -> str:
    if _docker_container_running(NETWORK_CORE_CONTAINER):
        console.print("[red]Error: MirrorNeuron network core is already running.[/red]")
        console.print("Use 'mn stop' to stop it first.")
        raise typer.Exit(1)

    host = (host or _detect_lan_ip()).strip() or "127.0.0.1"
    token = _resolve_network_token(force_new=force_new_token)
    node_name = _network_node_name(host)
    external_redis_url = os.getenv("MN_REDIS_URL", "").strip()
    redis_url = external_redis_url or _network_redis_url(token, NETWORK_REDIS_CONTAINER, 6379)
    redis_public_host, redis_public_port_value = _host_port_from_target(
        external_redis_url,
        host,
        str(redis_port),
    ) if external_redis_url else (host, str(redis_port))
    try:
        redis_public_port = int(redis_public_port_value)
    except ValueError:
        redis_public_port = redis_port

    _ensure_network_docker_network()
    if not external_redis_url:
        console.print("=> Starting network Redis...")
        _start_network_redis(host, redis_port, token)

    env = _network_core_env(
        token=token,
        host=host,
        node_name=node_name,
        cluster_nodes=node_name,
        grpc_port=grpc_port,
        dist_port=dist_port,
        redis_url=redis_url,
        redis_public_host=redis_public_host,
        redis_public_port=redis_public_port,
    )

    console.print("=> Starting MirrorNeuron core-only exposed node...")
    _start_network_core(env, host, grpc_port, dist_port)

    console.print("\n[green]MirrorNeuron exposed node is running.[/green]")
    console.print(f"Host: {host}")
    console.print(f"gRPC: {host}:{grpc_port}")
    console.print(f"Node: {node_name}")
    console.print(f"Token: {token}")
    console.print(f"\nOn the main box, add this node with:\n  mn add-node {host} --token {token}")
    return token

def _join_network(
    seed_host: str,
    token: str,
    host: Optional[str] = None,
    grpc_port: int = 50051,
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: Optional[int] = None,
) -> dict:
    from mn_sdk import Client
    from mn_cli.shared import client as local_client

    target = f"{seed_host}:{grpc_port}"
    handshake = Client(target=target, auth_token="", timeout=10).network_handshake(token)
    remote_node = handshake.get("node_name") or _network_node_name(seed_host)

    console.print(f"=> Adding MirrorNeuron network node {remote_node} from {target}...")
    try:
        status = local_client.add_node(remote_node, token=token)
    except TypeError:
        status = local_client.add_node(remote_node)
    except Exception as exc:
        console.print(f"[red]Error: Could not add {remote_node} to the local cluster.[/red]")
        console.print("Check that the local MirrorNeuron core is running, and that the remote host and token are correct.")
        console.print(f"[dim]{exc}[/dim]")
        raise typer.Exit(1) from exc
    console.print(f"[green]Remote node added. Status: {status}[/green]")
    console.print("Run 'mn nodes' or 'mn resource list' to inspect aggregate cluster resources.")
    return handshake

def _stop_network_runtime() -> None:
    for container in [NETWORK_CORE_CONTAINER, NETWORK_REDIS_CONTAINER]:
        subprocess.run(["docker", "rm", "-f", container], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "network", "rm", NETWORK_DOCKER_NETWORK], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

def check_status(pid_file: Path) -> int:
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)
            return 0 # Running
        except (ValueError, OSError):
            return 1 # Stale
    return 2 # Not running

def runtime_compose_available() -> bool:
    return RUNTIME_COMPOSE_FILE.exists() and RUNTIME_COMPOSE_ENV.exists()

def runtime_compose_cmd(*args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        str(RUNTIME_COMPOSE_ENV),
        "-f",
        str(RUNTIME_COMPOSE_FILE),
        *args,
    ]

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
    dist_port = os.getenv("MN_DIST_PORT", DEFAULT_DIST_PORT)
    web_ui_host = _web_ui_host()

    table = Table(title="Service endpoints", show_header=True, header_style="bold")
    table.add_column("Service")
    table.add_column("Host")
    table.add_column("Port")
    table.add_column("URL / target")

    table.add_row("Core gRPC", grpc_host, grpc_port, f"{grpc_host}:{grpc_port}")
    table.add_row("REST API", api_host, api_port, f"http://{api_host}:{api_port}/api/v1")
    if runtime_compose_available():
        table.add_row("Redis", "compose", "6379", "redis://redis:6379/0 (internal)")
        table.add_row(
            "Erlang EPMD",
            os.getenv("MN_EPMD_BIND_HOST", "127.0.0.1"),
            os.getenv("MN_EPMD_PORT", "4369"),
            f"{os.getenv('MN_EPMD_BIND_HOST', '127.0.0.1')}:{os.getenv('MN_EPMD_PORT', '4369')}",
        )
        table.add_row(
            "Erlang dist",
            os.getenv("MN_DIST_BIND_HOST", "127.0.0.1"),
            dist_port,
            f"{os.getenv('MN_DIST_BIND_HOST', '127.0.0.1')}:{dist_port}",
        )
        table.add_row("Context engine", "compose", "50052", "membrane-context-engine:50052 (internal)")
        table.add_row(
            "OpenShell",
            os.getenv("OPENSHELL_GATEWAY_BIND_HOST", "127.0.0.1"),
            os.getenv("OPENSHELL_GATEWAY_PORT", "8080"),
            _openshell_gateway_endpoint(),
        )
    else:
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

def _start_server(
    ip: str = None,
    *,
    token: Optional[str] = None,
    host: Optional[str] = None,
    grpc_port: int = 50051,
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: Optional[int] = None,
):
    if check_status(API_PID_FILE) == 0:
        console.print("[red]Error: MirrorNeuron API is already running.[/red]")
        console.print("Use 'mn stop' to stop it first.")
        raise typer.Exit(1)

    compose_runtime = runtime_compose_available()
    if not compose_runtime:
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

    if ip and not token:
        console.print("[red]Error: mn join requires --token from the main node.[/red]")
        raise typer.Exit(1)

    network_token = token or _resolve_network_token()
    if token:
        _write_network_token(network_token)
    advertised_host = _advertised_network_host(host)
    local_node_name = _network_node_name(advertised_host)
    join_handshake = _handshake_with_main_node(ip, network_token, grpc_port) if ip else None
    seed_node_name = join_handshake["node_name"] if join_handshake else local_node_name
    seed_redis_host = join_handshake["redis_host"] if join_handshake else advertised_host
    seed_redis_port = redis_port or (
        _parse_port(join_handshake["redis_port"], 6379) if join_handshake else _parse_port(os.getenv("MN_REDIS_PORT"), 6379)
    )

    console.print("===========================================")
    if ip:
        console.print(f"Joining Cluster at {ip} in Detached Mode...")
    else:
        console.print("Starting Services in Detached Mode...")
    console.print("===========================================")

    env = os.environ.copy()
    env.setdefault("MN_CORE_HOST", "0.0.0.0")
    env.setdefault("MN_API_HOST", _api_host())
    env.setdefault("MN_REDIS_HOST", _redis_host())
    env.setdefault("MN_EPMD_HOST", "0.0.0.0")
    env.setdefault("MN_DIST_HOST", "0.0.0.0")
    env.setdefault("MN_WEB_UI_HOST", _web_ui_host())
    env.setdefault("MN_CORE_GRPC_TARGET", f"localhost:{os.getenv('MN_GRPC_PORT', '50051')}")
    env["MN_NETWORK_JOIN_TOKEN"] = network_token
    env["MN_NETWORK_ADVERTISE_HOST"] = advertised_host
    env["MN_NETWORK_REDIS_HOST"] = seed_redis_host
    env["MN_NETWORK_REDIS_PORT"] = str(seed_redis_port)
    env.setdefault("MN_NODE_NAME", local_node_name)
    env.setdefault("MN_NODE_ROLE", "runtime")
    env["MN_DIST_PORT"] = str(dist_port)
    if not env.get("MN_COOKIE") or env.get("MN_COOKIE") == "mirrorneuron":
        env["MN_COOKIE"] = _derive_network_secret(network_token, "cookie")
    env.setdefault("MN_GRPC_BIND_HOST", "0.0.0.0")
    env.setdefault("MN_EPMD_BIND_HOST", "0.0.0.0")
    env.setdefault("MN_DIST_BIND_HOST", "0.0.0.0")
    env.setdefault("MN_REDIS_BIND_HOST", "0.0.0.0")
    env.setdefault("ERL_EPMD_ADDRESS", "0.0.0.0")
    env.setdefault(
        "ERL_AFLAGS",
        f"-kernel inet_dist_listen_min {env['MN_DIST_PORT']} inet_dist_listen_max {env['MN_DIST_PORT']}",
    )

    if join_handshake:
        redis_url = _redis_url_with_public_endpoint(
            join_handshake["redis_url"],
            seed_redis_host,
            seed_redis_port,
        )
        env["MN_REDIS_URL"] = redis_url
        env["MN_CLUSTER_NODES"] = seed_node_name
    else:
        env.setdefault("MN_CLUSTER_NODES", local_node_name)
        if not env.get("MN_REDIS_URL") and compose_runtime and _compose_supports_redis_password():
            redis_password = _derive_network_secret(network_token, "redis")
            env["MN_REDIS_PASSWORD"] = redis_password
            env["MN_REDIS_URL"] = f"redis://:{redis_password}@redis:6379/0"
            env.setdefault("MN_CONTEXT_REDIS_URL", f"redis://:{redis_password}@redis:6379/1")

    if not env.get("MN_GRPC_AUTH_TOKEN"):
        env["MN_GRPC_AUTH_TOKEN"] = _resolve_grpc_auth_token()
    if not env.get("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN"):
        env["MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN"] = _resolve_grpc_admin_token()

    if compose_runtime:
        env = _compose_runtime_env(env, ip)
        console.print("=> Starting MirrorNeuron Docker runtime (Compose)...")
        logger.info("Starting MirrorNeuron Docker Compose runtime")
        try:
            subprocess.run(runtime_compose_cmd("up", "-d"), check=True, stdout=subprocess.DEVNULL, env=env)
            console.print("   [green][Started][/green] Docker runtime (Compose project: mirror-neuron)")
        except (FileNotFoundError, subprocess.CalledProcessError):
            console.print("[red]Failed to start MirrorNeuron Docker Compose runtime.[/red]")
            raise typer.Exit(1)
    else:
        console.print("=> Starting MirrorNeuron Core Service (Docker)...")
        logger.info("Starting MirrorNeuron Core Docker container")
        subprocess.run(["docker", "rm", "-f", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

        cmd = ["docker", "run", "-d", "--name", "mirror-neuron-core"]

        cmd.extend(["-e", f"MN_NODE_NAME={env['MN_NODE_NAME']}"])
        cmd.extend(["-e", f"MN_COOKIE={env['MN_COOKIE']}"])
        cmd.extend(["-e", f"MN_GRPC_AUTH_TOKEN={env['MN_GRPC_AUTH_TOKEN']}"])
        cmd.extend(["-e", f"MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN={env['MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN']}"])
        cmd.extend(["-e", f"MN_NETWORK_JOIN_TOKEN={env['MN_NETWORK_JOIN_TOKEN']}"])
        cmd.extend(["-e", f"MN_NETWORK_ADVERTISE_HOST={env['MN_NETWORK_ADVERTISE_HOST']}"])
        cmd.extend(["-e", f"MN_NETWORK_REDIS_HOST={env['MN_NETWORK_REDIS_HOST']}"])
        cmd.extend(["-e", f"MN_NETWORK_REDIS_PORT={env['MN_NETWORK_REDIS_PORT']}"])
        cmd.extend(["-e", f"MN_CLUSTER_NODES={env['MN_CLUSTER_NODES']}"])
        cmd.extend(["-e", f"MN_NODE_ROLE={env['MN_NODE_ROLE']}"])
        cmd.extend(["-e", f"MN_DIST_PORT={env['MN_DIST_PORT']}"])
        cmd.extend(["-e", f"ERL_AFLAGS={env['ERL_AFLAGS']}"])

        core_publish_host = _docker_publish_host(env["MN_CORE_HOST"])
        epmd_publish_host = _docker_publish_host(env["MN_EPMD_HOST"])
        dist_publish_host = _docker_publish_host(env["MN_DIST_HOST"])

        system_name = os.uname().sysname

        if system_name == "Darwin":
            cmd.extend(["-p", f"{core_publish_host}:50051:50051", "-p", f"{epmd_publish_host}:4369:4369"])
            cmd.extend(["-p", f"{dist_publish_host}:{env['MN_DIST_PORT']}:{env['MN_DIST_PORT']}"])
            cmd.extend(["-e", f"MN_REDIS_URL={env.get('MN_REDIS_URL', 'redis://host.docker.internal:6379/0')}"])
            cmd.extend(["-e", "MN_EXECUTOR_MAX_CONCURRENCY=50"])
        else:
            cmd.extend(["--network", "host"])
            cmd.extend(["-e", "MN_EXECUTOR_MAX_CONCURRENCY=50"])
            if env.get("MN_REDIS_URL"):
                cmd.extend(["-e", f"MN_REDIS_URL={env['MN_REDIS_URL']}"])

        if system_name == "Darwin":
            cmd.extend(["-e", "MN_CORE_HOST=0.0.0.0"])
        else:
            cmd.extend(["-e", f"MN_CORE_HOST={env['MN_CORE_HOST']}"])
            cmd.extend(["-e", f"MN_REDIS_HOST={env['MN_REDIS_HOST']}"])
            cmd.extend(["-e", f"ERL_EPMD_ADDRESS={env['MN_EPMD_HOST']}"])

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
    console.print("\nNetwork token:")
    console.print(f"  {network_token}")
    console.print("Add another box with:")
    console.print(f"  mn join {advertised_host} --token {network_token}")
    console.print("Logs are available at:")
    console.print(f"  Core: {BEAM_LOG}")
    console.print(f"  API:  {API_LOG}")
    if WEB_UI_LOG.exists():
        console.print(f"  Web:  {WEB_UI_LOG}")
    console.print("\nRun 'mn stop' to shut down the services.")
    console.print("===========================================")
