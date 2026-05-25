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
DEFAULT_HOST = "localhost"
DEFAULT_GRPC_PORT = "55051"
DEFAULT_API_PORT = "54001"
DEFAULT_EPMD_PORT = "54369"
DEFAULT_DIST_PORT = "54370"
DEFAULT_WEB_UI_PORT = "55173"
DEFAULT_OPENSHELL_GATEWAY_PORT = "58080"
LEGACY_GRPC_PORT = "50051"
LEGACY_API_PORT = "4001"
LEGACY_EPMD_PORT = "4369"
LEGACY_DIST_PORT = "4370"
LEGACY_WEB_UI_PORT = "5173"
LEGACY_OPENSHELL_GATEWAY_PORT = "8080"
WEB_UI_PORT = DEFAULT_WEB_UI_PORT
REDIS_CONTAINER_PORT = 6379
REDIS_DYNAMIC_PORT_START = 56379
REDIS_DYNAMIC_PORT_END = 56478
NETWORK_TOKEN_FILE = DIR / "network.token"
NETWORK_REDIS_ENV_FILE = DIR / "network-redis.env"
NETWORK_DOCKER_NETWORK = "mirror-neuron-network"
NETWORK_CORE_CONTAINER = "mirror-neuron-network-core"
NETWORK_REDIS_CONTAINER = "mirror-neuron-network-redis"


def _openshell_config_dir() -> Path:
    return Path(os.getenv("OPENSHELL_CONFIG_DIR", str(Path.home() / ".config" / "openshell"))).expanduser()


def _openshell_gateway_endpoint(env: Optional[dict[str, str]] = None) -> str:
    values = env or os.environ
    configured_endpoint = values.get("OPENSHELL_GATEWAY_ENDPOINT")
    if configured_endpoint:
        return configured_endpoint

    gateway_name = values.get("OPENSHELL_GATEWAY", "").strip()
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

    return f"http://127.0.0.1:{values.get('OPENSHELL_GATEWAY_PORT', DEFAULT_OPENSHELL_GATEWAY_PORT)}"

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
    compose_env.setdefault("MN_NODE_ROLE", "runtime")

    if ip:
        compose_env.setdefault("MN_DIST_PORT", DEFAULT_DIST_PORT)
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

def _host_port_available(host: str, port: int) -> bool:
    bind_host = "127.0.0.1" if host in {"", "localhost"} else host
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((bind_host, port))
        return True
    except OSError:
        return False
    finally:
        probe.close()

def _container_publishes_port(container_name: str, target_port: int, published_port: int) -> bool:
    try:
        result = subprocess.run(
            ["docker", "port", container_name, f"{target_port}/tcp"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return False
    if result.returncode != 0:
        return False

    for line in result.stdout.splitlines():
        _, _, port_text = line.rpartition(":")
        try:
            if int(port_text) == published_port:
                return True
        except ValueError:
            continue
    return False

def _port_available_or_owned(host: str, port: int, owner_container: str, target_port: int) -> bool:
    return _host_port_available(host, port) or _container_publishes_port(owner_container, target_port, port)

def _find_available_port(host: str, preferred: int, fallback_start: int) -> int:
    if _host_port_available(host, preferred):
        return preferred

    for candidate in range(fallback_start, fallback_start + 100):
        if _host_port_available(host, candidate):
            return candidate
    return preferred

def _find_available_published_port(host: str, preferred: int, owner_container: str, target_port: int) -> int:
    if _port_available_or_owned(host, preferred, owner_container, target_port):
        return preferred

    for candidate in range(REDIS_DYNAMIC_PORT_START, REDIS_DYNAMIC_PORT_END + 1):
        if _port_available_or_owned(host, candidate, owner_container, target_port):
            return candidate
    return 0

def _parse_configured_port(value: object) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    try:
        port = int(str(value).strip())
    except ValueError:
        return None
    if 1 <= port <= 65535:
        return port
    return None

def _resolve_published_redis_port(
    *,
    bind_host: str,
    configured_port: object,
    explicit: bool,
    owner_container: str,
) -> int:
    requested_port = _parse_configured_port(configured_port)
    if explicit:
        if requested_port is None:
            console.print("[red]Error: MN_REDIS_PORT must be a TCP port between 1 and 65535.[/red]")
            raise typer.Exit(1)
        if not _port_available_or_owned(bind_host, requested_port, owner_container, REDIS_CONTAINER_PORT):
            console.print(f"[red]Error: Redis port {requested_port} is already in use.[/red]")
            raise typer.Exit(1)
        return requested_port

    preferred = requested_port or REDIS_DYNAMIC_PORT_START
    if not (REDIS_DYNAMIC_PORT_START <= preferred <= REDIS_DYNAMIC_PORT_END):
        preferred = REDIS_DYNAMIC_PORT_START

    selected = _find_available_published_port(
        bind_host,
        preferred,
        owner_container,
        REDIS_CONTAINER_PORT,
    )
    if selected == 0:
        console.print(
            f"[red]Error: No Redis port is available in {REDIS_DYNAMIC_PORT_START}-{REDIS_DYNAMIC_PORT_END}.[/red]"
        )
        raise typer.Exit(1)
    return selected

def _redis_url_with_database(redis_url: str, database: str) -> str:
    parsed = urlparse(redis_url)
    scheme = parsed.scheme or "redis"
    netloc = parsed.netloc
    if not netloc and parsed.hostname:
        netloc = parsed.hostname
    return f"{scheme}://{netloc}/{database}"

def _avoid_local_compose_port_conflicts(env: dict[str, str]) -> dict[str, str]:
    adjusted = dict(env)
    checks = (
        ("MN_EPMD_BIND_HOST", "MN_EPMD_PORT", int(DEFAULT_EPMD_PORT), int(DEFAULT_EPMD_PORT) + 100, "Erlang EPMD"),
        ("MN_DIST_BIND_HOST", "MN_DIST_PORT", int(DEFAULT_DIST_PORT), int(DEFAULT_DIST_PORT) + 100, "Erlang distribution"),
    )

    for host_key, port_key, default_port, fallback_start, label in checks:
        host = adjusted.get(host_key) or "127.0.0.1"
        port = _parse_port(adjusted.get(port_key), default_port)
        available_port = _find_available_port(host, port, fallback_start)
        if available_port != port:
            adjusted[port_key] = str(available_port)
            console.print(
                f"[yellow]=> {label} port {port} is already in use; using {available_port} for this local runtime.[/yellow]"
            )
            if port_key == "MN_DIST_PORT":
                adjusted["ERL_AFLAGS"] = (
                    f"-kernel inet_dist_listen_min {available_port} inet_dist_listen_max {available_port}"
                )
    if runtime_compose_available():
        updates = {
            key: adjusted[key]
            for key in ("MN_EPMD_PORT", "MN_DIST_PORT", "ERL_AFLAGS")
            if key in adjusted
        }
        _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)

    return adjusted

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
    grpc_port: int = int(DEFAULT_GRPC_PORT),
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: Optional[int] = None,
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
    selected_redis_port = _resolve_network_seed_redis_port(host, redis_port) if not external_redis_url else None
    redis_url = external_redis_url or _network_redis_url(token, NETWORK_REDIS_CONTAINER, 6379)
    redis_public_host, redis_public_port_value = _host_port_from_target(
        external_redis_url,
        host,
        str(selected_redis_port or REDIS_CONTAINER_PORT),
    ) if external_redis_url else (host, str(selected_redis_port))
    try:
        redis_public_port = int(redis_public_port_value)
    except ValueError:
        redis_public_port = selected_redis_port or REDIS_CONTAINER_PORT

    _ensure_network_docker_network()
    if not external_redis_url:
        console.print("=> Starting network Redis...")
        _start_network_redis(host, redis_public_port, token)

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
    grpc_port: int = int(DEFAULT_GRPC_PORT),
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: Optional[int] = None,
) -> dict:
    from mn_sdk import Client
    from mn_cli.shared import client as local_client

    target = f"{seed_host}:{grpc_port}"
    handshake = Client(target=target, auth_token="", timeout=10).network_handshake(token)
    remote_node = handshake.get("node_name") or _network_node_name(seed_host)
    redis_host, redis_port, redis_url = _validate_remote_redis_details(handshake, seed_host, token)

    console.print(f"=> Adding MirrorNeuron network node {remote_node} from {target}...")
    console.print(f"=> Remote Redis advertised at {redis_host}:{redis_port}.")
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
    console.print(f"Remote Redis URL: {redis_url}")
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

def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key] = value
    return values

def _write_env_file_values(path: Path, updates: dict[str, str]) -> None:
    try:
        original_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        original_lines = []

    lines: list[str] = []
    seen: set[str] = set()
    for line in original_lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _ = stripped.split("=", 1)
            if key in updates:
                lines.append(f"{key}={updates[key]}")
                seen.add(key)
                continue
        lines.append(line)

    for key, value in updates.items():
        if key not in seen:
            lines.append(f"{key}={value}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod env file %s", path, exc_info=True)

def _runtime_base_env(compose_runtime: bool) -> dict[str, str]:
    env = _read_env_file(RUNTIME_COMPOSE_ENV) if compose_runtime else {}
    env.update(os.environ)
    return env

def _env_or_default(env: dict[str, str], key: str, default: str, legacy_default: Optional[str] = None) -> str:
    value = str(env.get(key) or "").strip()
    if os.getenv(key, "").strip():
        return os.getenv(key, "").strip()
    if not value or (legacy_default is not None and value == legacy_default):
        return default
    return value

def _valid_port_text(value: str, default: str) -> str:
    parsed = _parse_configured_port(value)
    if parsed is None:
        return default
    return str(parsed)

def _native_endpoint_host(host: str) -> str:
    normalized = (host or "").strip()
    if normalized in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    return normalized

def _runtime_blueprint_env_updates(env: dict[str, str]) -> dict[str, str]:
    updates: dict[str, str] = {}
    for key in ("MN_BLUEPRINT_REPO", "MN_DEV_LOCAL_BLUEPRINT_REPO", "DEV_LOCAL_BLUEPRINT_REPO", "MN_RUNS_ROOT"):
        value = str(env.get(key) or "").strip()
        if value:
            updates[key] = value
    return updates

def _ensure_compose_native_port_settings(env: dict[str, str]) -> dict[str, str]:
    adjusted = dict(env)
    grpc_port = _valid_port_text(
        _env_or_default(adjusted, "MN_GRPC_PORT", DEFAULT_GRPC_PORT, LEGACY_GRPC_PORT),
        DEFAULT_GRPC_PORT,
    )
    api_port = _valid_port_text(
        _env_or_default(adjusted, "MN_API_PORT", DEFAULT_API_PORT, LEGACY_API_PORT),
        DEFAULT_API_PORT,
    )
    epmd_port = _valid_port_text(
        _env_or_default(adjusted, "MN_EPMD_PORT", DEFAULT_EPMD_PORT, LEGACY_EPMD_PORT),
        DEFAULT_EPMD_PORT,
    )
    dist_port = _valid_port_text(
        _env_or_default(adjusted, "MN_DIST_PORT", DEFAULT_DIST_PORT, LEGACY_DIST_PORT),
        DEFAULT_DIST_PORT,
    )
    web_ui_port = _valid_port_text(
        _env_or_default(adjusted, "MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT, LEGACY_WEB_UI_PORT),
        DEFAULT_WEB_UI_PORT,
    )
    openshell_port = _valid_port_text(
        _env_or_default(
            adjusted,
            "OPENSHELL_GATEWAY_PORT",
            DEFAULT_OPENSHELL_GATEWAY_PORT,
            LEGACY_OPENSHELL_GATEWAY_PORT,
        ),
        DEFAULT_OPENSHELL_GATEWAY_PORT,
    )
    openshell_bind_host = adjusted.get("OPENSHELL_GATEWAY_BIND_HOST") or "127.0.0.1"
    openshell_endpoint = _env_or_default(
        adjusted,
        "OPENSHELL_GATEWAY_ENDPOINT",
        f"http://{_native_endpoint_host(openshell_bind_host)}:{openshell_port}",
        f"http://127.0.0.1:{LEGACY_OPENSHELL_GATEWAY_PORT}",
    )
    if not os.getenv("OPENSHELL_GATEWAY_ENDPOINT", "").strip() and openshell_endpoint == (
        f"https://127.0.0.1:{LEGACY_OPENSHELL_GATEWAY_PORT}"
    ):
        openshell_endpoint = f"http://{_native_endpoint_host(openshell_bind_host)}:{openshell_port}"
    core_grpc_target = adjusted.get("MN_CORE_GRPC_TARGET") or f"localhost:{grpc_port}"
    if not os.getenv("MN_CORE_GRPC_TARGET", "").strip() and core_grpc_target == f"localhost:{LEGACY_GRPC_PORT}":
        core_grpc_target = f"localhost:{grpc_port}"

    updates = {
        "MN_GRPC_BIND_HOST": adjusted.get("MN_GRPC_BIND_HOST") or "127.0.0.1",
        "MN_GRPC_PORT": grpc_port,
        "MN_CORE_GRPC_TARGET": core_grpc_target,
        "MN_API_HOST": adjusted.get("MN_API_HOST") or DEFAULT_HOST,
        "MN_API_PORT": api_port,
        "MN_EPMD_BIND_HOST": adjusted.get("MN_EPMD_BIND_HOST") or "127.0.0.1",
        "MN_EPMD_PORT": epmd_port,
        "MN_DIST_BIND_HOST": adjusted.get("MN_DIST_BIND_HOST") or "127.0.0.1",
        "MN_DIST_PORT": dist_port,
        "MN_WEB_UI_HOST": adjusted.get("MN_WEB_UI_HOST") or DEFAULT_HOST,
        "MN_WEB_UI_PORT": web_ui_port,
        "OPENSHELL_GATEWAY_BIND_HOST": openshell_bind_host,
        "OPENSHELL_GATEWAY_PORT": openshell_port,
        "OPENSHELL_GATEWAY_ENDPOINT": openshell_endpoint,
    }
    updates.update(_runtime_blueprint_env_updates(adjusted))
    if not adjusted.get("ERL_AFLAGS") or LEGACY_DIST_PORT in adjusted.get("ERL_AFLAGS", ""):
        updates["ERL_AFLAGS"] = f"-kernel inet_dist_listen_min {dist_port} inet_dist_listen_max {dist_port}"

    adjusted.update(updates)
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    return adjusted

def _ensure_compose_redis_publish_settings(
    env: dict[str, str],
    *,
    token: str,
    advertised_host: str,
) -> tuple[dict[str, str], int]:
    adjusted = dict(env)
    bind_host = adjusted.get("MN_REDIS_BIND_HOST") or "0.0.0.0"
    explicit_port = bool(os.getenv("MN_REDIS_PORT", "").strip())
    redis_port = _resolve_published_redis_port(
        bind_host=bind_host,
        configured_port=adjusted.get("MN_REDIS_PORT"),
        explicit=explicit_port,
        owner_container="mirror-neuron-redis",
    )
    redis_password = _derive_network_secret(token, "redis")

    adjusted["MN_REDIS_BIND_HOST"] = bind_host
    adjusted["MN_REDIS_PORT"] = str(redis_port)
    adjusted["MN_REDIS_PASSWORD"] = redis_password
    adjusted["MN_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/0"
    adjusted["MN_CONTEXT_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/1"
    adjusted.setdefault("MN_NETWORK_JOIN_TOKEN", token)
    adjusted["MN_NETWORK_REDIS_HOST"] = os.getenv("MN_NETWORK_REDIS_HOST", "").strip() or advertised_host
    adjusted["MN_NETWORK_REDIS_PORT"] = str(redis_port)

    _write_env_file_values(
        RUNTIME_COMPOSE_ENV,
        {
            "MN_NETWORK_JOIN_TOKEN": token,
            "MN_REDIS_BIND_HOST": bind_host,
            "MN_REDIS_PORT": str(redis_port),
            "MN_REDIS_PASSWORD": redis_password,
            "MN_REDIS_URL": adjusted["MN_REDIS_URL"],
            "MN_CONTEXT_REDIS_URL": adjusted["MN_CONTEXT_REDIS_URL"],
            "MN_NETWORK_REDIS_HOST": adjusted["MN_NETWORK_REDIS_HOST"],
            "MN_NETWORK_REDIS_PORT": adjusted["MN_NETWORK_REDIS_PORT"],
        },
    )
    return adjusted, redis_port

def _resolve_network_seed_redis_port(host: str, requested_port: Optional[int]) -> int:
    persisted = _read_env_file(NETWORK_REDIS_ENV_FILE).get("MN_REDIS_PORT")
    env_port = os.getenv("MN_REDIS_PORT", "").strip()
    explicit = requested_port is not None or bool(env_port)
    configured_port: object = requested_port if requested_port is not None else (env_port or persisted)
    publish_host = _network_publish_host(host)
    selected_port = _resolve_published_redis_port(
        bind_host=publish_host,
        configured_port=configured_port,
        explicit=explicit,
        owner_container=NETWORK_REDIS_CONTAINER,
    )
    _write_env_file_values(NETWORK_REDIS_ENV_FILE, {"MN_REDIS_PORT": str(selected_port)})
    return selected_port

def _validate_remote_redis_details(handshake: dict, seed_host: str, token: str) -> tuple[str, int, str]:
    redis_host = str(handshake.get("redis_host") or "").strip()
    redis_port = _parse_configured_port(handshake.get("redis_port"))
    redis_url = str(handshake.get("redis_url") or "").strip()

    if not redis_host or redis_port is None or not redis_url:
        console.print(f"[red]Error: Remote node at {seed_host} did not advertise complete Redis details.[/red]")
        raise typer.Exit(1)

    parsed = urlparse(redis_url)
    if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname or not parsed.port:
        console.print(f"[red]Error: Remote node at {seed_host} returned an invalid Redis URL.[/red]")
        raise typer.Exit(1)
    if parsed.password != _derive_network_secret(token, "redis"):
        console.print(f"[red]Error: Remote node at {seed_host} did not advertise token-authenticated Redis.[/red]")
        raise typer.Exit(1)

    return redis_host, redis_port, redis_url

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

def _native_service_endpoints(ip: Optional[str] = None, web_ui_available: bool = False) -> list[dict[str, str]]:
    runtime_env = _runtime_base_env(runtime_compose_available())
    rows: list[dict[str, str]] = []

    if runtime_compose_available():
        grpc_host = _native_endpoint_host(runtime_env.get("MN_GRPC_BIND_HOST", "127.0.0.1"))
        grpc_port = runtime_env.get("MN_GRPC_PORT", DEFAULT_GRPC_PORT)
        api_host = _native_endpoint_host(runtime_env.get("MN_API_HOST") or _api_host())
        api_port = runtime_env.get("MN_API_PORT", DEFAULT_API_PORT)
        redis_host = _native_endpoint_host(
            runtime_env.get("MN_NETWORK_REDIS_HOST")
            or runtime_env.get("MN_REDIS_BIND_HOST", "0.0.0.0")
        )
        redis_port = runtime_env.get("MN_NETWORK_REDIS_PORT") or runtime_env.get(
            "MN_REDIS_PORT", str(REDIS_CONTAINER_PORT)
        )
        epmd_host = _native_endpoint_host(runtime_env.get("MN_EPMD_BIND_HOST", "127.0.0.1"))
        epmd_port = runtime_env.get("MN_EPMD_PORT", DEFAULT_EPMD_PORT)
        dist_host = _native_endpoint_host(runtime_env.get("MN_DIST_BIND_HOST", "127.0.0.1"))
        dist_port = runtime_env.get("MN_DIST_PORT", DEFAULT_DIST_PORT)
        openshell_host = _native_endpoint_host(runtime_env.get("OPENSHELL_GATEWAY_BIND_HOST", "127.0.0.1"))
        openshell_port = runtime_env.get("OPENSHELL_GATEWAY_PORT", DEFAULT_OPENSHELL_GATEWAY_PORT)
        openshell_endpoint = _openshell_gateway_endpoint(runtime_env)
    else:
        core_host = runtime_env.get("MN_CORE_HOST") or _core_host()
        grpc_host, grpc_port = _host_port_from_target(
            runtime_env.get(
                "MN_GRPC_TARGET",
                runtime_env.get("MN_CORE_GRPC_TARGET", f"{core_host}:{DEFAULT_GRPC_PORT}"),
            ),
            core_host,
            runtime_env.get("MN_GRPC_PORT", DEFAULT_GRPC_PORT),
        )
        api_host = runtime_env.get("MN_API_HOST") or _api_host()
        api_port = runtime_env.get("MN_API_PORT", DEFAULT_API_PORT)
        redis_host, redis_port = _redis_host_port(ip)
        epmd_host = runtime_env.get("MN_EPMD_HOST") or _epmd_host()
        epmd_port = runtime_env.get("MN_EPMD_PORT", DEFAULT_EPMD_PORT)
        dist_host = runtime_env.get("MN_DIST_HOST") or _dist_host()
        dist_port = runtime_env.get("MN_DIST_PORT", DEFAULT_DIST_PORT)
        openshell_host = ""
        openshell_port = ""
        openshell_endpoint = ""

    rows.extend(
        [
            {"service": "Core gRPC", "host": grpc_host, "port": str(grpc_port), "target": f"{grpc_host}:{grpc_port}"},
            {
                "service": "REST API",
                "host": api_host,
                "port": str(api_port),
                "target": f"http://{api_host}:{api_port}/api/v1",
            },
            {
                "service": "Redis",
                "host": redis_host,
                "port": str(redis_port),
                "target": f"redis://{redis_host}:{redis_port}/0"
                + (" (auth required)" if runtime_compose_available() else ""),
            },
            {
                "service": "Erlang EPMD",
                "host": epmd_host,
                "port": str(epmd_port),
                "target": f"{epmd_host}:{epmd_port}",
            },
            {
                "service": "Erlang dist",
                "host": dist_host,
                "port": str(dist_port),
                "target": f"{dist_host}:{dist_port}",
            },
        ]
    )
    if runtime_compose_available():
        rows.append(
            {
                "service": "OpenShell",
                "host": openshell_host,
                "port": str(openshell_port),
                "target": openshell_endpoint,
            }
        )
    if web_ui_available:
        web_ui_host = runtime_env.get("MN_WEB_UI_HOST") or _web_ui_host()
        web_ui_port = runtime_env.get("MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)
        rows.append(
            {
                "service": "Web UI",
                "host": web_ui_host,
                "port": str(web_ui_port),
                "target": f"http://{web_ui_host}:{web_ui_port}",
            }
        )
    return rows

def native_service_ports() -> list[dict[str, str]]:
    return _native_service_endpoints(web_ui_available=True)

def _print_service_endpoints(ip: Optional[str], web_ui_available: bool):
    rows = _native_service_endpoints(ip=ip, web_ui_available=web_ui_available)

    table = Table(title="Service endpoints", show_header=True, header_style="bold")
    table.add_column("Service")
    table.add_column("Host")
    table.add_column("Port")
    table.add_column("URL / target")

    for row in rows:
        table.add_row(row["service"], row["host"], row["port"], row["target"])

    console.print(table)

def _start_web_ui_if_installed(runtime_env: Optional[dict[str, str]] = None) -> bool:
    web_ui_dir = find_web_ui_dir()
    if not web_ui_dir:
        return False

    status = check_status(WEB_UI_PID_FILE)
    if status == 0:
        console.print("[yellow]=> Web UI is already running, skipping.[/yellow]")
        return True
    if status == 1:
        WEB_UI_PID_FILE.unlink(missing_ok=True)

    env = os.environ.copy()
    if runtime_env:
        env.update(runtime_env)
    web_ui_host = env.get("MN_WEB_UI_HOST") or _web_ui_host()
    env.setdefault("MN_WEB_UI_HOST", web_ui_host)
    env.setdefault("MN_API_HOST", _api_host())
    env.setdefault("MN_API_PORT", os.getenv("MN_API_PORT", DEFAULT_API_PORT))
    web_ui_port = env.get("MN_WEB_UI_PORT") or os.getenv("MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)
    env["MN_WEB_UI_PORT"] = web_ui_port
    console.print(f"=> Starting mn-web-ui (Vite on {web_ui_host}:{web_ui_port})...")
    with open(WEB_UI_LOG, "w") as out:
        p_web = subprocess.Popen(
            ["npm", "run", "dev", "--", "--host", web_ui_host, "--port", web_ui_port],
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
    grpc_port: int = int(DEFAULT_GRPC_PORT),
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
    if join_handshake:
        _validate_remote_redis_details(join_handshake, ip, network_token)
    env = _runtime_base_env(compose_runtime)
    local_redis_port: Optional[int] = None
    if compose_runtime:
        env = _ensure_compose_native_port_settings(env)
        env, local_redis_port = _ensure_compose_redis_publish_settings(
            env,
            token=network_token,
            advertised_host=advertised_host,
        )

    seed_node_name = join_handshake["node_name"] if join_handshake else local_node_name
    seed_redis_host = join_handshake["redis_host"] if join_handshake else advertised_host
    seed_redis_port = redis_port or (
        _parse_port(join_handshake["redis_port"], REDIS_CONTAINER_PORT)
        if join_handshake
        else (local_redis_port or _parse_port(os.getenv("MN_REDIS_PORT"), REDIS_CONTAINER_PORT))
    )

    console.print("===========================================")
    if ip:
        console.print(f"Joining Cluster at {ip} in Detached Mode...")
    else:
        console.print("Starting Services in Detached Mode...")
    console.print("===========================================")

    env.setdefault("MN_CORE_HOST", "0.0.0.0")
    env.setdefault("MN_API_HOST", _api_host())
    env.setdefault("MN_REDIS_HOST", _redis_host())
    env.setdefault("MN_EPMD_HOST", "0.0.0.0")
    env.setdefault("MN_DIST_HOST", "0.0.0.0")
    env.setdefault("MN_WEB_UI_HOST", _web_ui_host())
    env.setdefault("MN_GRPC_PORT", str(grpc_port))
    env.setdefault("MN_API_PORT", DEFAULT_API_PORT)
    env.setdefault("MN_EPMD_PORT", DEFAULT_EPMD_PORT)
    env.setdefault("MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)
    env.setdefault("MN_CORE_GRPC_TARGET", f"localhost:{env.get('MN_GRPC_PORT', DEFAULT_GRPC_PORT)}")
    env["MN_NETWORK_JOIN_TOKEN"] = network_token
    env["MN_NETWORK_ADVERTISE_HOST"] = advertised_host
    env["MN_NETWORK_REDIS_HOST"] = seed_redis_host
    env["MN_NETWORK_REDIS_PORT"] = str(seed_redis_port)
    env.setdefault("MN_NODE_NAME", local_node_name)
    env.setdefault("MN_NODE_ROLE", "runtime")
    if ip or not compose_runtime:
        env["MN_DIST_PORT"] = str(dist_port)
    else:
        env.setdefault("MN_DIST_PORT", str(dist_port))
    if not env.get("MN_COOKIE") or env.get("MN_COOKIE") == "mirrorneuron":
        if compose_runtime and not ip:
            env["MN_COOKIE"] = _resolve_mn_cookie()
        else:
            env["MN_COOKIE"] = _derive_network_secret(network_token, "cookie")
    env.setdefault("MN_GRPC_BIND_HOST", "0.0.0.0")
    env.setdefault("MN_EPMD_BIND_HOST", "0.0.0.0")
    env.setdefault("MN_DIST_BIND_HOST", "0.0.0.0")
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
        env["MN_CONTEXT_REDIS_URL"] = _redis_url_with_database(redis_url, "1")
        env["MN_CLUSTER_NODES"] = seed_node_name
        if compose_runtime:
            _write_env_file_values(
                RUNTIME_COMPOSE_ENV,
                {
                    "MN_NODE_NAME": env["MN_NODE_NAME"],
                    "MN_CLUSTER_NODES": seed_node_name,
                    "MN_REDIS_URL": redis_url,
                    "MN_CONTEXT_REDIS_URL": env["MN_CONTEXT_REDIS_URL"],
                    "MN_NETWORK_REDIS_HOST": seed_redis_host,
                    "MN_NETWORK_REDIS_PORT": str(seed_redis_port),
                    "MN_COOKIE": env["MN_COOKIE"],
                },
            )
    else:
        env.setdefault("MN_CLUSTER_NODES", local_node_name)
        if compose_runtime and _compose_supports_redis_password():
            redis_password = _derive_network_secret(network_token, "redis")
            env["MN_REDIS_PASSWORD"] = redis_password
            env["MN_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/0"
            env["MN_CONTEXT_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/1"

    if not env.get("MN_GRPC_AUTH_TOKEN"):
        env["MN_GRPC_AUTH_TOKEN"] = _resolve_grpc_auth_token()
    if not env.get("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN"):
        env["MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN"] = _resolve_grpc_admin_token()
    if compose_runtime:
        _write_env_file_values(
            RUNTIME_COMPOSE_ENV,
            {
                "MN_GRPC_AUTH_TOKEN": env["MN_GRPC_AUTH_TOKEN"],
                "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN": env["MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN"],
            },
        )

    if compose_runtime:
        env = _compose_runtime_env(env, ip)
        if not ip:
            env = _avoid_local_compose_port_conflicts(env)
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
        cmd.extend(["-e", f"MN_GRPC_PORT={env['MN_GRPC_PORT']}"])
        cmd.extend(["-e", f"MN_DIST_PORT={env['MN_DIST_PORT']}"])
        cmd.extend(["-e", f"ERL_AFLAGS={env['ERL_AFLAGS']}"])

        core_publish_host = _docker_publish_host(env["MN_CORE_HOST"])
        epmd_publish_host = _docker_publish_host(env["MN_EPMD_HOST"])
        dist_publish_host = _docker_publish_host(env["MN_DIST_HOST"])

        system_name = os.uname().sysname

        if system_name == "Darwin":
            cmd.extend(
                [
                    "-p",
                    f"{core_publish_host}:{env['MN_GRPC_PORT']}:{env['MN_GRPC_PORT']}",
                    "-p",
                    f"{epmd_publish_host}:{env['MN_EPMD_PORT']}:4369",
                ]
            )
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
        console.print(f"=> Starting mn-api (REST on port {env.get('MN_API_PORT', DEFAULT_API_PORT)})...")
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

    web_ui_available = _start_web_ui_if_installed(env)

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
