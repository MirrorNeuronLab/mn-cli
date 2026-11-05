"""Pure command, environment and endpoint projections for local runtime launch.

Service discovery, credential resolution, filesystem writes and process lifecycle
remain in ``server``. These helpers use only their arguments and preserve command
ordering and the existing endpoint normalization rules.
"""

from typing import Optional
from urllib.parse import urlparse


def _erl_aflags(dist_port: str | int) -> str:
    return (
        f"-connect_all false -kernel prevent_overlapping_partitions false "
        f"inet_dist_listen_min {dist_port} inet_dist_listen_max {dist_port}"
    )


def _erl_aflags_needs_update(value: Optional[str], dist_port: str | int) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    return (
        "-connect_all false" not in text
        or "prevent_overlapping_partitions false" not in text
        or f"inet_dist_listen_min {dist_port}" not in text
        or f"inet_dist_listen_max {dist_port}" not in text
    )


def _distributed_core_command() -> list[str]:
    command = (
        'case "${MN_NODE_NAME:-}" in ""|nonode@nohost|*[!a-zA-Z0-9_.@-]*|@*|*@|*@*@*|[!a-zA-Z0-9_]* ) '
        'echo "error: invalid MN_NODE_NAME; run mn runtime start to repair configuration" >&2; exit 1;; esac; '
        'case "$MN_NODE_NAME" in *@*) ;; *) echo "error: MN_NODE_NAME requires name@host" >&2; exit 1;; esac; '
        'case "${MN_NODE_NAME#*@}" in [!a-zA-Z0-9]*) echo "error: invalid node hostname" >&2; exit 1;; esac; '
        'if [ -x "bin/mirror_neuron" ]; then '
        'if [ -n "${MN_NODE_NAME:-}" ]; then '
        'if [ -z "${MN_COOKIE:-}" ] || [ "${MN_COOKIE:-}" = "mirrorneuron" ]; then '
        'echo "MN_COOKIE must be set to a non-default secret when MN_NODE_NAME enables distributed Erlang" >&2; exit 1; '
        "fi; "
        "unset ERL_EPMD_ADDRESS; "
        "epmd_bin=\"$(find erts-* -path '*/bin/epmd' -type f | head -n 1)\"; "
        'if [ -n "$epmd_bin" ]; then "$epmd_bin" -daemon; else epmd -daemon; fi; '
        "export RELEASE_DISTRIBUTION=name; "
        'export RELEASE_NODE="$MN_NODE_NAME"; '
        'export RELEASE_COOKIE="$MN_COOKIE"; '
        "fi; "
        "exec bin/mirror_neuron start; "
        "fi; "
        'if [ -n "${MN_NODE_NAME:-}" ]; then '
        'if [ -z "${MN_COOKIE:-}" ] || [ "${MN_COOKIE:-}" = "mirrorneuron" ]; then '
        'echo "MN_COOKIE must be set to a non-default secret when MN_NODE_NAME enables distributed Erlang" >&2; exit 1; '
        "fi; "
        'dist_port="${MN_DIST_PORT:-4370}"; '
        "unset ERL_EPMD_ADDRESS; "
        "epmd -daemon; "
        'exec elixir --name "$MN_NODE_NAME" --cookie "$MN_COOKIE" --erl '
        '"-kernel inet_dist_listen_min ${dist_port} inet_dist_listen_max ${dist_port}" -S mix run --no-halt; '
        "fi"
    )
    return ["sh", "-c", command]


def _docker_publish_host(host: str) -> str:
    return "127.0.0.1" if host == "localhost" else host


def _network_publish_host(host: str) -> str:
    normalized = (host or "").strip().lower()
    if normalized in {"", "localhost", "127.0.0.1", "::1"}:
        return "127.0.0.1"
    return "0.0.0.0"


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


def _redis_url_with_database(redis_url: str, database: str) -> str:
    parsed = urlparse(redis_url)
    scheme = parsed.scheme or "redis"
    netloc = parsed.netloc
    if not netloc and parsed.hostname:
        netloc = parsed.hostname
    return f"{scheme}://{netloc}/{database}"


def _docker_network_env(mode: str, name: str, alias: str) -> dict[str, str]:
    driver = "overlay" if mode == "overlay" else "bridge"
    return {
        "MN_DOCKER_NETWORK_MODE": mode,
        "MN_DOCKER_NETWORK_NAME": name,
        "MN_DOCKER_NETWORK_DRIVER": driver,
        "MN_DOCKER_NETWORK_ATTACHABLE": "true" if mode == "overlay" else "false",
        "MN_DOCKER_NETWORK_EXTERNAL": "true",
        "MN_NODE_ALIAS": alias,
    }


def _docker_network_run_args(mode: str, name: str, alias: str) -> list[str]:
    if mode == "disabled":
        return []
    return ["--network", name, "--network-alias", alias]


def _redis_url_with_public_endpoint(redis_url: str, host: str, port: int) -> str:
    parsed = urlparse(redis_url)
    scheme = parsed.scheme or "redis"
    path = parsed.path or "/0"
    userinfo = ""
    if "@" in parsed.netloc:
        userinfo = f"{parsed.netloc.rsplit('@', 1)[0]}@"
    return f"{scheme}://{userinfo}{host}:{port}{path}"


def _docker_env_args(env: dict[str, str]) -> list[str]:
    args: list[str] = []
    for key in sorted(env):
        if key.startswith("MN_") or key in {
            "ERL_AFLAGS",
            "ERL_EPMD_ADDRESS",
            "ERL_EPMD_PORT",
        }:
            args.extend(["-e", f"{key}={env[key]}"])
    return args


def _compose_profiles_with(value: object, required_profile: str) -> str:
    profiles: list[str] = []
    seen: set[str] = set()
    for raw_part in str(value or "").split(","):
        profile = raw_part.strip()
        if not profile:
            continue
        key = profile.lower()
        if key in seen:
            continue
        profiles.append(profile)
        seen.add(key)
    if required_profile.lower() not in seen:
        profiles.append(required_profile)
    return ",".join(profiles)


def _split_env_list(value: object) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _yaml_double_quote_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


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


def _cluster_endpoint_host(env: dict[str, str], host: str) -> str:
    normalized = (host or "").strip()
    advertised = str(env.get("MN_NETWORK_ADVERTISE_HOST") or "").strip()
    if advertised and _network_publish_host(advertised) == "0.0.0.0":
        if normalized in {"", "0.0.0.0", "::", "127.0.0.1", "localhost", "::1"}:
            return advertised
    return _native_endpoint_host(normalized)


def _host_port_from_target(
    target: str, default_host: str, default_port: str
) -> tuple[str, str]:
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
