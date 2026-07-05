import grpc

from .common import *

def _resolve_runtime_cluster_model(*, requirement: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any] | None:
    try:
        return resolve_cluster_model_placement(entry, resource_report=_runtime_resource_report)
    except Exception:
        logger.exception("Failed to resolve cluster model placement for %s", entry.get("id") or entry.get("model"))
        return None

def _cluster_node_grpc_target(node_name: str) -> str:
    return _cluster_node_endpoint(node_name)["grpc_target"]

def _cluster_node_endpoint(node_name: str) -> dict[str, Any]:
    node_name = str(node_name or "").strip()
    if not node_name:
        raise RuntimeError("cluster model placement did not return a target node")
    try:
        summary = json.loads(client.get_system_summary())
    except Exception as exc:
        raise RuntimeError(f"could not inspect cluster nodes for {node_name}: {exc}") from exc
    nodes = summary.get("nodes") if isinstance(summary, dict) else None
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        if str(node.get("name") or node.get("node") or "").strip() != node_name:
            continue
        host = str(node.get("grpc_host") or node.get("address") or "").strip()
        port = str(node.get("grpc_port") or "").strip()
        if not host or not port:
            raise RuntimeError(f"cluster node {node_name} does not advertise grpc_host/grpc_port")
        return {"grpc_target": f"{host}:{port}", "host": host, "port": port, "node": node}
    raise RuntimeError(f"cluster node {node_name} was not found in runtime summary")

def _node_native_sdk_grpc_info(node: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[Any] = [node.get("native_sdk_grpc")]
    hardware = node.get("hardware")
    if isinstance(hardware, dict):
        candidates.append(hardware.get("native_sdk_grpc"))
    node_info = node.get("node_info")
    if isinstance(node_info, dict):
        candidates.append(node_info.get("native_sdk_grpc"))
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return candidate
    return None

def _cluster_node_native_sdk_endpoint(node_name: str, node: dict[str, Any]) -> dict[str, str]:
    native = _node_native_sdk_grpc_info(node)
    if not native:
        raise RuntimeError(
            f"cluster node {node_name} does not advertise native SDK gRPC; "
            "restart that worker with an updated `mn runtime start --worker-node` "
            "so runtime model preparation can run outside Core"
        )
    if native.get("enabled") is False:
        raise RuntimeError(
            f"cluster node {node_name} advertises native SDK gRPC as disabled; "
            "start the node-local mn-python-sdk native runtime service before preparing models"
        )

    target = str(native.get("target") or "").strip()
    host = str(native.get("host") or "").strip()
    port = str(native.get("port") or "").strip()
    if target and (not host or not port) and ":" in target:
        parsed_host, parsed_port = target.rsplit(":", 1)
        host = host or parsed_host.strip()
        port = port or parsed_port.strip()
    if not target and host and port:
        target = f"{host}:{port}"
    if not target or not host or not port:
        raise RuntimeError(f"cluster node {node_name} advertises incomplete native SDK gRPC metadata")
    return {"target": target, "host": host, "port": port}

def _runtime_model_prepare_client(node: str, node_endpoint: dict[str, Any]) -> Client:
    timeout = _runtime_model_prepare_timeout_seconds()
    if _cluster_node_endpoint_is_local(node_endpoint):
        return Client(
            target=config.grpc_target,
            timeout=timeout,
            auth_token=config.grpc_auth_token,
            admin_token=config.grpc_admin_token,
        )

    native_endpoint = _cluster_node_native_sdk_endpoint(node, node_endpoint["node"])
    return Client(
        target=native_endpoint["target"],
        timeout=timeout,
        auth_token=config.grpc_auth_token,
        admin_token=config.grpc_admin_token,
    )

def _prepare_runtime_model_with_retry(runtime_client: Client, prepare_payload: dict[str, Any]) -> str:
    for attempt in range(2):
        try:
            return runtime_client.prepare_runtime_model(prepare_payload)
        except grpc.RpcError as exc:
            if attempt == 0 and _retryable_runtime_model_prepare_error(exc):
                console.print(
                    "[yellow]Runtime model prepare timed out or became unavailable; "
                    "retrying once with the same request...[/yellow]"
                )
                continue
            raise
    raise RuntimeError("runtime model prepare retry loop exited unexpectedly")

def _retryable_runtime_model_prepare_error(exc: grpc.RpcError) -> bool:
    try:
        code = exc.code()
    except Exception:
        return False
    return code in {grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE}

def _container_local_api_base_to_node_host(api_base: str, node_host: str) -> str:
    base = str(api_base or "").strip()
    host = str(node_host or "").strip()
    if not base or not host:
        return base
    parsed = urllib.parse.urlparse(base)
    hostname = (parsed.hostname or "").lower()
    if hostname not in {"host.docker.internal", "localhost", "127.0.0.1", "0.0.0.0"}:
        return base
    netloc = host
    port = parsed.port
    if port == 12434:
        try:
            port = int(os.environ.get("MN_DOCKER_MODEL_RUNNER_PROXY_PORT", "12435"))
        except ValueError:
            port = 12435
    if port:
        netloc = f"{host}:{port}"
    return urllib.parse.urlunparse(
        (
            parsed.scheme or "http",
            netloc,
            parsed.path or "",
            parsed.params or "",
            parsed.query or "",
            parsed.fragment or "",
        )
    )

def _prefer_default_single_node_agent_placement(manifest: dict[str, Any]) -> None:
    if str(os.environ.get("MN_BLUEPRINT_SINGLE_NODE_AGENTS", "1")).strip().lower() in FALSE_VALUES:
        return
    node_name = _local_runtime_node_name()
    if not node_name:
        return
    for node in manifest_nodes(manifest):
        if _node_has_explicit_node_placement(node):
            continue
        policies = node.get("policies") if isinstance(node.get("policies"), dict) else {}
        scheduler = policies.get("scheduler") if isinstance(policies.get("scheduler"), dict) else {}
        scheduler.setdefault("preferred_node", node_name)
        policies["scheduler"] = scheduler
        node["policies"] = policies

def _local_runtime_node_name() -> str:
    try:
        summary = json.loads(client.get_system_summary())
    except Exception:
        return ""
    nodes = summary.get("nodes") if isinstance(summary, dict) else None
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        if node.get("self?") is True or node.get("self") is True:
            return str(node.get("name") or node.get("node") or "").strip()
    return ""

def _node_has_explicit_node_placement(node: dict[str, Any]) -> bool:
    policies = node.get("policies") if isinstance(node.get("policies"), dict) else {}
    scheduler = policies.get("scheduler") if isinstance(policies.get("scheduler"), dict) else {}
    if str(scheduler.get("preferred_node") or scheduler.get("preferredNode") or "").strip():
        return True
    if str(policies.get("preferred_node") or policies.get("preferredNode") or "").strip():
        return True
    constraints = node.get("constraints") if isinstance(node.get("constraints"), list) else []
    return any(_is_node_name_constraint(constraint) for constraint in constraints if isinstance(constraint, dict))

def _is_node_name_constraint(constraint: dict[str, Any]) -> bool:
    attribute = str(
        constraint.get("attribute")
        or constraint.get("target")
        or constraint.get("l_target")
        or ""
    ).strip("${}")
    return attribute in {"node", "node.name", "node.unique.name"}

def _install_runtime_cluster_model(
    *,
    requirement: dict[str, Any],
    entry: dict[str, Any],
    model: dict[str, Any],
    cluster: dict[str, Any],
    backend: str,
    context_size: Any,
    force: bool,
) -> dict[str, Any]:
    node = str(cluster.get("node") or "").strip()
    model_ref = str(model.get("model") or docker_model_name(entry))
    model_label = str(model.get("id") or model_ref)
    node_label = node or "selected runtime node"
    node_endpoint = _cluster_node_endpoint(node)
    local_target = _cluster_node_endpoint_is_local(node_endpoint)
    transport = "local runtime coordinator" if local_target else "native SDK gRPC"
    console.print(
        f"[cyan]Preparing runtime model {model_label} on {node_label} with {transport}...[/cyan]"
    )
    runtime_client = _runtime_model_prepare_client(node, node_endpoint)
    prepare_payload = {
        "node": node,
        "model": model_ref,
        "id": model.get("id") or entry.get("id"),
        "provider": str(entry.get("provider") or "docker_model_runner"),
        "backend": str(backend or "auto"),
        "context_size": context_size,
        "force": force,
        "source": "mn-python-sdk",
    }
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        disable=not use_progress(),
    ) as progress:
        progress.add_task(
            (
                f"[cyan]Checking and preparing {model_label} on {node_label}; "
                f"waiting for {'local' if local_target else 'remote'} Docker Model Runner..."
            ),
            total=None,
        )
        response = _prepare_runtime_model_with_retry(runtime_client, prepare_payload)
    try:
        payload = json.loads(response)
    except Exception as exc:
        raise RuntimeError(f"runtime model prepare returned invalid JSON: {response}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("runtime model prepare returned a non-object response")
    if str(payload.get("status") or "").lower() in {"failed", "error"}:
        raise RuntimeError(str(payload.get("error") or payload.get("message") or "runtime model prepare failed"))
    endpoint = payload.get("endpoint") if isinstance(payload.get("endpoint"), dict) else {}
    upstream_endpoint = _cluster_runtime_model_upstream_endpoint(
        entry=entry,
        node=node,
        node_endpoint=node_endpoint,
        payload=payload,
        endpoint=endpoint,
    )
    return {
        "install": payload,
        "endpoint": upstream_endpoint,
    }

def _cluster_runtime_model_upstream_endpoint(
    *,
    entry: dict[str, Any],
    node: str,
    node_endpoint: dict[str, Any],
    payload: dict[str, Any],
    endpoint: dict[str, Any],
) -> dict[str, Any]:
    if _cluster_node_endpoint_is_local(node_endpoint):
        local_endpoint = docker_model_runner_endpoint(entry, node=node, source="local-dmr")
        local_endpoint["node"] = node
        return local_endpoint

    docker_model = docker_model_name(entry)
    api_model = str(endpoint.get("api_model") or endpoint.get("model") or entry.get("api_model") or docker_model)
    return {
        "provider": str(endpoint.get("provider") or entry.get("provider") or "docker_model_runner"),
        "model": api_model,
        "runtime_model": str(endpoint.get("runtime_model") or docker_model),
        "api_model": api_model,
        "api_base": _node_litellm_gateway_api_base(node_endpoint, payload, endpoint),
        "api_key": str(endpoint.get("api_key") or "not-needed"),
        "node": str(endpoint.get("node") or node),
        "source": "remote-dmr",
        **_route_aliases_payload(entry),
    }

def _route_aliases_payload(entry: dict[str, Any]) -> dict[str, Any]:
    aliases = entry.get("route_aliases")
    if not isinstance(aliases, list) or not aliases:
        return {}
    return {"route_aliases": [str(alias) for alias in aliases if str(alias or "").strip()]}

def _cluster_node_endpoint_is_local(node_endpoint: dict[str, Any]) -> bool:
    node = node_endpoint.get("node") if isinstance(node_endpoint.get("node"), dict) else {}
    if node.get("self?") is True or node.get("self") is True:
        return True
    host = str(node_endpoint.get("host") or "").strip().lower()
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True
    if host in _local_host_addresses():
        return True

    node_name = str(node.get("name") or node.get("node") or "").strip()
    return bool(node_name and node_name == _local_runtime_node_name())

@lru_cache(maxsize=1)
def _local_host_addresses() -> set[str]:
    hostnames = {"localhost", "127.0.0.1", "::1", "::", "0.0.0.0"}
    candidates: set[str] = {address.lower() for address in hostnames}
    try:
        candidates.update(_resolved_local_hostnames())
    except Exception:
        pass
    try:
        parsed = urllib.parse.urlparse(f"//{config.grpc_target}")
        if parsed.hostname:
            candidates.add(parsed.hostname.lower())
    except Exception:
        pass
    for env_key in ("MN_API_HOST", "MN_GRPC_TARGET", "MN_API_BASE_URL"):
        env_value = os.getenv(env_key, "")
        if env_value:
            candidates.update(_extract_host_candidates_from_text(env_value))
    return candidates

def _extract_host_candidates_from_text(value: str) -> set[str]:
    candidates: set[str] = set()
    text = str(value or "").strip()
    if not text:
        return candidates
    for part in (text, f"//{text}",):
        parsed = urllib.parse.urlparse(part)
        if parsed.hostname:
            candidates.add(parsed.hostname.lower())
    return candidates

def _resolved_local_hostnames() -> set[str]:
    addresses: set[str] = set()
    try:
        addresses.add(socket.gethostbyname(socket.gethostname()).lower())
    except Exception:
        pass
    try:
        addresses.update(addr.lower() for addr in socket.gethostbyname_ex(socket.gethostname())[2])
    except Exception:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM):
            if len(info) >= 5:
                entry = info[4][0]
                if isinstance(entry, str):
                    addresses.add(entry.lower().split("%", 1)[0])
    except Exception:
        pass
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("10.255.255.255", 1))
            addresses.add(probe.getsockname()[0].lower())
        finally:
            probe.close()
    except Exception:
        pass
    return addresses

def _node_litellm_gateway_api_base(
    node_endpoint: dict[str, Any],
    payload: dict[str, Any],
    endpoint: dict[str, Any],
) -> str:
    gateway = payload.get("gateway") if isinstance(payload.get("gateway"), dict) else {}
    host_base = str(gateway.get("host_api_base") or "").strip()
    host = str(node_endpoint.get("host") or "").strip()
    if host_base and host:
        parsed = urllib.parse.urlparse(host_base)
        scheme = parsed.scheme or "http"
        try:
            port = parsed.port or int(os.environ.get("MN_LITELLM_GATEWAY_PORT", "4000"))
        except ValueError:
            port = 4000
        path = parsed.path or "/v1"
        return urllib.parse.urlunparse((scheme, f"{host}:{port}", path, "", "", "")).rstrip("/")
    if host:
        port = os.environ.get("MN_LITELLM_GATEWAY_PORT", "4000")
        return f"http://{host}:{port}/v1".rstrip("/")
    return str(endpoint.get("api_base") or "http://mn-litellm-proxy:4000/v1").rstrip("/")

def _runtime_model_prepare_timeout_seconds() -> float:
    raw = str(os.environ.get("MN_RUNTIME_MODEL_PREPARE_TIMEOUT_SECONDS") or "").strip()
    if raw:
        try:
            value = float(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return DEFAULT_RUNTIME_MODEL_PREPARE_TIMEOUT_SECONDS

def _print_runtime_model_install_summary(summary: dict[str, Any]) -> None:
    models = summary.get("models") or []
    if not models:
        return

    prepared = [
        item
        for item in models
        if str(item.get("status") or "")
        in {
            "installed",
            "already_installed",
            "service_required",
            "cluster_provided",
            "runtime_node_install",
            "runtime_node_already_installed",
            "runtime_node_installed",
            "fallback_model",
            "service_registry",
            "model_remote",
            "explicit_config",
        }
    ]
    if prepared:
        labels = ", ".join(
            _runtime_model_ready_label(item)
            for item in prepared[:4]
        )
        if len(prepared) > 4:
            labels = f"{labels}, +{len(prepared) - 4} more"
        console.print(f"[green]Runtime models ready:[/green] {labels}")
    for error in summary.get("errors") or []:
        console.print(f"[red]Runtime model install failed: {error}[/red]")

def _runtime_model_ready_label(item: dict[str, Any]) -> str:
    label = str(item.get("id") or item.get("model") or "runtime model")
    fallback = item.get("fallback") if isinstance(item.get("fallback"), dict) else {}
    if str(item.get("status") or "") == "fallback_model" and fallback:
        fallback_label = str(fallback.get("id") or fallback.get("model") or "fallback model")
        return f"{label} -> {fallback_label}"
    status = str(item.get("status") or "")
    if status in {"runtime_node_installed", "runtime_node_already_installed"}:
        node = _runtime_model_ready_node(item)
        if node:
            if status == "runtime_node_already_installed":
                return f"{label} already installed on {node}"
            return f"{label} installed on {node}"
    return label

def _runtime_model_ready_node(item: dict[str, Any]) -> str:
    for value in (
        item.get("node"),
        (item.get("endpoint") or {}).get("node") if isinstance(item.get("endpoint"), dict) else None,
        (item.get("cluster") or {}).get("node") if isinstance(item.get("cluster"), dict) else None,
        (item.get("install") or {}).get("node") if isinstance(item.get("install"), dict) else None,
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


__all__ = [name for name in globals() if not name.startswith("__")]
