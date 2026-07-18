"""Reusable, service-free runtime-model cluster for run-path tests."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from mn_cli.libs import run_cmds
from mn_sdk import BlueprintModelOps


def fake_model_catalog() -> dict[str, dict[str, Any]]:
    """Return the smallest catalog that exercises adaptive chat and RAG models."""

    entries = [
        {
            "id": "gemma4:e2b",
            "provider": "docker_model_runner",
            "model": "docker.io/ai/gemma4:E2B",
            "api_model": "docker.io/ai/gemma4:E2B",
            "aliases": ["default", "small", "gemma4"],
            "backend": "llama.cpp",
            "context_size": 4096,
            "requirements": {
                "min_vram_gb": 8,
                "min_unified_memory_gb": 16,
                "min_cpu_ram_gb": 32,
            },
        },
        {
            "id": "nemotron3",
            "provider": "docker_model_runner",
            "model": "nemotron3",
            "dmr_model": "docker.io/ai/nemotron3:latest",
            "api_model": "docker.io/ai/nemotron3:latest",
            "aliases": ["medium", "nemotron3:latest"],
            "backend": "llama.cpp",
            "context_size": 8192,
            "fallback_model": "gemma4:e2b",
            "requirements": {
                "min_vram_gb": 48,
                "min_unified_memory_gb": 48,
            },
        },
        {
            "id": "rag-embedding",
            "provider": "docker_model_runner",
            "model": "huggingface.co/jinaai/jina-embeddings-v5-text-small-retrieval:Q4_K_M",
            "api_model": "huggingface.co/jinaai/jina-embeddings-v5-text-small-retrieval:Q4_K_M",
            "aliases": [
                "huggingface.co/jinaai/jina-embeddings-v5-text-small-retrieval:Q4_K_M",
                "jina-v5-small",
            ],
            "backend": "llama.cpp",
            "context_size": 8192,
            "requirements": {},
        },
    ]
    return {entry["id"]: entry for entry in entries}


def fake_runtime_node(
    name: str,
    *,
    memory_mb: int,
    vendor: str,
    host: str,
    self_node: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    driver = "cuda" if vendor == "nvidia" else "metal"
    capabilities = ["gpu", vendor, driver]
    resource = {
        "name": name,
        "status": "healthy",
        "scheduling_eligible": True,
        "devices": [
            {
                "kind": "gpu",
                "vendor": vendor,
                "driver": driver,
                "memory_total_mb": memory_mb,
                "memory_free_mb": memory_mb,
                "capabilities": capabilities,
            }
        ],
    }
    system = {
        "name": name,
        "status": "healthy",
        "scheduling_eligible": True,
        "self": self_node,
        "grpc_host": host,
        "grpc_port": 55051,
        "native_sdk_grpc": {
            "enabled": True,
            "host": host,
            "port": 55052,
            "capabilities": [
                "runtime_model_prepare_v1",
                "docker_worker_prepare_v1",
            ],
        },
    }
    return resource, system


@dataclass
class FakeRuntimeModelCluster:
    """In-memory cluster implementing all injected model preparation effects."""

    include_spark: bool = False
    catalog: dict[str, dict[str, Any]] = field(default_factory=fake_model_catalog)
    installed_by_node: dict[str, set[str]] = field(default_factory=dict)
    prepare_calls: list[dict[str, Any]] = field(default_factory=list)
    remote_reconciliations: list[dict[str, Any]] = field(default_factory=list)
    gateway_syncs: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        local_resource, local_system = fake_runtime_node(
            "mirror_neuron@mac",
            memory_mb=16384,
            vendor="apple",
            host="10.0.0.1",
            self_node=True,
        )
        self.resources = {"nodes": [local_resource]}
        self.system = {"nodes": [local_system]}
        if self.include_spark:
            spark_resource, spark_system = fake_runtime_node(
                "mirror_neuron@spark",
                memory_mb=131072,
                vendor="nvidia",
                host="10.0.0.2",
            )
            self.resources["nodes"].append(spark_resource)
            self.system["nodes"].append(spark_system)

    def dependencies(self) -> run_cmds.RuntimeModelDependencies:
        model_ops = BlueprintModelOps(
            load_model_catalog=lambda: copy.deepcopy(self.catalog),
            required_blueprint_models=run_cmds.required_blueprint_models,
            load_model_ownership=lambda: {"version": 1, "models": {}},
            resolve_model_entry=run_cmds.resolve_model_entry,
            docker_model_name=run_cmds.docker_model_name,
            cluster_provided_model=run_cmds.cluster_provided_model,
            record_model_owner=lambda *_args, **_kwargs: None,
            model_installed=lambda model: model
            in self.installed_by_node.get("mirror_neuron@mac", set()),
            install_model_entry=self._unexpected_local_install,
            resolve_model_endpoint=lambda **_kwargs: None,
            resolve_cluster_model=run_cmds._resolve_runtime_cluster_model,
            install_cluster_model=self.prepare_cluster_model,
        )
        gateway = run_cmds.RuntimeModelGatewayDependencies(
            reconcile_cluster_model_remotes=self.reconcile_cluster_model_remotes,
            installed_model_names=lambda: set(
                self.installed_by_node.get("mirror_neuron@mac", set())
            ),
            local_runtime_node_name=lambda: "mirror_neuron@mac",
            sync_litellm_gateway=self.sync_litellm_gateway,
            gateway_endpoint_map=run_cmds.gateway_endpoint_map,
            resolve_model_entry=lambda model: run_cmds.resolve_model_entry(
                model, catalog=self.catalog
            ),
            docker_model_runner_endpoint=run_cmds.docker_model_runner_endpoint,
        )
        return run_cmds.RuntimeModelDependencies(
            load_model_catalog=lambda: copy.deepcopy(self.catalog),
            model_ops=model_ops,
            resource_report=lambda: copy.deepcopy(self.resources),
            system_summary=lambda: copy.deepcopy(self.system),
            gateway=gateway,
        )

    def prepare_cluster_model(
        self,
        *,
        requirement: dict[str, Any],
        entry: dict[str, Any],
        cluster: dict[str, Any],
        **_kwargs: Any,
    ) -> dict[str, Any]:
        node = str(cluster["node"])
        runtime_model = run_cmds.docker_model_name(entry)
        installed = self.installed_by_node.setdefault(node, set())
        status = "already_installed" if runtime_model in installed else "installed"
        installed.add(runtime_model)
        host = next(
            item["grpc_host"]
            for item in self.system["nodes"]
            if item["name"] == node
        )
        api_base = (
            "http://host.docker.internal:12434/engines/v1"
            if node == "mirror_neuron@mac"
            else f"http://{host}:12434/engines/v1"
        )
        call = {
            "node": node,
            "runtime_model": runtime_model,
            "path": requirement.get("path"),
            "status": status,
            "api_base": api_base,
        }
        self.prepare_calls.append(call)
        return {
            "install": {"status": status},
            "endpoint": {
                "provider": "docker_model_runner",
                "model": str(entry.get("api_model") or runtime_model),
                "runtime_model": runtime_model,
                "api_model": str(entry.get("api_model") or runtime_model),
                "api_base": api_base,
                "node": node,
                "source": "local-dmr"
                if node == "mirror_neuron@mac"
                else "remote-dmr",
            },
        }

    def reconcile_cluster_model_remotes(
        self,
        runtime_endpoints: dict[str, dict[str, Any]],
        **kwargs: Any,
    ) -> None:
        self.remote_reconciliations.append(
            {
                "runtime_endpoints": copy.deepcopy(runtime_endpoints),
                **copy.deepcopy(kwargs),
            }
        )

    def sync_litellm_gateway(self, **kwargs: Any) -> dict[str, Any]:
        self.gateway_syncs.append(copy.deepcopy(kwargs))
        return {
            "status": "running",
            "api_base": "http://mn-litellm-proxy:4000/v1",
        }

    @staticmethod
    def _unexpected_local_install(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("selected-node preparation must not use local DMR install")
