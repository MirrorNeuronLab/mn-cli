import json

import pytest
from unittest.mock import Mock, patch

from runtime_model_fakes import FakeRuntimeModelCluster


class PatchProxy:
    def __init__(self, owner):
        self._owner = owner

    def __call__(self, target, *args, **kwargs):
        return self._owner._start_patch(patch(target, *args, **kwargs))

    def object(self, target, attribute, *args, **kwargs):
        return self._owner._start_patch(patch.object(target, attribute, *args, **kwargs))


class Mocker:
    def __init__(self):
        self._patches = []
        self.patch = PatchProxy(self)
        self.Mock = Mock

    def _start_patch(self, patcher):
        mocked = patcher.start()
        self._patches.append(patcher)
        return mocked

    def stopall(self):
        for patcher in reversed(self._patches):
            patcher.stop()
        self._patches.clear()


@pytest.fixture
def mocker():
    helper = Mocker()
    try:
        yield helper
    finally:
        helper.stopall()


@pytest.fixture
def fake_runtime_model_cluster_factory():
    """Build a deterministic local-only or Mac-plus-Spark model cluster."""

    return FakeRuntimeModelCluster


@pytest.fixture(autouse=True)
def stabilize_cluster_join_tests(request, monkeypatch):
    if request.node.name.startswith(("test_add_node_", "test_join_network_")):
        from mn_cli.runtime import server as runtime_server

        monkeypatch.setattr(
            runtime_server,
            "_confirm_joined_node",
            lambda _client, _remote_node, _token, status, **_kwargs: status,
        )


@pytest.fixture(autouse=True)
def use_cli_model_pull_for_legacy_install_tests(request, monkeypatch):
    name = request.node.name
    if name.startswith("test_model_install_") and "dmr" not in name and "gateway" not in name:
        from mn_cli.libs import model_cmds

        monkeypatch.setattr(model_cmds, "_endpoint_responds", lambda: False)
        monkeypatch.setattr("mn_sdk.model_service.endpoint_responds", lambda: False)


@pytest.fixture(autouse=True)
def stable_job_runtime_contract_adapter(monkeypatch):
    """Keep feature-focused run tests isolated behind the v2 runtime contract."""
    from mn_cli.libs import run_cmds

    created_bundles = {}

    def create_stable_job(manifest_json, payloads, **_kwargs):
        manifest = json.loads(manifest_json)
        force = bool((manifest.get("metadata", {}).get("mn_validation") or {}).get("force"))
        job_id = run_cmds.client.submit_job(manifest_json, payloads, force=force)
        created_bundles[str(job_id)] = (manifest_json, payloads)
        return json.dumps({"job_id": str(job_id)})

    def start_run(job_id, *, run_id, inputs):
        assert isinstance(inputs, dict)
        return json.dumps({"job_id": job_id, "run_id": run_id or f"{job_id}-run"})

    def create_job_schedule(job_id, *, schedule, source):
        manifest_json, payloads = created_bundles[job_id]
        return run_cmds.client.create_schedule(
            manifest_json,
            payloads,
            schedule=schedule,
            source=source,
        )

    monkeypatch.setattr(run_cmds.client, "create_stable_job", create_stable_job)
    monkeypatch.setattr(run_cmds.client, "start_run", start_run)
    monkeypatch.setattr(run_cmds.client, "create_job_schedule", create_job_schedule)
