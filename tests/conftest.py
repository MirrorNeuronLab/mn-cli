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
