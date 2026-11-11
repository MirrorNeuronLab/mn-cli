from types import SimpleNamespace
from unittest.mock import Mock
import mn_cli.shared as shared


def test_shared_client_is_lazy_and_does_not_bootstrap_host(monkeypatch):
    factory = Mock(return_value=SimpleNamespace(get_job=lambda job: job))
    monkeypatch.setattr(shared, "build_client", factory)
    proxy = shared.LazyClient()
    factory.assert_not_called()
    assert proxy.get_job("job") == "job"
    assert proxy.get_job("second") == "second"
    factory.assert_called_once_with(shared.config)


def test_internal_credentials_do_not_enter_surface_client(monkeypatch):
    factory = Mock()
    monkeypatch.setattr(shared, "SurfaceRuntimeClient", factory)
    shared.build_client(
        SimpleNamespace(
            grpc_target="private:123",
            grpc_auth_token="internal",
            grpc_admin_token="admin",
        )
    )
    factory.assert_called_once_with()
