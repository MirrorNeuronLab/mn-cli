from io import StringIO
from types import SimpleNamespace

from rich.console import Console

import mn_cli.libs.sys_cmds as sys_cmds
import mn_cli.shared


def test_leave_does_not_rotate_grpc_tokens(monkeypatch, tmp_path):
    auth_file = tmp_path / "grpc_auth.token"
    admin_file = tmp_path / "grpc_admin.token"
    auth_file.write_text("stable-auth-token\n")
    admin_file.write_text("stable-admin-token\n")
    calls = []

    monkeypatch.setattr(mn_cli.shared, "client", SimpleNamespace(remove_node=lambda node: "removed"))
    monkeypatch.setattr(mn_cli.shared, "console", Console(file=StringIO(), force_terminal=False, width=160))
    monkeypatch.setattr(sys_cmds, "_detach_local_docker_node_if_matches", lambda node: calls.append(node))

    sys_cmds.leave("mirror_neuron@192.168.4.20")

    assert calls == ["mirror_neuron@192.168.4.20"]
    assert auth_file.read_text().strip() == "stable-auth-token"
    assert admin_file.read_text().strip() == "stable-admin-token"
