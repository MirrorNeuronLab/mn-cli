"""CLI preflight must consume the shared hard-locality contract."""

import pytest
from mn_sdk.errors import AppError, normalize_exception

from mn_cli.libs.run_cmds.model_cluster import _preflight_and_apply_runtime_model_placement


@pytest.mark.parametrize("local_os,env", [
    ("darwin", {}), ("darwin", {"MN_BLUEPRINT_SINGLE_NODE_AGENTS": "0"}),
    ("darwin", {"MN_SELECTED_RUNTIME_NODE": "spark"}), ("linux", {}), ("windows", {}),
])
def test_cli_preflight_honors_locality_without_models_or_host_workers(local_os, env):
    manifest = {"runtime": {"placement": {"must_run_local": True}},
                "requirements": {"os": "darwin"},
                "flow": {"nodes": [{"node_id": "inspect"}], "edges": []}}
    report = {"nodes": [{"name": name, "self": local, "status": "healthy",
                         "coordination_store": {"identity": "test-store", "writable_primary": True, "healthy": True},
                         "hardware": {"platform": {"os": os}}}
                        for name, os, local in [("mini", local_os, True), ("spark", "darwin", False)]]}
    options = {"resource_report": report, "system_summary": report, "env": env}
    if local_os != "darwin" or env.get("MN_SELECTED_RUNTIME_NODE"):
        with pytest.raises((AppError, RuntimeError)) as raised:
            _preflight_and_apply_runtime_model_placement(manifest, **options)
        expected = "must run on this computer" if env.get("MN_SELECTED_RUNTIME_NODE") else "requires macOS"
        assert expected in normalize_exception(raised.value).user_message
        return
    result = _preflight_and_apply_runtime_model_placement(manifest, **options)
    assert result["selected_node"] == "mini"
    assert manifest["flow"]["nodes"][0]["constraints"][-1]["value"] == "mini"
