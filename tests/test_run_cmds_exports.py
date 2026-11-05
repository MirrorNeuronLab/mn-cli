"""The established run-command facade remains usable across internal splits."""

import json
from pathlib import Path

from mn_cli.libs import run_cmds


def test_established_run_command_exports_remain_available():
    expected = json.loads(
        (Path(__file__).parent / "fixtures" / "run_cmds_exports.json").read_text()
    )
    assert run_cmds.__all__ == expected
    assert all(hasattr(run_cmds, name) for name in expected)


def test_facade_reuses_command_and_dependency_objects():
    from importlib import import_module

    from mn_cli.libs.run_cmds import common, model_cluster, models, outputs

    assert (
        run_cmds.run_bundle
        is import_module("mn_cli.libs.run_cmds.handlers.run").run_bundle
    )
    assert (
        run_cmds.monitor
        is import_module("mn_cli.libs.run_cmds.handlers.monitor").monitor
    )
    assert run_cmds.fetch_and_save_results is outputs.fetch_and_save_results
    assert run_cmds.client is common.client is models.client is model_cluster.client
    assert (
        run_cmds._prepare_runtime_models_for_run_or_exit
        is models._prepare_runtime_models_for_run_or_exit
    )
