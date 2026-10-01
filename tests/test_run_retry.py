import json

import grpc
from typer.testing import CliRunner
from mn_cli.main import app

runner = CliRunner()
REVISION = "a" * 64


def test_dry_run_does_not_submit(mocker):
    plan = mocker.patch("mn_cli.libs.run_public.client.plan_run_retry", return_value=json.dumps({
        "run_id": "run-1", "eligible": True, "expected_attempt": 1, "checkpoint_revision": REVISION,
        "preserved_steps": ["done"], "retry_steps": ["failed"],
    }))
    submit = mocker.patch("mn_cli.libs.run_public.client.retry_run")
    result = runner.invoke(app, ["run", "retry", "run-1", "--dry-run", "--set", "budget.minutes=60", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["preserved_steps"] == ["done"]
    plan.assert_called_once_with("run-1", configuration_overrides={"budget.minutes": 60})
    submit.assert_not_called()


def test_retry_submits_selected_checkpoint(mocker):
    mocker.patch("mn_cli.libs.run_public.client.plan_run_retry", return_value=json.dumps({
        "eligible": True, "expected_attempt": 2, "checkpoint_revision": REVISION,
    }))
    submit = mocker.patch("mn_cli.libs.run_public.client.retry_run", return_value=json.dumps({"run_id": "run-1", "attempt": 3}))
    result = runner.invoke(app, ["run", "retry", "run-1", "--idempotency-key", "key-1", "--set", "budget.minutes=60", "--json"])
    assert result.exit_code == 0, result.output
    submit.assert_called_once_with("run-1", expected_attempt=2, checkpoint_revision=REVISION,
        configuration_overrides={"budget.minutes": 60}, idempotency_key="key-1")


def test_lost_response_resubmits_without_replanning_running_attempt(mocker):
    plan = mocker.patch("mn_cli.libs.run_public.client.plan_run_retry")
    submit = mocker.patch("mn_cli.libs.run_public.client.retry_run", return_value=json.dumps({"attempt": 2}))
    result = runner.invoke(app, ["run", "retry", "run-1", "--idempotency-key", "key-1",
        "--expected-attempt", "1", "--checkpoint-revision", REVISION, "--json"])
    assert result.exit_code == 0, result.output
    plan.assert_not_called()
    submit.assert_called_once_with("run-1", expected_attempt=1, checkpoint_revision=REVISION,
        configuration_overrides={}, idempotency_key="key-1")


def test_failed_resume_explains_retry(mocker):
    mocker.patch("mn_cli.libs.job_definition_cmds.client.get_run", return_value=json.dumps({"status": "failed"}))
    resume = mocker.patch("mn_cli.libs.job_definition_cmds.client.resume_run")
    result = runner.invoke(app, ["run", "resume", "run-1"])
    assert result.exit_code != 0
    assert "run retry" in result.output
    resume.assert_not_called()


class Missing(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.NOT_FOUND

    def details(self):
        return "not found"


def test_history_missing_control_returns_blocked_plan(mocker):
    mocker.patch("mn_cli.libs.run_public.client.plan_run_retry", side_effect=Missing())
    mocker.patch("mn_cli.libs.run_public.mapped_run_record", return_value={"run_id": "run-1"})
    result = runner.invoke(app, ["run", "retry", "run-1", "--dry-run", "--json"])
    assert result.exit_code == 0, result.output
    assert "control record is missing" in json.loads(result.stdout)["data"]["reason"]


def test_retry_plan_in_plain_and_normal_terminal_modes_at_narrow_and_wide_widths(mocker, monkeypatch):
    mocker.patch("mn_cli.libs.run_public.client.plan_run_retry", return_value=json.dumps({
        "run_id": "run-1", "eligible": False, "reason": "Restore the original API, then try again.",
        "preserved_steps": ["completed"], "retry_steps": ["unfinished"],
    }))
    submit = mocker.patch("mn_cli.libs.run_public.client.retry_run")
    for mode in ("plain", ""):
        for width in (60, 160):
            monkeypatch.setenv("MN_CLI_OUTPUT", mode)
            monkeypatch.setenv("COLUMNS", str(width))
            result = runner.invoke(app, ["run", "retry", "run-1", "--dry-run"])
            assert result.exit_code == 0, result.output
            assert "original API" in result.output
    submit.assert_not_called()
