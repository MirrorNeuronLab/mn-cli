import json

from typer.testing import CliRunner

from mn_cli.libs.deployment_cmds import read_bundle
from mn_cli.main import app

runner = CliRunner()


def test_deploy_command_passes_policy_and_payloads(mocker, tmp_path):
    mock_deploy = mocker.patch(
        "mn_cli.libs.deployment_cmds.client.deploy_job",
        return_value=json.dumps({"status": "awaiting_promotion"}),
    )

    bundle = tmp_path / "bundle"
    payloads = bundle / "payloads"
    payloads.mkdir(parents=True)
    (bundle / "manifest.json").write_text('{"graph_id": "agent-api"}', encoding="utf-8")
    (payloads / "input.json").write_bytes(b"{}")

    result = runner.invoke(
        app,
        [
            "deployment",
            "deploy",
            str(bundle),
            "--key",
            "agent-api",
            "--strategy",
            "canary",
            "--canary",
            "1",
            "--max-parallel",
            "2",
            "--auto-revert",
        ],
    )

    assert result.exit_code == 0
    assert "Deployment deploy successful." in result.stdout
    assert "Status: awaiting_promotion" in result.stdout
    mock_deploy.assert_called_once()
    args, kwargs = mock_deploy.call_args
    assert args[0] == '{"graph_id": "agent-api"}'
    assert args[1] == {"input.json": b"{}"}
    assert kwargs["deployment_key"] == "agent-api"
    assert kwargs["update_policy"]["strategy"] == "canary"
    assert kwargs["update_policy"]["canary"] == 1
    assert kwargs["update_policy"]["max_parallel"] == 2
    assert kwargs["update_policy"]["auto_revert"] is True


def test_read_bundle_uses_posix_payload_paths(tmp_path):
    bundle = tmp_path / "bundle"
    nested = bundle / "payloads" / "nested"
    nested.mkdir(parents=True)
    (bundle / "manifest.json").write_text('{"graph_id": "agent-api"}', encoding="utf-8")
    (nested / "input.json").write_bytes(b"{}")

    manifest_json, payloads = read_bundle(str(bundle))

    assert manifest_json == '{"graph_id": "agent-api"}'
    assert payloads == {"nested/input.json": b"{}"}


def test_deployment_promote_command(mocker):
    mock_promote = mocker.patch(
        "mn_cli.libs.deployment_cmds.client.promote_deployment",
        return_value=json.dumps({"status": "successful"}),
    )

    result = runner.invoke(app, ["deployment", "promote", "agent-api"])

    assert result.exit_code == 0
    assert "Deployment promote successful." in result.stdout
    assert "Status: successful" in result.stdout
    mock_promote.assert_called_once_with("agent-api")
