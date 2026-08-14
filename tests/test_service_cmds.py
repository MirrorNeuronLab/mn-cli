import json
from typer.testing import CliRunner

from mn_cli.main import app


runner = CliRunner()


def test_service_list_prints_registry_json(mocker):
    mock_list = mocker.patch(
        "mn_cli.libs.service_cmds.client.list_services",
        return_value=json.dumps({"services": [{"name": "ollama", "status": "passing"}]}),
    )

    result = runner.invoke(app, ["service", "list", "--name", "ollama"])

    assert result.exit_code == 0
    assert "ollama" in result.stdout
    mock_list.assert_called_once()
    assert mock_list.call_args.kwargs["name"] == "ollama"
    assert mock_list.call_args.kwargs["passing_only"] is True


def test_service_show_passes_tags_to_client(mocker):
    mock_resolve = mocker.patch(
        "mn_cli.libs.service_cmds.client.resolve_service",
        return_value=json.dumps({"services": [{"name": "vector-db"}]}),
    )

    result = runner.invoke(app, ["service", "show", "vector-db", "--tag", "embeddings"])

    assert result.exit_code == 0
    assert "vector-db" in result.stdout
    mock_resolve.assert_called_once()
    assert mock_resolve.call_args.args[0] == "vector-db"
    assert mock_resolve.call_args.kwargs["tags"] == ["embeddings"]


def test_service_show_does_not_reuse_previous_tags(mocker):
    mock_resolve = mocker.patch(
        "mn_cli.libs.service_cmds.client.resolve_service",
        return_value=json.dumps({"services": []}),
    )

    tagged = runner.invoke(app, ["service", "show", "vector-db", "--tag", "embeddings"])
    untagged = runner.invoke(app, ["service", "show", "vector-db"])

    assert tagged.exit_code == 0
    assert untagged.exit_code == 0
    assert mock_resolve.call_args_list[0].kwargs["tags"] == ["embeddings"]
    assert mock_resolve.call_args_list[1].kwargs["tags"] == []


def test_service_check_is_removed_with_blueprint_doctor_replacement():
    result = runner.invoke(app, ["service", "check", "./bundle", "--json"])

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert "mn blueprint doctor" in payload["error"]["message"]
