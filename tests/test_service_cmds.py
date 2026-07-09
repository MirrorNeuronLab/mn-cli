import json
import sys

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


def test_service_resolve_passes_tags_to_client(mocker):
    mock_resolve = mocker.patch(
        "mn_cli.libs.service_cmds.client.resolve_service",
        return_value=json.dumps({"services": [{"name": "vector-db"}]}),
    )

    result = runner.invoke(app, ["service", "resolve", "vector-db", "--tag", "embeddings"])

    assert result.exit_code == 0
    assert "vector-db" in result.stdout
    mock_resolve.assert_called_once()
    assert mock_resolve.call_args.args[0] == "vector-db"
    assert mock_resolve.call_args.kwargs["tags"] == ["embeddings"]


def test_service_resolve_does_not_reuse_previous_tags(mocker):
    mock_resolve = mocker.patch(
        "mn_cli.libs.service_cmds.client.resolve_service",
        return_value=json.dumps({"services": []}),
    )

    tagged = runner.invoke(app, ["service", "resolve", "vector-db", "--tag", "embeddings"])
    untagged = runner.invoke(app, ["service", "resolve", "vector-db"])

    assert tagged.exit_code == 0
    assert untagged.exit_code == 0
    assert mock_resolve.call_args_list[0].kwargs["tags"] == ["embeddings"]
    assert mock_resolve.call_args_list[1].kwargs["tags"] == []


def test_service_check_runs_local_required_service_validation(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "manifest_version": "1.0",
                "graph_id": "service-check",
                "job_name": "service-check",
                "entrypoints": ["worker"],
                "nodes": [{"node_id": "worker"}],
                "required_services": [
                    {
                        "name": "script-probe",
                        "origin": "external",
                        "checks": [
                            {
                                "name": "probe",
                                "type": "script",
                                "command": [sys.executable, "-c", "print('ok')"],
                            }
                        ],
                    }
                ],
            }
        )
    )

    result = runner.invoke(app, ["service", "check", str(bundle_dir)])

    assert result.exit_code == 0
    assert "Service check confirmed." in result.stdout
    assert "healthy" in result.stdout
