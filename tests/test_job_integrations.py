from types import SimpleNamespace

import pytest
import typer

from mn_cli.commands import job_integrations as commands


def test_connect_prepares_once_and_uses_saved_job(monkeypatch):
    seen = []
    session = "85a8718a-e115-4dd9-90f5-1827ca79b3f1"
    results = iter(
        [
            {"operation_id": "prep"},
            {
                "authorization": "authorized",
                "session": session,
                "channels": [{"id": "CTEAM", "name": "general"}],
            },
            {"delivery": "enabled"},
        ]
    )

    def request(method, path, **options):
        seen.append((method, path, options))
        return next(results)

    operations = []
    monkeypatch.setattr(
        commands, "watch_operation", lambda api, value: operations.append(value)
    )
    result = commands.execute(
        SimpleNamespace(request=request),
        ("job", "connect"),
        {"job_id": "vc-daily", "provider": "slack", "name": "acme", "channel": "CTEAM"},
    )
    assert result["delivery"] == "enabled"
    assert operations == [{"operation_id": "prep"}]
    assert all(
        path == "/jobs/vc-daily/integrations/slack/connect" for _, path, _ in seen
    )
    assert seen[-1][2]["json"] == {
        "name": "acme",
        "session": session,
        "channel": "CTEAM",
    }


def test_cancelled_connect_only_cancels_pending_setup(monkeypatch):
    seen = []

    def request(method, path, **options):
        seen.append(path)
        return {
            "authorization": "authorized",
            "session": "session",
            "channels": [{"id": "CTEAM", "name": "general"}],
        }

    def cancel(label):
        raise typer.Abort()

    monkeypatch.setattr(commands, "json_enabled", lambda: False)
    monkeypatch.setattr(typer, "prompt", cancel)
    with pytest.raises(typer.Abort):
        commands.execute(
            SimpleNamespace(request=request),
            ("job", "connect"),
            {"job_id": "job", "provider": "slack", "name": "acme"},
        )
    assert seen == [
        "/jobs/job/integrations/slack/connect",
        "/jobs/job/integrations/slack/cancel",
    ]
