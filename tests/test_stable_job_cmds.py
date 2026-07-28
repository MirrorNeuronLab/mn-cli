import json
from types import SimpleNamespace

import mn_cli.libs.stable_job_cmds as stable_job_cmds


def test_delete_cleans_every_historical_run_and_definition_resources(monkeypatch):
    cleaned = []
    printed = []
    monkeypatch.setattr(
        stable_job_cmds,
        "client",
        SimpleNamespace(
            list_runs=lambda job_id: json.dumps(
                {
                    "job_id": job_id,
                    "data": [
                        {"run_id": "run-1", "status": "completed"},
                        {"run_id": "run-2", "status": "failed"},
                    ],
                }
            ),
            delete_stable_job=lambda job_id, confirmed: json.dumps(
                {"job_id": job_id, "status": "deleted", "confirmed": confirmed}
            ),
        ),
    )
    monkeypatch.setattr(
        stable_job_cmds, "_cleanup_cleared_job_resources", cleaned.append
    )
    monkeypatch.setattr(
        stable_job_cmds.console,
        "print_json",
        lambda *, data: printed.append(data),
    )

    stable_job_cmds.delete("stable-job", yes=True)

    assert cleaned == ["run-1", "run-2", "stable-job"]
    assert printed == [
        {"job_id": "stable-job", "status": "deleted", "confirmed": True}
    ]
