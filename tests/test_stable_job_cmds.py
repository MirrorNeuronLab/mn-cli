import json
from types import SimpleNamespace

import mn_cli.libs.stable_job_cmds as stable_job_cmds
from mn_cli.libs.job_cleanup import JobResourceCleanupError


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
        stable_job_cmds,
        "cleanup_cleared_job_resources",
        lambda job_id, **_kwargs: cleaned.append(job_id),
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


def test_delete_attempts_all_local_cleanup_before_reporting_failure(monkeypatch):
    cleaned = []
    handled_errors = []
    monkeypatch.setattr(
        stable_job_cmds,
        "client",
        SimpleNamespace(
            list_runs=lambda _job_id: json.dumps(
                {"data": [{"run_id": "run-1"}, {"run_id": "run-2"}]}
            ),
            delete_stable_job=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("Core deletion must not start after local cleanup fails")
            ),
        ),
    )

    def cleanup(job_id, **_kwargs):
        cleaned.append(job_id)
        if job_id == "run-1":
            raise JobResourceCleanupError("OpenShell sandbox is busy")

    monkeypatch.setattr(
        stable_job_cmds, "cleanup_cleared_job_resources", cleanup
    )
    monkeypatch.setattr(
        stable_job_cmds,
        "handle_cli_error",
        lambda error, _console, action: handled_errors.append((str(error), action)),
    )

    stable_job_cmds.delete("stable-job", yes=True)

    assert cleaned == ["run-1", "run-2", "stable-job"]
    assert handled_errors == [
        (
            "OpenShell sandbox is busy",
            "job delete",
        )
    ]
