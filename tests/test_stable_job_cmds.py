import json
from contextlib import contextmanager
from types import SimpleNamespace

import mn_cli.libs.stable_job_cmds as stable_job_cmds
from mn_cli.libs.job_cleanup import JobResourceCleanupError


def test_run_resume_shows_activity_while_waiting_for_runtime(monkeypatch):
    events = []
    rendered = []

    @contextmanager
    def fake_activity(_console, message):
        events.append(("start", message))
        yield
        events.append(("end", message))

    def resume(run_id):
        assert events == [("start", "Resuming run run-1…")]
        return json.dumps({"run_id": run_id, "status": "resumed"})

    monkeypatch.setattr(stable_job_cmds, "activity", fake_activity)
    monkeypatch.setattr(stable_job_cmds, "client", SimpleNamespace(resume_run=resume))
    monkeypatch.setattr(
        stable_job_cmds,
        "print_success_confirmation",
        lambda _console, action, **kwargs: rendered.append((action, kwargs)),
    )
    monkeypatch.setattr(stable_job_cmds, "record_result", lambda _result: None)

    stable_job_cmds.run_resume("run-1")

    assert events == [
        ("start", "Resuming run run-1…"),
        ("end", "Resuming run run-1…"),
    ]
    assert rendered == [("Run resume", {"status": "resumed", "details": {"Run ID": "run-1"}})]


def test_create_prepares_source_bundle_before_stable_submission(monkeypatch, tmp_path):
    bundle = tmp_path / "blueprint"
    bundle.mkdir()
    prepared = SimpleNamespace(
        manifest_json='{"graph_id":"prepared","nodes":[]}',
        payloads={"runtime.py": b"prepared"},
    )
    calls = {}

    monkeypatch.setattr(
        stable_job_cmds,
        "read_bundle",
        lambda _bundle: ('{"apiVersion":"mn.workflow.source/v2"}', {"source.py": b"raw"}),
    )
    monkeypatch.setattr(
        stable_job_cmds,
        "prepare_manifest_for_submission",
        lambda bundle_dir, manifest, **kwargs: {
            "graph_id": "prepared",
            "source_manifest": manifest,
        },
    )
    monkeypatch.setattr(
        stable_job_cmds,
        "_stage_bundle_payloads",
        lambda _bundle_dir, _manifest: {"runtime.py": b"staged"},
    )

    def prepare(manifest_json, payloads, **kwargs):
        calls["prepare"] = (manifest_json, payloads, kwargs)
        return prepared

    monkeypatch.setattr(stable_job_cmds, "prepare_job_submission", prepare)
    monkeypatch.setattr(
        stable_job_cmds,
        "client",
        SimpleNamespace(
            create_stable_job=lambda manifest_json, payloads, **kwargs: json.dumps(
                {
                    "job_id": "stable-job",
                    "manifest_json": manifest_json,
                    "payloads": sorted(payloads),
                    "kwargs": kwargs,
                }
            )
        ),
    )
    printed = []
    monkeypatch.setattr(stable_job_cmds, "record_result", printed.append)

    stable_job_cmds.create(str(bundle), job_id="stable-job", config=None)

    assert calls["prepare"][2]["bundle_dir"] == str(bundle.resolve())
    assert calls["prepare"][2]["job_id"] == "stable-job"
    assert calls["prepare"][0]["graph_id"] == "prepared"
    assert calls["prepare"][1] == {"runtime.py": b"staged"}
    assert printed[0]["manifest_json"] == prepared.manifest_json
    assert printed[0]["payloads"] == ["runtime.py"]


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
    monkeypatch.setattr(stable_job_cmds, "record_result", printed.append)

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


def test_delete_cleans_historical_runs_from_v2_items_response(monkeypatch):
    cleaned = []
    printed = []
    monkeypatch.setattr(
        stable_job_cmds,
        "client",
        SimpleNamespace(
            list_runs=lambda _job_id: json.dumps({"items": [{"run_id": "run-1"}]}),
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
    monkeypatch.setattr(stable_job_cmds, "record_result", printed.append)

    stable_job_cmds.delete("stable-job", yes=True)

    assert cleaned == ["run-1", "stable-job"]
    assert printed == [{"job_id": "stable-job", "status": "deleted", "confirmed": True}]
