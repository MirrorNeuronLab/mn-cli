from __future__ import annotations

from mn_cli.libs.progress_stream import (
    ProgressSnapshotStream,
    stream_api_workflow_progress,
)
from mn_cli.libs.workflow_progress import BlueprintWorkflowProgress


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def __iter__(self):
        return iter(
            [
                b"event: snapshot\n",
                b'data: {"version":2,"run_id":"researcher-346dab41d3","steps":[]}\n',
                b"\n",
            ]
        )


def test_api_progress_stream_uses_v2_execution_run_route(mocker):
    captured = {}

    def open_request(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        return _Response()

    mocker.patch(
        "mn_cli.libs.progress_stream.urllib.request.urlopen",
        side_effect=open_request,
    )

    snapshots = list(
        stream_api_workflow_progress(
            "http://localhost:54001/api/v1",
            "researcher-346dab41d3",
            timeout=12,
        )
    )

    assert captured == {
        "url": "http://localhost:54001/api/v1/runs/researcher-346dab41d3/workflow-progress/stream",
        "timeout": 12,
    }
    assert snapshots[0]["version"] == 2


def test_runtime_model_prepare_progress_flushes_immediately():
    stream = ProgressSnapshotStream(BlueprintWorkflowProgress({"workflow": {"steps": []}}))

    assert stream.observe_event(
        {
            "type": "runtime_model_install_progress",
            "payload": {
                "request_id": "pull",
                "model": "nemotron-3.5-lightning:latest",
                "node": "spark",
                "phase": "downloading",
            },
        }
    )
