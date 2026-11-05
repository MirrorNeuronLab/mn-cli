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
                b"event: run.snapshot\n",
                b'data: {"type":"run.snapshot","data":{"version":2,"run_id":"researcher-346dab41d3","steps":[]}}\n',
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
        "url": "http://localhost:54001/api/v1/runs/researcher-346dab41d3/events/stream",
        "timeout": 12,
    }
    assert snapshots[0]["version"] == 2


def test_runtime_model_prepare_progress_flushes_immediately():
    stream = ProgressSnapshotStream(
        BlueprintWorkflowProgress({"workflow": {"steps": []}})
    )

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


def test_monitor_manifest_keeps_child_metadata_and_live_steps():
    view = BlueprintWorkflowProgress(
        {
            "workflow": {
                "steps": [
                    {"id": "review"},
                    {
                        "id": "review:p0",
                        "parent_step_id": "review",
                        "child_round": 0,
                        "child_phase": "planning",
                    },
                ]
            }
        }
    )
    assert view.steps_by_id["review:p0"].parent_step_id == "review"
    view.update(
        {
            "type": "workflow_child_plan_committed",
            "topology_delta": {
                "steps_added": [
                    {
                        "id": "review:r1:scan",
                        "parent_step_id": "review",
                        "child_round": 1,
                        "child_phase": "executing",
                    }
                ]
            },
        }
    )
    assert view.steps_by_id["review:r1:scan"].child_round == 1


def test_async_watch_closes_open_http_response_on_cancel(monkeypatch):
    import asyncio
    import httpx
    from mn_cli.libs.progress_stream import watch_api_workflow_progress

    async def observe():
        reading, closed = asyncio.Event(), asyncio.Event()

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                reading.set()
                await asyncio.Event().wait()
                yield b""

            async def aclose(self):
                closed.set()

        async def respond(request):
            assert request.url.path == "/api/v1/runs/example/events/stream"
            assert request.headers["authorization"] == "Bearer fixture"
            return httpx.Response(200, stream=Body())

        real_client = httpx.AsyncClient
        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            lambda **kw: real_client(transport=httpx.MockTransport(respond), **kw),
        )
        stream = watch_api_workflow_progress(
            "http://owner/api/v1", "example", api_token="fixture"
        )
        pending = asyncio.create_task(anext(stream))
        await asyncio.wait_for(reading.wait(), 2)
        pending.cancel()
        try:
            await pending
        except asyncio.CancelledError:
            pass
        assert closed.is_set()
        await stream.aclose()

    asyncio.run(observe())
