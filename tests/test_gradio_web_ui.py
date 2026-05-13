import json

from fastapi.testclient import TestClient

from mn_cli.gradio_web_ui import _view_payload_for_run_id, create_app


def test_view_payload_reads_saved_ui_and_events(tmp_path):
    runs_root = tmp_path / "runs"
    run_dir = runs_root / "bp-run"
    run_dir.mkdir(parents=True)
    (run_dir / "ui.json").write_text(
        json.dumps(
            {
                "adapter": "gradio",
                "title": "Safety Monitor",
                "blueprint_id": "safety",
                "components": [{"type": "events", "event_types": ["alert"]}],
            }
        )
    )
    (run_dir / "web_ui.json").write_text(json.dumps({"status": "available"}))
    (run_dir / "events.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"type": "frame"}),
                json.dumps({"type": "alert", "payload": {"severity": "high"}}),
            ]
        )
    )

    header, status, video, counts, events, latest = _view_payload_for_run_id("bp-run", runs_root)

    assert "Safety Monitor" in header
    assert "safety" in header
    assert status == "available"
    assert video is None
    assert counts == {"alert": 1}
    assert events == [{"type": "alert", "payload": {"severity": "high"}}]
    assert latest["type"] == "alert"


def test_central_gradio_app_serves_dynamic_run_path_and_local_video(tmp_path):
    runs_root = tmp_path / "runs"
    bundle_dir = tmp_path / "bundle"
    run_dir = runs_root / "bp-run"
    video_path = bundle_dir / "demo.mp4"
    run_dir.mkdir(parents=True)
    bundle_dir.mkdir()
    video_path.write_bytes(b"video")
    (run_dir / "ui.json").write_text(
        json.dumps(
            {
                "adapter": "gradio",
                "title": "Safety Monitor",
                "components": [{"type": "video", "source": video_path.as_uri()}],
                "metadata": {"bundle_dir": str(bundle_dir)},
            }
        )
    )

    client = TestClient(create_app(runs_root))

    assert client.get("/runs/bp-run/ui").status_code == 200
    video_response = client.get("/runs/bp-run/ui/video")
    assert video_response.status_code == 200
    assert video_response.content == b"video"
