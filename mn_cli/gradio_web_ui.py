from __future__ import annotations

import argparse
import json
import os
import re
import urllib.parse
from collections import Counter, deque
from pathlib import Path
from typing import Any

import gradio as gr
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse


DEFAULT_RUNS_ROOT = Path("~/.mn/runs")
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


def _safe_run_dir(run_id: str, runs_root: Path) -> Path:
    if not _SAFE_RUN_ID.match(run_id):
        raise HTTPException(status_code=400, detail="invalid run id")
    root = runs_root.expanduser().resolve()
    run_dir = (root / run_id).resolve()
    if not run_dir.is_relative_to(root):
        raise HTTPException(status_code=400, detail="invalid run id")
    return run_dir


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_event_tail(path: Path, *, max_events: int, event_types: set[str] | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    selected_types = event_types or set()
    events: deque[dict[str, Any]] = deque(maxlen=max_events)
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    event = json.loads(stripped)
                except json.JSONDecodeError:
                    event = {"type": "unparseable_event", "payload": {"line": stripped}}
                if not isinstance(event, dict):
                    continue
                if selected_types and event.get("type") not in selected_types:
                    continue
                events.append(event)
    except OSError:
        return []
    return list(events)


def _components(ui: dict[str, Any]) -> list[dict[str, Any]]:
    components = ui.get("components") if isinstance(ui.get("components"), list) else []
    return [item for item in components if isinstance(item, dict)]


def _component(ui: dict[str, Any], component_type: str) -> dict[str, Any]:
    for item in _components(ui):
        if item.get("type") == component_type:
            return item
    return {}


def _event_types(ui: dict[str, Any]) -> set[str]:
    raw_types = _component(ui, "events").get("event_types")
    if not isinstance(raw_types, list):
        return set()
    return {str(item) for item in raw_types if isinstance(item, str)}


def _first_video_source(ui: dict[str, Any]) -> str:
    source = _component(ui, "video").get("source")
    return str(source) if source else ""


def _local_source_path(source: str, run_dir: Path) -> Path | None:
    if not source:
        return None
    if source.startswith("file://"):
        parsed = urllib.parse.urlparse(source)
        return Path(urllib.parse.unquote(parsed.path)).expanduser().resolve()
    if "://" in source:
        return None
    candidate = Path(source).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def _allowed_local_roots(run_dir: Path, ui: dict[str, Any]) -> list[Path]:
    metadata = ui.get("metadata") if isinstance(ui.get("metadata"), dict) else {}
    roots = [run_dir.resolve()]
    bundle_dir = metadata.get("bundle_dir")
    if isinstance(bundle_dir, str) and bundle_dir:
        roots.append(Path(bundle_dir).expanduser().resolve())
    extra = os.getenv("MN_GRADIO_UI_ALLOWED_PATHS", "")
    for item in extra.split(os.pathsep):
        if item:
            roots.append(Path(item).expanduser().resolve())
    return roots


def _is_allowed_local_path(path: Path, roots: list[Path]) -> bool:
    return any(path == root or path.is_relative_to(root) for root in roots)


def _video_url(run_id: str, ui: dict[str, Any]) -> str | None:
    source = _first_video_source(ui)
    if not source:
        return None
    if source.startswith(("http://", "https://", "data:", "blob:")):
        return source
    return f"/runs/{urllib.parse.quote(run_id, safe='')}/ui/video"


def _view_payload_for_run_id(run_id: str, runs_root: Path) -> tuple[str, str, str | None, dict[str, int], list[dict[str, Any]], dict[str, Any]]:
    run_dir = _safe_run_dir(run_id, runs_root)
    ui = _read_json_file(run_dir / "ui.json")
    web_ui = _read_json_file(run_dir / "web_ui.json")
    run = _read_json_file(run_dir / "run.json")

    if not ui:
        title = "Blueprint UI not ready"
        header = f"# {title}\nRun `{run_id}` does not have a saved `ui.json` yet."
        return header, "not_ready", None, {}, [], {}

    max_events = int(_component(ui, "events").get("max_events") or 200)
    events = _read_event_tail(run_dir / "events.jsonl", max_events=max_events, event_types=_event_types(ui))
    counts = dict(Counter(str(event.get("type") or "event") for event in events))
    title = str(ui.get("title") or web_ui.get("title") or "Blueprint Run")
    blueprint_id = str(ui.get("blueprint_id") or (web_ui.get("metadata") or {}).get("blueprint_id") or "unknown")
    status = str(web_ui.get("status") or run.get("status") or "available")
    header = f"# {title}\nRun `{run_id}`  \nBlueprint `{blueprint_id}`"
    latest = events[-1] if events else {}
    return header, status, _video_url(run_id, ui), counts, events, latest


def _run_id_from_request(request: gr.Request | None) -> str:
    raw_request = getattr(request, "request", None)
    if raw_request is None:
        return ""
    path_params = getattr(raw_request, "path_params", {}) or {}
    run_id = path_params.get("run_id")
    if run_id:
        return str(run_id)
    query_params = getattr(raw_request, "query_params", {}) or {}
    return str(query_params.get("run_id") or "")


def _build_blocks(runs_root: Path) -> gr.Blocks:
    with gr.Blocks(title="MirrorNeuron Blueprint UI") as demo:
        header = gr.Markdown("# MirrorNeuron Blueprint UI")
        with gr.Row():
            status = gr.Textbox(label="Status", interactive=False)
            refresh_note = gr.Textbox(label="Source", value="~/.mn run store", interactive=False)
        with gr.Row():
            video = gr.Video(label="Video Source")
            with gr.Column():
                counts = gr.JSON(label="Event Counts")
                latest = gr.JSON(label="Latest Event")
        events = gr.JSON(label="Event Tail")
        refresh = gr.Button("Refresh")

        def refresh_view(request: gr.Request):
            run_id = _run_id_from_request(request)
            if not run_id:
                return "# Blueprint UI not ready\nNo run id was provided.", "not_ready", "missing run id", None, {}, {}, []
            view = _view_payload_for_run_id(run_id, runs_root)
            markdown, status_value, video_value, count_value, event_value, latest_value = view
            return markdown, status_value, str(runs_root.expanduser()), video_value, count_value, latest_value, event_value

        outputs = [header, status, refresh_note, video, counts, latest, events]
        demo.load(refresh_view, inputs=None, outputs=outputs)
        refresh.click(refresh_view, inputs=None, outputs=outputs)
        if hasattr(gr, "Timer"):
            timer = gr.Timer(value=2.0)
            timer.tick(refresh_view, inputs=None, outputs=outputs)
    return demo


def create_app(runs_root: str | Path | None = None) -> FastAPI:
    resolved_runs_root = Path(runs_root or os.getenv("MN_RUNS_ROOT") or DEFAULT_RUNS_ROOT).expanduser()
    app = FastAPI(title="MirrorNeuron Central Gradio UI")

    @app.get("/")
    def index():
        root = resolved_runs_root.expanduser()
        links = []
        if root.exists():
            for run_dir in sorted(root.iterdir(), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:20]:
                if (run_dir / "ui.json").exists():
                    run_id = run_dir.name
                    href = f"/runs/{urllib.parse.quote(run_id, safe='')}/ui"
                    links.append(f'<li><a href="{href}">{run_id}</a></li>')
        body = "<h1>MirrorNeuron Blueprint UI</h1>"
        body += "<p>Open a run URL such as <code>/runs/&lt;run_id&gt;/ui</code>.</p>"
        if links:
            body += "<ul>" + "".join(links) + "</ul>"
        return HTMLResponse(body)

    @app.get("/runs/{run_id}/ui/video")
    def run_video(run_id: str):
        run_dir = _safe_run_dir(run_id, resolved_runs_root)
        ui = _read_json_file(run_dir / "ui.json")
        source_path = _local_source_path(_first_video_source(ui), run_dir)
        if source_path is None:
            raise HTTPException(status_code=404, detail="local video not configured")
        if not _is_allowed_local_path(source_path, _allowed_local_roots(run_dir, ui)):
            raise HTTPException(status_code=403, detail="video source is outside allowed roots")
        if not source_path.exists() or not source_path.is_file():
            raise HTTPException(status_code=404, detail="video source not found")
        return FileResponse(source_path)

    gr.mount_gradio_app(app, _build_blocks(resolved_runs_root), path="/runs/{run_id}/ui")
    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the central MirrorNeuron Gradio blueprint UI.")
    parser.add_argument("--host", default=os.getenv("MN_GRADIO_UI_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MN_GRADIO_UI_PORT", "7860")))
    parser.add_argument("--runs-root", default=os.getenv("MN_RUNS_ROOT", str(DEFAULT_RUNS_ROOT)))
    parser.add_argument("--log-level", default=os.getenv("MN_GRADIO_UI_LOG_LEVEL", "info"))
    args = parser.parse_args(argv)

    import uvicorn

    uvicorn.run(
        create_app(args.runs_root),
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
