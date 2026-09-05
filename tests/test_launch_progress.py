from io import StringIO
from threading import Event

import pytest
from rich.console import Console

from mn_cli.libs import launch_progress, ui


@pytest.mark.parametrize(
    "mode,width", [("plain", 40), ("plain", 120), ("rich", 60), ("rich", 120)]
)
def test_launch_wait_reports_elapsed_time_and_stops_after_return(
    monkeypatch, mode, width
):
    monkeypatch.setenv("MN_CLI_OUTPUT", mode)
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setattr(launch_progress, "HEARTBEAT_SECONDS", 0.01)
    stream = StringIO()
    console = Console(file=stream, force_terminal=False, width=width)
    observed = Event()
    original = ui.print_info

    def record(console, message):
        original(console, message)
        if "still waiting" in message:
            observed.set()

    monkeypatch.setattr(ui, "print_info", record)
    with ui.launch_activity(
        console, "Prepare DockerWorker", "building or reusing its image"
    ):
        assert observed.wait(2)
    output = stream.getvalue()
    assert "elapsed" in output
    assert "Prepare DockerWorker" in output
    assert "\x1b" not in output
    assert "%" not in output
    observed.clear()
    assert not observed.wait(0.03)
    assert stream.getvalue() == output


def test_launch_wait_stops_on_interruption_without_swallowing_it(monkeypatch):
    monkeypatch.setenv("MN_CLI_OUTPUT", "plain")
    stream = StringIO()
    console = Console(file=stream)
    with pytest.raises(KeyboardInterrupt), ui.launch_activity(console, "Prepare runtime"):
        raise KeyboardInterrupt
    assert "completed" not in stream.getvalue()


def test_json_launch_does_not_create_progress_or_write_output(monkeypatch):
    monkeypatch.setattr("mn_cli.output.json_enabled", lambda: True)
    monkeypatch.setattr(
        launch_progress,
        "Thread",
        lambda **_kwargs: pytest.fail("JSON must not start progress"),
    )
    stream = StringIO()
    with ui.launch_activity(Console(file=stream), "Prepare runtime"):
        pass
    assert stream.getvalue() == ""


def test_interactive_launch_renders_literal_label_and_elapsed_time(monkeypatch):
    monkeypatch.setenv("MN_CLI_OUTPUT", "rich")
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setattr("mn_cli.terminal.use_progress", lambda _stream: True)
    stream = StringIO()
    with ui.launch_activity(
        Console(file=stream, force_terminal=True, width=60), "Prepare [worker]"
    ):
        pass
    output = stream.getvalue()
    assert "[worker]" in output
    assert "0:00" in output


def test_elapsed_display_supports_long_builds():
    assert launch_progress.elapsed_text(125.9) == "2m 05s"
