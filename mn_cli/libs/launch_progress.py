"""Presentation for blocking launch calls; never runs or retries the operation."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from threading import Event, Thread

from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

HEARTBEAT_SECONDS = 10.0


def elapsed_text(seconds: float) -> str:
    seconds = max(0, int(seconds))
    minutes, seconds = divmod(seconds, 60)
    return f"{minutes}m {seconds:02d}s" if minutes else f"{seconds}s"


@contextmanager
def launch_activity(
    console, label: str, detail: str = "", *, expectation: str = ""
) -> Iterator[None]:
    from mn_cli.libs.ui import _is_plain_confirmation_mode, _status_console, print_info
    from mn_cli.output import json_enabled
    from mn_cli.terminal import use_progress

    if json_enabled():
        yield
        return

    status_console = _status_console(console)
    started = time.monotonic()
    message = f"{label} — {detail}" if detail else label
    if expectation:
        message += f" {expectation}"
    print_info(console, escape(message))
    interactive = not _is_plain_confirmation_mode() and use_progress(
        status_console.file
    )
    stopped = Event()

    def heartbeat() -> None:
        while not stopped.wait(HEARTBEAT_SECONDS):
            print_info(
                console,
                escape(
                    f"{label} — still waiting ({elapsed_text(time.monotonic() - started)} elapsed)."
                ),
            )

    progress = None
    thread = None
    if interactive:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}", markup=False),
            TimeElapsedColumn(),
            console=status_console,
            transient=True,
        )
        progress.add_task(label, total=None)
        progress.start()
    else:
        thread = Thread(target=heartbeat, name="mn-launch-progress", daemon=True)
        thread.start()
    try:
        yield
    finally:
        stopped.set()
        if thread is not None:
            thread.join()
        if progress is not None:
            progress.stop()
    elapsed = time.monotonic() - started
    if elapsed >= 1:
        print_info(console, escape(f"{label} — completed in {elapsed_text(elapsed)}."))
