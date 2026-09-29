"""Hold a macOS idle-sleep assertion only while a run is being followed."""

from contextlib import contextmanager
import os
import subprocess
import sys


def awake_command(command: list[str]) -> list[str]:
    if sys.platform == "darwin":
        return ["/usr/bin/caffeinate", "-i", *command]
    return command


@contextmanager
def prevent_idle_sleep():
    process = None
    if sys.platform == "darwin":
        process = subprocess.Popen(
            ["/usr/bin/caffeinate", "-i", "-w", str(os.getpid())],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    try:
        yield
    finally:
        if process is not None:
            process.terminate()
            process.wait()
