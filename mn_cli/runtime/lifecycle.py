"""Serialize startup with destructive installer reset outside the deleted tree."""
from contextlib import contextmanager
import os
from pathlib import Path
import subprocess


def process_start(pid):
    try:
        return subprocess.check_output(["ps", "-p", str(pid), "-o", "lstart="],
                                       text=True, stderr=subprocess.DEVNULL, timeout=2).strip()
    except subprocess.CalledProcessError as error:
        if error.returncode == 1:
            return ""
        raise RuntimeError("Could not verify runtime process identity.") from error
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError("Could not verify runtime process identity.") from error


@contextmanager
def runtime_start_guard(home: Path):
    lock = home.parent / f".{home.name}.lifecycle.lock"
    reset = home.parent / f".{home.name}.reset.lock"
    if reset.exists():
        owner = (reset / "owner").read_text().split("|", 1)[0]
        if owner == os.environ.get("MN_RESET_OWNER_PID"):
            yield
            return
        raise RuntimeError("Runtime reset is in progress. Wait for it to finish before starting.")
    lock.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(2):
        try:
            lock.mkdir(mode=0o700)
            break
        except FileExistsError:
            try:
                pid, birth = (lock / "owner").read_text().rstrip("\n").split("|", 1)
                alive = process_start(int(pid))
            except (OSError, ValueError):
                raise RuntimeError("Runtime lifecycle lock is incomplete. Inspect it before retrying.") from None
            if alive and alive == birth:
                if pid == os.environ.get("MN_INSTALL_OWNER_PID"):
                    yield
                    return
                raise RuntimeError("Another runtime start or reset is in progress.") from None
            (lock / "owner").unlink()
            lock.rmdir()
    else:
        raise RuntimeError("Could not acquire runtime lifecycle lock.")
    try:
        birth = process_start(os.getpid())
        if not birth:
            raise RuntimeError("Could not verify runtime startup process identity.")
        (lock / "owner").write_text(f"{os.getpid()}|{birth}\n")
        if reset.exists():
            raise RuntimeError("Runtime reset is in progress.")
        yield
    finally:
        (lock / "owner").unlink(missing_ok=True)
        lock.rmdir()
