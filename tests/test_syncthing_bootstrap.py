import os
import subprocess

import pytest

from mn_cli.runtime.syncthing_bootstrap import SHARED_STORAGE_OWNER_SCRIPT


@pytest.mark.parametrize("owner", ["501:20", "1000:1000", "0:0"])
def test_sidecar_uses_actual_mounted_storage_owner(tmp_path, owner):
    stat = tmp_path / "stat"
    stat.write_text(f"#!/bin/sh\nprintf '%s' '{owner}'\n")
    stat.chmod(0o755)
    result = subprocess.run(
        ["/bin/sh", "-ec", SHARED_STORAGE_OWNER_SCRIPT + '\nprintf "%s:%s" "$PUID" "$PGID"'],
        env={**os.environ, "PATH": str(tmp_path), "PUID": "1000", "PGID": "1000"},
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == owner


def test_unreadable_shared_mount_fails_before_starting_daemon(tmp_path):
    stat = tmp_path / "stat"
    stat.write_text("#!/bin/sh\nexit 1\n")
    stat.chmod(0o755)
    result = subprocess.run(
        ["/bin/sh", "-ec", SHARED_STORAGE_OWNER_SCRIPT + '\nprintf "daemon started"'],
        env={**os.environ, "PATH": str(tmp_path)},
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert result.stdout == ""
