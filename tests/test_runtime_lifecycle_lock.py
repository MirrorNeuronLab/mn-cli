import pytest

from mn_cli.runtime import lifecycle


def test_start_reset_exclusion_and_stale_pid_recovery(tmp_path, monkeypatch):
    home = tmp_path / "runtime home"
    monkeypatch.setattr(lifecycle, "process_start", lambda pid: "current-birth")
    with lifecycle.runtime_start_guard(home):
        with pytest.raises(RuntimeError, match="in progress"):
            with lifecycle.runtime_start_guard(home):
                pytest.fail("Concurrent start entered")
    lock = tmp_path / ".runtime home.lifecycle.lock"
    lock.mkdir()
    (lock / "owner").write_text("999999|old-birth\n")
    with lifecycle.runtime_start_guard(home):
        assert lock.exists()
    assert not lock.exists()
    reset = tmp_path / ".runtime home.reset.lock"
    reset.mkdir()
    (reset / "owner").write_text("999999|owner-birth\n")
    with pytest.raises(RuntimeError, match="reset is in progress"):
        with lifecycle.runtime_start_guard(home):
            pytest.fail("Reset fence bypassed")
    monkeypatch.setenv("MN_RESET_OWNER_PID", "999999")
    with lifecycle.runtime_start_guard(home):
        assert reset.exists()


def test_installer_child_joins_only_its_verified_parent_owner(tmp_path, monkeypatch):
    home = tmp_path / "runtime"
    lock = tmp_path / ".runtime.lifecycle.lock"
    lock.mkdir()
    (lock / "owner").write_text("999999|installer-birth\n")
    monkeypatch.setattr(lifecycle, "process_start", lambda pid: "installer-birth")
    monkeypatch.setenv("MN_INSTALL_OWNER_PID", "999999")
    with lifecycle.runtime_start_guard(home):
        assert lock.exists()
    assert lock.exists()
    monkeypatch.setenv("MN_INSTALL_OWNER_PID", "another-parent")
    with pytest.raises(RuntimeError, match="in progress"):
        with lifecycle.runtime_start_guard(home):
            pytest.fail("A different installer owner entered")
