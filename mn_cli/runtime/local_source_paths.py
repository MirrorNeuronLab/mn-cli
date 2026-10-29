"""Resolve local development dependencies against an execution node's checkout."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path, PurePosixPath, PureWindowsPath

from mn_sdk.requirement_extras import split_requirement_extras


def rebase_local_source_requirement(requirement: str, roots: Iterable[Path]) -> str:
    """Translate a foreign absolute path only to one verified checkout source.

    The workspace-relative suffix identifies the project, independently of the
    submitter's username, checkout location, OS, or repository name. Existing
    local paths and unresolved or ambiguous requirements retain normal pip
    validation. Resolving symlinks must never escape the receiving checkout.
    """
    location, extras = split_requirement_extras(requirement)
    if Path(location).expanduser().exists():
        return requirement

    windows = PureWindowsPath(location)
    source = windows if windows.is_absolute() else PurePosixPath(location)
    if not source.is_absolute() or ".." in source.parts:
        return requirement

    matches: set[Path] = set()
    for checkout in roots:
        checkout = checkout.expanduser().resolve()
        # Skip the foreign drive/share or POSIX root. A suffix is accepted only
        # when it identifies an actual Python project or wheel in this checkout.
        for index in range(1, len(source.parts)):
            candidate = checkout.joinpath(*source.parts[index:]).resolve()
            if candidate == checkout or not candidate.is_relative_to(checkout):
                continue
            if (candidate.is_dir() and candidate.joinpath("pyproject.toml").is_file()) or (
                candidate.is_file() and candidate.suffix == ".whl"
            ):
                matches.add(candidate)

    if len(matches) != 1:
        return requirement
    return str(matches.pop()) + extras
