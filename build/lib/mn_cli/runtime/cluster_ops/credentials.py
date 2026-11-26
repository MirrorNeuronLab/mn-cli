from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class ClusterCredentials:
    """Cluster credential fingerprints for safe diagnostics.

    Raw tokens and cookies are intentionally not exposed by diagnostic helpers.
    """

    token_sha256: str = ""
    cookie_sha256: str = ""

    @classmethod
    def from_values(cls, *, token: str = "", cookie: str = "") -> "ClusterCredentials":
        return cls(
            token_sha256=_sha256(token),
            cookie_sha256=_sha256(cookie),
        )

    def matches(self, other: "ClusterCredentials") -> bool:
        return bool(
            self.token_sha256
            and self.cookie_sha256
            and self.token_sha256 == other.token_sha256
            and self.cookie_sha256 == other.cookie_sha256
        )


def _sha256(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
