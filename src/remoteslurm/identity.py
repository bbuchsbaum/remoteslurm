"""Execution-control identity shared by the client and local session daemon."""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path
from typing import Any

from .transport import stub_sha

CONTROL_PROTOCOL = 1


@lru_cache(maxsize=1)
def source_sha() -> str:
    """Hash the loaded package sources so same-version editable builds remain distinguishable."""
    digest = hashlib.sha256()
    root = Path(__file__).resolve().parent
    for path in sorted(root.glob("*.py")):
        digest.update(path.name.encode("utf-8") + b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()[:16]


def control_identity() -> dict[str, Any]:
    """Return the exact code identity expected across client, daemon, and remote stub."""
    from . import __version__

    sha = stub_sha()
    package_sha = source_sha()
    return {
        "control_protocol": CONTROL_PROTOCOL,
        "version": __version__,
        "stub_sha": sha,
        "source_sha": package_sha,
        "build_id": f"{CONTROL_PROTOCOL}:{__version__}:{package_sha}:{sha}",
    }
