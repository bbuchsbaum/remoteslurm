from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from remoteslurm.cluster import Cluster

FAKESLURM = Path(__file__).parent / "fakeslurm"


@pytest.fixture
def sandbox(tmp_path: Path) -> Path:
    """A scratch tree with a few files of known content."""
    (tmp_path / "proj").mkdir()
    (tmp_path / "proj" / "a.txt").write_text("alpha\nbeta\ngamma\n")
    (tmp_path / "proj" / "b.log").write_text("".join(f"line {i}\n" for i in range(1000)))
    (tmp_path / "proj" / "bin.dat").write_bytes(b"\x00\x01\x02" * 100)
    (tmp_path / "proj" / "sub").mkdir()
    (tmp_path / "proj" / "sub" / "c.py").write_text("import os\nprint('beta')\n")
    (tmp_path / "proj" / ".hidden").write_text("secret\n")
    return tmp_path


@pytest.fixture
def cluster(sandbox: Path) -> Iterator[Cluster]:
    """A Cluster running the stub locally with HOME pointed at the sandbox."""
    from remoteslurm.transport import LocalTransport

    env = {"HOME": str(sandbox), "PATH": f"{FAKESLURM}{os.pathsep}{os.environ['PATH']}"}
    from remoteslurm.config import HostConfig

    host = HostConfig(name="local", ssh="local", mfa=False)
    c = Cluster(host, LocalTransport(env=env))
    c.session.start()
    try:
        yield c
    finally:
        c.close()
