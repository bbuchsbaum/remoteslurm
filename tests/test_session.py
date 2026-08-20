from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from remoteslurm.errors import SessionDied
from remoteslurm.session import Session
from remoteslurm.transport import LocalTransport, Transport


class NoisyTransport(Transport):
    """Simulates rc-file noise before the stub starts and a stray line afterwards."""

    def spawn(self) -> subprocess.Popen[bytes]:
        stub = Path(__file__).resolve().parents[1] / "src" / "remoteslurm" / "stub.py"
        code = (
            "import sys, runpy\n"
            "print('Lmod has detected the following error: blah')\n"
            "print('Welcome to cluster', flush=True)\n"
            f"sys.argv=[{str(stub)!r}]\n"
            f"runpy.run_path({str(stub)!r}, run_name='__main__')\n"
        )
        return subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


class DyingTransport(Transport):
    def spawn(self) -> subprocess.Popen[bytes]:
        return subprocess.Popen(
            [sys.executable, "-c", "import sys; print('garbage'); sys.exit(3)"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


class BootstrapErrorTransport(Transport):
    def spawn(self) -> subprocess.Popen[bytes]:
        return subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys; print('REMOTESLURM-ERROR python not found', file=sys.stderr); "
                "sys.exit(98)",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


def test_preamble_is_discarded() -> None:
    s = Session(NoisyTransport())
    s.start()
    try:
        assert s.call("ping")["protocol"] == 1
        assert any("Lmod" in line for line in s.preamble)
    finally:
        s.close()


def test_startup_death_reports_preamble() -> None:
    s = Session(DyingTransport(), ready_timeout=10)
    with pytest.raises(SessionDied) as ei:
        s.start()
    assert "garbage" in ei.value.details["preamble"]


def test_bootstrap_error_line() -> None:
    s = Session(BootstrapErrorTransport(), ready_timeout=10)
    with pytest.raises(SessionDied) as ei:
        s.start()
    assert "python not found" in ei.value.message


def test_stub_crash_fails_inflight_and_respawns() -> None:
    s = Session(LocalTransport())
    s.start()
    try:
        pid = s.remote_pid
        assert pid
        fut = s.submit("run", {"argv": ["sleep", "5"]})
        os.kill(pid, signal.SIGKILL)
        with pytest.raises(SessionDied):
            fut.result(timeout=10)
        # next call transparently respawns
        assert s.call("ping")["pid"] != pid
        assert s.spawn_count == 2
    finally:
        s.close()


def test_close_is_idempotent() -> None:
    s = Session(LocalTransport())
    s.start()
    s.close()
    s.close()
    assert not s.alive
