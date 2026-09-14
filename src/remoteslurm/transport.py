"""Transports: how a stub process is spawned.

* :class:`SSHTransport` — spawns ``ssh`` against the user's OpenSSH config and relies on a
  ControlMaster socket established interactively (Duo/MFA clusters) or automatically.
* :class:`LocalTransport` — runs the stub directly with the local interpreter. Used by the
  test-suite and handy for dry runs.
"""

from __future__ import annotations

import hashlib
import importlib.resources
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from typing import Any

from .errors import AuthRequired, NotConnected, RemoteTimeout

# `connection` warns when a master with a known session_lifetime has less than this left.
EXPIRY_WARN_SECONDS = 3600

SSH_BATCH_OPTS = [
    "-T",
    "-o",
    "BatchMode=yes",
    "-o",
    "ControlMaster=no",
    "-o",
    "ConnectTimeout=8",
    "-o",
    "LogLevel=ERROR",
]


@lru_cache(maxsize=1)
def stub_source() -> str:
    return importlib.resources.files("remoteslurm").joinpath("stub.py").read_text("utf-8")


@lru_cache(maxsize=1)
def stub_sha() -> str:
    return hashlib.sha256(stub_source().encode("utf-8")).hexdigest()[:16]


class Transport:
    name = "base"

    def spawn(self) -> subprocess.Popen[bytes]:
        raise NotImplementedError

    def describe(self) -> str:
        return self.name


@dataclass
class LocalTransport(Transport):
    python: str = sys.executable
    env: dict[str, str] | None = None
    name: str = "local"

    def spawn(self) -> subprocess.Popen[bytes]:
        path = importlib.resources.files("remoteslurm").joinpath("stub.py")
        env = dict(os.environ)
        if self.env:
            env.update(self.env)
        return subprocess.Popen(
            [self.python, "-u", str(path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )


@dataclass
class SSHTransport(Transport):
    """Spawn the stub over ssh, bootstrapping it on the remote in the same round trip."""

    alias: str
    mfa: bool = True
    python: str = "python3"
    install_dir: str | None = None  # remote dir; default ~/.cache/remoteslurm with fallbacks
    control_path: str | None = None  # only used when the ssh config lacks one
    control_persist: str = "12h"
    extra_ssh_opts: list[str] = field(default_factory=list)
    name: str = "ssh"

    # -- ssh helpers -----------------------------------------------------------------
    def _ssh_base(self) -> list[str]:
        cmd = ["ssh", *SSH_BATCH_OPTS, *self.extra_ssh_opts]
        if self.control_path:
            cmd += ["-o", f"ControlPath={self.control_path}"]
        return cmd

    def control_cmd(self, op: str) -> list[str]:
        cmd = ["ssh", "-o", "LogLevel=ERROR"]
        if self.control_path:
            cmd += ["-o", f"ControlPath={self.control_path}"]
        return [*cmd, "-O", op, self.alias]

    def master_alive(self) -> bool:
        try:
            r = subprocess.run(self.control_cmd("check"), capture_output=True, timeout=10)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False
        return r.returncode == 0

    def master_pid(self) -> int | None:
        """PID of the local ssh master (``ssh -O check`` prints it), or None if none is running."""
        try:
            r = subprocess.run(self.control_cmd("check"), capture_output=True, timeout=10)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None
        if r.returncode != 0:
            return None
        m = re.search(rb"pid=(\d+)", r.stderr + r.stdout)
        return int(m.group(1)) if m else None

    def master_lifetime(self, session_lifetime: str | int | None = None) -> dict[str, Any]:
        """When the live master started and, given the site's ``session_lifetime``, when it ends.

        Returns ``master_pid``, ``control_persist``, ``connected_at``/``age_seconds``/``age`` (from
        the local master process's start time) and, when ``session_lifetime`` is set,
        ``expires_at``/``remaining_seconds``/``expiring`` (less than ``EXPIRY_WARN_SECONDS``
        left). Without it there is no fixed expiry to report — ControlPersist is an *idle*
        timeout — and ``lifetime_note`` says so.
        """
        from .config import parse_duration

        pid = self.master_pid()
        age = process_age(pid) if pid else None
        out: dict[str, Any] = {"master_pid": pid, "control_persist": self.control_persist}
        if age is None:
            out["lifetime_note"] = "could not determine when the ssh master started"
            return out
        now = time.time()
        out.update(
            {
                "connected_at": _iso(now - age),
                "age_seconds": age,
                "age": format_duration(age),
                "expires_at": None,
                "remaining_seconds": None,
                "expiring": False,
            }
        )
        if session_lifetime is None:
            out["lifetime_note"] = (
                "no fixed expiry known: OpenSSH keeps the master open while any client is "
                f"attached and closes it {self.control_persist} after the last one leaves "
                "(ControlPersist is an idle timeout). Set session_lifetime in the host config "
                "if the site cuts connections after a fixed time."
            )
            return out
        remaining = parse_duration(session_lifetime) - age
        out["expires_at"] = _iso(now + remaining)
        out["remaining_seconds"] = max(0, remaining)
        out["expiring"] = remaining < EXPIRY_WARN_SECONDS
        return out

    def master_exit(self) -> None:
        try:
            subprocess.run(self.control_cmd("exit"), capture_output=True, timeout=10)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

    def connect_cmd(self) -> list[str]:
        """Command a *human* runs (or we run with a TTY) to establish the master."""
        cmd = [
            "ssh",
            "-fN",
            "-o",
            "ControlMaster=auto",
            "-o",
            f"ControlPersist={self.control_persist}",
        ]
        if self.control_path:
            cmd += ["-o", f"ControlPath={self.control_path}"]
        cmd += [*self.extra_ssh_opts, self.alias]
        return cmd

    def establish_master(self, interactive: bool) -> None:
        """Establish the ControlMaster. Non-interactive only for hosts without MFA."""
        cmd = self.connect_cmd()
        if not interactive:
            cmd = [cmd[0], "-o", "BatchMode=yes", *cmd[1:]]
            r = subprocess.run(cmd, capture_output=True, timeout=60)
            if r.returncode != 0:
                raise AuthRequired(
                    f"could not establish ssh master for {self.alias}: "
                    f"{r.stderr.decode(errors='replace').strip()}",
                    action=f"run: remoteslurm connect {self.alias}",
                )
            return
        r = subprocess.run(cmd)
        if r.returncode != 0:
            raise AuthRequired(
                f"ssh to {self.alias} failed (exit {r.returncode})",
                action="check `ssh " + self.alias + "` works interactively, then retry",
            )

    def ensure_master(self) -> None:
        if self.master_alive():
            return
        self.master_exit()  # clear a stale socket if any
        if self.mfa:
            raise NotConnected(
                f"no live ssh connection to {self.alias} "
                "(MFA host; cannot authenticate automatically)",
                action=f"run in a terminal: remoteslurm connect {self.alias}",
            )
        self.establish_master(interactive=False)

    def run_ssh(
        self, remote_cmd: str, *, timeout: float = 60, input: bytes | None = None
    ) -> subprocess.CompletedProcess[bytes]:
        """Run a raw command over the (already established) master. Escape hatch."""
        try:
            return subprocess.run(
                [*self._ssh_base(), self.alias, remote_cmd],
                capture_output=True,
                timeout=timeout,
                input=input,
            )
        except subprocess.TimeoutExpired as e:
            raise RemoteTimeout(
                f"ssh command timed out after {timeout}s", command=remote_cmd
            ) from e

    # -- stub bootstrap ----------------------------------------------------------------
    def remote_bootstrap_script(self) -> str:
        """Shell snippet that installs the stub if missing and execs it (one round-trip).

        The stub source is fed on stdin *before* the protocol stream; the snippet reads it
        up to a sentinel line only when the file is missing, so steady state costs nothing.
        """
        sha = stub_sha()
        name = f"stub-{sha}.py"
        dirs = [self.install_dir] if self.install_dir else []
        dirs += [
            "$HOME/.cache/remoteslurm",
            "/tmp/$USER/.remoteslurm",
        ]
        dir_expr = " ".join(
            shlex.quote(d) if d.startswith("/") and "$" not in d else d for d in dirs
        )
        py = shlex.quote(self.python)
        n = len(stub_source().encode("utf-8"))
        return (
            "D='';"
            f"for c in {dir_expr}; do "
            'if mkdir -p "$c" 2>/dev/null && [ -w "$c" ] && [ -O "$c" ]; '
            'then chmod 700 "$c"; D="$c"; break; fi; done;'
            'if [ -z "$D" ]; then echo "REMOTESLURM-ERROR no writable install dir" >&2; '
            "exit 97; fi;"
            f'P="$D/{name}";'
            # Read exactly N bytes of stub source from stdin. BusyBox `head -c` may buffer-read
            # past N on a pipe (stealing bytes that belong to the protocol stream), so use it only
            # when it is GNU coreutils; otherwise fall back to `dd bs=1`, which reads one byte at a
            # time and never over-reads. The rest of stdin is the JSON protocol.
            f"RS_N={n};"
            "if head --version 2>/dev/null | grep -q coreutils; then RS_H=1; else RS_H=0; fi;"
            'rs_read() { if [ "$RS_H" = 1 ]; then head -c "$RS_N"; '
            'else dd bs=1 count="$RS_N" 2>/dev/null; fi; };'
            'if [ ! -f "$P" ]; then rs_read > "$P.tmp.$$" && mv "$P.tmp.$$" "$P"; '
            "else rs_read > /dev/null; fi;"
            # Remove superseded stubs left by older shas (only stub-*.py in the resolved dir $D,
            # never the one we just installed/kept at $P). Keeps the install dir from growing.
            'for f in "$D"/stub-*.py; do [ "$f" = "$P" ] || rm -f "$f"; done;'
            # Python discovery: the configured interpreter, then python3, then python; if none is
            # on PATH and a module system is present, `module load python` and retry python3.
            f"PY='';for cand in {py} python3 python; do "
            'if command -v "$cand" >/dev/null 2>&1; then PY="$cand"; break; fi; done;'
            'if [ -z "$PY" ]; then '
            "command -v module >/dev/null 2>&1 && module load python 2>/dev/null; "
            "command -v python3 >/dev/null 2>&1 && PY=python3; fi;"
            'if [ -z "$PY" ]; then echo "REMOTESLURM-ERROR python not found" >&2; exit 98; fi;'
            'exec "$PY" -u "$P"'
        )

    def spawn(self) -> subprocess.Popen[bytes]:
        self.ensure_master()
        # The user's login shell may be csh/tcsh; always run the POSIX snippet under sh.
        remote_cmd = "sh -c " + shlex.quote(self.remote_bootstrap_script())
        proc = subprocess.Popen(
            [*self._ssh_base(), self.alias, remote_cmd],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Always send the stub source first; the remote consumes exactly that many bytes
        # (installing it or discarding it), then the JSON protocol begins.
        assert proc.stdin is not None
        try:
            proc.stdin.write(stub_source().encode("utf-8"))
            proc.stdin.flush()
        except BrokenPipeError:
            pass
        return proc

    def describe(self) -> str:
        return f"ssh:{self.alias}"


def ssh_available() -> bool:
    return shutil.which("ssh") is not None


def parse_etime(text: str) -> int | None:
    """Seconds in a ``ps -o etime`` value (``[[dd-]hh:]mm:ss``), or None if it doesn't parse."""
    s = text.strip()
    days = 0
    try:
        if "-" in s:
            d, s = s.split("-", 1)
            days = int(d)
        parts = [int(x) for x in s.split(":")]
    except ValueError:
        return None
    if not 1 <= len(parts) <= 3 or days < 0 or any(p < 0 for p in parts):
        return None
    h, m, sec = [0] * (3 - len(parts)) + parts
    return days * 86400 + h * 3600 + m * 60 + sec


def process_age(pid: int) -> int | None:
    """Seconds since local process ``pid`` started (portable ``ps -o etime=``), or None."""
    try:
        r = subprocess.run(
            ["ps", "-o", "etime=", "-p", str(pid)], capture_output=True, text=True, timeout=5
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if r.returncode != 0:
        return None
    return parse_etime(r.stdout)


def format_duration(secs: float) -> str:
    """Compact duration: ``2d3h``, ``5h07m``, ``42m``."""
    secs = int(max(0, secs))
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return f"{d}d{h}h"
    if h:
        return f"{h}h{m:02d}m"
    return f"{m}m"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).astimezone().isoformat(timespec="seconds")
