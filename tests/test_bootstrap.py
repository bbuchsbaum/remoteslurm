"""WP-F4: bootstrap snippet portability — shell matrix, BusyBox `head`, python discovery.

The remote bootstrap snippet (``SSHTransport.remote_bootstrap_script``) runs on the login node
under ``sh -c`` before any Python is available, so it must be portable POSIX shell. These tests
exercise it *hermetically*: no ssh, no remote — the snippet is fed the stub source on stdin with a
fake ``python3`` on ``PATH`` and a temp install dir, so it installs the stub locally and "execs"
the fake interpreter. Every branch (fresh install, already-installed, BusyBox ``head``, GNU
``head``, python fallbacks) is covered without a cluster.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess

import pytest

from remoteslurm.transport import SSHTransport, stub_sha, stub_source

SHELLS = ["sh", "bash", "dash", "zsh", "ksh"]
AVAILABLE_SHELLS = [s for s in SHELLS if shutil.which(s)]

STUB_SRC = stub_source().encode("utf-8")
STUB_NAME = f"stub-{stub_sha()}.py"


def _write_exe(path: str, body: str) -> None:
    with open(path, "w") as f:
        f.write(body)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _fake_python(bindir: str, name: str = "python3") -> str:
    """A stand-in interpreter that prints a READY marker with the path it was handed (argv[2])."""
    p = os.path.join(bindir, name)
    _write_exe(p, '#!/bin/sh\nprintf "RS-READY %s\\n" "$2"\n')
    return p


def _run(snippet: str, *, shell: str = "sh", path: str, stdin: bytes = STUB_SRC):
    env = dict(os.environ)
    env["PATH"] = path
    return subprocess.run([shell, "-c", snippet], input=stdin, capture_output=True, env=env)


def _snippet(install_dir: str, python: str = "python3") -> str:
    return SSHTransport(alias="x", install_dir=install_dir, python=python).remote_bootstrap_script()


# --------------------------------------------------------------------------- syntax
@pytest.mark.parametrize("shell", AVAILABLE_SHELLS)
def test_snippet_is_valid_shell_syntax(shell: str, tmp_path) -> None:
    snip = _snippet(str(tmp_path))
    r = subprocess.run([shell, "-n", "-c", snip], capture_output=True, text=True)
    assert r.returncode == 0, f"{shell} -n failed: {r.stderr}"


# --------------------------------------------------------------------------- install + exec matrix
@pytest.mark.parametrize("shell", AVAILABLE_SHELLS)
def test_snippet_installs_stub_and_execs_interpreter(shell: str, tmp_path) -> None:
    install = tmp_path / "install"
    bindir = tmp_path / "bin"
    install.mkdir()
    bindir.mkdir()
    _fake_python(str(bindir))
    snip = _snippet(str(install))
    r = _run(snip, shell=shell, path=f"{bindir}{os.pathsep}{os.environ['PATH']}")
    assert r.returncode == 0, r.stderr
    dest = install / STUB_NAME
    assert dest.exists(), "stub not installed"
    assert dest.read_bytes() == STUB_SRC, "installed stub content mismatch"
    # exec replaced the shell with the fake interpreter, handed the resolved stub path.
    assert f"RS-READY {dest}".encode() in r.stdout


def test_already_installed_branch_does_not_rewrite(tmp_path) -> None:
    install = tmp_path / "install"
    bindir = tmp_path / "bin"
    install.mkdir()
    bindir.mkdir()
    _fake_python(str(bindir))
    path = f"{bindir}{os.pathsep}{os.environ['PATH']}"
    snip = _snippet(str(install))
    # First run installs.
    assert _run(snip, path=path).returncode == 0
    dest = install / STUB_NAME
    # Tamper the installed file; a second run must NOT overwrite it (it only drains stdin).
    with open(dest, "ab") as f:
        f.write(b"\n# TAMPER-MARKER\n")
    r = _run(snip, path=path)
    assert r.returncode == 0, r.stderr
    assert dest.read_bytes().endswith(b"# TAMPER-MARKER\n"), "already-installed stub was rewritten"
    assert f"RS-READY {dest}".encode() in r.stdout


# --------------------------------------------------------------------------- BusyBox / GNU head
def test_busybox_head_uses_dd_fallback(tmp_path) -> None:
    """A `head` whose --version lacks 'coreutils' and errors on -c: only the dd path can install."""
    install = tmp_path / "install"
    bindir = tmp_path / "bin"
    install.mkdir()
    bindir.mkdir()
    _fake_python(str(bindir))
    _write_exe(
        str(bindir / "head"),
        "#!/bin/sh\n"
        'if [ "$1" = "--version" ]; then echo "BusyBox v1.36.1 multi-call binary"; exit 0; fi\n'
        'echo "head: unrecognized -c (busybox)" >&2; exit 1\n',
    )
    snip = _snippet(str(install))
    r = _run(snip, path=f"{bindir}{os.pathsep}{os.environ['PATH']}")
    assert r.returncode == 0, r.stderr
    dest = install / STUB_NAME
    # If `head -c` had been used it would have failed; correct content proves the dd fallback ran.
    assert dest.exists() and dest.read_bytes() == STUB_SRC


def test_gnu_head_path_reads_exactly_n(tmp_path) -> None:
    """A `head` reporting coreutils takes the `head -c N` branch (delegated to the real head)."""
    real_head = shutil.which("head")
    assert real_head
    install = tmp_path / "install"
    bindir = tmp_path / "bin"
    install.mkdir()
    bindir.mkdir()
    _fake_python(str(bindir))
    _write_exe(
        str(bindir / "head"),
        "#!/bin/sh\n"
        'if [ "$1" = "--version" ]; then echo "head (GNU coreutils) 9.3"; exit 0; fi\n'
        f'exec {real_head} "$@"\n',
    )
    snip = _snippet(str(install))
    r = _run(snip, path=f"{bindir}{os.pathsep}{os.environ['PATH']}")
    assert r.returncode == 0, r.stderr
    dest = install / STUB_NAME
    assert dest.exists() and dest.read_bytes() == STUB_SRC


# --------------------------------------------------------------------------- python discovery
def _minimal_bin(tmp_path, tools: list[str]) -> str:
    """A PATH dir with only ``tools`` symlinked in (used to force python discovery outcomes)."""
    b = tmp_path / "minbin"
    b.mkdir()
    for t in tools:
        src = shutil.which(t)
        if src:
            os.symlink(src, b / t)
    return str(b)


# Externals the snippet needs before it reaches the interpreter (rest are shell builtins).
_CORE_TOOLS = ["mkdir", "chmod", "head", "grep", "dd", "mv", "rm", "cat", "sh"]


def test_python_discovery_falls_back_to_python(tmp_path) -> None:
    """No python3 on PATH but `python` present: the for-loop resolves the configured chain to it."""
    install = tmp_path / "install"
    install.mkdir()
    minbin = _minimal_bin(tmp_path, _CORE_TOOLS)
    _write_exe(os.path.join(minbin, "python"), '#!/bin/sh\nprintf "RS-READY-PY2 %s\\n" "$2"\n')
    snip = _snippet(str(install))  # configured interpreter is python3 (absent) -> falls to python
    r = _run(snip, path=minbin)
    assert r.returncode == 0, r.stderr
    dest = install / STUB_NAME
    assert dest.exists() and dest.read_bytes() == STUB_SRC
    assert f"RS-READY-PY2 {dest}".encode() in r.stdout


def test_python_not_found_emits_structured_error(tmp_path) -> None:
    """No interpreter and no module system: the snippet exits 98 with REMOTESLURM-ERROR."""
    install = tmp_path / "install"
    install.mkdir()
    minbin = _minimal_bin(tmp_path, _CORE_TOOLS)  # deliberately no python/python3/module
    snip = _snippet(str(install))
    r = _run(snip, path=minbin)
    assert r.returncode == 98, r.stdout
    assert b"REMOTESLURM-ERROR python not found" in r.stderr
