from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from remoteslurm import cli
from remoteslurm import cluster as cluster_mod

FAKESLURM = Path(__file__).parent / "fakeslurm"


@pytest.fixture
def cli_env(sandbox: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    cfg = tmp_path / "config.toml"
    path = f"{FAKESLURM}{os.pathsep}{os.environ['PATH']}"
    cfg.write_text(
        'default_host = "local"\n[hosts.local]\nssh = "local"\nmfa = false\n'
        f'[hosts.local.env]\nHOME = "{sandbox}"\nPATH = "{path}"\n'
    )
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(cfg))
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.delenv("REMOTESLURM_DEFAULT_HOST", raising=False)
    yield sandbox
    for c in list(cluster_mod._clusters.values()):
        c.close()


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, str, str]:
    rc = cli.main(list(argv))
    out = capsys.readouterr()
    return rc, out.out, out.err


def test_split_target() -> None:
    assert cli.split_target("mycluster:~/x", None) == ("mycluster", "~/x")
    assert cli.split_target("~/x", "h") == ("h", "~/x")
    assert cli.split_target("/abs:weird", "h") == ("h", "/abs:weird")
    assert cli.split_target("mycluster:", "h") == ("mycluster", "~")
    assert cli.split_target(None, "h") == ("h", "~")


def test_ls_human_and_json(cli_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, _ = run(capsys, "ls", "~/proj")
    assert rc == 0 and "a.txt" in out and "sub/" in out
    rc, out, _ = run(capsys, "--json", "ls", "local:~/proj", "--limit", "2")
    d = json.loads(out)
    assert rc == 0 and d["truncated"] and [e["name"] for e in d["entries"]] == [".hidden", "a.txt"]
    rc, out, err = run(capsys, "ls", "~/proj", "--limit", "2")
    assert "more" in err


def test_cat_tail_grep_find(cli_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, _ = run(capsys, "cat", "~/proj/a.txt")
    assert rc == 0 and out == "alpha\nbeta\ngamma\n"
    rc, out, err = run(capsys, "cat", "~/proj/b.log", "--max-bytes", "10")
    assert out == "line 0\nlin" and "truncated" in err
    rc, out, _ = run(capsys, "tail", "-n", "2", "~/proj/b.log")
    assert out == "line 998\nline 999\n"
    rc, out, _ = run(capsys, "grep", "beta", "~/proj", "-g", "*.txt")
    assert rc == 0 and out.strip().endswith("a.txt:2:beta")
    rc, out, _ = run(capsys, "find", "~/proj", "*.py")
    assert out.strip().endswith("sub/c.py")


def test_run(cli_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, _ = run(capsys, "run", "echo hi; exit 4")
    assert rc == 4 and out == "hi\n"
    rc, out, _ = run(capsys, "--json", "run", "--argv", "echo", "a b")
    assert rc == 0 and json.loads(out)["stdout"] == "a b\n"


def test_put_get(cli_env: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    src = tmp_path / "up.txt"
    src.write_text("up\n")
    rc, out, _ = run(capsys, "put", str(src), "~/incoming/")
    assert rc == 0 and (cli_env / "incoming/up.txt").read_text() == "up\n"
    dest = tmp_path / "down"
    dest.mkdir()
    rc, out, _ = run(capsys, "get", "~/proj/a.txt", str(dest))
    assert rc == 0 and (dest / "a.txt").read_text() == "alpha\nbeta\ngamma\n"


def test_pack_command_file(
    cli_env: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    commands = tmp_path / "commands.txt"
    commands.write_text("echo one\necho two\necho three\n")

    rc, out, _ = run(
        capsys,
        "--json",
        "pack",
        str(commands),
        "--max-processes",
        "2",
        "--batches",
        "2",
    )
    result = json.loads(out)

    assert rc == 0
    assert result["n"] == 3
    assert result["batches"] == 2
    assert result["max_processes"] == 2


def test_errors_are_structured(cli_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, err = run(capsys, "cat", "~/nope")
    assert rc == 1 and "error [not_found]" in err
    rc, out, err = run(capsys, "--json", "cat", "~/nope")
    assert rc == 1 and json.loads(out)["error"] == "not_found"


def test_not_connected_exit_code(
    cli_env: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from remoteslurm.transport import SSHTransport

    monkeypatch.setattr(SSHTransport, "master_alive", lambda self: False)
    monkeypatch.setattr(SSHTransport, "master_exit", lambda self: None)
    rc, out, err = run(capsys, "--json", "ls", "someMfaHost:~")
    assert rc == 3
    d = json.loads(out)
    assert d["error"] == "not_connected" and "remoteslurm connect someMfaHost" in d["action"]


def test_config_and_mcp_config(
    cli_env: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc, out, _ = run(capsys, "--json", "config")
    d = json.loads(out)
    assert rc == 0 and d["default_host"] == "local" and "local" in d["hosts"]
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(tmp_path / "fresh.toml"))
    rc, out, _ = run(capsys, "config", "--init")
    assert rc == 0 and (tmp_path / "fresh.toml").exists()
    rc, out, _ = run(capsys, "config", "--init")
    assert rc == 1


def test_run_detach_proc_and_wait(cli_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, _ = run(
        capsys,
        "run",
        "--no-daemon",
        "--json",
        "--detach",
        "echo hi; sleep 0.3; echo READY; sleep 300",
    )
    assert rc == 0
    pid = json.loads(out)["pid"]
    try:
        rc, out, _ = run(
            capsys,
            "wait",
            "--no-daemon",
            "--pid",
            str(pid),
            "--pattern",
            "READY",
            "--timeout",
            "20",
        )
        assert rc == 0 and out.strip() == "READY"
        rc, out, _ = run(capsys, "proc", "--no-daemon", str(pid))
        assert rc == 0 and f"pid {pid} running" in out
        rc, out, _ = run(capsys, "proc", "--no-daemon")
        assert rc == 0 and str(pid) in out
        rc, out, _ = run(capsys, "proc", "tail", str(pid), "--no-daemon")
        assert rc == 0 and "hi\nREADY\n" in out
        rc, out, _ = run(capsys, "proc", "kill", str(pid), "--grace", "2", "--no-daemon")
        assert rc == 0 and out.startswith("killed")
        # exited, but by SIGTERM (rc 143) -> non-zero exit, like `wait JOB` on a failed job
        rc, _, err = run(capsys, "wait", "--no-daemon", "--pid", str(pid), "--timeout", "10")
        assert rc == 1 and "rc=143" in err
    finally:
        cli.main(["proc", "kill", str(pid), "--signal", "KILL", "--grace", "0", "--no-daemon"])
        capsys.readouterr()


def test_wait_and_proc_argument_errors(cli_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, _, err = run(capsys, "wait", "--no-daemon")
    assert rc == 1 and "invalid_arg" in err
    rc, _, err = run(capsys, "wait", "123", "--pid", "5", "--no-daemon")
    assert rc == 1 and "not both" in err
    rc, _, err = run(capsys, "proc", "tail", "--no-daemon")
    assert rc == 1 and "needs a PID" in err
    rc, _, err = run(capsys, "proc", "frobnicate", "--no-daemon")
    assert rc == 1 and "unknown action" in err


def test_doctor_local(cli_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, err = run(capsys, "--json", "doctor")
    d = json.loads(out)
    names = {c["check"]: c for c in d["checks"]}
    assert "stub session" in names and names["stub session"]["ok"]
    assert "home writable" in names
