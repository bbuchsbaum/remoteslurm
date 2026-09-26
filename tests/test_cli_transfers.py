"""Transfer regressions: argv fidelity and actual rsync destination layout."""

import argparse
import os
import shlex
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from remoteslurm import cli
from remoteslurm import sync as sync_mod
from remoteslurm.errors import InvalidArgument
from remoteslurm.transport import SSHTransport


def put_args(src, dest, **kw):
    return argparse.Namespace(
        src=src, dest=dest, host="fixture", rsync=False, force=False, json=True, **kw
    )


@pytest.mark.parametrize("slash", [False, True])
@pytest.mark.parametrize("style", ["relative", "absolute", "tilde"])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("dest_slash", [False, True])
def test_put_rsync_layout(tmp_path, monkeypatch, slash, style, existing, dest_slash):
    found = sync_mod.find_rsync()
    if found is None or found[1] < (3, 1):
        pytest.skip("rsync >= 3.1 is required")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    source = tmp_path / "bundle with spaces"
    source.mkdir()
    (source / "payload").write_bytes(b"payload\x00\xff")
    dest = tmp_path / "target with spaces"
    if existing:
        dest.mkdir()
        (dest / "keep").write_text("preserve")
    target = str(dest) + ("/" if dest_slash else "")
    raw = {"relative": source.name, "absolute": str(source), "tilde": "~/" + source.name}[style] + (
        "/" if slash else ""
    )
    c = MagicMock()
    c.transport = SSHTransport(alias="fixture")
    # Exercise the real CLI and rsync wrapper, replacing only SSH with a local server.
    fake = tmp_path / "fake ssh"
    fake.write_text(f'#!/bin/sh\nshift\nshift\nexec {shlex.quote(found[0])} "$@"\n')
    fake.chmod(0o755)
    with (
        patch.object(cli, "get_cluster", return_value=c),
        patch.object(c.transport, "_ssh_base", return_value=[str(fake)]),
        patch.object(sync_mod, "expand_remote", side_effect=lambda _, p: p),
    ):
        assert cli.cmd_put(put_args(raw, target)) == 0
    c._check_protected.assert_called_once_with(target, force=False, action="put into")
    # Direct local rsync is the layout oracle, including version-specific behavior
    # for a destination that does not exist yet. expanduser preserves source slash.
    reference = tmp_path / "reference"
    if existing:
        reference.mkdir()
        (reference / "keep").write_text("preserve")
    subprocess.run(
        [
            found[0],
            "-az",
            "-s",
            os.path.expanduser(raw),
            str(reference) + ("/" if dest_slash else ""),
        ],
        check=True,
        capture_output=True,
    )

    def layout(root):
        return {
            p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()
        }

    assert layout(dest) == layout(reference)
    if slash:
        assert (dest / "payload").read_bytes() == b"payload\x00\xff"
    elif existing:
        assert (dest / source.name / "payload").read_bytes() == b"payload\x00\xff"

    if existing:
        assert (dest / "keep").read_text() == "preserve"


@pytest.mark.parametrize("direction", ["put", "get"])
@pytest.mark.parametrize("custom", [False, True])
def test_cli_rsync_reuses_active_transport(tmp_path, direction, custom):
    source = tmp_path / "source file"
    source.write_bytes(b"content")
    t = SSHTransport(
        alias="fixture",
        control_path="/tmp/socket with spaces" if custom else None,
        extra_ssh_opts=[
            "-p",
            "2222",
            "-J",
            "bastion",
            "-o",
            "ControlPath=/tmp/ignored",
            "-o",
            "BatchMode=no",
            "-o",
            "ConnectTimeout=0",
        ]
        if custom
        else [],
    )
    c = MagicMock(transport=t)
    c.stat.return_value = {"type": "file", "size": 7}
    args = put_args(str(source), "/remote/target/")
    args.rsync = True
    if direction == "get":
        args.src, args.dest = "/remote/source/", str(source)
    with (
        patch.object(cli, "get_cluster", return_value=c),
        patch.object(sync_mod, "find_rsync", return_value=("rsync", (3, 5))),
        patch.object(sync_mod, "expand_remote", side_effect=lambda _, p: p),
        patch.object(cli.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run,
    ):
        assert getattr(cli, "cmd_" + direction)(args) == 0
    argv = run.call_args.args[0]
    ssh = shlex.split(argv[argv.index("-e") + 1])
    assert ssh == t._ssh_base()
    assert "BatchMode=yes" in ssh and "ConnectTimeout=8" in ssh
    assert "--delete" not in argv
    if custom:
        assert ssh.index("ControlPath=/tmp/socket with spaces") < ssh.index(
            "ControlPath=/tmp/ignored"
        )
        assert ssh.index("BatchMode=yes") < ssh.index("BatchMode=no")
        assert ssh.index("ConnectTimeout=8") < ssh.index("ConnectTimeout=0")
    assert argv[-2:] == (
        [str(source), "fixture:/remote/target/"]
        if direction == "put"
        else ["fixture:/remote/source/", str(source)]
    )


def test_put_keeps_small_file_and_protected_path_behavior(tmp_path):
    source = tmp_path / "file"
    source.write_bytes(b"content")
    c = MagicMock()
    with patch.object(cli, "get_cluster", return_value=c), patch.object(cli, "emit"):
        assert cli.cmd_put(put_args(str(source), "/remote/")) == 0
    c.write.assert_called_once_with("/remote/file", b"content", force=False)
    c._check_protected.side_effect = InvalidArgument("protected")
    args = put_args(str(source), "/protected/")
    args.rsync = True
    with patch.object(cli, "get_cluster", return_value=c), patch.object(cli, "_rsync") as transfer:
        with pytest.raises(InvalidArgument, match="protected"):
            cli.cmd_put(args)
    transfer.assert_not_called()
