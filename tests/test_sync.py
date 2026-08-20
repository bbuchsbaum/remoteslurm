"""WP-A project sync: rsync command building, itemize parsing, and e2e via a fake ssh.

The e2e tests run real local rsync (>= 3.1) through a fake ``ssh`` script that drops the
host argument and execs the rest locally, so ``ALIAS:PATH`` targets land on this machine.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from remoteslurm import sync as sync_mod
from remoteslurm.cluster import Cluster
from remoteslurm.config import HostConfig, ProjectConfig
from remoteslurm.errors import ConfigError, InvalidArgument, TooLarge

RSYNC = sync_mod.find_rsync()
HAVE_RSYNC31 = RSYNC is not None and RSYNC[1] >= (3, 1)
rsync31 = pytest.mark.skipif(
    not HAVE_RSYNC31,
    reason=f"local rsync >= 3.1 required for e2e sync tests (found {RSYNC})",
)


# --------------------------------------------------------------------------- helpers/fixtures
@pytest.fixture
def fake_ssh(tmp_path: Path) -> Path:
    """A stand-in for ssh: drop the host argument, exec the command locally.

    rsync invokes ``ssh HOST rsync --server ...``; we additionally pin the server-side
    rsync to the >=3.1 binary the client uses (macOS ``/usr/bin/rsync`` is 2.6.9 and
    cannot serve a protocol-30 client).
    """
    assert RSYNC is not None
    script = tmp_path / "fake-ssh"
    script.write_text(f'#!/bin/sh\nshift\nshift\nexec {RSYNC[0]} "$@"\n')
    script.chmod(0o755)
    return script


@pytest.fixture
def synced(sandbox: Path, cluster: Cluster, monkeypatch: pytest.MonkeyPatch):
    """A (cluster, project, local, remote) tuple wired for local-rsync sync."""
    monkeypatch.setattr(cluster.transport, "alias", "fakehost", raising=False)
    local = sandbox / "mylocal"
    (local / "sub").mkdir(parents=True)
    (local / "a.txt").write_text("alpha\n")
    (local / "sub" / "b.txt").write_text("beta\n")
    remote = sandbox / "myremote"
    project = ProjectConfig(name="demo", local=str(local), remote=str(remote))
    cluster.host.projects = {"demo": project}
    return cluster, project, local, remote


def do_sync(cluster: Cluster, project: ProjectConfig, fake_ssh: Path, **kw: Any) -> dict[str, Any]:
    return sync_mod.sync(cluster, project, ssh_cmd=[str(fake_ssh)], **kw)


# --------------------------------------------------------------------------- riskiest first
@rsync31
def test_push_modify_push_reports_one_updated(synced, fake_ssh: Path) -> None:
    cluster, project, local, remote = synced
    r1 = do_sync(cluster, project, fake_ssh)
    assert r1["rc"] == 0 and r1["direction"] == "push" and not r1["dry_run"]
    assert (remote / "a.txt").read_text() == "alpha\n"
    assert (remote / "sub" / "b.txt").read_text() == "beta\n"
    assert r1["counts"] is not None and r1["counts"]["created"] >= 2
    assert r1["files"] == 2

    (local / "a.txt").write_text("alpha\nchanged\n")
    r2 = do_sync(cluster, project, fake_ssh)
    assert (remote / "a.txt").read_text() == "alpha\nchanged\n"
    c = r2["counts"]
    assert c is not None
    assert c["updated"] == 1 and c["created"] == 0 and c["deleted"] == 0
    assert r2["files"] == 1


# --------------------------------------------------------------------------- parser
RSYNC3_PUSH_FIRST = """\
<f+++++++++ a.txt
cd+++++++++ sub/
<f+++++++++ sub/b.txt

Number of files: 4 (reg: 2, dir: 2)
Number of created files: 3 (reg: 2, dir: 1)
Number of deleted files: 0
Number of regular files transferred: 2
Total file size: 6 bytes
Total transferred file size: 6 bytes
Literal data: 6 bytes
Matched data: 0 bytes
File list size: 0
File list generation time: 0.008 seconds
File list transfer time: 0.000 seconds
Total bytes sent: 227
Total bytes received: 62

sent 227 bytes  received 62 bytes  192.67 bytes/sec
total size is 6  speedup is 0.02
"""

RSYNC3_PULL_WITH_DELETE = """\
*deleting   old.txt
>f.st...... a.txt
>f+++++++++ new.txt

Number of files: 4 (reg: 3, dir: 1)
Number of created files: 1 (reg: 1)
Number of deleted files: 1
Number of regular files transferred: 2
Total file size: 1,234 bytes
Total transferred file size: 1,100 bytes

sent 100 bytes  received 1,200 bytes  866.67 bytes/sec
total size is 1,234  speedup is 0.95
"""

# macOS openrsync / rsync 2.6.9: 9-character itemize fields, "Number of files transferred".
RSYNC269_OUTPUT = """\
>f+++++++ a.txt
cd+++++++ sub/

Number of files: 4
Number of files transferred: 2
Total file size: 6 bytes
Total transferred file size: 6 bytes

sent 227 bytes  received 62 bytes  192.67 bytes/sec
total size is 6  speedup is 0.02
"""


def test_parse_itemize_rsync3_push() -> None:
    c = sync_mod.parse_itemize(RSYNC3_PUSH_FIRST)
    assert c == {"created": 3, "updated": 0, "deleted": 0, "files": 2, "bytes": 6}


def test_parse_itemize_rsync3_pull_and_delete() -> None:
    c = sync_mod.parse_itemize(RSYNC3_PULL_WITH_DELETE)
    assert c == {"created": 1, "updated": 1, "deleted": 1, "files": 2, "bytes": 1100}


def test_parse_itemize_degrades_to_none() -> None:
    assert sync_mod.parse_itemize(RSYNC269_OUTPUT) is None  # 9-char 2.6.9 itemize
    assert sync_mod.parse_itemize("total garbage\nnothing here\n") is None
    assert sync_mod.parse_itemize("") is None
    # itemize lines alone without a --stats block are not enough
    assert sync_mod.parse_itemize("<f+++++++++ a.txt\n") is None


def test_parse_itemize_ignores_attr_only_lines() -> None:
    text = ".d..t...... sub/\n" + RSYNC3_PUSH_FIRST
    c = sync_mod.parse_itemize(text)
    assert c is not None and c["created"] == 3 and c["updated"] == 0


# --------------------------------------------------------------------------- command building
def test_build_rsync_cmd_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(sync_mod.ENV_SSH, raising=False)
    cmd = sync_mod.build_rsync_cmd("rsync", "/l", "/r", "host", excludes=["*.nii.gz", "results/"])
    assert cmd[0] == "rsync" and "-s" in cmd and "--itemize-changes" in cmd and "--stats" in cmd
    assert "-n" not in cmd and "--delete" not in cmd
    filters = [a for a in cmd if a.startswith("--filter=")]
    builtins = [f"--filter=- {p}" for p in sync_mod.BUILTIN_EXCLUDES]
    assert filters[: len(builtins)] == builtins  # built-ins come first
    assert filters[len(builtins) :] == ["--filter=- *.nii.gz", "--filter=- results/"]
    assert cmd[cmd.index("-e") + 1] == sync_mod.DEFAULT_SSH
    assert cmd[-2:] == ["/l/", "host:/r"]
    pulled = sync_mod.build_rsync_cmd("rsync", "/l", "/r", "host", pull=True, dry_run=True)
    assert pulled[-2:] == ["host:/r/", "/l"] and "-n" in pulled


def test_build_rsync_cmd_ssh_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(sync_mod.ENV_SSH, "/tmp/env-ssh")
    cmd = sync_mod.build_rsync_cmd("rsync", "/l", "/r", "h")
    assert cmd[cmd.index("-e") + 1] == "/tmp/env-ssh"
    cmd = sync_mod.build_rsync_cmd("rsync", "/l", "/r", "h", ssh_cmd=["/x/fake ssh", "-v"])
    assert cmd[cmd.index("-e") + 1] == "'/x/fake ssh' -v"  # param beats env


def test_require_rsync_too_old(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_mod, "find_rsync", lambda: ("/usr/bin/rsync", (2, 6)))
    with pytest.raises(InvalidArgument) as ei:
        sync_mod._require_rsync()
    assert "brew install rsync" in (ei.value.action or "")
    monkeypatch.setattr(sync_mod, "find_rsync", lambda: None)
    with pytest.raises(InvalidArgument):
        sync_mod._require_rsync()


def test_rsync_version_reports_tuple_or_none() -> None:
    v = sync_mod.rsync_version()
    assert v is None or (isinstance(v, tuple) and len(v) == 2)


# --------------------------------------------------------------------------- resolve_project
def _host_with_projects(tmp_path: Path) -> HostConfig:
    (tmp_path / "a" / "b").mkdir(parents=True)
    return HostConfig(
        name="h",
        ssh="h",
        projects={
            "outer": ProjectConfig(name="outer", local=str(tmp_path / "a"), remote="/r/outer"),
            "inner": ProjectConfig(
                name="inner", local=str(tmp_path / "a" / "b"), remote="/r/inner"
            ),
        },
    )


def test_resolve_project_explicit_and_unknown(tmp_path: Path) -> None:
    host = _host_with_projects(tmp_path)
    assert sync_mod.resolve_project(host, "outer", tmp_path).name == "outer"
    with pytest.raises(ConfigError) as ei:
        sync_mod.resolve_project(host, "nope", tmp_path)
    assert "inner" in (ei.value.action or "") and "outer" in (ei.value.action or "")


def test_resolve_project_by_cwd_longest_match(tmp_path: Path) -> None:
    host = _host_with_projects(tmp_path)
    deep = tmp_path / "a" / "b" / "c"
    deep.mkdir()
    assert sync_mod.resolve_project(host, None, deep).name == "inner"
    assert sync_mod.resolve_project(host, None, tmp_path / "a").name == "outer"
    with pytest.raises(ConfigError) as ei:
        sync_mod.resolve_project(host, None, tmp_path)
    assert "pass a project name" in (ei.value.action or "")


def test_resolve_project_none_configured(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as ei:
        sync_mod.resolve_project(HostConfig(name="h", ssh="h"), None, tmp_path)
    assert "projects" in (ei.value.action or "")


def test_project_config_parsing(tmp_path: Path) -> None:
    cfg = tmp_path / "c.toml"
    cfg.write_text(
        '[hosts.hpc]\nssh = "hpc"\nmax_sync_files = 10\n'
        '[hosts.hpc.projects.mvpa]\nlocal = "~/code/mvpa"\nremote = "$PROJECT/mvpa"\n'
        'exclude = [".git", "*.nii.gz"]\ndelete = true\n'
    )
    from remoteslurm.config import Config

    host = Config.load(cfg).host("hpc")
    assert host.max_sync_files == 10 and host.max_sync_bytes == 2 * 1024**3
    p = host.projects["mvpa"]
    assert p.remote == "$PROJECT/mvpa" and p.delete is True and p.exclude == [".git", "*.nii.gz"]
    assert "projects" not in host.extra
    cfg.write_text('[hosts.hpc]\n[hosts.hpc.projects.x]\nlocal = "~/x"\nremote = "/x"\nbogus = 1\n')
    with pytest.raises(ConfigError):
        Config.load(cfg)


# --------------------------------------------------------------------------- e2e behaviours
@rsync31
def test_excludes_honoured(synced, fake_ssh: Path) -> None:
    cluster, project, local, remote = synced
    (local / ".git").mkdir()
    (local / ".git" / "HEAD").write_text("ref\n")
    (local / "__pycache__").mkdir()
    (local / "__pycache__" / "m.pyc").write_text("x")
    (local / "big.nii.gz").write_text("brain\n")
    (local / "results").mkdir()
    (local / "results" / "out.csv").write_text("1\n")
    project.exclude = ["*.nii.gz", "results/"]
    do_sync(cluster, project, fake_ssh)
    assert (remote / "a.txt").exists()
    assert not (remote / ".git").exists()
    assert not (remote / "__pycache__").exists()
    assert not (remote / "big.nii.gz").exists()
    assert not (remote / "results").exists()


@rsync31
def test_pull(synced, fake_ssh: Path) -> None:
    cluster, project, local, remote = synced
    remote.mkdir()
    (remote / "result.txt").write_text("output\n")
    r = do_sync(cluster, project, fake_ssh, pull=True)
    assert r["direction"] == "pull" and r["marker"] is None
    assert (local / "result.txt").read_text() == "output\n"
    assert (local / "a.txt").exists()  # pull without --delete leaves local extras alone


@rsync31
def test_dry_run_has_no_side_effects(synced, fake_ssh: Path) -> None:
    cluster, project, local, remote = synced
    r = do_sync(cluster, project, fake_ssh, dry_run=True)
    assert r["dry_run"] is True and r["marker"] is None
    assert r["counts"] is not None and r["counts"]["created"] >= 2
    assert not remote.exists()  # not even mkdir


@rsync31
def test_delete_double_opt_in(synced, fake_ssh: Path) -> None:
    cluster, project, local, remote = synced
    do_sync(cluster, project, fake_ssh)
    (remote / "stale.txt").write_text("old\n")
    # project does not allow delete -> refused
    with pytest.raises(InvalidArgument) as ei:
        do_sync(cluster, project, fake_ssh, delete=True)
    assert "delete" in ei.value.message
    # delete allowed on the project but not requested -> file survives
    project.delete = True
    do_sync(cluster, project, fake_ssh)
    assert (remote / "stale.txt").exists()
    # both -> deleted
    r = do_sync(cluster, project, fake_ssh, delete=True)
    assert not (remote / "stale.txt").exists()
    assert r["counts"] is not None and r["counts"]["deleted"] == 1


@rsync31
def test_guard_trips_and_force_overrides(synced, fake_ssh: Path) -> None:
    cluster, project, local, remote = synced
    cluster.host.max_sync_files = 1
    with pytest.raises(TooLarge) as ei:
        do_sync(cluster, project, fake_ssh)
    assert "force" in (ei.value.action or "")
    assert not (remote / "a.txt").exists()
    # dry runs are never guarded; force skips the guard
    do_sync(cluster, project, fake_ssh, dry_run=True)
    do_sync(cluster, project, fake_ssh, force=True)
    assert (remote / "a.txt").exists()
    # byte limit too
    cluster.host.max_sync_files = 50_000
    cluster.host.max_sync_bytes = 1
    (local / "a.txt").write_text("bigger than one byte\n")
    with pytest.raises(TooLarge):
        do_sync(cluster, project, fake_ssh)


@rsync31
def test_marker_written_with_git_rev(synced, fake_ssh: Path) -> None:
    cluster, project, local, remote = synced
    if not shutil.which("git"):
        pytest.skip("git not available")
    env = {**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null"}
    subprocess.run(["git", "init", "-q"], cwd=local, check=True, env=env)
    subprocess.run(["git", "add", "-A"], cwd=local, check=True, env=env)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "x"],
        cwd=local,
        check=True,
        env=env,
    )
    rev = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=local, capture_output=True, text=True, env=env
    ).stdout.strip()
    r = do_sync(cluster, project, fake_ssh)
    marker = json.loads((remote / sync_mod.MARKER).read_text())
    assert marker == r["marker"]
    assert marker["local_git_rev"] == rev and marker["local_dirty"] is False
    assert marker["project"] == "demo" and marker["pushed_at"]
    assert marker["files"] == r["files"] and marker["bytes"] == r["bytes"]
    (local / "a.txt").write_text("dirty now\n")
    r2 = do_sync(cluster, project, fake_ssh)
    assert r2["marker"] is not None and r2["marker"]["local_dirty"] is True
    # the marker itself is excluded from the next sync (never pushed back or counted)
    r3 = do_sync(cluster, project, fake_ssh, pull=True)
    assert not (local / sync_mod.MARKER).exists() and r3["rc"] == 0


@rsync31
def test_marker_without_git(synced, fake_ssh: Path) -> None:
    cluster, project, local, remote = synced
    r = do_sync(cluster, project, fake_ssh)
    m = r["marker"]
    assert m is not None and m["local_git_rev"] is None and m["local_dirty"] is None


def test_sync_requires_alias(sandbox: Path, cluster: Cluster) -> None:
    project = ProjectConfig(name="p", local=str(sandbox / "proj"), remote=str(sandbox / "r"))
    if not HAVE_RSYNC31:
        pytest.skip("needs rsync >= 3.1 (alias check comes after the version check)")
    with pytest.raises(InvalidArgument) as ei:
        sync_mod.sync(cluster, project)  # LocalTransport has no ssh alias
    assert "alias" in ei.value.message


def test_expand_remote(cluster: Cluster, sandbox: Path) -> None:
    assert sync_mod.expand_remote(cluster, "$HOME/x") == f"{sandbox}/x"
    assert sync_mod.expand_remote(cluster, str(sandbox)) == str(sandbox)
    assert sync_mod.expand_remote(cluster, "$UNSET_VAR_XYZ") == "$UNSET_VAR_XYZ"  # falls back


# --------------------------------------------------------------------------- CLI
@pytest.fixture
def cli_sync_env(
    sandbox: Path, tmp_path: Path, fake_ssh: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    from remoteslurm import cluster as cluster_mod
    from remoteslurm import transport as transport_mod

    fakeslurm = Path(__file__).parent / "fakeslurm"
    path = f"{fakeslurm}{os.pathsep}{os.environ['PATH']}"
    local = sandbox / "clidemo"
    local.mkdir()
    (local / "f.txt").write_text("data\n")
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        'default_host = "local"\n[hosts.local]\nssh = "local"\nmfa = false\n'
        f'[hosts.local.env]\nHOME = "{sandbox}"\nPATH = "{path}"\n'
        f'[hosts.local.projects.demo]\nlocal = "{local}"\n'
        f'remote = "{sandbox}/cliremote"\n'
    )
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(cfg))
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv(sync_mod.ENV_SSH, str(fake_ssh))
    monkeypatch.delenv("REMOTESLURM_DEFAULT_HOST", raising=False)
    monkeypatch.setattr(transport_mod.LocalTransport, "alias", "fakehost", raising=False)
    cluster_mod._clusters.clear()
    yield sandbox
    for c in list(cluster_mod._clusters.values()):
        c.close()
    cluster_mod._clusters.clear()


def run_cli(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, str, str]:
    from remoteslurm import cli

    rc = cli.main(list(argv))
    out = capsys.readouterr()
    return rc, out.out, out.err


@rsync31
def test_cli_sync_json(cli_sync_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, _ = run_cli(capsys, "--json", "--no-daemon", "sync", "demo")
    assert rc == 0, out
    d = json.loads(out)
    assert d["project"] == "demo" and d["direction"] == "push" and d["rc"] == 0
    assert (cli_sync_env / "cliremote" / "f.txt").read_text() == "data\n"
    assert d["counts"] is not None and d["counts"]["created"] >= 1
    rc, out, _ = run_cli(capsys, "--no-daemon", "sync", "demo", "--dry-run")
    assert rc == 0 and "[dry-run]" in out


def test_cli_sync_unknown_project(cli_sync_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, err = run_cli(capsys, "--no-daemon", "sync", "nope")
    assert rc == 1 and "config_error" in err and "demo" in err


def test_cli_projects(cli_sync_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, _ = run_cli(capsys, "--no-daemon", "projects")
    assert rc == 0 and "demo" in out and "cliremote" in out
    rc, out, _ = run_cli(capsys, "--json", "--no-daemon", "projects")
    d = json.loads(out)
    assert d["count"] == 1 and d["projects"][0]["name"] == "demo"
    assert "marker" not in d["projects"][0]  # not read without --verbose


@rsync31
def test_cli_projects_verbose_marker(
    cli_sync_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc, _, _ = run_cli(capsys, "--json", "--no-daemon", "sync", "demo")
    assert rc == 0
    rc, out, _ = run_cli(capsys, "--json", "--no-daemon", "projects", "--verbose")
    d = json.loads(out)
    assert d["projects"][0]["marker"]["project"] == "demo"


# --------------------------------------------------------------------------- MCP server
@pytest.fixture
def mcp_sync(synced, fake_ssh: Path, monkeypatch: pytest.MonkeyPatch):
    from remoteslurm import server

    cluster, project, local, remote = synced
    monkeypatch.setattr(server, "_get_cluster", lambda host: cluster)
    monkeypatch.setenv(sync_mod.ENV_SSH, str(fake_ssh))
    return cluster, project, local, remote


_ALL_MCP = None


def _call(tool: str, **args: Any) -> dict[str, Any]:
    import asyncio

    from mcp.shared.memory import create_connected_server_and_client_session

    from remoteslurm import server

    global _ALL_MCP
    if _ALL_MCP is None:
        _ALL_MCP = server.make_mcp("all")  # `projects` is only in the `all` tool set

    async def go() -> dict[str, Any]:
        async with create_connected_server_and_client_session(_ALL_MCP) as client:
            res = await client.call_tool(tool, args)
            assert not res.isError, res.content
            if res.structuredContent is not None:
                return dict(res.structuredContent)
            return dict(json.loads(res.content[0].text))  # type: ignore[union-attr]

    return asyncio.run(go())


@rsync31
def test_mcp_sync_push_and_pull(mcp_sync) -> None:
    cluster, project, local, remote = mcp_sync
    r = _call("sync", project="demo")
    assert r["direction"] == "push" and r["rc"] == 0
    assert (remote / "a.txt").exists()
    r = _call("sync", project="demo", direction="pull", dry_run=True)
    assert r["direction"] == "pull" and r["dry_run"] is True
    r = _call("sync", project="demo", direction="sideways")
    assert r["error"] == "invalid_arg"


def test_mcp_sync_unknown_project(mcp_sync) -> None:
    r = _call("sync", project="nope")
    assert r["error"] == "config_error" and "demo" in r["action"]


def test_mcp_projects(mcp_sync) -> None:
    r = _call("projects")
    assert r["count"] == 1 and r["projects"][0]["name"] == "demo"
    assert r["projects"][0]["delete"] is False
