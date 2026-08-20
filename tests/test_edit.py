"""WP-B: the `edit` and `diff` ops end to end (stub, Cluster, CLI, MCP)."""

from __future__ import annotations

import asyncio
import json
import os
import stat as statmod
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from remoteslurm import cli, server
from remoteslurm import cluster as cluster_mod
from remoteslurm.cluster import Cluster
from remoteslurm.errors import InvalidArgument, NotFound, TooLarge

FAKESLURM = Path(__file__).parent / "fakeslurm"


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    d = tmp_path / "state"
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(d))
    return d


def _mode(p: Path) -> int:
    return statmod.S_IMODE(p.stat().st_mode)


# --------------------------------------------------------------------------- edit (stub op)


def test_crlf_expect_mismatch_refused(cluster: Cluster, sandbox: Path) -> None:
    """Riskiest case first: CRLF, no trailing newline, `old` twice, expect=1 -> refused,
    file bytes and mode untouched."""
    p = sandbox / "proj" / "crlf.txt"
    original = b"alpha\r\nbeta\r\ngamma\r\nbeta"
    p.write_bytes(original)
    p.chmod(0o640)
    with pytest.raises(InvalidArgument) as ei:
        cluster.edit("~/proj/crlf.txt", "beta", "delta")
    e = ei.value
    assert e.details["count"] == 2 and e.details["expect"] == 1
    assert e.details["lines"] == [2, 4]
    assert p.read_bytes() == original
    assert _mode(p) == 0o640


def test_edit_not_found_reports_closest(cluster: Cluster) -> None:
    with pytest.raises(NotFound) as ei:
        cluster.edit("~/proj/a.txt", "betaa", "delta")
    closest = ei.value.details["closest"]
    assert "beta" in closest
    assert len(closest) <= 3 and all(len(line) <= 200 for line in closest)


def test_edit_single_replacement_preserves_crlf_and_mode(cluster: Cluster, sandbox: Path) -> None:
    p = sandbox / "proj" / "crlf.txt"
    p.write_bytes(b"alpha\r\nbeta\r\ngamma\r\nbeta")
    p.chmod(0o640)
    r = cluster.edit("~/proj/crlf.txt", "gamma", "delta")
    assert r["replacements"] == 1 and r["first_line"] == 3
    assert p.read_bytes() == b"alpha\r\nbeta\r\ndelta\r\nbeta"
    assert _mode(p) == 0o640
    assert "-gamma" in r["preview"] and "+delta" in r["preview"]
    assert len(r["preview"]) <= 4096


def test_edit_all_replaces_every_occurrence(cluster: Cluster, sandbox: Path) -> None:
    p = sandbox / "proj" / "many.txt"
    p.write_text("x=1\nx=2\nx=3\n")
    r = cluster.edit("~/proj/many.txt", "x=", "y=", all=True)
    assert r["replacements"] == 3 and r["first_line"] == 1
    assert p.read_text() == "y=1\ny=2\ny=3\n"


def test_edit_expect_two(cluster: Cluster, sandbox: Path) -> None:
    p = sandbox / "proj" / "two.txt"
    p.write_text("beta\nmid\nbeta\n")
    r = cluster.edit("~/proj/two.txt", "beta", "delta", expect=2)
    assert r["replacements"] == 2
    assert p.read_text() == "delta\nmid\ndelta\n"


def test_edit_rejects_empty_and_identical_old(cluster: Cluster) -> None:
    with pytest.raises(InvalidArgument):
        cluster.edit("~/proj/a.txt", "", "delta")
    with pytest.raises(InvalidArgument):
        cluster.edit("~/proj/a.txt", "beta", "beta")


def test_edit_unicode(cluster: Cluster, sandbox: Path) -> None:
    p = sandbox / "proj" / "uni.txt"
    p.write_text("café\nnaïve — ok\n", encoding="utf-8")
    r = cluster.edit("~/proj/uni.txt", "café", "thé")
    assert r["replacements"] == 1
    assert p.read_text(encoding="utf-8") == "thé\nnaïve — ok\n"


def test_edit_refuses_binary(cluster: Cluster) -> None:
    with pytest.raises(InvalidArgument, match="binary"):
        cluster.edit("~/proj/bin.dat", "x", "y")


def test_edit_refuses_huge_file(cluster: Cluster, sandbox: Path) -> None:
    p = sandbox / "proj" / "big.txt"
    p.write_bytes(b"a" * (8 * 1024 * 1024 + 1))
    with pytest.raises(TooLarge):
        cluster.edit("~/proj/big.txt", "aaa", "bbb")


def test_edit_preview_bounded(cluster: Cluster, sandbox: Path) -> None:
    p = sandbox / "proj" / "wide.txt"
    p.write_text("".join(f"line {i} {'x' * 150}\n" for i in range(200)))
    r = cluster.edit("~/proj/wide.txt", "line ", "row ", all=True)
    assert r["replacements"] == 200
    assert len(r["preview"]) <= 4096


def test_edit_is_audited(cluster: Cluster, sandbox: Path, _isolated_state: Path) -> None:
    cluster.edit("~/proj/a.txt", "beta", "delta")
    log = (_isolated_state / "local" / "audit.log").read_text()
    ev = json.loads(log.strip().splitlines()[-1])
    assert ev["event"] == "edit" and ev["replacements"] == 1


# --------------------------------------------------------------------------- diff (stub op)


def test_diff_identical_content(cluster: Cluster) -> None:
    r = cluster.diff("~/proj/a.txt", "alpha\nbeta\ngamma\n")
    assert r["identical"] is True and r["diff"] == "" and r["truncated"] is False


def test_diff_different_content(cluster: Cluster) -> None:
    r = cluster.diff("~/proj/a.txt", "alpha\nBETA\ngamma\n")
    assert r["identical"] is False
    assert "-beta" in r["diff"] and "+BETA" in r["diff"]
    assert r["truncated"] is False


def test_diff_against_remote_path(cluster: Cluster, sandbox: Path) -> None:
    (sandbox / "proj" / "a2.txt").write_text("alpha\nbeta\ndelta\n")
    r = cluster.diff("~/proj/a.txt", path_b="~/proj/a2.txt")
    assert r["identical"] is False and r["path_b"].endswith("a2.txt")
    assert "-gamma" in r["diff"] and "+delta" in r["diff"]
    r2 = cluster.diff("~/proj/a.txt", path_b="~/proj/a.txt")
    assert r2["identical"] is True


def test_diff_truncation(cluster: Cluster, sandbox: Path) -> None:
    (sandbox / "proj" / "d1.txt").write_text("".join(f"a{i}\n" for i in range(50)))
    r = cluster.diff("~/proj/d1.txt", "".join(f"b{i}\n" for i in range(50)), max_lines=2)
    assert r["truncated"] is True and r["lines"] == 2


def test_diff_refuses_binary_and_missing_b(cluster: Cluster) -> None:
    with pytest.raises(InvalidArgument, match="binary"):
        cluster.diff("~/proj/bin.dat", "x")
    with pytest.raises(InvalidArgument):
        cluster.call("diff", path="~/proj/a.txt")


# --------------------------------------------------------------------------- write mode backport


def test_write_preserves_mode_of_existing_file(cluster: Cluster, sandbox: Path) -> None:
    p = sandbox / "proj" / "a.txt"
    p.chmod(0o750)
    cluster.write("~/proj/a.txt", "new content\n")
    assert _mode(p) == 0o750
    cluster.write("~/proj/a.txt", "again\n", mode=0o644)
    assert _mode(p) == 0o644


# --------------------------------------------------------------------------- CLI


@pytest.fixture
def cli_env(sandbox: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    cfg = tmp_path / "config.toml"
    path = f"{FAKESLURM}{os.pathsep}{os.environ['PATH']}"
    cfg.write_text(
        'default_host = "local"\n[hosts.local]\nssh = "local"\nmfa = false\n'
        f'[hosts.local.env]\nHOME = "{sandbox}"\nPATH = "{path}"\n'
    )
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(cfg))
    monkeypatch.delenv("REMOTESLURM_DEFAULT_HOST", raising=False)
    yield sandbox
    for c in list(cluster_mod._clusters.values()):
        c.close()


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, str, str]:
    rc = cli.main(list(argv))
    out = capsys.readouterr()
    return rc, out.out, out.err


def test_cli_edit(cli_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, out, _ = run(capsys, "edit", "~/proj/a.txt", "--old", "beta", "--new", "delta")
    assert rc == 0 and "1 replacement(s) at line 2" in out and "+delta" in out
    assert (cli_env / "proj" / "a.txt").read_text() == "alpha\ndelta\ngamma\n"
    rc, out, err = run(capsys, "edit", "~/proj/a.txt", "--old", "nope", "--new", "x")
    assert rc == 1 and "error [not_found]" in err
    rc, out, _ = run(capsys, "--json", "edit", "~/proj/a.txt", "--old", "alpha", "--new", "omega")
    d = json.loads(out)
    assert rc == 0 and d["replacements"] == 1 and d["first_line"] == 1


def test_cli_edit_from_files(
    cli_env: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    oldf = tmp_path / "old.txt"
    newf = tmp_path / "new.txt"
    oldf.write_text("beta\ngamma\n")
    newf.write_text("B\nG\n")
    rc, out, _ = run(
        capsys, "edit", "~/proj/a.txt", "--old-file", str(oldf), "--new-file", str(newf)
    )
    assert rc == 0
    assert (cli_env / "proj" / "a.txt").read_text() == "alpha\nB\nG\n"
    rc, _, err = run(capsys, "edit", "~/proj/a.txt", "--new", "x")
    assert rc == 1 and "--old" in err


def test_cli_diff(cli_env: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    same = tmp_path / "same.txt"
    same.write_text("alpha\nbeta\ngamma\n")
    rc, out, _ = run(capsys, "diff", "~/proj/a.txt", str(same))
    assert rc == 0 and out == ""
    other = tmp_path / "other.txt"
    other.write_text("alpha\ndelta\ngamma\n")
    rc, out, _ = run(capsys, "diff", "~/proj/a.txt", str(other))
    assert rc == 1 and "-beta" in out and "+delta" in out
    rc, out, _ = run(capsys, "--json", "diff", "~/proj/a.txt", str(other))
    d = json.loads(out)
    assert rc == 1 and d["identical"] is False
    rc, _, err = run(capsys, "diff", "~/proj/a.txt")
    assert rc == 1 and "LOCALFILE" in err


# --------------------------------------------------------------------------- MCP


@pytest.fixture
def mcp_cluster(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> Cluster:
    monkeypatch.setattr(server, "_get_cluster", lambda host: cluster)
    return cluster


def call(tool: str, **args: Any) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with create_connected_server_and_client_session(server.mcp) as client:
            res = await client.call_tool(tool, args)
            assert not res.isError, res.content
            if res.structuredContent is not None:
                return dict(res.structuredContent)
            return dict(json.loads(res.content[0].text))  # type: ignore[union-attr]

    return asyncio.run(go())


def test_mcp_edit(mcp_cluster: Cluster, sandbox: Path) -> None:
    r = call("edit", path="~/proj/a.txt", old="beta", new="delta")
    assert r["replacements"] == 1 and r["first_line"] == 2
    assert (sandbox / "proj" / "a.txt").read_text() == "alpha\ndelta\ngamma\n"
    r = call("edit", path="~/proj/a.txt", old="nope", new="x")
    assert r["error"] == "not_found" and "closest" in r["details"]


def test_mcp_diff(mcp_cluster: Cluster) -> None:
    r = call("diff", path="~/proj/a.txt", content="alpha\nbeta\ngamma\n")
    assert r["identical"] is True
    r = call("diff", path="~/proj/a.txt", content="alpha\nBETA\ngamma\n")
    assert r["identical"] is False and "+BETA" in r["diff"]
