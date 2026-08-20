from __future__ import annotations

import base64
from pathlib import Path

import pytest

from remoteslurm.cluster import Cluster
from remoteslurm.errors import InvalidArgument, NotFound, RemoteTimeout


def test_ping_and_info(cluster: Cluster, sandbox: Path) -> None:
    assert cluster.ping()["protocol"] == 1
    info = cluster.info()
    assert info["home"] == str(sandbox)
    assert "python" in info


def test_ls_basic_and_pagination(cluster: Cluster, sandbox: Path) -> None:
    r = cluster.ls("~/proj")
    names = [e["name"] for e in r["entries"]]
    assert names == [".hidden", "a.txt", "b.log", "bin.dat", "sub"]
    assert r["total"] == 5 and not r["truncated"]
    sub = next(e for e in r["entries"] if e["name"] == "sub")
    assert sub["type"] == "dir"
    r2 = cluster.ls("~/proj", limit=2)
    assert [e["name"] for e in r2["entries"]] == [".hidden", "a.txt"]
    assert r2["truncated"] and r2["next_token"] == "2"
    r3 = cluster.ls("~/proj", limit=2, token=r2["next_token"])
    assert [e["name"] for e in r3["entries"]] == ["b.log", "bin.dat"]
    r4 = cluster.ls("~/proj", hidden=False)
    assert ".hidden" not in [e["name"] for e in r4["entries"]]


def test_ls_file_and_missing(cluster: Cluster) -> None:
    r = cluster.ls("~/proj/a.txt")
    assert r["entries"][0]["size"] == 17
    with pytest.raises(NotFound):
        cluster.ls("~/nope")


def test_read_text_slices(cluster: Cluster) -> None:
    r = cluster.read("~/proj/a.txt")
    assert r["content"] == "alpha\nbeta\ngamma\n" and r["eof"] and not r["binary"]
    r = cluster.read("~/proj/b.log", max_bytes=100)
    assert r["truncated"] and not r["eof"] and r["length"] == 100
    r = cluster.read("~/proj/b.log", head=3)
    assert r["content"] == "line 0\nline 1\nline 2\n"
    r = cluster.read("~/proj/b.log", tail=2)
    assert r["content"] == "line 998\nline 999\n" and r["truncated"]
    r = cluster.read("~/proj/b.log", offset=-9)
    assert r["content"] == "line 999\n"
    r = cluster.read("~/proj/b.log", offset=7, max_bytes=7)
    assert r["content"] == "line 1\n"


def test_read_tail_small_file(cluster: Cluster) -> None:
    r = cluster.read("~/proj/a.txt", tail=50)
    assert r["content"] == "alpha\nbeta\ngamma\n" and not r["truncated"]


def test_read_binary(cluster: Cluster) -> None:
    r = cluster.read("~/proj/bin.dat", max_bytes=6)
    assert r["binary"] and base64.b64decode(r["content_b64"]) == b"\x00\x01\x02\x00\x01\x02"
    assert cluster.read_bytes("~/proj/bin.dat")[:3] == b"\x00\x01\x02"
    with pytest.raises(InvalidArgument):
        cluster.read_text("~/proj/bin.dat")


def test_read_dir_is_error(cluster: Cluster) -> None:
    with pytest.raises(InvalidArgument):
        cluster.read("~/proj")


def test_write_roundtrip(cluster: Cluster, sandbox: Path) -> None:
    cluster.write("~/new/dir/x.txt", "héllo\n")
    assert (sandbox / "new/dir/x.txt").read_text() == "héllo\n"
    cluster.write("~/new/dir/x.txt", "more\n", append=True)
    assert cluster.read_text("~/new/dir/x.txt") == "héllo\nmore\n"
    cluster.write("~/new/blob", b"\x00\xff")
    assert (sandbox / "new/blob").read_bytes() == b"\x00\xff"
    cluster.write("~/new/script.sh", "#!/bin/sh\n", mode=0o700)
    assert (sandbox / "new/script.sh").stat().st_mode & 0o777 == 0o700


def test_mkdir_rm(cluster: Cluster, sandbox: Path) -> None:
    cluster.mkdir("~/d1/d2")
    assert (sandbox / "d1/d2").is_dir()
    with pytest.raises(InvalidArgument):
        cluster.rm("~/d1")
    cluster.rm("~/d1", recursive=True)
    assert not (sandbox / "d1").exists()
    with pytest.raises(InvalidArgument):
        cluster.rm("~")


def test_glob(cluster: Cluster) -> None:
    r = cluster.glob("~/proj", "*.py")
    assert [m["path"].endswith("sub/c.py") for m in r["matches"]] == [True]
    r = cluster.glob("~/proj", "*", type="dir")
    assert [Path(m["path"]).name for m in r["matches"]] == ["sub"]
    r = cluster.glob("~/proj", "*", limit=2)
    assert r["truncated"] and len(r["matches"]) == 2
    assert not any(".hidden" in m["path"] for m in cluster.glob("~/proj", "*")["matches"])
    assert any(".hidden" in m["path"] for m in cluster.glob("~/proj", "*", hidden=True)["matches"])


def test_grep(cluster: Cluster) -> None:
    r = cluster.grep("beta", "~/proj")
    files = sorted(Path(m["file"]).name for m in r["matches"])
    assert files == ["a.txt", "c.py"]
    assert r["files_skipped"] >= 1  # binary file skipped
    r = cluster.grep("BETA", "~/proj", glob="*.txt", ignore_case=True, context=1)
    assert len(r["matches"]) == 1 and r["matches"][0]["line"] == 2
    assert r["matches"][0]["before"] == ["alpha"]
    r = cluster.grep(r"line \d+", "~/proj/b.log", max_matches=5)
    assert r["truncated"] and len(r["matches"]) == 5
    with pytest.raises(InvalidArgument):
        cluster.grep("(", "~/proj")


def test_run_argv_and_shell(cluster: Cluster, sandbox: Path) -> None:
    r = cluster.run(["echo", "hi"])
    assert r["rc"] == 0 and r["stdout"] == "hi\n"
    r = cluster.run("echo $HOME; exit 3")
    assert r["rc"] == 3 and r["stdout"].strip() == str(sandbox)
    r = cluster.run("cat", stdin="piped")
    assert r["stdout"] == "piped"
    r = cluster.run(["pwd"], cwd="~/proj")
    assert r["stdout"].strip().endswith("proj")
    r = cluster.run("head -c 100000 /dev/zero | tr '\\0' x", max_output=1000)
    assert r["stdout_truncated"] and len(r["stdout"]) == 1000
    with pytest.raises(NotFound):
        cluster.run(["definitely-not-a-command-xyz"])


def test_run_timeout(cluster: Cluster) -> None:
    with pytest.raises(RemoteTimeout):
        cluster.run(["sleep", "5"], timeout=1)


def test_run_disabled(cluster: Cluster) -> None:
    from remoteslurm.errors import PermissionDenied

    cluster.host.allow_run = False
    try:
        with pytest.raises(PermissionDenied):
            cluster.run(["echo"])
    finally:
        cluster.host.allow_run = True


def test_path_with_nul_rejected(cluster: Cluster) -> None:
    with pytest.raises(InvalidArgument):
        cluster.stat("~/a\x00b")


def test_concurrent_pipelined_requests(cluster: Cluster) -> None:
    futs = [
        cluster.session.submit("run", {"argv": ["sh", "-c", f"sleep 0.2; echo {i}"]})
        for i in range(12)
    ]
    outs = [f.result(timeout=30)["stdout"].strip() for f in futs]
    assert outs == [str(i) for i in range(12)]
