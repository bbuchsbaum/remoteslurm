"""MCP server tests: drive the FastMCP app through the in-memory client against a local Cluster."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from remoteslurm import server
from remoteslurm.cluster import Cluster
from remoteslurm.errors import NotFound
from remoteslurm.slurm import JobStatus

CORE_EXPECTED = {
    "info",
    "ls",
    "read",
    "edit",
    "grep",
    "write",
    "run",
    "proc_status",
    "proc_tail",
    "proc_kill",
    "submit",
    "jobs",
    "diagnose",
    "sync",
    "cancel",
    "connection",
    "wait",
}
ALL_EXPECTED = CORE_EXPECTED | {
    "glob",
    "diff",
    "job_output",
    "sinfo",
    "projects",
    "sweep",
    "queue_info",
    "quota",
    "events",
}

# Roundtrip tests exercise tools that are only in the `all` set (glob/job_output/sinfo), so
# drive an all-tools server. The core/all split itself is checked by the list_tools tests.
_ALL_MCP = server.make_mcp("all")


@pytest.fixture
def mcp_cluster(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> Cluster:
    monkeypatch.setattr(server, "_get_cluster", lambda host: cluster)
    monkeypatch.setattr(
        "remoteslurm.config.Config.load",
        classmethod(lambda cls, path=None: _FakeConfig(cluster)),
    )
    return cluster


class _FakeConfig:
    def __init__(self, cluster: Cluster) -> None:
        self._c = cluster

    def host(self, name: str | None) -> Any:
        return self._c.host


def call(tool: str, **args: Any) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with create_connected_server_and_client_session(_ALL_MCP) as client:
            res = await client.call_tool(tool, args)
            assert not res.isError, res.content
            if res.structuredContent is not None:
                return dict(res.structuredContent)
            return dict(json.loads(res.content[0].text))  # type: ignore[union-attr]

    return asyncio.run(go())


def _tool_names(m: Any) -> set[str]:
    async def go() -> set[str]:
        async with create_connected_server_and_client_session(m) as client:
            tools = await client.list_tools()
            assert all(t.description for t in tools.tools)
            return {t.name for t in tools.tools}

    return asyncio.run(go())


def test_list_tools_core_default() -> None:
    # The default server (no REMOTESLURM_MCP_TOOLS) exposes only the core set.
    assert _tool_names(server.mcp) == CORE_EXPECTED


def test_list_tools_all_set() -> None:
    assert _tool_names(server.make_mcp("all")) == ALL_EXPECTED


def test_guide_resource_present() -> None:
    async def go() -> str:
        async with create_connected_server_and_client_session(server.mcp) as client:
            res = await client.read_resource("remoteslurm://guide")
            return res.contents[0].text  # type: ignore[union-attr]

    text = asyncio.run(go())
    assert "remoteslurm" in text and "diagnose" in text


def test_ls_roundtrip(mcp_cluster: Cluster) -> None:
    r = call("ls", path="~/proj", limit=2)
    assert {e["name"] for e in r["entries"]} <= {".hidden", "a.txt", "b.log", "bin.dat", "sub"}
    assert r["truncated"] is True and r["next_token"]
    assert "summary" in r and "next_token" in r["summary"]
    r2 = call("ls", path="~/proj", limit=2, token=r["next_token"])
    assert r2["offset"] == 2


def test_read_text_and_tail(mcp_cluster: Cluster) -> None:
    r = call("read", path="~/proj/a.txt")
    assert r["content"] == "alpha\nbeta\ngamma\n"
    r = call("read", path="~/proj/b.log", tail=2)
    assert r["content"].splitlines() == ["line 998", "line 999"]


def test_read_binary(mcp_cluster: Cluster) -> None:
    r = call("read", path="~/proj/bin.dat")
    assert r["binary"] is True
    assert r["content_b64"]
    assert "binary" in r["note"]


def test_read_clamps_max_bytes(mcp_cluster: Cluster) -> None:
    r = call("read", path="~/proj/b.log", max_bytes=0)
    assert len(r["content"]) == 1  # clamped to 1 byte, not an error


def test_grep_and_glob(mcp_cluster: Cluster) -> None:
    r = call("grep", pattern="beta", path="~/proj")
    files = {m["file"].rsplit("/", 1)[-1] for m in r["matches"]}
    assert files == {"a.txt", "c.py"}
    r = call("glob", path="~/proj", pattern="**/*.py")
    assert [e["path"].rsplit("/", 1)[-1] for e in r["matches"]] == ["c.py"]


def test_write_then_read(mcp_cluster: Cluster) -> None:
    r = call("write", path="~/proj/new/out.txt", content="hello\n")
    assert "error" not in r
    r = call("write", path="~/proj/new/out.txt", content="world\n", append=True)
    assert call("read", path="~/proj/new/out.txt")["content"] == "hello\nworld\n"


def test_not_found_is_dict_not_exception(mcp_cluster: Cluster) -> None:
    r = call("read", path="~/proj/nope.txt")
    assert r["error"] == "not_found"
    assert "message" in r


def test_run_permission_denied(mcp_cluster: Cluster) -> None:
    mcp_cluster.host.allow_run = False
    try:
        r = call("run", cmd="echo hi")
    finally:
        mcp_cluster.host.allow_run = True
    assert r["error"] == "permission"
    assert "allow_run" in r["action"]


def test_run_ok(mcp_cluster: Cluster) -> None:
    r = call("run", cmd="echo hi; echo err >&2; exit 3", timeout=0)
    assert r["rc"] == 3
    assert r["stdout"].strip() == "hi"
    assert r["stderr"].strip() == "err"


def test_run_safe_accepts_argv_over_mcp(mcp_cluster: Cluster) -> None:
    mcp_cluster.host.allow_run = "safe"
    try:
        r = call("run", cmd=["python3", "-c", "print('safe-ok')"])
    finally:
        mcp_cluster.host.allow_run = True
    assert r["rc"] == 0
    assert r["stdout"].strip() == "safe-ok"
    audit = (mcp_cluster.registry.path.parent / "audit.log").read_text()
    assert '"event": "run"' in audit


def test_internal_error_is_dict(mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("kaboom")

    monkeypatch.setattr(Cluster, "ls", boom)
    r = call("ls", path="~")
    assert r["error"] == "internal"
    assert "kaboom" in r["message"]


def test_connection_local(mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("remoteslurm.cluster._clusters", {mcp_cluster.host.name: mcp_cluster})
    r = call("connection")
    assert r["host"] == "local"
    assert r["transport"] == "local"
    assert r["master_alive"] is True
    assert r["stub_alive"] is True
    assert r["action"] is None


def test_connection_ssh_not_alive(mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    from remoteslurm.config import HostConfig
    from remoteslurm.transport import SSHTransport

    hc = HostConfig(name="mycluster", ssh="mycluster", mfa=True)
    monkeypatch.setattr("remoteslurm.config.Config.load", classmethod(lambda cls, p=None: _H(hc)))
    monkeypatch.setattr("remoteslurm.cluster._clusters", {})
    monkeypatch.setattr(SSHTransport, "master_alive", lambda self: False)
    r = call("connection", host="mycluster")
    assert r["master_alive"] is False and r["stub_alive"] is False
    assert r["action"] == "run in a terminal: remoteslurm connect mycluster"
    assert "ssh" in r["connect_cmd"]


class _H:
    def __init__(self, hc: Any) -> None:
        self.hc = hc

    def host(self, name: str | None) -> Any:
        return self.hc


def test_output_cap(mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(server.ENV_MAX_CHARS, "50")
    r = call("read", path="~/proj/b.log")
    assert len(r["content"]) == 50
    assert r["truncated_by_server"] is True
    monkeypatch.delenv(server.ENV_MAX_CHARS)
    r = call("read", path="~/proj/a.txt")
    assert "truncated_by_server" not in r


# -- slurm tools via monkeypatched Cluster methods -----------------------------------------
def _st(job_id: str, state: str = "RUNNING") -> JobStatus:
    return JobStatus(
        job_id=job_id,
        state=state,
        source="squeue",
        stdout_path="/home/u/slurm-%j.out",
        script_path="/home/u/job.sh",
        workdir="/home/u",
    )


def test_submit_and_jobs(mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake_submit(self: Cluster, script: Any = None, **kw: Any) -> Any:
        from remoteslurm.jobs import Job

        seen.update(kw, script=script)
        return Job(self, "4242")

    monkeypatch.setattr(Cluster, "submit", fake_submit)
    monkeypatch.setattr(Cluster, "job_status", lambda self, jid, refresh=False: _st(jid, "PENDING"))
    monkeypatch.setattr(Cluster, "jobs", lambda self, **kw: [_st("1"), _st("2", "COMPLETED")])

    r = call(
        "submit",
        script="#!/bin/bash\necho hi\n",
        name="t",
        options={"time": "1:00:00"},
        args=["--exclusive"],
    )
    assert r["job_id"] == "4242" and r["state"] == "PENDING"
    assert r["stdout_path"].endswith(".out")
    assert seen["time"] == "1:00:00" and seen["args"] == ["--exclusive"]

    r = call("jobs", job_id="4242")
    assert r["job_id"] == "4242" and r["state"] == "PENDING"
    r = call("jobs")
    assert r["count"] == 2 and [j["job_id"] for j in r["jobs"]] == ["1", "2"]


def test_submit_invalid_arg(mcp_cluster: Cluster) -> None:
    r = call("submit")
    assert r["error"] == "invalid_arg"


def test_job_output_and_cancel(mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        Cluster,
        "job_output",
        lambda self, jid, **kw: {"content": "done\n", "job_id": jid, "state": "COMPLETED", **kw},
    )
    r = call("job_output", job_id="7", tail=5)
    assert r["content"] == "done\n" and r["tail"] == 5

    got: list[Any] = []
    monkeypatch.setattr(
        Cluster,
        "cancel",
        lambda self, ids, **kw: (got.append(ids), {"rc": 0, "cancelled": ["7"], "skipped": ["8"]})[
            1
        ],
    )
    r = call("cancel", job_id="7, 8")
    assert got == [["7", "8"]] and r["cancelled"] == ["7"] and r["skipped"] == ["8"]

    def missing(self: Cluster, jid: str, **kw: Any) -> Any:
        raise NotFound("no log", path="x")

    monkeypatch.setattr(Cluster, "job_output", missing)
    assert call("job_output", job_id="9")["error"] == "not_found"


def test_sinfo_and_info(mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Cluster, "sinfo", lambda self: [{"partition": "debug"}])
    r = call("sinfo")
    assert r["count"] == 1 and r["partitions"][0]["partition"] == "debug"
    r = call("info")
    assert r["home"] == mcp_cluster.home


def test_mcp_detach_proc_tools_and_wait(mcp_cluster: Cluster) -> None:
    r = call("run", cmd="echo hello; sleep 0.3; echo READY; sleep 300", detach=True)
    pid = r["pid"]
    try:
        w = call("wait", pid=pid, pattern="READY", timeout=20)
        assert w["done"] and w["met"] and w["reason"] == "matched" and w["line"] == "READY"
        assert call("proc_status", pid=pid)["state"] == "running"
        assert "hello" in call("proc_tail", pid=pid, lines=5)["content"]
        assert call("proc_status")["procs"][0]["pid"] == pid
        k = call("proc_kill", pid=pid, grace=2)
        assert k["killed"] and k["state"] == "exited" and k["rc"] == 143
    finally:
        mcp_cluster.proc_kill(pid, signal="KILL", grace=0)


def test_mcp_wait_needs_exactly_one_kind_of_target(mcp_cluster: Cluster) -> None:
    assert call("wait")["error"] == "invalid_arg"
    assert call("wait", job_id="1", pid=2)["error"] == "invalid_arg"
    assert call("proc_status", pid=99999999)["error"] == "not_found"


def test_mcp_proc_kill_honours_confirm(mcp_cluster: Cluster) -> None:
    mcp_cluster.host.confirm = ["proc_kill"]
    r = call("run", cmd="sleep 300", detach=True)
    try:
        assert call("proc_kill", pid=r["pid"])["needs_confirmation"] is True
        assert call("proc_status", pid=r["pid"])["state"] == "running"
        assert call("proc_kill", pid=r["pid"], confirm=True, grace=1)["killed"] is True
    finally:
        mcp_cluster.host.confirm = []
        mcp_cluster.proc_kill(r["pid"], signal="KILL", grace=0)


def _lifetime_setup(monkeypatch: pytest.MonkeyPatch, hc: Any, age: int) -> None:
    from remoteslurm.transport import SSHTransport

    monkeypatch.setattr("remoteslurm.config.Config.load", classmethod(lambda cls, p=None: _H(hc)))
    monkeypatch.setattr("remoteslurm.cluster._clusters", {})
    monkeypatch.setattr(SSHTransport, "master_alive", lambda self: True)
    monkeypatch.setattr(SSHTransport, "master_pid", lambda self: 4242)
    monkeypatch.setattr("remoteslurm.transport.process_age", lambda pid: age)


def test_connection_warns_before_session_lifetime(
    mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    from remoteslurm.config import HostConfig

    hc = HostConfig(name="mycluster", ssh="mycluster", mfa=True, session_lifetime="5h30m")
    _lifetime_setup(monkeypatch, hc, 5 * 3600 + 600)
    r = call("connection", host="mycluster")
    assert r["master_alive"] is True and r["master_pid"] == 4242
    assert r["age"] == "5h10m" and r["age_seconds"] == 18600 and r["connected_at"]
    assert r["remaining_seconds"] == 1200 and r["expires_at"] and r["expiring"] is True
    assert "in 20m" in r["warning"] and "reconnect" in r["warning"]
    assert r["action"] == "run in a terminal: remoteslurm connect --force mycluster"


def test_connection_without_session_lifetime_reports_age_only(
    mcp_cluster: Cluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    from remoteslurm.config import HostConfig

    hc = HostConfig(name="mycluster", ssh="mycluster", mfa=True)
    _lifetime_setup(monkeypatch, hc, 3600)
    r = call("connection", host="mycluster")
    assert r["age"] == "1h00m" and r["expires_at"] is None and r["remaining_seconds"] is None
    assert "warning" not in r and r["action"] is None
    assert "idle timeout" in r["lifetime_note"]


@pytest.mark.parametrize(
    ("text", "secs"),
    [
        ("05:07", 307),
        ("01:02:03", 3723),
        ("2-03:04:05", 2 * 86400 + 3 * 3600 + 4 * 60 + 5),
        ("  42:00\n", 2520),
        ("", None),
        ("abc", None),
    ],
)
def test_parse_etime(text: str, secs: int | None) -> None:
    from remoteslurm.transport import parse_etime

    assert parse_etime(text) == secs


def test_parse_duration_and_session_lifetime_validation() -> None:
    from remoteslurm.config import HostConfig, parse_duration
    from remoteslurm.errors import ConfigError

    assert parse_duration("24h") == 86400
    assert parse_duration("1h30m") == 5400
    assert parse_duration("2D") == 172800
    assert parse_duration(90) == 90
    for bad in ("", "h", "1x", "soon", 0, -5, True):
        with pytest.raises(ValueError):
            parse_duration(bad)  # type: ignore[arg-type]
    with pytest.raises(ConfigError):
        HostConfig(name="x", ssh="x", session_lifetime="soon")


def test_mcp_config_snippet() -> None:
    d = json.loads(server.mcp_config_snippet("mycluster"))
    srv = d["mcpServers"]["remoteslurm"]
    assert srv["command"] == "remoteslurm-mcp"
    assert srv["env"]["REMOTESLURM_DEFAULT_HOST"] == "mycluster"
