"""WP-E2: safety rails — protected paths, recursive-rm guards, allow_run modes, confirm flow."""

from __future__ import annotations

import asyncio
import json
import shutil
from collections.abc import Callable
from pathlib import Path

import pytest

from remoteslurm.cluster import Cluster
from remoteslurm.config import DEFAULT_PROTECTED_PATHS, Config, HostConfig
from remoteslurm.errors import ConfigError, ConfirmationRequired, InvalidArgument, PermissionDenied


# --------------------------------------------------------------------------- protected paths
@pytest.mark.parametrize("target", ["~/.ssh/id_rsa", "~/.bashrc", "~/.cache/remoteslurm/stub.py"])
def test_write_refuses_protected_path(cluster: Cluster, target: str) -> None:
    with pytest.raises(PermissionDenied) as ei:
        cluster.write(target, "x\n")
    assert "force" in (ei.value.action or "")


def test_write_protected_allowed_with_force(cluster: Cluster, sandbox: Path) -> None:
    r = cluster.write("~/.ssh/config", "Host *\n", force=True)
    assert (sandbox / ".ssh" / "config").read_text() == "Host *\n"
    assert r["path"].endswith(".ssh/config")


def test_edit_refuses_protected_path(cluster: Cluster, sandbox: Path) -> None:
    cluster.write("~/.bashrc", "export A=1\n", force=True)
    with pytest.raises(PermissionDenied):
        cluster.edit("~/.bashrc", "A=1", "A=2")
    r = cluster.edit("~/.bashrc", "A=1", "A=2", force=True)
    assert r["replacements"] == 1


def test_rm_refuses_protected_path(cluster: Cluster, sandbox: Path) -> None:
    cluster.write("~/.ssh/known_hosts", "h\n", force=True)
    with pytest.raises(PermissionDenied):
        cluster.rm("~/.ssh/known_hosts")
    assert cluster.rm("~/.ssh/known_hosts", force=True)["removed"] is True


def test_absolute_protected_path_matches_via_home(cluster: Cluster, sandbox: Path) -> None:
    # An absolute path (no leading ~) still matches the ~-relative protected glob via the home.
    abs_ssh = str(sandbox / ".ssh" / "authorized_keys")
    with pytest.raises(PermissionDenied):
        cluster.write(abs_ssh, "key\n")


def test_unprotected_write_still_works(cluster: Cluster, sandbox: Path) -> None:
    cluster.write("~/notes/todo.txt", "ok\n")
    assert (sandbox / "notes" / "todo.txt").read_text() == "ok\n"


# --------------------------------------------------------------------------- recursive rm guards
def test_rm_recursive_refuses_scratch_root(
    make_cluster: Callable[..., Cluster], tmp_path: Path
) -> None:
    scratch = tmp_path / "scratch_root"
    scratch.mkdir()
    (scratch / "keep").write_text("x\n")
    c = make_cluster({"SCRATCH": str(scratch)})
    with pytest.raises(InvalidArgument) as ei:
        c.rm(str(scratch), recursive=True, force=True)  # force skips the client check; stub refuses
    assert "root" in ei.value.message
    assert scratch.exists()  # nothing was removed


def test_rm_recursive_refuses_shallow_path(cluster: Cluster) -> None:
    shallow = Path("/tmp/rs_rmtest_shallow")  # depth 2 -> refused before any deletion
    shallow.mkdir(exist_ok=True)
    (shallow / "f").write_text("x\n")
    try:
        with pytest.raises(InvalidArgument) as ei:
            cluster.rm(str(shallow), recursive=True, force=True)
        assert "shallow" in ei.value.message
        assert shallow.exists()  # guard fired before rmtree
    finally:
        shutil.rmtree(shallow, ignore_errors=True)


def test_rm_recursive_deep_path_ok(cluster: Cluster, sandbox: Path) -> None:
    (sandbox / "deep").mkdir()
    (sandbox / "deep" / "child").write_text("x\n")
    assert cluster.rm("~/deep", recursive=True)["removed"] is True
    assert not (sandbox / "deep").exists()


# --------------------------------------------------------------------------- allow_run modes
def test_allow_run_safe_rejects_shell_string(cluster: Cluster) -> None:
    cluster.host.allow_run = "safe"
    with pytest.raises(PermissionDenied) as ei:
        cluster.run("echo hi")
    assert "argv" in (ei.value.action or "")


def test_allow_run_safe_rejects_non_allowlisted_exe(cluster: Cluster) -> None:
    cluster.host.allow_run = "safe"
    with pytest.raises(PermissionDenied):
        cluster.run(["definitely-not-allowed-xyz", "arg"])


def test_allow_run_safe_permits_python3_argv(cluster: Cluster) -> None:
    cluster.host.allow_run = "safe"
    r = cluster.run(["python3", "-c", "print('ok')"])
    assert r["rc"] == 0 and r["stdout"].strip() == "ok"


def test_allow_run_false_disables(cluster: Cluster) -> None:
    cluster.host.allow_run = False
    with pytest.raises(PermissionDenied):
        cluster.run(["python3", "-c", "print(1)"])


def test_allow_run_safe_applies_to_compute(cluster: Cluster) -> None:
    cluster.host.allow_run = "safe"
    with pytest.raises(PermissionDenied):
        cluster.run("hostname", compute=True, time="00:01:00")  # shell string form rejected


# --------------------------------------------------------------------------- confirm flow (library)
def test_rm_confirm_required(make_cluster: Callable[..., Cluster], sandbox: Path) -> None:
    c = make_cluster(confirm=["rm"])
    c.write("~/todelete.txt", "x\n")
    with pytest.raises(ConfirmationRequired) as ei:
        c.rm("~/todelete.txt")
    assert "todelete.txt" in ei.value.what
    assert (sandbox / "todelete.txt").exists()  # not removed
    assert c.rm("~/todelete.txt", confirm=True)["removed"] is True


def test_cancel_confirm_required(make_cluster: Callable[..., Cluster]) -> None:
    c = make_cluster(confirm=["cancel"])
    with pytest.raises(ConfirmationRequired) as ei:
        c.cancel("12345")
    assert "12345" in ei.value.what
    # confirm=True proceeds to scancel (12345 is not ours -> skipped, no ConfirmationRequired)
    r = c.cancel("12345", confirm=True)
    assert r["skipped"] == ["12345"]


# --------------------------------------------------------------------------- confirm flow (MCP)
def _call_all_tools(cluster: Cluster, monkeypatch: pytest.MonkeyPatch, tool: str, **args: object):
    from mcp.shared.memory import create_connected_server_and_client_session

    from remoteslurm import server

    monkeypatch.setattr(server, "_get_cluster", lambda host: cluster)
    m = server.make_mcp("all")

    async def go() -> dict[str, object]:
        async with create_connected_server_and_client_session(m) as client:
            res = await client.call_tool(tool, args)
            if res.structuredContent is not None:
                return dict(res.structuredContent)
            return dict(json.loads(res.content[0].text))  # type: ignore[union-attr]

    return asyncio.run(go())


def test_mcp_cancel_needs_confirmation(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    cluster.host.confirm = ["cancel"]
    r = _call_all_tools(cluster, monkeypatch, "cancel", job_id="12345")
    assert r.get("needs_confirmation") is True
    assert "12345" in r["what"]
    assert "error" not in r


def test_mcp_cancel_with_confirm_proceeds(
    cluster: Cluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster.host.confirm = ["cancel"]
    r = _call_all_tools(cluster, monkeypatch, "cancel", job_id="12345", confirm=True)
    assert r.get("skipped") == ["12345"]
    assert "needs_confirmation" not in r


def test_mcp_write_protected_needs_force(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    r = _call_all_tools(cluster, monkeypatch, "write", path="~/.ssh/evil", content="x\n")
    assert r["error"] == "permission"
    r2 = _call_all_tools(
        cluster, monkeypatch, "write", path="~/.ssh/evil", content="x\n", force=True
    )
    assert "error" not in r2 and r2["path"].endswith(".ssh/evil")


# --------------------------------------------------------------------------- config parsing
def test_config_allow_run_safe_and_lists(tmp_path: Path) -> None:
    cfg = tmp_path / "c.toml"
    cfg.write_text(
        '[hosts.h]\nssh = "h"\nallow_run = "safe"\n'
        'confirm = ["rm", "cancel"]\nprotected_paths = ["~/x/**"]\n'
        'run_allowlist = ["python3", "Rscript"]\n'
    )
    h = Config.load(cfg).hosts["h"]
    assert h.allow_run == "safe"
    assert h.confirm == ["rm", "cancel"]
    assert h.protected_paths == ["~/x/**"]
    assert h.run_allowlist == ["python3", "Rscript"]


def test_config_allow_run_bool_backcompat(tmp_path: Path) -> None:
    cfg = tmp_path / "c.toml"
    cfg.write_text('[hosts.h]\nssh = "h"\nallow_run = false\n')
    assert Config.load(cfg).hosts["h"].allow_run is False


def test_config_allow_run_invalid_rejected(tmp_path: Path) -> None:
    cfg = tmp_path / "c.toml"
    cfg.write_text('[hosts.h]\nssh = "h"\nallow_run = "sorta"\n')
    with pytest.raises(ConfigError):
        Config.load(cfg)


def test_config_safety_defaults() -> None:
    h = HostConfig(name="x", ssh="x")
    assert h.protected_paths == DEFAULT_PROTECTED_PATHS
    assert h.confirm == []
    assert "python*" in h.run_allowlist
    assert h.allow_run is True


def test_safe_mode_rejects_bash_dash_c(make_cluster):
    """allow_run='safe' must reject `bash -c '<arbitrary>'` even as argv (closes the hole)."""
    import pytest

    from remoteslurm.errors import PermissionDenied

    c = make_cluster(allow_run="safe")
    # a bare allowed executable is fine
    assert c.run(["echo", "ok"])["stdout"].strip() == "ok"
    # bash script.sh is allowed (running a script file)
    # but bash -c '<anything>' is arbitrary execution -> refused
    with pytest.raises(PermissionDenied):
        c.run(["bash", "-c", "echo pwned"])
    with pytest.raises(PermissionDenied):
        c.run(["sh", "-lc", "echo pwned"])
