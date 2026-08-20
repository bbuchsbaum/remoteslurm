"""WP-F4: a host's ``ssh_opts`` (e.g. ProxyJump) flow through every ssh invocation.

Jump hosts belong either in ``~/.ssh/config`` (transparent to remoteslurm) or, per host, in
``[hosts.<h>] ssh_opts``. When set in config they must reach: the persistent-transport argv
(``_ssh_base`` / ``connect_cmd``) and rsync's ``-e`` string (``sync.transport_ssh_opts``), so
remoteslurm and its rsync both traverse the same bastion. All unit-tested on argv — no real ssh.
"""

from __future__ import annotations

from types import SimpleNamespace

from remoteslurm import sync as sync_mod
from remoteslurm.cluster import Cluster
from remoteslurm.config import HostConfig
from remoteslurm.transport import SSHTransport

PROXY = ["-o", "ProxyJump=bastion"]


def _sublist(hay: list[str], needle: list[str]) -> bool:
    return any(hay[i : i + len(needle)] == needle for i in range(len(hay) - len(needle) + 1))


def test_config_ssh_opts_reach_transport() -> None:
    host = HostConfig.from_dict("cluster", {"ssh": "cluster", "ssh_opts": PROXY})
    t = Cluster._transport_for(host)
    assert isinstance(t, SSHTransport)
    assert t.extra_ssh_opts == PROXY
    # Every ssh command the transport builds carries the opts.
    assert _sublist(t._ssh_base(), PROXY)
    assert _sublist(t.connect_cmd(), PROXY)


def test_transport_ssh_opts_includes_proxyjump_for_rsync() -> None:
    t = SSHTransport(alias="cluster", extra_ssh_opts=PROXY, control_path="/tmp/cm-sock")
    fake_cluster = SimpleNamespace(transport=t)
    opts = sync_mod.transport_ssh_opts(fake_cluster)  # type: ignore[arg-type]
    assert opts is not None
    assert _sublist(opts, PROXY)
    # ProxyJump plus the ControlPath both land in the rsync -e argv.
    assert _sublist(opts, ["-o", "ControlPath=/tmp/cm-sock"])
    assert opts[0] == "ssh"


def test_transport_ssh_opts_appear_in_rsync_e_string() -> None:
    t = SSHTransport(alias="cluster", extra_ssh_opts=PROXY)
    opts = sync_mod.transport_ssh_opts(SimpleNamespace(transport=t))  # type: ignore[arg-type]
    cmd = sync_mod.build_rsync_cmd("rsync", "/src", "/dst", "cluster", ssh_opts=opts)
    e_idx = cmd.index("-e")
    rsh = cmd[e_idx + 1]
    assert "ProxyJump=bastion" in rsh
