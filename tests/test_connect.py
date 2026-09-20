"""Connection diagnostics must prove socket reuse, without requiring live SSH or MFA."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from remoteslurm import cli
from remoteslurm.cluster import Cluster
from remoteslurm.errors import AuthRequired, ConfigError, NotConnected
from remoteslurm.transport import SSHTransport


@pytest.fixture
def ssh(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    state = SimpleNamespace(
        config="controlmaster auto\ncontrolpath /tmp/review.sock\ncontrolpersist 43200\n",
        config_rc=0,
        connect_rc=0,
        alive=False,
        reusable=True,
        calls=[],
    )

    def run(argv, **kwargs):
        assert argv[0] == "ssh", argv
        state.calls.append(argv)
        if "-G" in argv:
            return subprocess.CompletedProcess(argv, state.config_rc, state.config, "bad config")
        if "-O" in argv:
            if argv[argv.index("-O") + 1] == "exit":
                state.alive = False
            return subprocess.CompletedProcess(
                argv, 0 if state.alive else 255, b"", b"Master running (pid=1234)"
            )
        assert "-fN" in argv, argv
        state.alive = state.connect_rc == 0 and state.reusable
        return subprocess.CompletedProcess(argv, state.connect_rc, b"", b"authentication failed")

    monkeypatch.setattr("remoteslurm.transport.subprocess.run", run)
    return state


@pytest.mark.parametrize("interactive", [True, False])
@pytest.mark.parametrize("config", ["controlmaster auto\n", "controlpath none\n"])
def test_missing_control_path_fails_before_authentication(ssh, interactive, config) -> None:
    ssh.config = config
    transport = SSHTransport(alias="user@bare.invalid")
    with pytest.raises(ConfigError, match="no effective ControlPath") as error:
        transport.establish_master(interactive=interactive)
    assert error.value.details["alias"] == "user@bare.invalid"
    assert "control_path" in error.value.action
    assert all("-G" in argv for argv in ssh.calls)


@pytest.mark.parametrize("interactive", [True, False])
def test_successful_ssh_exit_requires_a_reusable_master(ssh, interactive) -> None:
    ssh.reusable = False
    with pytest.raises(NotConnected, match="no reusable master") as error:
        SSHTransport(alias="cluster").establish_master(interactive=interactive)
    assert error.value.code == "not_connected"
    assert error.value.details["control_path"] == "/tmp/review.sock"
    assert "ControlPath" in error.value.action
    assert "-O" in ssh.calls[-1] and "check" in ssh.calls[-1]


@pytest.mark.parametrize("interactive", [True, False])
def test_successful_master_is_verified_for_both_authentication_modes(ssh, interactive) -> None:
    SSHTransport(alias="cluster").establish_master(interactive=interactive)
    assert ssh.alive
    assert "-O" in ssh.calls[-1] and "check" in ssh.calls[-1]
    establishment = next(argv for argv in ssh.calls if "-fN" in argv and "-G" not in argv)
    assert ("BatchMode=yes" in establishment) is not interactive


@pytest.mark.parametrize("interactive", [True, False])
def test_authentication_failure_preserves_auth_required(ssh, interactive) -> None:
    ssh.connect_rc = 255
    with pytest.raises(AuthRequired):
        SSHTransport(alias="cluster").establish_master(interactive=interactive)
    assert not any("-O" in argv for argv in ssh.calls)


def test_invalid_ssh_config_fails_before_authentication(ssh) -> None:
    ssh.config_rc = 255
    with pytest.raises(ConfigError, match="bad config"):
        SSHTransport(alias="cluster").establish_master(interactive=True)
    assert all("-G" in argv for argv in ssh.calls)


@pytest.mark.parametrize(
    "failure", [FileNotFoundError("ssh"), subprocess.TimeoutExpired("ssh", 10)]
)
def test_unavailable_config_inspection_is_a_structured_error(monkeypatch, failure) -> None:
    run = Mock(side_effect=failure)
    monkeypatch.setattr("remoteslurm.transport.subprocess.run", run)
    with pytest.raises(ConfigError, match="could not inspect SSH configuration"):
        SSHTransport(alias="cluster").establish_master(interactive=True)
    assert run.call_count == 1


@pytest.fixture
def connect_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Mock]:
    cfg = tmp_path / "config.toml"
    cfg.write_text('[hosts.named]\nssh = "user@configured.invalid"\n')
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(tmp_path / "absent.toml"))
    monkeypatch.delenv("REMOTESLURM_DEFAULT_HOST", raising=False)
    cluster = SimpleNamespace(
        ping=lambda: None,
        info=lambda: {"hostname": "review", "python": "3.11"},
    )
    connect = Mock(return_value=cluster)
    monkeypatch.setattr(Cluster, "connect", connect)
    return cfg, connect


@pytest.mark.parametrize("host", ["named", "user@bare.invalid"])
def test_cli_verifies_before_success_and_honors_config(ssh, connect_cli, capsys, host) -> None:
    cfg, connect = connect_cli
    rc = cli.main(["--config", str(cfg), "connect", host])
    output = capsys.readouterr()
    assert rc == 0, output
    assert "ssh master established" in output.out
    assert "stub running" in output.out
    assert connect.call_args.kwargs["config"].path == cfg
    expected_alias = "user@configured.invalid" if host == "named" else host
    assert all(argv[-1] == expected_alias for argv in ssh.calls)
    assert "-O" in ssh.calls[-1] and "check" in ssh.calls[-1]


@pytest.mark.parametrize("missing_path", [True, False])
def test_cli_never_reports_unusable_master_as_established(
    ssh, connect_cli, capsys, missing_path
) -> None:
    cfg, connect = connect_cli
    if missing_path:
        ssh.config = "controlmaster auto\n"
    ssh.reusable = False
    rc = cli.main(["connect", "named", "--config", str(cfg)])
    output = capsys.readouterr()
    assert rc == (1 if missing_path else 3), output
    assert "ssh master established" not in output.out
    assert "ControlPath" in output.err
    assert "complete MFA" not in output.err
    connect.assert_not_called()


def test_existing_master_does_not_reauthenticate(ssh, connect_cli, capsys, monkeypatch) -> None:
    cfg, _ = connect_cli
    ssh.alive = True
    monkeypatch.setattr(SSHTransport, "master_lifetime", lambda *_: {})
    assert cli.main(["--config", str(cfg), "connect", "named"]) == 0
    assert "already alive" in capsys.readouterr().out
    assert all("-O" in argv for argv in ssh.calls)


def test_doctor_uses_effective_host_options(ssh, connect_cli, capsys, monkeypatch) -> None:
    monkeypatch.setattr("remoteslurm.sync.find_rsync", lambda: ("rsync", (3, 2)))
    cfg, connect = connect_cli
    cfg.write_text(
        '[hosts.named]\nssh = "user@configured.invalid"\n'
        'ssh_opts = ["-F", "/tmp/alternate-ssh-config", "-o", "Port=2222"]\n'
    )
    rc = cli.main(["--config", str(cfg), "--json", "doctor", "--host", "named"])
    result = json.loads(capsys.readouterr().out)
    assert rc == 3
    check = next(item for item in result["checks"] if item["check"] == "ssh ControlMaster")
    assert check["ok"] is True
    assert "ControlPath=/tmp/review.sock" in check["detail"]
    assert all("/tmp/alternate-ssh-config" in argv and "Port=2222" in argv for argv in ssh.calls)
    connect.assert_not_called()
