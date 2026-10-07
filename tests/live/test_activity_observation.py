"""Opt-in two-submission qualification; remote fixtures are retained as evidence."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

from remoteslurm import Cluster, daemon
from remoteslurm.activity.monitor import enroll, status
from remoteslurm.activity.store import ActivityStore
from remoteslurm.config import Config
from remoteslurm.identity import control_identity
from remoteslurm.workspace import service

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not os.environ.get("REMOTESLURM_LIVE"),
        reason="explicit REMOTESLURM_LIVE authorization required",
    ),
]


def stop_owned(socket: Path, owner: int) -> None:
    assert daemon.daemon_status(socket)["pid"] == owner
    assert daemon.stop_daemon(socket)
    deadline = time.monotonic() + 60
    while daemon.daemon_available(socket):
        if time.monotonic() >= deadline:
            pytest.fail("owned daemon did not complete shutdown within one minute")
        time.sleep(0.2)


@pytest.mark.timeout(900)
def test_live_activity_agent_exit_daemon_restart_array_and_mixed_pack(tmp_path, monkeypatch):
    host = os.environ.get("REMOTESLURM_LIVE_HOST")
    root = os.environ.get("REMOTESLURM_LIVE_CWD")
    walltime = os.environ.get("REMOTESLURM_LIVE_TIME")
    if not host or not root or not walltime:
        pytest.skip("live host, scratch cwd and explicit walltime are required")
    cfg = Config.load()
    hc = cfg.host(host)
    # Fail before remote writes if source/tests differ from the immutable candidate.
    subprocess.run(
        ["git", "diff", "--exit-code", "HEAD", "--", "src", "tests", "pyproject.toml"], check=True
    )
    candidate = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    suffix = uuid.uuid4().hex[:10]
    work_root = root.rstrip("/") + "/rslurm-observation-" + suffix
    socket = Path(tempfile.mkdtemp(prefix="rsoa-")) / "d.sock"
    monkeypatch.setenv(daemon.ENV_SOCKET, str(socket))
    monkeypatch.delenv(daemon.ENV_NO_DAEMON, raising=False)
    cluster = Cluster.connect(host, config=cfg)
    identity = control_identity()
    info = cluster.info(refresh=True)
    assert info["stub_sha"] == identity["stub_sha"]
    names, jobs, owner = [], [], None
    evidence = {
        "candidate": candidate,
        "identity": identity,
        "host": host,
        "root": work_root,
        "resources": {
            "account": hc.account,
            "partition": hc.partition,
            "time": walltime,
            "cpus_per_task": 1,
            "mem": "256M",
        },
        "result": "INCOMPLETE",
    }
    script = tmp_path / "fixture.sh"
    script.write_text(
        "#!/bin/bash\npython3 - <<'PY'\nimport json,os,sys\n"
        "p=json.loads(os.environ['RS_PARAMS_JSON'])\n"
        "out=json.loads(os.environ['RS_OUTPUTS_JSON'])['result']\n"
        "if p['sample']=='fail': sys.exit(7)\n"
        "with open(out,'w') as f: f.write('observation fixture\\n')\nPY\n"
    )
    try:
        cluster.mkdir(work_root)
        workspace = service.subscribe(service.select(cfg, host, path=work_root, cluster=cluster))
        for mode in ("array", "pack"):
            name = "observe" + mode + suffix
            names.append(name)
            samples = ["one", "two"] if mode == "array" else ["success", "fail"]
            execution = {"mode": mode}
            if mode == "pack":
                execution.update(units_per_allocation=2, max_processes=1)
            spec = {
                "schema": 1,
                "name": name,
                "host": host,
                "workspace": {"remote_root": work_root, "output_root": work_root},
                "inventories": {
                    "samples": {"key": ["sample"], "rows": [{"sample": s} for s in samples]}
                },
                "stages": {
                    "analysis": {
                        "foreach": "samples",
                        "script": str(script),
                        "execution": execution,
                        "resources": evidence["resources"],
                        "outputs": [
                            {
                                "name": "result",
                                "kind": "file",
                                "path": mode + "-{sample}.txt",
                                "min_bytes": 1,
                            }
                        ],
                    }
                },
            }
            path = tmp_path / (name + ".json")
            path.write_text(json.dumps(spec))
            command = [
                sys.executable,
                "-m",
                "remoteslurm.cli",
                "--no-daemon",
                "--json",
                "-H",
                host,
                "campaign",
            ]
            started = subprocess.run(
                [*command, "start", str(path), "--run-id", "qualification"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            assert started.returncode == 0, started.stdout + started.stderr
            # The submitting agent process exits before observation starts.
            applied = subprocess.run(
                [*command, "apply", name, "--run", "qualification"],
                capture_output=True,
                text=True,
                timeout=180,
            )
            assert applied.returncode == 0, applied.stdout + applied.stderr
            ledger = cluster.call(
                "campaign_ledger_read",
                campaign_dir=hc.campaign_dir,
                name=name,
                run_id="qualification",
            )
            jobs.extend(
                str(a["job_id"]) for g in ledger["groups"] for a in g["attempts"] if a.get("job_id")
            )
            evidence.update(campaigns=names[:], job_ids=jobs[:], socket=str(socket))
            enroll(hc, name, "qualification")
        assert len(jobs) == 2
        # Erase our isolated local imports, exercising passive recovery after agent loss.
        store = ActivityStore()
        with store.transaction() as db:
            db.execute("DELETE FROM events")
            db.execute(
                "DELETE FROM documents WHERE namespace IN "
                "('work','semantic','campaign_unit','campaign','checkpoint')"
            )
        assert daemon.spawn_daemon(socket, config_path=cfg.path)
        owner = daemon.daemon_status(socket)["pid"]
        deadline = time.monotonic() + 600
        restarted = False
        while time.monotonic() < deadline:
            work = store.documents("work", limit=1000)
            if work and not restarted:
                stop_owned(socket, owner)
                assert daemon.spawn_daemon(socket, config_path=cfg.path)
                new_owner = daemon.daemon_status(socket)["pid"]
                assert new_owner != owner
                evidence["daemon_pids"] = [owner, new_owner]
                owner, restarted = new_owner, True
            array = [w for w in work if w.get("campaign") == names[0]]
            pack = [w for w in work if w.get("campaign") == names[1]]
            if (
                len(array) == 2
                and len(pack) == 2
                and all(
                    w.get("execution") == "COMPLETED" and w.get("artifacts") == "PRESENT"
                    for w in array
                )
                and {w.get("execution") for w in pack} == {"COMPLETED", "FAILED"}
            ):
                break
            time.sleep(5)
        else:
            pytest.fail("live observation did not converge within ten minutes")
        assert restarted and all(w["validation"] == "NOT_RUN" for w in work)
        output_receipts = {
            filename: cluster.read_text(work_root + "/" + filename)
            for filename in ("array-one.txt", "array-two.txt", "pack-success.txt")
        }
        assert all(value == "observation fixture\n" for value in output_receipts.values())
        snapshot = service.snapshot(workspace)
        assert not snapshot["attention"]["authorization"]["grants_authority"]
        evidence.update(
            result="PASS",
            job_ids=jobs,
            monitor=status(host),
            work=[
                {
                    k: w.get(k)
                    for k in (
                        "campaign",
                        "unit_id",
                        "attempt_id",
                        "job_id",
                        "execution",
                        "artifacts",
                        "validation",
                        "marker_dir",
                    )
                }
                for w in work
            ],
            recovery_events=len(
                [
                    e
                    for e in store.query(limit=1000)["events"]
                    if e["kind"] == "submission.recovered"
                ]
            ),
            output_receipts=output_receipts,
        )
    finally:
        if (
            owner
            and daemon.daemon_available(socket)
            and daemon.daemon_status(socket)["pid"] == owner
        ):
            stop_owned(socket, owner)
        cluster.close()
        output = os.environ.get("REMOTESLURM_LIVE_EVIDENCE")
        if output:
            Path(output).write_text(json.dumps(evidence, indent=2) + "\n")
