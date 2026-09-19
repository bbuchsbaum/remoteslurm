from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from remoteslurm.campaigns.spec import compile_campaign
from remoteslurm.errors import InvalidArgument


def validation_definition(
    root: Path,
    *,
    name: str = "validated",
    validator: list[str] | None = None,
    validator_timeout: int = 10,
    stable: bool = False,
    sha256: bool = False,
):
    (root / "run.sh").write_text("#!/bin/sh\ntrue\n")
    stage: dict = {
        "foreach": "items",
        "script": "run.sh",
        "outputs": [
            {
                "name": "result",
                "kind": "file",
                "path": "sub-{subject}/result.txt",
                "min_bytes": 1,
                "sha256": sha256,
                **(
                    {
                        "stable_for": 0.2,
                        "settle_timeout": 1.0,
                        "settle_interval": 0.2,
                    }
                    if stable
                    else {}
                ),
            }
        ],
    }
    if validator is not None:
        stage["validators"] = [{"kind": "command", "argv": validator, "timeout": validator_timeout}]
    return compile_campaign(
        {
            "schema": 1,
            "name": name,
            "host": "local",
            "workspace": {
                "remote_root": str(root),
                "output_root": str(root / "outputs"),
            },
            "inventories": {"items": {"key": ["subject"], "rows": [{"subject": "001"}]}},
            "stages": {"analysis": stage},
        },
        base_dir=root,
    )


def write_product(root: Path, text: str = "ok\n") -> Path:
    product = root / "outputs" / "sub-001" / "result.txt"
    product.parent.mkdir(parents=True, exist_ok=True)
    product.write_text(text)
    return product


def test_verify_stores_receipt_and_later_mutation_is_stale(cluster, sandbox: Path) -> None:
    (sandbox / "check.py").write_text(
        "import pathlib, sys\nraise SystemExit(0 if pathlib.Path(sys.argv[1]).read_text() else 2)\n"
    )
    definition = validation_definition(
        sandbox,
        validator=["python3", "check.py", "{output:result}"],
    )
    cluster.campaigns.start(definition, run_id="run-verify")
    product = write_product(sandbox)

    verified = cluster.campaigns.verify("validated", run_id="run-verify")
    assert verified["counts"] == {"PASSED": 1}
    receipt_id = verified["results"][0]["receipt_id"]
    receipt = cluster.campaigns.receipts(
        "validated", "validation", run_id="run-verify", receipt_id=receipt_id
    )["value"]
    assert receipt["result"] == "PASSED"
    assert receipt["validators"][0]["rc"] == 0

    product.write_text("changed and longer\n")
    status = cluster.campaigns.status(
        "validated", run_id="run-verify", refresh=True, include_units=True
    )
    assert status["units"][0]["artifacts"]["state"] == "CHANGED"
    assert status["units"][0]["validation"]["state"] == "STALE"

    reverified = cluster.campaigns.verify("validated", run_id="run-verify")
    assert reverified["counts"] == {"PASSED": 1}
    current = cluster.campaigns.status("validated", run_id="run-verify", include_units=True)[
        "units"
    ][0]
    assert receipt_id in current["validation"]["receipt_history"]
    assert (
        cluster.campaigns.receipts(
            "validated", "validation", run_id="run-verify", receipt_id=receipt_id
        )["value"]["result"]
        == "PASSED"
    )


def test_status_observes_settling_once_without_running_validation(cluster, sandbox: Path) -> None:
    definition = validation_definition(sandbox, name="settling", stable=True)
    cluster.campaigns.start(definition, run_id="run-settle")
    write_product(sandbox)
    started = time.monotonic()
    first = cluster.campaigns.status(
        "settling", run_id="run-settle", refresh=True, include_units=True
    )
    assert time.monotonic() - started < 0.2
    assert first["units"][0]["artifacts"]["state"] == "SETTLING"
    assert first["units"][0]["validation"]["state"] == "NOT_RUN"
    time.sleep(0.25)
    second = cluster.campaigns.status(
        "settling", run_id="run-settle", refresh=True, include_units=True
    )
    assert second["units"][0]["artifacts"]["state"] == "PRESENT"
    assert second["units"][0]["validation"]["state"] == "NOT_RUN"


def test_sha256_detects_same_size_same_mtime_mutation(cluster, sandbox: Path) -> None:
    definition = validation_definition(sandbox, name="hashed", sha256=True)
    cluster.campaigns.start(definition, run_id="run-hash")
    product = write_product(sandbox, "one\n")
    original_mtime = product.stat().st_mtime
    assert cluster.campaigns.verify("hashed", run_id="run-hash")["counts"] == {"PASSED": 1}

    product.write_text("two\n")
    os.utime(product, (original_mtime, original_mtime))
    status = cluster.campaigns.status("hashed", run_id="run-hash", refresh=True, include_units=True)
    assert status["units"][0]["artifacts"]["state"] == "CHANGED"
    assert status["units"][0]["validation"]["state"] == "STALE"


@pytest.mark.parametrize(
    ("argv", "timeout", "expected_state", "failure_code"),
    [
        (["python3", "-c", "raise SystemExit(7)"], 10, "FAILED", "validator_exit"),
        (
            ["python3", "-c", "import time; time.sleep(2)"],
            1,
            "ERROR",
            "validator_timeout",
        ),
        (["command-that-does-not-exist-rslurm"], 10, "ERROR", "validator_crash"),
    ],
)
def test_validator_failure_modes_are_distinct(
    cluster,
    sandbox: Path,
    argv: list[str],
    timeout: int,
    expected_state: str,
    failure_code: str,
) -> None:
    definition = validation_definition(
        sandbox,
        name="validatorcase",
        validator=argv,
        validator_timeout=timeout,
    )
    cluster.campaigns.start(definition, run_id="run-validator")
    write_product(sandbox)
    result = cluster.campaigns.verify("validatorcase", run_id="run-validator")
    assert result["counts"] == {expected_state: 1}
    receipt = cluster.campaigns.receipts(
        "validatorcase",
        "validation",
        run_id="run-validator",
        receipt_id=result["results"][0]["receipt_id"],
    )["value"]
    assert receipt["validators"][0]["failure_code"] == failure_code


def test_closed_runs_revalidate_and_archived_runs_require_restore(cluster, sandbox: Path) -> None:
    definition = validation_definition(sandbox, name="lifecycleverify")
    cluster.campaigns.start(definition, run_id="run-life")
    write_product(sandbox)
    cluster.campaigns.close("lifecycleverify", run_id="run-life")
    assert cluster.campaigns.verify("lifecycleverify", run_id="run-life")["counts"] == {"PASSED": 1}
    cluster.campaigns.archive("lifecycleverify", run_id="run-life")
    with pytest.raises(InvalidArgument, match="restore"):
        cluster.campaigns.verify("lifecycleverify", run_id="run-life")
    cluster.campaigns.restore("lifecycleverify", run_id="run-life")
    assert cluster.campaigns.verify("lifecycleverify", run_id="run-life")["counts"] == {"PASSED": 1}


def test_named_pilot_preflight_is_not_production_validation(cluster, sandbox: Path) -> None:
    (sandbox / "run.sh").write_text("#!/bin/sh\ntrue\n")
    source = {
        "schema": 1,
        "name": "pilotcase",
        "host": "local",
        "workspace": {
            "remote_root": str(sandbox),
            "output_root": str(sandbox / "production"),
        },
        "inventories": {
            "items": {
                "key": ["subject", "session"],
                "rows": [{"subject": "001", "session": "01"}, {"subject": "002"}],
            }
        },
        "stages": {
            "analysis": {
                "foreach": "items",
                "script": "run.sh",
                "outputs": [
                    {
                        "name": "result",
                        "alternatives": [
                            {
                                "path": "sub-{subject}/ses-{session}/result.txt",
                                "when_present": ["session"],
                            },
                            {
                                "path": "sub-{subject}/result.txt",
                                "when_absent": ["session"],
                            },
                        ],
                        "min_bytes": 1,
                    }
                ],
            }
        },
        "pilots": {
            "small": {
                "inventory": "items",
                "select": {"subject": ["001", "002"]},
                "output_root": str(sandbox / "pilot"),
            }
        },
    }
    definition = compile_campaign(source, base_dir=sandbox)
    cluster.campaigns.start(definition, run_id="run-production")
    sessioned = sandbox / "pilot" / "sub-001" / "ses-01" / "result.txt"
    sessionless = sandbox / "pilot" / "sub-002" / "result.txt"
    sessioned.parent.mkdir(parents=True)
    sessionless.parent.mkdir(parents=True)
    sessioned.write_text("ok\n")
    sessionless.write_text("ok\n")

    preflight = cluster.campaigns.preflight(definition, against="small")
    assert preflight["result"] == "PASSED"
    assert preflight["sections"]["pilot"]["production_evidence"] is False
    assert (
        cluster.campaigns.require_preflight(definition, against="small")["receipt_id"]
        == preflight["receipt_id"]
    )
    production = cluster.campaigns.status("pilotcase", run_id="run-production", include_units=True)
    assert {unit["validation"]["state"] for unit in production["units"]} == {"NOT_RUN"}

    (sandbox / "run.sh").write_text("#!/bin/sh\necho changed\n")
    changed = compile_campaign(source, base_dir=sandbox)
    with pytest.raises(InvalidArgument, match="no current passing preflight"):
        cluster.campaigns.require_preflight(changed, against="small")
