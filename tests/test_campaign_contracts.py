from __future__ import annotations

from pathlib import Path

import pytest

from remoteslurm.campaigns.contracts import evaluate_output, fixture_evidence, stage_contract
from remoteslurm.campaigns.spec import (
    compile_campaign,
    pilot_alternative_coverage,
    pilot_units,
)
from remoteslurm.errors import InvalidArgument


def observation(*matches: dict, path: str = "/out") -> dict:
    return {"path": path, "matches": list(matches), "truncated": False, "observed_at": 10.0}


def test_contract_failures_remain_distinct() -> None:
    results = fixture_evidence()
    assert all(item["passed"] for item in results)
    assert {item["expected_failure"] for item in results} == {
        "cardinality_exact",
        "min_bytes",
        "cardinality_min",
        "sha256",
        "unstable",
        "permission_denied",
    }

    duplicate = evaluate_output(
        {"path": "/out/*.txt", "kind": "file", "exact_matches": 1},
        observation(
            {"path": "/out/a.txt", "exists": True, "kind": "file", "size": 1},
            {"path": "/out/b.txt", "exists": True, "kind": "file", "size": 1},
        ),
        observed_epoch=10.0,
    )
    assert duplicate["state"] == "ERROR"
    assert duplicate["failure_codes"] == ["cardinality_exact"]

    optional = evaluate_output(
        {"path": "/out/optional.txt", "kind": "file", "required": False},
        observation(path="/out/optional.txt"),
        observed_epoch=10.0,
    )
    assert optional["state"] == "PRESENT"
    assert optional["passed"] is True


def test_settling_requires_one_unchanged_interval() -> None:
    contract = {
        "path": "/out/result",
        "kind": "file",
        "stable_for": 2,
        "settle_timeout": 5,
    }
    current = observation(
        {"path": "/out/result", "exists": True, "kind": "file", "size": 4, "mtime": 1}
    )
    first = evaluate_output(contract, current, observed_epoch=10.0)
    assert first["state"] == "SETTLING"
    second = evaluate_output(
        contract,
        current,
        prior_stability=first["stability"],
        observed_epoch=12.1,
    )
    assert second["state"] == "PRESENT"


def test_pilot_compiles_both_session_layouts_and_contract_identity(tmp_path: Path) -> None:
    (tmp_path / "run.sh").write_text("#!/bin/sh\ntrue\n")
    value = {
        "schema": 1,
        "name": "layouts",
        "host": "local",
        "workspace": {
            "remote_root": str(tmp_path),
            "output_root": str(tmp_path / "production"),
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
                "output_root": str(tmp_path / "pilot"),
            }
        },
    }
    definition = compile_campaign(value, base_dir=tmp_path)
    coverage = pilot_alternative_coverage(definition, "small")
    assert coverage[0]["missing"] == []
    paths = [unit.outputs["result"] for unit in pilot_units(definition, "small")]
    assert any("ses-01" in path for path in paths)
    assert any(path.endswith("sub-002/result.txt") for path in paths)
    assert len(stage_contract(definition.stages[0])["contract_id"]) == 64


def test_settling_contract_requires_a_sufficient_timeout(tmp_path: Path) -> None:
    (tmp_path / "run.sh").write_text("true\n")
    with pytest.raises(InvalidArgument, match="settle_timeout"):
        compile_campaign(
            {
                "schema": 1,
                "name": "badsettle",
                "host": "local",
                "workspace": {"remote_root": "/work", "output_root": "/work/out"},
                "inventories": {"one": {"key": ["id"], "rows": [{"id": "one"}]}},
                "stages": {
                    "only": {
                        "foreach": "one",
                        "script": "run.sh",
                        "outputs": [
                            {
                                "name": "result",
                                "path": "result.txt",
                                "stable_for": 2,
                                "settle_timeout": 1,
                            }
                        ],
                    }
                },
            },
            base_dir=tmp_path,
        )
