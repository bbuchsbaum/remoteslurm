from __future__ import annotations

from pathlib import Path

import pytest

from remoteslurm.campaigns.spec import compile_campaign
from remoteslurm.errors import InvalidArgument


def campaign_value(tmp_path: Path) -> dict:
    (tmp_path / "step.sh").write_text("#!/bin/sh\ntrue\n")
    (tmp_path / "finish.sh").write_text("#!/bin/sh\ntrue\n")
    return {
        "schema": 1,
        "name": "study",
        "host": "cluster",
        "workspace": {
            "remote_root": "$WORK/study",
            "output_root": "$WORK/study/derivatives",
        },
        "inventories": {
            "participants": {
                "key": ["subject", "session"],
                "rows": [
                    {"subject": "002"},
                    {"subject": "001", "session": "01"},
                ],
            }
        },
        "environments": {},
        "stages": {
            "preprocess": {
                "foreach": "participants",
                "script": "step.sh",
                "outputs": [
                    {
                        "name": "product",
                        "kind": "file",
                        "alternatives": [
                            {
                                "path": "sub-{subject}/ses-{session}/result.json",
                                "when_present": ["session"],
                            },
                            {
                                "path": "sub-{subject}/result.json",
                                "when_absent": ["session"],
                            },
                        ],
                    }
                ],
            },
            "finish": {
                "foreach": "participants",
                "script": "finish.sh",
                "needs": [
                    {
                        "stage": "preprocess",
                        "on": ["subject", "session"],
                        "require": "verified",
                    }
                ],
            },
        },
    }


def test_compile_is_row_order_independent_and_resolves_optional_outputs(tmp_path: Path) -> None:
    value = campaign_value(tmp_path)
    first = compile_campaign(value, base_dir=tmp_path)
    value["inventories"]["participants"]["rows"].reverse()
    second = compile_campaign(value, base_dir=tmp_path)

    assert first.definition_id == second.definition_id
    assert len(first.units) == 4
    outputs = {unit.keys["subject"]: unit.outputs for unit in first.units if unit.outputs}
    assert outputs["001"]["product"].endswith("sub-001/ses-01/result.json")
    assert outputs["002"]["product"].endswith("sub-002/result.json")
    finish = [unit for unit in first.units if unit.stage == "finish"]
    assert all(len(unit.dependencies) == 1 for unit in finish)


def test_script_bytes_are_part_of_definition_identity(tmp_path: Path) -> None:
    value = campaign_value(tmp_path)
    before = compile_campaign(value, base_dir=tmp_path)
    (tmp_path / "step.sh").write_text("#!/bin/sh\necho changed\n")
    after = compile_campaign(value, base_dir=tmp_path)
    assert before.definition_id != after.definition_id


@pytest.mark.parametrize(
    ("change", "path"),
    [
        (lambda value: value.update({"surprise": True}), "campaign"),
        (
            lambda value: value["inventories"]["participants"]["rows"].append(
                {"subject": "001", "session": "01"}
            ),
            "inventories.participants",
        ),
        (
            lambda value: value["inventories"]["participants"]["rows"].append(
                {"subject": "bad/value"}
            ),
            "stages.preprocess.outputs[0]",
        ),
    ],
)
def test_compile_fails_closed_with_location(tmp_path: Path, change, path: str) -> None:
    value = campaign_value(tmp_path)
    change(value)
    with pytest.raises(InvalidArgument, match=path.replace("[", r"\[")):
        compile_campaign(value, base_dir=tmp_path)


def test_dependency_cycle_is_rejected(tmp_path: Path) -> None:
    value = campaign_value(tmp_path)
    value["stages"]["preprocess"]["needs"] = [{"stage": "finish", "on": ["subject", "session"]}]
    with pytest.raises(InvalidArgument, match="dependency cycle"):
        compile_campaign(value, base_dir=tmp_path)


def test_output_collisions_are_rejected(tmp_path: Path) -> None:
    value = campaign_value(tmp_path)
    value["stages"]["preprocess"]["outputs"][0] = {
        "name": "product",
        "path": "same.json",
    }
    with pytest.raises(InvalidArgument, match="output path collides"):
        compile_campaign(value, base_dir=tmp_path)


def test_environment_and_validator_file_bytes_change_identity(tmp_path: Path) -> None:
    value = campaign_value(tmp_path)
    (tmp_path / "lock.txt").write_text("locked=1\n")
    (tmp_path / "validate.py").write_text("print('ok')\n")
    value["environments"] = {"standard": {"declaration": {"lockfile": "lock.txt"}}}
    value["stages"]["preprocess"]["environment"] = "standard"
    value["stages"]["preprocess"]["validators"] = [
        {"kind": "command", "argv": ["python", "validate.py", "{output:product}"]}
    ]
    before = compile_campaign(value, base_dir=tmp_path)
    (tmp_path / "validate.py").write_text("print('changed')\n")
    after = compile_campaign(value, base_dir=tmp_path)
    assert before.definition_id != after.definition_id


def test_fifty_thousand_units_compile_within_the_declared_bound(tmp_path: Path) -> None:
    (tmp_path / "run.sh").write_text("#!/bin/sh\ntrue\n")
    definition = compile_campaign(
        {
            "schema": 1,
            "name": "large",
            "host": "cluster",
            "workspace": {"remote_root": "/work", "output_root": "/work/out"},
            "inventories": {
                "items": {
                    "key": ["id"],
                    "rows": [{"id": f"u{index:05d}"} for index in range(50_000)],
                }
            },
            "stages": {"only": {"foreach": "items", "script": "run.sh"}},
        },
        base_dir=tmp_path,
    )
    assert len(definition.units) == 50_000


def test_external_inventory_row_order_does_not_change_identity(tmp_path: Path) -> None:
    (tmp_path / "run.sh").write_text("#!/bin/sh\ntrue\n")
    inventory = tmp_path / "items.tsv"
    inventory.write_text("id\nb\na\n")
    value = {
        "schema": 1,
        "name": "ordered",
        "host": "cluster",
        "workspace": {"remote_root": "/work", "output_root": "/work/out"},
        "inventories": {"items": {"source": "items.tsv", "format": "tsv", "key": ["id"]}},
        "stages": {"only": {"foreach": "items", "script": "run.sh"}},
    }
    before = compile_campaign(value, base_dir=tmp_path)
    inventory.write_text("id\na\nb\n")
    after = compile_campaign(value, base_dir=tmp_path)
    assert before.definition_id == after.definition_id
    assert before.inventories[0].source_sha256 != after.inventories[0].source_sha256


@pytest.mark.parametrize(
    "execution",
    [
        {"mode": "unknown"},
        {"mode": "single", "max_concurrent": 2},
        {"mode": "array", "max_processes": 2},
        {"mode": "array", "max_array_size": 0},
        {"mode": "pack", "units_per_allocation": 10_001},
        {"mode": "pack", "surprise": 1},
    ],
)
def test_execution_policy_fails_closed(tmp_path: Path, execution: dict) -> None:
    value = campaign_value(tmp_path)
    value["stages"]["preprocess"]["execution"] = execution
    with pytest.raises(InvalidArgument, match="stages.preprocess.execution"):
        compile_campaign(value, base_dir=tmp_path)


def test_execution_policy_defaults_are_canonical(tmp_path: Path) -> None:
    value = campaign_value(tmp_path)
    value["stages"]["preprocess"]["execution"] = {
        "mode": "pack",
        "max_processes": 4,
    }
    definition = compile_campaign(value, base_dir=tmp_path)
    stage = definition.stage("preprocess")
    assert stage.execution == {
        "mode": "pack",
        "max_processes": 4,
        "units_per_allocation": 4,
        "max_array_size": 1000,
    }
