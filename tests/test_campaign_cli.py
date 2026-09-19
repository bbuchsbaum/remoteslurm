from __future__ import annotations

import json
from pathlib import Path

from remoteslurm.cli import build_parser, main


def test_campaign_plan_cli_reports_stable_definition(tmp_path: Path, capsys) -> None:
    (tmp_path / "run.sh").write_text("#!/bin/sh\ntrue\n")
    manifest = tmp_path / "campaign.toml"
    manifest.write_text(
        """
schema = 1
name = "study"
host = "local"

[workspace]
remote_root = "/work/study"
output_root = "/work/study/outputs"

[inventories.items]
key = ["subject"]
rows = [{ subject = "001" }, { subject = "002" }]

[stages.analysis]
foreach = "items"
script = "run.sh"

[[stages.analysis.outputs]]
name = "result"
kind = "file"
path = "sub-{subject}/result.txt"
min_bytes = 1
""".strip()
        + "\n"
    )

    assert main(["campaign", "plan", str(manifest), "--json"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["campaign"] == "study"
    assert result["unit_count"] == 2
    assert len(result["definition_id"]) == 64


def test_campaign_nested_parser_accepts_global_flags_after_leaf(tmp_path: Path, capsys) -> None:
    (tmp_path / "run.sh").write_text("#!/bin/sh\ntrue\n")
    manifest = tmp_path / "campaign.toml"
    manifest.write_text(
        """
schema = 1
name = "study"
host = "local"
[workspace]
remote_root = "/work"
output_root = "/work/out"
[inventories.one]
key = ["id"]
rows = [{ id = "one" }]
[stages.only]
foreach = "one"
script = "run.sh"
""".strip()
        + "\n"
    )
    assert main(["campaign", "plan", str(manifest), "--json", "--limit", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["stages"][0]["name"] == "only"


def test_wp4_campaign_commands_have_bounded_parser_options() -> None:
    parser = build_parser()
    preflight = parser.parse_args(["campaign", "preflight", "campaign.toml", "--against", "small"])
    assert preflight.file == "campaign.toml"
    assert preflight.against == "small"
    verify = parser.parse_args(
        [
            "campaign",
            "verify",
            "study",
            "--run",
            "run-1",
            "--stage",
            "analysis",
            "--limit",
            "25",
            "--no-settle",
        ]
    )
    assert verify.limit == 25
    assert verify.no_settle is True


def test_wp5_campaign_commands_have_explicit_bounded_options() -> None:
    parser = build_parser()
    apply = parser.parse_args(
        ["campaign", "apply", "study", "--run", "run-1", "--max-groups", "25"]
    )
    assert apply.max_groups == 25
    retry = parser.parse_args(
        [
            "campaign",
            "retry",
            "study",
            "--state",
            "validation=failed",
            "--where",
            "subject=001",
            "--reason",
            "fixed validator",
            "--accept-duplicate-risk",
            "--apply",
        ]
    )
    assert retry.state == ["validation=failed"]
    assert retry.apply is True
    cancel = parser.parse_args(["campaign", "cancel", "study", "--all-active", "--apply", "--yes"])
    assert cancel.all_active is True and cancel.yes is True
    drive = parser.parse_args(
        ["campaign", "drive", "study", "--max-passes", "3", "--interval", "0"]
    )
    assert drive.max_passes == 3 and drive.interval == 0
