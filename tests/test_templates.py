"""Notes, submit templates and learned-notes (WP-C C1)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import remoteslurm.jobs as jobs_mod
from remoteslurm.config import HostConfig, Template
from remoteslurm.errors import ConfigError, InvalidArgument, SlurmError
from remoteslurm.jobs import read_learned_notes


@pytest.fixture(autouse=True)
def _isolated_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    d = tmp_path / "rs-state"
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(d))
    return d


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs_mod.time, "sleep", lambda *_: None)


def registry_jobs(state_dir: Path) -> dict[str, dict]:
    data = json.loads((state_dir / "local" / "jobs.json").read_text())
    return {j["job_id"]: j for j in data["jobs"]}


def fake_state(sandbox: Path) -> dict:
    return json.loads((sandbox / ".fakeslurm.json").read_text())


# -- config parsing -------------------------------------------------------------------------
def test_config_parses_notes_and_templates() -> None:
    hc = HostConfig.from_dict(
        "mycluster",
        {
            "ssh": "mycluster",
            "notes": "walltime >= 15 min",
            "templates": {
                "cpu": {
                    "partition": "compute",
                    "time": "01:00:00",
                    "cpus_per_task": 4,
                    "preamble": "module load python\n",
                },
                "debug": {"inherit": "cpu", "partition": "debug", "time": "00:10:00"},
            },
        },
    )
    assert hc.notes == "walltime >= 15 min"
    assert set(hc.templates) == {"cpu", "debug"}
    cpu = hc.templates["cpu"]
    assert cpu.options == {"partition": "compute", "time": "01:00:00", "cpus_per_task": 4}
    assert cpu.preamble == "module load python\n"
    assert cpu.inherit is None
    assert hc.templates["debug"].inherit == "cpu"


def test_resolve_inherit_one_level() -> None:
    hc = HostConfig.from_dict(
        "h",
        {
            "templates": {
                "cpu": {"partition": "compute", "cpus_per_task": 4, "preamble": "module load py\n"},
                "debug": {"inherit": "cpu", "partition": "debug", "time": "00:10:00"},
            }
        },
    )
    t = hc.resolve_template("debug")
    assert t.options == {"partition": "debug", "cpus_per_task": 4, "time": "00:10:00"}
    assert t.preamble == "module load py\n"  # inherited from cpu
    assert t.inherit is None


def test_resolve_unknown_template_errors() -> None:
    hc = HostConfig.from_dict("h", {"templates": {"a": {"inherit": "nope"}}})
    with pytest.raises(ConfigError):
        hc.resolve_template("a")  # inherits a missing parent
    with pytest.raises(ConfigError):
        hc.resolve_template("missing")  # not defined at all


def test_resolve_cycle_errors() -> None:
    hc = HostConfig.from_dict("h", {"templates": {"a": {"inherit": "b"}, "b": {"inherit": "a"}}})
    with pytest.raises(ConfigError):
        hc.resolve_template("a")


def test_template_summaries_survive_cycle() -> None:
    hc = HostConfig.from_dict(
        "h",
        {"templates": {"a": {"inherit": "b", "time": "1:00:00"}, "b": {"inherit": "a"}}},
    )
    s = hc.template_summaries()  # best effort, must not raise
    assert set(s) == {"a", "b"}


# -- submit merge precedence ----------------------------------------------------------------
def test_submit_merge_precedence(make_cluster, sandbox, _isolated_state) -> None:
    tmpl = Template(name="cpu", options={"time": "02:00:00", "partition": "compute"})
    c = make_cluster(defaults={"time": "00:30:00"}, templates={"cpu": tmpl})
    # user script pins #SBATCH --time; template pins another; explicit kwarg wins over both.
    script = "#!/bin/bash\n#SBATCH --time=09:00:00\necho hi\n"
    job = c.submit(script, template="cpu", time="00:05:00")

    rec = registry_jobs(_isolated_state)[job.job_id]
    assert rec["meta"]["template"] == "cpu"
    assert rec["meta"]["options"]["time"] == "00:05:00"  # explicit beat template beat defaults
    assert rec["meta"]["options"]["partition"] == "compute"  # from the template
    assert "--time=00:05:00" in rec["sbatch_args"]
    assert "--time=02:00:00" not in rec["sbatch_args"]
    assert "--time=00:30:00" not in rec["sbatch_args"]
    # the command-line flag reached (and overrode the #SBATCH directive in) FakeSlurm
    assert fake_state(sandbox)["jobs"][job.job_id]["time_limit"] == "00:05:00"


def test_submit_inherit_options_applied(make_cluster, _isolated_state) -> None:
    c = make_cluster(
        templates={
            "cpu": Template(name="cpu", options={"partition": "compute", "cpus_per_task": 4}),
            "debug": Template(name="debug", options={"partition": "debug"}, inherit="cpu"),
        }
    )
    job = c.submit("#!/bin/bash\necho hi\n", template="debug")
    opts = registry_jobs(_isolated_state)[job.job_id]["meta"]["options"]
    assert opts["partition"] == "debug"  # child override
    assert opts["cpus_per_task"] == 4  # inherited from cpu


# -- preamble placement ---------------------------------------------------------------------
def test_template_preamble_wraps_generated_script(make_cluster, _isolated_state) -> None:
    tmpl = Template(
        name="cpu",
        options={"partition": "compute"},
        preamble="module load python\nsource venv/bin/activate\n",
        epilogue="echo done-epilogue\n",
    )
    c = make_cluster(templates={"cpu": tmpl})
    job = c.submit("#!/bin/bash\necho body\n", template="cpu")
    text = Path(registry_jobs(_isolated_state)[job.job_id]["script_path"]).read_text()
    lines = text.splitlines()
    assert lines[0] == "#!/bin/bash"
    assert lines[1].startswith("# remoteslurm: template=cpu")
    assert lines.index("module load python") < lines.index("echo body")
    assert lines.index("echo body") < lines.index("echo done-epilogue")


def test_template_without_preamble_still_adds_header(make_cluster, _isolated_state) -> None:
    c = make_cluster(templates={"cpu": Template(name="cpu", options={"partition": "compute"})})
    job = c.submit("#!/bin/bash\necho body\n", template="cpu")
    text = Path(registry_jobs(_isolated_state)[job.job_id]["script_path"]).read_text()
    assert "# remoteslurm: template=cpu" in text
    assert "echo body" in text


# -- path= submissions ----------------------------------------------------------------------
def test_template_preamble_refused_for_path(make_cluster, _isolated_state) -> None:
    tmpl = Template(name="cpu", options={"partition": "compute"}, preamble="module load python\n")
    c = make_cluster(templates={"cpu": tmpl})
    remote = c.write("~/myjob.sh", "#!/bin/bash\necho hi\n")["path"]
    with pytest.raises(InvalidArgument):
        c.submit(path=remote, template="cpu")
    # force_preamble applies the *options* only; the remote script is left untouched.
    job = c.submit(path=remote, template="cpu", force_preamble=True)
    rec = registry_jobs(_isolated_state)[job.job_id]
    assert "--partition=compute" in rec["sbatch_args"]
    assert Path(remote).read_text() == "#!/bin/bash\necho hi\n"


def test_template_options_only_for_path_without_preamble(make_cluster, _isolated_state) -> None:
    c = make_cluster(templates={"cpu": Template(name="cpu", options={"partition": "compute"})})
    remote = c.write("~/myjob2.sh", "#!/bin/bash\necho hi\n")["path"]
    job = c.submit(path=remote, template="cpu")  # no preamble -> allowed
    assert "--partition=compute" in registry_jobs(_isolated_state)[job.job_id]["sbatch_args"]


def test_submit_unknown_template_errors(make_cluster) -> None:
    c = make_cluster()
    with pytest.raises(ConfigError):
        c.submit("#!/bin/bash\necho hi\n", template="nope")


# -- learned notes --------------------------------------------------------------------------
def test_learned_notes_on_policy_reject_and_dedupe(cluster) -> None:
    reject = "#!/bin/bash\n# FAKESLURM_REJECT\necho no\n"
    with pytest.raises(SlurmError):
        cluster.submit(reject)
    notes = read_learned_notes("local")
    assert [n for n in notes if "Walltime must be" in n]

    # a second identical rejection must not duplicate the line
    with pytest.raises(SlurmError):
        cluster.submit(reject)
    notes2 = read_learned_notes("local")
    assert len([n for n in notes2 if "Walltime must be" in n]) == 1


def test_learned_notes_not_written_for_transient_error(cluster) -> None:
    with pytest.raises(SlurmError):
        cluster.submit("#!/bin/bash\n# FAKESLURM_REJECT_TRANSIENT\necho no\n")
    assert read_learned_notes("local") == []  # transient errors are never remembered


# -- info() surface -------------------------------------------------------------------------
def test_info_includes_notes_templates_learned(make_cluster) -> None:
    c = make_cluster(
        notes="be nice to the login node",
        templates={"cpu": Template(name="cpu", options={"partition": "compute"})},
    )
    info = c.info(refresh=True)
    assert info["notes"] == "be nice to the login node"
    assert "cpu" in info["templates"]
    assert info["templates"]["cpu"]["options"]["partition"] == "compute"
    assert info["learned_notes"] == []


# -- CLI ------------------------------------------------------------------------------------
def test_cli_templates_notes_and_guide(tmp_path, monkeypatch, capsys) -> None:
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        'default_host = "h"\n'
        "[hosts.h]\n"
        'ssh = "local"\n'
        'notes = "rule one"\n'
        "[hosts.h.templates.cpu]\n"
        'partition = "compute"\n'
        'time = "01:00:00"\n'
    )
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(cfg))
    from remoteslurm import cli

    assert cli.main(["templates", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert "cpu" in out["templates"]

    assert cli.main(["templates", "--show", "cpu", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["options"]["partition"] == "compute"

    assert cli.main(["notes", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["notes"] == "rule one"

    assert cli.main(["agent-guide"]) == 0
    assert "remoteslurm" in capsys.readouterr().out


def test_docs_agent_guide_matches_constant() -> None:
    from remoteslurm.guide import AGENT_GUIDE

    docs = Path(__file__).resolve().parents[1] / "docs" / "agent-guide.md"
    assert docs.read_text() == AGENT_GUIDE
