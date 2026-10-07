# Activity and terminal observation qualification

The implementation is under local qualification. The immutable implementation candidate and
full-suite results will be recorded here after verification. Hosted CI has not run for this
candidate, and no new live jobs have been submitted for this work.

The automated specimens cover early acknowledgements before campaign projection, Python/MCP
causal equivalence, array and packed member identity, unavailable journal recording, passive
imports after agent loss, insertion/checkpoint interruption, one-read continuation across
restarts, immutable semantic context, bounded reader attachments, history/cursor gaps, stale
or conflicting scheduler identities, mixed pack outcomes, changed output signatures, unavailable
telemetry, persisted budgets/cadence, auth gaps, disable/remove and legacy watch compatibility.
The user has confirmed that file navigation now works in herdr-gpui. Headless terminal tests
cover mouse and keyboard navigation at 80×24 and 120×40.

Run the complete local matrix with:

```sh
.venv/bin/pytest -q
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
.venv/bin/mypy src/remoteslurm
uvx vermin -t=3.6- --no-tips --violations src/remoteslurm/stub.py
uv build
```

## Prepared live gate

[The opt-in test](../../tests/live/test_activity_observation.py) submits exactly two job groups:
an array of two successful one-CPU tasks and one one-CPU allocation with two sequential packed
members (one succeeds and one exits 7). Each allocation requests 256 MiB and five minutes on
the configured Nibi CPU account/partition. It writes only a unique
`/scratch/brad/rslurm-observation-<suffix>` fixture and its uniquely named campaign records.
It starts a separate daemon/socket/state namespace, lets the submitting processes exit, clears
that isolated local import cache, restarts its own daemon and checks truthful passive recovery,
separate scheduler/output/validation states and exact per-member failure. Remote fixtures remain
available as evidence. The test never starts an agent on the login node or prompts for MFA.

Read-only prerequisites checked: an existing Nibi SSH master is alive; configured account is
`def-brad_cpu`, partition is `cpubase_bycore_b2`; the helper reports working Slurm tools.
The live gate requires explicit submission authorization. The terminal workspace plan says
“This document does not authorize deployment, publication, or remote test submissions.”
Preparation and local tests therefore do not close A4.2, V4 or their parent epics.

After authorization, run from the immutable candidate:

```sh
REMOTESLURM_LIVE=1 REMOTESLURM_LIVE_HOST=nibi \
  REMOTESLURM_LIVE_CWD=/scratch/brad REMOTESLURM_LIVE_TIME=00:05:00 \
  REMOTESLURM_LIVE_EVIDENCE=/private/tmp/rslurm-observation-live.json \
  .venv/bin/pytest -q tests/live/test_activity_observation.py
```

The test fails before remote writes when source/tests differ from HEAD. A passing receipt must
retain candidate SHA, client/helper identities, daemon restart PIDs, allocation IDs and unit
outcomes. Queue/auth or source-identity failures leave the live gate open.
