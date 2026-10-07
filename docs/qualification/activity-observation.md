# Activity and terminal observation qualification

The qualified candidate is `8bc5a6aa8acee5fcdb58df62b84ee20f4d31c6e5`.
Its full suite passed **726 tests, 8 opt-in live tests skipped** in 313.42 seconds;
the authorized observation lifecycle passed separately on this same candidate.
Production source, package configuration, lockfile and CI workflow are identical to
`73f804495463ef14625e9047250eea3e55d9e85e`, which passed the preceding local matrix.
Build identity: `1:0.2.0:ac3cfa34ff30d5eb:25019df537b83a72`.
Ruff, formatting, mypy (44 source files), the helper's static Python 3.6 floor,
source archive and wheel builds passed. A fresh base-wheel environment imported CLI/MCP
without Textual and refused UI startup before configuration/authentication. Actual CLI PTYs
restored alternate screen and cursor on both `q` and Ctrl-C, with exit status zero.

Raw suite evidence: `/private/tmp/remoteslurm-motes-full-06.log` and its `.meta.json` sidecar.
A [copy of the raw log](evidence/activity-observation-local.log) and
[candidate-bound metadata](evidence/activity-observation-local.json) are checked in alongside
the live receipt and reproducible specimens.
Hosted CI has not run for this candidate. The authorized Nibi live lifecycle passed;
no publication or release was performed.
The preceding implementation candidate `761b941` passed 725 tests. Final installed-state
verification exposed a trailing-separator cache lookup mismatch; the final candidate fixes it
without normalizing remote `..` or unknown symlinks. Its 28 affected reader/file/UI checks passed,
then the complete matrix passed again. Installed command and daemon identities match the final
code, and `/scratch/brad` now resolves the root previously enrolled as `/scratch/brad/` offline.
The live specimen uses bounded owned-daemon shutdown and independent fixture output receipts.
Its CLI subprocess invokes `remoteslurm.cli` and its TOML campaign files are locally compiled
before submission. Earlier preparation attempts failed before any allocations were submitted.

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

## Passing live gate

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
The user explicitly authorized these two submissions. The test passed in 141.67 seconds
on 2026-10-06 (local date), against the candidate above. Its
[portable receipt](evidence/activity-observation-nibi.json) retains the exact build,
campaign/attempt identities, observations and independent scheduler/member/output receipts.

| Submission | Verified outcome |
| --- | --- |
| Array `23355244` | Tasks `_0` and `_1` completed with exit 0; both expected files contain `observation fixture\n`. |
| Pack `23355246` | Allocation `_0` failed with exit 1; exact member markers distinguish exit 7 from exit 0, and the successful member's file is present. |

The submitting processes exited before observation began. After clearing only the isolated
imports, daemon PID 255 recovered work; restart to PID 3467 retained/recovered all four units
and four `submission.recovered` events. All validation states remain `NOT_RUN`; the deliberately
failed member's artifact is `MISSING`, while the other three are `PRESENT`. Attention remains
advisory and grants no execution authority. The call budget temporarily suspended a scheduler
source; its visible gap resolved before the test converged.

The isolated daemon stopped successfully. The user's existing daemon PID 69264 and its
Nibi helper stayed alive. No jobs were resubmitted and no remote evidence was removed.
Live MFA expiry, laptop sleep and storage faults were not induced; those cases are covered
by automated fault tests, not this live receipt.

For a separately authorized reproduction, run from the immutable candidate:

```sh
REMOTESLURM_LIVE=1 REMOTESLURM_LIVE_HOST=nibi \
  REMOTESLURM_LIVE_CWD=/scratch/brad REMOTESLURM_LIVE_TIME=00:05:00 \
  REMOTESLURM_LIVE_EVIDENCE=/private/tmp/rslurm-observation-live.json \
  .venv/bin/pytest -q tests/live/test_activity_observation.py
```

Raw live evidence: `/private/tmp/remoteslurm-observation-live-04.log`, its `.meta.json`
sidecar and `/private/tmp/rslurm-observation-live-04.json`. A
[copy of the raw live log](evidence/activity-observation-live.log) is checked in.
The checked-in receipt also adds
independent read-only accounting and exact member-marker checks collected after the test.

A4.2, V4 and their WP8a/terminal-workspace parent epics are complete against these local,
terminal and live receipts. Earlier WP4/WP5 gates and later WP8b work are unaffected.
Closing these implementation issues does not assert hosted CI, publication or release.

The test fails before remote writes when source/tests differ from HEAD. A passing receipt must
retain candidate SHA, client/helper identities, daemon restart PIDs, allocation IDs and unit
outcomes. Queue/auth or source-identity failures leave the live gate open.
