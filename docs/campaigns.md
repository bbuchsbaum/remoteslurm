# Campaign workspaces

Campaigns describe a finite, reproducible set of scientific work without turning remoteslurm into
a workflow engine. A definition fixes inventories, stages, dependency joins, output contracts,
validator identities, resources, environments, and named pilots. A run then accumulates scheduler,
artifact, validation, dependency, and freshness evidence on the remote cluster.

## Lifecycle

```bash
rslurm campaign plan campaign.toml
rslurm campaign preflight campaign.toml --against small
rslurm campaign start campaign.toml --run-id analysis-1
rslurm campaign apply study --run analysis-1 --require-preflight small
rslurm campaign adopt study --run analysis-1 --stage preprocess --array-job 12345
rslurm campaign status study --run analysis-1 --refresh
rslurm campaign verify study --run analysis-1 --stage preprocess
rslurm campaign retry study --run analysis-1 --stage preprocess \
  --state validation=failed --reason 'corrected validator' --dry-run
rslurm campaign drive study --run analysis-1 --max-passes 20 --interval 30
rslurm campaign close study --run analysis-1
```

`plan` compiles locally. `preflight` stores an immutable qualification receipt. `start` records a
definition and creates a run but does not submit. `apply` observes once and submits one bounded,
idempotent pass of eligible work. `adopt` binds existing work. `status --refresh` performs one
bounded observation pass and never runs a validator. `verify` may wait for declared filesystem
stability and then writes an immutable receipt for every selected production unit.

Open and closed runs can be refreshed and revalidated. Closing prevents new adoption or future
execution intent. Archived runs are read-only until restored, and restore returns them to closed.

## Output contracts

Each stage may declare reusable per-unit outputs:

```toml
[[stages.preprocess.outputs]]
name = "product"
kind = "file"                       # file | directory | symlink
alternatives = [
  { path = "sub-{subject}/ses-{session}/result.json", when_present = ["session"] },
  { path = "sub-{subject}/result.json", when_absent = ["session"] },
]
cardinality = "exactly_one"         # exactly_one | zero_or_one | one_or_more
min_bytes = 1
sha256 = true                       # collect a digest; a 64-character digest requires equality
stable_for = 5
settle_timeout = 60
settle_interval = 1
stable_dimensions = ["existence", "size", "mtime", "sha256"]

[[stages.preprocess.validators]]
kind = "command"
argv = ["python3", "scripts/check_result.py", "{output:product}"]
timeout = 300
files = ["scripts/check_result.py"]
```

The cardinality can instead use `exact_matches`, `min_matches`, and `max_matches`. Byte predicates
also have `exact_bytes`, `min_bytes`, and `max_bytes`. A path containing glob metacharacters is
expanded remotely, with `max_matches_scanned` capped at 1,024. One observation request accepts at
most 1,024 contracts and 2,048 returned matches. A regular-file hash is capped at 256 MiB and one
request hashes at most 1 GiB.

Validators use a literal argv array. `{field}` expands an inventory value and `{output:name}`
expands a resolved output path; neither invokes a shell. The executable is checked during preflight,
and execution follows the configured `allow_run` and `run_allowlist` policy. Each validator has a
maximum 3,600-second timeout and retains at most 8 KiB from each output stream.

## Settling and freshness

`status --refresh` records one metadata sample. An output that has not remained unchanged for its
`stable_for` interval is `SETTLING`; status returns immediately. `verify` is the operation that polls
at the declared interval, up to `settle_timeout`.

A passing receipt records the exact contract id, expanded paths, metadata and optional hashes,
settling evidence, validator evidence, remote control identity, and known limitations. A later
refresh compares current evidence with the receipt's artifact signature. Mutation changes
validation from `PASSED` to `STALE` and keeps the original receipt addressable.

## Preflight

Preflight has four named sections:

1. **Static** records the already-compiled schema, inventory, DAG, path, validator, and contract
   identities.
2. **Remote** checks workspace permissions, Slurm and validator tools, the resolved environment,
   and the durable campaign store.
3. **Fixtures** exercise missing, empty, partial, corrupt, changing, and permission-denied evidence
   through the same contract evaluator used for production.
4. **Pilot** optionally checks the exact contracts against a named inventory selection and output
   root. Every applicable session/layout alternative must be covered.

A preflight receipt is current only for the same definition, contract and validator identities,
pilot selection, output roots, tools, stub, protocol, Slurm version, and configured environment
observations. `CampaignManager.require_preflight()` is the fail-closed gate used by the execution
slice before scheduler mutation. Pilot validation is labeled `production_evidence = false` and does
not update any production run.

## Bounded selection and receipts

```bash
rslurm campaign verify study --run analysis-1 --stage preprocess --limit 100 --offset 0
rslurm campaign verify study --run analysis-1 --unit preprocess.0123456789abcdef01234567
rslurm campaign receipts study --kind preflight
rslurm campaign receipts study --kind validation --run analysis-1
```

One verification call accepts at most 500 units. Use `next_offset` to continue a larger selection.
Receipt files are canonical JSON named by their SHA-256 digest. Rewriting a receipt id with
different bytes is rejected by the remote store.

The Python surface is `cluster.campaigns.preflight(...)`, `verify(...)`, `receipts(...)`, and
`require_preflight(...)`. MCP exposes `campaign_preflight`, `campaign_verify`, and
`campaign_receipts` with the same limits.

## Managed execution

Execution policy is part of each immutable stage definition:

```toml
[stages.preprocess.execution]
mode = "array"             # single | array | pack
max_array_size = 1000
max_concurrent = 32

[stages.preprocess.resources]
time = "04:00:00"
cpus_per_task = 8
mem = "32G"
```

Packed stages also declare `units_per_allocation` and `max_processes`. The former controls how many
units share one allocation; the latter bounds simultaneous unit processes inside it. All execution
integers and job-group counts are capped. `plan` reports the resolved execution and resource
policy, while `apply --max-groups N` limits one mutation pass.

Before `sbatch`, remoteslurm writes the exact wrapper bytes, unit map, resources, working directory,
environment identity, and a unique attempt marker to the remote run. A run-scoped ledger reserves
every unit in the group in the same durable intent. Repeating `apply`, including with a narrower
unit selection, recovers that original whole group. If a lost response cannot be reconciled
uniquely from `squeue` and `sacct`, the unit remains `UNKNOWN`; it is never submitted again
automatically. Array-task scheduler rows are reconciled as evidence for their parent submission.

Arrays retain an exact, immutable index-to-unit map across deterministic split groups. Packed jobs
write atomic `started.json` and `finished.json` records plus bounded stdout/stderr paths for each
unit. Allocation state without a unit marker is labeled `allocation_only`; allocation completion
does not claim that every packed unit ran.

Retries require an explicit selector and reason. Previewing does not authorize execution:

```bash
rslurm campaign retry study --run analysis-1 --stage preprocess \
  --where subject=001 --state validation=failed --reason 'fixed input' --dry-run
rslurm campaign retry study --run analysis-1 --unit preprocess.0123456789ab \
  --reason 'fixed input' --apply
```

Failed, cancelled, unresolved, and invalid units are retryable. An `UNKNOWN` attempt additionally
requires `--accept-duplicate-risk`, as does a cancellation recorded only by remoteslurm's own
`scancel` request: refresh until `squeue` or `sacct` confirms it, or accept the risk that the
original job is still running. Packed units are judged by their per-unit markers, so a cancelled
allocation leaves unfinished units `UNKNOWN`, which always needs that acceptance. An authorization
approves replacing the attempt as it looked then: if a refresh shows the original job live again
or its output valid, the authorization is voided (`voided_retry_units` in the `apply` result and a
`retry_authorizations_voided` event) and nothing is submitted. A later failure needs a new retry.
Authorization consumption,
the expected previous attempt identity, replacement intent, and replacement unit reservations are
committed atomically before scheduler mutation. A concurrent caller follows that same replacement;
prior failed attempts and validation receipts remain in history. `drive` repeats bounded
apply/refresh passes while the client stays attached and never invents a retry or runs validation.

Cancellation previews by default and resolves the affected units, including every packed sibling
sharing the selected allocation:

```bash
rslurm campaign cancel study --run analysis-1 --stage preprocess
rslurm campaign cancel study --run analysis-1 --stage preprocess --apply
```

It remains available for active jobs retained by `close --allow-active`. Artifact and validation
evidence are preserved after cancellation. Python exposes `apply`, `retry`, `cancel`, and `drive` on
`cluster.campaigns`; the core MCP set exposes the corresponding `campaign_*` tools.
