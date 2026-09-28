# Run a campaign

A campaign is a TOML definition of a finite set of work units: inventories (for example
subjects × sessions), stages with dependencies, per-unit output contracts, validators, resources,
and named pilots. A *run* of that definition accumulates scheduler, artifact, validation,
dependency, and freshness evidence in a durable store on the cluster. Use a campaign when the
user has many units across one or more stages and needs to know which ones produced valid
outputs. For one job, use `submit` or `ensure` instead.

The definition schema is documented at
https://github.com/bbuchsbaum/remoteslurm/blob/main/docs/campaigns.md. Do not invent schema
fields; read an existing definition or that document first.

## Tools

The MCP campaign tools belong to the default `core` set. `campaign_start` and
`campaign_preflight` take the definition `file`; the others take the campaign `name` declared in
it. Pass `run_id` unless exactly one run is open, which includes every call after the run is
closed. The CLI uses `rslurm campaign <subcommand> NAME --run RUN`.

| Step | MCP | CLI | Mutates the scheduler? |
|---|---|---|---|
| Compile and inspect a definition | — | `campaign plan FILE` | no |
| Qualify the definition (and a pilot's existing outputs) | `campaign_preflight(file, against=...)` | `campaign preflight FILE --against PILOT` | no |
| Create a durable run | `campaign_start(file, run_id=...)` | `campaign start FILE --run-id ID` | no |
| Bind existing jobs or outputs | `campaign_adopt(name, stage, job_id / array_job_id / unit_jobs / output_only)` | `campaign adopt NAME --stage S --job / --array-job / --job-map / --output-only` | no |
| Observe once | `campaigns(name, refresh=true)` | `campaign status NAME --refresh` | no |
| List problem units | `campaign_failures(name)` | `campaign failures NAME` | no |
| Submit one bounded pass | `campaign_apply(name, max_groups=..., require_preflight=...)` | `campaign apply NAME --max-groups N --require-preflight [PILOT]` | yes |
| Repeat apply/refresh while attached | `campaign_drive(name, max_passes=..., interval=...)` | `campaign drive NAME` | yes |
| Validate outputs, write receipts | `campaign_verify(name, stage=...)` | `campaign verify NAME --stage S` | no (runs validators) |
| Read receipts or history | `campaign_receipts`, `campaign_events` | `campaign receipts`, `campaign events` | no |
| Authorize retries | `campaign_retry(name, reason, ...)` | `campaign retry NAME --reason R ...` | only with `apply` |
| Cancel active jobs | `campaign_cancel(name, ...)` | `campaign cancel NAME ...` | only with `apply` |
| Close, archive, restore | `campaign_lifecycle(name, action)` | `campaign close/archive/restore NAME` | no |

`campaign_start` and `campaign_preflight` read the TOML file from the MCP server's local
filesystem. Over MCP there is no `plan`; use the CLI or read `campaign_start`'s result.

## Usual sequence

1. `plan` the definition and check the unit count and resolved execution policy.
2. `preflight` the definition. It checks schema, remote tools, and permissions, and stores a
   receipt that `apply` can require. `against=PILOT` additionally validates outputs that already
   exist for that pilot selection; preflight never submits work, so pilot outputs must be
   produced beforehand.
3. `start` a run. This records intent and submits nothing.
4. `adopt` work that already ran or is running, so it is not submitted again.
5. `apply` with `require_preflight` to submit one bounded pass of eligible groups. Use `drive`
   to repeat apply/refresh passes while the client stays attached. Neither retries nor validates.
   An MCP `drive` call must finish within the per-call limit (about 25 minutes). Each pass
   also includes an apply and a refresh, so keep `max_passes × interval` well below it.
6. Refresh with `campaigns(name, refresh=true)` to observe progress. Refresh never submits,
   retries, cancels, or validates.
7. `verify` finished stages. Only verification writes production validation receipts.
8. Inspect `campaign_failures`, fix the cause, then retry explicitly (below).
9. `close` the run when done, and `archive` it when the user wants it read-only.

## Reading state

Each unit reports dependency, execution, artifact, validation, and freshness states separately.
Report them separately too:

- Scheduler `COMPLETED`, outputs present, and validation `PASSED` are three different claims.
- `SETTLING` means an output has not yet been stable for its declared interval. `verify` waits
  for settling; `status --refresh` does not.
- `STALE` means an artifact changed after it passed validation. The old receipt is kept.
- Pilot (preflight) receipts never count as production validation.
- `UNKNOWN` execution means a lost submission reply could not be matched to exactly one job.
  The unit is never resubmitted automatically.

## Retries and cancellation

- Retries need an open run, a selector (`stage`, `unit_id`, `where`, or `states`), and a
  `reason`. Only units whose execution is `FAILED` or `UNKNOWN`, or whose validation is `FAILED`,
  `STALE`, or `ERROR`, are retryable; cancelled units are not, so rerunning them needs a new run. Preview
  with `dry_run=true`. Without `apply`, a retry records authorization only; with `apply=true` it
  also runs one apply pass. Retrying `UNKNOWN` work also needs `accept_duplicate_risk=true`; set
  it only on the user's explicit instruction. Earlier attempts and receipts stay in history.
- `campaign_cancel` needs a selector: `stage` and/or `unit_id`, or `all_active` alone. It
  previews the affected jobs by default, including every packed sibling that
  shares an allocation. Cancel with `apply=true` only when the user asked for cancellation of
  that selection. If the result is `needs_confirmation`, nothing was cancelled; add
  `confirm=true` (CLI `--yes`) only under that same authorization.
- `campaign_lifecycle` actions are `close`, `archive`, and `restore`; archive and restore
  require `run_id`.
- Closing stops new submission, adoption, and retries; refresh and `verify` still work. A run
  with active or unresolved attempts closes only with `allow_active=true`, and cancellation
  stays available for those jobs.
- `archive` needs a closed run with no intended, pending, or running attempts, and
  `accept_unresolved=true` when attempts are `UNKNOWN`. Archived runs are read-only. `restore`
  returns a run to closed, not open.
