---
name: remoteslurm
description: Operate Slurm clusters through remoteslurm MCP tools or the rslurm CLI. Use for remote cluster files, project sync, login-node commands, batch/packed/array jobs, durable verified tasks (ensure), multi-stage campaigns, job monitoring and telemetry, and failure diagnosis. Also use to install or configure remoteslurm, its SSH/MFA connection, or its MCP server.
---

# remoteslurm

Use remoteslurm's structured MCP tools when they are registered; otherwise use the `rslurm` CLI
with `--json` for programmatic results. Prefer its bounded operations over hand-written `ssh`.
Keep cluster-specific paths, accounts, partitions, modules, and policy in the user's
configuration, never in commands you invent.

If neither the MCP tools nor `rslurm` are available, read
[references/setup.md](references/setup.md) and install before doing anything else.

## Orient when the task needs it

- Before the first submission, compute allocation, project sync, or site-policy decision, call
  `info` and use its `notes`, `templates`, `projects`, environment values, and `learned_notes`.
  A bounded read of an already-known path does not require a fresh inventory.
- On a structured error, follow its `action`. For `not_connected` or `auth_required`, call
  `connection` and give the user its exact connect command. The user performs MFA in their own
  terminal; never attempt it for them.
- Check `connection` before long or unattended work when the site enforces a session lifetime.
  Act on an expiry warning before starting.
- For an enrolled project, read `workspace(project=..., host=...)` at the beginning or
  resumption of work. Keep `next_cursor` and use `after` at meaningful task boundaries;
  these are local cached reads, not a polling or wakeup mechanism. Treat stale observations,
  omitted sections, and history gaps explicitly; resnapshot when `resync_required` is true.
- Report the host/root and the second-terminal attach command:
  `rslurm -H HOST view PROJECT` (or `view --path REMOTE_ROOT`). Both terminals must share the
  config/state namespace. If no workspace is enrolled, continue with ordinary structured
  tools and explain how the user can open the viewer; cached reads do not enroll a workspace.
- `workspace_inspect` queues bounded `list`, `preview`, or `tail` reads; retain its request ID
  and read that receipt later. Monitoring must already be enabled. `activity_recorded: false`
  means local visibility failed, not that an accepted job should be resubmitted.
- Enroll a campaign with `rslurm -H HOST monitor watch --campaign NAME --run-id RUN`.
  `monitor_status` and CLI `monitor status --json` read local source freshness, budgets and gaps.
  Passive recovery never writes the remote ledger or campaign view; `projection_lag` means an
  explicit attached refresh is needed to reconcile that view. Historical attempts do not
  replace current retries, and imported validation does not authorize new execution.

## Load only the relevant reference

| Request | Read |
|---|---|
| Files, sync, `run`, `submit`, `pack`, `sweep`, `ensure`, monitoring, telemetry, diagnosis | [references/operations.md](references/operations.md) |
| A finite set of work units across stages, with output contracts, validation, and retries | [references/campaigns.md](references/campaigns.md) |
| Installation, host configuration, SSH/MFA, MCP registration, `doctor` failures | [references/setup.md](references/setup.md) |

Load a second reference only when the request spans both. For ordinary development of the
remoteslurm package itself, inspect the repository normally.

## Evidence rules

- Slurm `COMPLETED` means the scheduler finished. It is not a verified result. `ensure` reports
  `VERIFIED` and campaigns report validation separately; claim only what the evidence states.
- `UNKNOWN` means a submission could not be matched to a job without risking duplication. Never
  retry it unless the user explicitly accepts duplicate execution.
- `needs_confirmation` means nothing happened. Confirm only when the user's request already
  authorizes that mutation; otherwise ask.

## Finish the requested outcome

Submitting a job is not completion when the user asked for results. Continue through
appropriate monitoring, diagnose in-scope failures, and verify the expected remote state or
output. Stop after submission when that is the requested outcome, or when continuing needs a new
user decision, credentials, materially different resources, or authorization outside the
original task. Report job IDs, remote paths, and terminal states exactly.
