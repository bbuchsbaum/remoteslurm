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
