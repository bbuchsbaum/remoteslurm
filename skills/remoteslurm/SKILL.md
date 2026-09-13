---
name: remoteslurm
description: Operate Slurm clusters through remoteslurm MCP tools or rslurm. Use for remote cluster files, project sync, command execution, batch or packed jobs, monitoring, and diagnosis.
---

# remoteslurm

Use remoteslurm's structured MCP tools when available; otherwise use the `rslurm` CLI with
`--json` for programmatic results. Prefer its bounded operations over hand-written SSH. Keep
cluster-specific paths, accounts, partitions, modules, and policy in user configuration.

## Orient when the task needs it

- Before the first submission, compute allocation, project sync, or site-policy decision, call
  `info` and use its `notes`, `templates`, `projects`, environment values, and `learned_notes`.
  A bounded read of an already-known path does not require a fresh inventory.
- On a structured error, use its `action`. For `not_connected` or `auth_required`, call
  `connection` and give the user its exact connect command; do not attempt MFA for them.
- Check `connection` before long or unattended work when the site enforces a session lifetime.
  Act on an expiry warning before starting.

## Load only the relevant workflow

- For files, synchronization, execution, submission, packed jobs, monitoring, or diagnosis, read
  [references/operations.md](references/operations.md).
- For installation, host configuration, SSH/MFA, MCP registration, or `doctor` failures, read
  [references/setup.md](references/setup.md).

Do not load both references unless the request spans both. For ordinary development of the
remoteslurm package, inspect the repository normally; load these operational details only when
the change depends on cluster behavior.

## Finish the requested outcome

Submitting a job is not completion when the user asked for results. Continue through appropriate
monitoring, diagnose in-scope failures, and verify the expected remote state or output. Stop after
submission when that is the requested outcome, or when continuing needs a new user decision,
credentials, materially different resources, or authorization outside the original task.
