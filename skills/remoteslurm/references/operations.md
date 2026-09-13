# Operate a cluster

Use MCP tool names below when connected through remoteslurm MCP. With the CLI, use corresponding
`rslurm` commands and add `--json` when structured output is useful.

## Files and projects

- Prefer bounded `ls`, `read`, `grep`, and `glob`; page or narrow large results.
- Use `edit` for a small exact replacement and `diff` against known content.
- Use configured `sync` projects for source trees. Dry-run when the change set or destination is
  uncertain. Treat `delete` and `force` as explicit escalations.
- `~` and variables such as `$WORK` are evaluated in the remote login environment.

## Choose an execution mode

| Work | Use |
|---|---|
| Short, light login-node check | `run` |
| Short interactive compute work | `run` with `compute=true` |
| Batch or substantial compute work | `submit` |
| Independent commands sharing nodes | `pack` |
| Parameter grid with per-task environment | `sweep` |
| Login-node process that must outlive the call | `run` with `detach=true` |

Prefer named templates from `info`. Let host defaults and the template provide site policy;
override resources only when the task requires it. Give `submit` either script content or a
remote script path, never both.

`pack` stores one shell command per line and uses GNU Parallel inside one-node, one-task array
allocations. `max_processes` caps child processes inside each allocation; `batches` distributes
commands across allocations; `max_concurrent` throttles how many allocations Slurm runs at once.
GNU Parallel must be available in the job environment. Size the allocation for the aggregate CPU,
memory, and GPU demand of its concurrent children.

Keep login-node work light. Do not put `&` in a normal `run`; use detached execution and retain
the PID and log. Observe it with `proc_status` or `proc_tail`, wait with bounded `wait` calls, and
use `proc_kill` only when stopping it is in scope.

If a compute call cannot fit its queue wait plus walltime inside the reported limit, use `submit`.
Use detached runs for appropriate login-node work, not as a substitute for scheduled compute.

## Observe and diagnose

- Use `jobs(job_id=...)` and its `terminal` field for state. Use bounded `wait`; do not tight-poll.
- Use `diagnose(job_id)` when work fails or remains pending unexpectedly. Follow its scheduler,
  resource, log, and submission hints before reading logs manually.
- Use `job_output` or CLI `output` when raw output is needed. For arrays, inspect the base summary,
  then diagnose a failing task ID when task-level evidence is needed.

## Respect boundaries

`needs_confirmation` means no action occurred. Reissue with confirmation only when the user's
request already authorizes that mutation; otherwise ask. Do not convert a protected-path or size
refusal into a forced operation without checking the target and scope.

Keep cancellation, deletion, sync, submission, and execution within the requested host, project,
paths, and jobs. Report job IDs, remote paths, and terminal states precisely.
