# Operate a cluster

Tool names below are MCP tools. With the CLI, use the corresponding `rslurm` command and add
`--json` when you need structured output. Optional tools marked *(all)* appear only when the MCP
server runs with `REMOTESLURM_MCP_TOOLS=all`; the CLI always has them.

## Files and projects

- Prefer bounded `ls`, `read`, and `grep`, plus `glob` *(all)*. Page or narrow large results.
- Change a remote file with `edit` (exact string replacement), not `read` followed by `write`.
  `diff` *(all)* compares a remote file with local content.
- Use configured `sync` projects for source trees. Dry-run when the change set or destination is
  uncertain. Treat `delete` and `force` as explicit escalations.
- `~` and variables such as `$WORK` expand in the remote login environment.
- Use CLI `put LOCAL HOST:REMOTE` and `get HOST:REMOTE LOCAL` for files, directories, and existing
  archives; directories and large files go through rsync. `sync(direction="pull")` downloads a
  configured project. `pack` schedules compute commands; it does not transfer archives.

## Choose an execution mode

| Work | Use |
|---|---|
| Short, light login-node check | `run` |
| Short interactive compute work | `run` with `compute=true` |
| Batch or substantial compute work | `submit` |
| One job whose result must survive a lost client and be validated before reuse | `ensure` |
| Independent commands sharing one-node allocations | `pack` |
| Parameter grid with per-task environment | `sweep` *(all)* |
| Many units across stages with output contracts and validation | campaign tools; see [campaigns.md](campaigns.md) |
| Login-node process that must outlive the call | `run` with `detach=true` |

Prefer named templates from `info`. Let host defaults and the template supply site policy, and
override resources only when the task requires it. Give `submit` either `script` (content) or
`path` (an existing remote script), never both. If `submit` returns `recorded: false`, Slurm
accepted the job but the local record was lost: do not submit again; call `adopt(job_id=...)`.

`pack` stores one shell command per line and runs them with GNU Parallel inside one-node,
one-task array allocations. `max_processes` caps child processes inside each allocation;
`batches` distributes commands across allocations; `max_concurrent` throttles how many
allocations Slurm runs at once. GNU Parallel must be available in the job environment. Size the
allocation for the combined CPU, memory, and GPU demand of its concurrent children.

Keep login-node work light. Do not put `&` in a normal `run`; use `detach=true` and keep the
returned PID and log path. Observe with `proc_status` or `proc_tail`, wait with bounded
`wait(pid=..., pattern=...)`, and use `proc_kill` only when stopping it is in scope.

A single MCP call is limited to about 25 minutes. If a compute call cannot fit its queue wait
plus walltime inside that limit, use `submit`. Timeout or cancellation results include
allocation identity and cleanup evidence: reconcile `job_ids`/`allocation_name` before
resubmitting when `cleanup.confirmed` is false. `submit` creates fixed log parent directories and
refuses Slurm substitutions in directory components; use `logs/%j.out`, not `%j/log.out`.

## Durable tasks with `ensure`

`ensure` takes a manifest (CLI: a local TOML file; MCP: an object with the same fields). Over
MCP, pass the script as `script_inline`, because `script` is resolved on the MCP server's local
filesystem. The manifest names remote `inputs`, `outputs`, a `validate` argv, and resources; the
inputs are hashed into the task identity.

- Repeat the identical manifest to recover, observe, or revalidate. It never resubmits an active
  or ambiguous attempt.
- `VERIFIED` is the result state: the validator passed and output fingerprints match.
  `COMPLETED` means only that Slurm finished.
- `retry=true` creates a new attempt after `FAILED`, `INVALID`, or `REJECTED`. `UNKNOWN` needs
  `retry_unknown=true`, which risks duplicate execution; use it only on the user's explicit
  instruction.

The manifest schema is documented at
https://github.com/bbuchsbaum/remoteslurm/blob/main/docs/durable-tasks.md.

## Observe and diagnose

- Use `jobs(job_id=...)` for one full record. Listings are compact, byte-bounded pages; filter by
  `name`, `since` (submission time), `states`, or `job_ids`, and follow `next_offset` while
  `has_more`. Arrays are one record keyed by base ID; pass `123_4` for one task.
- `jobs(job_id=..., usage=true)` samples normalized CPU, process, and memory telemetry from
  `sstat` while running or `sacct` after completion. A `progress` table
  (`{"kind": "file_count", "path": ..., "total": ...}`) counts matching output files; declare it
  on `submit` to persist it with the job. Progress counts are observations, not validation.
- Use bounded `wait(job_id=...)` for short waits (at most 300 s per call). A timeout returns
  `terminal: false`; call again or switch to a watch. A timed-out wait is not a background
  watcher.
- For long cohorts use `watch(job_ids=[...], notify=true)` and keep the returned `watch_id`. The
  local daemon keeps observing after the call and after the agent exits, and resumes after a
  daemon restart. `watch(action="status", watch_id=...)` reads the result and observer health;
  `action="stop"` stops observing without cancelling jobs. Notifications are desktop
  notifications; waking an agent needs client integration. Sleep or MFA loss causes visible
  observation gaps, and a stale observer heartbeat is not active monitoring.
- Use `diagnose(job_id)` when work fails or stays pending unexpectedly. Follow its verdict and
  hints before reading logs manually. `log_errors` gives bounded error excerpts; check
  `scan_truncated`.
- Use `job_output` *(all)* or CLI `output` when raw output is needed. For arrays, inspect the base
  summary, then diagnose a failing task ID.

## Respect boundaries

`needs_confirmation` means no action occurred. Reissue with confirmation only when the user's
request already authorizes that mutation; otherwise ask. Do not turn a protected-path or size
refusal into a forced operation without checking the target and scope.

Keep cancellation, deletion, sync, submission, and execution within the requested host, project,
paths, and jobs.
