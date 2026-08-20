# Changelog

## 0.2.0 (in progress)

v2 "workflow layer" — see docs/plans/v2-plan.md.

- Project sync: `rslurm sync`/`projects` (rsync push/pull with excludes, dry-run, size guard,
  double-opt-in `--delete`, sync marker); MCP `sync`/`projects`. Uses the transport's ControlMaster.
- Remote editing: stub `edit`/`diff` ops, `rslurm edit`/`diff`, MCP `edit`/`diff` (exact-string
  replacement with occurrence checks; whole-file write no longer needed for small changes).
- Cluster knowledge: per-host `notes` + submit `templates` (preamble/epilogue, `inherit`); auto-learned
  policy notes from sbatch rejections; `rslurm notes`/`templates`/`submit --template`.
- Failure triage: `rslurm diagnose JOB` / MCP `diagnose` — merged status + script + log tails + a
  verdict and hints (OOM, timeout, node failure, missing module/path, pending reasons).
- Agent guidance: `rslurm agent-guide`, MCP resource `remoteslurm://guide`; MCP tool sets
  `REMOTESLURM_MCP_TOOLS=core|all` (default core).
- Job arrays: `submit --array`/`--dependency`; aggregated array status (`87✓ 10▶ 3✗`),
  per-task `status --tasks`, `%A/%a`-aware `output`, whole-array or per-task `cancel`.
- Parameter sweeps: `rslurm sweep -P k=v1,v2 …` / `Cluster.sweep` / MCP `sweep` — one array job
  with a params table injected as `RS_PARAM_*`; failed tasks' params shown in status/diagnose.
- Faster `jobs()`: batched `sacct` (allocations-only by default; steps opt-in).
- Cancellable long ops: the stub runs `run`/`srun`/`sbatch` in a separate pool (so `ping`/`ls`
  stay instant), a `cancel` op kills the process group, and a client-side `run` timeout or Ctrl-C
  cancels the remote process instead of leaking it. Request ids are client-generated (survive the daemon).
- Compute-node runs: `rslurm run --compute` / `Cluster.run(compute=True)` / MCP `run(compute=True)`
  execute on an allocated node via `srun` (with `--template`/partition/time/cpus/mem/gpus and a
  queue-wait timeout); distinguishes "still queued" from "ran".
- Safety rails: `allow_run = false | "safe"` (safe = argv-only, executable allow-list, no `bash -c`);
  `protected_paths` block write/edit/rm/put/sync-delete unless forced; `confirm` list gates rm/cancel
  behind an explicit confirmation; recursive `rm` refuses `$SCRATCH`/`$PROJECT` roots and shallow paths.
- Queue intelligence: `rslurm queue` (partitions with idle nodes, my accounts/QOS + limits,
  fair-share, my pending jobs with scheduler start estimates) and `rslurm quota` (Alliance
  `diskusage_report` via `quota_command`, else `df -h`); `Cluster.queue_info`/`estimate_start`/
  `quota`; MCP `queue_info`/`quota` (in the `all` set). `diagnose` now adds the start estimate to a
  PENDING verdict. Parsers tolerate missing tools/columns (a missing `sshare`/`sacctmgr` degrades
  to empty, never an error).
- Watch & notifications: `rslurm watch JOB… [--all] [--notify] [--poll]` — a foreground loop that
  prints state transitions, records terminal events to `<state>/<host>/events.jsonl`, and fires a
  desktop notification (`notify_command`; macOS/Linux defaults) on each finish (exit 0 only if all
  COMPLETED). `rslurm events` drains unseen events (byte-offset cursor) or `--all`/`--since`; MCP
  `events`. MCP `wait(job_id, timeout)` is a *bounded* poll (cap 300 s) returning `terminal:false`
  on timeout — agents loop as needed (in the core set).
- Housekeeping: `JobRegistry.prune` drops long-finished records (kept once/hour via a `last_pruned`
  stamp in the registry file); `jobs()` prunes opportunistically; `rslurm jobs --prune` forces it,
  `rslurm forget JOBID` removes one. `rslurm clean [--dry-run] [--older-than-days]` removes generated
  scripts/sweeps older than the cutoff. The stub bootstrap now deletes superseded `stub-*.py` (other
  shas) from the install dir.

## 0.1.0 — 2026-08-20

Initial release: persistent-ssh transport (ControlMaster, MFA-safe), stdlib-only remote stub,
bounded filesystem/Slurm operations, job registry, CLI (`remoteslurm`/`rslurm`), MCP server,
local session daemon. Verified live on trillium.alliancecan.ca (Slurm 25.11).
