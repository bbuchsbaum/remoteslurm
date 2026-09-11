# Changelog

## Unreleased

- MCP: cancelling a tool call (a client timeout or abort) now cancels the remote work it
  started — a login-node `run`, a `compute=True` srun (releasing its allocation), a `wait` —
  instead of leaving it to run out its own timeout; the stub also honours a cancel that arrives
  while the op is still queued. Side-effecting calls such as `submit` are left to finish, so a
  submitted job is still recorded. `run` and `sync` timeouts are capped at 1500 s
  (`REMOTESLURM_MCP_MAX_CALL`) so a call ends before a client's 30-minute abort, and a
  `compute=True` run whose queue wait + walltime could exceed that is refused with a hint.
  `Cluster.run(max_seconds=)` exposes the same check to library callers.
- A lingering run's background output is now drained into a `lingering_log` (up to 16 MiB;
  logs untouched for 7 days are pruned) instead of the pipes being closed, so a mistakenly
  backgrounded command keeps running as long as the stub session does, and
  `wait(path=..., pattern=...)` can watch it.
- Login-node `run` is now bounded in every case. It returns about 2 s after the command exits
  even if a background child still holds stdout/stderr (the result is flagged `lingering`, with
  a note), where it used to wait for the timeout. After a timeout kill it drains leftover
  output for at most 2 s; previously a `setsid` child holding the pipes blocked one of the stub's four
  slow-pool workers until that child exited. Non-streaming runs now use the streaming path's
  bounded-memory reader, so a timeout error carries the first `max_output` bytes, not the last.
- Detached runs: `run(detach=True)` / `rslurm run --detach` / MCP `run(detach=true)` start a
  command in its own session, with output appended to a log, and return `{pid, pgid, log}` at
  once. The exit status is recorded remotely, so it survives the connection. New
  `proc_status`, `proc_tail` and `proc_kill` (library, MCP core tools, and
  `rslurm proc status|tail|kill`); `proc_kill` signals the whole process group and escalates to
  SIGKILL after a grace period.
- Waiting on login-node state: `Cluster.wait_for(pid= | path= [, pattern=])`, MCP
  `wait(pid=… | path=…, pattern=…)`, and `rslurm wait --pid/--path/--pattern`. The remote side
  checks every 0.5 s and scans only newly appended log bytes; a timed-out pattern wait returns an
  `offset` to resume from.
- `connection` reports the ssh master's `connected_at` and `age`. With the new per-host
  `session_lifetime` it also reports `expires_at`/`remaining_seconds`, and a `warning` plus
  `action` once less than an hour remains. `connect` and `doctor` show the master's age.
- Made the public package cluster-neutral: the generated config, README, agent guide, and live
  test no longer contain a built-in site, account, partition, storage path, or walltime.
- Added per-host `env_vars`, `quota_paths`, and `protected_roots` so sites can expose their own
  storage conventions without teaching the core about `$SCRATCH`, `$PROJECT`, or other names.
- Added `quota_format = "raw" | "pairs" | "df"`. Custom quota commands now remain raw by default;
  sites opt into a generic parser explicitly.
- Removed the site-specific scratch fallback from remote stub installation. The portable fallback
  chain is now the configured `install_dir`, remote home cache, then owner-only `/tmp` storage.
- Made real-cluster tests generic and fully opt-in through `REMOTESLURM_LIVE_*` variables.

## 0.2.0 — 2026-08-20

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
  behind an explicit confirmation; recursive `rm` refuses configured storage roots and shallow paths.
- Queue intelligence: `rslurm queue` (partitions, my accounts/QOS/limits, fair-share, pending jobs
  with scheduler start estimates) and `rslurm quota`; MCP `queue_info`/`quota`. `diagnose` shows the
  start estimate for a PENDING job.
- Watch & events: `rslurm watch` (live state transitions, desktop notification on finish, exit code
  reflects success), `rslurm events` (drain "what finished while I was away"), bounded MCP `wait`.
- Housekeeping: registry auto-prune, `rslurm clean` for old generated scripts, superseded stubs
  removed on bootstrap.
- Live streaming (CLI): `rslurm run --stream` prints output as it arrives; `rslurm tail -f` uses a
  stub-side follow op. Stub protocol bumped to v2 (multi-frame); non-streaming behavior unchanged.
- Portability: `squeue -u <user>` (works on Slurm < 20.02), bootstrap hardening (BusyBox `head`
  → `dd` fallback, python3→python→module-load discovery), synthesized Slurm-20 parser fixtures,
  ProxyJump/jump-host support, macOS CI + a Python-3.7 stub-smoke lane.
- Streaming robustness: `follow`/`tail -f` runs in a dedicated stub pool (never starves
  run/srun/sbatch) and emits keepalive frames so an abandoned tail is reclaimed within ~20 s.
- Queue intelligence: `rslurm queue` (partitions with idle nodes, my accounts/QOS + limits,
  fair-share, my pending jobs with scheduler start estimates) and `rslurm quota` (a configured
  site command or `df -h` fallback); `Cluster.queue_info`/`estimate_start`/
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
- Portability (F4): `squeue`/`squeue --start` now query with `-u <user>` instead of `--me` (works on
  Slurm < 20.02). The bootstrap snippet detects non-GNU `head` (e.g. BusyBox) and falls back to
  `dd bs=1` so it never over-reads the protocol stream, and discovers the interpreter through a
  fallback chain (configured → `python3` → `python` → `module load python`). Parsers are tested
  against synthesized older-Slurm (~20.02) fixtures alongside recorded newer fixtures, including the old
  `ReqMem` `4Gn`/`4Gc` suffix form. Per-host `ssh_opts` (e.g. `ProxyJump`) flow through both the ssh
  transport and rsync. CI runs the test matrix on macOS as well as Linux and executes the stub under
  Python 3.7 (closest proxy for the 3.6 floor). Live validation on a second independent cluster
  remained pending.

## 0.1.0 — 2026-08-20

Initial release: persistent-ssh transport (ControlMaster, MFA-safe), stdlib-only remote stub,
bounded filesystem/Slurm operations, job registry, CLI (`remoteslurm`/`rslurm`), MCP server,
local session daemon. Verified live on the primary development cluster (Slurm 25.11).
