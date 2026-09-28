"""The agent guide: a short CLAUDE.md/AGENTS.md block for coding agents.

The text lives here as a module constant so both the CLI (``rslurm agent-guide``) and the
MCP resource ``remoteslurm://guide`` can serve it without shipping ``docs/``.
``docs/agent-guide.md`` is a byte-for-byte copy (a test enforces it).
"""

from __future__ import annotations

AGENT_GUIDE = """\
# Working with a remote Slurm cluster (remoteslurm)

You control a remote Slurm login node through the `remoteslurm` tools (MCP) or the
`rslurm` CLI. The connection is already set up; if a tool returns `error: not_connected`,
tell the user to run `remoteslurm connect <host>` in a terminal (MFA can't be done for them).

## Start here
- Call `info` first and **read its `notes`** — they hold the cluster's rules (walltime
  minimums, default account, which partitions exist, how to load software). Also read
  `templates` (ready-made submit configs), `projects` (configured sync roots), and
  `learned_notes` (policy rejections seen before).
- Don't guess account/partition/walltime. Use a template or the values from `notes`.

## Moving code and files
- Use `sync` to push a whole project (rsync, respects excludes), not repeated `put`s.
  `sync(dry_run=true)` first if unsure; `info.projects` lists what's configured (the optional
  `projects` tool returns the same contracts in the full MCP tool set).
- To change one file remotely, use `edit` (exact string replace) — do **not** `read` the
  whole file and `write` it back. `diff` (full tool set) checks a remote file against what you
  expect.
- CLI `put LOCAL HOST:REMOTE` and `get HOST:REMOTE LOCAL` handle individual files, directories,
  and tarballs; large files/directories use rsync. `sync(direction="pull")` retrieves a configured
  project. `pack` schedules compute commands; it does not create or transfer archives.

## Running jobs
- Use `ensure` for a single job whose result must survive a lost client and be checked before
  reuse. Repeat the same manifest object to recover it. Treat `VERIFIED` as the result state;
  `COMPLETED` is only scheduler completion. Never retry `UNKNOWN` unless the user explicitly accepts
  possible duplicate execution.
- Submit with `submit`. Prefer a **template**: `submit(template="cpu", script=...)` fills in
  options and prepends the module-load/venv preamble. Give either `script` (content) or
  `path` (an existing remote script), never both. If submission returns `recorded: false`, Slurm
  accepted the job: do not submit again; run `adopt(job_id=...)` to recover its local record.
- Use `pack` for independent shell commands that should share one or more one-node allocations.
  `max_processes` caps GNU Parallel children inside each allocation; `max_concurrent` separately
  throttles array allocations. GNU Parallel must be available in the template's job environment.
- Don't run heavy work through `run` — that's the login node. `run` is for quick checks
  (`squeue`, `ls`, `git`), and only when the host allows it.
- For login-node work that must keep running after the call (a server, an install, a setup
  script), use `run(detach=True)`: it returns `{pid, log}` at once and survives the connection.
  Check it with `proc_status`/`proc_tail`, stop it with `proc_kill`, and block with
  `wait(pid=...)` — or `wait(pid=..., pattern="READY")` to return as soon as its log prints a
  marker. Don't put `&` in a plain `run`: the call returns ~2 s after the command exits
  (`lingering: true`) and the background process survives only as long as this session.
- Keep each call under ~25 min: `run` timeouts are capped at the per-call limit
  (`REMOTESLURM_MCP_MAX_CALL`, default 1500 s), and a `compute=True` run
  must fit its `queue_timeout` + `time` in that. For longer work use `submit` or
  `run(detach=True)` for login-node work. For a Slurm cohort, use
  `watch(job_ids=[...], notify=True)` and retain its watch_id. This returns immediately; the
  daemon watches until all jobs finish or any job fails. `watch(action="status", watch_id=...)`
  reads the saved result and observer health locally; `action="stop"` unsubscribes without
  cancelling jobs. Notifications are desktop delivery; agent wakeup needs client integration.
- A compute queue deadline is enforced separately from runtime. On timeout/cancellation,
  remoteslurm checks cleanup of its owned allocation. If `cleanup.confirmed` is false, reconcile
  `job_ids`/`allocation_name` before resubmitting. Missing evidence is not successful cleanup.
- Submit creates fixed stdout/stderr parent directories before sbatch. Put Slurm substitutions
  such as `%j` in filenames; unresolved substitutions in directory components are refused.

## Campaigns (many units across stages)
- A campaign TOML defines inventories, stages, output contracts, and validators. Use the
  `campaign_*` tools when the user needs to know which of many units produced valid outputs.
- `campaign_start` creates a run and submits nothing. `campaign_adopt` binds jobs or outputs that
  already exist. `campaign_apply` submits one bounded pass; `campaign_drive` repeats apply/refresh
  while attached. `campaigns(name=..., refresh=True)` observes only and never submits.
- Only `campaign_verify` writes production validation receipts. Report scheduler completion,
  output presence, and validation separately; pilot receipts are not production validation.
- Retries need a selector and a `reason`; preview with `dry_run=True`. `UNKNOWN` work also needs
  `accept_duplicate_risk=True` — only on the user's explicit instruction. `campaign_cancel`
  previews unless `apply=True`.

## After a job finishes (or won't start)
- Use `diagnose <job_id>` instead of manually reading logs. It returns a plain-English
  `verdict` (out-of-memory, timeout, missing module, cancelled, pending-reason, …), concrete
  `hints`, stderr/stdout tails, bounded `log_errors` excerpts with line numbers, the sacct steps,
  and the sync marker. `scan_truncated` means the error scan did not cover the entire log.
- To check state, call `jobs` (a compact page) or `jobs(job_id=...)` (one full record). Filter by
  `name` (glob), `since` (submission time, ISO/epoch), `states`, or `job_ids`; `fields` selects
  fields. Follow `next_offset` while `has_more`. Listings default to 50 rows and 32 KiB, with
  array counts and a failure sample. `compact=False` remains byte-bounded. Each record has
  `terminal` (done?), `state`, `exit_code`, `reason`. For one job, `jobs(job_id=..., usage=True)`
  adds normalized live `sstat` or terminal `sacct` telemetry; submit may persist a bounded
  `file_count` progress observer. MCP `wait` is bounded and returns `terminal:false` on timeout;
  it does not create a background watcher. When
  `registry_available` is false, the result contains scheduler-visible data without local history.

## Rules of thumb
- Read `notes` before submitting; obey the cluster's walltime/account/partition rules.
- `sync`, not `put`. `edit`, not read+write. `diagnose`, not log spelunking. Templates, not
  hand-tuned flags.
- Before a long or unattended stretch, call `connection`; if it carries a `warning`, ask the
  user to reconnect first.
- Paths are on the *cluster*, not your laptop. `~` and configured variables such as `$WORK`
  expand remotely.
"""
