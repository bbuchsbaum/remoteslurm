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
  whole file and `write` it back. `diff` checks a remote file against what you expect.

## Running jobs
- Submit with `submit`. Prefer a **template**: `submit(template="cpu", script=...)` fills in
  options and prepends the module-load/venv preamble. Give either `script` (content) or
  `path` (an existing remote script), never both.
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
- Keep each call under ~25 min: `run` timeouts are capped at 1500 s, and a `compute=True` run
  must fit its `queue_timeout` + `time` in that. For longer work use `submit` or
  `run(detach=True)`, then loop on `wait` (≤300 s per call).

## After a job finishes (or won't start)
- Use `diagnose <job_id>` instead of manually reading logs. It returns a plain-English
  `verdict` (out-of-memory, timeout, missing module, cancelled, pending-reason, …), concrete
  `hints`, the stderr/stdout tails, the sacct steps, and the sync marker. Act on the hints.
- To check progress, call `jobs` (all) or `jobs(job_id=...)` (one). Each record has
  `terminal` (done?), `state`, `exit_code`, `reason`. Poll `jobs`; avoid tight `wait` loops
  (MCP `wait` is bounded and returns `terminal:false` on timeout — loop only if needed).

## Rules of thumb
- Read `notes` before submitting; obey the cluster's walltime/account/partition rules.
- `sync`, not `put`. `edit`, not read+write. `diagnose`, not log spelunking. Templates, not
  hand-tuned flags.
- Before a long or unattended stretch, call `connection`; if it carries a `warning`, ask the
  user to reconnect first.
- Paths are on the *cluster*, not your laptop. `~` and configured variables such as `$WORK`
  expand remotely.
