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
  `templates` (ready-made submit configs) and `learned_notes` (policy rejections seen before).
- Don't guess account/partition/walltime. Use a template or the values from `notes`.

## Moving code and files
- Use `sync` to push a whole project (rsync, respects excludes), not repeated `put`s.
  `sync(dry_run=true)` first if unsure; `projects` lists what's configured.
- To change one file remotely, use `edit` (exact string replace) — do **not** `read` the
  whole file and `write` it back. `diff` checks a remote file against what you expect.

## Running jobs
- Submit with `submit`. Prefer a **template**: `submit(template="cpu", script=...)` fills in
  options and prepends the module-load/venv preamble. Give either `script` (content) or
  `path` (an existing remote script), never both.
- Don't run heavy work through `run` — that's the login node. `run` is for quick checks
  (`squeue`, `ls`, `git`), and only when the host allows it.

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
- Paths are on the *cluster*, not your laptop. `~`/`$SCRATCH`/`$PROJECT` expand remotely.
"""
