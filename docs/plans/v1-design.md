# remoteslurm — design & implementation plan (v1)

## Context

A local coding agent (Claude Code / Codex on a Mac) needs a fast, turnkey, robust window onto a remote Slurm login node: browse directories, read small files, grep, tail logs, submit/monitor/cancel jobs. Today the agent would have to hand-craft `ssh host '...'` strings and pay a full connection + Duo MFA round-trip per command, which is unusable.

The initial validation target used interactive MFA, Python 3.11, Slurm 25.11, shared home/storage,
and an SSH alias with ControlMaster/ControlPersist. The design must stay general: other clusters
may lack MFA, use different storage variables, have older Slurm/Python (3.6), or no
`squeue --json`.

Decisions already agreed with the user: Python; system OpenSSH + ControlMaster as transport (MFA forces this); a remote stub speaking JSON; core library → CLI → MCP server, all three in v1.

## Architecture

```
 local                                              remote login node
 ┌──────────────────────────────┐                   ┌──────────────────────────┐
 │ CLI (argparse)  MCP (FastMCP)│                   │ ~/.cache/remoteslurm/    │
 │        └────┬────┘           │  ssh -S <ctl sock> │   stub-<sha>.py          │
 │     Cluster (core, sync)     │ ──── exec ───────▶│  JSON-lines req/resp     │
 │     Session (1 per host)     │ ◀── stdin/stdout ─│  thread pool, no shell   │
 │     ssh transport / local tr.│                   │  scandir/read/grep/sbatch│
 └──────────────────────────────┘                   └──────────────────────────┘
```

### Transport (`transport.py`)
- All library-spawned ssh: `ssh -T -o BatchMode=yes -o ControlMaster=no -o ConnectTimeout=5 -o LogLevel=ERROR <alias> <cmd>`. BatchMode makes a Duo prompt fail fast instead of hanging; `ControlMaster=no` stops the library from becoming a master.
- Liveness: `ssh -O check` is necessary not sufficient (stale socket after laptop sleep). Real test is stub `ping` with short timeout. On ping timeout → `-O check`; OK → respawn stub; not OK → `-O exit` to clear stale socket, then: host `mfa=true` → raise `NotConnected(action="run: remoteslurm connect <host>")`; `mfa=false` → auto-establish master (`ssh -fN -o ControlMaster=yes`).
- `remoteslurm connect <host>`: interactive foreground `ssh -fN -o ControlMaster=auto -o ControlPersist=<cfg> <alias>` so the user does MFA once in their terminal. Works with the user's existing ssh config; if the alias lacks ControlPath, we pass our own (`~/.ssh/remoteslurm-%C`).
- `LocalTransport`: runs the stub via `subprocess` with no ssh. Used for the whole test suite and CI.

### Remote stub (`stub.py`, single file, stdlib only, Python ≥3.6 syntax)
- Launch in one round trip: `test -f $P || { cat > $P.tmp && mv $P.tmp $P; }; exec python3 -u $P` (stub source on stdin when missing; filename is content-hashed so local/remote skew is impossible by construction). Install dir: `~/.cache/remoteslurm`, then `/tmp/$USER`; a site may configure another directory locally.
- Handshake: first line `REMOTESLURM-READY <proto> <sha> <pyver>`; client discards everything before it (rc-file/module noise). Every response line is prefixed `\x1e` (RS); anything else is logged and skipped.
- Protocol: JSON-lines, `{"id", "op", "args"}` → `{"id", "ok", "result"|"error", "done"}`; streaming ops send multiple `done:false` frames. Stub serves with a small thread pool (4) + `cancel` op; traps EPIPE and exits quietly.
- Ops (all with hard caps + timeouts, argv lists only, never `shell=True`, paths `expanduser`+`realpath`, reject NUL):
  `ping, whoami/env, ls (scandir+stat, paged 200, next_token), stat, read (bytes/lines, offset incl. negative=tail, head/tail, binary detect via NUL in first 8KB, base64 payload, 64KB default cap), write (small, atomic), grep (regex, glob filter, 200 matches, skip >50MB), glob/find (bounded), run (argv or `bash -lc` opt-in, bounded stdout/stderr, timeout), sbatch (--parsable; script content or path; returns jobid + scontrol show job for stdout/stderr paths), squeue (delimited --format, not --json), sacct (--parsable2 -n), scontrol_job, scancel (only ids owned by `whoami`, verified first), sinfo (partitions summary)`.

### Core (`cluster.py`, `session.py`, `slurm.py`, `jobs.py`, `config.py`, `errors.py`)
- `Session`: one per host; writer lock + reader thread + `dict[id, Future]`; heartbeat ping every 30s; on EOF fails all in-flight futures with `SessionDied`; auto-respawn when master is alive.
- `Cluster(host)`: public sync API mirroring the ops; `run_ssh(cmd)` fallback for anything outside the stub. Thread-safe; MCP uses `asyncio.to_thread`.
- `Job` model: `job_id` (accepts `123`, `123_4`; hetero `123+1` parsed but unsupported), script path, stdout/stderr paths, submit time. `status()` merges squeue (live) → scontrol (MinJobAge window) → sacct (finished): `{state, reason, exit_code, elapsed, max_rss, node, stdout_path, stderr_path}`; while between squeue disappearance and sacct arrival → `state="COMPLETING?"`/`accounting_pending=true` (never "not found" in a grace window). Cluster-wide squeue rate limit (~10s cache). `wait(poll, backoff)`.
- Job registry: `~/.local/state/remoteslurm/<host>/jobs.json` so a fresh agent session can recover handles. Audit log of every `run`/`write`/`scancel` in the same dir.
- Config: `~/.config/remoteslurm/config.toml`:
  ```toml
  [hosts.mycluster]
  ssh = "mycluster"         # alias in ~/.ssh/config
  mfa = true
  account = "research"
  python = "python3"        # optional override
  allow_run = true
  ```
  Unknown host → treated as a bare ssh alias with defaults (turnkey for non-MFA systems).
- Error taxonomy (shared by CLI/MCP): `not_connected(action)`, `auth_required`, `timeout`, `not_found`, `permission`, `too_large`, `slurm_error(stderr)`, `session_died`, `invalid_arg`.

### CLI (`cli.py`, argparse, entry points `remoteslurm` and short alias `rslurm`)
`connect`, `doctor` (ssh config, master alive, python, Slurm version, home writable, stub install), `ls`, `cat`/`read`, `tail [-n] [-f]`, `grep`, `find`, `run`, `put`/`get` (small via stub; `--rsync` for big), `submit`, `jobs`/`status`, `wait`, `cancel`, `sinfo`, `config`, `mcp-config` (prints the `mcpServers` snippet). Paths as `host:path` or `--host`. `--json` on every subcommand (agent-friendly); human output otherwise.

### MCP server (`server.py`, FastMCP, stdio, entry point `remoteslurm-mcp`)
Eight tools, all returning dicts with `truncated`/`next_token` fields and "how to get more" hints:
`ls, read, grep, write, run, submit, jobs, cancel` (+ `connect_status` for the not-connected case). Host param defaults to `REMOTESLURM_DEFAULT_HOST`.

## Files (src layout, hatchling, uv — matching `codex-council-mcp` conventions)
```
pyproject.toml            # hatchling, py>=3.11 local, mcp[cli]>=1.8,<2, ruff, mypy, pytest
src/remoteslurm/{__init__,errors,config,transport,stub,session,cluster,slurm,jobs,cli,server}.py
src/remoteslurm/py.typed
tests/{conftest.py, test_stub_ops.py, test_session.py, test_slurm_parsing.py, test_jobs.py, test_cli.py, test_server.py, live/test_cluster.py}
tests/fakeslurm/{sbatch,squeue,sacct,scancel,scontrol}   # PATH-shadowed scripts driven by JSON fixtures
tests/fixtures/slurm/*.txt                                # recorded outputs (25.11 now; add others later)
README.md, docs/plans/v1-design.md (this plan, copied into repo), .github/workflows/ci.yml
```

## Milestones (each verified before the next)

1. **Scaffold + stub + local transport** — pyproject, stub with handshake/framing/`ping`/`ls`/`stat`/`read`/`write`/`grep`/`glob`/`run`; `LocalTransport`; `Session`. Verify: pytest over local transport (tmp dirs, binary files, truncation, concurrency with 20 pipelined requests, stub crash → `SessionDied`). `python3.6`-syntax check of stub via `ruff --target-version py36` / `vermin` if available.
2. **SSH transport + connect/doctor** — liveness state machine, stub bootstrap in one RTT, `remoteslurm connect/doctor`. Verify live on the configured validation host: `doctor` passes; a home-directory listing is fast after warm-up; kill stub remotely → auto-respawn; close the SSH master → clean `not_connected` error with action text.
3. **Slurm layer** — parsers (squeue/sacct/scontrol), `Job`, registry, status merge with accounting-lag grace, `wait`, `cancel` ownership check. Verify: parsing tests against fixtures + FakeSlurm; live: submit a short job on the configured host, poll to COMPLETED, read its `.out`, confirm registry persists across processes.
4. **CLI** — all subcommands, `--json`. Verify: CLI tests via local transport + FakeSlurm; live smoke script.
5. **MCP server** — 8 tools, caps, `asyncio.to_thread`, `mcp-config`. Verify: in-process FastMCP client tests; register in Claude Code and drive a real session (ls → read → submit → jobs).
6. **Docs + CI** — README (purpose, install, connect flow, tools list, safety defaults, troubleshooting, env knobs, validation commands), CI (lint + test matrix 3.11–3.13, `uv build`). Fresh-context review subagent pass before calling v1 done.

## Verification (end-to-end)
- `uv run --extra dev pytest` green; `ruff check && ruff format --check && mypy` clean.
- Live on an explicitly configured host (gated `REMOTESLURM_LIVE=1`): doctor → ls/read/grep latency numbers → short-job lifecycle → socket recovery.
- MCP: `remoteslurm mcp-config` snippet added to Claude Code; agent completes the ls/read/submit/status loop without hand-written ssh.

## Explicitly out of v1
Quotas/usage reporting, MCP streaming `follow`, sqlite registry, heterogeneous jobs, async transport, per-cluster job templates (easy follow-up), non-OpenSSH transports.

## Implementation notes (2026-08-20, post-review)

- **Added: local session daemon** (`daemon.py`). Measured: every new SSH channel on the primary validation cluster costs ~2 s
  even over a live master (PAM + rc files), so a per-invocation CLI would never be fast. The CLI now
  talks to a per-user unix-socket daemon that keeps stub sessions warm (auto-spawned, flock-guarded,
  idle exit 4 h). Warm CLI calls: ~150 ms. `--no-daemon` / `REMOTESLURM_NO_DAEMON` bypass it.
- Liveness is lazy rather than a background heartbeat: a session idle > 90 s is probed with a 10 s
  ping before reuse; any timeout checks `ssh -O check` and raises `not_connected` (with action) if the
  master is gone. Verified live: `ssh -O exit` -> exit code 3 with the connect instruction, no hang.
- The bootstrap snippet runs under `sh -c` (csh/tcsh login shells), install dirs must be owned by
  the user (`[ -O ]`, chmod 700).
- Job registry writes are merge-on-write under `flock` (multiple CLIs + MCP server share the file).
- `wait()` raises `slurm_error` for jobs unknown to squeue/scontrol/sacct instead of spinning.
- Jobs submitted from script content run with cwd `$HOME` unless `cwd` is given.
- Open (low severity, from review): no stub `cancel` op / separate pool for long `run`s; `%A/%a`
  output-path templates for array tasks; `scancel` ownership check when `squeue -j` fails; registry
  pruning; `squeue --me` needs Slurm >= 20.02.
