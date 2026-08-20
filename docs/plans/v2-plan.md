# remoteslurm v2 — workflow layer (items 1–12)

## Context

v1 (committed, verified live on trillium) delivers the plumbing: persistent-ssh transport, remote
stub, bounded filesystem/Slurm ops, CLI, MCP server, session daemon. The gap analysis showed the
remaining friction is at the *workflow* level: an agent still moves files by hand, rewrites whole
files to edit them, knows cluster rules only from memory, and needs 3–5 round trips to understand a
failed job. v2 closes those gaps so an agent can take a project from laptop to a finished, explained
Slurm job, then hardens the rough edges (long `run`s, protected paths, housekeeping, portability).

User decisions (2026-08-20): ship **all six work packages**; second cluster for portability =
**Nibi** (`nibi.alliancecan.ca`); trillium keeps **`allow_run = true`** (full shell, audited).
Verified on trillium: `diskusage_report`, `sshare -P`, `sacctmgr -P show qos` all available; GNU
coreutils `head` 9.3.

Invariants carried over from v1 (do not break): stub stdlib-only & py3.6 syntax; every op bounded
and paged; no shell in stub ops except the explicit `run`/`srun` escape hatches; logic lives in the
client — the daemon only multiplexes stub calls; structured errors with `action`; all features
testable locally with `LocalTransport` + FakeSlurm; live-verified on trillium (debug partition)
before a WP is called done. `allow_run`/safety rails are enforced client-side: the daemon socket is
same-user trust (documented).

## Order (revised after review)

| # | WP | Contents | Why here |
|---|---|---|---|
| 1 | **A** | project sync | biggest daily-workflow gap, fully client-side |
| 2 | **B** | `edit` / `diff` stub ops | small, high value, independent |
| 3 | **C** | cluster notes, templates, `diagnose`, agent guide | makes submits reproducible and failures explainable |
| 4 | **D1/D2** | arrays + sweeps | needs parser rewrite; no new stub machinery |
| 5 | **E1** | stub pool split + request ids + `cancel` (no streaming yet) | prerequisite for anything long-running |
| 6 | **D3** | `run --compute` via `srun` | only safe once cancel exists |
| 7 | **E2** | safety rails (protected paths, confirm flag, run modes) | before agents get `sweep`+`srun` in anger |
| 8 | **F3** | housekeeping (registry prune, batched sacct, script cleanup, stale stubs) | cheap, reduces drift |
| 9 | **F1** | queue intelligence (`queue`, `quota`, start estimates) | feeds `diagnose` PENDING hints |
| 10 | **F2** | `watch`/notifications (CLI foreground) + bounded MCP `wait` | no daemon logic |
| 11 | **E3** | streaming (`run --stream`, `follow`) — protocol v2 across stub/session/daemon | largest protocol change; last so everything else ships first |
| 12 | **F4** | portability: Nibi fixtures, older-Slurm fixtures, bootstrap/shell matrix, macOS CI | needs Nibi access (MFA) |

Each WP: implement → `uv run --no-sync pytest -q` + ruff + mypy + vermin(stub) green → live check on
trillium → fresh-context review subagent → fix → commit → README + CHANGELOG entry.

---

## WP-A — Project sync (item 1)

**Config** (`config.py`: new `ProjectConfig`; `HostConfig.projects: dict[str, ProjectConfig]`):
```toml
[hosts.trillium.projects.mvpa]
local   = "~/code/mvpa"
remote  = "$PROJECT/mvpa"          # expanded remotely
exclude = [".git", "__pycache__", "*.nii.gz", "results/"]   # rsync filter syntax
delete  = false                    # may --delete on push (still needs --delete on the call)
```
Resolution: `--project NAME` → else the project whose `local` contains CWD → else `config_error`
with an `action` listing projects. Built-in excludes: `.git/`, `.venv/`, `__pycache__/`, `*.pyc`,
`.DS_Store`, `.remoteslurm-sync.json`.

**Implementation** — new `src/remoteslurm/sync.py`, a *client-side* module (not a stub op and not a
`Cluster` method: rsync runs on the laptop). `sync(cluster, project, *, pull, dry_run, delete,
force) -> dict`:
- Remote path expansion via `cluster.run(["sh","-c",'eval "printf %s $1"',"_",path])` (helper moved
  from `cli._rsync` into `sync.py`; `put/get` reuse it). `cluster.mkdir(remote)` before push.
- Command: `rsync -az -s --itemize-changes --stats --filter=… [-n] [--delete] -e "ssh -o
  ControlMaster=no -o BatchMode=yes" SRC ALIAS:DST`. Alias comes from `cluster.transport.alias`
  (present for both direct and daemon-backed clusters). Requires local rsync ≥ 3.1 — `doctor` checks
  `rsync --version` and reports; macOS openrsync/2.6.9 → clear error with `brew install rsync`.
- Parse `--itemize-changes` (`>f.st......`, `cd+++++++++`, `*deleting`) into
  `{created, updated, deleted, files, bytes}`; parse failures degrade to `counts: null`, never an
  error. Guard: > `max_files` (50k) or > `max_bytes` (2 GB) on a non-dry-run push → `too_large` with
  action "run --dry-run / tighten excludes / --force".
- After a successful push write `<remote>/.remoteslurm-sync.json` via `cluster.write`
  `{pushed_at, local_git_rev, local_dirty, files, bytes}` (git info via `git rev-parse HEAD` and
  `git status --porcelain`, best effort). `diagnose` (WP-C) reads it.
- Ctrl-C kills rsync; `--timeout` default 1800 s.

**Surfaces:** CLI `rslurm sync [PROJECT] [--pull] [--dry-run] [--delete] [--force]`,
`rslurm projects`; MCP `sync(project=None, direction="push", dry_run=False, delete=False)`,
`projects()`. Both run rsync in the client process.

**Tests (first: the riskiest):** push → modify one file → push again; itemize parser reports exactly
one `updated` on rsync 3.x output, and `counts: null` on recorded 2.6.9 output. Then: e2e with a fake
`ssh` script (`exec "$@"` locally) against tmp trees — excludes, pull, dry-run has no side effects,
`--delete` double opt-in, guard trips, marker written with git rev. Live: push a fixture project to
`$SCRATCH/rs_sync_test`, pull back, marker present.

---

## WP-B — Remote `edit` / `diff` (item 2)

**Stub ops** (`stub.py`, bytes-in/bytes-out like `op_read`/`op_write`):
- `edit(path, old, new, expect=1, all=False)`:
  - refuse binary (NUL probe) and files > 8 MB; `old == ""` or `old == new` → `invalid_arg`.
  - Encode `old`/`new` as UTF-8; count byte occurrences. With `all=True`, replace every occurrence
    (`expect` ignored; returned `replacements` = count). Otherwise count must equal `expect`:
    `0` → `not_found` with `closest` = up to 3 near-matching lines (`difflib.get_close_matches` over
    lines, bounded); `>expect` → `invalid_arg` with the line numbers of each occurrence.
  - Preserve line endings by construction (byte replacement), preserve mode (`os.chmod` after
    `os.replace`) — and backport mode preservation to `op_write`. Atomic via
    `path.<pid>.tmp` + `os.replace` (document: inode changes, hard links not preserved).
  - Return `{path, replacements, first_line, preview}`; `preview` = unified diff of ±3 lines around
    the first change, ≤ 4 KB.
- `diff(path, content=None, content_b64=None, path_b=None, context=3, max_lines=500)` → unified diff
  (`difflib.unified_diff`), `truncated` flag, `identical` bool.

**Surfaces:** `Cluster.edit/diff`; CLI `rslurm edit PATH --old S --new S [--all] [--expect N]` and
`--old-file/--new-file` for multi-line; `rslurm diff PATH [LOCALFILE]`; MCP `edit`, `diff`. Audit
`edit`.

**Tests (first):** CRLF file without trailing newline, `old` occurs twice, `expect=1` → refused,
bytes identical, mode identical. Then 0/1/many/`all`, unicode, binary refusal, preview bound,
`diff` vs content and vs another file. vermin still ≥3.6. Live: edit a scratch file on trillium.

---

## WP-C — Cluster notes, templates, `diagnose`, agent guide (items 3 & 4)

### C1. Notes & templates
```toml
[hosts.trillium]
notes = """Walltime >= 15 min except on `debug` (max 1 h, 1 job). Default account rrg-brad.
Login nodes: no heavy compute. Software: `module load StdEnv/2023 python/3.11`."""

[hosts.trillium.templates.cpu]
partition = "compute"; time = "01:00:00"; cpus_per_task = 4; mem = "16G"
preamble = "module load StdEnv/2023 python/3.11\nsource $PROJECT/venvs/mvpa/bin/activate\n"

[hosts.trillium.templates.debug]
inherit = "cpu"; partition = "debug"; time = "00:10:00"
```
- `Template` dataclass: sbatch options + `preamble` + `epilogue` + `inherit` (one level, cycle
  error). `HostConfig.notes: str`, `templates: dict[str, Template]`.
- **Precedence unchanged from v1**: options stay *command-line flags* (they override `#SBATCH` lines
  in user scripts, matching Slurm semantics and FakeSlurm). Merge = host defaults < template <
  explicit kwargs. The generated script gets a comment header `# remoteslurm: template=cpu
  options={…}` (documentation only) and the template preamble/epilogue around the body. `path=`
  submissions (existing remote scripts) accept templates for *options only*; preamble is not
  injected (documented; `invalid_arg` if the template has a preamble and `--force-preamble` absent).
- Registry: `JobRecord.meta["template"]`, `meta["options"]` (merged), `meta["sync_marker"]` — no
  new top-level fields (no schema migration).
- `info()` gains `notes`, `templates`, `learned_notes`; `doctor` warns when `notes` is empty.
- Learned notes: on sbatch `slurm_error`, if stderr matches an allow-list of *policy* patterns
  (`Walltime must be`, `Invalid account`, `Invalid partition`, `QOS`, `exceeds .* limit`,
  `not permitted`), append the line (deduped, capped 50) to
  `~/.local/state/remoteslurm/<host>/learned_notes.txt`; transient errors never qualify.
- CLI: `rslurm templates [--show NAME]`, `rslurm submit --template NAME`, `rslurm notes`.

### C2. `diagnose`
- `Cluster.diagnose(job_id, *, tail=60)` → `{status, script (≤16 KB), stdout_tail, stderr_tail,
  steps, sync, verdict, hints, total_truncated}` with an explicit overall cap (64 KB) applied
  field-by-field in a fixed priority (verdict/hints > tails > script > steps).
- Rules in `slurm.py` `DIAGNOSTICS: list[Rule]` (pure, fixture-tested): OOM (state or
  MaxRSS ≥ 0.95×ReqMem), TIMEOUT (+ last stdout line), NODE_FAIL/BOOT_FAIL, FAILED + stderr
  signatures (`command not found`, `No module named`, `Permission denied`, `No such file`,
  `CUDA`/`torch` device errors, `Killed`), `CANCELLED by <uid>` (self vs other), PENDING reasons
  (`Priority`, `Resources`, `QOSMaxJobsPerUserLimit`, `AssocGrpBillingMinutes`, `ReqNodeNotAvail`,
  `Dependency`) with plain-English text and, once F1 lands, the `squeue --start` estimate.
  Sync marker: "ran git rev X pushed at T; local HEAD now Y (dirty)".
- Surfaces: CLI `rslurm diagnose JOB` (headline verdict, hints, tails); MCP `diagnose`.

### C3. Agent guide
- `docs/agent-guide.md` + `rslurm agent-guide` (prints a CLAUDE.md/AGENTS.md block: call `info`
  first and read `notes`; `sync` not `put`; `edit` not `write`; `diagnose` after failures; use
  templates; prefer `jobs`/`status` over polling `wait`). MCP resource `remoteslurm://guide` with the
  same text.
- MCP tool sets: `REMOTESLURM_MCP_TOOLS=core|all`, **default `core`** =
  `info ls read edit grep write run submit jobs diagnose sync cancel connection`; `all` adds the
  rest. Documented in `mcp-config` output.

**Tests (first):** user script already containing `#SBATCH --time` + template `time` + explicit
`time=` → command-line flag wins, recorded in `meta["options"]`, FakeSlurm sees the flag. Then
`inherit`, preamble placement, `path=` + preamble refusal, learned-notes allow-list and dedupe,
every DIAGNOSTICS rule via FakeSlurm `FAKESLURM_OUTCOME=oom|timeout|nodefail|fail:<stderr>`,
total cap ordering, MCP core/all sets. Live: `--template debug`; a deliberate OOM on debug
(`python -c "bytearray(10**12)"` with `--mem=1G`) diagnosed as OOM.

---

## WP-D — Arrays, sweeps (D1/D2), compute-node runs (D3, after E1)

### D1. Arrays & dependencies — parser rewrite budgeted
- `slurm.py`: `parse_squeue` must handle collapsed pending tasks `123_[5-9%4]` (expand the bracket
  into task ids with `pending` state) and `123_4` rows; `parse_sacct` must group `123_N` and
  `123_N.batch` under parent `123` with per-task records (today `partition(".")` keys collide
  correctly per task but there is no parent roll-up). Fix the inverted `all_steps` flag in
  `op_sacct`.
- `job_status("123")` on an array parent → `JobStatus(state=<aggregate>, tasks={COMPLETED: n,
  RUNNING: n, PENDING: n, FAILED: n}, failed_tasks=[…], terminal = all tasks terminal)`; aggregate
  state = FAILED if any failed and none running, RUNNING if any running, PENDING if any pending,
  else COMPLETED/CANCELLED. `job_status("123_4")` unchanged. `wait("123")` waits for all; CLI
  `wait` exit code = 0 only if every task COMPLETED (documented).
- `job_output`: expand `%A %a %x %u %N %j %J`; `job_output("123_4")` resolves the task file, using
  `scontrol show job 123_4` when needed.
- `cancel("123")` cancels the array; `cancel("123_4")` one task (stub regex already allows both).
- CLI `jobs`: arrays as one row with `TASKS` (`87✓ 10▶ 3✗`); `status 123` lists failed ids;
  `status 123 --tasks` expands. `submit --array 0-9%4 --dependency afterok:123` are plain options.

### D2. Sweeps
- `Cluster.sweep(params, *, script|path, template, name, max_concurrent)`: `params` = dict of lists
  (cartesian) or list of dicts. Always generates a *wrapper* script remotely (`<scripts>/<name>-sweep.sh`)
  that reads `params.tsv` (written next to it) by `$SLURM_ARRAY_TASK_ID`, exports
  `RS_PARAM_<NAME>` and `RS_PARAMS_JSON`, then `source`s the user script / runs `path`. Submitted
  as one array with `%N` = `max_concurrent`. Registry `meta["sweep"] = {params_path, n, names}`;
  `status`/`diagnose` show the params of failed tasks. CLI `rslurm sweep script.sh -P lr=0.1,0.01
  -P seed=1,2,3 --template cpu`; MCP `sweep`.

### D3. `run --compute` (implemented after E1)
- Stub op `srun(argv|cmd, partition, time, cpus, mem, gpus, account, queue_timeout=600,
  max_output)` → `srun --quiet --unbuffered -p … -t … --account … -- bash -c cmd` (argv form: no
  shell), `start_new_session=True`, registered for `cancel`, served by the **slow pool**. Client
  timeout = queue_timeout + walltime + 30 s; on client timeout send `cancel` (frees the allocation).
  Distinguish "never started" (`queue_timeout` elapsed, `srun: job … queued and waiting for resources`
  on stderr) from "ran and timed out".
- `Cluster.run(cmd, compute=True, template="debug", **resources)`; CLI `rslurm run --compute
  [--template debug] 'cmd'`; MCP `run(compute=True, …)`. Respects `allow_run` like `run`.

**Tests (first):** `job_status("123")` with squeue showing `123_[5-9]` pending + `123_4` running and
sacct holding `123_0..3` finished → correct aggregate, `failed_tasks`, not terminal. Then
FakeSlurm `--array` support (N tasks, `%A_%a` outputs, collapsed pending display), sweep wrapper +
`params.tsv` + env injection, output path expansion, `srun` shim (runs locally after a configurable
fake queue wait; `FAKESLURM_SRUN_QUEUE=never` to test queue timeout). Live: 4-task array on debug;
3-param sweep; `run --compute --template debug hostname`.

---

## WP-E — Stub pool, request ids, cancel (E1); safety rails (E2); streaming (E3)

### E1. Pool split + request ids + `cancel` (protocol 1.1, still single-frame)
- Stub: `fast` executor (8 threads: ping/info/ls/stat/read/write/edit/diff/grep/glob/mkdir/rm/
  squeue/sacct/scontrol/sinfo/scancel) and `slow` executor (4 threads: run/srun/sbatch). Each slow
  request registers its `Popen` under the request id; `cancel(id)` → `os.killpg` (slow ops use
  `start_new_session=True`) → the original request returns `{cancelled: true, rc, stdout, stderr}`
  (partial output kept).
- `ping` therefore never queues behind `run`, which also fixes the `Session._probe` false-positive
  (probe killing a stub that was merely busy).
- **Request ids become client-generated** (`uuid4().hex[:12]`) so they survive the daemon hop:
  `Session.submit(op, args, request_id=None)`; `Session.cancel(request_id)`; daemon request gains
  optional `"id"` and a `_cancel` control op `{host, id}`; `DaemonSession.call` generates the id,
  and on client-side timeout / Ctrl-C sends `_cancel`. `Cluster.run/srun` pass
  `cancel_on_timeout=True`.
- CLI: Ctrl-C during `run` sends cancel before exiting 130.

### E2. Safety rails
- `HostConfig.protected_paths: list[str]` (globs; default `~/.ssh/**`, `~/.bashrc`,
  `~/.bash_profile`, `~/.cache/remoteslurm/**`): `write/edit/rm/put` and `sync --delete` refuse
  with `permission` + action; `--force` overrides in CLI, `force=True` in MCP/library.
- `rm -r` refuses path depth < 3 and the `$HOME`/`$SCRATCH`/`$PROJECT` roots (stub-side, using
  `info` env) in addition to today's checks.
- `HostConfig.confirm: list[str]` (e.g. `["rm", "cancel", "sync_delete", "sweep"]`): CLI prompts
  y/N unless `--yes`; MCP/library require a plain **`confirm=True`** argument (no token machinery)
  and otherwise return `{needs_confirmation: true, what: "<summary>"}`.
- `allow_run`: `true` (default, trillium) | `false` | `"safe"` (argv only + executable allow-list
  `run_allowlist`, default python*/Rscript/git/ls/cat/head/tail/wc/du/df/rsync/module/squeue/sacct/
  sinfo/scontrol). `srun` obeys the same setting. Enforcement is client-side; README states the
  daemon socket is same-user trust.
- Audit covers `edit`, `sync`, `srun`, `sweep`, `rm`.

### E3. Streaming — protocol 2 (last; only after everything above has shipped)
- Stub: `run(stream=True)` / `srun(stream=True)` emit `{"id", "ok": true, "chunk": {...},
  "done": false}` frames then a final `done: true`; new `follow(path, offset, idle_timeout)` op
  polls the file every 0.5 s and emits appended bytes as chunks until cancelled/idle.
- `Session`: futures resolve only on `done: true`; a `call_stream()` generator yields chunks; the
  reader keeps pending entries across frames (fix `_dispatch` popping on first frame).
- Daemon wire: multi-line replies — each frame one line, connection closes after `done: true`;
  client socket close → daemon sends stub `cancel` for that id. `DaemonSession.call_stream`.
- CLI: `run --stream` live output; `tail -f` uses `follow` (through the daemon). MCP stays
  request/response (documented).
- Bump stub `PROTOCOL = 2`; client asserts READY protocol ≥ 2 (sha pinning makes mismatch
  impossible, kept as a guard).

**Tests (first, E1):** through the *daemon path*: `run sleep 30` then `ping` returns < 1 s; `cancel`
kills the child `sleep` (pgroup gone); the run returns `cancelled: true`. Then pool separation under
4 concurrent runs, client-timeout → cancel → no lingering remote process, protected-path refusals,
`confirm=True` flow via in-memory MCP client, allow-list enforcement for `run` and `srun`, E3 frame
ordering, `follow` delivers appended lines and stops on idle.

---

## WP-F — Queue intelligence (F1), watch/notify (F2), housekeeping (F3), portability (F4)

### F1. Queue intelligence
- Stub ops return raw text; parsing in `slurm.py`: `squeue_start(jobs)` (`squeue --start -h -o
  "%i|%S|%r" -j`), `sshare()` (`sshare -U -P`), `qos()` (`sacctmgr -P -n show qos
  format=name,maxwall,maxjobspu,maxtresperuser,priority`), `assoc()` (`sacctmgr -P -n show assoc
  user=$USER format=account,partition,qos,grptres,maxjobs`), `quota()` → host `quota_command`
  (trillium/Alliance: `diskusage_report --per_user`) else `df -h` of home/scratch/project.
- `Cluster.queue_info()` (partitions + my accounts/QOS + fair-share + pending jobs with estimates),
  `Cluster.estimate_start(job)`, `Cluster.quota()`. CLI `rslurm queue`, `rslurm quota`; MCP
  `queue_info`, `quota` (in the `all` set). `diagnose` adds the estimate to PENDING verdicts.
- Every parser tolerates missing columns/formats: unknown → `null`, never an error (site formats
  vary; fixtures recorded from trillium and Nibi).

### F2. Watch & notifications (no daemon logic)
- CLI `rslurm watch JOB… [--all] [--notify] [--poll 30]`: foreground loop via the daemon; prints
  transitions; on each terminal state runs `notify_command` (config; default macOS `osascript
  display notification`, Linux `notify-send`); exit = 0 iff all COMPLETED. Appends terminal events to
  `~/.local/state/remoteslurm/<host>/events.jsonl`.
- MCP `wait(job_id, timeout=120)`: bounded, returns `terminal:false` + current status on timeout
  (most MCP clients cap tool calls; agents loop); docstring says so. `events(since=None)` (MCP) /
  `rslurm events` read and mark-read the events file. No progress-notification machinery.

### F3. Housekeeping
- Registry `prune(older_than="30d", keep_active=True)` + `forget`; `jobs()` runs prune at most once
  per hour — the timestamp lives in the registry file and is read/updated under the same flock.
- `jobs()`: one batched `sacct -j a,b,c` for all registry-known-but-not-queued ids; `scontrol` only
  for ids still missing and inside the grace window.
- `rslurm clean [--dry-run] [--older-than 30d]`: removes generated scripts/params under
  `.remoteslurm/scripts/` (stub `glob` + `rm`), never automatic.
- Bootstrap snippet removes `stub-*.py` not matching the current sha (after the new one is in place).

### F4. Portability
- Nibi: add `[hosts.nibi]` (user does `remoteslurm connect nibi` once); record
  squeue/sacct/scontrol/sinfo/sshare/sacctmgr fixtures with a throwaway debug job; parser tests
  parameterised over `{trillium-25.11, nibi-<ver>}` plus hand-built Slurm 20.x fixtures from docs.
- `squeue --me` → `-u $(getpass.getuser())`.
- Bootstrap: detect BusyBox head (`head --version` lacks "coreutils") → `dd bs=1 count=N`; test the
  snippet under `sh`, `bash`, `dash`, `zsh`, `tcsh` (skip-if-missing) via a fake `ssh` that runs the
  command through that shell with the stub source on stdin; cover the "already installed" branch.
  Python discovery: `python3` → `python` → `module load python` inside the snippet.
- `ProxyJump`/`ssh_opts` test with a fake ssh; document jump hosts.
- CI: add `macos-latest` to the test matrix; keep vermin gate; run stub tests under Python 3.7
  (`uv python install 3.7`) as the closest available proxy for 3.6.

**Tests (first):** `jobs()` batched `sacct -j a,b,c` with one id missing still yields a status for
each; `prune` concurrent with `put` from another process loses nothing. Then parsers over both
fixture sets, `watch` transitions + events file, bounded MCP `wait`, shell matrix for bootstrap.

---

## Files

- New: `sync.py`, `docs/agent-guide.md`, `CHANGELOG.md`, `tests/test_sync.py`, `tests/test_edit.py`,
  `tests/test_templates.py`, `tests/test_diagnose.py`, `tests/test_arrays.py`, `tests/test_cancel.py`,
  `tests/test_safety.py`, `tests/test_queue.py`, `tests/fixtures/slurm/nibi/*`, `tests/fakeslurm/srun`.
- Modified: `config.py` (ProjectConfig, Template, notes, protected_paths, confirm, allow_run modes,
  quota_command, notify_command), `stub.py` (edit/diff, pools, request registry, cancel, srun,
  follow, array-safe scancel), `session.py` (client ids, cancel, multi-frame in E3), `daemon.py`
  (id passthrough, `_cancel`, multi-frame in E3), `slurm.py` (array parsing, DIAGNOSTICS, F1
  parsers), `jobs.py` (templates merge, array aggregation, sweep, diagnose, prune/batching),
  `cluster.py` (new methods), `cli.py`, `server.py` (new tools, core/all sets, resource), `README.md`.

## Verification (end-to-end, at the end of v2)
1. Local: full suite green on macOS + ubuntu (CI), ruff/mypy/vermin clean, `uv build`.
2. Live on trillium, from a fresh Claude Code session using only the MCP tools: `info` → read notes →
   `sync` the fixture project → `edit` a parameter → `submit --template debug` → `jobs` →
   `diagnose` a deliberately failing job → `sweep` 3 params → `run --compute` hostname →
   `cancel` a running task → `queue`/`quota`. No hand-written ssh anywhere.
3. Live on Nibi: `doctor` green; a debug job lifecycle; fixtures recorded.

## Explicitly out of v2
Windows; async Python API; non-OpenSSH transports; daemon-side job watching; MCP progress
notifications; Globus; PBS/LSF; HMAC confirm tokens.
