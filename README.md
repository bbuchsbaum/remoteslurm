# remoteslurm

Fast, agent-friendly control of a remote Slurm login node from your laptop — a Python
library, a CLI (`remoteslurm` / `rslurm`), and an MCP server for Claude Code, Codex and
other coding agents.

You keep your agent session local (your repo, your context, your tools); remoteslurm gives
it a low-latency, bounded, structured window onto the cluster: browse directories, read
small files, grep, tail logs, submit jobs, watch them, read their output, cancel them.

## What this is for

- **MFA clusters.** Login nodes behind Duo/YubiKey can't be scripted with a fresh `ssh` per
  command. remoteslurm rides on an OpenSSH `ControlMaster` connection you authenticate
  **once**; everything after that is ~20–50 ms per operation.
- **Agents.** Every operation has hard output caps, pagination tokens and structured errors
  with an `action` field ("run in a terminal: `remoteslurm connect trillium`"). An agent
  can't accidentally `cat` a 40 GB file into its context or hang on an MFA prompt.
- **Turnkey.** `remoteslurm doctor` checks the whole chain; `remoteslurm connect` does the
  one interactive step; a tiny stdlib-only stub is installed on the login node
  automatically (content-hashed, so local/remote versions can never drift).

## When not to use it

- Bulk data movement — use `rsync`/Globus (`remoteslurm put/get --rsync` just shells out).
- Interactive shells or TUIs on the cluster.
- Running things *on compute nodes* — remoteslurm talks to the **login node** and Slurm.

## Install

```bash
uv tool install remoteslurm          # or: pipx install remoteslurm / pip install remoteslurm
remoteslurm --version
```

Requires Python ≥ 3.11 locally, OpenSSH, and `python3` (≥ 3.6) on the login node.

## Setup (once per cluster)

1. Make sure `ssh <alias>` works and uses a persistent master. In `~/.ssh/config`:

   ```
   Host trillium
     HostName trillium.alliancecan.ca
     User brad
     ControlMaster auto
     ControlPath ~/.ssh/sockets/%r@%h-%p
     ControlPersist 12h
   ```

2. Write the config (`remoteslurm config --init` creates `~/.config/remoteslurm/config.toml`):

   ```toml
   default_host = "trillium"

   [hosts.trillium]
   ssh = "trillium"         # ssh alias
   mfa = true               # never try to authenticate non-interactively
   account = "rrg-someone"  # default --account for sbatch
   # partition = "compute"
   # [hosts.trillium.defaults]
   # time = "1:00:00"
   ```

   Hosts without MFA (`mfa = false`) are connected automatically; hosts not in the config
   are treated as bare ssh aliases with `mfa = true` (safe default).

3. Authenticate once, then check everything:

   ```bash
   remoteslurm connect trillium     # Duo prompt happens here, in your terminal
   remoteslurm doctor
   ```

When the master expires (laptop sleep, `ControlPersist` timeout) every command returns
`not_connected` with the exact command to run; nothing ever blocks on a hidden MFA prompt.

## CLI

Paths may be `HOST:PATH`; otherwise `--host`, `default_host` or `$REMOTESLURM_DEFAULT_HOST`
is used. `~` and `$SCRATCH`-style variables are expanded on the remote side. Every command
accepts `--json`.

```bash
rslurm ls -l '$SCRATCH/proj'                 # paged; --limit/--token
rslurm cat proj/run.log --tail 50           # bounded reads; --head/--offset/--max-bytes
rslurm tail -f proj/run.log                 # follow
rslurm grep 'Error' proj -g '*.log' -C 2    # bounded regex search
rslurm find proj '*.nii.gz' --type file
rslurm run 'module load python; python -V' --login
rslurm put local.py trillium:proj/          # small files via the stub, large/dirs via rsync
rslurm get trillium:proj/results.csv .

rslurm submit job.sh -p debug -t 00:10:00 -n test    # local script file
printf '#!/bin/bash\nhostname\n' | rslurm submit - --cwd '$SCRATCH'
rslurm submit --remote '$SCRATCH/proj/job.sh' -o gpus-per-node=1 -o mem=16G
rslurm jobs                                 # queue + recently submitted (with final state)
rslurm status 2166996
rslurm wait 2166996 --poll 10
rslurm output 2166996 -n 100                # the job's stdout file
rslurm cancel 2166996                       # only your own jobs; ownership is verified
rslurm sinfo
```

### Speed: the session daemon

Opening a new ssh channel costs ~2 s on many clusters (PAM session + rc files). The CLI
therefore talks to a small per-user daemon that keeps stub sessions warm; it is started on
demand and exits after 4 h idle. Warm CLI calls take ~150 ms end to end.

```bash
rslurm daemon status|stop|start
rslurm --no-daemon ls ~          # one-off direct session
```

## MCP server (Claude Code, Codex, …)

```bash
remoteslurm mcp-config                 # prints the mcpServers snippet
claude mcp add remoteslurm -s user -e REMOTESLURM_DEFAULT_HOST=trillium -- remoteslurm-mcp
```

Tools: `ls`, `read`, `grep`, `glob`, `write`, `run`, `submit`, `jobs`, `job_output`,
`cancel`, `sinfo`, `info`, `connection`. All return JSON with `truncated` / `next_token`
hints; errors come back as `{"error": "<code>", "message": ..., "action": ...}` rather than
exceptions. `connection` never starts anything — it reports whether the ssh master is alive
and what to run if not.

Example prompts once registered:

```text
List what's in $SCRATCH/proj on trillium and show me the last 30 lines of the newest .log.
Submit scripts/fit.sh on the debug partition with 10 minutes, wait for it, and show the output.
Why did job 2166996 fail? Check its state, exit code and stderr.
```

## Library

```python
from remoteslurm import Cluster

c = Cluster.connect("trillium")              # raises NotConnected with an action if needed
c.ls("$SCRATCH")["entries"]
c.read("~/proj/run.log", tail=50)["content"]
c.grep(r"nan", "$SCRATCH/proj", glob="*.log")

job = c.submit("#!/bin/bash\nhostname\n", name="hello", partition="debug", time="00:05:00")
st = job.wait(poll=10)                       # JobStatus: state, exit_code, elapsed, max_rss, ...
print(st.state, job.output(tail=20)["content"])
```

## Safety defaults

- Library-spawned `ssh` always uses `BatchMode=yes` and `ControlMaster=no`: it can fail, never prompt.
- The remote stub never uses a shell for its own operations (argv lists only); `run`/`run
  --compute` (srun) are the explicit escape hatches. Per host, `allow_run = false` disables
  them and `allow_run = "safe"` requires an argv list whose executable is in `run_allowlist`.
  Enforcement is client-side; the daemon socket is same-user trust (`0600`, owner-only).
- `scancel` only acts on jobs `squeue` attributes to you; `rm` refuses `/`, `$HOME` and its
  parent, and a recursive `rm` also refuses the `$SCRATCH`/`$PROJECT` roots and any path
  shallower than three components.
- `protected_paths` (default `~/.ssh/**`, `~/.bashrc`, `~/.bash_profile`,
  `~/.cache/remoteslurm/**`) block `write`/`edit`/`rm`/`put`/`sync --delete` unless you pass
  `--force` (CLI) / `force=True` (library, MCP). Ops listed in `confirm` (e.g. `["rm",
  "cancel"]`) prompt `y/N` in the CLI (`--yes` skips) and require `confirm=True` in the
  library/MCP (otherwise a `ConfirmationRequired` / `{needs_confirmation: true}` reply).
- Reads are capped (64 KB default, 4 MB max), writes at 8 MB, listings at 2000 entries,
  grep at 50 MB/file; binary files are detected and returned base64-encoded.
- Every `submit`, `run`, `cancel` is appended to `~/.local/state/remoteslurm/<host>/audit.log`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `not_connected` / exit code 3 | `remoteslurm connect <host>` (MFA), then retry |
| `ssh ControlMaster` check fails in `doctor` | add the `ControlMaster/ControlPath/ControlPersist` lines to `~/.ssh/config` |
| `REMOTESLURM-ERROR python not found` | set `python = "/path/to/python3"` in the host config |
| slow CLI (~2 s per call) | `remoteslurm daemon status` — the daemon should be running; `REMOTESLURM_NO_DAEMON` disables it |
| job shows `accounting_pending` | Slurm's accounting lags a little after a job leaves the queue; retry in a few seconds |
| `scancel` says *skipped* | the job is not yours or no longer exists |

Set `REMOTESLURM_DEBUG=1` to print error details.

## Environment knobs

| Variable | Meaning |
|---|---|
| `REMOTESLURM_CONFIG` | alternate config file |
| `REMOTESLURM_DEFAULT_HOST` | default host (overrides config) |
| `REMOTESLURM_STATE_DIR` | registry/audit/daemon log dir (default `~/.local/state/remoteslurm`) |
| `REMOTESLURM_SOCKET` | daemon socket path |
| `REMOTESLURM_DAEMON_IDLE` | daemon idle exit, seconds (default 14400) |
| `REMOTESLURM_NO_DAEMON` | never use/start the daemon |
| `REMOTESLURM_MCP_MAX_CHARS` | cap on any string in an MCP result (default 200000) |

## Development

```bash
uv venv && uv pip install -e '.[dev]'
uv run --no-sync pytest -q              # runs the stub locally with FakeSlurm shims, no cluster needed
uv run --no-sync ruff check src tests && uv run --no-sync ruff format --check src tests
uv run --no-sync mypy
uvx vermin -t=3.6- --violations src/remoteslurm/stub.py   # the remote stub must stay 3.6-compatible
REMOTESLURM_LIVE=1 uv run --no-sync pytest -q tests/live     # optional: against a real cluster
```

Design notes: `docs/plans/v1-design.md`.

## Roadmap

Job templates per cluster, array-job helpers, `follow` streaming over MCP, usage/quota
reporting, Windows support for the daemon (named pipes).
