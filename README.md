# remoteslurm

[Agent skill](skills/remoteslurm/SKILL.md) · [Agent guide](docs/agent-guide.md) ·
[Durable tasks](docs/durable-tasks.md) · [Campaigns](docs/campaigns.md) ·
[Changelog](CHANGELOG.md) · [Issues](https://github.com/bbuchsbaum/remoteslurm/issues)

**remoteslurm is a Python toolkit for operating a Slurm cluster through an SSH alias.** It gives
people, scripts, and coding agents one coherent way to inspect remote files, synchronize projects,
submit work, follow jobs, retrieve results, and diagnose failures.

You authenticate once in a terminal, including MFA where the site requires it; later operations
reuse that OpenSSH connection. A small, standard-library-only Python stub performs structured,
bounded operations on the login node. Cluster names, accounts, partitions, storage paths,
templates, and policy notes live in your local configuration, not in the package.

remoteslurm has three interfaces over the same operations:

- the `rslurm` command line (also installed as `remoteslurm`), where every operational command
  accepts `--json`;
- the `remoteslurm-mcp` server for coding agents such as Claude Code and Codex; and
- the `remoteslurm` Python library.

> **Status:** Version 0.2.0 is a source preview. Install it from GitHub; there is no release on
> PyPI yet. APIs may still change.

## Install

You need Python 3.11 or newer and OpenSSH locally. Install the tools with
[uv](https://docs.astral.sh/uv/):

```bash
uv tool install git+https://github.com/bbuchsbaum/remoteslurm
remoteslurm --version
```

This installs `remoteslurm`, `rslurm`, and `remoteslurm-mcp`. Upgrade later with
`uv tool upgrade remoteslurm`; from a clone, `uv tool install .` also works.

Nothing needs to be installed on the cluster. The login node needs Python 3.6 or newer and the
usual Slurm command-line tools. Project `sync` needs rsync 3.1 or newer locally; `put` and `get`
use rsync on both ends for directories and files larger than 4 MiB.

## Connect to a cluster

First make sure a normal SSH alias reaches the cluster. For an MFA-protected cluster, configure a
persistent OpenSSH master so authentication happens in a visible terminal instead of a hidden
subprocess:

```sshconfig
Host mycluster
  HostName login.hpc.example.edu
  User alice
  ControlMaster auto
  ControlPath ~/.ssh/sockets/%r@%h-%p
  ControlPersist 12h
```

Create the socket directory, generate the local configuration, then connect and check the whole
path to Slurm:

```bash
mkdir -p ~/.ssh/sockets
remoteslurm config --init        # writes ~/.config/remoteslurm/config.toml
remoteslurm connect mycluster    # authenticate here, once
remoteslurm doctor mycluster
rslurm info
```

`config --init` uses `mycluster` as a placeholder host; edit the alias and site values before
connecting. `REMOTESLURM_CONFIG` or the global `--config` option selects a different file.
`doctor` checks the SSH setup, remote Python, stub startup, a writable remote home, and the Slurm
command-line tools.

## Use it from a coding agent

Two optional add-ons make remoteslurm easier for an agent to use: the MCP server, which gives it
structured tools, and the agent skill, which tells it how to use them safely. Without either, an
agent can still drive the `rslurm` CLI with `--json`.

### Register the MCP server

```bash
# Claude Code
claude mcp add remoteslurm -s user -e REMOTESLURM_DEFAULT_HOST=mycluster -- remoteslurm-mcp

# Codex
codex mcp add remoteslurm --env REMOTESLURM_DEFAULT_HOST=mycluster -- remoteslurm-mcp
```

For other clients, `remoteslurm mcp-config mycluster` prints the JSON registration. Restart the
agent session so the tools load. The default `core` tool set covers the normal workflow; add
`REMOTESLURM_MCP_TOOLS=all` for optional extras (see [MCP server details](#mcp-server-details)).

### Install the agent skill

The skill in [`skills/remoteslurm`](skills/remoteslurm/SKILL.md) is a standard `SKILL.md`
directory. It covers orientation, execution choices, durable tasks, campaigns, monitoring, and
setup, and loads each part only when a task needs it. Copy it into your agent's skills
directory:

```bash
src="$(mktemp -d)"
git clone --depth 1 https://github.com/bbuchsbaum/remoteslurm "$src"

# Claude Code
mkdir -p ~/.claude/skills
rsync -a --delete "$src/skills/remoteslurm/" ~/.claude/skills/remoteslurm/

# Codex
mkdir -p "${CODEX_HOME:-$HOME/.codex}/skills"
rsync -a --delete "$src/skills/remoteslurm/" "${CODEX_HOME:-$HOME/.codex}/skills/remoteslurm/"

rm -rf "$src"
```

Run the same commands again to update the skill; `--delete` replaces the installed copy, so any
local edits to it are lost. The third-party
[`skills`](https://www.npmjs.com/package/skills) installer can do this too:
`npx skills add bbuchsbaum/remoteslurm -g`.

### Let the agent do the setup

Paste this into Claude Code, Codex, or another agent that can run shell commands:

```text
Install remoteslurm (https://github.com/bbuchsbaum/remoteslurm) and set it up for my Slurm cluster.
1. Install the tools: uv tool install git+https://github.com/bbuchsbaum/remoteslurm
2. Install the agent skill from skills/remoteslurm in that repository into your own skills
   directory (~/.claude/skills/remoteslurm for Claude Code, ${CODEX_HOME:-$HOME/.codex}/skills/remoteslurm
   for Codex), then read its SKILL.md and references/setup.md.
3. Ask me for my cluster's SSH alias. Check ~/.ssh/config for a persistent ControlMaster entry
   and propose one if it is missing. Run `remoteslurm config --init` and fill in the host with
   values I confirm; do not invent accounts, partitions, or paths.
4. Tell me the exact `remoteslurm connect <alias>` command to run in my own terminal. I will do
   any MFA myself.
5. Run `remoteslurm doctor <alias>` and fix what it reports.
6. Register the MCP server for yourself with REMOTESLURM_DEFAULT_HOST=<alias>, then tell me to
   restart the session.
```

The MCP resource `remoteslurm://guide` and `rslurm agent-guide` print a short instruction block
suitable for a project's `CLAUDE.md` or `AGENTS.md`; it is the same text as the
[agent guide](docs/agent-guide.md).

## What you can do

| Goal | Commands |
|---|---|
| Inspect the remote workspace | `info`, `ls`, `cat`, `tail`, `grep`, `find`, `diff` |
| Move or update work | `sync`, `put`, `get`, `edit` |
| Run work through Slurm | `submit`, `pack`, `sweep`, `run --compute` |
| Run one job whose result must be validated and recoverable | `ensure` |
| Run and validate many units across stages | `campaign` (`plan`, `preflight`, `start`, `list`, `runs`, `adopt`, `apply`, `drive`, `status`, `verify`, `retry`, `cancel`, `close`, …) |
| Keep login-node work running | `run --detach`, `proc`, `wait --pid`/`--path` |
| Observe or recover jobs | `jobs`, `status`, `watch`, `wait`, `events`, `adopt` |
| Understand or stop a job | `diagnose`, `output`, `cancel` |
| Inspect capacity and storage | `sinfo`, `queue`, `quota` |
| Read site configuration | `templates`, `projects`, `notes`, `config` |
| Housekeeping | `forget`, `clean`, `daemon status\|start\|stop` |

`rslurm COMMAND --help` documents every option. Remote paths may use `~` and variables exported
by the remote login environment. Select a cluster with `HOST:PATH`, `--host`, `default_host`, or
`REMOTESLURM_DEFAULT_HOST`.

Use `run` only for short login-node checks. `run --compute` requests an interactive allocation
through `srun`; substantial work should normally go through `submit`. For login-node work that
must outlive the call, such as a server, an install, or a setup script, `run --detach` returns the
process ID and log path at once. `proc status|tail|kill` manage the process, and
`wait --pid PID [--pattern REGEX]` blocks until it exits or its log prints a marker.

## From a local project to a finished job

### Describe the cluster once

A host configuration captures the details that otherwise leak into scripts and agent prompts:

```toml
default_host = "mycluster"

[hosts.mycluster]
ssh = "mycluster"
mfa = true
account = "research"
partition = "standard"
env_vars = ["WORK"]
protected_roots = ["$WORK"]
notes = "Use the short partition only for jobs under 30 minutes."

[hosts.mycluster.templates.cpu]
time = "01:00:00"
cpus_per_task = 4
mem = "16G"
preamble = "module load python/3.11\nsource $WORK/venvs/project/bin/activate\n"

[hosts.mycluster.projects.analysis]
local = "~/code/analysis"
remote = "$WORK/analysis"
exclude = [".git", "__pycache__", "results/"]
delete = false
```

The account, partition, storage variable, module, and paths above are examples, not package
defaults. Replace them with values from your cluster.

### Sync, submit, and follow

```bash
# Preview, then synchronize the configured project.
rslurm sync analysis --dry-run
rslurm sync analysis

# Submit a local script through the named template.
rslurm submit scripts/fit.sh --template cpu --cwd '$WORK/analysis' -n fit
# Submitted job 12345 (PENDING)

rslurm watch 12345 --notify
rslurm output 12345 -n 100
```

To bring results back, use `rslurm sync analysis --pull`, or `rslurm get HOST:REMOTE LOCAL` for
individual files and directories. `rslurm put LOCAL HOST:REMOTE` uploads.

### Follow many jobs

For a long cohort, register a background watch and keep the returned `watch_id`:

```bash
rslurm watch 12345 12346 --background --notify --poll 60
rslurm watch --watch-id WATCH_ID --json
rslurm watch --watch-id WATCH_ID --stop

rslurm jobs --name 'cohort-*' --state RUNNING --limit 25
rslurm jobs --since 2026-09-01 --fields job_id,name,state,exit_code
```

A background watch lives in the local session daemon (`rslurm daemon status`). It keeps observing
after the command or agent exits and resumes saved subscriptions when the daemon next starts. The
default condition is all jobs terminal **or any job failed**; `--condition all_terminal` waits
for the whole set. Identical starts return the same watch, and stopping a watch never cancels
jobs. Desktop notifications are best effort, and waking an agent requires client integration.
Status reads expose the observer heartbeat, observation age, and connection errors. Laptop sleep
and daemon downtime create observation gaps; nothing restarts the daemon after login or reboot.

Job listings default to 50 compact records within a 32 KiB JSON budget. Arrays show task counts
and a failure sample. Follow `next_offset` while `has_more`, or narrow by ID, name glob,
submission time, and state. `--detail` and `--fields` still obey the byte limit; `rslurm status
JOB` returns full detail for one job.

### Resource use and progress

```bash
rslurm status 12345 --usage
rslurm watch 12345 --usage --usage-interval 60
```

Running jobs are sampled with `sstat` and finished jobs with `sacct`. The result records its
source and sample time and normalizes allocated CPUs, live PIDs, CPU time, effective CPU use,
allocation utilization, and RSS. `watch` refuses usage intervals below 60 seconds because `sstat`
contacts the Slurm controller.

For computations that write countable files, declare progress at submission:

```bash
rslurm submit scripts/fit.sh --cwd '$WORK/analysis' \
  --progress-path results/null-plans --progress-pattern '*.rds' --progress-total 800
rslurm status 12345 --usage
```

`status --usage` then reports observed, total, and percentage from a bounded file scan, and says
whether the scan was truncated. Progress is an observation: it does not validate the files.

### Pack many small commands

`pack` runs independent shell commands with GNU Parallel inside one or more one-node
allocations. `--max-processes` limits concurrent processes on each node; `--max-concurrent`
limits how many allocations Slurm runs at once.

```bash
# One allocation, at most 10 commands running on its node at once.
rslurm pack commands.txt --template cpu --max-processes 10

# Split the list across 4 allocations, 10 processes per node, at most 2 nodes at once.
rslurm pack commands.txt --template cpu --batches 4 --max-processes 10 --max-concurrent 2
```

Each nonblank line is one shell command. GNU Parallel must be available in the template's job
environment. Unless the host, template, or `--cpus` says otherwise, each allocation requests one
CPU per concurrent process. For a parameter grid, `rslurm sweep script.sh -P alpha=0.1,0.2`
submits a job array with one task per combination.

### When a job fails or never starts

```bash
rslurm diagnose 12345
```

`diagnose` combines job state, exit status, resource use, pending reason, scheduler metadata, and
bounded log tails into a verdict with concrete next steps. It scans up to 1 MiB from the start of
each log and returns up to eight error excerpts with line numbers, so a scheduler epilogue does
not hide early errors. `scan_truncated` and `match_limit_reached` report incomplete scans.

Before submission, remoteslurm creates the fixed parent directories of `--output` and `--error`.
It refuses Slurm substitutions in directory components (`%j/log.out`) but accepts them in
filenames (`logs/%A_%a.out`). Compute runs enforce `queue_timeout` separately from walltime and
cancel their own allocation when abandoned. Their result includes `job_ids`, `allocation_name`,
and cleanup evidence; if cleanup is unconfirmed, reconcile before resubmitting.

### Local records and recovery

remoteslurm keeps a local registry of submitted jobs. It checks that the registry is writable
before calling `sbatch`. If the registry fails after Slurm accepts a job, the command still
succeeds and returns `submitted: true`, `recorded: false`, the job ID, and a recovery command.
Do not resubmit; adopt the job once local state is writable:

```bash
rslurm adopt 12345
```

`jobs` and `status` fall back to scheduler-visible data when local history is unavailable and
mark the result `registry_available: false`. Set `REMOTESLURM_STATE_DIR` when the default state
location is not writable, as in some sandboxed agent sessions:

```bash
REMOTESLURM_STATE_DIR=/tmp/remoteslurm-state rslurm --no-daemon submit scripts/fit.sh
```

Generated batch wrappers are staged under `~/.remoteslurm/scripts` on the cluster regardless of
`--cwd`, so submitting from a clean Git checkout leaves it clean. Set the host's `script_dir` to
stage elsewhere; submission results give the exact `script_path`.

## Durable tasks

For one batch job whose result must survive a lost client and be validated before reuse,
describe it in a task manifest and repeat `ensure` until it reports `VERIFIED`:

```bash
rslurm ensure fit.toml
```

The task identity covers the script, remote input fingerprints, resolved resources, declared
environment, outputs, validation command, and an optional `[progress]` observer. Intent is stored
on the cluster before `sbatch`. An interrupted submission is reconciled by its unique attempt
marker; an ambiguous one is reported as `UNKNOWN` and never retried automatically. `VERIFIED`
means the validation command passed and the declared outputs still match their recorded
fingerprints; `COMPLETED` means only that Slurm finished. See
[durable tasks](docs/durable-tasks.md) for the manifest, retries, Python API, and MCP tool.

## Campaigns

A campaign definition groups finite inventories (for example, subjects and sessions) and stage
dependencies into work units with stable identities. A run of that definition can adopt work
that already exists, submit the rest in bounded passes, reconcile scheduler and filesystem
evidence, validate output contracts, and be closed or archived.

In the commands below, `study` is the `name` declared in `campaign.toml`:

```bash
# Compile locally and inspect units and execution policy.
rslurm campaign plan campaign.toml

# Qualify the definition, remote tools, and permissions; this stores a preflight receipt.
rslurm campaign preflight campaign.toml

# Create the run (submits nothing), then bind work that already exists.
rslurm campaign start campaign.toml --run-id first-pass
rslurm campaign adopt study --run first-pass --stage preprocess --array-job 12345
rslurm campaign adopt study --run first-pass --stage group --output-only

# Submit one bounded pass of eligible work, or repeat passes while attached.
rslurm campaign apply study --run first-pass --require-preflight
rslurm campaign drive study --run first-pass --max-passes 20 --interval 30

# Observe once (squeue, sacct, output metadata, sstat); never submits or validates.
rslurm campaign status study --run first-pass --refresh
rslurm campaign failures study --run first-pass

# Validate outputs; only this writes production validation receipts.
rslurm campaign verify study --run first-pass --stage preprocess
rslurm campaign receipts study --kind validation --run first-pass

# Retries are explicit. Preview first; --apply also submits. UNKNOWN work also
# requires --accept-duplicate-risk.
rslurm campaign retry study --run first-pass --stage preprocess \
  --state validation=failed --reason 'fixed validator' --dry-run

# Closing blocks new submission, adoption, and retries; refresh and revalidation still work.
rslurm campaign close study --run first-pass
rslurm campaign archive study --run first-pass
```

Each unit reports dependency, execution, artifact, validation, and freshness states separately,
so scheduler `COMPLETED`, present outputs, and passed validation remain distinct claims. Stages
run as `single`, `array`, or `pack` jobs. Each `apply` pass records its exact submission intent on
the cluster before calling `sbatch`, so a lost reply is recovered rather than resubmitted; an
ambiguous attempt stays `UNKNOWN`. Retries append new attempts instead of replacing failures; cancelled units cannot be retried
within the same run. `campaign cancel` previews the affected jobs unless given `--apply`.

A definition may declare named `[pilots]`, each a small inventory selection with its own output
root. `preflight --against PILOT` validates outputs that already exist there, and
`apply --require-preflight PILOT` then requires that receipt. Preflight never submits work, so
produce pilot outputs first.

Output contracts describe required or optional files, directories, and symlinks, with glob
cardinality, size limits, optional SHA-256 digests, and settling intervals. Validators are argv
arrays with bounded time and output and obey the host's `allow_run` policy. A later change to a
validated artifact marks it `STALE` while keeping the old receipt. Pilot receipts never count as
production validation. Set the host's `campaign_dir` when `~/.remoteslurm/campaigns` is not
visible from every login node. The [campaign guide](docs/campaigns.md) documents the schema and
APIs.

## MCP server details

`remoteslurm-mcp` is a stdio MCP server. Its default `core` tool set covers cluster information,
bounded file reading and editing, login-node and compute execution, detached processes,
submission, `adopt`, `ensure`, `pack`, job listing, waiting and watches, diagnosis,
synchronization, cancellation, connection state, and all campaign tools. Set
`REMOTESLURM_MCP_TOOLS=all` to add `glob`, `diff`, `job_output`, `sinfo`, `projects`, `sweep`,
`queue_info`, `quota`, and `events`.

`run` and `sync` calls stay under 25 minutes (`REMOTESLURM_MCP_MAX_CALL`, default 1500 s),
because clients such as Claude Code abort silent calls after 30 minutes. Cancelling a `run` or
`wait` call stops its remote process or wait; side-effecting calls such as `submit` finish, so
their results are kept.

An agent should call `info` first. Its `notes`, `templates`, `projects`, and `learned_notes` are
the local policy contract: they say where work belongs and how to submit it, without teaching
this repository about any one institution.

## Python API

The library follows the same lifecycle as the CLI:

```python
from remoteslurm import Cluster

with Cluster.connect("mycluster") as cluster:
    print(cluster.info()["slurm_version"])

    job = cluster.submit(
        "#!/bin/bash\nhostname\n",
        name="hello",
        template="cpu",
    )
    status = job.wait(poll=10)
    print(status.state)
    print(job.status(refresh=True, usage=True).usage)
    print(job.output(tail=20)["content"])
```

`Cluster` also exposes the bounded file, search, removal, synchronization, queue, quota, sweep,
and diagnostic operations, plus `ensure` and `cluster.campaigns`. Errors are structured
subclasses of `RemoteSlurmError`, including `NotConnected`, `NotFound`, `PermissionDenied`, and
`SlurmError`.

## Safety and trust boundaries

- Reads, directory listings, searches, log tails, and MCP results are bounded.
- Internal stub operations execute argument vectors. `run` and a site-configured quota command are
  explicit command-execution escape hatches.
- `allow_run = false` disables `run`; `allow_run = "safe"` requires an argument vector, checks the
  executable against `run_allowlist`, and rejects shell `-c` forms.
- `protected_paths` guard writes, edits, removals, uploads, and sync deletion unless explicitly
  overridden.
- Recursive removal (Python `Cluster.rm`; the CLI and MCP have no remove command) refuses `/`,
  the remote home and its parent, shallow paths, and every root in `protected_roots`.
- Cancellation verifies scheduler ownership before calling `scancel`. Operations named in the
  host's `confirm` list, such as `rm` or `cancel`, require explicit confirmation.
- Submissions, runs, cancellations, edits, removals, and synchronization are recorded in local
  per-host state.

The daemon socket and local configuration are same-user trust boundaries. remoteslurm is an
operator tool, not a privilege-separation layer or a multi-tenant security boundary. It does not
provision cluster access, bypass MFA, or replace the rules enforced by Slurm and the site.

## Compatibility and site integration

The intended target is a Slurm login node reachable through system OpenSSH, with:

- Python 3.11 or newer on the local machine;
- Python 3.6 or newer on the remote login node;
- standard Slurm tools such as `sbatch`, `squeue`, `sacct`, and `scontrol`; and
- rsync 3.1 or newer locally for project `sync`, and rsync on both ends for large or directory
  `put`/`get` transfers.

CI tests Python 3.11–3.13 on Linux and macOS. The remote stub is standard-library-only; its Python
3.6 syntax floor is checked statically and its startup is exercised under Python 3.7. Real-cluster
behavior still depends on the installed Slurm version and site policy.

Prefer `ProxyJump` in `~/.ssh/config` for jump hosts. If that is not possible, set
`ssh_opts = ["-o", "ProxyJump=bastion.example.edu"]` for the host; the same options apply to SSH
and project synchronization.

For site-specific quota tooling, configure a command and select its output format explicitly:

```toml
[hosts.mycluster]
quota_command = "site-quota --user"
quota_format = "raw" # raw | pairs | df
```

`raw` is the conservative default. Without `quota_command`, `rslurm quota` runs `df -h` over the
configured `quota_paths`. Optional queue, account, QOS, and fair-share commands degrade to partial
results when a cluster does not provide them.

## Troubleshooting

| Symptom | What to do |
|---|---|
| `not_connected` | Run `remoteslurm connect <host>` in a terminal, then retry. |
| The ControlMaster check fails | Add `ControlMaster`, `ControlPath`, and `ControlPersist` to the SSH alias. |
| Remote Python is missing | Set `python = "/path/to/python3"` for that host. |
| A configured variable is absent | Export it in the remote login environment. |
| Project sync rejects local rsync | Install rsync 3.1+ locally (for example, `brew install rsync`). |
| A finished job is temporarily unknown | Slurm accounting may be lagging; retry after a few seconds. |
| Agent tools are missing after registration | Restart the agent session; check `remoteslurm mcp-config`. |

Set `REMOTESLURM_DEBUG=1` for additional local error detail.

## Development

```bash
git clone https://github.com/bbuchsbaum/remoteslurm
cd remoteslurm
uv venv
uv pip install -e '.[dev]'
uv run --no-sync pytest -q
uv run --no-sync ruff check src tests
uv run --no-sync ruff format --check src tests
uv run --no-sync mypy
uvx vermin -t=3.6- --violations src/remoteslurm/stub.py
uv build
```

The regular suite uses a fake Slurm installation. Live tests are opt-in and carry no built-in
cluster, account, partition, path, or walltime:

```bash
REMOTESLURM_LIVE=1 \
REMOTESLURM_LIVE_HOST=mycluster \
REMOTESLURM_LIVE_TEMPLATE=short \
uv run --no-sync pytest -q tests/live
```

You may instead provide `REMOTESLURM_LIVE_PARTITION`, `REMOTESLURM_LIVE_QOS`,
`REMOTESLURM_LIVE_TIME`, or `REMOTESLURM_LIVE_CWD`. Omitted values fall back to configured or
scheduler defaults. The durable-task live test runs when `REMOTESLURM_LIVE_CWD` is set, creates an
isolated subdirectory there, and removes its outputs and remote task record after validation. Set
`REMOTESLURM_LIVE_FAULTS=1` to also exercise recovery after the submitting stub exits immediately
following scheduler acceptance.

The campaign live test also requires `REMOTESLURM_LIVE_CWD`. It exercises lost-response array
recovery, mixed array outcomes, packed markers, concurrent retry authorization, verified per-unit
dependency release, and cancellation with isolated campaigns and work directories. Set
`REMOTESLURM_LIVE_EVIDENCE` to write its machine-readable qualification receipt.

Design history lives in [docs/plans](docs/plans/).

## License

MIT
