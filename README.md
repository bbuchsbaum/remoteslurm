# remoteslurm

[Agent guide](docs/agent-guide.md) · [Changelog](CHANGELOG.md) ·
[Design notes](docs/plans/v2-plan.md) ·
[Issues](https://github.com/bbuchsbaum/remoteslurm/issues)

**remoteslurm is a Python toolkit for operating a Slurm cluster through an SSH alias.** It gives
people, scripts, and coding agents one coherent way to inspect remote files, synchronize projects,
submit work, follow jobs, retrieve results, and diagnose failures.

Connect interactively once when MFA is required; subsequent operations use the established
OpenSSH connection. A small, standard-library-only Python stub supplies structured, bounded
operations on the login node, while cluster names, accounts, partitions, storage paths, templates,
and policy notes remain in local configuration—not in the package.

> **Status:** Version 0.2.0 is a source preview. Install it from a checkout; there is currently no
> `remoteslurm` release on PyPI. APIs may still change.

## Quick start

Install from this checkout on a local machine with Python 3.11 or newer:

```bash
uv tool install .
remoteslurm --version
```

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

Create the socket directory once, generate the local configuration, and verify the entire path to
Slurm:

```bash
mkdir -p ~/.ssh/sockets
remoteslurm config --init
remoteslurm connect mycluster
remoteslurm doctor --host mycluster
rslurm info
```

`config --init` writes `~/.config/remoteslurm/config.toml` with `mycluster` as the example host.
Edit the alias and site values before connecting. `doctor` checks the SSH setup, remote Python,
stub startup, writable home directory, and Slurm command-line tools. Every operational command
also accepts `--json` for machine-readable output.

## From a local project to a finished job

A useful host configuration captures the details that otherwise leak into scripts and agent
prompts:

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

The account, partition, storage variable, module, and paths above are examples—not package
defaults. Replace them with values from your cluster.

With that configuration, the ordinary workflow stays short:

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

If the job fails or never starts, ask for an explanation instead of manually spelunking through
scheduler records and log files:

```bash
rslurm diagnose 12345
```

`diagnose` combines job state, exit status, resource use, pending reason, scheduler metadata, and
bounded log tails into a verdict with concrete next steps.

## What you can do

| Goal | Commands |
|---|---|
| Inspect the remote workspace | `info`, `ls`, `cat`, `tail`, `grep`, `find`, `diff` |
| Move or update work | `sync`, `put`, `get`, `edit` |
| Run work through Slurm | `submit`, `sweep`, `run --compute` |
| Keep login-node work running | `run --detach`, `proc`, `wait --pid`/`--path` |
| Observe jobs | `jobs`, `status`, `wait`, `watch`, `events` |
| Understand or stop a job | `output`, `diagnose`, `cancel` |
| Inspect capacity and storage | `sinfo`, `queue`, `quota` |

Remote paths may use `~` and variables exported by the remote login environment. Select a cluster
with `HOST:PATH`, `--host`, `default_host`, or `REMOTESLURM_DEFAULT_HOST`.

Use `run` only for short login-node checks. `run --compute` requests an interactive allocation
through `srun`; substantial work should normally go through `submit`. For login-node work that
must outlive the call, such as a server, an install, or a setup script, `run --detach` returns the
process id and log path at once. `proc status|tail|kill` manage the process, and
`wait --pid PID [--pattern REGEX]` blocks until it exits or its log prints a marker.

## Coding agents and MCP

`remoteslurm-mcp` is a stdio MCP server exposing the same cluster-neutral operations. Generate a
client registration snippet with:

```bash
remoteslurm mcp-config --host mycluster
```

The default `core` tool set covers cluster information, bounded file operations, execution
(including detached login-node processes), submission, job monitoring, diagnosis,
synchronization, cancellation, waiting, and connection state. Set `REMOTESLURM_MCP_TOOLS=all` to
add project and queue inspection, quota, sweeps, output, events, globbing, and diffs. `run` and
`sync` stay under 25 minutes (`REMOTESLURM_MCP_MAX_CALL`, default 1500 s) because clients such as
Claude Code abort silent calls after 30 minutes. Cancelling a `run` or `wait` call stops its
remote process or wait; side-effecting calls such as `submit` finish, so their results are kept.

An agent should call `info` first. The returned `notes`, `templates`, `projects`, and
`learned_notes` are its local policy contract: they say where work belongs and how it should be
submitted without teaching this repository about one institution. Print a ready-to-use agent
instruction block with:

```bash
rslurm agent-guide
```

The same instructions are available as the MCP resource `remoteslurm://guide` and in the
[agent guide](docs/agent-guide.md).

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
    print(job.output(tail=20)["content"])
```

`Cluster` also exposes the bounded file, search, synchronization, queue, quota, sweep, and
diagnostic operations used by the CLI and MCP server. Errors are structured subclasses of
`RemoteSlurmError`, including `NotConnected`, `NotFound`, `PermissionDenied`, and `SlurmError`.

## Safety and trust boundaries

- Reads, directory listings, searches, log tails, and MCP results are bounded.
- Internal stub operations execute argument vectors. `run` and a site-configured quota command are
  explicit command-execution escape hatches.
- `allow_run = false` disables `run`; `allow_run = "safe"` requires an argument vector, checks the
  executable against `run_allowlist`, and rejects shell `-c` forms.
- `protected_paths` guard writes, edits, removals, uploads, and sync deletion unless explicitly
  overridden.
- Recursive removal refuses `/`, the remote home and its parent, shallow paths, and every root in
  `protected_roots`.
- Cancellation verifies scheduler ownership before calling `scancel`. Operations named in
  `confirm`, such as `rm` or `cancel`, require explicit confirmation.
- Submissions, runs, cancellations, edits, removals, and synchronization operations are recorded
  in local per-host state.

The daemon socket and local configuration are same-user trust boundaries. remoteslurm is an
operator tool, not a privilege-separation layer or a multi-tenant security boundary. It does not
provision cluster access, bypass MFA, or replace the rules enforced by Slurm and the site.

## Compatibility and site integration

The intended target is a Slurm login node reachable through system OpenSSH with:

- Python 3.11 or newer on the local machine;
- Python 3.6 or newer on the remote login node;
- standard Slurm tools such as `sbatch`, `squeue`, `sacct`, and `scontrol`; and
- `rsync` 3.1 or newer only for project synchronization and large transfers.

CI is configured to test Python 3.11–3.13 on Linux and macOS. The remote stub is
standard-library-only; its Python 3.6 syntax floor is checked statically and its boot path is
exercised under Python 3.7. Real-cluster behavior remains dependent on the installed Slurm version
and site policy.

Prefer `ProxyJump` in `~/.ssh/config` for jump hosts. If that is not possible, set
`ssh_opts = ["-o", "ProxyJump=bastion.example.edu"]` for the host; the same options flow to SSH
and project synchronization.

For site-specific quota tooling, configure a command and select its output contract explicitly:

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
| Project sync rejects local rsync | Install rsync 3.1+ or use bounded `put` and `get`. |
| A finished job is temporarily unknown | Slurm accounting may be lagging; retry after a few seconds. |

Set `REMOTESLURM_DEBUG=1` for additional local error detail.

## Development

```bash
uv venv
uv pip install -e '.[dev]'
uv run --no-sync pytest -q
uv run --no-sync ruff check src tests
uv run --no-sync ruff format --check src tests
uv run --no-sync mypy
uvx vermin -t=3.6- --violations src/remoteslurm/stub.py
uv build
```

The regular suite uses a fake Slurm installation. Live tests are fully opt-in and carry no
built-in cluster, account, partition, path, or walltime:

```bash
REMOTESLURM_LIVE=1 \
REMOTESLURM_LIVE_HOST=mycluster \
REMOTESLURM_LIVE_TEMPLATE=short \
uv run --no-sync pytest -q tests/live
```

You may instead provide `REMOTESLURM_LIVE_PARTITION`, `REMOTESLURM_LIVE_TIME`, or
`REMOTESLURM_LIVE_CWD`. Omitted values fall back to configured or scheduler defaults.

## License

MIT
