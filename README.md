# remoteslurm

`remoteslurm` gives a local program or coding agent a bounded, structured way to work with a
remote Slurm cluster over OpenSSH. It includes a Python library, the `rslurm` CLI, and an MCP
server. The package is cluster-neutral: SSH aliases, accounts, partitions, storage variables,
quota commands, templates, and policy notes all live in the user's local configuration.

The remote side is a small, standard-library-only Python stub installed automatically over the
existing SSH connection. It can inspect files, make bounded edits, submit and monitor jobs,
retrieve output, diagnose failures, and synchronize configured projects.

[Changelog](CHANGELOG.md) · [Agent guide](docs/agent-guide.md) ·
[Issue tracker](https://github.com/bbuchsbaum/remoteslurm/issues)

## Status and scope

Version 0.2.0 is a source preview. Install it from a checkout; it is not currently advertised as
a PyPI release. Local tests use a fake Slurm installation, while real-cluster tests are optional
and configured entirely through environment variables.

The intended target is any Slurm login node that is reachable with system OpenSSH and has:

- Python 3.6 or newer on the remote login node;
- the standard Slurm command-line tools (`sbatch`, `squeue`, `sacct`, and friends);
- Python 3.11 or newer on the local machine; and
- `rsync` 3.1 or newer only when project sync or large transfers are needed.

Slurm versions and site policies vary. Queue/account information therefore degrades gracefully
when optional commands are missing, and nonstandard quota output is raw unless a parser is
selected in local config.

## Install

From a checkout:

```bash
uv tool install .
remoteslurm --version
```

For development:

```bash
uv venv
uv pip install -e '.[dev]'
uv run --no-sync pytest -q
```

## Configure a cluster

First make a normal SSH alias. A persistent master is strongly recommended and is required for
clusters whose authentication cannot be completed non-interactively.

```sshconfig
Host mycluster
  HostName login.hpc.example.edu
  User alice
  ControlMaster auto
  ControlPath ~/.ssh/sockets/%r@%h-%p
  ControlPersist 12h
```

Run `remoteslurm config --init`, then edit `~/.config/remoteslurm/config.toml`:

```toml
default_host = "mycluster"

[hosts.mycluster]
ssh = "mycluster"
mfa = true
account = "research"
partition = "standard"
env_vars = ["WORK", "LAB_STORAGE"]
quota_paths = ["~", "$WORK", "$LAB_STORAGE"]
protected_roots = ["$WORK", "$LAB_STORAGE"]
notes = "Use the short partition only for jobs under 30 minutes."

[hosts.mycluster.defaults]
time = "01:00:00"

[hosts.mycluster.templates.cpu]
partition = "standard"
time = "01:00:00"
cpus_per_task = 4
mem = "16G"
preamble = "module load python/3.11\nsource $WORK/venvs/project/bin/activate\n"

[hosts.mycluster.templates.short]
inherit = "cpu"
partition = "short"
time = "00:10:00"

[hosts.mycluster.projects.analysis]
local = "~/code/analysis"
remote = "$WORK/analysis"
exclude = [".git", "__pycache__", "results/"]
delete = false
```

Only `ssh` is needed for basic use. The other values are examples, not package defaults. In
particular, do not copy account, partition, storage, or module settings without adapting them to
your site.

For a site-specific quota command:

```toml
[hosts.mycluster]
quota_command = "site-quota --user"
quota_format = "raw" # raw | pairs | df
```

`raw` is the safe default. `pairs` recognizes fixed-width `<used>/<limit>` pairs; `df` parses
`df -h`-style output. Without `quota_command`, `rslurm quota` runs `df -h` on `quota_paths`.

Authenticate and verify the connection:

```bash
remoteslurm connect mycluster
remoteslurm doctor --host mycluster
```

Every library-spawned SSH process uses batch mode, so an expired interactive connection fails
with a structured `not_connected` error instead of hanging on a hidden prompt.

### Jump hosts

Put `ProxyJump` in `~/.ssh/config` when possible. Alternatively, configure SSH arguments locally:

```toml
[hosts.mycluster]
ssh = "mycluster"
ssh_opts = ["-o", "ProxyJump=bastion.example.edu"]
```

The same options are used by SSH and by project sync.

## Use the CLI

Paths are remote paths. `~` and variables exported by the remote login environment expand on the
remote host. Commands accept `--json`; `HOST:PATH`, `--host`, `default_host`, and
`REMOTESLURM_DEFAULT_HOST` select the cluster.

```bash
rslurm info
rslurm ls '$WORK/analysis'
rslurm cat '$WORK/analysis/run.log' --tail 50
rslurm grep 'Error' '$WORK/analysis' -g '*.log' -C 2
rslurm sync analysis --dry-run
rslurm sync analysis

rslurm submit scripts/fit.sh --template cpu -n fit
rslurm jobs
rslurm wait 12345 --poll 10
rslurm output 12345 -n 100
rslurm diagnose 12345
rslurm cancel 12345
rslurm queue
rslurm quota
```

Use `run` only for short login-node checks. `run --compute` uses `srun`; substantial work should
normally be submitted with `submit`.

## Give an agent access through MCP

`remoteslurm-mcp` is a stdio MCP server. Generate a client configuration with:

```bash
remoteslurm mcp-config --host mycluster
```

The default `core` tool set includes `info`, bounded file operations, `run`, `submit`, `jobs`,
`diagnose`, `sync`, `cancel`, `wait`, and `connection`. Set
`REMOTESLURM_MCP_TOOLS=all` to add glob/diff, project and queue inspection, quota, sweeps, output,
and event tools.

An agent should begin with `info`. Its `notes`, `templates`, `projects`, and `learned_notes` are
the local policy contract: they let the same MCP tools work at a university cluster, a national
facility, or a private Slurm installation without adding site logic to this repository. Install
the full agent workflow with:

```bash
rslurm agent-guide
```

The guide is also exposed as the MCP resource `remoteslurm://guide`.

## Python API

```python
from remoteslurm import Cluster

with Cluster.connect("mycluster") as cluster:
    print(cluster.info()["slurm_version"])
    print(cluster.ls("$WORK/analysis")["entries"])

    job = cluster.submit(
        "#!/bin/bash\nhostname\n",
        name="hello",
        template="short",
    )
    status = job.wait(poll=10)
    print(status.state, job.output(tail=20)["content"])
```

## Safety model

- Reads, directory listings, searches, and MCP results are bounded.
- The stub uses argv execution for internal operations. `run` and site-configured quota commands
  are explicit escape hatches.
- `allow_run = false` disables `run`; `allow_run = "safe"` requires an argv list whose executable
  matches `run_allowlist` and rejects shell `-c` forms.
- `protected_paths` block writes, edits, removal, upload, and sync deletion unless explicitly
  forced.
- Recursive removal always refuses `/`, the remote home and its parent, shallow paths, and every
  root in `protected_roots`.
- Cancellation checks scheduler ownership before calling `scancel`.
- Operations listed in `confirm`, such as `rm` or `cancel`, require explicit confirmation.
- Submissions, runs, cancellations, edits, removals, and sync operations are recorded in local
  per-host state.

The daemon socket and the local configuration are same-user trust boundaries. This is an
operator tool, not a privilege-separation or multi-tenant security boundary.

## Troubleshooting

| Symptom | Action |
|---|---|
| `not_connected` | Run `remoteslurm connect <host>` in a terminal, then retry. |
| ControlMaster check fails | Add `ControlMaster`, `ControlPath`, and `ControlPersist` to the SSH alias. |
| Remote Python is missing | Set `python = "/path/to/python3"` for that host. |
| A configured variable is absent | Ensure it is exported in the remote login environment. |
| Project sync rejects local rsync | Install rsync 3.1 or newer or use bounded `put`/`get`. |
| A finished job is temporarily unknown | Slurm accounting may lag; retry after a few seconds. |

Set `REMOTESLURM_DEBUG=1` for additional local error detail.

## Development and live validation

```bash
uv run --no-sync pytest -q
uv run --no-sync ruff check src tests
uv run --no-sync ruff format --check src tests
uv run --no-sync mypy
uvx vermin -t=3.6- --violations src/remoteslurm/stub.py
uv build
```

Live tests have no built-in cluster, account, partition, path, or walltime:

```bash
REMOTESLURM_LIVE=1 \
REMOTESLURM_LIVE_HOST=mycluster \
REMOTESLURM_LIVE_TEMPLATE=short \
uv run --no-sync pytest -q tests/live
```

Alternatively set any of `REMOTESLURM_LIVE_PARTITION`, `REMOTESLURM_LIVE_TIME`, or
`REMOTESLURM_LIVE_CWD`. Omit them to use configured or scheduler defaults.
