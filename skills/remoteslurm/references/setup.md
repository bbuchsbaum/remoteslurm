# Set up remoteslurm

Use this reference only for installation, configuration, connection, MCP registration, or
connectivity diagnosis. Steps marked **user** need the person at the keyboard; give them the
exact command and wait.

## 1. Install the CLI and MCP server

Requires Python 3.11 or newer and OpenSSH locally. Project `sync` needs local rsync 3.1+;
`put`/`get` of directories or files over 4 MiB need rsync on both ends. There is no PyPI release;
install from GitHub:

```bash
uv tool install git+https://github.com/bbuchsbaum/remoteslurm
remoteslurm --version
```

This puts `remoteslurm`, its alias `rslurm`, and `remoteslurm-mcp` on `PATH`. Upgrade with
`uv tool upgrade remoteslurm`. From a local checkout, `uv tool install .` works too. Nothing needs
to be installed on the cluster: remoteslurm copies a standard-library Python stub to
`~/.cache/remoteslurm` on the login node over SSH. The login node needs Python 3.6 or newer and
the usual Slurm command-line tools.

## 2. Configure SSH and the host

The host's `ssh` value names an OpenSSH alias. Hostname, user, jump hosts, and connection
multiplexing belong in `~/.ssh/config`. An MFA site needs a persistent ControlMaster so the user
authenticates once in a visible terminal and later calls reuse that connection:

```sshconfig
Host mycluster
  HostName login.hpc.example.edu
  User alice
  ControlMaster auto
  ControlPath ~/.ssh/sockets/%r@%h-%p
  ControlPersist 12h
```

Then create the socket directory and an example configuration:

```bash
mkdir -p ~/.ssh/sockets
remoteslurm config --init      # writes ~/.config/remoteslurm/config.toml
```

The generated file uses `mycluster` as a placeholder. Edit the alias and site values with the
user. Never present example accounts, partitions, paths, templates, or module commands as real
site values; ask or read the site's documentation.

## 3. Connect and verify

```bash
remoteslurm connect mycluster      # user: performs MFA in their own terminal
remoteslurm doctor mycluster
```

`doctor` checks SSH, remote Python, stub startup, a writable remote home, and Slurm tools. Fix
the failing check it names. Set the host's `python` when the login node's default `python3` is
unsuitable. Preserve a structured error's `action` or `connect_cmd` exactly when relaying it.
Reconnect only when `connection` shows the master is unavailable or expiring.

## 4. Register the MCP server

`remoteslurm mcp-config mycluster` prints the registration JSON for the current version. The
equivalent commands are:

```bash
# Claude Code
claude mcp add remoteslurm -s user -e REMOTESLURM_DEFAULT_HOST=mycluster -- remoteslurm-mcp

# Codex
codex mcp add remoteslurm --env REMOTESLURM_DEFAULT_HOST=mycluster -- remoteslurm-mcp
```

Restart the agent session afterwards so the tools load. The default `core` tool set covers the
normal lifecycle, including durable tasks, packed jobs, watches, and campaigns. Add
`REMOTESLURM_MCP_TOOLS=all` only for the optional tools: `glob`, `diff`, `job_output`, `sinfo`,
`projects`, `sweep`, `queue_info`, `quota`, and `events`. The MCP resource `remoteslurm://guide`
and `rslurm agent-guide` print usage guidance for the installed version.

## Store site knowledge

Persistent facts belong in the host's configuration: scheduler defaults, policy `notes`, named
resource `templates`, named sync `projects`, selected environment variables, quota settings,
session lifetime, protected roots, and run policy. After editing, call `info` to confirm what
agents see. Do not encode one institution's policy in this skill.
