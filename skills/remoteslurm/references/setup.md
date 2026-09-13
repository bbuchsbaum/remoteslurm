# Set up remoteslurm

Use this reference only for installation, configuration, connection, MCP registration, or
connectivity diagnosis.

## Install and configure

remoteslurm currently installs from a source checkout and requires Python 3.11 or newer locally.
From this checkout, the documented installation is:

```bash
uv tool install .
remoteslurm --version
```

Create an example config with `remoteslurm config --init`, then edit the generated host. Never
present example accounts, partitions, paths, templates, or module commands as real site values.

The host's `ssh` value names an OpenSSH alias. Prefer expressing hostnames, usernames, jump hosts,
and multiplexing in `~/.ssh/config`. MFA sites normally need a persistent ControlMaster so the
user authenticates once in a visible terminal and later calls reuse it.

```bash
remoteslurm connect <host>
remoteslurm doctor --host <host>
```

The user must perform interactive MFA. Preserve a structured error's `action` or `connect_cmd`
exactly. Reconnect only when `connection` shows that the master is unavailable or expiring.

`doctor` checks SSH, remote Python, stub startup, writable remote home, and Slurm tools. Address
its failing check. Set the host's `python` when the login node's default `python3` is unsuitable.

## Store site knowledge

Use host configuration for persistent facts: scheduler defaults, policy `notes`, named resource
`templates`, named sync `projects`, selected environment variables, quota settings, session
lifetime, protected roots, and run policy. After editing, use `info` to verify what agents see.
Do not encode one institution's policy in this skill.

## Register MCP

Generate client-specific configuration rather than guessing it:

```bash
remoteslurm mcp-config --host <host>
```

The default `core` tools cover the normal lifecycle, including packed jobs. Enable `all` only for
optional operations such as detailed queues, quota, sweeps, events, globbing, or diffs. The MCP
resource `remoteslurm://guide` contains guidance for the installed version.
