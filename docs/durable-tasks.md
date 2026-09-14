# Durable tasks with `ensure`

`rslurm ensure` gives a single batch computation a persistent identity and a replayable result
check. Repeating the same manifest recovers the existing Slurm job, reports its current state, or
revalidates its outputs. The durable record lives on the cluster, so it survives a client process,
session daemon, SSH connection, or laptop disappearing.

## Manifest

```toml
version = 1
name = "fit-subject-01"
host = "mycluster"                 # optional when a default host is configured
script = "scripts/fit.sh"          # local, relative to this manifest
cwd = "$WORK/project"              # remote working directory
inputs = [
  "$WORK/project/data/sub-01.tsv",
  "$WORK/project/scripts/model.R",
]
outputs = ["$WORK/project/results/sub-01.rds"]
validate = [
  "Rscript",
  "$WORK/project/scripts/check-fit.R",
  "$WORK/project/results/sub-01.rds",
]
validation_timeout = 300
template = "cpu"

[resources]
time = "02:00:00"
mem = "8G"
cpus_per_task = 2

[environment]
container = "registry.example/analysis@sha256:0123456789abcdef"
renv_lock_sha256 = "..."
```

`script_inline` may replace `script`, particularly for the Python or MCP interface. `inputs` and
`outputs` are remote regular files. Relative remote paths are resolved under `cwd`; `~` and remote
environment variables are expanded by the stub. Inputs are SHA-256 hashed before the task identity
is computed. Directories, job arrays, and dependencies are outside the version 1 contract.

The validator is an argv array and runs on the login node after Slurm reports `COMPLETED`. It is
also run on every later `ensure`. There is no implicit shell expansion; put environment setup in an
executable validation script and invoke it as `['bash', 'check.sh']` when necessary. Declare that
script as an input so changing it changes the task identity.

The environment table is identity-bearing user data. Pin a container by digest or record lockfile
and module identities appropriate to the site. When no container digest is present, `ensure`
returns an explicit reuse limitation rather than claiming a hermetic execution.

## Recovery and retries

```bash
rslurm ensure fit.toml
rslurm ensure fit.toml --json
rslurm ensure fit.toml --retry
```

Before calling `sbatch`, remoteslurm writes an attempt with state `SUBMITTING` to the remote task
store and assigns it a unique `rse-...` Slurm job name. If the response is lost, the next call
reconciles that marker against `squeue` and `sacct`. A unique match recovers the original job. No
match, or multiple matches, produces `UNKNOWN` and does not submit again.

`--retry` creates a new, retained attempt after `FAILED`, `INVALID`, or `REJECTED`. It does not
override `UNKNOWN`. `--retry-unknown` is a separate explicit escape hatch because the original job
may still exist and duplicate execution is possible.

The externally visible states are:

| State | Meaning |
|---|---|
| `PENDING`, `RUNNING` | One identified Slurm attempt is active. |
| `COMPLETED` | Slurm completed; validation has not yet established a valid result. |
| `VERIFIED` | The validator passed and every output matches the receipt fingerprint. |
| `INVALID` | Validation failed, an output is missing, or verified output bytes changed. |
| `FAILED`, `REJECTED` | Slurm execution or submission failed. |
| `UNKNOWN` | Submission or scheduler identity cannot be resolved without risking duplication. |

The default task store is `~/.remoteslurm/tasks` on the cluster. Set `task_dir` for a host when its
home is not shared or durable:

```toml
[hosts.mycluster]
task_dir = "$WORK/.remoteslurm/tasks"
```

Each task directory contains the canonical contract, all attempts, scheduler evidence, validation
evidence, and the current receipt. The client also mirrors recovered job IDs into its ordinary
local job registry for compatibility with `status`, `output`, and `diagnose`.

## Python and MCP

```python
from remoteslurm import Cluster, TaskSpec

cluster = Cluster.connect("mycluster")
result = cluster.ensure(TaskSpec.load("fit.toml"))
assert result["state"] in {"PENDING", "RUNNING", "VERIFIED"}
```

The MCP `ensure` tool accepts the manifest as an object. Use `script_inline` unless the MCP server
can read the referenced local script path. It returns the same task ID, state, attempt history,
scheduler record, validation evidence, and receipt as the CLI's JSON form.

Before a durable submission, the client checks the exact local daemon build and the remote stub
SHA. A mismatch fails before `sbatch` with an actionable `execution_mismatch` error. Existing task
receipts retain the control-plane identity used for their attempt.
