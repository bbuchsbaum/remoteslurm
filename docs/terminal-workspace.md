# Watch a remote workspace from a local terminal

Keep Claude Code or Codex in one terminal and open the workspace viewer in
another. Both processes run locally, share remoteslurm's configuration and state
directory, and use the existing SSH connection to the cluster. The viewer does
not start an agent on the login node.

Install the optional UI from this checkout:

```sh
uv pip install -e '.[dev,tui]'
# For an isolated command installation instead: uv tool install '.[tui]'
```

Authenticate and open a configured project:

```sh
rslurm connect mycluster --view --project analysis
```

Authentication finishes before the full-screen interface opens. Plain `connect`
still authenticates and returns. To attach later, or select a directory directly:

```sh
rslurm -H mycluster view analysis
rslurm -H mycluster view --path /scratch/user/analysis
```

With no selection, `view` uses the configured project containing the local
working directory, or presents a project chooser. The remote root must exist.
Use the same `--config` / `REMOTESLURM_CONFIG` and `REMOTESLURM_STATE_DIR` in both
terminals. An agent inside a different container or machine needs a separate
state-sharing arrangement; this version does not synchronize journals.

## Reading the view

Work, Files, Logs, and Activity tabs work at both 80 and 120 columns. Files show
a tree beside the preview on wide terminals; a narrow terminal opens previews
in Logs. The header names the host/root, observation age, and evidence needing
attention. Color is supplementary to the text labels.

| Key | Action |
| --- | --- |
| Tab | Move focus |
| Up / Down | Select a file or directory in the tree |
| Right / Left | Expand or collapse a folder; move to its child or parent |
| Enter | Expand a directory or inspect a file |
| `f` / Esc | Focus the file tree; return from the path input or a preview |
| `/` | Enter a remote path |
| `l` | Open the selected job's recorded stdout log |
| `r` | Refresh the selected file/folder and request another scheduler observation |
| `?` | Show keyboard help |
| `q` / Ctrl-C | Close the viewer |

Opening the Files tab focuses its tree. Use `/` to edit the directory path,
then Enter to browse it or Esc to return to the tree. Move focus out of the path
input before using letter shortcuts. Selected jobs
show recorded paths and a quoted `rslurm get` command for retrieving output.
Directory links show their target and are not recursively followed; explicitly
enter a directory target in the path input to navigate there.

Closing the viewer leaves jobs, SSH, and the persistent workspace subscription
running. Selected-log reads stop when the viewer stops requesting them; queued
reads expire after 30 seconds. To stop a workspace's background observation:

```sh
rslurm -H mycluster monitor status --project analysis --json
rslurm -H mycluster monitor remove --project analysis
rslurm -H mycluster monitor watch --campaign study --run-id RUN
rslurm -H mycluster monitor status --json
rslurm monitor disable
```

This does not cancel jobs or stop independent job watches. The local daemon owns
collection, including when multiple viewers are attached. It never submits,
retries, cancels, drives a campaign, or invokes validators as part of observation.

## Cached and structured views

```sh
rslurm -H mycluster view analysis --offline
rslurm -H mycluster view analysis --plain
rslurm -H mycluster view analysis --offline --plain --json
rslurm -H mycluster workspace analysis --json
rslurm -H mycluster workspace analysis --after CURSOR --json
rslurm -H mycluster activity --since 30m --follow --json
```

Offline mode reads retained observations and file captures without SSH, daemon
startup, or enrollment. `--no-daemon` also selects cached mode. Plain mode writes
scrolling output without an alternate screen; `--json` emits JSONL. An offline
plain invocation emits one snapshot. An explicit root must have been opened
online before it can be resolved from the cache.

The core MCP tools `workspace`, `activity`, and `workspace_inspect` expose the same
local data. Each reader keeps its own `next_cursor`; readers never consume each
other's events. When `resync_required` is true, read a new snapshot without
`after`. Check `history_truncated`, `snapshot_omitted`, and `work_omitted` rather
than interpreting omitted information as absence.

Inspection queues a bounded remote read and returns immediately:

```sh
rslurm -H mycluster workspace-inspect analysis --kind list --file . --json
rslurm -H mycluster workspace-inspect analysis --kind list --request-id REQUEST_ID --json
```

For an explicit workspace root, replace `analysis` with `--path REMOTE_ROOT`.
Use `--file` for the inspected path. `--offset` and `--token` page a captured
directory without reading it again. Tokens expire after five minutes or cache
eviction. Monitoring must already be enabled for new inspection requests.

## What the evidence means

The initial viewer covers locally registered jobs whose recorded working
directories are under the root, including registrations from before the viewer
opened and durable tasks that registered a scheduler job. It displays at most
25 jobs, prioritizing previously observed active jobs, and reports additional
known records. Use the existing `jobs` command for broader job detail. It does
not discover arbitrary work submitted outside remoteslurm.

`COMPLETED` means scheduler completion. Ordinary registrations remain `UNCHECKED`
and `NOT_RUN` until evidence is available. Enrolled campaigns import canonical
unit/attempt mappings, declared outputs and existing validation receipts. Output
and pack-marker observations update their independent columns; changed output
can make validation stale. Use explicit `ensure` or campaign verification to run
validators. Previous attempts remain labeled as history and do not determine
the current retry's attention state.

Scheduler observations target a 30-second interval. Terminal records are
retained and stop regular scheduler queries; explicit refresh can recheck them.
Unknown accounting or missing array rows remain unknown. Local registrations
provide the job identity basis. Canonical campaign bindings also check the
scheduler attempt marker. Lost responses can be matched without updating the
remote ledger; unresolved identity stays unknown. Commit references pin
historical labels, command names, resources and output contracts. Missing or
omitted context is explicit in the structured reader.

Remote root anchors include the observed login host, user, resolved path and
filesystem identity. A changed endpoint or replaced root suspends observation;
reattaching creates a new workspace identity and does not inherit its predecessor's
jobs. A cluster that moves sessions between login hosts can therefore require
reattachment. Config/build mismatches with an existing daemon are reported.

Directory scans inspect at most 4,096 entries, 1 MiB metadata, or two seconds of
cooperative enumeration. A blocking filesystem call is bounded by the helper's
slow-worker timeout. Captures are explicitly partial when a cap is reached;
unlisted files may exist. Narrow the directory or inspect a known path directly.

Previews and selected tails return at most 64 KiB. Tails read at most once per
five seconds per shared path; rotation/truncation and omitted earlier bytes are
labeled. Binary previews show metadata. These are bounded current tails, not
lossless log archives. Cached file captures share a 64 MiB eviction budget;
completed request receipts last five minutes. Errors retain the prior file
capture, with its original observation time.

The journal retains up to 30 days / 256 MiB of event payloads; all current
documents share a separate 128 MiB admission budget. SQLite disk allocation can
exceed those logical payload totals. Local recording failures are reported
without turning an accepted submission into a failed submission. Never resubmit
solely because `activity_recorded` is false.

The source preview has local automated, terminal and authorized Nibi lifecycle qualification.
Hosted CI, publication and release have not been performed for this candidate.

The completed observation candidate `8bc5a6a` passed 726 tests with 8 opt-in live
tests skipped, plus static checks, package/base-install and actual PTY exit checks.
See [current qualification](qualification/activity-observation.md) for exact
identity, specimens and the passing two-job live gate on `8bc5a6a`. That live check recovered
all four units after submitting-process exit and an isolated daemon restart, verified
the three successful output files, and retained the failed packed member separately.
The older first-viewer
receipt below is historical evidence.

Local qualification recorded on 2026-10-06 for build
`1:0.2.0:785b9ce4da6bf7b0:be065b0b8a2c79c1`: the full suite passed 694 tests with
7 live tests skipped. After the final project-preflight and failed-navigation
fixes, the affected CLI, UI, workspace, daemon and identity tests passed 49 tests.
Ruff, formatting, mypy, the helper's static Python 3.6 floor, and source/wheel
builds passed. The base wheel loaded CLI/MCP without Textual; headless layouts at
80×24 and 120×40 and real PTY `q`/Ctrl-C cleanup were checked. No cluster jobs were
submitted for this qualification.
