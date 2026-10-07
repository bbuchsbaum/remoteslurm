# Local terminal view of remote workspaces

Status updated 2026-10-06: terminal viewer and the shared WP8a observation foundation
are implemented; the authorized Nibi lifecycle passed on `8bc5a6a`. See the
[user guide](../terminal-workspace.md) for shipped command forms, limits, and
the [qualification receipt](../qualification/activity-observation.md) for exact evidence.
This document preserves the original target design and acceptance criteria.

Implementation choices for the first viewer: consistent tabs at both widths,
with a split file tree/preview above 100 columns; `/` edits the remote path;
bounded repeated log tails use expiring shared requests instead of a continuous
stream lease. The file cache has a stricter combined 64 MiB cap and directory
captures a 1 MiB metadata cap. Scheduler jobs are shown from local registrations;
canonical campaign bindings also recover array and packed member identities. Artifact
and validation columns show independently evidenced states; validation remains `NOT_RUN`
until explicitly performed. Passive campaign recovery survives daemon restart.

## Outcome and first complete experience

Give a person using Claude Code or Codex a second local terminal where they can
see remote jobs, browse files, inspect logs, and follow recent activity. Give the
agent the same observations through structured tools. The agent and viewer stay
on the user's machine; the remote side retains the existing Python helper and
Slurm jobs.

The first complete experience is: authenticate in the second terminal, open the
workspace, submit through the agent, see the job appear, open its log and outputs,
close the viewer, and reopen it with recorded history and accurate observation
ages. A second viewer must not create another scheduler polling loop.

The user requested a plan for this workflow. Textual, the command forms, limits,
and delivery phases below are proposed implementation decisions. This document
does not authorize deployment, publication, or remote test submissions.

## Foundation at planning time (historical)

| Source | Present behavior | Work needed |
| --- | --- | --- |
| [`cli.py`](../../src/remoteslurm/cli.py), `cmd_connect` | Authenticates, establishes the SSH master, checks the helper, and returns. Agent output is not streamed to this terminal. | Enter a local viewer only when requested. |
| [`config.py`](../../src/remoteslurm/config.py), `ProjectConfig`; [`sync.py`](../../src/remoteslurm/sync.py), `resolve_project` | Configured local/remote root pairs; selection by name or deepest matching local root. | Reuse project resolution and add durable observation identity. |
| [`cluster.py`](../../src/remoteslurm/cluster.py), `ls`, `stat`, `read` | File metadata and bounded file reads. | Cache observations and add browsing that bounds enumeration work. |
| [`stub.py`](../../src/remoteslurm/stub.py), `op_ls` | Pages returned entries after `os.listdir` and sorting all names. | A response limit alone is insufficient for huge directories. |
| [`job_monitor.py`](../../src/remoteslurm/job_monitor.py), `WatchStore`, `JobMonitor`, `collect`; [`daemon.py`](../../src/remoteslurm/daemon.py) | Persistent narrow job subscriptions, ownership lock, shared queries within that monitor, heartbeat, restart recovery. | Integrate with the planned activity observer and share collection across both. |
| [`activity/model.py`](../../src/remoteslurm/activity/model.py), [`attention.py`](../../src/remoteslurm/activity/attention.py) | A0 event, semantic-context, and attention contracts are implemented. | A1–A4 storage, producers, general observation, and MCP delivery remain open. |
| [`server.py`](../../src/remoteslurm/server.py), `_get_cluster`; `cli.py`, `get_cluster` | MCP can connect directly while CLI operations can use the daemon. | Record activity at shared operation boundaries, not only daemon interception. |
| `cli.py`, `_rsync`; `sync.py`, `sync`; `cluster.py`, file operations | Some transfers are CLI subprocesses; sync and file operations have other entry points. | Record equivalent transfer/edit outcomes without duplicating logical operations. |

Reuse [WP8a's activity plan](activity-journal.md) and the
[versioned activity contract](../activity-contract.md). This plan adds workspace
and terminal consumers. WP8b resource advice and WP6 campaign filesystem commands
are separate work; neither is required for a first viewer.

## Terminal workflow and lifecycle

```sh
# Terminal A: the user runs their coding agent as usual.
# Terminal B: authenticate, then display a configured project.
rslurm connect mycluster --view --project analysis

# Attach later using the existing connection; no interactive MFA here.
rslurm -H mycluster view analysis

# Inspect an explicit remote root without configuring synchronization.
rslurm -H mycluster view --path /scratch/user/analysis

# Retained observations only: no SSH, daemon startup, or subscription changes.
rslurm -H mycluster view analysis --offline

# Scrolling text for narrow/noninteractive terminals; no alternate screen.
rslurm -H mycluster view analysis --plain

# Structured cached state for agents and scripts.
rslurm -H mycluster workspace analysis --json
rslurm -H mycluster workspace analysis --after CURSOR --json

# Request an observed directory page, returning a request ID.
rslurm -H mycluster workspace-inspect analysis --kind list --file .

# Stop this workspace's persistent observation, leaving jobs untouched.
rslurm -H mycluster monitor remove --project analysis
rslurm -H mycluster monitor remove --path /scratch/user/analysis
```

Keep plain `connect` behavior unchanged. With `--view`, check terminal capability,
arguments, and the optional UI dependency before starting MFA. Authenticate in
the foreground before entering the alternate screen. If connection succeeds but
the viewer fails, report those outcomes separately; do not undo the connection.
Reject `connect --view` on a non-TTY before network work and point to the plain
viewer or JSON snapshot command.

`view` selects an explicit project, then the project containing local cwd. If
neither resolves, an interactive project chooser appears; it does not enroll all
projects. An explicit `--path` is mutually exclusive with a project and has no
implied local mirror. Plain mode requires an unambiguous selection and returns
an actionable error otherwise. Offline explicit paths must resolve from a prior
cached mapping; they cannot expand remote environment variables without SSH.

Attaching an online view creates or reuses a persistent subscription for that
workspace and starts/wakes the local daemon. Enrollment is scoped to that root
and associated work; it does not turn on observation for all jobs on the host.
This extends WP8a's subscription model with a workspace scope. Print that
monitoring continues after exit, with the exact removal command. Repeated attach
is idempotent. New operations matching the subscribed workspace enroll through
shared producers, including MCP and direct Python callers.

`q`/Ctrl-C restores the terminal and releases viewer-owned inspection leases. It
does not cancel jobs, terminate the SSH master, stop another viewer, or remove
persistent subscriptions. Explicit removal stops only that subscription;
overlapping watches retain their ownership. `--no-daemon` permits cached display
with a suspended label and no hidden foreground collector. The UI must not
write into the agent's terminal, start the agent, or depend on terminal-specific
split-pane automation.

Both terminals must use the same local user/state namespace and compatible
remoteslurm installations. Expose the resolved configuration path, state path,
target identity, and client/daemon build identity in diagnostics so different
`REMOTESLURM_STATE_DIR` values or stale MCP installations explain an empty view.
An agent inside a separate container or on another machine is outside this
first shared-state guarantee. Local recording failures, including sandbox
permissions, remain visible without changing an accepted remote outcome.

## Shared architecture and contracts

```text
Agent CLI / MCP / Python operations       Terminal viewer / plain feed
                  |                                  |
          shared operation producers          cached queries + inspect requests
                  |                                  |
                  +------ local SQLite state --------+
                                   |
                         one local observer owner
                                   |
                     existing SSH connection + helper
                                   |
                  Slurm / declared outputs / selected files
```

**Workspace identity.** Add a small `workspace/` module using the activity store
and its migrations. A workspace identifies the configured target and resolved
absolute remote root; project name and local root are presentation/selection
metadata. Pin endpoint/user and root identity. A changed alias destination or
remote-root expansion produces a different identity or an explicit conflict.
It must never silently inherit another target's cache or subscription.

Associate work using explicit project/workspace context when supplied, otherwise
recorded remote cwd contained within a configured root. Use deepest-root matching
and retain its basis. Ambiguous, inaccessible, or unmapped work stays visibly
unassigned. Do not derive membership from job names or scan every scheduler job.
Pin project configuration context on events; later project renaming must not
rewrite historical meaning. Use the existing exact array/packed attempt mapping
and semantic snapshots for labels, logs, and output contracts.

**Snapshot and changes.** Expose `workspace_snapshot` and `workspace_changes` in
the Python layer, the `workspace` CLI command, and one core MCP `workspace` tool
whose optional `after` selects changes. Cached reads return immediately without
SSH or daemon startup. Reuse the A1 query engine and cursor contract. The initial
snapshot and cursor are atomic in local storage, but remote sources were sampled
at different times and must carry their individual timestamps.

The versioned response includes workspace identity/roots, active work, attention,
observed file summaries, recent activity, monitor health, section coverage,
`next_cursor`, `has_more`, and history gaps. Changes contain bounded events and
changed projection records, with explicit removals. Cursor identity includes the
workspace/filter and journal. Readers are independent. An expired cursor asks
the client to resnapshot with a visible gap. Recompute freshness at read time,
including when the latest event sequence has not changed.

Start with 25 work records, 100 events, 200 file entries, and a combined 128 KiB
response cap. Every section states omitted counts when known and continuation
when available. Metadata can say unknown; an absent page is not an empty result.
Expose independent detail pagination so a large file section cannot hide an
attention item. Do not echo arbitrary environment values or full scripts into
ordinary activity records.

**Explicit inspection.** Add Python/core MCP `workspace_inspect` and CLI
`workspace-inspect` request surfaces for `list`, `preview`, `tail`, or `refresh`.
Requests enter a bounded local
queue; the daemon performs the remote read and stores an observation. Return a
request ID and cached result if available, with completion/error state available
on later reads. Do not hold an agent call open indefinitely. UI and agent share
this service, cache keys, and limits; current `ls`/`read` tools remain available.
Coalesce equivalent in-flight requests by target/path/read parameters and expose
their completion even when no content changed. Disabled monitoring/offline mode
rejects remote inspection with an actionable result.

Update the agent skill to read a workspace snapshot when beginning or resuming
remote work, retain its cursor, and request changes at meaningful task boundaries.
Have the agent report the host/root and viewer attach command. Reading cached
state does not start monitoring or wake an idle agent; automatic turn hooks and
wakeups require client integration and are not promised by this feature.

**One observer.** Extend A3 around the existing daemon and `JobMonitor`; extract
a common scheduling/collection seam so legacy background watches and new
workspace subscriptions share known-job batches. Preserve legacy conditions,
notifications, and result persistence. Merely opening another consumer performs
zero additional scheduler reads. Different explicit file requests may consume
the shared bounded inspection budget.

The observer never invokes `ensure`, submit, apply, drive, retry, cancel, or
validators, and never commits remote campaign state or execution-ledger updates.
Keep observation ahead of a stale campaign projection visibly distinct. Preserve
accepted/unknown submission outcomes if local recording fails. Journal history
is presentation evidence, not execution authority.

## Files, logs, and operating limits

Create a bounded directory-snapshot operation using incremental enumeration,
with caps on entries inspected, metadata calls, bytes, and elapsed work. Do not
implement the viewer by repeatedly calling today's all-names `op_ls`. Paginate
an explicitly identified captured snapshot, with expiring path-bound cursors and
a bounded snapshot cache. A directory changing during capture is not an atomic
filesystem snapshot. Mark truncated/incomplete coverage; expired captures require
refresh. Preserve the legacy `ls` interface unless compatibility tests justify
an additive extension.

For v1, a capture that reaches its scan cap remains explicitly partial; it does
not promise eventual enumeration of an arbitrarily large flat directory. Keep
direct known-path inspection available, show this limit in the UI, and permit
navigation to a narrower directory. Do not label local-page filtering as a way
to find files that were never captured. Resumable exhaustive enumeration is a
separate extension requiring its own cursor/resource contract.

Proposed initial budgets, adjustable only within enforced limits:

| Operation | Initial bound |
| --- | --- |
| Scheduler and usage | Reuse A3: 30-second scheduler target, batches at most 1,000 known IDs; usage at least 60 seconds apart. |
| Directory capture | At most 4,096 inspected entries, 2 MiB metadata, and a 2-second cooperative scan budget; remote read deadline 10 seconds. |
| Directory presentation | At most 200 entries / 64 KiB per page; indicate partial capture and observed time. |
| Text preview | 64 KiB; binary files show metadata in v1. |
| Selected live log | At most 64 KiB per read, no faster than 5 seconds; one selected stream per viewer, coalesced across viewers. |
| Background files | Declared outputs/markers only under A3 budgets; no recursive root scan. |
| Inspect requests | At most 32 queued and one in flight per host; coalesce duplicates, cancel obsolete queued requests, return explicit busy/deferred status. |
| File cache | 64 MiB metadata and 32 MiB preview/log cache per state namespace; evict least recently used entries; preserve freshness/eviction metadata. |

Keep cached contents and SQLite files within the existing same-user trust
boundary with restrictive permissions. Ordinary activity records retain paths
and summaries; previews/log buffers have their own bounded eviction policy.

A filesystem syscall can outlast a cooperative scan deadline. Use the existing
slow-worker/timeout boundary and cap in-flight work; test it rather than claiming
hard preemption. Schedule inspection within A3's per-host RPC limits, with
reserved scheduler opportunities so browsing cannot starve state updates. Keep
at most two host workers and no catch-up burst after sleep. Directory snapshot
tokens expire after five minutes; eviction also expires them explicitly.

Expand folders on demand. Show path, type, size, mtime, symlink target, observation
age, coverage, and last observed change. Navigating beyond the chosen root or
following an escaping symlink requires explicit navigation and updates the scope
label; recursive traversal never follows links. Observe permission errors and
vanished paths without discarding the previous snapshot. A name absent from a
partial capture is not proven deleted. External edits appear only when their
paths are sampled, with unknown actor and observation time.

Tail selected logs through expiring leases renewed by the viewer. A crashed or
closed viewer stops renewal; a lease expires after 30 seconds. Detect truncation
and rotation from available file identity/size observations, reset the read
position, and show a gap when identity is insufficient. Keep bounded buffers and
deduplicate repeated bytes; no whole-log download for a tail. Sanitize terminal
control sequences and render filenames/logs as literal text, including markup.

Record write/edit and sync/put/get requests and outcomes through a shared activity
producer; adapt the CLI rsync path to it. A failed transfer may have copied some
files: retain partial/unknown outcome rather than claiming no change. Record a
bounded summary and source/destination identity. Do not infer per-file checksums,
continuous transfer percentages, or content equality from an rsync return code.
Local/remote presence and observed metadata are separate facts; verified equality
requires recorded content evidence.

The first viewer is for observation and inspection. For retrieving a result it
shows a safely quoted existing `rslurm get` command and destination guidance;
the user or agent executes the transfer. Integrated download/open controls,
editing, cancellation, retry, filesystem mounts, whole-project mirroring, image
viewers, browser hosting, multi-machine state synchronization, and resource
recommendations are deferred.

## Terminal interface

Use Textual as an optional `tui` extra, imported only by the interactive command.
The base package, CLI JSON/plain modes, MCP server, and remote helper do not
depend on it. Pin a compatible version range after checking supported Python
versions. [Textual's widgets](https://textual.textualize.io/widget_gallery/)
provide the tree/table primitives; populate them from our observation service.

At 120 columns show a file tree beside a work table/detail area, with a compact
activity feed below. At 80 columns use Files, Work, Logs, and Activity tabs; at
smaller sizes retain navigation and a resize/plain-mode hint. Header: host,
project/root, connection/monitor status, and observation age. The selected job
links to its recorded cwd, log paths, expected outputs, and exact attempt.

Keyboard: Tab changes focus, arrows navigate, Enter inspects, `/` filters the
loaded view, `r` requests bounded refresh, `?` shows help, and `q` exits. Label
filter coverage so loaded-page filtering cannot masquerade as remote search.
Provide a path-copy fallback as selectable text; do not depend on clipboard
escape support. Color supplements state text. Preserve scroll/focus/selection
when updates arrive, and show a new-activity count while the user reads history.

Render cached data first and perform no remote work on the UI thread. Cover empty
projects, initial loading, partially loaded directories, unknown submission,
completed-but-unvalidated work, stale telemetry, permission denial, lost auth,
expired cursors, journal failure, and changed targets. Reconnect instructions
use the existing structured action and require the user's terminal MFA. Neither
the viewer nor the observer prompts for MFA in the background.

## Delivery sequence and ownership

Keep one writer per shared module during implementation. The dependency order
below concerns deliverables; UI fixtures can be developed after V1's response
contract is fixed. They do not qualify the live workflow.

| Phase | Deliverable and likely paths | Acceptance boundary |
| --- | --- | --- |
| Foundation: existing A1–A4 | `activity/`, `jobs.py`, `tasks.py`, `attempts.py`, `campaigns/manager.py`, `daemon.py`, `job_monitor.py`, `server.py` | Transactional journal, independent cursors, all-client producers, passive recovery, shared read-only observation, and core MCP activity. Reuse existing tasks. |
| V1: workspace state | New `workspace/model.py`, `query.py`, `service.py`; activity migrations; `config.py`, `sync.py`, `cli.py`, `server.py` | Stable root/target identity; bounded local-only snapshots and changes; same facts for Python, CLI, and MCP; truthful unmapped work and gaps. |
| V2: files and inspection | New `workspace/files.py`; `stub.py`, `cluster.py`, `sync.py`, CLI transfer adapters, observer integration | Truly bounded listing, previews/tails, shared request queue, file/transfer activity, exact work/file relationships, and visible partial coverage. |
| V3: terminal client | New `tui/`; `cli.py`, `pyproject.toml`, lockfile | Independent terminal, optional dependency, `connect --view`, attach/offline/plain paths, responsive navigation, and correct exit/lease/subscription lifecycle. |
| V4: qualification and docs | `tests/`, `tests/live/`, `.github/workflows/ci.yml`, README, agent guide, skill references | Two-terminal workflow and faults proved against the final candidate; published claims match the evidence. |

The first useful development checkpoint is A1+A2 plus V1: a person and agent can
read the same recorded workspace activity. It is not yet continuous monitoring.
The first product milestone is V1–V3 with the functioning A3/A4 foundation and
the V4 acceptance exercise below. Finish WP8a's qualification as a release gate;
do not rename a fixture-backed UI demo as operational readiness.

Mote is authoritative for status and dependencies:

| Work | Issue |
| --- | --- |
| Existing WP8a epic | `bd-01M2ZFABZXW9EP5VKPJ5B8W9J7` |
| A1.1 journal | `bd-01M2ZFC39QNXCZ58GWK2ZQ50WB` |
| Terminal workspace epic | `bd-01M49EP3E4RR2R1BEVPFKJ5W8G` |
| V1; depends on A1.2 and A2.1 | `bd-01M49EP3KXK80VTH8PCTK0A9XF` |
| V2; depends on V1 and A3.2 | `bd-01M49EP3T764KMGB5HXTQ6J7RS` |
| V3; depends on V2 and A4.1 | `bd-01M49EP40GJ70ZXNCA6NG56ZTE` |
| V4; depends on V3 and A4.2 | `bd-01M49EP460E63NDF662CT8YW19` |

## Verification and release evidence

Add focused tests with independent expected facts, including:

- Two independent producer processes (CLI and MCP), two viewers, and an agent
  cursor: no lost/consumed events, equivalent identity and attention, and zero
  extra scheduler calls for additional readers. Include existing background
  watches in the same call-count assertion.
- Offline snapshots and rendering: spies fail on any SSH or daemon-start call.
  Clock advancement alone expires freshness. Retention gaps force resnapshot;
  new events during snapshot-to-follow handoff are neither skipped nor repeated.
- Direct Python and CLI rsync operations: one correlated logical operation,
  truthful partial transfer and unknown acceptance, no automatic resubmission
  after journal failure, and no arbitrary payload leakage in ordinary summaries.
- Huge/changing directories: instrument enumeration to prove the work cap,
  independently from response size; test partial absence, eviction, expired
  pagination, slow metadata, links, non-UTF-8 names, and target/root changes.
- Logs: append, rotation, truncation, permission loss, binary data, escape and
  markup injection, viewer crash/lease expiry, and shared polling of the same log.
- Headless UI fixtures at 120x40 and 80x24: keyboard journey, resize, focus
  stability, all material failure states, and responsiveness during slow reads.
  A real PTY exercise separately checks alternate-screen cleanup, Ctrl-C, Unicode,
  non-TTY behavior, and authentication before UI entry.
- Mutation-forbidden spies on all observer/inspection paths. Stopping/removing
  a view or monitor subscription never invokes a scheduler mutation or validator.

Run the affected tests for each phase. At integration, run the repository's
existing commands (these were inspected, not executed for this plan):

```sh
uv run --no-sync pytest -q
uv run --no-sync ruff check src tests
uv run --no-sync ruff format --check src tests
uv run --no-sync mypy
uvx vermin -t=3.6- --no-tips --violations src/remoteslurm/stub.py
uv build
git diff --check
```

Add CI installation/testing of the `tui` extra alongside the base-install path.
Preserve the current Linux/macOS, Python 3.11–3.13 matrix and Python 3.7 helper
runtime smoke (`python tests/stub_smoke.py` in its existing CI environment).

The candidate-specific live exercise uses an authorized small scratch workspace
and configured site resources. In one terminal authenticate/open the viewer; in
another submit via CLI and MCP, write/edit and sync a tiny script, run successful
and failing work, inspect logs/outputs, and explicitly validate results through
the existing API. Close/reopen the viewer, disconnect the agent, restart the
daemon, and exercise loss/recovery of observation. Confirm two viewers share
collection and that a finished job with missing/unchecked outputs stays distinct
from verified results. Reuse WP8a's array/pack recovery tests instead of making
a duplicate campaign qualification suite.

Record source/helper hashes, job IDs, workspace path, observation times, RPC and
byte counts, UI evidence, and validation receipts. Simulate auth expiry/sleep in
automated tests; document any live cases not exercised. Report local tests,
terminal inspection, hosted CI, live qualification, and release separately.

## Completion boundary

The local journal, shared producers, daemon-owned workspace observation, bounded files,
structured tools, optional terminal view, passive canonical recovery and semantic
artifact/validation projection are implemented. The authorized small-array/mixed-pack
lifecycle recovered four units after agent exit and daemon restart. See the linked
qualification report for automated, terminal and live evidence. Hosted CI, publication
and release have not been performed; earlier WP4/WP5 gates and WP8b remain separate work.
