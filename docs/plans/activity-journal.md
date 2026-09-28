# Shared activity journal and local monitor

Status: proposed; implementation has not started. Reviewed on 2026-09-20 against
`c70e37e6b4323e96c61c7ad9f40793148363cda4`.

## Recommendation

Build a shared local activity journal, a daemon-owned observer, and a terminal feed. Campaigns
are the richest initial consumer; ordinary submissions, arrays, packs, and durable tasks use the
same event envelope. Put a bounded MCP reader in the first release. Defer the dashboard, resource
advice, and general filesystem/command auditing.

The journal is the source for activity views, not the authority for execution. Remote execution
ledgers, committed campaign history, scheduler evidence, and validation receipts retain their
existing authority. The journal records requests and imports or derives explicitly attributed
facts from those sources. It must never authorize a retry or turn a missing observation into a
successful outcome.

“Shared” initially means CLI, MCP, and Python clients using the same local user/state directory.
It does not mean a synchronized multi-machine activity service. Other machines' committed
campaign history can be imported; their unrecorded local intentions cannot be reconstructed.

## Review of the current implementation

| Existing seam | What the code provides | Consequence for this proposal |
| --- | --- | --- |
| [`watch.py`](../../src/remoteslurm/watch.py), [`cmd_watch`](../../src/remoteslurm/cli.py) | A foreground poller writes terminal or unknown-job events to best-effort JSONL. Default readers share one per-host byte cursor. | It confirms the visibility gap. Do not reuse its destructive-read semantics for several consumers. |
| [`JobRegistry.audit`](../../src/remoteslurm/jobs.py) | A separate best-effort action log; ordinary `submit` preserves accepted success when local registry recording fails. | There are already several histories. Consolidate new production through shared hooks and preserve accepted outcomes on journal failure. |
| [`daemon.py`](../../src/remoteslurm/daemon.py) | An on-demand session multiplexer with a four-hour idle default. Higher-level logic lives in clients. | Autonomous observation requires persistent subscriptions, ownership, scheduling, and revised idle behavior. |
| [`CampaignStore`](../../src/remoteslurm/campaigns/store.py), [`op_campaign_events`](../../src/remoteslurm/stub.py) | Committed remote events with bounded pages. Event pagination walks backward through HEAD-reachable history. | The existing cursor is a history cursor, not a subscription watermark. Imports need commit identity and a bounded forward-consumption contract. |
| [`_campaign_drive_attempt`](../../src/remoteslurm/stub.py), [`_merge_submission_outcomes`](../../src/remoteslurm/campaigns/manager.py) | Acceptance is written to the execution ledger before the client separately commits campaign events/views. | A disconnected client can leave accepted work absent from the campaign journal. Watching only that journal is insufficient. |
| [`CampaignManager._refresh`](../../src/remoteslurm/campaigns/manager.py) | Reads the complete snapshot, queries the user's queue, scans all declared outputs, samples telemetry, and commits a new view. Refresh events mostly contain counts and source availability. | Repeating this every 30 seconds would cause avoidable scans, hashes, remote view growth, and writer conflicts. Existing events alone cannot reproduce every historical state change. |
| [`evidence.py`](../../src/remoteslurm/evidence.py), [`reconcile.py`](../../src/remoteslurm/reconcile.py) | Source availability, observation timestamps, identity correlation, and separate state axes. | Reuse these contracts and reconciliation rules; do not create a second interpretation of Slurm state. |
| [`server.py`](../../src/remoteslurm/server.py) | MCP connects directly through `Cluster.connect`; it does not necessarily use the CLI daemon. Legacy `events` is outside the default core tool set. | Daemon-only interception misses MCP activity. Instrument shared domain operations and expose the new local reader in the core set. |

One existing projection needs correction before it becomes the attention feed. Direct local calls
to `_next_action` produced:

| Execution / artifacts / freshness | Current result |
| --- | --- |
| `COMPLETED / MISSING / FRESH`, validation `NOT_RUN` | `complete`: “No unresolved work remains.” |
| `CANCELLED / MISSING / FRESH`, validation `NOT_RUN` | `complete`: “No unresolved work remains.” |
| `RUNNING / ERROR / UNAVAILABLE`, validation `NOT_RUN` | `active`: “Active jobs need no intervention.” |

These are projection checks, not live-cluster reproductions. They show why attention must account
for missing artifacts, cancellation, dependency conflicts, source availability, and evidence age.
Reuse and repair the common projection so campaign status and activity agree. An old `FRESH`
snapshot also needs an age check at read time; losing the monitor must not leave the display fresh.

Review validation: `tests/test_watch.py`, `test_campaign_store.py`, `test_campaign_manager.py`, and
`test_daemon.py` passed together: **37 passed in 28.82 seconds**. No remote operations were run.
The existing campaign plan still records pending WP5 candidate-specific live requalification;
this review does not close that gate.

## Event and query contracts

Every event has a versioned envelope. Use these fields before adding presentation conveniences:

| Field group | Required meaning |
| --- | --- |
| `schema_version`, `event_id`, `seq` | Stable logical event identity and locally assigned increasing ingestion sequence. Sequence orders the journal, not remote causality. |
| `kind`, `provenance`, `source` | What happened; whether it is a request, remote record, scheduler/filesystem/validator observation, or derived assessment; the concrete producer/source. |
| `recorded_at`, `observed_at`, optional `occurred_at` | Local durable ingestion time, evidence observation time, and source event time only when actually known. Preserve clock/source limitations. |
| `actor`, `operation_id`, optional `causation_id` | CLI/MCP/Python/monitor origin and correlation across a request and its outcome. Agent/session identity is supplied explicitly or left unknown. |
| `subject` | Host identity, campaign/definition/run/stage/unit/group/attempt/task identifiers as applicable; scheduler parent, child, and packed slot remain distinct. |
| `evidence_ref`, `payload`, `limitations` | Exact remote commit/record/receipt reference, bounded factual content, and coverage or uncertainty. Derived events cite their inputs and projection version. |

Use a stable configured target identity in addition to the human host alias. Pin subscriptions to
the resolved endpoint/user and campaign/task roots; detect a changed target instead of joining
unrelated clusters under a reused alias. Correlate jobs with submission time and attempt marker
where available. Legacy/adopted jobs retain explicit weaker identity confidence.

Initial event families:

- `operation.requested`, `operation.returned`, `operation.failed`, `operation.outcome_unknown`.
- `submission.intent_recorded`, `submission.accepted`, `submission.rejected`,
  `submission.recovered`, `submission.unresolved`.
- `scheduler.observed`, `artifacts.observed`, `validation.recorded`, `validation.stale`.
- `campaign.changed`, `resources.sampled`, `attention.raised`, `attention.resolved`.
- `monitor.started`, `monitor.suspended`, `monitor.resumed`, `monitor.gap`, `journal.degraded`.

An acknowledged `sbatch` response can support acceptance without a remotely persisted receipt;
label that difference. A lost response is an unknown outcome, not rejection. An imported durable
ledger record can later establish acceptance. A scheduler match recovered later must retain its
recovery source and uncertainty about the original acceptance time.

Keep execution, artifacts, validation, dependency, and freshness separate in payloads and summaries.
“12 outputs present” must specify whether it counts declared outputs, resolved files, or units
satisfying their output contracts. Resource summaries count allocations once and carry sample
coverage. Existing lifetime CPU averages must be labeled as such; interval utilization belongs to
the later WP8 calculations. Missing telemetry is unavailable, not zero use.

`attention` is a reproducible advisory projection. Give each issue a stable key, evidence refs,
severity, suggested operator action, and authorization state (`required`, `available`, `consumed`,
or `unknown`). Assert “retry is not authorized” only from current authorization evidence. Emit on
issue creation, material change, or resolution; do not repeat the same warning every poll.

## Local storage and readers

Use a local SQLite database through the standard library, with WAL, explicit durable transactions,
bounded lock waits, and a schema version. Keep it on local storage. SQLite is preferable here to
extending the existing JSONL cursor protocol because event insertion, deduplication, projection
updates, and import checkpoints need one transaction across several processes. JSONL remains the
stream/export format.

Maintain immutable event rows plus separate mutable tables for subscriptions, import checkpoints,
current projections, and monitor health. The journal is an observation history, not a new campaign
database. New records correct earlier knowledge; they do not rewrite old claims.

- Give imported events unique keys from source identity plus remote event/commit identity. Local
  operation IDs distinguish repeated requests from repeated delivery of the same request.
  Execution-ledger observations instead key stable attempt facts by attempt identity, fact kind,
  and relevant content digest; the ledger itself is mutable. Retain the bounded evidence used to
  make the claim locally rather than relying on a path whose contents will later change.
- Commit imported events and their watermark atomically. Replaying a page is harmless. Coalesce
  unchanged observations using persisted comparison state, so restart does not replay transitions.
  Do not globally deduplicate equal state values: `RUNNING -> PENDING -> RUNNING` is meaningful.
- Query with an opaque cursor containing journal identity and sequence. Bind filtered pagination
  to its filter, enforce event-count and byte limits, and return `next_cursor`, `has_more`, and the
  retained-history boundary. Reading never consumes another reader's position.
- Default to the latest 100 matching events, displayed in ingestion order. `--since 30m` filters
  local ingestion time, so a receipt recovered now remains visible even if its source timestamp
  is old. Display both times when they differ materially. Cursor continuation is the reliable
  resume mechanism; relative time is a convenience query.
- Read a snapshot and its cursor in one transaction, then follow strictly after that cursor.
  `--follow` polls only the local journal and never starts its own scheduler loop. JSON follow
  emits one versioned JSON object per line; diagnostics go to stderr.
- Bound payloads and response bytes from the first release. Propose configurable retention of
  30 days / 256 MiB, pruning only an old contiguous event prefix. Retain current projections,
  active identities, import checkpoints, and remote evidence references. An expired cursor returns
  an explicit history-gap result and a resynchronization cursor; it never silently skips history.
  Append-only means immutable retained records, not infinite retention.

Local storage failure must not recast a scheduler-accepted job as a failed submission. Return the
accepted job ID plus `activity_recorded=false` and a recording warning; write a bounded stderr or
MCP diagnostic when the journal itself is unavailable. Preserve existing submission preflight and
retry contracts. This activity layer cannot promise complete auditing during disk failure. Remote
campaign facts are recoverable; unrecorded local requests may not be.

## Producers and passive recovery

Instrument shared operation boundaries in `jobs.py`, `tasks.py`, `attempts.py`, and
`campaigns/manager.py`, with CLI/MCP supplying actor context. Avoid logging every low-level RPC.
An array or pack has one correlated high-level request, constituent scheduler attempts, and a
bounded unit summary. Record cancellation requests separately from cancellation acknowledgement
and observed terminal state. Record validation only when the existing verification path runs.

Two small remote read extensions are prerequisites for reliable recovery:

1. A bounded, genuinely read-only view of the canonical campaign execution ledger and durable
   task records, including attempt identity and receipt content/digest. Do not read an eventually
   updated group mirror as if it were the canonical ledger. Never invoke `ensure`, `apply`, or
   `_campaign_drive_attempt` from the observer: those can submit an `INTENDED` attempt.
2. A commit-pinned campaign change interface. Expose HEAD-reachable commit metadata and bounded
   historical views/deltas, with a stable high watermark and event ordinal. Add forward `after`
   semantics without changing the existing newest-first history cursor. Never combine an old
   event with today's mutable view and call it historical evidence.

Use these to import missing accepted/rejected/unresolved attempts, committed validation receipts,
and lifecycle changes after client loss. Initial synchronization records a baseline and an explicit
history coverage boundary. Advance the source watermark only after every page through the pinned
target commit has been imported; work arriving later belongs to the next pass. If only baseline
evidence is available, say “observed state,” not “transition occurred at this time.”

For unknown job IDs, permit a separately budgeted, read-only recovery lookup for a *known attempt
marker and submission window*. Preserve source availability and ambiguous matches. This is the
narrow exception to ID-only scheduler queries; it does not authorize resubmission. A local recovery
observation need not mutate the remote execution ledger.

## Monitor lifecycle and operating budget

Put the observer inside the existing local daemon, using a dedicated bounded worker rather than
request-handler threads. Hold one monitor ownership lock per local state namespace, independent
of socket overrides. A second daemon may serve requests but cannot start a duplicate observer.
Local activity reads work with no daemon and no SSH connection.

Enable monitoring explicitly per host. Enabling enrolls known active registry jobs, discoverable
open runs, and closed runs with unresolved retained attempts. Page campaign discovery slowly;
do not list every Slurm job. New submissions/adoptions on an enabled host persist a subscription
before attempting to wake the daemon. MCP and direct Python clients use this same path, even
though their workload RPCs may bypass the daemon. Disabled monitoring still permits journaling.

Proposed commands, using the existing host option consistently:

```sh
rslurm -H nibi monitor enable
rslurm -H nibi monitor status
rslurm -H nibi activity --since 30m
rslurm -H nibi activity --follow
rslurm -H nibi activity --after CURSOR --json
rslurm -H nibi campaign watch study --run analysis-1
rslurm -H nibi monitor disable
```

`campaign watch` explicitly subscribes that run and attaches a filtered view. Its startup message
states that observation continues after Ctrl-C and gives the command to unsubscribe that run.
Add `monitor remove --campaign study --run analysis-1` and equivalent job selection. Stopping a
viewer never cancels jobs. `--no-daemon` records and reads activity but clearly reports suspended
background collection; it must not quietly start a second foreground implementation.

An enabled host with pollable subscriptions keeps the daemon alive beyond its transport idle
timeout. Persist suspended targets and next-check metadata. After daemon restart, resume from
checkpoints. Agent disconnect is covered; laptop sleep, power loss, expired MFA, and daemon death
cause observation gaps. There is no new login/reboot supervisor in this slice. The next daemon
start resumes monitoring, and both cached views and MCP expose a missing/stale heartbeat meanwhile.

Starting budgets, to be enforced and measured rather than described as “modest”:

| Work | Initial policy |
| --- | --- |
| Scheduler | 30-second target cadence with jitter; only known IDs, batches of at most 1,000, shared across subscriptions. |
| Usage | At least 60 seconds between samples; distinct active allocations only, with coverage and source timestamps. |
| Artifacts/markers | Attempt-scoped markers and declared output paths, changed/terminal work first; bounded batches with persisted continuation. Idle output rechecks at a slower configurable cadence. |
| Hashes | Respect existing contract caps plus a total per-pass byte budget. Defer excess work and mark coverage partial; never claim a skipped hash refresh established current validity. |
| Discovery/history | Small paged passes; expose backlog and last fully imported remote revision. Never scan entire history on every tick. |
| Host concurrency | One collection pass per host; explicit RPC/byte budgets and deadlines; no overlap or catch-up burst after sleep. Multiple subscribers share observations. |
| Log excerpts | On failures or selected meaningful transitions, at most 20 lines / 4 KiB per stream and a bounded number of affected units per pass. |

Start with eight remote calls per host pass, a 10-second deadline per read, at most 1,024 output
contracts and 64 MiB of file hashing per pass, and three units' log excerpts per failure pass.
Use at most two host workers, fair continuation, and separate scheduler/artifact work queues so
large scans cannot starve status checks. Partial coverage is expected when these budgets bind;
publish backlog and age rather than promising every unit was refreshed within 30 seconds. Tune
these defaults from qualification evidence without removing hard limits.

Extract collection from reconciliation and persistence instead of repeatedly calling today's
whole-run `_refresh`. Reuse the extracted collector for explicit status refresh as well. Persist
campaign observation changes coherently at a pinned revision, with bounded CAS retry. Unchanged
polls update local observation health without rewriting all remote campaign views. Preserve useful
freshness checkpoints at a slower rate and retain exact references for any committed change.

On authentication or transport failure, emit one suspended event, back off, retain last evidence
with its original age, and report the existing structured reconnect action. Never attempt MFA.
An active-state observation alone does not mean no attention is needed if evidence is stale.
Retire fast scheduler polling after terminal accounting settles; retain slower artifact/receipt
checks for subscribed runs. Archived runs receive no new remote refresh work. Runs closed with
active attempts remain observable, and permanently unresolved runs are visible but backed off.

Log tails are evidence attachments with source path, attempt ID, capture time, and truncation.
Deduplicate repeated captures; aggregate array failures with bounded examples. Do not put raw
scripts, environment values, tokens, or arbitrary command output in ordinary event summaries.
Captured logs remain potentially sensitive local data; use restrictive file permissions and
terminal-control escaping. Log collection failure cannot block state collection.

## Delivery sequence

| Step | Deliverable and principal files | Acceptance boundary |
| --- | --- | --- |
| A0: Semantics and correctness | Event schema/fixtures; repair shared attention rules in `campaigns/manager.py`; define source identity, cursor, freshness, and retention contracts. | Missing outputs, cancellation, blocked/conflicting dependencies, stale validation, and unavailable sources never produce a misleading all-clear. A suggested action is never authorization. |
| A1: Journal and terminal reader | New `activity/model.py`, `store.py`, `query.py`, `render.py`; `cli.py` local `activity` commands. | Concurrent writers, two independent readers, crash replay, bounded pagination, history-to-follow handoff, and expired cursors work without SSH. |
| A2: Producers and recovery | Shared hooks in `jobs.py`, `tasks.py`, `attempts.py`, `campaigns/manager.py`; bounded ledger and commit-pinned reads in `stub.py`/`campaigns/store.py`. | CLI, direct Python, and MCP emit equivalent attributed facts. Acceptance survives a crash before client journal/view updates; passive recovery makes zero submission calls. |
| A3: Persistent observation | New `activity/monitor.py` and collector adapters; `daemon.py`/`config.py` lifecycle; monitor controls and `campaign watch` in `cli.py`. | Agent exit and daemon restart preserve subscriptions; known work updates once per shared cadence; auth loss and sleep become visible gaps; observation never submits/retries/cancels/validates. |
| A4: Agent surface and qualification | Core MCP `activity` reader and explicit monitor controls; update `skills/remoteslurm/references/operations.md`, guide, docs, and compatibility tests. | Bounded local context survives offline state; all readers agree; FakeSlurm matrix and candidate-specific live lifecycle pass. |

The first externally useful development checkpoint is A1+A2: requests and imported outcomes can
be followed while existing clients operate. The first release that satisfies this proposal is
**A0–A4**, including unattended observation after an agent disconnects. Do not market a journal
without a running observer as durable monitoring.

MCP `activity(host, after, since, campaign, run_id, limit)` returns bounded `active_work`,
`attention`, `events`, `next_cursor`, `has_more`, `monitor_health`, evidence ages, and history/import
coverage. Read only local state. Bound each section separately and identify omitted items. Expose
the latest recorded per-run revision; this is not an atomic cross-cluster snapshot. MCP tool
availability does not itself make agents call it at turn start: document that workflow in the
skill, and leave automatic turn hooks to clients that support them.

Keep `rslurm watch` exit semantics and legacy `events` cursor behavior during migration. Do not
silently repoint existing consumers to a new schema or cursor. A labeled, deduplicated import of
old terminal events is optional; weak legacy provenance must not be upgraded. Once both paths
are tested, document the new API as the preferred multi-consumer surface.

## Verification and release gates

Use the existing FakeSlurm and fault-injection seams. Test observable behavior and durable records:

- Interrupt before dispatch, after remote intent, after Slurm acceptance but before the client
  response, and after local insert but before import-watermark commit. Recover evidence without
  duplicate logical events or duplicate jobs. Plain submissions with unknowable outcomes remain
  explicitly limited; campaign guarantees must not be silently attributed to them.
- Follow with two CLI readers and one MCP reader while concurrent producers append. No reader
  consumes another's events; filtered pagination, clock skew, late receipts, retention, and
  restart preserve sequence semantics. Test `RUNNING -> PENDING -> RUNNING` and reused job IDs.
- Crash between execution-ledger persistence and campaign-view commit. The monitor finds the
  accepted attempt through the passive reader even when the campaign journal lacks it.
- Race monitor import/refresh against apply, retry, verify, and another collector. Pinned reads
  never combine revisions; stale attempts never replace a current attempt; CAS failures remain
  visible and do not invent observations.
- Preserve scheduler-complete/output-missing/validation-not-run as three separate facts. Cover
  accounting lag, delayed filesystem visibility, artifact mutation, failed/unavailable validators,
  and terminal packed allocations without final unit markers.
- Prove the observer cannot invoke workload mutations or validators with forbidden-call spies.
  Cover daemon idle/restart, separate-socket ownership, expired authentication, host config changes,
  closed-active and archived runs, and the no-daemon path.
- Assert remote call counts, bytes scanned/hashed, event volume, and retained storage for large
  arrays/campaigns with multiple viewers. A second viewer adds zero scheduler RPCs. Unchanged
  polls produce no transition spam and do not create full remote snapshots each tick.
- Inject full/unwritable journal storage after successful submission. Preserve the accepted job ID
  and report degraded activity. Test log truncation, hostile terminal sequences, and source errors.

Run Ruff, formatting, mypy, the full existing suite, package builds, and the Python 3.6 stub floor
when implementing. Qualify the final source/stub hashes live with a small array, a mixed-result
pack, an agent disconnect, daemon restart, and explicit output verification. Retain exact
submission, monitor, artifact, and validation evidence. Local tests and prior WP5 receipts do not
substitute for this release's live evidence.

## Relationship to WP8

Add this plan as **WP8a: shared activity and observation** in the campaign roadmap. Keep
**WP8b: resource interpretation** for interval metrics, retained telemetry rollups, evidence-based
failure grouping, and packing advice. The activity foundation can proceed alongside WP6/WP7;
it uses current bounded primitives and does not require deployment or broad filesystem features.
Existing WP4/WP5 qualification gaps remain independently open.

A dashboard later consumes the same query and projection contract. It introduces no independent
scheduler polling, authorization logic, or alternative definition of completion.
