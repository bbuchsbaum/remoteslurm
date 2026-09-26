# Activity contract v1

A0 freezes the contracts in `remoteslurm.activity.model` and the advisory projection in
`remoteslurm.activity.attention`. Campaign `next_action` uses that projection, including on
cached status reads. Storage, producers, passive recovery, and a background observer remain
subsequent work; importing this module does not enable monitoring.

## Authority and evidence

The journal is **presentation history**, never an execution or authorization authority.
The remote execution ledger owns attempt intent and acceptance; scheduler observations own
scheduler state; filesystem observations own artifact facts; validation receipts own validation
results at a particular artifact signature and contract; committed campaign records own definition,
dependencies, lifecycle, and retry authorization. A successful scheduler outcome cannot establish
artifact presence or validation. A request cannot establish its outcome.

Use `reconcile_job` before projecting scheduler observations. Its precedence remains live queue,
accounting, control, then retained registry evidence within the existing retention/grace rules.
Conflicting identities remain conflict. A newer ingestion timestamp does not override a stronger
source. Do not combine evidence for different attempts, reused job IDs, endpoints, or contracts.
Contradictory artifact signatures or contract IDs invalidate the applicability of a passed receipt.
Unavailable sources are uncertainty, never evidence of absence or success. Collection route
(direct refresh, passive recovery, observer) is not a source-precedence input.

`EvidenceRef` identifies an authority, concrete source, immutable record/commit/receipt or captured
observation identity, optional content digest, and limitations. Mutable ledger paths alone are not
immutable evidence: capture bounded content and its digest with the attempt identity. Derived
events must cite input evidence and the projection version. Legacy records keep their weaker
identity confidence and missing evidence; importing them must not upgrade certainty.

## Envelope and identity

`ActivityEvent` v1 serializes to a JSON object with these fields:

| Fields | Meaning |
| --- | --- |
| `schema_version`, `event_id`, `journal_id`, `seq` | Version 1; stable logical event ID; local journal identity; positive, increasing ingestion sequence. |
| `kind`, `provenance`, `source` | Namespaced event kind; request, remote record, observation, or derived assessment; concrete producer. |
| `subject` | Configured target identity plus display alias; campaign, definition, run, stage, unit, group, attempt, task, job, array child, and packed member as applicable. |
| `recorded_at`, `observed_at`, `occurred_at` | Time durably ingested, time evidence was observed, and actual source event time when known. Unknown times are null; timestamps include a timezone. |
| `freshness` | Recorded evidence assessment: FRESH, STALE, PARTIAL, UNAVAILABLE, or CONFLICT. Readers also calculate age. |
| `actor`, `operation_id`, `causation_id`, `correlation_id` | Explicit origin and linkage, or null when unknown. An actor name is attribution, not authority. |
| `evidence_refs`, `payload`, `limitations`, `projection_version` | Factual bounded content and cited evidence; derived assessments identify their rule version. |

Resolve `target_id` from endpoint, user, and configured campaign/task roots. An alias alone cannot
join observations across configuration changes. Attempt ID, job submission time, and attempt marker
protect against job-ID reuse. Allocation/job identity, array child ID, and packed member are distinct.
A pack allocation's completion does not establish each member's outcome.

Source event IDs/commits provide imported logical identities. Mutable ledger facts use attempt,
fact kind, and captured-content digest. Local operation IDs distinguish repeated requests from
repeated delivery. Do not globally deduplicate equal states: RUNNING -> PENDING -> RUNNING is
meaningful. Ingestion order is not remote causal order; preserve observed and occurred times for
late receipts and clock limitations.

## Advisory attention

`project_attention(snapshot, now=..., max_age=120)` is pure. Normalized execution, artifact,
validation, dependency, freshness, and optional monitor-health evidence produce the same result
regardless of how it was collected. It neither invokes validators nor submits, retries, or cancels.
The legacy `kind`, `count`, `examples`, and `reason` fields remain, with projection version 1 and
an ordered `issues` collection retaining all applicable categories.

Each issue separates observed axis states, evidence/source references, age/freshness, severity,
recommended action, and authorization. Counts cover all matching units; examples/facts are limited
to five per category. The issue key is campaign/run/category. Failed/cancelled work, dependency
blocks/conflicts, missing/changed/unchecked/settling outputs, failed/stale/not-run validation,
unresolved execution, and unavailable/stale evidence cannot return `complete`. Active work is
informational only after higher-priority attention and uncertainty have been considered.
`complete` requires every unit to be COMPLETED, artifacts PRESENT, validation PASSED, dependency
SATISFIED or NOT_APPLICABLE, and evidence FRESH. An empty run is `empty`, not complete.

Read-time freshness expires after the supplied budget (default 120 seconds). Missing, malformed,
or future observation times cannot support FRESH. Retained terminal execution facts are not erased
by age; the assessment becomes uncertain. A stale validation receipt retains its historical result
but cannot establish validity of changed artifacts/contracts. Missing optional monitor health means
monitoring is unspecified, never healthy; explicit DISABLED is allowed for direct observations.
An unhealthy monitor or expired HEALTHY heartbeat is an independent uncertainty.

A0 has no current authorization collector. It returns `authorization.state=unknown` and
`grants_authority=false`. Future adapters may report required/available/consumed only from current,
attempt-scoped authorization records, with evidence and age. A recommendation itself never grants
permission or proves a retry is unauthorized. Existing execution APIs enforce authorization.

Campaign cached status now reads the complete bounded unit snapshot to recompute attention before
pagination, without querying Slurm, validating outputs, or persisting the derived assessment.
This can require more campaign document reads than the former summary-only path. A later pinned
collector/projection cache can optimize it without weakening read-time freshness checks.

## Immutable semantic context

`Subject.semantic_ref` optionally pins a `SemanticRef`: campaign/run identity, campaign commit,
workspace commit, and contract digest. `SemanticSnapshot` captures immutable JSON containing a
`units` array; each unit can include its name, keys, parameters, contract, and `attempt_members`.
Member mappings contain attempt ID, allocation job ID, array task ID, and packed member, with null
for inapplicable dimensions. References must come from a commit-pinned authoritative read; the
resolver does not fetch or independently authenticate commits or digests.

`resolve_semantics` defaults to `meaning=at_event` and resolves only the event's pinned reference.
`meaning=current` requires a separately supplied current reference. It never substitutes today's
unit name or configuration when historical data is absent. Availability is `attached` for an exact,
unambiguous match, `unavailable` for absent references or unresolved members, and `stale` when
conflicting captures invalidate an immutable reference. Current state does not make a correctly
pinned historical capture stale. Returned dictionaries cannot mutate captured JSON.

A stable unit ID resolves directly within the pinned run; a subject without a unit ID requires an
exact attempt/allocation/array-child or packed-member mapping. Allocation-only lookup is ambiguous
and must remain unavailable. Renaming or reconfiguring a unit in a later commit changes current
presentation while leaving `at_event` unchanged. Semantic context explains meaning; it does not
upgrade an event's execution, evidence, freshness, or authorization.

## Storage, cursors, and retention contract for subsequent slices

A1 must insert immutable retained events, source checkpoints, and projection changes atomically.
Readers have independent opaque cursors bound to journal identity, ingestion sequence, and filter;
reading never consumes another reader's position. Snapshot-plus-cursor is atomic. Default reads
return the latest 100 events in ingestion order. `since` filters ingestion time; cursor continuation
is the reliable resume mechanism. Bound event count and response bytes and expose `has_more`.

Retention defaults to 30 days / 256 MiB, pruning an old contiguous event prefix while retaining
active identities, current projections, checkpoints, and authority references. Expired cursors must
return an explicit history gap plus a resynchronization cursor, never silently skip. Retained records
are immutable; append-only does not mean infinite retention. Retention cannot rewrite history or
relabel unavailable semantic snapshots as current. A recording failure after scheduler acceptance
must preserve the accepted job ID and report `activity_recorded=false`; it cannot imply submission
failure or authorize a repeat. These are storage requirements, not claims that A1 exists.
