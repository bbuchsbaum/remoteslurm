# Web dashboard plan (`rslurm dash`)

Status: proposal, 2026-11-05. Not started.

## Goal

One command opens a local browser page that shows, live, what our remote Slurm work
is doing: what is queued, what is running, what finished or failed, the logs, and
the activity stream. When something fails or completes, the page says so at once,
including in a desktop notification. It should look and feel like a good product
page: calm, dense where it matters, and quick to read.

```sh
rslurm dash                 # every configured host, opens the browser
rslurm -H trillium dash     # one host
rslurm dash --project analysis --no-open --port 8765
```

## Architecture: another reader of the daemon's journal

The daemon already collects everything the TUI shows. Its `WorkspaceObserver`
writes scheduler, output, and attention facts into the local SQLite `ActivityStore`,
and `workspace.service.snapshot` and `inspect_request` read that store and queue
bounded remote reads (file listings and 64 KiB log tails). The dashboard is a second
viewer of the same store, like the TUI:

```
 browser  ──SSE / JSON──▶  rslurm dash (Starlette + uvicorn, 127.0.0.1)
                                │  reads only
                                ▼
                     ActivityStore (SQLite WAL)  ◀── daemon observer ──ssh──▶ cluster
                     inspect_request queue       ──▶ daemon (bounded tails)
```

Rules that follow from this:

- **The dashboard process never opens SSH.** All remote reads go through the
  daemon's existing budgets: 8 reads per host per 30 s, a 64 KiB tail cap, and at
  most one tail per path every 5 s. Opening ten tabs adds no cluster load.
- **Read-only.** Observation never submits, cancels, retries, or adopts. The activity
  contract says so, and the dashboard keeps that rule. The only writes are queued
  bounded reads (tails and listings) into the daemon's existing inspect queue.
- **No new runtime dependencies in practice.** `mcp` already pulls in `starlette`,
  `uvicorn`, and `sse-starlette`. We declare them explicitly in `[project]
  dependencies` so we don't rely on them arriving transitively.
- **No frontend build step and no CDN.** The page is static HTML, CSS, and ES-module
  JS shipped inside the wheel (`src/remoteslurm/web/static/`). Small libraries
  (uPlot for the timeline) are vendored. It works offline and through VPNs, and
  `uv tool install` stays the only install step.

### Server (`src/remoteslurm/web/`)

| Endpoint | Source | Purpose |
| --- | --- | --- |
| `GET /api/overview` | store: workspaces, observer health, per-host `connection` status | Header, host pills, KPI strip |
| `GET /api/work?host=&workspace=` | `service.snapshot` per workspace | Job table (queued/running/finished, arrays rolled up) |
| `GET /api/job/{id}` | snapshot work card + registry (`jobs.py`) | Detail drawer: resources, paths, exit, reason, `rslurm get` command |
| `POST /api/tail` | `service.inspect_request(kind="tail")` | Queue a stdout/stderr tail; returns a request id |
| `GET /api/tail/{rid}` | request receipt | Poll the tail result |
| `GET /api/activity?after=` | `store.query` with an independent cursor | Activity feed with filters |
| `GET /api/stream` | server loop over `store.query(after=cursor)` every 1 s | **SSE**: `work`, `activity`, `health`, `alert` events |
| `GET /` | static | The app |

Each SSE client keeps its own journal cursor; readers never consume each other's
events. On `resync_required`, the server sends a full snapshot. Alerts are derived
on the server from state transitions: a work card entering FAILED, CANCELLED,
TIMEOUT, OUT_OF_MEMORY, or COMPLETED, or attention moving to `act`. Each alert is
deduplicated by `event_id`, so a reload doesn't replay old alerts as new ones.

**Security.** Bind to `127.0.0.1` only. A random token is generated per launch and
placed in the opened URL; it is exchanged for an HttpOnly SameSite=Strict cookie.
The server checks the Host header to block DNS rebinding, sends no CORS headers,
and sets a strict CSP (`default-src 'self'`). All remote text is rendered as text
nodes, never as `innerHTML`; log content is untrusted.

**Launch.** `rslurm dash` makes sure the daemon is running and monitoring is
enabled (the same path as `view`), picks a free port, starts uvicorn, waits until
`/healthz` answers, and then calls `webbrowser.open`. `Ctrl-C` stops the web
server only. Jobs, SSH, and the daemon keep running, as they do when the TUI closes.
`--no-open` prints the URL, which is useful on a remote desktop.

## The page

A single screen with no page navigation. Light and dark themes follow the OS.
Type is Inter, vendored, with tabular numerals and a monospace face for logs.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ remoteslurm   ● trillium  live 4s   ● nibi  live 12s   ○ fir  off    ⌘K  ◐    │
├──────────────────────────────────────────────────────────────────────────────┤
│  RUNNING 12     PENDING 40     DONE (24h) 318     FAILED (24h) 3 ▲   CPU-h 1.2k│
├───────────────────────────────────────────────┬──────────────────────────────┤
│ [Queue 40] [Running 12] [Finished] [All]  🔍   │  ATTENTION                    │
│ ───────────────────────────────────────────── │  ✕ fit_sub-07  OUT_OF_MEMORY  │
│ ● 4412871  fit_sweep[0-199]  ███████░░ 143/200 │    MaxRSS 31.8G / 32G  → log  │
│ ● 4412880  preproc           RUNNING  01:12:09 │  ⚠ nibi observation stale 3m  │
│ ◌ 4412901  mvpa_perm         PENDING  Priority │ ───────────────────────────── │
│ ✓ 4412766  qc_report         COMPLETED  0:14   │  ACTIVITY              filter ▾│
│ ✕ 4412702  fit_sub-07        OOM  exit 0:125   │  14:02 ✓ qc_report completed  │
│                                               │  14:01 ▶ preproc started       │
├───────────────────────────────────────────────┤  13:58 ✕ fit_sub-07 OOM        │
│ TIMELINE  last 24h  ▁▂▅▇█▇▅▃ (per-job bars)    │  …                            │
└───────────────────────────────────────────────┴──────────────────────────────┘
          click a job ▶ right-side drawer: details · stdout · stderr · files
```

- **Header.** Shows a pill for each host with its connection and observer freshness.
  The pill's color always comes with a text label, and freshness uses the same
  thresholds as the TUI: stale after 90 s. A cached or stale state is never
  shown as live.
- **KPI strip.** Shows counts with sparklines for the last 24 h. A count animates
  only when it changes.
- **Job table.** Has Queue, Running, Finished, and All tabs. Arrays and sweeps
  collapse to one row with a per-task progress bar and expand to show their tasks.
  Rows show the pending reason, elapsed versus time limit, and the exit code. Rows
  that change state flash briefly. The table is sortable, filterable, and
  keyboard-navigable (`j`/`k`, Enter, `/`).
- **Job drawer.** Shows details, a **live log tail** for stdout or stderr (auto-follow,
  pausable, polled every 5 s through the queue, with truncation and rotation labeled
  as the TUI labels them), the work-directory listing, and a copyable `rslurm get …`
  command. For an OOM or timeout, it shows the `diagnose` hint already stored in
  the journal.
- **Attention panel.** Lists errors and uncertainty first, using `project_attention`
  issues as they are. It never shows "complete" while evidence is stale.
- **Activity feed.** Shows the journal with filters by kind, host, and campaign.
  The same cursor also drives the alerts.
- **Timeline.** Shows a Gantt chart of the last 24 h, one bar per job, colored by
  final state. It answers questions like "what ran overnight".
- **Alerts.** Each alert shows an in-page toast, adds a count to the tab title
  (`(2) remoteslurm`), sets a favicon badge, and, once the user allows it, sends
  a browser `Notification`. A per-kind mute stays in `localStorage`.
- **Campaigns.** A switcher shows the units, attempts, and attention for an
  enrolled campaign run. This is phase W4.

## Coverage: what "in queue" means

On its own, the observer shows **only jobs remoteslurm registered** (submitted
through it or adopted) under an attached workspace root. Two additions, both
read-only, close that gap in W1:

1. **Discovery.** A new observer collector runs one `squeue -u $USER` and one
   `sacct -X -u $USER -S <last check>` per host, on the existing 30 s scheduler
   cadence and inside the 8-reads-per-30-s budget. That's about 2 reads per window.
   Every job we own appears, whatever submitted it. Jobs that remoteslurm doesn't
   track are labeled "untracked", and their evidence is never merged into tracked
   work cards. The first scan reaches back 7 days. If `sacct` is unavailable or
   slow, the finished section reads "accounting unavailable", never "none".
2. **Enrichment.** For active untracked jobs, `scontrol show job` gives the work
   directory and the stdout/stderr paths. That enables log tails and file views,
   and work-directory matching files each job under the right workspace. A finished
   job first seen after Slurm's `MinJobAge` may have no recorded log path. Its
   fallback `WorkDir/slurm-%j.out` is shown as a guess.

Auto-adopting discovered jobs writes to the local registry, so it's deferred (see
Out of scope).

## Phases

Each phase ends with its tests green, ruff/mypy clean, and the result checked on
screen.

| Phase | Deliverable | Verification |
| --- | --- | --- |
| **W0** | `web/` package, `rslurm dash` launch path, token/cookie/Host guard, `/healthz`, static shell page, explicit deps | Starlette `TestClient` tests: auth required, Host rejection, CSP header; `dash --no-open` boots and the browser opens |
| **W1** | `/api/overview`, `/api/work`, SSE stream, header, KPI strip, job table with array rollup; untracked-job discovery and enrichment collector | Seeded `ActivityStore` fixtures (reusing the `test_workspace*.py` builders); SSE delta and resync tests; collector read-budget and "accounting unavailable" tests against FakeSlurm |
| **W2** | Job drawer, queued log tails, file listing | Tests on the inspect queue round trip; truncation and rotation labels; XSS test with a hostile log line |
| **W3** | Activity feed, attention panel, alert derivation, toasts, Notifications, title and favicon badge | Transition-to-alert unit tests (FAILED/OOM/COMPLETED, dedupe across reload, no alert from stale data) |
| **W4** | Multi-host aggregation, timeline, campaign switcher | Collector budget test against FakeSlurm; timeline from fixture history |
| **W5** | Polish and qualification: empty, loading, error, and offline states; keyboard; narrow width; light and dark | Playwright screenshots at 1440 and 1024 widths in both themes, critiqued by a fresh-context subagent; live gate on Nibi with a 2-job + small-array submit watched end-to-end; one-page doc `docs/web-dashboard.md` |

Rough size: W0–W3 is the usable core, about 2–3 focused sessions. W4–W5 take
another 1–2 sessions.

## Decisions (2026-11-05)

1. **Read-only.** The dashboard and its collectors only read. It has no Cancel
   button, no auto-adopt, and no writes to the cluster or the job registry.
2. **Untracked jobs:** discovery and enrichment are included in W1, as described
   above.
3. **Scope at launch:** all configured hosts by default; `-H` narrows to one. This
   is the default, not yet confirmed.

## Out of scope for v1

Any control action (cancel, retry, submit, adopt or auto-adopt) until read-only v1 has shipped; remote or multi-user access (anything beyond localhost), authentication beyond the
local token, editing files, historical analytics
beyond the 30-day journal, and mobile layouts.

## Notes

- `src/remoteslurm/tui/app.py` has uncommitted work in progress. The dashboard
  doesn't modify it. If we later share formatting helpers (state labels, human
  sizes), we extract them into a neutral module after that work lands.

## Design (hill climb, 2026-11-05)

The page design was refined through a visual hill climb: a baseline plus three rounds, each reviewed
by three independent critics (an observability product designer, an accessibility and live-UI
engineer, and an HPC neuroimaging user who also owned evidence fidelity). Mean rubric scores over 8
dimensions:

| Round | Visual | Function/a11y | User/fidelity |
| --- | --- | --- | --- |
| v0 | 5.38 | 5.50 | 6.12 |
| v1b | 7.38 | 6.62 | 7.50 |
| v2e | 7.75 | 7.12 | 7.88 |
| v3b (selected) | 8.00 | 7.25 | 8.00 |

All three critics chose v3b. Its prototype source is in `docs/design/web-dashboard-prototype/`; the
example data in `fixtures.js` is illustrative, not real jobs. The automated gate passed with 49
checks covering layout shift (load and live), overflow, contrast, the keyboard core task, modal
focus, focus stability under live updates, stale-host and accounting-gap honesty, adversarial names,
late fonts, alert-strip clipping and junk text.

Design decisions carried into W0–W3:

- One card per host, with freshness first. Counts are in jobs (an array counts as one), and
  allocation is in the host's own unit (nodes on Trillium, cores on Nibi). No sparklines unless they
  are derived from the journal.
- Stale (> 90 s) host: amber card with "As of HH:MM"; every row on that host is marked "as of"; the
  timeline bar ends at the last read, then dotted. Accounting silent: finished jobs from before
  the gap are "retained"; later finishers show scheduler state only, with no MaxRSS.
- A one-line alert strip that persists until marked seen, never overlays content and never shifts
  the layout; one polite announcer; desktop notifications only on permission.
- Rows use a roving tabindex. The drawer and help dialog are modal (background inert). Periodic
  renders patch nodes in place, and when a focused item leaves the view the hand-off is announced.

Carry-forwards the final reviews identified (fix in the production build, not the prototype):

1. Put the sections in reading order in the DOM (Needs attention, Jobs, Activity, Timeline) and
   place them with `grid-template-areas`, with Activity and Timeline side by side between 720 and
   1100 px. This fixes the WCAG 1.3.2/2.4.3 order mismatch and the sparse 1024 layout together.
2. Vendor the fonts (WOFF2, `font-display: optional`, metric-matched fallback) and drop the 1 s
   render hold.
3. Hand-off announcements use accessible names without truncating identifiers; keep a focused feed
   item rendered.
4. After an accounting gap, label the source of the exit code (`scontrol`) or show it as unknown.
5. Failure summaries keep their key fact per kind (for a timeout, the last checkpoint time).
6. Broaden the accounting-gap check: every tab, 390 and 1440, text and attributes, every
   accounting-only field.
7. `aria-controls` on "Show all"; expanded alerts before "Mark seen" in Tab order; one duration
   precision rule; no wrapping between a number and its label.
