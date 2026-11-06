# `rslurm dash` HTTP contract (v1, read-only)

Server: `src/remoteslurm/web/` (Starlette). Page: `src/remoteslurm/web/static/`. All times are
Unix epoch seconds (floats) unless a field says otherwise. All durations are seconds.

## Access

- Bound to `127.0.0.1`. The launch URL is `/?token=T`; the server answers with a redirect to `/`
  and sets an HttpOnly, SameSite=Strict cookie. Every route except `/healthz` requires the cookie.
- The Host header must be `127.0.0.1:PORT` or `localhost:PORT`.
- POST routes also require `Content-Type: application/json` and the header `X-Rslurm: 1`.
- CSP: `default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self';
  font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'`.
  No inline scripts, no `style="..."` attributes in markup (set styles through `el.style`), no CDN.

## `GET /api/state` (also the SSE `state` event)

```jsonc
{
  "now": 1792496344.3,               // server clock; the page derives its clock skew from this
  "journal": "9f...",                // activity journal id (scopes the page's seen-alert watermark)
  "demo": false,                     // true only for `rslurm dash --demo` (show "Example data")
  "observer": {
    "running": true,                 // local daemon answers
    "enabled": true,                 // monitoring control switch
    "heartbeat": 1792496340.1,       // observer loop heartbeat, or null
    "build_ok": true,                // false: the daemon runs an older build (restart it)
    "note": null                     // one plain sentence when something needs the user
  },
  "hosts": [{
    "id": "trillium",
    "login": "tri-login03",          // or null
    "observedAt": 1792496320.0,      // last successful scheduler read for this host, or null
    "error": null,                   // short code of the last failed read (e.g. "not_connected")
    "action": null,                  // what the user can do about it, one sentence
    "accounting": "ok",              // "ok" | "unavailable" | "unknown"
    "accountingSince": 1792496100.0, // last successful sacct read; null if never
    "alloc": null,                   // "node" | "core" | null (unknown)
    "coresPerNode": null,
    "coverage": "all"                // "all": every job of yours (squeue -u); "tracked": registered work only
  }],
  "jobs": [{
    "key": "trillium:4412702",       // unique: host + scheduler id
    "id": "4412702", "name": "fit_sweep", "host": "trillium",
    "partition": "compute", "account": "def-x", "user": "brad",
    "tracked": true,                 // submitted or adopted through remoteslurm
    "workspace": "/scratch/brad/proj", // workspace root it belongs to, or null
    "state": "RUNNING",              // Slurm state, normalized (CANCELLED by ... -> CANCELLED)
    "reason": "Priority",            // pending reason, or null
    "submit": 1792470000.0, "start": 1792471000.0, "end": null, "estStart": null,
    "timelimit": 10800,              // seconds or null
    "elapsed": 3600,                 // seconds as of observedAt, or null
    "nodes": 1, "nodelist": "tri-c0412", "cpus": 192, "mem": "750G",
    "maxrss": null,                  // human size, ONLY from sacct; null when unknown
    "exit": "0:0",                   // "code:signal" or null
    "exitSource": "sacct",           // "sacct" | null
    "workdir": "/scratch/...", "stdout": "/scratch/.../x.out", "stderr": null,
    "pathsGuessed": false,           // true: stdout is the WorkDir/slurm-%j.out default, not recorded
    "source": "squeue",              // "squeue" | "sacct" | "retained" | "registry"
    "vanishedAt": null,              // state UNKNOWN: when it left the queue with no accounting yet
    "observedAt": 1792496320.0,
    "array": null,                   // or the object below
    "hint": null                     // one or two sentences for failed/pending jobs, or null
  }],
  "coverage": "one sentence describing what this page can and cannot see"
}
```

`array` for job arrays (one row per array):

```jsonc
{
  "n": 200,                          // tasks known (ids below), not necessarily the declared size
  "ids": [0, 1, 2],                  // task ids, ascending
  "states": ["COMPLETED", "RUNNING", "PENDING"], // parallel to ids
  "limit": 16,                       // %N throttle if known, else null
  "pendingReason": "JobArrayTaskLimit",
  "exits": {"63": "1:0"},            // failed tasks only
  "taskInfo": {"63": {"elapsed": 3120, "maxrss": "212G", "node": "tri-c0533", "end": 1792480000.0}},
  "pathsTask": "1"                   // the task whose log path scontrol reported, or null
}
```

`state` may be `"UNKNOWN"`: the job left the queue and accounting has not yet said how it ended
(never shown as still running). Arrays with more than 2,000 tasks send `ids`/`states` for at most 2,000 (failed and active first)
and set `"n"` to the full count.

## `GET /api/activity?after=CURSOR&limit=200`

```jsonc
{
  "events": [{
    "seq": 1042, "id": "event-id", "t": 1792496100.0,
    "kind": "fail",                  // "ok" | "fail" | "run" | "pend" | "warn" | "info"
    "host": "nibi", "job": "8830187",// job may be null
    "text": "fit_sub-07 ran out of memory",
    "detail": "exit 0:125",          // or null
    "alert": true                    // a finish or failure worth an alert
  }],
  "cursor": "opaque", "resync": false
}
```

Without `after` it returns the latest `limit` events (oldest first). With `after` it returns events
after the cursor. `resync: true` means history was pruned past the cursor; reload state.

## `GET /api/stream?after=CURSOR` (Server-Sent Events)

- `state`: the full `/api/state` payload; sent on connect and whenever it changes (checked every
  2 s).
- `activity`: `{"events": [...], "cursor": "..."}` with new feed items.
- `ping`: `{"now": epoch}` every 15 s.

The browser's `EventSource` reconnects on its own; the page passes its last cursor as `after`.

## Logs and files (queued through the daemon's bounded reads)

`POST /api/tail` with `{"host": "nibi", "job": "8830187", "which": "out" | "err", "task": 63 | null}`.
`POST /api/files` with `{"host": "nibi", "job": "8830187"}` lists the job's work directory.
The server resolves the path from the job's own record; the page never sends a path.

Both return, and `GET /api/tail/{request}` / `GET /api/files/{request}` poll:

```jsonc
{
  "request": "rid" ,                 // null when nothing was queued
  "state": "QUEUED",                 // "QUEUED" | "DONE" | "ERROR" | "UNAVAILABLE"
  "path": "/project/.../fit_sub-07.out",
  "pathGuessed": false,
  "error": null, "action": null,     // why it is unavailable and what to do
  "result": null,                    // when DONE: see below
  "cached": null                     // the last capture of the same file, if any (same shape)
}
```

Tail result: `{"lines": ["..."], "observedAt": epoch, "size": 1234, "truncated": true,
"gap": "bounded tail; earlier bytes are omitted" | null, "binary": false}` (last 200 lines,
at most 64 KiB). Files result: `{"observedAt": epoch, "entries": [{"name", "type", "size",
"mtime"}], "partial": false}`.

Remote text is untrusted: render it with text nodes only.
