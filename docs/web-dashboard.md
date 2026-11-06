# The web dashboard (`rslurm dash`)

`rslurm dash` opens a page in your browser that shows, as it happens, what your Slurm work is
doing on every configured host: what is queued, what is running, what finished or failed in the
last 7 days, the logs, and a stream of activity. It is read-only.

```bash
rslurm dash                     # every configured host
rslurm -H nibi dash             # one host
rslurm dash --no-open --port 8765
rslurm dash --demo              # example data, to see the page without a cluster
```

Press Ctrl-C in the terminal to stop the page. Jobs and the background observer keep running.

## Before you start

The page reads what the background observer (the local `rslurm` daemon) has already collected;
the page itself never opens SSH. The observer needs a live connection to each host, so on a
cluster with MFA, connect first:

```bash
rslurm connect nibi
```

A host without a connection shows "Can't read" with the command to run. `rslurm dash` starts the
observer if it isn't running. If the running observer is from an older build of remoteslurm, the
page says so; `rslurm dash --restart-daemon` restarts it.

## What it can see

- **Every job you own** on a host while the page is open (or while a terminal view is attached to
  a project there): one `squeue -u $USER` every 30 seconds and one `sacct` every 60 seconds,
  reaching back 7 days on the first read. Jobs remoteslurm didn't submit are labelled
  *untracked*.
- **Logs** come from the path Slurm reports (`scontrol show job`) or remoteslurm's own record.
  When neither is known, the page tries Slurm's default `slurm-<jobid>.out` in the job's work
  directory and labels it a guess. Each read is the last 200 lines, at most 64 KiB.
- **Peak memory (MaxRSS)** comes only from accounting. If `sacct` stops answering, jobs that
  finish afterwards show the scheduler's state with "no accounting"; the page never fills the gap
  with a guess. A job that leaves the queue before accounting reports it shows as *unknown*, not
  as still running.

Every read goes through the observer's per-host budget (at most 8 calls per 30 seconds, at most
3 log reads), so opening more tabs adds no load on the cluster. Closing the page ends its
polling of hosts that no terminal view is watching within about two minutes.

## Alerts

Finishes and failures that happen while the page is open appear in a one-line strip at the top
and in the tab title, as `(2) remoteslurm`. Desktop notifications are off until you turn them
on. Alerts are counted from the moment you open the page, and "Mark seen" is remembered in this
browser, so reloading doesn't replay old alerts.

## Security

The server listens on `127.0.0.1` only. The link it prints carries a one-time token that the
page exchanges for a cookie; requests without it, or with any other `Host` header, are refused.
The page loads nothing from the internet (fonts are bundled) and renders remote text, including
log lines, as plain text. The page can ask for a job's logs or work-directory listing, but it
can't name a path itself.

## Not yet

Cancel, retry, submit, and adopting untracked jobs are deliberately absent from this version.
