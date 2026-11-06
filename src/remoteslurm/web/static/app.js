// rslurm dash: live page over the local server's read-only API (docs/plans/web-dashboard-api.md).
// Remote text (job names, log lines, paths) is rendered with text nodes only.
(function () {
  "use strict";
  const STALE_AFTER = 90;
  const DAY = 86400;

  // ---------- clock: the page works in seconds relative to T0 (first server `now`) ----------
  let skew = 0; // server clock minus browser clock, seconds
  let T0 = null;
  const nowE = () => Date.now() / 1000 + skew;
  const rel = () => nowE() - T0;
  const R = (e) => (e == null ? null : e - T0);

  // ---------- Slurm states ----------
  const PENDING_LIKE = new Set([
    "PENDING",
    "REQUEUED",
    "REQUEUE_HOLD",
    "REQUEUE_FED",
    "RESV_DEL_HOLD",
    "PREEMPTED_HOLD",
    "SPECIAL_EXIT",
  ]);
  const ACTIVE = new Set([
    ...PENDING_LIKE,
    "RUNNING",
    "SUSPENDED",
    "COMPLETING",
    "CONFIGURING",
    "RESIZING",
    "SIGNALING",
    "STAGE_OUT",
    "STOPPED",
  ]);
  const FAILED = new Set([
    "FAILED",
    "OUT_OF_MEMORY",
    "TIMEOUT",
    "NODE_FAIL",
    "BOOT_FAIL",
    "DEADLINE",
    "PREEMPTED",
    "REVOKED",
  ]);
  const isPend = (s) => PENDING_LIKE.has(s);
  const isRun = (s) => ACTIVE.has(s) && !PENDING_LIKE.has(s);
  const isDone = (s) => !ACTIVE.has(s) && s !== "UNKNOWN";
  const LABEL = {
    RUNNING: "Running",
    PENDING: "Pending",
    COMPLETED: "Completed",
    COMPLETING: "Completing",
    FAILED: "Failed",
    OUT_OF_MEMORY: "Out of memory",
    TIMEOUT: "Timed out",
    CANCELLED: "Cancelled",
    NODE_FAIL: "Node failed",
    BOOT_FAIL: "Boot failed",
    PREEMPTED: "Preempted",
    SUSPENDED: "Suspended",
    REQUEUED: "Requeued",
    UNKNOWN: "Unknown",
    VANISHED: "Outcome unknown",
  };
  const label = (s) =>
    LABEL[s] ||
    String(s || "Unknown")
      .toLowerCase()
      .replace(/_/g, " ")
      .replace(/^./, (c) => c.toUpperCase());
  const cls = (s) =>
    isPend(s)
      ? "pend"
      : isRun(s)
        ? "run"
        : s === "COMPLETED"
          ? "ok"
          : FAILED.has(s)
            ? "fail"
            : "cancel";
  const REASON = {
    Priority: "Waiting behind higher-priority jobs",
    Resources: "Waiting for nodes to free up",
    QOSMaxJobsPerUserLimit: "At your per-user running-job limit",
    JobArrayTaskLimit: "Held by the array's own throttle",
    Dependency: "Waiting for a dependency to finish",
    DependencyNeverSatisfied: "A dependency failed; this job can never start",
    BeginTime: "Waiting for its requested start time",
    JobHeldUser: "Held by you (scontrol hold)",
    JobHeldAdmin: "Held by an administrator",
    ReqNodeNotAvail: "Requested nodes are not available",
    AssocGrpBillingMinutes: "The allocation is out of billing minutes",
    PartitionTimeLimit: "Requested time exceeds the partition limit",
  };
  const reasonText = (r) => REASON[r] || r || "Waiting";

  // ---------- state ----------
  const F = {
    hosts: [],
    jobs: [],
    activity: [],
    demo: false,
    observer: {},
    coverage: "",
    journal: null,
  };
  const S = {
    tab: "active",
    host: null,
    q: "",
    cursor: null,
    open: null,
    openJob: null,
    dtab: "log",
    task: null,
    seenSeq: 0,
    showAll: false,
    feedAll: false,
    keys: true,
    help: false,
    tl: null,
    lost: false,
    authLost: false,
    bootError: null,
    paused: false,
  };
  let prefs = {};
  try {
    prefs = JSON.parse(localStorage.getItem("rsdash") || "{}");
  } catch (e) {
    prefs = {};
  }
  if (prefs.keys === false) S.keys = false;
  const savePrefs = () => {
    try {
      localStorage.setItem("rsdash", JSON.stringify({ keys: S.keys }));
    } catch (e) {
      /* storage blocked */
    }
  };
  const seenKey = () => "rsdash.seen." + F.journal;
  function loadSeen() {
    try {
      const v = localStorage.getItem(seenKey());
      return v == null ? null : Number(v);
    } catch (e) {
      return null;
    }
  }
  function saveSeen(seq) {
    try {
      localStorage.setItem(seenKey(), String(seq));
    } catch (e) {
      /* storage blocked: the watermark lasts for this page view only */
    }
  }

  // ---------- helpers ----------
  function h(tag, attrs, ...kids) {
    const svg = attrs && attrs.svg;
    const el = svg
      ? document.createElementNS("http://www.w3.org/2000/svg", tag)
      : document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k === "svg" || v == null || v === false) continue;
      if (k === "on")
        for (const [e, fn] of Object.entries(v)) el.addEventListener(e, fn);
      else if (k === "style" && typeof v === "object")
        Object.assign(el.style, v);
      else el.setAttribute(k, v === true ? "" : v);
    }
    for (const kid of kids.flat(Infinity))
      if (kid != null && kid !== false)
        el.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
    return el;
  }
  const s = (tag, attrs, ...k) => h(tag, { ...attrs, svg: true }, ...k);
  // Bidi controls and other invisible formatting characters are shown, never obeyed.
  const clean = (t) =>
    String(t == null ? "" : t).replace(
      /[‪-‮⁦-⁩‎‏؜]/g,
      (c) => `⟨U+${c.charCodeAt(0).toString(16).toUpperCase()}⟩`,
    );
  // One duration rule everywhere: seconds below a minute, m+ss below 10 min, minutes below an
  // hour, h+mm below a day, then d+h.
  function dur(sec) {
    if (sec == null || !isFinite(sec)) return "unknown";
    sec = Math.max(0, Math.round(sec));
    const d = Math.floor(sec / DAY),
      hh = Math.floor((sec % DAY) / 3600),
      mm = Math.floor((sec % 3600) / 60),
      ss = sec % 60;
    if (d) return `${d}d ${hh}h`;
    if (hh) return `${hh}h ${String(mm).padStart(2, "0")}m`;
    if (mm >= 10) return `${mm}m`;
    if (mm) return `${mm}m ${String(ss).padStart(2, "0")}s`;
    return `${ss}s`;
  }
  const agoTxt = (secs) => (secs < 45 ? "just now" : `${dur(secs)} ago`);
  function clock(r, sec) {
    if (r == null || !isFinite(r)) return "unknown";
    const d = new Date((T0 + r) * 1000);
    const t = d.toLocaleTimeString("en-CA", {
      hour: "2-digit",
      minute: "2-digit",
      second: sec ? "2-digit" : undefined,
      hour12: false,
    });
    if (Math.abs(rel() - r) < 20 * 3600) return t;
    return d.toLocaleDateString("en-CA", { weekday: "short" }) + " " + t;
  }
  const iso = (r) => new Date((T0 + r) * 1000).toISOString();
  function walltime(sec) {
    if (sec == null) return "no limit";
    const d = Math.floor(sec / DAY),
      hh = Math.floor((sec % DAY) / 3600),
      mm = Math.floor((sec % 3600) / 60),
      ss = Math.floor(sec % 60);
    const p = (n) => String(n).padStart(2, "0");
    return (d ? `${d}-${p(hh)}` : `${hh}`) + `:${p(mm)}:${p(ss)}`;
  }
  function bytes(n) {
    if (n == null) return "";
    const u = ["B", "K", "M", "G", "T"];
    let i = 0;
    while (n >= 1024 && i < u.length - 1) {
      n /= 1024;
      i++;
    }
    return (i ? n.toFixed(n < 10 ? 1 : 0) : String(n)) + u[i];
  }
  const midTrunc = (t, n) =>
    t.length <= n
      ? t
      : t.slice(0, Math.ceil((n - 1) / 2)) +
        "…" +
        t.slice(t.length - Math.floor((n - 1) / 2));
  const plural = (n, w) => `${n} ${w}${n === 1 ? "" : "s"}`;
  const job = (key) =>
    F.jobs.find((j) => j.key === key) ||
    (S.openJob && S.openJob.key === key ? S.openJob : null);
  const host = (id) =>
    F.hosts.find((x) => x.id === id) || {
      id,
      observedAt: null,
      accounting: "unknown",
      coverage: "tracked",
    };
  const age = (ho) =>
    ho.observedAt == null ? Infinity : rel() - ho.observedAt;
  const fresh = (ho) => age(ho) <= STALE_AFTER;
  const obsAt = (ho) => ho.observedAt;
  function counts(j) {
    const c = {
      COMPLETED: 0,
      RUNNING: 0,
      PENDING: 0,
      FAILED: 0,
      OTHER: 0,
      UNLISTED: 0,
    };
    j.array.states.forEach((t) => {
      if (t === "COMPLETED") c.COMPLETED++;
      else if (FAILED.has(t)) c.FAILED++;
      else if (isRun(t)) c.RUNNING++;
      else if (isPend(t)) c.PENDING++;
      else c.OTHER++;
    });
    c.UNLISTED = Math.max(0, j.array.n - j.array.states.length);
    return c;
  }
  const bucket = (t) =>
    t === "COMPLETED"
      ? "COMPLETED"
      : FAILED.has(t)
        ? "FAILED"
        : isRun(t)
          ? "RUNNING"
          : isPend(t)
            ? "PENDING"
            : "OTHER";
  const failedTasks = (j) =>
    j.array.ids.filter((id, i) => FAILED.has(j.array.states[i]));
  const asOf = (j) =>
    fresh(host(j.host)) ? rel() : (obsAt(host(j.host)) ?? rel());
  function elapsed(j) {
    if (j.start != null) {
      if (isRun(j.state)) return asOf(j) - j.start;
      if (j.end != null) return j.end - j.start;
    }
    return j.elapsed;
  }
  const finishedAt = (j) => j.end ?? j.vanishedAt ?? j.observedAt;
  const vanished = (j) => j.state === "UNKNOWN" && j.vanishedAt != null;
  const recent = (j) => (finishedAt(j) ?? -Infinity) > rel() - DAY;
  const needsAttention = (j) =>
    (FAILED.has(j.state) && recent(j)) ||
    (j.array &&
      failedTasks(j).length > 0 &&
      (ACTIVE.has(j.state) || recent(j)));
  // Accounting gap: sacct went silent before this job finished, so no accounting fields exist.
  const acctGap = (j) => {
    const ho = host(j.host);
    return (
      isDone(j.state) &&
      ho.accounting === "unavailable" &&
      (j.end == null ||
        ho.accountingSince == null ||
        j.end > ho.accountingSince)
    );
  };
  const retained = (j) => {
    const ho = host(j.host);
    return isDone(j.state) && ho.accounting === "unavailable" && !acctGap(j);
  };
  const peak = (j) => (acctGap(j) ? null : j.maxrss);
  const sacctSilent = (j) => host(j.host).accounting === "unavailable";
  const exitUnknown = (j) =>
    sacctSilent(j)
      ? `exit unknown (sacct silent since ${clock(host(j.host).accountingSince)})`
      : "exit unknown";
  // The one fact that explains a failure, kept in every summary.
  function keyFact(j) {
    if (j.state === "OUT_OF_MEMORY")
      return peak(j)
        ? `MaxRSS ${peak(j)} of ${j.mem || "its"} request`
        : `Over its ${j.mem || ""} memory request; MaxRSS unknown${acctGap(j) ? " (sacct silent)" : ""}`.replace(
            "  ",
            " ",
          );
    if (j.state === "TIMEOUT")
      return `Reached its ${walltime(j.timelimit)} limit`;
    if (j.state === "NODE_FAIL") return "A node it ran on failed";
    if (j.state === "PREEMPTED") return "Preempted by a higher-priority job";
    return j.exit ? `exit ${j.exit}` : exitUnknown(j);
  }
  const nameOf = (el) =>
    (el.dataset && el.dataset.name) ||
    el.getAttribute?.("aria-label") ||
    el.textContent.trim();

  // Re-render without losing keyboard focus: interactive nodes carry data-key.
  function keep(fn) {
    const a = document.activeElement,
      key = a && a.dataset ? a.dataset.key : null;
    const group = key ? key.split(":")[0] : null;
    const peers = group
      ? [...document.querySelectorAll(`[data-key^="${group}:"]`)]
      : [];
    const idx = peers.indexOf(a);
    const was = key ? nameOf(a) : null;
    fn();
    if (!key || (a.isConnected && document.activeElement === a)) return;
    let el = document.querySelector(`[data-key="${CSS.escape(key)}"]`);
    let handoff = false;
    if (!el && group === "alert") {
      el = document.querySelector('[data-key="alerts:more"]');
      handoff = !!el;
    }
    if (!el && idx >= 0) {
      handoff = true;
      // the focused item left (e.g. a job finished): fall back to its neighbour
      const now = [...document.querySelectorAll(`[data-key^="${group}:"]`)];
      el = now[Math.min(idx, now.length - 1)] || null;
      if (el && group === "row") {
        now.forEach((r) => (r.tabIndex = -1));
        el.tabIndex = 0;
        S.cursor = el.dataset.id;
      }
    }
    if (!el && (group === "ft" || group === "task"))
      el = document.querySelector('[data-key="dtab:log"]');
    if (!el && group === "alerts") el = document.getElementById("jobs-h");
    if (el && el !== document.activeElement) {
      el.focus({ preventScroll: true });
      if (handoff) {
        const now = nameOf(el);
        setTimeout(
          () =>
            announce(`${was} moved out of this view. Focus is now on ${now}.`),
          700,
        );
      }
    }
  }

  // ---------- network ----------
  async function api(path, body) {
    const init = body
      ? {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", "X-Rslurm": "1" },
          body: JSON.stringify(body),
        }
      : { credentials: "same-origin" };
    const r = await fetch(path, init);
    if (r.status === 401 || r.status === 403) {
      S.authLost = true;
      renderNote();
      throw new Error("access expired");
    }
    if (!r.ok) throw new Error(`${path} answered ${r.status}`);
    return r.json();
  }

  // ---------- data adapter: contract payloads -> the relative-time model ----------
  function adaptHost(x) {
    return {
      ...x,
      observedAt: R(x.observedAt),
      accountingSince: R(x.accountingSince),
    };
  }
  function adaptJob(j) {
    const o = {
      ...j,
      submit: R(j.submit),
      start: R(j.start),
      end: R(j.end),
      estStart: R(j.estStart),
      vanishedAt: R(j.vanishedAt),
      observedAt: R(j.observedAt),
    };
    if (j.array) {
      const taskInfo = {};
      for (const [k, v] of Object.entries(j.array.taskInfo || {}))
        taskInfo[k] = { ...v, end: R(v.end) };
      o.array = {
        n: j.array.n ?? (j.array.ids || []).length,
        ids: j.array.ids || [],
        states: j.array.states || [],
        limit: j.array.limit ?? null,
        pendingReason: j.array.pendingReason || null,
        exits: j.array.exits || {},
        taskInfo,
      };
    }
    return o;
  }
  const adaptEvent = (a) => ({ ...a, t: R(a.t) });
  function setSkew(serverNow) {
    if (typeof serverNow === "number") skew = serverNow - Date.now() / 1000;
  }
  function applyState(st, first) {
    setSkew(st.now);
    if (T0 == null) T0 = st.now;
    const prevOpen = S.open ? JSON.stringify(job(S.open)) : null;
    F.demo = !!st.demo;
    F.journal = st.journal;
    F.observer = st.observer || {};
    F.coverage = st.coverage || "";
    F.hosts = (st.hosts || []).map(adaptHost);
    F.jobs = (st.jobs || []).map(adaptJob);
    if (S.open) {
      const j = F.jobs.find((x) => x.key === S.open);
      if (j) S.openJob = j;
    }
    if (first) return;
    keep(() => {
      renderMeta();
      renderNote();
      renderHosts();
      renderJobs();
      renderAttention();
      renderTimeline();
      renderAlerts();
    });
    if (S.open && JSON.stringify(job(S.open)) !== prevOpen) keep(renderDrawer);
  }
  function addActivity(events, live) {
    const have = new Set(F.activity.map((a) => a.seq));
    const incoming = (events || [])
      .filter((a) => !have.has(a.seq))
      .map(adaptEvent);
    if (!incoming.length) return;
    F.activity.push(...incoming);
    F.activity.sort((a, b) => a.seq - b.seq);
    if (F.activity.length > 500) F.activity.splice(0, F.activity.length - 500);
    if (!live) return;
    keep(() => renderActivity(true));
    const alerts = incoming.filter((a) => a.alert && a.seq > S.seenSeq);
    if (alerts.length) {
      const a = alerts[alerts.length - 1];
      announce(
        `${a.kind === "fail" ? "Failure" : "Finished"}: ${a.text}.${a.detail ? " " + a.detail : ""}`,
      );
      if ("Notification" in window && Notification.permission === "granted") {
        for (const x of alerts.slice(-3)) {
          try {
            new Notification(x.text, {
              body: `${x.host}${x.detail ? " · " + x.detail : ""}`,
              tag: "rsdash-" + x.seq,
            });
          } catch (e) {
            /* blocked */
          }
        }
      }
      keep(renderAlerts);
    }
  }

  // ---------- header bits ----------
  function renderMeta() {
    document.getElementById("example").hidden = !F.demo;
    document.getElementById("coverage").textContent = F.coverage
      ? "Coverage: " + F.coverage
      : "";
  }
  function renderNote() {
    const box = document.getElementById("obsnote");
    const o = F.observer || {};
    let msg = null;
    if (S.authLost)
      msg = [
        h("b", null, "This page's access has expired."),
        " rslurm dash was restarted or closed. Open the link it printed when it started, or run ",
        h("code", null, "rslurm dash"),
        " again.",
      ];
    else if (S.bootError)
      msg = [h("b", null, "Could not load the dashboard data. "), S.bootError];
    else if (S.lost)
      msg = [
        h("b", null, "Lost contact with rslurm dash."),
        " Retrying every few seconds; what you see is the last data received. If you closed it, run ",
        h("code", null, "rslurm dash"),
        " again.",
      ];
    else if (o.note) msg = [o.note];
    else if (o.running === false)
      msg = [
        h("b", null, "The local observer is not running,"),
        " so nothing is being read from the clusters. This page shows the last recorded state.",
      ];
    else if (o.enabled === false)
      msg = [
        h("b", null, "Monitoring is turned off."),
        " Turn it on with ",
        h("code", null, "rslurm monitor enable"),
        ".",
      ];
    else if (o.build_ok === false)
      msg = [
        h("b", null, "The observer runs an older remoteslurm build."),
        " Restart it with ",
        h("code", null, "rslurm daemon stop"),
        " and reopen ",
        h("code", null, "rslurm dash"),
        ".",
      ];
    box.hidden = !msg;
    box.replaceChildren(...(msg || []));
  }

  // ---------- hosts band ----------
  function hostStats(hid) {
    const st = { run: 0, pend: 0, done: 0, fail: 0, nodes: 0, cores: 0 };
    F.jobs
      .filter((j) => j.host === hid)
      .forEach((j) => {
        if (isRun(j.state)) {
          st.run++;
          if (j.array) {
            const c = counts(j);
            st.nodes += c.RUNNING * (j.nodes || 0);
            st.cores += c.RUNNING * (j.cpus || 0);
          } else {
            st.nodes += j.nodes || 0;
            st.cores += j.cpus || 0;
          }
        } else if (isPend(j.state)) st.pend++;
        else if (recent(j) && j.state === "COMPLETED") st.done++;
        else if (recent(j) && FAILED.has(j.state)) st.fail++;
      });
    return st;
  }
  function ageText(ho) {
    if (ho.observedAt == null)
      return ho.error ? `not read · ${ho.error}` : "waiting for the first read";
    return fresh(ho)
      ? `live · read ${Math.max(0, Math.round(age(ho)))}s ago`
      : `stale · last read ${clock(obsAt(ho), true)} (${dur(age(ho))} ago)`;
  }
  function allocText(ho, st) {
    if (ho.alloc === "node")
      return `${plural(st.nodes, "node")} · ${st.cores.toLocaleString()} cores`;
    if (ho.alloc === "core") return `${st.cores.toLocaleString()} cores`;
    if (!st.nodes && !st.cores) return null;
    return `${plural(st.nodes, "node")} · ${st.cores.toLocaleString()} cores`;
  }
  function renderHosts() {
    document.getElementById("hosts").replaceChildren(
      ...F.hosts.map((ho) => {
        const st = hostStats(ho.id),
          ok = fresh(ho),
          never = ho.observedAt == null,
          any = st.run + st.pend + st.done + st.fail;
        const seg = (n, c) =>
          n ? h("span", { class: "b-" + c, style: { flexGrow: n } }) : null;
        const alloc = allocText(ho, st);
        return h(
          "button",
          {
            class: "hostcard" + (ok ? "" : " stale"),
            type: "button",
            "data-key": "host:" + ho.id,
            "data-name": ho.id,
            "aria-pressed": S.host === ho.id ? "true" : "false",
            "aria-label": `${ho.id}: ${ok ? "live" : never ? "not read yet" : "stale, last known values"}. ${st.run} running, ${st.pend} pending, ${st.fail} failed and ${st.done} completed in 24 hours.${ho.coverage === "tracked" ? " Shows registered work only." : ""} ${S.host === ho.id ? "Showing only this host; select to show all hosts." : "Select to show only this host."}`,
            on: {
              click: () => {
                S.host = S.host === ho.id ? null : ho.id;
                S.cursor = null;
                keep(renderAll);
                announce(
                  S.host ? `Showing ${ho.id} only` : "Showing all hosts",
                );
              },
            },
          },
          h(
            "div",
            { class: "hc-top" },
            h("span", { class: "beat", "aria-hidden": "true" }),
            h("span", { class: "hc-name" }, clean(ho.id)),
            h("span", { class: "hc-age num", "data-age": ho.id }, ageText(ho)),
            S.host === ho.id
              ? h("span", { class: "hc-filter" }, "Showing only this host")
              : null,
          ),
          h(
            "div",
            { class: "stack", "aria-hidden": "true" },
            any
              ? [
                  seg(st.run, "run"),
                  seg(st.pend, "pend"),
                  seg(st.fail, "fail"),
                  seg(st.done, "ok"),
                ]
              : h("span", { class: "b-none", style: { flexGrow: 1 } }),
          ),
          h(
            "div",
            { class: "hc-nums num" },
            ok
              ? null
              : h(
                  "span",
                  { class: "hc-lk" },
                  never ? "Not read yet" : `As of ${clock(obsAt(ho))}:`,
                ),
            h("span", null, h("b", null, st.run), " running"),
            h("span", null, h("b", null, st.pend), " pending"),
            h(
              "span",
              { class: st.fail ? "is-fail" : null },
              h("b", null, st.fail),
              " failed",
            ),
            h("span", null, h("b", null, st.done), " done"),
            h("span", { class: "muted" }, "last 24 h"),
            alloc
              ? h("span", { class: "hc-alloc" }, alloc + " allocated")
              : null,
          ),
          ho.accounting === "unavailable"
            ? h(
                "div",
                { class: "hc-warn" },
                ho.accountingSince != null
                  ? `Accounting (sacct) silent since ${clock(ho.accountingSince)}: later finishes may be missing`
                  : "Accounting (sacct) not answering: finished jobs may be missing",
              )
            : null,
          ho.coverage === "tracked"
            ? h(
                "div",
                { class: "hc-cov" },
                "Registered work only: jobs submitted outside remoteslurm are not shown",
              )
            : null,
        );
      }),
    );
  }
  function tickAges() {
    let flip = false;
    F.hosts.forEach((ho) => {
      const now = fresh(ho);
      if (ho._fresh !== undefined && ho._fresh !== now) flip = true;
      ho._fresh = now;
      const el = document.querySelector(`[data-age="${CSS.escape(ho.id)}"]`);
      if (el) el.textContent = ageText(ho);
    });
    if (flip) keep(renderAll);
  }

  // ---------- jobs ledger ----------
  const TABS = [
    { id: "active", label: "Active", f: (j) => ACTIVE.has(j.state) },
    { id: "attention", label: "Failures", f: needsAttention },
    { id: "finished", label: "Finished", f: (j) => isDone(j.state) || vanished(j) },
    { id: "all", label: "All", f: () => true },
  ];
  const visible = () =>
    F.jobs.filter(
      (j) =>
        (!S.host || j.host === S.host) &&
        (!S.q ||
          [j.name, j.id, j.partition, j.account, j.host]
            .join(" ")
            .toLowerCase()
            .includes(S.q.toLowerCase())),
    );

  function progress(j) {
    if (j.array) {
      const c = counts(j),
        n = j.array.n || 1;
      const seg = (k, c2) =>
        c[k]
          ? h("span", {
              class: "b-" + c2,
              style: { width: (100 * c[k]) / n + "%" },
            })
          : null;
      return h(
        "div",
        { class: "progress" },
        h(
          "div",
          { class: "bar", "aria-hidden": "true" },
          seg("COMPLETED", "ok"),
          seg("FAILED", "fail"),
          seg("RUNNING", "run"),
        ),
        h(
          "small",
          { class: "num" },
          h("span", { class: "nw" }, `${c.COMPLETED} of ${n} tasks done`),
          " · ",
          h("span", { class: "nw" }, `${c.RUNNING} running`),
          c.FAILED
            ? h("b", { class: "t-fail nw" }, ` · ${c.FAILED} failed`)
            : null,
        ),
      );
    }
    if (isRun(j.state)) {
      const e = elapsed(j);
      if (e == null) return h("small", { class: "muted num" }, "running");
      if (!j.timelimit)
        return h("small", { class: "muted num" }, `${dur(e)} · no limit`);
      const frac = Math.min(1, e / j.timelimit);
      return h(
        "div",
        { class: "progress" },
        h(
          "div",
          { class: "bar", "aria-hidden": "true" },
          h("span", {
            class: frac > 0.85 ? "b-warn" : "b-run",
            style: { width: frac * 100 + "%" },
          }),
        ),
        h(
          "small",
          { class: "num" },
          h("span", { class: "nw" }, `${dur(e)} of ${walltime(j.timelimit)}`),
          frac > 0.85 ? h("b", { class: "t-warn nw" }, " · near limit") : null,
        ),
      );
    }
    if (isPend(j.state))
      return h(
        "div",
        { class: "reason" },
        h("span", null, reasonText(j.reason)),
        j.reason ? h("small", { class: "mono" }, j.reason) : null,
      );
    if (vanished(j))
      return h("small", { class: "muted" }, "left the queue; accounting has not reported how it ended");
    if (j.state === "UNKNOWN")
      return h("small", { class: "muted" }, "not seen by the scheduler");
    const e = elapsed(j);
    if (j.state === "CANCELLED")
      return h(
        "small",
        { class: "muted num" },
        e == null ? "cancelled" : `cancelled after ${dur(e)}`,
      );
    return h(
      "small",
      { class: "muted num" },
      e == null
        ? "run time unknown"
        : `ran ${dur(e)}${j.timelimit ? ` of ${walltime(j.timelimit)}` : ""}`,
    );
  }
  function when(j) {
    const ho = host(j.host),
      stale = !fresh(ho);
    let txt;
    if (isPend(j.state))
      txt =
        j.estStart != null
          ? `Slurm est. start ${clock(j.estStart)}`
          : j.submit != null
            ? `queued ${agoTxt(rel() - j.submit)}`
            : "queued";
    else if (isRun(j.state))
      txt = j.start != null ? `since ${clock(j.start)}` : "running";
    else if (j.end != null) txt = `ended ${agoTxt(rel() - j.end)}`;
    else if (vanished(j)) txt = `left the queue ${agoTxt(rel() - j.vanishedAt)}`;
    else if (j.submit != null) txt = `submitted ${agoTxt(rel() - j.submit)}`;
    else txt = "time unknown";
    return h(
      "div",
      null,
      h("span", { class: "num" }, txt),
      stale && ACTIVE.has(j.state)
        ? h(
            "div",
            { class: "asof" },
            obsAt(ho) == null
              ? "host not read yet"
              : `as of ${clock(obsAt(ho))} · host stale`,
          )
        : null,
      retained(j)
        ? h(
            "div",
            { class: "asof" },
            `retained · as of ${clock(ho.accountingSince)}`,
          )
        : null,
      acctGap(j)
        ? h(
            "div",
            { class: "asof" },
            `as of ${clock(j.observedAt ?? obsAt(ho))} (squeue) · no accounting`,
          )
        : null,
      j.source === "retained" && !retained(j) && !acctGap(j)
        ? h("div", { class: "asof" }, `retained · as of ${clock(j.observedAt)}`)
        : null,
    );
  }
  function detail(j) {
    if (FAILED.has(j.state)) return keyFact(j);
    if (j.array) {
      const c = counts(j);
      if (c.PENDING)
        return j.array.limit
          ? `${c.PENDING} waiting on the %${j.array.limit} throttle`
          : `${c.PENDING} waiting · ${reasonText(j.array.pendingReason || j.reason).toLowerCase()}`;
      return j.nodelist
        ? h("span", { class: "mono" }, j.nodelist)
        : `${plural(j.array.n, "task")}`;
    }
    if (j.state === "CANCELLED")
      return j.exit
        ? h("span", null, "exit ", h("span", { class: "mono" }, j.exit))
        : exitUnknown(j);
    if (isPend(j.state))
      return [
        j.nodes ? plural(j.nodes, "node") : null,
        j.cpus ? `${j.cpus} cores` : null,
        walltime(j.timelimit),
      ]
        .filter(Boolean)
        .join(" · ");
    if (j.state === "COMPLETED")
      return j.exit
        ? h("span", null, "exit ", h("span", { class: "mono" }, j.exit))
        : exitUnknown(j);
    if (vanished(j)) return "outcome unknown: sacct has no record yet";
    if (j.state === "UNKNOWN")
      return j.tracked ? "registered with remoteslurm" : "";
    return j.nodelist
      ? h("span", { class: "mono" }, j.nodelist)
      : j.nodes
        ? plural(j.nodes, "node")
        : "";
  }
  function chip(state, stale) {
    return h(
      "span",
      { class: "chip c-" + cls(state) + (stale ? " is-stale" : "") },
      h("i", { "aria-hidden": "true" }),
      label(state),
    );
  }

  function rowFor(j) {
    const stale = !fresh(host(j.host));
    return h(
      "tr",
      {
        class: "r-" + cls(j.state) + (stale ? " is-stale" : ""),
        "data-id": j.key,
        "data-host": j.host,
        "data-key": "row:" + j.key,
        "data-name": `${clean(j.name)} ${j.id} on ${j.host}`,
        tabindex: S.cursor === j.key ? "0" : "-1",
        "aria-current": S.open === j.key ? "true" : null,
        on: {
          click: () => {
            S.cursor = j.key;
            openDrawer(j.key);
          },
          keydown: rowKeys,
          focus: () => {
            S.cursor = j.key;
          },
        },
      },
      h(
        "td",
        { class: "c-state" },
        chip(vanished(j) ? "VANISHED" : j.state, stale && ACTIVE.has(j.state)),
      ),
      h(
        "td",
        { class: "c-job" },
        h("span", { class: "jobname" }, clean(j.name || "(no name)")),
        j.array ? h("span", { class: "mono muted" }, ` [${j.array.n}]`) : null,
        h("span", { class: "mono muted jid" }, " " + j.id),
        h("span", { class: "jhost" }, " · " + j.host),
      ),
      h("td", { class: "c-host" }, clean(j.host)),
      h("td", { class: "c-prog" }, progress(j)),
      h("td", { class: "c-when" }, when(j)),
      h("td", { class: "c-detail" }, detail(j)),
    );
  }
  function renderJobs() {
    const vis = visible();
    const tabsEl = document.getElementById("tabs");
    const tabBtns = TABS.map((t) => {
      const n = vis.filter(t.f).length,
        sel = S.tab === t.id;
      return h(
        "button",
        {
          role: "tab",
          type: "button",
          id: "tab-" + t.id,
          "data-key": "tab:" + t.id,
          "aria-selected": String(sel),
          "aria-controls": "jobpanel",
          tabindex: sel ? "0" : "-1",
          class: t.id === "attention" && n ? "has-fail" : null,
          on: { click: () => setTab(t.id), keydown: tabKeys },
        },
        t.label,
        h("span", { class: "n num" }, n),
      );
    });
    if (tabsEl.children.length === TABS.length)
      [...tabsEl.children].forEach((b, i) => {
        // patch in place: focus stays put
        b.replaceChildren(...tabBtns[i].childNodes);
        for (const a of ["aria-selected", "tabindex", "class"]) {
          const v = tabBtns[i].getAttribute(a);
          v == null ? b.removeAttribute(a) : b.setAttribute(a, v);
        }
      });
    else tabsEl.replaceChildren(...tabBtns);
    document
      .getElementById("jobpanel")
      .setAttribute("aria-labelledby", "tab-" + S.tab);
    const tab = TABS.find((t) => t.id === S.tab);
    const rank = (j) =>
      needsAttention(j) && !ACTIVE.has(j.state)
        ? 0
        : isRun(j.state)
          ? 1
          : isPend(j.state)
            ? 2
            : j.state === "UNKNOWN"
              ? 4
              : 3;
    const t = (j) => j.end ?? j.start ?? j.submit ?? j.observedAt ?? -Infinity;
    const rows = vis
      .filter(tab.f)
      .sort((a, b) => rank(a) - rank(b) || t(b) - t(a));
    const body = document.getElementById("jobrows");
    if (!rows.length) {
      const msg = !F.jobs.length
        ? null
        : S.q
          ? `No jobs match “${S.q}”.`
          : S.tab === "attention"
            ? "No failures in the last 24 hours."
            : S.tab === "active"
              ? "Nothing queued or running right now."
              : "Nothing here right now.";
      body.replaceChildren(
        h(
          "tr",
          null,
          h(
            "td",
            { colspan: 6 },
            msg ? h("p", { class: "none" }, msg) : emptyState(),
          ),
        ),
      );
      return;
    }
    const tracked = rows.filter((j) => j.tracked),
      untracked = rows.filter((j) => !j.tracked);
    const order = [...tracked, ...untracked].map((j) => j.key);
    if (!order.includes(S.cursor)) S.cursor = order[0];
    const out = tracked.map(rowFor);
    if (untracked.length) {
      out.push(
        h(
          "tr",
          { class: "groupcap" },
          h(
            "td",
            { colspan: 6 },
            h("span", { class: "tag dashed" }, "untracked"),
            ` ${untracked.length} submitted outside remoteslurm. Scheduler state only: no output checks, no validation.`,
          ),
        ),
      );
      untracked.forEach((j) => out.push(rowFor(j)));
    }
    // Same rows in the same order: patch cells in place so a focused row is never rebuilt.
    const old = [...body.children];
    const same =
      old.length === out.length &&
      old.every(
        (tr, i) => (tr.dataset.id || "cap") === (out[i].dataset.id || "cap"),
      );
    if (same)
      old.forEach((tr, i) => {
        tr.replaceChildren(...out[i].childNodes);
        for (const a of ["class", "tabindex", "aria-current", "data-name"]) {
          const v = out[i].getAttribute(a);
          v == null ? tr.removeAttribute(a) : tr.setAttribute(a, v);
        }
      });
    else body.replaceChildren(...out);
  }
  function setTab(id) {
    S.tab = id;
    S.cursor = null;
    keep(renderJobs);
  }
  function tabKeys(e) {
    const i = TABS.findIndex((t) => t.id === S.tab);
    const next =
      e.key === "ArrowRight"
        ? i + 1
        : e.key === "ArrowLeft"
          ? i - 1
          : e.key === "Home"
            ? 0
            : e.key === "End"
              ? TABS.length - 1
              : null;
    if (next == null) return;
    e.preventDefault();
    S.tab = TABS[(next + TABS.length) % TABS.length].id;
    S.cursor = null;
    renderJobs();
    document.getElementById("tab-" + S.tab).focus();
  }
  function rowKeys(e) {
    if (e.altKey || e.ctrlKey || e.metaKey) return;
    const rows = [...document.querySelectorAll("#jobrows tr[data-id]")];
    const i = rows.indexOf(e.currentTarget);
    let n = null;
    if (e.key === "ArrowDown" || e.key === "j")
      n = Math.min(rows.length - 1, i + 1);
    else if (e.key === "ArrowUp" || e.key === "k") n = Math.max(0, i - 1);
    else if (e.key === "Home") n = 0;
    else if (e.key === "End") n = rows.length - 1;
    else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      openDrawer(e.currentTarget.dataset.id);
      return;
    }
    if (n == null) return;
    e.preventDefault();
    rows.forEach((r) => (r.tabIndex = -1));
    rows[n].tabIndex = 0;
    rows[n].focus();
    S.cursor = rows[n].dataset.id;
  }
  function emptyState() {
    const names = F.hosts.map((x) => x.id);
    const all = F.hosts.length && F.hosts.every((x) => x.coverage === "all");
    const first = names[0] || "HOST";
    return h(
      "div",
      { class: "empty" },
      h("h3", null, "No jobs to show"),
      h(
        "p",
        null,
        names.length
          ? `Watching ${names.join(" and ")}. ` +
              (all
                ? "Jobs you submit, with rslurm or with plain sbatch, appear here within about 30 seconds."
                : "Jobs submitted through rslurm appear here within about 30 seconds of the next scheduler read.")
          : "No hosts are being watched yet.",
      ),
      h(
        "div",
        { class: "cmds" },
        h("code", null, `rslurm -H ${first} submit job.sh`),
        all
          ? h("code", null, "sbatch job.sh   # also shown, marked untracked")
          : null,
      ),
    );
  }

  // ---------- attention + activity ----------
  function attentionItems() {
    const items = [];
    F.hosts
      .filter((x) => !fresh(x))
      .forEach((x) =>
        items.push(
          x.observedAt == null
            ? {
                g: "warn",
                t: `${x.id} has not been read yet`,
                d:
                  (x.error ? `The last attempt failed (${x.error}). ` : "") +
                  (x.action ||
                    `Check the connection with rslurm -H ${x.id} doctor.`),
              }
            : {
                g: "warn",
                t: `${x.id} is stale`,
                d:
                  `No scheduler read since ${clock(obsAt(x), true)}. Its jobs show their last known state, marked “as of”. ` +
                  (x.action ||
                    `Check the connection with rslurm -H ${x.id} doctor.`),
              },
        ),
      );
    F.hosts
      .filter((x) => x.accounting === "unavailable")
      .forEach((x) =>
        items.push({
          g: "warn",
          t: `${x.id}: accounting not answering`,
          d:
            x.accountingSince != null
              ? `sacct has not answered since ${clock(x.accountingSince)}, so jobs on ${x.id} that finished after that may be missing. Finished jobs listed for ${x.id} come from earlier reads.`
              : `sacct is not answering on ${x.id}, so finished jobs there may be missing.`,
        }),
      );
    F.jobs
      .filter((j) => FAILED.has(j.state) && recent(j))
      .sort((a, b) => (finishedAt(b) ?? 0) - (finishedAt(a) ?? 0))
      .forEach((j) =>
        items.push({
          g: "fail",
          t: `${j.name}: ${label(j.state).toLowerCase()}`,
          d: j.hint || keyFact(j) + ".",
          act: j.array ? "See failed tasks" : "Open log",
          job: j.key,
          dtab: j.array ? "tasks" : null,
          meta: `${j.host} · ${j.end != null ? "ended " + clock(j.end) : "end time unknown"}`,
        }),
      );
    F.jobs
      .filter((j) => j.array && !FAILED.has(j.state) && needsAttention(j))
      .forEach((j) => {
        const ft = failedTasks(j);
        const list =
          ft.length > 12
            ? ft.slice(0, 12).join(", ") + ` and ${ft.length - 12} more`
            : ft.join(", ");
        items.push({
          g: "fail",
          t: `${j.name}: ${ft.length} of ${j.array.n} tasks failed`,
          d: `Task${ft.length === 1 ? "" : "s"} ${list} ended in failure.${ACTIVE.has(j.state) ? " The other tasks keep running." : ""}`,
          act: "See failed tasks",
          job: j.key,
          dtab: "tasks",
          meta: `${j.host} · ${ACTIVE.has(j.state) ? "array still running" : "array finished"}`,
        });
      });
    return items;
  }
  function renderAttention() {
    const items = attentionItems();
    const n = document.getElementById("attn-n");
    n.textContent = items.length || "";
    n.hidden = !items.length;
    document.getElementById("attention").replaceChildren(
      ...(items.length
        ? items.map((it) =>
            h(
              "li",
              { class: "a-" + it.g },
              h("span", { class: "sym s-" + it.g, "aria-hidden": "true" }),
              h(
                "div",
                null,
                h("div", { class: "t" }, clean(it.t)),
                it.meta ? h("div", { class: "meta" }, it.meta) : null,
                h("div", { class: "d" }, clean(it.d)),
                it.job
                  ? h(
                      "button",
                      {
                        class: "link",
                        type: "button",
                        "data-key": "att:" + it.job + ":" + (it.dtab || ""),
                        "data-name": `${it.act} for ${clean(it.t)}`,
                        on: { click: () => openDrawer(it.job, it.dtab) },
                      },
                      it.act,
                      h("span", { "aria-hidden": "true" }, " →"),
                    )
                  : null,
              ),
            ),
          )
        : [
            h(
              "li",
              { class: "a-ok" },
              h("span", { class: "sym s-ok", "aria-hidden": "true" }),
              h(
                "div",
                null,
                h("div", { class: "t" }, "Nothing needs attention"),
                h(
                  "div",
                  { class: "d" },
                  F.hosts.length
                    ? "Every host is live and nothing failed in the last 24 hours."
                    : "No hosts are being watched yet.",
                ),
              ),
            ),
          ]),
    );
  }
  function renderActivity(flash) {
    const list = F.activity
      .filter((a) => !S.host || a.host === S.host)
      .sort((a, b) => b.seq - a.seq);
    const feed = document.getElementById("feed");
    const toggle = document.getElementById("feed-more");
    if (!list.length) {
      toggle.hidden = true;
      feed.replaceChildren(h("li", { class: "none" }, "No activity yet."));
      return;
    }
    const total = list.length;
    let shown = S.feedAll ? list : list.slice(0, 8);
    // A focused feed item stays rendered even when newer items push it past the first eight.
    const a0 = document.activeElement;
    const fk =
      a0 && a0.dataset && a0.dataset.key && a0.dataset.key.startsWith("feed:")
        ? a0.dataset.key
        : null;
    if (fk && !shown.some((a) => "feed:" + a.seq === fk)) {
      const keepIt = list.find((a) => "feed:" + a.seq === fk);
      if (keepIt) shown = [...shown, keepIt];
    }
    toggle.hidden = total <= 8;
    toggle.textContent = S.feedAll ? "Show fewer" : `Show all ${total}`;
    toggle.setAttribute("aria-expanded", String(!!S.feedAll));
    feed.replaceChildren(
      ...shown.map((a, i) => {
        const j = a.job ? job(a.host + ":" + a.job) : null;
        const body = [
          h("span", { class: "t" }, clean(a.text)),
          h(
            "span",
            { class: "d" },
            clean(`${a.host}${a.detail ? " · " + a.detail : ""}`),
          ),
        ];
        return h(
          "li",
          { class: flash && i === 0 ? "new" : null },
          h(
            "time",
            { class: "num", datetime: a.t != null ? iso(a.t) : null },
            clock(a.t),
          ),
          h("span", {
            class: "sym s-" + (a.kind || "info"),
            "aria-hidden": "true",
          }),
          j
            ? h(
                "button",
                {
                  class: "msg",
                  type: "button",
                  "data-key": "feed:" + a.seq,
                  "data-name": clean(a.text),
                  on: { click: () => openDrawer(j.key) },
                },
                body,
              )
            : h("div", { class: "msg static" }, body),
        );
      }),
    );
  }

  // ---------- timeline ----------
  function renderTimeline() {
    const box = document.getElementById("timeline");
    const W = Math.max(280, box.clientWidth || 900);
    const narrow = W < 640,
      left = narrow ? 112 : 150,
      right = 14,
      top = 24,
      rowH = 24;
    const js = F.jobs
      .filter(
        (j) =>
          (!S.host || j.host === S.host) &&
          j.state !== "UNKNOWN" &&
          (j.start != null || j.submit != null) &&
          (ACTIVE.has(j.state) ? true : (j.end ?? -Infinity) > rel() - DAY) &&
          !(isPend(j.state) && (j.submit ?? 0) < rel() - DAY),
      )
      .sort((a, b) => (a.start ?? a.submit) - (b.start ?? b.submit))
      .slice(-60);
    const note = document.getElementById("tl-note");
    if (!js.length) {
      box.replaceChildren(
        h("p", { class: "none" }, "No jobs ran in the last 24 hours."),
      );
      note.textContent = "Last 24 hours";
      return;
    }
    const x1 = rel();
    const first = Math.min(
      ...js.map((j) =>
        Math.max(Math.min(j.start ?? j.submit, j.submit ?? j.start), x1 - DAY),
      ),
    );
    const x0 = Math.max(x1 - DAY, Math.floor((T0 + first) / 3600) * 3600 - T0);
    note.textContent = `Since ${clock(x0)} · dashed = waiting in queue · dotted = unknown since last read · ◆ = failed array task`;
    const span = Math.max(60, x1 - x0),
      H = top + js.length * rowH + 6;
    const x = (r) =>
      left + ((Math.min(Math.max(r, x0), x1) - x0) / span) * (W - left - right);
    const step =
      span / 3600 > 12
        ? narrow
          ? 6
          : 3
        : narrow
          ? 3
          : span / 3600 > 3
            ? 2
            : 1;
    const kids = [];
    for (let hr = Math.ceil((T0 + x0) / 3600); hr * 3600 - T0 <= x1; hr++) {
      const r = hr * 3600 - T0;
      const localHour = new Date(hr * 3600 * 1000).getHours();
      if (localHour % step) continue;
      kids.push(
        s("line", { x1: x(r), x2: x(r), y1: top - 6, y2: H, class: "grid" }),
      );
      if (x(x1) - x(r) > 44)
        kids.push(
          s(
            "text",
            { x: x(r), y: 13, "text-anchor": "middle", class: "tick" },
            clock(r),
          ),
        );
    }
    kids.push(
      s("line", { x1: x(x1), x2: x(x1), y1: top - 6, y2: H, class: "nowline" }),
    );
    kids.push(
      s(
        "text",
        { x: x(x1) - 3, y: 13, "text-anchor": "end", class: "tick now" },
        "now",
      ),
    );
    if (!js.some((j) => j.key === S.tl)) S.tl = js[0].key;
    js.forEach((j, i) => {
      const y = top + i * rowH,
        ho = host(j.host),
        stale = !fresh(ho);
      const lastRead = obsAt(ho) ?? x1;
      const g = s("g", {
        class: "tl-row",
        tabindex: S.tl === j.key ? "0" : "-1",
        role: "button",
        "data-key": "tl:" + j.key,
        "data-id": j.key,
        "data-name": `${clean(j.name)} ${j.id} on the timeline`,
        "aria-label": `${clean(j.name)} ${j.id}: ${label(j.state)}. Open details.`,
        on: {
          click: () => openDrawer(j.key),
          keydown: tlKeys,
          focus: () => {
            S.tl = j.key;
          },
        },
      });
      g.append(
        s("rect", { x: 0, y: y, width: W, height: rowH, class: "tl-hit" }),
      );
      g.append(
        s(
          "text",
          { x: 0, y: y + 16, class: "tl-lbl" },
          midTrunc(clean(j.name || j.id), narrow ? 14 : 22),
        ),
      );
      if (j.submit != null && (j.start == null || j.start > j.submit)) {
        const qEnd = j.start ?? (stale ? lastRead : x1);
        g.append(
          s("line", {
            x1: x(j.submit),
            x2: x(qEnd),
            y1: y + 12,
            y2: y + 12,
            class: "tl-queue",
          }),
        );
      }
      if (j.start != null) {
        const end =
          j.end ??
          (isRun(j.state) ? (stale ? lastRead : x1) : (j.observedAt ?? x1));
        g.append(
          s("rect", {
            x: x(j.start),
            y: y + 6,
            width: Math.max(3, x(end) - x(j.start)),
            height: 12,
            rx: 3,
            class: "tl-bar f-" + cls(j.state),
          }),
        );
        if (j.array && j.array.taskInfo)
          Object.values(j.array.taskInfo).forEach((ti) => {
            if (ti.end == null) return;
            g.append(
              s("rect", {
                x: x(ti.end) - 4,
                y: y + 8,
                width: 8,
                height: 8,
                class: "tl-taskfail",
                transform: `rotate(45 ${x(ti.end)} ${y + 12})`,
              }),
            );
          });
        if (isRun(j.state) && stale) {
          g.append(
            s("line", {
              x1: x(end),
              x2: x(x1),
              y1: y + 12,
              y2: y + 12,
              class: "tl-unknown",
            }),
          );
          g.append(
            s("circle", {
              cx: x(end),
              cy: y + 12,
              r: 4.5,
              class: "tl-lastread",
            }),
          );
        }
      }
      g.append(
        s(
          "title",
          null,
          `${clean(j.name)} ${j.id}: ${label(j.state)}${stale && ACTIVE.has(j.state) ? ", unknown since " + clock(obsAt(ho)) : ""}`,
        ),
      );
      kids.push(g);
    });
    box.replaceChildren(
      s(
        "svg",
        {
          width: W,
          height: H,
          viewBox: `0 0 ${W} ${H}`,
          role: "group",
          "aria-label": `Timeline of ${js.length} jobs. Use up and down arrows to move between jobs.`,
        },
        kids,
      ),
    );
  }
  function tlKeys(e) {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      openDrawer(e.currentTarget.dataset.id);
      return;
    }
    if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(e.key)) return;
    e.preventDefault();
    const rows = [...document.querySelectorAll(".tl-row")];
    let n;
    if (e.key === "Home") n = rows[0];
    else if (e.key === "End") n = rows[rows.length - 1];
    else {
      const i = rows.indexOf(e.currentTarget);
      n =
        rows[
          Math.max(
            0,
            Math.min(rows.length - 1, i + (e.key === "ArrowDown" ? 1 : -1)),
          )
        ];
    }
    rows.forEach((r) => r.setAttribute("tabindex", "-1"));
    n.setAttribute("tabindex", "0");
    n.focus();
    S.tl = n.dataset.id;
  }

  // ---------- modal plumbing (drawer and help share it) ----------
  function setInert(on) {
    document.getElementById("app").inert = on;
  }
  function trapTab(e, container, onEsc) {
    if (e.key === "Escape") {
      e.preventDefault();
      onEsc();
      return;
    }
    if (e.key !== "Tab") return;
    const f = [
      ...container.querySelectorAll('button, input, [tabindex="0"], a[href]'),
    ].filter((x) => !x.disabled && x.getClientRects().length);
    if (!f.length) return;
    const first = f[0],
      last = f[f.length - 1];
    if (!container.contains(document.activeElement)) {
      e.preventDefault();
      first.focus();
    } else if (e.shiftKey && document.activeElement === first) {
      e.preventDefault();
      last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault();
      first.focus();
    }
  }

  // ---------- bounded reads (logs, file listings) through the daemon's queue ----------
  const READS = new Map();
  const readKey = (kind, j, which, task) =>
    `${kind}|${j.key}|${which || ""}|${task ?? ""}`;
  function readEntry(kind, j, which, task) {
    return READS.get(readKey(kind, j, which, task));
  }
  function startRead(kind, j, which, task) {
    const k = readKey(kind, j, which, task);
    const e = READS.get(k) || {};
    if (e.inflight) return;
    Object.assign(e, {
      inflight: true,
      started: Date.now(),
      giveup: false,
      failed: null,
    });
    READS.set(k, e);
    const body =
      kind === "files"
        ? { host: j.host, job: j.id }
        : { host: j.host, job: j.id, which, task: task ?? null };
    const done = () => {
      e.inflight = false;
      if (e.state === "ERROR" && e.error === "budget_exhausted" && (e.retries || 0) < 6) {
        e.retries = (e.retries || 0) + 1;
        setTimeout(() => {
          if (S.open === j.key) startRead(kind, j, which, task);
        }, 6000);
      } else if (e.state === "DONE") e.retries = 0;
      refreshPane();
    };
    const merge = (res) => {
      e.state = res.state;
      e.request = res.request || null;
      e.path = res.path || e.path || null;
      e.pathGuessed = !!res.pathGuessed;
      e.error = res.error || null;
      e.action = res.action || null;
      if (res.result) e.result = res.result;
      if (res.cached && !e.result) e.cached = res.cached;
    };
    const poll = () => {
      if (e.state !== "QUEUED" || !e.request) return done();
      if (Date.now() - e.started > 30000) {
        e.giveup = true;
        return done();
      }
      refreshPane();
      setTimeout(() => {
        api(
          `/api/${kind === "files" ? "files" : "tail"}/${encodeURIComponent(e.request)}`,
        )
          .then((res) => {
            merge(res);
            poll();
          })
          .catch((err) => {
            e.failed = String(err.message || err);
            done();
          });
      }, 1000);
    };
    api(kind === "files" ? "/api/files" : "/api/tail", body)
      .then((res) => {
        merge(res);
        poll();
      })
      .catch((err) => {
        e.failed = String(err.message || err);
        done();
      });
    refreshPane();
  }
  // Re-render only the drawer's panel (keeps the drawer, its scroll and focus where they are).
  function refreshPane() {
    if (!S.open) return;
    const panel = document.getElementById("dpanel");
    const j = job(S.open);
    if (!panel || !j) return;
    keep(() => panel.replaceChildren(paneBody(j)));
  }
  function wantRead(j) {
    if (S.dtab === "log" || S.dtab === "err") {
      const which = S.dtab === "log" ? "out" : "err";
      if (!logPathKnown(j, which) || (isPend(j.state) && !j.array)) return;
      const task = j.array ? currentTask(j) : null;
      const e = readEntry("tail", j, which, task);
      if (
        !e ||
        (!e.inflight &&
          !e.result &&
          !e.giveup &&
          !e.failed &&
          e.state !== "UNAVAILABLE")
      )
        startRead("tail", j, which, task);
    } else if (S.dtab === "files" && j.workdir) {
      const e = readEntry("files", j);
      if (!e) startRead("files", j);
    }
  }
  const currentTask = (j) =>
    S.task != null ? S.task : (failedTasks(j)[0] ?? j.array.ids[0] ?? 0);
  function logPathKnown(j, which) {
    if (which === "out") return !!j.stdout;
    return !!j.stderr && j.stderr !== j.stdout;
  }
  function displayPath(j, which, task) {
    const p = which === "out" ? j.stdout : j.stderr;
    if (!p) return null;
    if (
      j.array &&
      j.array.pathsTask != null &&
      task != null &&
      String(task) !== String(j.array.pathsTask) &&
      !/%a/.test(p)
    )
      return null;
    let out = p.replace(/%j/g, j.id).replace(/%A/g, j.id);
    if (task != null) out = out.replace(/%a/g, String(task));
    return out;
  }

  // ---------- drawer ----------
  let lastFocusKey = null;
  function openDrawer(key, dtab) {
    const j = job(key);
    if (!j) return;
    const a = document.activeElement;
    lastFocusKey = a && a.dataset ? a.dataset.key : null;
    S.open = key;
    S.openJob = j;
    S.paused = false;
    S.task = j.array ? (failedTasks(j)[0] ?? null) : null;
    S.dtab =
      dtab ||
      (j.array
        ? "tasks"
        : FAILED.has(j.state) && logPathKnown(j, "err")
          ? "err"
          : "log");
    S.cursor = key;
    renderDrawer();
    keep(renderJobs);
    requestAnimationFrame(() =>
      document.querySelector(".drawer .close")?.focus(),
    );
  }
  function closeDrawer() {
    S.open = null;
    S.openJob = null;
    renderDrawer();
    renderJobs();
    const back =
      lastFocusKey &&
      document.querySelector(`[data-key="${CSS.escape(lastFocusKey)}"]`);
    (
      back ||
      document.querySelector('#jobrows tr[data-id][tabindex="0"]') ||
      document.getElementById("q")
    ).focus();
  }
  function logPane(j, which) {
    const ho = host(j.host),
      stale = !fresh(ho);
    const task = j.array ? currentTask(j) : null;
    if (which === "err" && !logPathKnown(j, "err")) {
      return h(
        "p",
        { class: "none" },
        j.stdout
          ? "No separate error log: stderr goes to the output log (the job was submitted without --error)."
          : "No error log path is known for this job.",
      );
    }
    if (which === "out" && !j.stdout) {
      return h(
        "div",
        { class: "none" },
        h("p", null, "Slurm did not report an output log path for this job."),
        j.workdir
          ? h(
              "p",
              null,
              "If the script used Slurm's default, the log would be ",
              h("span", { class: "mono" }, `${j.workdir}/slurm-${j.id}.out`),
              " (a guess, not checked).",
            )
          : null,
      );
    }
    const e = readEntry("tail", j, which, task) || {};
    const path = e.path || displayPath(j, which, task);
    const guessed = e.path
      ? e.pathGuessed || (e.path === j.stdout && j.pathsGuessed)
      : j.pathsGuessed;
    const data = e.result || e.cached || null;
    const isCached = !e.result && !!e.cached;
    const lines = data && data.lines ? data.lines : [];
    let readNote;
    if (data)
      readNote =
        `${lines.length ? `last ${plural(lines.length, "line")} · ` : ""}` +
        (isCached
          ? `as of ${clock(R(data.observedAt))}`
          : `read ${agoTxt(rel() - R(data.observedAt))}`) +
        (e.inflight ? " · refreshing…" : "");
    else if (e.inflight) readNote = "asking the daemon…";
    else readNote = "not read yet";
    const head = h(
      "div",
      { class: "logbar" },
      h("span", { class: "mono path" }, clean(path || "")),
      h("span", { class: "muted num" }, readNote),
    );
    const notes = [];
    if (guessed)
      notes.push(
        h(
          "p",
          { class: "muted" },
          "This path is Slurm's default (WorkDir/slurm-<id>.out), a guess: the job did not record its log path.",
        ),
      );
    if (data && data.gap) notes.push(h("p", { class: "note-gap" }, data.gap));
    else if (data && data.truncated)
      notes.push(
        h(
          "p",
          { class: "note-gap" },
          "Bounded tail: earlier bytes are omitted.",
        ),
      );
    let content;
    if (j.array && task != null) {
      const ti = j.array.taskInfo?.[task];
      const ex = j.array.exits?.[task];
      if (ti || ex)
        notes.unshift(
          h(
            "dl",
            { class: "facts taskfacts" },
            [
              ["Task", String(task)],
              ["Exit", ex ? h("span", { class: "mono" }, ex) : "unknown"],
              [
                "Elapsed",
                ti && ti.elapsed != null
                  ? `${dur(ti.elapsed)}${j.timelimit ? ` of ${walltime(j.timelimit)}` : ""}`
                  : "unknown",
              ],
              [
                "MaxRSS",
                ti && ti.maxrss
                  ? `${ti.maxrss}${j.mem ? ` of ${j.mem}` : ""}`
                  : "unknown",
              ],
              [
                "Node",
                ti && ti.node
                  ? h("span", { class: "mono" }, ti.node)
                  : "unknown",
              ],
              ["Ended", ti && ti.end != null ? clock(ti.end) : "unknown"],
            ].map(([k, v]) =>
              h("div", null, h("dt", null, k), h("dd", null, v)),
            ),
          ),
        );
    }
    if (data && data.binary)
      content = h("p", { class: "none" }, "Binary file: no text preview.");
    else if (lines.length)
      content = h(
        "pre",
        { class: "log" },
        lines.map((l, i) =>
          h(
            "div",
            {
              class:
                /error|Error|ERROR|Traceback|oom_kill|Killed|CANCELLED|DUE TO TIME LIMIT/.test(
                  l,
                )
                  ? "err"
                  : null,
            },
            h("i", { "aria-hidden": "true" }, i + 1),
            clean(l),
          ),
        ),
      );
    else if (data)
      content = h("p", { class: "none" }, "The log file exists and is empty.");
    else if (isPend(j.state) && !j.array)
      content = h(
        "p",
        { class: "none" },
        "The job has not started, so there is no output yet.",
      );
    else if (e.failed)
      content = h(
        "p",
        { class: "none" },
        `The request did not reach rslurm dash (${e.failed}).`,
      );
    else if ((e.state === "ERROR" && e.error === "budget_exhausted"))
      content = h("p", { class: "none" }, "The daemon is at its read budget for this host; asking again in a few seconds.");
    else if (e.state === "UNAVAILABLE" || e.state === "ERROR")
      content = h(
        "div",
        { class: "none" },
        h("p", null, "The log can't be read right now", e.error ? h("span", { class: "muted" }, ` (${e.error})`) : null, "."),
        e.action ? h("p", null, e.action) : null,
      );
    else if (e.giveup)
      content = h(
        "p",
        { class: "none" },
        "No answer from the daemon after 30 seconds. It may be busy, or the host may be unreachable.",
      );
    else if (stale && !e.inflight)
      content = h(
        "p",
        { class: "none" },
        `${j.host} is not answering, so the log can't be read right now.`,
      );
    else
      content = h(
        "p",
        { class: "none" },
        "Asking the daemon for the last 64 KiB; it appears here within a few seconds.",
      );
    const retry =
      e.giveup || e.failed || (e.state === "ERROR" && !(e.state === "ERROR" && e.error === "budget_exhausted"))
        ? h(
            "button",
            {
              class: "btn",
              type: "button",
              "data-key": "retry:" + which,
              on: { click: () => startRead("tail", j, which, task) },
            },
            "Try again",
          )
        : null;
    const pause =
      ACTIVE.has(j.state) && data
        ? h(
            "button",
            {
              class: "btn quiet",
              type: "button",
              "data-key": "pause",
              "aria-pressed": String(S.paused),
              on: {
                click: () => {
                  S.paused = !S.paused;
                  refreshPane();
                },
              },
            },
            S.paused ? "Resume updates" : "Pause updates",
          )
        : null;
    return h(
      "div",
      null,
      head,
      retry || pause
        ? h(
            "div",
            { class: "logctl" },
            pause,
            retry,
            pause
              ? h(
                  "span",
                  { class: "muted" },
                  S.paused
                    ? "Updates paused."
                    : "Updates every 5 seconds while the job runs.",
                )
              : null,
          )
        : null,
      notes,
      content,
      path
        ? h(
            "div",
            { class: "copy" },
            h("code", { id: "getcmd" }, `rslurm -H ${j.host} get ${path} .`),
            h(
              "button",
              {
                class: "btn",
                type: "button",
                "data-key": "copy",
                on: { click: (ev) => copyCmd(ev.currentTarget) },
              },
              "Copy",
            ),
          )
        : null,
    );
  }
  function filesPane(j) {
    if (!j.workdir)
      return h(
        "p",
        { class: "none" },
        "No work directory is known for this job.",
      );
    const e = readEntry("files", j) || {};
    const data = e.result || e.cached || null;
    const head = h(
      "div",
      { class: "logbar" },
      h("span", { class: "mono path" }, clean(e.path || j.workdir)),
      h(
        "span",
        { class: "muted num" },
        data
          ? `${plural((data.entries || []).length, "entry")} · ${e.result ? "read " + agoTxt(rel() - R(data.observedAt)) : "as of " + clock(R(data.observedAt))}${e.inflight ? " · refreshing…" : ""}`
          : e.inflight
            ? "asking the daemon…"
            : "not read yet",
      ),
    );
    let content;
    if (data) {
      const entries = (data.entries || [])
        .slice()
        .sort((a, b) => String(a.name).localeCompare(String(b.name)));
      content = entries.length
        ? h(
            "ul",
            { class: "files" },
            entries.map((x) =>
              h(
                "li",
                null,
                h(
                  "span",
                  { class: x.type === "dir" ? "dir" : null },
                  clean(x.name) + (x.type === "dir" ? "/" : ""),
                ),
                h(
                  "span",
                  { class: "muted" },
                  x.type === "dir" ? "" : bytes(x.size),
                ),
                h(
                  "span",
                  { class: "muted" },
                  x.mtime != null ? agoTxt(rel() - R(x.mtime)) : "",
                ),
              ),
            ),
          )
        : h("p", { class: "none" }, "The directory is empty.");
      if (data.partial)
        content = [
          h(
            "p",
            { class: "note-gap" },
            "Partial listing: the directory has more entries than one read returns.",
          ),
          content,
        ];
    } else if (e.failed)
      content = h(
        "p",
        { class: "none" },
        `The request did not reach rslurm dash (${e.failed}).`,
      );
    else if ((e.state === "ERROR" && e.error === "budget_exhausted"))
      content = h("p", { class: "none" }, "The daemon is at its read budget for this host; asking again in a few seconds.");
    else if (e.state === "UNAVAILABLE" || e.state === "ERROR")
      content = h(
        "div",
        { class: "none" },
        h("p", null, "The directory can't be listed right now", e.error ? h("span", { class: "muted" }, ` (${e.error})`) : null, "."),
        e.action ? h("p", null, e.action) : null,
      );
    else if (e.giveup)
      content = h(
        "p",
        { class: "none" },
        "No answer from the daemon after 30 seconds.",
      );
    else
      content = h(
        "p",
        { class: "none" },
        "Listing the work directory through the daemon's read queue…",
      );
    return h("div", null, head, content);
  }
  function copyCmd(btn) {
    const txt = document.getElementById("getcmd").textContent;
    const done = () => {
      btn.textContent = "Copied";
      setTimeout(() => (btn.textContent = "Copy"), 1500);
    };
    try {
      navigator.clipboard.writeText(txt).then(done, selectCmd);
    } catch (e) {
      selectCmd();
    }
  }
  function selectCmd() {
    const r = document.createRange();
    r.selectNodeContents(document.getElementById("getcmd"));
    const sel = getSelection();
    sel.removeAllRanges();
    sel.addRange(r);
  }
  function pickTask(id) {
    S.task = id;
    S.dtab = "log";
    renderDrawer();
    document.querySelector('[data-key="dtab:log"]')?.focus();
  }

  function tasksPane(j) {
    const ft = failedTasks(j),
      c = counts(j);
    const cell = (id, i) => {
      const t = j.array.states[i];
      return FAILED.has(t)
        ? h("button", {
            class: "cell t-FAILED" + (S.task === id ? " on" : ""),
            type: "button",
            "data-key": "task:" + id,
            "aria-label": `Task ${id}: ${label(t).toLowerCase()}, exit ${j.array.exits[id] || "unknown"}. Show its log.`,
            on: { click: () => pickTask(id) },
          })
        : h("span", {
            class: "cell t-" + bucket(t),
            title: `task ${id}: ${label(t)}`,
            "aria-hidden": "true",
          });
    };
    const waiting = c.PENDING
      ? `, ${c.PENDING} waiting (${reasonText(j.array.pendingReason).toLowerCase()}${j.array.limit ? `, ${j.array.limit} at a time` : ""})`
      : "";
    return h(
      "div",
      null,
      h(
        "div",
        { class: "tasksum" },
        h(
          "p",
          null,
          h("b", null, `${c.COMPLETED} done`),
          `, ${c.RUNNING} running${waiting}`,
          c.FAILED ? h("b", { class: "t-fail" }, `, ${c.FAILED} failed`) : null,
          c.OTHER ? `, ${c.OTHER} cancelled or other` : null,
          c.UNLISTED ? `, ${c.UNLISTED} not listed` : null,
          ".",
        ),
        ft.length
          ? h(
              "ul",
              { class: "failed" },
              ft.slice(0, 50).map((id) => {
                const ti = j.array.taskInfo?.[id];
                return h(
                  "li",
                  null,
                  h(
                    "button",
                    {
                      class: "link",
                      type: "button",
                      "data-key": "ft:" + id,
                      on: { click: () => pickTask(id) },
                    },
                    `Task ${id}`,
                  ),
                  h(
                    "span",
                    { class: "mono" },
                    ` exit ${j.array.exits[id] || "unknown"}`,
                  ),
                  h(
                    "span",
                    { class: "muted" },
                    ti
                      ? ` · ${dur(ti.elapsed)} · MaxRSS ${ti.maxrss || "unknown"} · ${ti.node || "node unknown"}`
                      : " · details not read yet",
                  ),
                );
              }),
            )
          : null,
      ),
      h(
        "div",
        {
          class: "tape",
          role: "group",
          "aria-label": `Task grid: ${c.COMPLETED} done, ${c.RUNNING} running, ${c.PENDING} waiting, ${c.FAILED} failed. Failed tasks are buttons.`,
        },
        j.array.ids.map(cell),
      ),
      h(
        "div",
        { class: "legend", "aria-hidden": "true" },
        ["COMPLETED", "RUNNING", "PENDING", "FAILED"].map((k) =>
          h(
            "span",
            null,
            h("i", { class: "cell t-" + k }),
            `${label(k)} ${c[k]}`,
          ),
        ),
      ),
    );
  }
  function paneBody(j) {
    if (S.dtab === "tasks" && j.array) return tasksPane(j);
    if (S.dtab === "log") return logPane(j, "out");
    if (S.dtab === "err") return logPane(j, "err");
    return filesPane(j);
  }
  function facts(j) {
    const ho = host(j.host),
      stale = !fresh(ho);
    const mono = (v) => h("span", { class: "mono" }, clean(v));
    const where = `${j.host}${j.partition ? " · " + j.partition : ""}${j.account ? " · " + j.account : ""}`;
    const readFrom = `${j.source || "unknown"}${j.observedAt != null ? ", " + clock(j.observedAt) : ""}`;
    if (j.array)
      return [
        [
          "Array",
          `${plural(j.array.n, "task")}${j.array.limit ? ` · ${j.array.limit} at a time` : ""}`,
        ],
        [
          "Per task",
          [
            j.nodes ? plural(j.nodes, "node") : null,
            j.cpus ? `${j.cpus} cores` : null,
            j.mem,
          ]
            .filter(Boolean)
            .join(" · ") || "unknown",
        ],
        ["Per-task limit", walltime(j.timelimit)],
        j.start != null
          ? [
              "Array running for",
              `${dur(elapsed(j))} · since ${clock(j.start)}`,
            ]
          : ["Submitted", j.submit != null ? clock(j.submit) : "unknown"],
        ["Host", where],
        ["Job ID", mono(j.id)],
        ["Read from", readFrom],
        ["Work directory", j.workdir ? mono(j.workdir) : "unknown"],
      ];
    const exitV = vanished(j)
      ? "unknown: sacct has not reported it yet"
      : !isDone(j.state)
      ? "—"
      : j.exit
        ? h(
            "span",
            null,
            mono(j.exit),
            j.exitSource
              ? h("span", { class: "muted" }, ` (${j.exitSource})`)
              : null,
          )
        : exitUnknown(j);
    return [
      ["Job ID", mono(j.id)],
      ["Host", where],
      [
        "Allocation",
        j.nodes || j.nodelist
          ? h(
              "span",
              null,
              j.nodes ? plural(j.nodes, "node") : "",
              j.nodelist
                ? h("span", { class: "mono" }, " · " + clean(j.nodelist))
                : null,
            )
          : "not allocated yet",
      ],
      [
        "Cores · memory",
        `${j.cpus ?? "?"} · ${j.mem || "?"}${peak(j) ? " (peak " + peak(j) + ")" : acctGap(j) ? ` (peak unknown: sacct silent${ho.accountingSince != null ? " since " + clock(ho.accountingSince) : ""})` : ""}`,
      ],
      [
        isPend(j.state) ? "Queued" : "Elapsed",
        isPend(j.state)
          ? `${j.submit != null ? dur((stale ? (obsAt(ho) ?? rel()) : rel()) - j.submit) : "unknown"} · limit ${walltime(j.timelimit)}`
          : `${dur(elapsed(j))}${j.timelimit ? ` of ${walltime(j.timelimit)}` : ""}${stale && isRun(j.state) ? " (as of " + clock(obsAt(ho)) + ")" : ""}`,
      ],
      ["Exit", exitV],
      ["Read from", readFrom],
      ["Work directory", j.workdir ? mono(j.workdir) : "unknown"],
      j.tracked && j.workspace ? ["Workspace", mono(j.workspace)] : null,
    ].filter(Boolean);
  }
  function renderDrawer() {
    const root = document.getElementById("drawer-root");
    if (!S.open) {
      root.replaceChildren();
      if (!S.help) setInert(false);
      return;
    }
    const j = job(S.open);
    if (!j) {
      S.open = null;
      root.replaceChildren();
      if (!S.help) setInert(false);
      return;
    }
    setInert(true);
    const ho = host(j.host),
      stale = !fresh(ho),
      gone = !F.jobs.some((x) => x.key === j.key);
    const tabs = [
      j.array ? ["tasks", "Tasks"] : null,
      ["log", j.array ? `Task ${currentTask(j)} log` : "Output log"],
      ["err", "Errors (stderr)"],
      ["files", "Files"],
    ].filter(Boolean);
    if (!tabs.some((t) => t[0] === S.dtab)) S.dtab = tabs[0][0];
    const dtabKeys = (e) => {
      const ids = tabs.map((t) => t[0]),
        i = ids.indexOf(S.dtab);
      const n =
        e.key === "ArrowRight"
          ? i + 1
          : e.key === "ArrowLeft"
            ? i - 1
            : e.key === "Home"
              ? 0
              : e.key === "End"
                ? ids.length - 1
                : null;
      if (n == null) return;
      e.preventDefault();
      S.dtab = ids[(n + ids.length) % ids.length];
      renderDrawer();
      document.querySelector(`[data-key="dtab:${S.dtab}"]`).focus();
    };
    const drawer = h(
      "div",
      {
        class: "drawer",
        role: "dialog",
        "aria-modal": "true",
        "aria-labelledby": "dtitle",
        on: { keydown: (e) => trapTab(e, e.currentTarget, closeDrawer) },
      },
      h(
        "header",
        null,
        h(
          "div",
          { class: "titlerow" },
          chip(vanished(j) ? "VANISHED" : j.state, stale && ACTIVE.has(j.state)),
          h("h2", { id: "dtitle" }, clean(j.name || j.id)),
          h(
            "button",
            {
              class: "btn close",
              type: "button",
              "data-key": "close",
              "aria-label": "Close details",
              on: { click: closeDrawer },
            },
            "Close",
            h("kbd", { "aria-hidden": "true" }, "Esc"),
          ),
        ),
        h(
          "div",
          { class: "sub" },
          j.tracked
            ? "tracked by remoteslurm"
            : h(
                "span",
                null,
                h("span", { class: "tag dashed" }, "untracked"),
                " submitted outside remoteslurm",
              ),
          stale && obsAt(ho) != null
            ? h(
                "span",
                { class: "asof" },
                ` · ${j.host} stale, as of ${clock(obsAt(ho), true)}`,
              )
            : null,
          gone
            ? h("span", { class: "asof" }, " · no longer in the current list")
            : null,
        ),
      ),
      j.hint
        ? h(
            "div",
            { class: "hint" },
            h("b", null, label(j.state) + "."),
            " ",
            clean(j.hint),
          )
        : null,
      h(
        "dl",
        { class: "facts" },
        facts(j).map(([k, v]) =>
          h("div", null, h("dt", null, k), h("dd", null, v)),
        ),
      ),
      h(
        "div",
        { class: "dtabs", role: "tablist", "aria-label": "Job detail views" },
        tabs.map(([id, lbl]) =>
          h(
            "button",
            {
              role: "tab",
              type: "button",
              "aria-selected": String(S.dtab === id),
              tabindex: S.dtab === id ? "0" : "-1",
              "data-key": "dtab:" + id,
              "aria-controls": "dpanel",
              on: {
                click: () => {
                  S.dtab = id;
                  keep(renderDrawer);
                },
                keydown: dtabKeys,
              },
            },
            lbl,
          ),
        ),
      ),
      h(
        "div",
        {
          class: "dbody",
          id: "dpanel",
          role: "tabpanel",
          tabindex: "0",
          "data-key": "dpanel",
          "aria-label": tabs.find((t) => t[0] === S.dtab)[1],
        },
        paneBody(j),
      ),
    );
    root.replaceChildren(
      h("div", { class: "scrim", on: { click: closeDrawer } }),
      drawer,
    );
    wantRead(j);
  }

  // ---------- alerts ----------
  function announce(msg) {
    const el = document.getElementById("announcer");
    el.textContent = "";
    setTimeout(() => (el.textContent = msg), 50);
  }
  const unseen = () =>
    F.activity
      .filter((a) => a.alert && a.seq > S.seenSeq)
      .sort((a, b) => b.seq - a.seq);
  function setFavicon(n) {
    const badge = n
      ? `<circle cx="24" cy="8" r="8" fill="#c4291f"/><text x="24" y="12" font-size="11" font-family="Arial,sans-serif" font-weight="700" text-anchor="middle" fill="#fff">${n > 9 ? "9+" : n}</text>`
      : "";
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#2c4fcf"/><rect x="6" y="8" width="20" height="4" rx="2" fill="#fff"/><rect x="6" y="14" width="14" height="4" rx="2" fill="#fff"/><rect x="6" y="20" width="8" height="4" rx="2" fill="#fff"/>${badge}</svg>`;
    const link = document.getElementById("favicon");
    const href = "data:image/svg+xml," + encodeURIComponent(svg);
    if (link.getAttribute("href") !== href) link.setAttribute("href", href);
  }
  // The strip always occupies one line, so new events never push the page down.
  function renderAlerts() {
    const box = document.getElementById("alerts");
    const list = unseen();
    document.title =
      (list.length ? `(${list.length}) ` : "") + "remoteslurm dash";
    setFavicon(list.length);
    const watching = F.hosts.map((x) => x.id).join(" and ");
    if (!list.length) {
      S.showAll = false;
      box.className = "alerts quiet";
      box.replaceChildren(
        h(
          "div",
          { class: "wrap alerts-in" },
          h(
            "span",
            { class: "muted" },
            watching
              ? `Watching ${watching}. New failures and completions appear here.`
              : "New failures and completions appear here.",
          ),
        ),
      );
      return;
    }
    const a = list[0],
      more = list.length - 1;
    box.className = "alerts" + (list.some((x) => x.kind === "fail") ? " has-fail" : "");
    const line = (x) => {
      const j = x.job ? job(x.host + ":" + x.job) : null;
      return h(
        "span",
        { class: "al-item" },
        h("span", {
          class: "sym s-" + (x.kind || "info"),
          "aria-hidden": "true",
        }),
        j
          ? h(
              "button",
              {
                class: "link",
                type: "button",
                "data-key": "alert:" + x.seq,
                "data-name": clean(x.text),
                on: { click: () => openDrawer(j.key) },
              },
              clean(x.text),
            )
          : h("span", { class: "al-t" }, clean(x.text)),
        h(
          "span",
          { class: "muted al-d" },
          clean(
            ` ${x.host}${x.detail ? " · " + x.detail : ""} · ${clock(x.t)}`,
          ),
        ),
      );
    };
    const markSeen = () => {
      S.seenSeq = Math.max(S.seenSeq, ...F.activity.map((x) => x.seq));
      saveSeen(S.seenSeq);
      S.showAll = false;
      renderAlerts();
      document.getElementById("jobs-h").focus();
      announce("Marked seen");
    };
    const seenBtn = (label2) =>
      h(
        "button",
        {
          class: "btn",
          type: "button",
          "data-key": "alerts:seen",
          on: { click: markSeen },
        },
        label2,
      );
    const expanded = S.showAll && more;
    box.replaceChildren(
      ...[
        h(
          "div",
          { class: "wrap alerts-in" },
          h("span", { class: "alerts-h" }, `${list.length} new`),
          line(a),
          more
            ? h(
                "button",
                {
                  class: "link more",
                  type: "button",
                  "data-key": "alerts:more",
                  "aria-expanded": String(!!S.showAll),
                  "aria-controls": "alerts-more",
                  on: {
                    click: () => {
                      S.showAll = !S.showAll;
                      keep(renderAlerts);
                    },
                  },
                },
                S.showAll ? "Hide" : `+${more} more`,
              )
            : null,
          // While expanded, Mark seen follows the list so Tab reaches every alert first.
          expanded ? null : seenBtn("Mark seen"),
        ),
        expanded
          ? h(
              "ul",
              { class: "alerts-more", id: "alerts-more" },
              list.slice(1, 50).map((x) => h("li", null, line(x))),
              h("li", { class: "al-seen" }, seenBtn("Mark all seen")),
            )
          : null,
      ].filter(Boolean),
    );
  }
  function setupNotify() {
    const btn = document.getElementById("notify");
    const lbl = () => {
      const p =
        "Notification" in window ? Notification.permission : "unsupported";
      btn.hidden = p !== "default" && p !== "granted";
      btn.textContent =
        p === "granted" ? "Desktop alerts on" : "Turn on desktop alerts";
      btn.disabled = p === "granted";
    };
    lbl();
    btn.addEventListener("click", () => {
      try {
        Notification.requestPermission().then(lbl, lbl);
      } catch (e) {
        lbl();
      }
    });
  }

  // ---------- help / shortcuts ----------
  function renderHelp() {
    const root = document.getElementById("help-root");
    if (!S.help) {
      root.replaceChildren();
      if (!S.open) setInert(false);
      return;
    }
    setInert(true);
    const close = () => {
      S.help = false;
      renderHelp();
      document.getElementById("helpbtn").focus();
    };
    const notif =
      "Notification" in window ? Notification.permission : "unsupported";
    root.replaceChildren(
      h("div", { class: "scrim", on: { click: close } }),
      h(
        "div",
        {
          class: "help",
          role: "dialog",
          "aria-modal": "true",
          "aria-labelledby": "help-h",
          on: { keydown: (e) => trapTab(e, e.currentTarget, close) },
        },
        h("h2", { id: "help-h" }, "Keyboard"),
        h(
          "dl",
          null,
          [
            [
              "Tab",
              "Move between hosts, views, the job list, the timeline and panels",
            ],
            ["↑ ↓ or j k", "Move within the job list or the timeline"],
            ["Enter", "Open the selected job"],
            ["Esc", "Close details or this help"],
            ["← →", "Switch views when a view tab is focused"],
            ["/", "Filter jobs (single-key shortcut)"],
            ["?", "This help (single-key shortcut)"],
          ].map(([k, v]) =>
            h("div", null, h("dt", null, h("kbd", null, k)), h("dd", null, v)),
          ),
        ),
        h(
          "label",
          { class: "toggle" },
          h("input", {
            type: "checkbox",
            id: "keys-toggle",
            checked: S.keys ? true : null,
            on: {
              change: (e) => {
                S.keys = e.target.checked;
                savePrefs();
              },
            },
          }),
          " Single-key shortcuts (/ and ?)",
        ),
        notif === "denied" || notif === "unsupported"
          ? h(
              "p",
              { class: "muted" },
              "Desktop alerts are blocked by this browser, so new events show only in the strip under the top bar.",
            )
          : null,
        F.coverage
          ? h("p", { class: "muted" }, "Coverage: " + F.coverage)
          : null,
        h(
          "p",
          { class: "muted" },
          "This page is read-only: it never submits, cancels or changes jobs.",
        ),
        h(
          "button",
          {
            class: "btn",
            type: "button",
            id: "help-close",
            on: { click: close },
          },
          "Close",
        ),
      ),
    );
    document.getElementById("help-close").focus();
  }

  // ---------- render ----------
  function renderAll() {
    renderMeta();
    renderNote();
    renderHosts();
    renderJobs();
    renderAttention();
    renderActivity();
    renderTimeline();
    renderAlerts();
  }

  // ---------- live connection ----------
  let cursor = null;
  let es = null;
  function reloadActivity() {
    api("/api/activity?limit=200")
      .then((act) => {
        F.activity = [];
        addActivity(act.events, false);
        cursor = act.cursor;
        keep(() => {
          renderActivity();
          renderAlerts();
        });
      })
      .catch(() => {});
  }
  function connect() {
    if (es) es.close();
    es = new EventSource(
      "/api/stream" + (cursor ? "?after=" + encodeURIComponent(cursor) : ""),
    );
    es.addEventListener("open", () => {
      if (S.lost) {
        S.lost = false;
        renderNote();
      }
    });
    es.addEventListener("state", (e) => {
      try {
        applyState(JSON.parse(e.data), false);
      } catch (err) {
        console.warn("bad state event", err);
      }
    });
    es.addEventListener("activity", (e) => {
      try {
        const d = JSON.parse(e.data);
        if (d.cursor) cursor = d.cursor;
        if (d.resync) return reloadActivity();
        addActivity(d.events, true);
      } catch (err) {
        console.warn("bad activity event", err);
      }
    });
    es.addEventListener("ping", (e) => {
      try {
        setSkew(JSON.parse(e.data).now);
      } catch (err) {
        /* ignore */
      }
    });
    es.addEventListener("error", () => {
      es.close();
      es = null;
      if (!S.lost) {
        S.lost = true;
        renderNote();
      }
      // An expired cookie (dash restarted) shows up as 401/403 on a plain request.
      fetch("/api/state", { credentials: "same-origin" })
        .then((r) => {
          if (r.status === 401 || r.status === 403) {
            S.authLost = true;
            renderNote();
          } else setTimeout(connect, 1000);
        })
        .catch(() => setTimeout(connect, 3000));
    });
  }

  function boot() {
    const q = document.getElementById("q");
    let annT = null;
    q.addEventListener("input", () => {
      S.q = q.value;
      S.cursor = null;
      renderJobs();
      clearTimeout(annT);
      annT = setTimeout(() => {
        const n = document.querySelectorAll("#jobrows tr[data-id]").length;
        announce(n ? `${plural(n, "job")} shown` : "No jobs match");
      }, 600);
    });
    q.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        q.value = "";
        S.q = "";
        renderJobs();
        q.blur();
      }
    });
    document.getElementById("theme").addEventListener("click", () => {
      const r = document.documentElement,
        dark = r.dataset.theme
          ? r.dataset.theme === "dark"
          : matchMedia("(prefers-color-scheme: dark)").matches;
      r.dataset.theme = dark ? "light" : "dark";
    });
    document.getElementById("feed-more").addEventListener("click", () => {
      S.feedAll = !S.feedAll;
      keep(renderActivity);
    });
    document.getElementById("helpbtn").addEventListener("click", () => {
      S.help = true;
      renderHelp();
    });
    setupNotify();
    document.addEventListener("keydown", (e) => {
      if (e.altKey || e.ctrlKey || e.metaKey || !S.keys || S.open || S.help)
        return;
      if (e.target.closest("input, textarea, select, [contenteditable]"))
        return;
      if (e.key === "/") {
        e.preventDefault();
        q.focus();
      } else if (e.key === "?") {
        e.preventDefault();
        S.help = true;
        renderHelp();
      }
    });
    if ("ResizeObserver" in window) {
      let w = 0;
      new ResizeObserver((en) => {
        const nw = Math.round(en[0].contentRect.width);
        if (nw !== w) {
          w = nw;
          if (T0 != null) keep(renderTimeline);
        }
      }).observe(document.getElementById("timeline"));
    }
    Promise.all([api("/api/state"), api("/api/activity?limit=200")])
      .then(([st, act]) => {
        applyState(st, true);
        cursor = act.cursor;
        addActivity(act.events, false);
        const maxSeq = Math.max(0, ...F.activity.map((a) => a.seq));
        const stored = loadSeen();
        // First visit for this journal: start from now rather than replaying history as alerts.
        S.seenSeq = stored == null ? maxSeq : stored;
        if (stored == null) saveSeen(S.seenSeq);
        F.hosts.forEach((ho) => (ho._fresh = fresh(ho)));
        renderAll();
        document.documentElement.classList.add("ready");
        connect();
        setInterval(tickAges, 1000);
        setInterval(() => keep(renderJobs), 5000);
        setInterval(() => {
          // Follow an open, active job's log every 5 s unless paused.
          if (!S.open || S.paused || !(S.dtab === "log" || S.dtab === "err"))
            return;
          const j = job(S.open);
          if (!j || !ACTIVE.has(j.state) || (isPend(j.state) && !j.array))
            return;
          const which = S.dtab === "log" ? "out" : "err";
          if (logPathKnown(j, which))
            startRead("tail", j, which, j.array ? currentTask(j) : null);
        }, 5000);
      })
      .catch((err) => {
        if (T0 == null) T0 = Date.now() / 1000;
        S.bootError = S.authLost
          ? null
          : String(err.message || err) + ". Reload the page to try again.";
        renderNote();
        document.documentElement.classList.add("ready");
      });
  }
  if (document.readyState === "loading")
    document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
