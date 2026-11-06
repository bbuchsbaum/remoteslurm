(function () {
  "use strict";
  const SPEC = window.SPECIMEN || "live";
  const F = JSON.parse(JSON.stringify(window.FIXTURE));
  const NOW0 = Date.parse("2026-11-05T14:02:00-05:00") / 1000;
  const live = SPEC === "live";
  const t0 = Date.now() / 1000;
  const rel = () => (live ? Date.now() / 1000 - t0 : 0); // seconds since NOW0
  const STALE_AFTER = 90;

  const ACTIVE = new Set(["RUNNING", "PENDING"]);
  const FAILED = new Set(["FAILED", "OUT_OF_MEMORY", "TIMEOUT", "NODE_FAIL"]);
  const LABEL = {
    RUNNING: "Running",
    PENDING: "Pending",
    COMPLETED: "Completed",
    FAILED: "Failed",
    OUT_OF_MEMORY: "Out of memory",
    TIMEOUT: "Timed out",
    CANCELLED: "Cancelled",
    NODE_FAIL: "Node failed",
  };
  const CLS = {
    RUNNING: "run",
    PENDING: "pend",
    COMPLETED: "ok",
    FAILED: "fail",
    OUT_OF_MEMORY: "fail",
    TIMEOUT: "fail",
    NODE_FAIL: "fail",
    CANCELLED: "cancel",
  };
  const REASON = {
    Priority: "Waiting behind higher-priority jobs",
    Resources: "Waiting for nodes to free up",
    QOSMaxJobsPerUserLimit: "At your per-user running-job limit",
    JobArrayTaskLimit: "Held by the array's own throttle",
    Dependency: "Waiting for a dependency to finish",
  };

  const S = {
    tab: "active",
    host: null,
    q: "",
    cursor: null,
    open: null,
    dtab: "log",
    task: null,
    unseen: [],
    showAll: false,
    keys: true,
    help: false,
    tl: null,
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
  const clean = (t) =>
    String(t).replace(
      /[‪-‮⁦-⁩‎‏]/g,
      (c) => `⟨U+${c.charCodeAt(0).toString(16).toUpperCase()}⟩`,
    );
  function dur(sec) {
    sec = Math.max(0, Math.round(sec));
    const d = Math.floor(sec / 86400),
      hh = Math.floor((sec % 86400) / 3600),
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
    const d = new Date((NOW0 + r) * 1000);
    return d.toLocaleTimeString("en-CA", {
      hour: "2-digit",
      minute: "2-digit",
      second: sec ? "2-digit" : undefined,
      hour12: false,
      timeZone: "America/Toronto",
    });
  }
  function walltime(sec) {
    const hh = Math.floor(sec / 3600),
      mm = Math.floor((sec % 3600) / 60);
    return `${hh}:${String(mm).padStart(2, "0")}:00`;
  }
  const midTrunc = (t, n) =>
    t.length <= n
      ? t
      : t.slice(0, Math.ceil((n - 1) / 2)) +
        "…" +
        t.slice(t.length - Math.floor((n - 1) / 2));
  const job = (id) => F.jobs.find((j) => j.id === id);
  const host = (id) => F.hosts.find((x) => x.id === id);
  const fresh = (ho) => ho.observedAge <= STALE_AFTER;
  const obsAt = (ho) => rel() - ho.observedAge; // relative time of last scheduler read
  function counts(j) {
    const c = { COMPLETED: 0, RUNNING: 0, PENDING: 0, FAILED: 0 };
    j.array.tasks.forEach((t) => (c[t] += 1));
    return c;
  }
  const failedTasks = (j) =>
    j.array.tasks
      .map((t, i) => (t === "FAILED" ? i : null))
      .filter((x) => x != null);
  const asOf = (j) => (fresh(host(j.host)) ? rel() : obsAt(host(j.host)));
  const elapsed = (j) =>
    j.state === "RUNNING" ? asOf(j) - j.start : (j.end ?? 0) - (j.start ?? 0);
  const needsAttention = (j) =>
    FAILED.has(j.state) || (j.array && failedTasks(j).length > 0);
  const acctGap = (j) => {
    const ho = host(j.host);
    return !ACTIVE.has(j.state) && ho.accounting === "unavailable" && j.end > ho.accountingSince;
  };
  const retained = (j) => {
    const ho = host(j.host);
    return !ACTIVE.has(j.state) && ho.accounting === "unavailable" && !acctGap(j);
  };
  const peak = (j) => (acctGap(j) ? null : j.maxrss);

  // Re-render without losing keyboard focus: interactive nodes carry data-key.
  function keep(fn) {
    const a = document.activeElement,
      key = a && a.dataset ? a.dataset.key : null;
    const group = key ? key.split(":")[0] : null;
    const peers = group
      ? [...document.querySelectorAll(`[data-key^="${group}:"]`)]
      : [];
    const idx = peers.indexOf(a);
    fn();
    if (!key || (a.isConnected && document.activeElement === a)) return;
    let el = document.querySelector(`[data-key="${CSS.escape(key)}"]`);
    let handoff = false;
    if (!el && group === "alert") el = document.querySelector('[data-key="alerts:more"]');
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
    if (el && el !== document.activeElement) {
      el.focus({ preventScroll: true });
      if (handoff || group === "alert") {
        const was = (a.querySelector?.(".jobname") || a).textContent.trim().slice(0, 40);
        const now = (el.querySelector?.(".jobname") || el).textContent.trim().slice(0, 40);
        setTimeout(() => announce(`${was} moved out of this view. Focus is now on ${now}.`), 700);
      }
    }
  }

  // ---------- hosts band ----------
  function hostStats(hid) {
    const st = { run: 0, pend: 0, done: 0, fail: 0, nodes: 0, cores: 0 };
    F.jobs
      .filter((j) => j.host === hid)
      .forEach((j) => {
        if (j.state === "RUNNING") {
          st.run++;
          if (j.array) {
            const c = counts(j);
            st.nodes += c.RUNNING;
            st.cores += c.RUNNING * 192;
          } else {
            st.nodes += j.nodes;
            st.cores += j.cpus;
          }
        } else if (j.state === "PENDING") st.pend++;
        else if (j.end > -86400 && j.state === "COMPLETED") st.done++;
        else if (j.end > -86400 && FAILED.has(j.state)) st.fail++;
      });
    return st;
  }
  function ageText(ho) {
    return fresh(ho)
      ? `live · read ${Math.max(0, Math.round(ho.observedAge))}s ago`
      : `stale · last read ${clock(obsAt(ho), true)} (${dur(ho.observedAge)} ago)`;
  }
  function renderHosts() {
    document.getElementById("hosts").replaceChildren(
      ...F.hosts.map((ho) => {
        const st = hostStats(ho.id),
          ok = fresh(ho),
          any = st.run + st.pend + st.done + st.fail;
        const seg = (n, cls) =>
          n ? h("span", { class: "b-" + cls, style: { flexGrow: n } }) : null;
        const alloc =
          ho.alloc === "node"
            ? `${st.nodes} nodes · ${st.cores.toLocaleString()} cores`
            : `${st.cores} cores`;
        return h(
          "button",
          {
            class: "hostcard" + (ok ? "" : " stale"),
            type: "button",
            "data-key": "host:" + ho.id,
            "aria-pressed": S.host === ho.id ? "true" : "false",
            "aria-label": `${ho.id}: ${ok ? "live" : "stale, last known values"}. ${st.run} running, ${st.pend} pending, ${st.fail} failed and ${st.done} completed in 24 hours. ${S.host === ho.id ? "Showing only this host; select to show all hosts." : "Select to show only this host."}`,
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
            h("span", { class: "hc-name" }, ho.id),
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
              : h("span", { class: "hc-lk" }, `As of ${clock(obsAt(ho))}:`),
            h("span", null, h("b", null, st.run), " running"),
            h("span", null, h("b", null, st.pend), " pending"),
            h(
              "span",
              { class: st.fail ? "is-fail" : null },
              h("b", null, st.fail),
              " failed",
            ),
            h("span", null, h("b", null, st.done), " done"),
            h("span", { class: "hc-alloc" }, alloc + " allocated"),
          ),
          ho.accounting === "unavailable"
            ? h(
                "div",
                { class: "hc-warn" },
                `Accounting (sacct) silent since ${clock(ho.accountingSince)}: later finishes may be missing`,
              )
            : null,
        );
      }),
    );
  }
  function tickAges() {
    F.hosts.forEach((ho) => {
      const wasFresh = fresh(ho);
      ho.observedAge = ho.stuck
        ? ho.observedAge + 1
        : ho.observedAge >= 29
          ? 1
          : ho.observedAge + 1;
      if (wasFresh !== fresh(ho)) {
        keep(renderAll);
        return;
      }
      const el = document.querySelector(`[data-age="${ho.id}"]`);
      if (el) el.textContent = ageText(ho);
    });
  }

  // ---------- jobs ledger ----------
  const TABS = [
    { id: "active", label: "Active", f: (j) => ACTIVE.has(j.state) },
    { id: "attention", label: "Failures", f: needsAttention },
    { id: "finished", label: "Finished", f: (j) => !ACTIVE.has(j.state) },
    { id: "all", label: "All", f: () => true },
  ];
  const visible = () =>
    F.jobs.filter(
      (j) =>
        (!S.host || j.host === S.host) &&
        (!S.q ||
          (j.name + " " + j.id + " " + (j.campaign || ""))
            .toLowerCase()
            .includes(S.q.toLowerCase())),
    );

  function progress(j) {
    if (j.array) {
      const c = counts(j),
        n = j.array.n;
      const seg = (k, cls) =>
        c[k]
          ? h("span", {
              class: "b-" + cls,
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
          `${c.COMPLETED} of ${n} tasks done · ${c.RUNNING} running`,
          c.FAILED
            ? h("b", { class: "t-fail nw" }, ` · ${c.FAILED} failed`)
            : null,
        ),
      );
    }
    if (j.state === "RUNNING") {
      const e = elapsed(j),
        frac = Math.min(1, e / j.timelimit);
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
          `${dur(e)} of ${walltime(j.timelimit)}`,
          frac > 0.85 ? h("b", { class: "t-warn" }, " · near limit") : null,
        ),
      );
    }
    if (j.state === "PENDING")
      return h(
        "div",
        { class: "reason" },
        h("span", null, REASON[j.reason] || j.reason),
        h("small", { class: "mono" }, j.reason),
      );
    if (j.state === "CANCELLED")
      return h(
        "small",
        { class: "muted num" },
        `cancelled after ${dur(elapsed(j))}`,
      );
    return h(
      "small",
      { class: "muted num" },
      `ran ${dur(elapsed(j))} of ${walltime(j.timelimit)}`,
    );
  }
  function when(j) {
    const ho = host(j.host),
      stale = !fresh(ho);
    let txt;
    if (j.state === "PENDING")
      txt =
        j.estStart != null
          ? `Slurm est. start ${clock(j.estStart)}`
          : `queued ${agoTxt(rel() - j.submit)}`;
    else if (j.state === "RUNNING") txt = `since ${clock(j.start)}`;
    else txt = `ended ${agoTxt(rel() - j.end)}`;
    return h(
      "div",
      null,
      h("span", { class: "num" }, txt),
      stale && ACTIVE.has(j.state)
        ? h("div", { class: "asof" }, `as of ${clock(obsAt(ho))} · host stale`)
        : null,
      retained(j)
        ? h(
            "div",
            { class: "asof" },
            `retained · as of ${clock(ho.accountingSince)}`,
          )
        : null,
      acctGap(j)
        ? h("div", { class: "asof" }, `as of ${clock(obsAt(ho))} (squeue) · no accounting`)
        : null,
    );
  }
  function detail(j) {
    if (j.state === "OUT_OF_MEMORY")
      return peak(j)
        ? h("span", null, "MaxRSS ", h("b", null, peak(j)), ` of ${j.mem} requested`)
        : `Over ${j.mem} request · MaxRSS unknown (sacct silent)`;
    if (j.state === "TIMEOUT") return `Hit ${walltime(j.timelimit)} limit`;
    if (j.state === "CANCELLED")
      return `Cancelled by ${j.cancelledBy || "unknown"}`;
    if (j.array) {
      const c = counts(j);
      return `${c.PENDING} waiting on the %${j.array.limit} throttle · ${j.sweepParam}`;
    }
    if (j.state === "PENDING")
      return `${j.nodes} node${j.nodes > 1 ? "s" : ""} · ${walltime(j.timelimit)}`;
    if (j.state === "COMPLETED")
      return h("span", null, "exit ", h("span", { class: "mono" }, j.exit));
    return j.nodelist
      ? h("span", { class: "mono" }, j.nodelist)
      : `${j.nodes} node`;
  }
  function chip(state, stale) {
    return h(
      "span",
      { class: "chip c-" + CLS[state] + (stale ? " is-stale" : "") },
      h("i", { "aria-hidden": "true" }),
      LABEL[state],
    );
  }

  function rowFor(j) {
    const stale = !fresh(host(j.host));
    return h(
      "tr",
      {
        class: "r-" + CLS[j.state] + (stale ? " is-stale" : ""),
        "data-id": j.id,
        "data-host": j.host,
        "data-key": "row:" + j.id,
        tabindex: S.cursor === j.id ? "0" : "-1",
        "aria-current": S.open === j.id ? "true" : null,
        on: {
          click: () => {
            S.cursor = j.id;
            openDrawer(j.id);
          },
          keydown: rowKeys,
          focus: () => {
            S.cursor = j.id;
          },
        },
      },
      h(
        "td",
        { class: "c-state" },
        chip(j.state, stale && ACTIVE.has(j.state)),
      ),
      h(
        "td",
        { class: "c-job" },
        h("span", { class: "jobname" }, clean(j.name)),
        j.array ? h("span", { class: "mono muted" }, ` [${j.array.n}]`) : null,
        h("span", { class: "mono muted jid" }, " " + j.id),
        h("span", { class: "jhost" }, " · " + j.host),
        j.campaign ? h("span", { class: "tag" }, j.campaign) : null,
      ),
      h("td", { class: "c-host" }, j.host),
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
        : j.state === "RUNNING"
          ? 1
          : j.state === "PENDING"
            ? 2
            : 3;
    const rows = vis
      .filter(tab.f)
      .sort(
        (a, b) =>
          rank(a) - rank(b) ||
          (b.end ?? b.start ?? b.submit) - (a.end ?? a.start ?? a.submit),
      );
    const body = document.getElementById("jobrows");
    if (!rows.length) {
      const msg = !F.jobs.length
        ? null
        : S.q
          ? `No jobs match “${S.q}”.`
          : S.tab === "attention"
            ? "No failures in the last 24 hours."
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
    const order = [...tracked, ...untracked].map((j) => j.id);
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
        for (const a of ["class", "tabindex", "aria-current"]) {
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
    return h(
      "div",
      { class: "empty" },
      h("h3", null, "No jobs in the last 24 hours"),
      h(
        "p",
        null,
        "trillium and nibi are connected and being watched. Jobs you submit, with rslurm or with plain sbatch, appear here within about 30 seconds.",
      ),
      h(
        "div",
        { class: "cmds" },
        h("code", null, "rslurm -H trillium submit job.sh"),
        h("code", null, "sbatch job.sh   # also shown, marked untracked"),
      ),
    );
  }

  // ---------- attention + activity ----------
  function attentionItems() {
    const items = [];
    F.hosts
      .filter((x) => !fresh(x))
      .forEach((x) =>
        items.push({
          g: "warn",
          t: `${x.id} is stale`,
          d: `No scheduler read since ${clock(obsAt(x), true)}. Its jobs show their last known state, marked “as of”. Check the connection with rslurm -H ${x.id} doctor.`,
        }),
      );
    F.hosts
      .filter((x) => x.accounting === "unavailable")
      .forEach((x) =>
        items.push({
          g: "warn",
          t: `${x.id}: accounting not answering`,
          d: `sacct has not answered since ${clock(x.accountingSince)}, so jobs on ${x.id} that finished after that may be missing. Finished jobs listed for ${x.id} come from earlier reads.`,
        }),
      );
    F.jobs
      .filter((j) => FAILED.has(j.state) && j.end > -86400)
      .sort((a, b) => b.end - a.end)
      .forEach((j) =>
        items.push({
          g: "fail",
          t: `${j.name}: ${LABEL[j.state].toLowerCase()}`,
          d: j.hint ? j.hint.split(/(?<=\.) /)[0] : `exit ${j.exit}`,
          act: "Open log",
          job: j.id,
          dtab: "log",
          meta: `${j.host} · ended ${clock(j.end)}`,
        }),
      );
    F.jobs
      .filter((j) => j.array && failedTasks(j).length)
      .forEach((j) => {
        const ft = failedTasks(j);
        items.push({
          g: "fail",
          t: `${j.name}: ${ft.length} of ${j.array.n} tasks failed`,
          d: `Tasks ${ft.join(", ")} exited non-zero. The other tasks keep running.`,
          act: "See failed tasks",
          job: j.id,
          dtab: "tasks",
          meta: `${j.host} · array still running`,
        });
      });
    return items;
  }
  function renderAttention() {
    const items = attentionItems();
    const n = document.getElementById("attn-n");
    n.textContent = items.length || "";
    n.hidden = !items.length;
    document
      .getElementById("attention")
      .replaceChildren(
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
                  h("div", { class: "d" }, it.d),
                  it.job
                    ? h(
                        "button",
                        {
                          class: "link",
                          type: "button",
                          "data-key": "att:" + it.job + it.dtab,
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
                    "Every host is live and nothing failed in the last 24 hours.",
                  ),
                ),
              ),
            ]),
      );
  }
  function renderActivity(flash) {
    const list = F.activity
      .filter((a) => !S.host || a.host === S.host)
      .sort((a, b) => b.t - a.t);
    const feed = document.getElementById("feed");
    if (!list.length) {
      feed.replaceChildren(h("li", { class: "none" }, "No activity yet."));
      return;
    }
    const total = list.length;
    const shown = S.feedAll ? list : list.slice(0, 8);
    const toggle = document.getElementById("feed-more");
    toggle.hidden = total <= 8;
    toggle.textContent = S.feedAll ? "Show fewer" : `Show all ${total}`;
    toggle.setAttribute("aria-expanded", String(!!S.feedAll));
    feed.replaceChildren(
      ...shown.map((a, i) =>
        h(
          "li",
          { class: flash && i === 0 ? "new" : null },
          h(
            "time",
            {
              class: "num",
              datetime: new Date((NOW0 + a.t) * 1000).toISOString(),
            },
            clock(a.t),
          ),
          h("span", { class: "sym s-" + a.kind, "aria-hidden": "true" }),
          h(
            "button",
            {
              class: "msg",
              type: "button",
              "data-key": "feed:" + a.job + ":" + Math.round(a.t),
              on: { click: () => openDrawer(a.job) },
            },
            h("span", { class: "t" }, a.text),
            h("span", { class: "d" }, `${a.host} · ${a.detail}`),
          ),
        ),
      ),
    );
  }

  // ---------- timeline ----------
  function renderTimeline() {
    const box = document.getElementById("timeline");
    const W = Math.max(300, box.clientWidth || 900);
    const narrow = W < 640,
      left = narrow ? 112 : 150,
      right = 14,
      top = 24,
      rowH = 24;
    const js = F.jobs
      .filter(
        (j) =>
          (!S.host || j.host === S.host) &&
          (j.end == null || j.end > -86400) &&
          !(j.state === "PENDING" && j.submit < -86400),
      )
      .sort((a, b) => (a.start ?? a.submit) - (b.start ?? b.submit));
    const note = document.getElementById("tl-note");
    if (!js.length) {
      box.replaceChildren(
        h("p", { class: "none" }, "No jobs ran in the last 24 hours."),
      );
      note.textContent = "Last 24 hours";
      return;
    }
    const first = Math.min(
      ...js.map((j) =>
        Math.max(Math.min(j.start ?? j.submit, j.submit), -86400),
      ),
    );
    const x0 = Math.max(
        -86400,
        Math.floor((NOW0 + first) / 3600) * 3600 - NOW0,
      ),
      x1 = rel();
    note.textContent = `Since ${clock(x0)} · dashed = waiting in queue · dotted = unknown since last read · ◆ = failed array task`;
    const span = x1 - x0,
      H = top + js.length * rowH + 6;
    const x = (r) =>
      left + ((Math.min(Math.max(r, x0), x1) - x0) / span) * (W - left - right);
    const step = span / 3600 > 12 ? (narrow ? 6 : 3) : narrow ? 3 : 2;
    const kids = [];
    for (let hr = Math.ceil((NOW0 + x0) / 3600); hr * 3600 - NOW0 <= x1; hr++) {
      const r = hr * 3600 - NOW0;
      if (((hr % 24) + 24) % step) continue;
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
    if (!js.some((j) => j.id === S.tl)) S.tl = js[0].id;
    js.forEach((j, i) => {
      const y = top + i * rowH,
        ho = host(j.host),
        stale = !fresh(ho);
      const g = s("g", {
        class: "tl-row",
        tabindex: S.tl === j.id ? "0" : "-1",
        role: "button",
        "data-key": "tl:" + j.id,
        "data-id": j.id,
        "aria-label": `${j.name} ${j.id}: ${LABEL[j.state]}. Open details.`,
        on: {
          click: () => openDrawer(j.id),
          keydown: tlKeys,
          focus: () => {
            S.tl = j.id;
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
          midTrunc(j.name, narrow ? 14 : 22),
        ),
      );
      if (j.submit != null && (j.start == null || j.start > j.submit)) {
        const qEnd = j.start ?? (stale ? obsAt(ho) : x1);
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
        const end = j.end ?? (stale ? obsAt(ho) : x1);
        g.append(
          s("rect", {
            x: x(j.start),
            y: y + 6,
            width: Math.max(3, x(end) - x(j.start)),
            height: 12,
            rx: 3,
            class: "tl-bar f-" + CLS[j.state],
          }),
        );
        if (j.array && j.array.taskInfo)
          Object.values(j.array.taskInfo).forEach((ti) =>
            g.append(
              s("rect", {
                x: x(ti.at) - 4,
                y: y + 8,
                width: 8,
                height: 8,
                class: "tl-taskfail",
                transform: `rotate(45 ${x(ti.at)} ${y + 12})`,
              }),
            ),
          );
        if (j.state === "RUNNING" && stale) {
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
          `${j.name} ${j.id}: ${LABEL[j.state]}${stale && ACTIVE.has(j.state) ? ", unknown since " + clock(obsAt(ho)) : ""}`,
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
    const rows = [...document.querySelectorAll(".tl-row")],
      i = e.key === "Home" ? 1 : e.key === "End" ? rows.length - 2 : rows.indexOf(e.currentTarget);
    const n =
      rows[
        Math.max(
          0,
          Math.min(rows.length - 1, i + (e.key === "ArrowDown" ? 1 : -1)),
        )
      ];
    rows.forEach((r) => r.setAttribute("tabindex", "-1"));
    n.setAttribute("tabindex", "0");
    n.focus();
    S.tl = n.dataset.id;
  }

  // ---------- modal plumbing (drawer and help share it) ----------
  function setInert(on) {
    const app = document.getElementById("app");
    app.inert = on;
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

  // ---------- drawer ----------
  let lastFocusKey = null;
  function openDrawer(id, dtab) {
    const a = document.activeElement;
    lastFocusKey = a && a.dataset ? a.dataset.key : null;
    const j = job(id);
    S.open = id;
    S.task = j.array ? (failedTasks(j)[0] ?? null) : null;
    S.dtab =
      dtab ||
      (j.array
        ? "tasks"
        : FAILED.has(j.state) && F.logs[id]?.err
          ? "err"
          : "log");
    S.cursor = id;
    renderDrawer();
    keep(renderJobs);
    requestAnimationFrame(() =>
      document.querySelector(".drawer .close")?.focus(),
    );
  }
  function closeDrawer() {
    S.open = null;
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
    const L = (j.array ? null : F.logs[j.id]) || {};
    const ho = host(j.host),
      stale = !fresh(ho);
    let path = which === "out" ? j.stdout : j.stderr;
    if (j.array && which === "out")
      path = (j.stdout || "").replace("%A", j.id).replace("%a", S.task ?? 0);
    if (path) path = j.workdir + "/" + path.replace("%j", j.id);
    if (which === "err" && !j.stderr && j.tracked) {
      return h(
        "p",
        { class: "none" },
        "No separate stderr file: this job was submitted without --error, so stderr is merged into the output log.",
      );
    }
    if (!path) {
      return h(
        "div",
        { class: "none" },
        h(
          "p",
          null,
          "Slurm did not report a log path, and remoteslurm did not submit this job.",
        ),
        h(
          "p",
          null,
          "If the script used Slurm's default, the log would be ",
          h("span", { class: "mono" }, `${j.workdir}/slurm-${j.id}.out`),
          " (a guess, not checked).",
        ),
      );
    }
    const lines = (which === "out" ? L.out : L.err) || [];
    const readNote =
      j.logRead != null && !j.array
        ? `${lines.length ? `last ${lines.length} line${lines.length === 1 ? "" : "s"} · ` : ""}read ${dur(-j.logRead)} ago`
        : "not read yet";
    const head = h(
      "div",
      { class: "logbar" },
      h("span", { class: "mono path" }, path),
      h("span", { class: "muted num" }, readNote),
    );
    let content;
    if (j.array && S.task != null) {
      const ti = j.array.taskInfo?.[S.task];
      content = h(
        "div",
        null,
        ti
          ? h(
              "dl",
              { class: "facts taskfacts" },
              [
                ["Task", String(S.task)],
                ["Exit", h("span", { class: "mono" }, j.array.exits[S.task])],
                ["Elapsed", `${dur(ti.elapsed)} of ${walltime(j.timelimit)}`],
                ["MaxRSS", `${ti.maxrss} of ${j.mem}`],
                ["Node", h("span", { class: "mono" }, ti.node)],
                ["Failed at", clock(ti.at)],
              ].map(([k, v]) =>
                h("div", null, h("dt", null, k), h("dd", null, v)),
              ),
            )
          : null,
        h(
          "p",
          { class: "none" },
          stale
            ? `${j.host} is not answering, so this log can't be read right now.`
            : "Not read yet. Opening a task log asks the daemon for its last 64 KiB; it appears here within a few seconds.",
        ),
      );
    } else if (lines.length)
      content = h(
        "pre",
        { class: "log" },
        lines.map((l, i) =>
          h(
            "div",
            {
              class: /error|Error|Traceback|oom_kill|Killed|CANCELLED/.test(l)
                ? "err"
                : null,
            },
            h("i", { "aria-hidden": "true" }, i + 1),
            clean(l),
          ),
        ),
      );
    else if (j.logEmpty)
      content = h("p", { class: "none" }, "The log file exists and is empty.");
    else if (j.state === "PENDING")
      content = h(
        "p",
        { class: "none" },
        "The job has not started, so there is no output yet.",
      );
    else if (stale)
      content = h(
        "p",
        { class: "none" },
        `${j.host} is not answering, so the log can't be read right now.`,
      );
    else
      content = h(
        "p",
        { class: "none" },
        "Not read yet. Opening this tab asks the daemon for the last 64 KiB; it appears here within a few seconds.",
      );
    return h(
      "div",
      null,
      head,
      content,
      h(
        "div",
        { class: "copy" },
        h("code", { id: "getcmd" }, `rslurm -H ${j.host} get ${path} .`),
        h(
          "button",
          {
            class: "btn",
            type: "button",
            "data-key": "copy",
            on: { click: (e) => copyCmd(e.currentTarget) },
          },
          "Copy",
        ),
      ),
    );
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
  function pickTask(i) {
    S.task = i;
    S.dtab = "log";
    renderDrawer();
    document.querySelector('[data-key="dtab:log"]')?.focus();
  }

  function tasksPane(j) {
    const ft = failedTasks(j),
      c = counts(j);
    const cell = (t, i) =>
      t === "FAILED"
        ? h("button", {
            class: "cell t-FAILED" + (S.task === i ? " on" : ""),
            type: "button",
            "data-key": "task:" + i,
            "aria-label": `Task ${i} failed, exit ${j.array.exits[i] || "unknown"}. Show its log.`,
            on: { click: () => pickTask(i) },
          })
        : h("span", {
            class: "cell t-" + t,
            title: `task ${i}: ${LABEL[t]}`,
            "aria-hidden": "true",
          });
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
          `, ${c.RUNNING} running, ${c.PENDING} waiting (${REASON[j.array.pendingReason].toLowerCase()}, ${j.array.limit} at a time)`,
          c.FAILED ? h("b", { class: "t-fail" }, `, ${c.FAILED} failed`) : null,
          ".",
        ),
        ft.length
          ? h(
              "ul",
              { class: "failed" },
              ft.map((i) => {
                const ti = j.array.taskInfo?.[i];
                return h(
                  "li",
                  null,
                  h(
                    "button",
                    {
                      class: "link",
                      type: "button",
                      "data-key": "ft:" + i,
                      on: { click: () => pickTask(i) },
                    },
                    `Task ${i}`,
                  ),
                  h(
                    "span",
                    { class: "mono" },
                    ` exit ${j.array.exits[i] || "unknown"}`,
                  ),
                  h(
                    "span",
                    { class: "muted" },
                    ti
                      ? ` · ${dur(ti.elapsed)} · MaxRSS ${ti.maxrss} · ${ti.node}`
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
        j.array.tasks.map(cell),
      ),
      h(
        "div",
        { class: "legend", "aria-hidden": "true" },
        ["COMPLETED", "RUNNING", "PENDING", "FAILED"].map((k) =>
          h(
            "span",
            null,
            h("i", { class: "cell t-" + k }),
            `${LABEL[k]} ${c[k]}`,
          ),
        ),
      ),
    );
  }
  function renderDrawer() {
    const root = document.getElementById("drawer-root");
    if (!S.open) {
      root.replaceChildren();
      if (!S.help) setInert(false);
      return;
    }
    setInert(true);
    const j = job(S.open),
      ho = host(j.host),
      stale = !fresh(ho);
    const tracked = (v, unk) =>
      j.tracked
        ? v
          ? v.replace("_", " ").toLowerCase()
          : unk
        : "not checked (untracked)";
    const facts = j.array
      ? [
          ["Array", `${j.array.n} tasks · ${j.array.limit} at a time`],
          ["Per task", `${j.perTask} · ${j.mem}`],
          ["Per-task limit", walltime(j.timelimit)],
          ["Array running for", `${dur(elapsed(j))} · since ${clock(j.start)}`],
          ["Host", `${j.host} · ${j.partition}`],
          ["Job ID", h("span", { class: "mono" }, j.id)],
          ["Outputs", tracked(j.artifacts, "unknown")],
          ["Validation", tracked(j.validation, "unknown")],
          ["Work directory", h("span", { class: "mono" }, j.workdir)],
        ]
      : [
          ["Job ID", h("span", { class: "mono" }, j.id)],
          ["Host", `${j.host} · ${j.partition}`],
          [
            "Allocation",
            `${j.nodes} node${j.nodes > 1 ? "s" : ""}${j.nodelist ? " · " + j.nodelist : ""}`,
          ],
          [
            "Cores · memory",
            `${j.cpus} · ${j.mem}${peak(j) ? " (peak " + peak(j) + ")" : acctGap(j) ? " (peak unknown: sacct silent since " + clock(host(j.host).accountingSince) + ")" : ""}`,
          ],
          [
            j.state === "PENDING" ? "Queued" : "Elapsed",
            j.state === "PENDING"
              ? `${dur((stale ? obsAt(ho) : rel()) - j.submit)} · limit ${walltime(j.timelimit)}`
              : `${dur(elapsed(j))} of ${walltime(j.timelimit)}${stale && j.state === "RUNNING" ? " (as of " + clock(obsAt(ho)) + ")" : ""}`,
          ],
          [
            "Exit",
            j.state === "CANCELLED"
              ? h(
                  "span",
                  null,
                  h("span", { class: "mono" }, j.exit),
                  " · by " + (j.cancelledBy || "unknown"),
                )
              : j.exit
                ? h("span", { class: "mono" }, j.exit)
                : "—",
          ],
          ["Outputs", tracked(j.artifacts, "unknown")],
          ["Validation", tracked(j.validation, "unknown")],
          ["Work directory", h("span", { class: "mono" }, j.workdir)],
        ];
    const tabs = [
      j.array ? ["tasks", "Tasks"] : null,
      ["log", j.array ? `Task ${S.task ?? 0} log` : "Output log"],
      ["err", "Errors (stderr)"],
      ["files", "Files"],
    ].filter(Boolean);
    let body;
    if (S.dtab === "tasks" && j.array) body = tasksPane(j);
    else if (S.dtab === "log") body = logPane(j, "out");
    else if (S.dtab === "err") body = logPane(j, "err");
    else
      body = h(
        "p",
        { class: "none" },
        `Opening Files lists ${j.workdir} through the daemon's read queue (at most 4,096 entries).`,
      );
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
          chip(j.state, stale && ACTIVE.has(j.state)),
          h("h2", { id: "dtitle" }, clean(j.name)),
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
          j.campaign ? h("span", { class: "tag" }, j.campaign) : null,
          j.tracked
            ? "tracked by remoteslurm"
            : h(
                "span",
                null,
                h("span", { class: "tag dashed" }, "untracked"),
                " submitted outside remoteslurm",
              ),
          stale
            ? h(
                "span",
                { class: "asof" },
                ` · ${j.host} stale, as of ${clock(obsAt(ho), true)}`,
              )
            : null,
        ),
      ),
      j.hint
        ? h(
            "div",
            { class: "hint" },
            h("b", null, LABEL[j.state] + "."),
            " ",
            j.hint,
          )
        : null,
      h(
        "dl",
        { class: "facts" },
        facts.map(([k, v]) =>
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
        body,
      ),
    );
    root.replaceChildren(
      h("div", { class: "scrim", on: { click: closeDrawer } }),
      drawer,
    );
  }

  // ---------- alerts ----------
  function announce(msg) {
    const el = document.getElementById("announcer");
    el.textContent = "";
    setTimeout(() => (el.textContent = msg), 50);
  }
  function alert(kind, title, detail, id) {
    S.unseen.unshift({
      kind,
      title,
      detail,
      id,
      t: rel(),
      key: id + ":" + Math.round(rel()),
    });
    announce(
      `${kind === "fail" ? "Failure" : "Finished"}: ${title}. ${detail}`,
    );
    if (
      live &&
      "Notification" in window &&
      Notification.permission === "granted"
    ) {
      try {
        new Notification(title, { body: detail });
      } catch (e) {
        /* blocked */
      }
    }
    keep(renderAlerts);
  }
  // The strip always occupies one line, so new events never push the page down.
  function renderAlerts() {
    const box = document.getElementById("alerts");
    document.title =
      (S.unseen.length ? `(${S.unseen.length}) ` : "") + "remoteslurm dash";
    const watching = F.hosts.map((x) => x.id).join(" and ");
    if (!S.unseen.length) {
      box.className = "alerts quiet";
      box.replaceChildren(
        h(
          "div",
          { class: "wrap alerts-in" },
          h(
            "span",
            { class: "muted" },
            `Watching ${watching}. New failures and completions appear here.`,
          ),
        ),
      );
      return;
    }
    const a = S.unseen[0],
      more = S.unseen.length - 1;
    box.className = "alerts" + (a.kind === "fail" ? " has-fail" : "");
    const line = (x) =>
      h(
        "span",
        { class: "al-item" },
        h("span", { class: "sym s-" + x.kind, "aria-hidden": "true" }),
        h(
          "button",
          {
            class: "link",
            type: "button",
            "data-key": "alert:" + x.key,
            on: { click: () => openDrawer(x.id) },
          },
          x.title,
        ),
        h("span", { class: "muted al-d" }, ` ${x.detail} · ${clock(x.t)}`),
      );
    box.replaceChildren(...[
      h(
        "div",
        { class: "wrap alerts-in" },
        h("span", { class: "alerts-h" }, `${S.unseen.length} new`),
        line(a),
        more
          ? h(
              "button",
              {
                class: "link more",
                type: "button",
                "data-key": "alerts:more",
                "aria-expanded": String(S.showAll),
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
        h(
          "button",
          {
            class: "btn",
            type: "button",
            "data-key": "alerts:seen",
            on: {
              click: () => {
                S.unseen = [];
                S.showAll = false;
                renderAlerts();
                document.getElementById("jobs-h").focus();
                announce("Marked seen");
              },
            },
          },
          "Mark seen",
        ),
      ),
      S.showAll && more
        ? h(
            "ul",
            { class: "alerts-more" },
            S.unseen.slice(1).map((x) => h("li", null, line(x))),
          )
        : null].filter(Boolean),
    );
  }
  function setupNotify() {
    const btn = document.getElementById("notify");
    const label = () => {
      const p =
        "Notification" in window ? Notification.permission : "unsupported";
      btn.hidden = p !== "default" && p !== "granted";
      btn.textContent =
        p === "granted" ? "Desktop alerts on" : "Turn on desktop alerts";
      btn.disabled = p === "granted";
    };
    label();
    btn.addEventListener("click", () => {
      try {
        Notification.requestPermission().then(label, label);
      } catch (e) {
        label();
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
    renderHosts();
    renderJobs();
    renderAttention();
    renderActivity();
    renderTimeline();
    renderAlerts();
  }

  function boot() {
    if (SPEC === "alert") {
      Object.assign(F.hosts[1], {
        observedAge: 214,
        stuck: true,
        accounting: "unavailable",
        accountingSince: -400,
      });
      const j = job("4412702");
      j.array.tasks[150] = "FAILED";
      j.array.exits[150] = "1:0";
      j.array.taskInfo[150] = {
        elapsed: 2 * 3600 + 40,
        maxrss: "205G",
        node: "tri-c0702",
        at: -5,
      };
      F.activity.forEach((a) => {
        if (a.job === "8830187") a.detail = "seen by squeue · MaxRSS unknown (sacct silent)";
      });
      F.activity.push({
        t: -5,
        kind: "fail",
        host: "trillium",
        text: "fit_sweep task 150 failed",
        job: "4412702",
        detail: "exit 1:0",
      });
    }
    if (SPEC === "empty") {
      F.jobs = [];
      F.activity = [];
    }
    const q = document.getElementById("q");
    let annT = null;
    q.addEventListener("input", () => {
      S.q = q.value;
      S.cursor = null;
      renderJobs();
      clearTimeout(annT);
      annT = setTimeout(() => {
        const n = document.querySelectorAll("#jobrows tr[data-id]").length;
        announce(n ? `${n} job${n > 1 ? "s" : ""} shown` : "No jobs match");
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
          keep(renderTimeline);
        }
      }).observe(document.getElementById("timeline"));
    }
    renderAll();
    document.documentElement.classList.add("ready");
    if (SPEC === "drawer") openDrawer("8830187");
    if (SPEC === "alert") {
      S.unseen = [
        {
          kind: "fail",
          title: "fit_sweep task 150 failed",
          detail: "trillium · exit 1:0 · 3 of 200 tasks have failed",
          id: "4412702",
          t: -5,
          key: "a1",
        },
        {
          kind: "ok",
          title: "qc_report completed",
          detail: "trillium · 14m 22s · outputs present",
          id: "4412812",
          t: -72,
          key: "a2",
        },
      ];
      renderAlerts();
    }
    if (live) {
      setInterval(tickAges, 1000);
      setTimeout(() => {
        const j = job("4412702");
        j.array.tasks[150] = "FAILED";
        j.array.exits[150] = "1:0";
        j.array.taskInfo[150] = {
          elapsed: 2 * 3600 + 40,
          maxrss: "205G",
          node: "tri-c0702",
          at: rel(),
        };
        F.activity.push({
          t: rel(),
          kind: "fail",
          host: "trillium",
          text: "fit_sweep task 150 failed",
          job: "4412702",
          detail: "exit 1:0",
        });
        keep(() => {
          renderHosts();
          renderJobs();
          renderAttention();
          renderActivity(true);
          renderTimeline();
        });
        alert(
          "fail",
          "fit_sweep task 150 failed",
          "trillium · exit 1:0 · 3 of 200 tasks have failed",
          "4412702",
        );
      }, 7000);
      setTimeout(() => {
        const j = job("4412766");
        j.state = "COMPLETED";
        j.end = rel();
        j.exit = "0:0";
        j.artifacts = "SETTLING";
        F.activity.push({
          t: j.end,
          kind: "ok",
          host: "trillium",
          text: "fmriprep_sub-14 completed",
          job: j.id,
          detail: `exit 0:0 · ${dur(j.end - j.start)}`,
        });
        keep(() => {
          renderAll();
          renderActivity(true);
          if (S.open === j.id) renderDrawer();
        });
        alert(
          "ok",
          "fmriprep_sub-14 completed",
          `trillium · ${dur(j.end - j.start)} · outputs not checked yet`,
          j.id,
        );
      }, 16000);
      setInterval(() => keep(renderJobs), 5000);
    }
  }
  // Render once the UI faces are ready (max 1 s). If they are late, commit to the fallback
  // stack for this page view, so a late font can never reflow what the user is reading.
  const faces = ['400 14px "Hanken Grotesk"', '600 14px "Hanken Grotesk"', '700 14px "Hanken Grotesk"', '400 12px "JetBrains Mono"'];
  const sheet = window.__fontSheet || Promise.resolve();
  const ready = sheet.then(() => (document.fonts && document.fonts.load ? Promise.all(faces.map((f) => document.fonts.load(f).catch(() => null))) : null)).then(() => "fonts");
  Promise.race([ready, new Promise((r) => setTimeout(() => r("timeout"), 1000))]).then((why) => {
    const ok = why === "fonts" && faces.every((f) => { try { return document.fonts.check(f); } catch (e) { return false; } });
    if (!ok) document.documentElement.classList.add("nofont");
    boot();
  });
})();
