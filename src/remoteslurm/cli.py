"""``remoteslurm`` / ``rslurm`` command line interface (argparse, ``--json`` everywhere)."""

from __future__ import annotations

import argparse
import base64
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from . import __version__
from .cluster import Cluster
from .config import EXAMPLE_CONFIG, Config, config_path, state_dir
from .errors import InvalidArgument, NotConnected, RemoteSlurmError, SlurmError
from .transport import SSHTransport, ssh_available

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_NOT_CONNECTED = 3


# ----------------------------------------------------------------------------- helpers
def split_target(target: str | None, default_host: str | None) -> tuple[str | None, str]:
    """``host:path`` -> (host, path); bare path -> (default_host, path). ``~`` if empty."""
    if target is None:
        return default_host, "~"
    if ":" in target and not target.startswith(("/", "~", ".", "$")):
        host, _, path = target.partition(":")
        return host or default_host, path or "~"
    return default_host, target


def emit(args: argparse.Namespace, data: Any, human: Callable[[Any], None] | None = None) -> None:
    if args.json or human is None:
        print(json.dumps(data, indent=2, default=str))
    else:
        human(data)


def fmt_size(n: int | None) -> str:
    if n is None:
        return "-"
    f = float(n)
    for unit in ("B", "K", "M", "G", "T"):
        if f < 1024 or unit == "T":
            return f"{f:.0f}{unit}" if unit == "B" else f"{f:.1f}{unit}"
        f /= 1024
    return str(n)


def fmt_time(ts: float | None) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))


def get_cluster(args: argparse.Namespace, host: str | None = None) -> Cluster:
    """Prefer the local session daemon (keeps ssh channels warm); fall back to a direct session."""
    from .daemon import connect_via_daemon

    cfg = Config.load(Path(args.config) if getattr(args, "config", None) else None)
    name = host or args.host
    if not getattr(args, "no_daemon", False):
        c = connect_via_daemon(name, cfg)
        if c is not None:
            return c
    return Cluster.connect(name, config=cfg)


def cmd_daemon(args: argparse.Namespace) -> int:
    from . import daemon

    if args.action == "status":
        if not daemon.daemon_available():
            emit(args, {"running": False}, lambda d: print("daemon not running"))
            return EXIT_ERROR
        st = daemon.daemon_status()
        st["running"] = True
        emit(
            args,
            st,
            lambda d: print(
                f"daemon pid {d['pid']} up {d['uptime']}s, {d['calls']} calls, "
                f"socket {d['socket']}\n"
                + "\n".join(
                    f"  {n}: {'alive' if h['alive'] else 'idle'} (remote pid {h['remote_pid']}, "
                    f"{h['spawns']} spawn(s), {h['transport']})"
                    for n, h in d["hosts"].items()
                )
            ),
        )
        return EXIT_OK
    if args.action == "stop":
        ok = daemon.stop_daemon()
        emit(
            args, {"stopped": ok}, lambda d: print("daemon stopped" if ok else "daemon not running")
        )
        return EXIT_OK if ok else EXIT_ERROR
    if args.action == "start":
        if daemon.daemon_available():
            print("daemon already running")
            return EXIT_OK
        ok = daemon.spawn_daemon()
        print("daemon started" if ok else "failed to start daemon (see daemon.log in state dir)")
        return EXIT_OK if ok else EXIT_ERROR
    if args.action == "run":
        daemon.main()
        return EXIT_OK
    return EXIT_USAGE


# ----------------------------------------------------------------------------- commands
def cmd_connect(args: argparse.Namespace) -> int:
    cfg = Config.load()
    host = cfg.host(args.host_name or args.host)
    if host.ssh == "local":
        print("local host needs no connection")
        return EXIT_OK
    t = SSHTransport(
        alias=host.ssh,
        mfa=host.mfa,
        control_path=host.control_path,
        control_persist=host.control_persist,
        extra_ssh_opts=list(host.ssh_opts),
    )
    if t.master_alive() and not args.force:
        print(f"✓ ssh master for {host.ssh} is already alive")
    else:
        if args.force:
            t.master_exit()
        print(f"Establishing persistent ssh connection to {host.ssh} (complete MFA if prompted)...")
        t.establish_master(interactive=True)
        print(f"✓ ssh master established for {host.ssh} (ControlPersist {host.control_persist})")
    c = Cluster.connect(host.name, config=cfg)
    t0 = time.time()
    c.ping()
    ms = (time.time() - t0) * 1000
    info = c.info()
    print(f"✓ stub running on {info['hostname']} (python {info['python']}, {ms:.0f} ms round-trip)")
    if info.get("slurm_version"):
        print(f"✓ {info['slurm_version']}")
    return EXIT_OK


def cmd_doctor(args: argparse.Namespace) -> int:
    ok_all = True
    results: list[dict[str, Any]] = []

    def check(name: str, ok: bool | None, detail: str = "", fix: str = "") -> None:
        nonlocal ok_all
        if ok is False:
            ok_all = False
        results.append({"check": name, "ok": ok, "detail": detail, "fix": fix})
        if not args.json:
            mark = "✓" if ok else ("✗" if ok is False else "•")
            line = f"{mark} {name}"
            if detail:
                line += f": {detail}"
            print(line)
            if fix and not ok:
                print(f"    -> {fix}")

    cfgp = config_path()
    cfg = Config.load()
    check(
        "config file",
        cfgp.exists() or None,
        str(cfgp),
        "optional; `remoteslurm config --init` writes an example",
    )
    try:
        host = cfg.host(args.host_name or args.host)
    except RemoteSlurmError as e:
        check("host", False, e.message, e.action or "")
        return EXIT_ERROR
    check("host", True, f"{host.name} (ssh alias {host.ssh!r}, mfa={host.mfa})")
    if host.ssh != "local":
        check("ssh binary", ssh_available(), shutil.which("ssh") or "not found")
        try:
            g = subprocess.run(["ssh", "-G", host.ssh], capture_output=True, text=True, timeout=10)
            opts = dict(line.split(" ", 1) for line in g.stdout.splitlines() if " " in line)
        except Exception:
            opts = {}
        cm = opts.get("controlmaster", "false")
        cp = opts.get("controlpath", "none")
        cpers = opts.get("controlpersist", "no")
        good_cm = cm in ("auto", "yes", "autoask") and cp != "none"
        check(
            "ssh ControlMaster",
            good_cm or (host.control_path is not None),
            f"ControlMaster={cm} ControlPath={cp} ControlPersist={cpers}",
            "add to ~/.ssh/config:  ControlMaster auto / "
            "ControlPath ~/.ssh/sockets/%r@%h-%p / ControlPersist 12h",
        )
        t = SSHTransport(alias=host.ssh, mfa=host.mfa, control_path=host.control_path)
        alive = t.master_alive()
        check("ssh master alive", alive, "", f"run: remoteslurm connect {host.name}")
        if not alive:
            emit(args, {"ok": False, "checks": results}, lambda d: None)
            return EXIT_NOT_CONNECTED
    try:
        t0 = time.time()
        c = Cluster.connect(host.name, config=cfg)
        start_ms = (time.time() - t0) * 1000
        check(
            "stub session", True, f"started in {start_ms:.0f} ms, remote pid {c.session.remote_pid}"
        )
        t0 = time.time()
        c.ping()
        check("round-trip", True, f"{(time.time() - t0) * 1000:.0f} ms")
        info = c.info()
        check("remote python", True, f"{info['python']} ({info['stub']})")
        tools = info["slurm_tools"]
        missing = [k for k, v in tools.items() if not v]
        check("slurm tools", not missing, info.get("slurm_version") or "", f"missing: {missing}")
        w = c.write("~/.cache/remoteslurm/.doctor", "ok\n")
        c.rm(w["path"])
        check("home writable", True, info["home"])
        env = info.get("env", {})
        check(
            "env",
            True,
            ", ".join(f"{k}={v}" for k, v in env.items() if k in ("SCRATCH", "PROJECT"))
            or "no SCRATCH/PROJECT",
        )
        if not missing:
            q = c.squeue(refresh=True)
            check("squeue", True, f"{len(q)} job(s) in my queue")
    except RemoteSlurmError as e:
        check("stub session", False, e.message, e.action or "")
    emit(args, {"ok": ok_all, "checks": results}, lambda d: None)
    return EXIT_OK if ok_all else EXIT_ERROR


def cmd_ls(args: argparse.Namespace) -> int:
    host, path = split_target(args.target, args.host)
    c = get_cluster(args, host)
    r = c.ls(path, limit=args.limit, token=args.token, hidden=not args.no_hidden)

    def human(r: dict[str, Any]) -> None:
        for e in r["entries"]:
            t = {"dir": "d", "link": "l", "file": "-", "other": "?"}[e["type"]]
            name = e["name"] + ("/" if e["type"] == "dir" else "")
            if args.long:
                mode = f"{t}{e.get('mode', 0):04o}"
                print(f"{mode}  {fmt_size(e.get('size')):>8}  {fmt_time(e.get('mtime'))}  {name}")
            else:
                print(name)
        if r["truncated"]:
            more = r["total"] - r["offset"] - len(r["entries"])
            print(f"... {more} more (use --token {r['next_token']})", file=sys.stderr)

    emit(args, r, human)
    return EXIT_OK


def cmd_cat(args: argparse.Namespace) -> int:
    host, path = split_target(args.target, args.host)
    c = get_cluster(args, host)
    r = c.read(path, max_bytes=args.max_bytes, offset=args.offset, head=args.head, tail=args.tail)

    def human(r: dict[str, Any]) -> None:
        if r.get("binary"):
            sys.stdout.buffer.write(base64.b64decode(r["content_b64"]))
        else:
            sys.stdout.write(r["content"])
        if r["truncated"]:
            nxt = r["offset"] + r["length"]
            print(
                f"[truncated: showed {r['length']} of {r['size']} bytes; "
                f"use --offset {nxt} or --max-bytes]",
                file=sys.stderr,
            )

    emit(args, r, human)
    return EXIT_OK


def cmd_tail(args: argparse.Namespace) -> int:
    host, path = split_target(args.target, args.host)
    c = get_cluster(args, host)
    r = c.read(path, tail=args.lines, max_bytes=args.max_bytes)
    if args.json and not args.follow:
        print(json.dumps(r, indent=2))
        return EXIT_OK
    sys.stdout.write(r.get("content", ""))
    sys.stdout.flush()
    if not args.follow:
        return EXIT_OK
    offset = r["size"]
    try:
        while True:
            time.sleep(args.interval)
            r = c.read(path, offset=offset, max_bytes=args.max_bytes)
            if r["length"]:
                sys.stdout.write(r.get("content", ""))
                sys.stdout.flush()
                offset += r["length"]
    except KeyboardInterrupt:
        return EXIT_OK


def cmd_grep(args: argparse.Namespace) -> int:
    host, path = split_target(args.target, args.host)
    c = get_cluster(args, host)
    r = c.grep(
        args.pattern,
        path,
        glob=args.glob,
        max_matches=args.max_matches,
        ignore_case=args.ignore_case,
        max_depth=args.max_depth,
        context=args.context,
        hidden=args.hidden,
    )

    def human(r: dict[str, Any]) -> None:
        for m in r["matches"]:
            print(f"{m['file']}:{m['line']}:{m['text']}")
        if r["truncated"]:
            print(f"... truncated at {len(r['matches'])} matches (--max-matches)", file=sys.stderr)

    emit(args, r, human)
    return EXIT_OK


def cmd_find(args: argparse.Namespace) -> int:
    host, path = split_target(args.target, args.host)
    c = get_cluster(args, host)
    r = c.glob(
        path,
        args.pattern,
        limit=args.limit,
        max_depth=args.max_depth,
        type=args.type,
        hidden=args.hidden,
    )

    def human(r: dict[str, Any]) -> None:
        for m in r["matches"]:
            print(m["path"])
        if r["truncated"]:
            print(f"... truncated at {len(r['matches'])} (--limit)", file=sys.stderr)

    emit(args, r, human)
    return EXIT_OK


def cmd_run(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    cmd = " ".join(args.cmd) if len(args.cmd) > 1 or not args.argv else args.cmd[0]
    r = c.run(
        list(args.cmd) if args.argv else cmd,
        cwd=args.cwd,
        timeout=args.timeout,
        login=args.login,
        max_output=args.max_output,
    )
    c.registry.audit("run", cmd=cmd, rc=r["rc"])

    def human(r: dict[str, Any]) -> None:
        sys.stdout.write(r["stdout"])
        if r["stderr"]:
            sys.stderr.write(r["stderr"])
        if r["stdout_truncated"] or r["stderr_truncated"]:
            print("[output truncated; use --max-output]", file=sys.stderr)

    emit(args, r, human)
    return r["rc"] if not args.json else EXIT_OK


def cmd_put(args: argparse.Namespace) -> int:
    host, dest = split_target(args.dest, args.host)
    src = Path(args.src).expanduser()
    if not src.exists():
        raise InvalidArgument(f"local file not found: {src}")
    c = get_cluster(args, host)
    if args.rsync or src.is_dir() or src.stat().st_size > 4 * 1024 * 1024:
        return _rsync(c, str(src), dest, to_remote=True, args=args)
    if dest.endswith("/"):
        dest = dest + src.name
    r = c.write(dest, src.read_bytes())
    emit(
        args,
        r,
        lambda r: print(f"✓ {src} -> {host or c.host.name}:{r['path']} ({fmt_size(r['size'])})"),
    )
    return EXIT_OK


def cmd_get(args: argparse.Namespace) -> int:
    host, src = split_target(args.src, args.host)
    c = get_cluster(args, host)
    dest = Path(args.dest or ".").expanduser()
    st = c.stat(src)
    if args.rsync or st["type"] == "dir" or st["size"] > 4 * 1024 * 1024:
        return _rsync(c, src, str(dest), to_remote=False, args=args)
    if dest.is_dir():
        dest = dest / Path(st["path"]).name
    data = c.read_bytes(src, max_bytes=4 * 1024 * 1024 + 1)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    emit(
        args,
        {"path": str(dest), "size": len(data)},
        lambda r: print(f"✓ {c.host.name}:{st['path']} -> {dest} ({fmt_size(len(data))})"),
    )
    return EXIT_OK


def _rsync(c: Cluster, src: str, dest: str, *, to_remote: bool, args: argparse.Namespace) -> int:
    if not isinstance(c.transport, SSHTransport):
        raise InvalidArgument("rsync transfer requires an ssh host")
    if not shutil.which("rsync"):
        raise InvalidArgument("rsync not found locally")
    alias = c.transport.alias
    # expand ~ and $VARS remotely without interpolating into a shell string; -s protects args
    target = dest if to_remote else src
    remote = c.run(["sh", "-c", 'eval "printf %s $1"', "_", target.replace('"', "")])["stdout"]
    remote = remote or target
    rs = [
        "rsync",
        "-az",
        "-s",
        "--info=progress2",
        "-e",
        "ssh -o ControlMaster=no -o BatchMode=yes",
    ]
    rs += [src, f"{alias}:{remote}"] if to_remote else [f"{alias}:{remote}", dest]
    if not args.json:
        print(" ".join(shlex.quote(x) for x in rs), file=sys.stderr)
    r = subprocess.run(rs)
    return r.returncode


def cmd_submit(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    options: dict[str, Any] = {}
    for kv in args.opt or []:
        if "=" in kv:
            k, v = kv.split("=", 1)
            options[k.replace("-", "_")] = v
        else:
            options[kv.replace("-", "_")] = True
    if args.time:
        options["time"] = args.time
    if args.partition:
        options["partition"] = args.partition
    if args.account:
        options["account"] = args.account
    script: str | None = None
    path: str | None = None
    if args.script == "-":
        script = sys.stdin.read()
    elif args.remote:
        path = args.script
    else:
        p = Path(args.script).expanduser()
        if p.exists():
            script = p.read_text()
        elif args.script.startswith(("#!", "#SBATCH")) or "\n" in args.script:
            script = args.script
        else:
            path = args.script  # assume it's a remote path
    if script is not None and not script.startswith("#!"):
        script = "#!/bin/bash\n" + script
    job = c.submit(
        script, path=path, name=args.name, cwd=args.cwd, args=args.sbatch_arg or [], **options
    )
    st = job.status()
    d = st.to_dict()
    emit(
        args,
        d,
        lambda d: print(
            f"Submitted job {job.job_id} ({d.get('state')})\n"
            f"  script: {d.get('script_path')}\n  stdout: {d.get('stdout_path')}"
        ),
    )
    return EXIT_OK


def _print_jobs(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("no jobs")
        return
    cols = ["job_id", "name", "state", "elapsed", "time_limit", "nodelist", "reason", "exit_code"]
    widths = {k: max(len(k), *(len(str(r.get(k, "") or "")) for r in rows)) for k in cols}
    print("  ".join(k.upper().ljust(widths[k]) for k in cols))
    for r in rows:
        print("  ".join(str(r.get(k, "") or "").ljust(widths[k]) for k in cols))


def cmd_jobs(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    sts = c.jobs(include_finished=not args.live, refresh=args.refresh)
    rows = [s.to_dict() for s in sts]
    emit(args, {"jobs": rows, "count": len(rows)}, lambda d: _print_jobs(d["jobs"]))
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    out = [c.job_status(j, refresh=True).to_dict() for j in args.job_id]

    def human(rows: list[dict[str, Any]]) -> None:
        for d in rows:
            print(
                f"{d['job_id']}: {d['state']}"
                + (" (accounting pending)" if d.get("accounting_pending") else "")
            )
            for k in (
                "name",
                "reason",
                "exit_code",
                "elapsed",
                "time_limit",
                "nodelist",
                "partition",
                "start_time",
                "end_time",
                "max_rss",
                "stdout_path",
                "script_path",
            ):
                if d.get(k) is not None:
                    v = fmt_size(d[k]) if k == "max_rss" else d[k]
                    print(f"  {k:12} {v}")

    emit(args, out if len(out) > 1 else out[0], lambda d: human(d if isinstance(d, list) else [d]))
    return EXIT_OK


def cmd_wait(args: argparse.Namespace) -> int:
    c = get_cluster(args)

    def cb(st: Any) -> None:
        if not args.json and not args.quiet:
            print(
                f"{time.strftime('%H:%M:%S')}  {st.job_id}: {st.state}"
                + (f" ({st.reason})" if st.reason else ""),
                file=sys.stderr,
            )

    st = c.wait(args.job_id, poll=args.poll, timeout=args.timeout, callback=cb)
    emit(
        args,
        st.to_dict(),
        lambda d: print(
            f"{d['job_id']}: {d['state']} exit={d.get('exit_code')} elapsed={d.get('elapsed')}"
        ),
    )
    return EXIT_OK if st.state == "COMPLETED" else EXIT_ERROR


def cmd_cancel(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    r = c.cancel(args.job_id)
    emit(
        args,
        r,
        lambda r: print(
            f"cancelled: {', '.join(r['cancelled']) or 'none'}"
            + (f"; skipped (not yours/unknown): {', '.join(r['skipped'])}" if r["skipped"] else "")
        ),
    )
    return EXIT_OK if r["cancelled"] else EXIT_ERROR


def cmd_output(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    r = c.job_output(
        args.job_id,
        tail=args.lines,
        max_bytes=args.max_bytes,
        stream="stderr" if args.stderr else "stdout",
    )
    emit(args, r, lambda r: print(r.get("content", ""), end=""))
    return EXIT_OK


def cmd_sinfo(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    parts = c.sinfo()

    def human(parts: list[dict[str, Any]]) -> None:
        print(
            f"{'PARTITION':14} {'AVAIL':6} {'TIMELIMIT':12} {'NODES':>6} {'IDLE':>6}  "
            f"{'CPUS':>5} {'MEM(MB)':>9}  GRES"
        )
        for p in parts:
            name = p["partition"] + ("*" if p["default"] else "")
            print(
                f"{name:14} {p['avail']:6} {p['time_limit']:12} "
                f"{p['nodes_total']:>6} {p['nodes_idle']:>6}  "
                f"{p['cpus_per_node'] or '':>5} {p['mem_per_node_mb'] or '':>9}  {p['gres'] or ''}"
            )

    emit(args, parts, human)
    return EXIT_OK


def cmd_info(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    info = c.info(refresh=True)
    emit(
        args,
        info,
        lambda i: print("\n".join(f"{k:14} {v}" for k, v in i.items() if k != "slurm_tools")),
    )
    return EXIT_OK


def cmd_config(args: argparse.Namespace) -> int:
    p = config_path()
    if args.init:
        if p.exists() and not args.force:
            print(f"{p} already exists (use --force to overwrite)", file=sys.stderr)
            return EXIT_ERROR
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(EXAMPLE_CONFIG)
        print(f"wrote example config to {p}")
        return EXIT_OK
    cfg = Config.load()
    data = {
        "config_path": str(p),
        "exists": p.exists(),
        "default_host": cfg.default_host,
        "state_dir": str(state_dir()),
        "hosts": {
            n: {k: v for k, v in h.__dict__.items() if k != "name"} for n, h in cfg.hosts.items()
        },
    }
    emit(args, data, lambda d: print(json.dumps(d, indent=2)))
    return EXIT_OK


def cmd_mcp_config(args: argparse.Namespace) -> int:
    from .server import mcp_config_snippet

    host = args.host_name or args.host or Config.load().default_host
    snippet = mcp_config_snippet(host)
    print(snippet if isinstance(snippet, str) else json.dumps(snippet, indent=2))
    if not args.json:
        print(
            "\nAdd to ~/.claude.json (Claude Code) or your agent's MCP config, or run:\n"
            f"  claude mcp add remoteslurm -s user -e REMOTESLURM_DEFAULT_HOST={host or '<host>'} "
            "-- remoteslurm-mcp",
            file=sys.stderr,
        )
    return EXIT_OK


# ----------------------------------------------------------------------------- parser
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="remoteslurm",
        description=(
            "Fast, agent-friendly control of a remote Slurm login node "
            "over a persistent ssh connection."
        ),
        epilog=(
            "Paths may be given as HOST:PATH; otherwise --host / default_host / "
            "$REMOTESLURM_DEFAULT_HOST is used."
        ),
    )
    p.add_argument("--version", action="version", version=f"remoteslurm {__version__}")
    p.add_argument("-H", "--host", help="host name from config (or a bare ssh alias)")
    p.add_argument("--json", action="store_true", help="machine-readable JSON output")
    p.add_argument("--config", help="alternate config.toml")
    p.add_argument(
        "--no-daemon",
        action="store_true",
        help="do not use/start the local session daemon (slower: new ssh channel per call)",
    )
    sub = p.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True
    # The global flags are also accepted after the subcommand (agents put them anywhere).
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-H", "--host", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    common.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS
    )
    common.add_argument("--config", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    common.add_argument(
        "--no-daemon", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS
    )

    def add(
        name: str, fn: Callable[[argparse.Namespace], int], help: str, **kw: Any
    ) -> argparse.ArgumentParser:
        sp = sub.add_parser(name, help=help, description=help, parents=[common], **kw)
        sp.set_defaults(func=fn)
        return sp

    sp = add("connect", cmd_connect, "establish the persistent ssh connection (do MFA once)")
    sp.add_argument("host_name", nargs="?")
    sp.add_argument("--force", action="store_true", help="tear down and re-establish the master")

    sp = add("doctor", cmd_doctor, "diagnose ssh config, connection, remote python and Slurm tools")
    sp.add_argument("host_name", nargs="?")

    sp = add("ls", cmd_ls, "list a remote directory (paged)")
    sp.add_argument("target", nargs="?", help="[HOST:]PATH (default ~)")
    sp.add_argument("-l", "--long", action="store_true")
    sp.add_argument("--no-hidden", action="store_true", help="omit dotfiles")
    sp.add_argument("--limit", type=int, default=200)
    sp.add_argument("--token", help="pagination token from a previous call")

    sp = add("cat", cmd_cat, "print a (bounded) slice of a remote file", aliases=["read"])
    sp.add_argument("target", help="[HOST:]PATH")
    sp.add_argument("--max-bytes", type=int, default=65536)
    sp.add_argument("--offset", type=int, default=0, help="byte offset (negative = from end)")
    sp.add_argument("--head", type=int, help="first N lines")
    sp.add_argument("--tail", type=int, help="last N lines")

    sp = add("tail", cmd_tail, "last lines of a remote file, optionally following")
    sp.add_argument("target", help="[HOST:]PATH")
    sp.add_argument("-n", "--lines", type=int, default=50)
    sp.add_argument("-f", "--follow", action="store_true")
    sp.add_argument("--interval", type=float, default=2.0)
    sp.add_argument("--max-bytes", type=int, default=65536)

    sp = add("grep", cmd_grep, "regex search in remote files (bounded)")
    sp.add_argument("pattern")
    sp.add_argument("target", nargs="?", help="[HOST:]PATH (file or dir, default ~)")
    sp.add_argument("-g", "--glob", help="filename glob filter, e.g. '*.log'")
    sp.add_argument("-i", "--ignore-case", action="store_true")
    sp.add_argument("-C", "--context", type=int, default=0)
    sp.add_argument("--max-matches", type=int, default=200)
    sp.add_argument("--max-depth", type=int, default=10)
    sp.add_argument("--hidden", action="store_true")

    sp = add("find", cmd_find, "find files/dirs by glob under a remote path", aliases=["glob"])
    sp.add_argument("target", help="[HOST:]PATH")
    sp.add_argument("pattern", nargs="?", default="*")
    sp.add_argument("-t", "--type", choices=["file", "dir"])
    sp.add_argument("--limit", type=int, default=500)
    sp.add_argument("--max-depth", type=int, default=10)
    sp.add_argument("--hidden", action="store_true")

    sp = add("run", cmd_run, "run a command on the login node (bounded output)")
    sp.add_argument(
        "cmd", nargs="+", help="shell command (quoted) or, with --argv, an argument vector"
    )
    sp.add_argument("--argv", action="store_true", help="treat CMD words as argv (no shell)")
    sp.add_argument("--cwd")
    sp.add_argument("--timeout", type=int, default=60)
    sp.add_argument(
        "--login", action="store_true", help="use a login shell (bash -lc; loads modules, slower)"
    )
    sp.add_argument("--max-output", type=int, default=65536)

    sp = add(
        "put", cmd_put, "upload a local file (small via stub; --rsync or large/dirs via rsync)"
    )
    sp.add_argument("src")
    sp.add_argument("dest", help="[HOST:]PATH (trailing / = into directory)")
    sp.add_argument("--rsync", action="store_true")

    sp = add(
        "get", cmd_get, "download a remote file (small via stub; --rsync or large/dirs via rsync)"
    )
    sp.add_argument("src", help="[HOST:]PATH")
    sp.add_argument("dest", nargs="?", default=".")
    sp.add_argument("--rsync", action="store_true")

    sp = add(
        "submit",
        cmd_submit,
        "submit a batch job (local script file, '-' for stdin, or remote path)",
    )
    sp.add_argument(
        "script",
        help="local script path, '-' (stdin), inline script text, or remote path (with --remote)",
    )
    sp.add_argument("--remote", action="store_true", help="SCRIPT is a path on the cluster")
    sp.add_argument("-n", "--name", help="job name")
    sp.add_argument("--cwd", help="remote working directory for the job")
    sp.add_argument("-t", "--time")
    sp.add_argument("-p", "--partition")
    sp.add_argument("-A", "--account")
    sp.add_argument(
        "-o",
        "--opt",
        action="append",
        metavar="KEY=VAL",
        help="any sbatch option, e.g. -o gpus-per-node=1 -o mem=16G",
    )
    sp.add_argument(
        "--sbatch-arg", action="append", metavar="ARG", help="raw extra sbatch argument"
    )

    sp = add(
        "jobs", cmd_jobs, "list my jobs (queue + recently submitted)", aliases=["queue", "squeue"]
    )
    sp.add_argument("--live", action="store_true", help="only jobs currently in the queue")
    sp.add_argument("--refresh", action="store_true", help="bypass the squeue cache")

    sp = add("status", cmd_status, "detailed status of job(s)")
    sp.add_argument("job_id", nargs="+")

    sp = add("wait", cmd_wait, "block until a job finishes")
    sp.add_argument("job_id")
    sp.add_argument("--poll", type=float, default=15.0)
    sp.add_argument("--timeout", type=float)
    sp.add_argument("-q", "--quiet", action="store_true")

    sp = add("cancel", cmd_cancel, "cancel job(s) (only your own)", aliases=["scancel"])
    sp.add_argument("job_id", nargs="+")

    sp = add("output", cmd_output, "show a job's stdout (or stderr) file", aliases=["out", "log"])
    sp.add_argument("job_id")
    sp.add_argument("-n", "--lines", type=int, default=100)
    sp.add_argument("--stderr", action="store_true")
    sp.add_argument("--max-bytes", type=int, default=65536)

    add("sinfo", cmd_sinfo, "partition summary", aliases=["partitions"])
    add("info", cmd_info, "remote user/home/env/slurm version")

    sp = add("config", cmd_config, "show config, or --init to write an example")
    sp.add_argument("--init", action="store_true")
    sp.add_argument("--force", action="store_true")

    sp = add("daemon", cmd_daemon, "manage the local session daemon (status|stop|start|run)")
    sp.add_argument(
        "action", choices=["status", "stop", "start", "run"], nargs="?", default="status"
    )

    sp = add("mcp-config", cmd_mcp_config, "print the MCP server registration snippet")
    sp.add_argument("host_name", nargs="?")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except NotConnected as e:
        _report_error(args, e)
        return EXIT_NOT_CONNECTED
    except SlurmError as e:
        _report_error(args, e)
        return EXIT_ERROR
    except RemoteSlurmError as e:
        _report_error(args, e)
        return EXIT_ERROR
    except KeyboardInterrupt:
        return 130
    except BrokenPipeError:
        return EXIT_OK


def _report_error(args: argparse.Namespace, e: RemoteSlurmError) -> None:
    if getattr(args, "json", False):
        print(json.dumps(e.to_dict()))
    else:
        print(f"error [{e.code}]: {e.message}", file=sys.stderr)
        if e.action:
            print(f"  -> {e.action}", file=sys.stderr)
        if os.environ.get("REMOTESLURM_DEBUG") and e.details:
            print(json.dumps(e.details, indent=2, default=str), file=sys.stderr)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
