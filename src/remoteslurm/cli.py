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
    # A warning (never a hard failure): agents rely on `notes` to learn the cluster's rules.
    check(
        "cluster notes",
        True if host.notes else None,
        (host.notes.replace("\n", " ")[:60] if host.notes else "not set"),
        f'add notes = "..." under [hosts.{host.name}] so agents know the cluster rules',
    )
    if host.templates:
        check("templates", True, ", ".join(sorted(host.templates)))
    from . import sync as sync_mod

    found_rsync = sync_mod.find_rsync()
    if found_rsync and found_rsync[1] >= sync_mod.MIN_RSYNC:
        v = found_rsync[1]
        check("rsync", True, f"{found_rsync[0]} ({v[0]}.{v[1]})")
    else:
        detail = (
            f"{found_rsync[0]} is {found_rsync[1][0]}.{found_rsync[1][1]} (need >= 3.1)"
            if found_rsync
            else "not found"
        )
        # Only fail doctor when the host actually has sync projects configured.
        check(
            "rsync",
            False if host.projects else None,
            detail,
            "brew install rsync (needed for `rslurm sync`)",
        )
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
        w = c.write("~/.cache/remoteslurm/.doctor", "ok\n", force=True)
        c.rm(w["path"], force=True)
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
    command = list(args.cmd) if args.argv else cmd
    if args.compute:
        return _cmd_run_compute(args, c, command, cmd)
    r = c.run(
        command,
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


def _cmd_run_compute(
    args: argparse.Namespace, c: Cluster, command: str | list[str], cmd: str
) -> int:
    r = c.run(
        command,
        compute=True,
        template=args.template,
        partition=args.partition,
        time=args.time,
        cpus=args.cpus,
        mem=args.mem,
        gpus=args.gpus,
        queue_timeout=args.queue_timeout,
        login=args.login,
        max_output=args.max_output,
        cwd=args.cwd,
    )

    def human(r: dict[str, Any]) -> None:
        if not r.get("started"):
            print(f"job did not start: {r.get('reason')}", file=sys.stderr)
            return
        sys.stdout.write(r.get("stdout", ""))
        if r.get("stderr"):
            sys.stderr.write(r["stderr"])
        print(f"[ran on {r.get('node')} in {r.get('elapsed')}s, rc {r.get('rc')}]", file=sys.stderr)

    emit(args, r, human)
    if not args.json:
        return EXIT_OK if r.get("started") and r.get("rc") == 0 else EXIT_ERROR
    return EXIT_OK


def cmd_put(args: argparse.Namespace) -> int:
    host, dest = split_target(args.dest, args.host)
    src = Path(args.src).expanduser()
    if not src.exists():
        raise InvalidArgument(f"local file not found: {src}")
    c = get_cluster(args, host)
    if args.rsync or src.is_dir() or src.stat().st_size > 4 * 1024 * 1024:
        c._check_protected(dest, force=args.force, action="put into")
        return _rsync(c, str(src), dest, to_remote=True, args=args)
    if dest.endswith("/"):
        dest = dest + src.name
    r = c.write(dest, src.read_bytes(), force=args.force)
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


def _str_or_file(inline: str | None, file: str | None, what: str) -> str:
    if (inline is None) == (file is None):
        raise InvalidArgument(f"exactly one of --{what} or --{what}-file is required")
    if file is not None:
        p = Path(file).expanduser()
        if not p.is_file():
            raise InvalidArgument(f"local file not found: {p}")
        return p.read_text(encoding="utf-8")
    assert inline is not None
    return inline


def cmd_edit(args: argparse.Namespace) -> int:
    host, path = split_target(args.target, args.host)
    old = _str_or_file(args.old, args.old_file, "old")
    new = _str_or_file(args.new, args.new_file, "new")
    c = get_cluster(args, host)
    r = c.edit(path, old, new, expect=args.expect, all=args.all, force=args.force)

    def human(r: dict[str, Any]) -> None:
        print(f"edited {r['path']}: {r['replacements']} replacement(s) at line {r['first_line']}")
        if r.get("preview"):
            print(r["preview"])

    emit(args, r, human)
    return EXIT_OK


def cmd_diff(args: argparse.Namespace) -> int:
    host, path = split_target(args.target, args.host)
    if (args.localfile is None) == (args.remote is None):
        raise InvalidArgument("give a LOCALFILE or --remote PATH_B (not both)")
    c = get_cluster(args, host)
    if args.localfile is not None:
        local = Path(args.localfile).expanduser()
        if not local.is_file():
            raise InvalidArgument(f"local file not found: {local}")
        r = c.diff(path, local.read_bytes(), context=args.context, max_lines=args.max_lines)
        r["path_b"] = str(local)
    else:
        r = c.diff(path, path_b=args.remote, context=args.context, max_lines=args.max_lines)

    def human(r: dict[str, Any]) -> None:
        if r["diff"]:
            print(r["diff"])
        if r["truncated"]:
            print(f"[diff truncated at {r['lines']} lines; use --max-lines]", file=sys.stderr)

    emit(args, r, human)
    return EXIT_OK if r["identical"] else EXIT_ERROR


def _rsync(c: Cluster, src: str, dest: str, *, to_remote: bool, args: argparse.Namespace) -> int:
    from . import sync as sync_mod

    if not isinstance(c.transport, SSHTransport):
        raise InvalidArgument("rsync transfer requires an ssh host")
    found = sync_mod.find_rsync()
    if found is None:
        raise InvalidArgument("rsync not found locally", action="brew install rsync")
    alias = c.transport.alias
    # expand ~ and $VARS remotely without interpolating into a shell string; -s protects args
    target = dest if to_remote else src
    remote = sync_mod.expand_remote(c, target)
    rs = [
        found[0],
        "-az",
        "-s",
        "--info=progress2",
        "-e",
        sync_mod.DEFAULT_SSH,
    ]
    rs += [src, f"{alias}:{remote}"] if to_remote else [f"{alias}:{remote}", dest]
    if not args.json:
        print(" ".join(shlex.quote(x) for x in rs), file=sys.stderr)
    r = subprocess.run(rs)
    return r.returncode


def cmd_sync(args: argparse.Namespace) -> int:
    from . import sync as sync_mod

    c = get_cluster(args)
    project = sync_mod.resolve_project(c.host, args.project, Path.cwd())
    r = sync_mod.sync(
        c,
        project,
        pull=args.pull,
        dry_run=args.dry_run,
        delete=args.delete,
        force=args.force,
        force_protected=args.force,
        timeout=args.timeout,
    )

    def human(r: dict[str, Any]) -> None:
        arrow = "<-" if r["direction"] == "pull" else "->"
        pre = "[dry-run] " if r["dry_run"] else "✓ "
        print(f"{pre}{r['project']}: {r['local']} {arrow} {c.host.name}:{r['remote']}")
        if r["counts"]:
            cnt = r["counts"]
            print(
                f"  created {cnt['created']}, updated {cnt['updated']}, "
                f"deleted {cnt['deleted']} ({cnt['files']} file(s), "
                f"{fmt_size(cnt['bytes'])} transferred)"
            )
        else:
            print("  (could not parse rsync output; transfer completed with rc 0)")

    emit(args, r, human)
    return EXIT_OK


def cmd_projects(args: argparse.Namespace) -> int:
    from . import sync as sync_mod

    cfg = Config.load(Path(args.config) if getattr(args, "config", None) else None)
    host = cfg.host(args.host)
    rows: list[dict[str, Any]] = []
    c: Cluster | None = get_cluster(args) if (args.verbose and host.projects) else None
    for name, p in sorted(host.projects.items()):
        row: dict[str, Any] = {
            "name": name,
            "local": p.local,
            "remote": p.remote,
            "delete": p.delete,
            "exclude": p.exclude,
        }
        if c is not None:
            row["marker"] = sync_mod.read_marker(c, p)
        rows.append(row)

    def human(d: dict[str, Any]) -> None:
        if not d["projects"]:
            print(f"no projects configured for host {host.name}")
            return
        for r in d["projects"]:
            line = f"{r['name']:16} {r['local']} -> {r['remote']}"
            if r["delete"]:
                line += "  [delete allowed]"
            print(line)
            if marker := r.get("marker"):
                rev = (marker.get("local_git_rev") or "?")[:12]
                dirty = " (dirty)" if marker.get("local_dirty") else ""
                print(f"{'':16} last push {marker.get('pushed_at')} rev {rev}{dirty}")

    emit(args, {"projects": rows, "host": host.name, "count": len(rows)}, human)
    return EXIT_OK


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
        script,
        path=path,
        name=args.name,
        cwd=args.cwd,
        args=args.sbatch_arg or [],
        template=args.template,
        force_preamble=args.force_preamble,
        array=args.array,
        dependency=args.dependency,
        **options,
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


def cmd_sweep(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    params: dict[str, list[str]] = {}
    for spec in args.param or []:
        if "=" not in spec:
            raise InvalidArgument(f"bad -P {spec!r}, expected NAME=v1,v2")
        k, v = spec.split("=", 1)
        params[k] = v.split(",")
    if not params:
        raise InvalidArgument("at least one -P NAME=v1,v2 is required")
    options: dict[str, Any] = {}
    for kv in args.opt or []:
        if "=" in kv:
            k, v = kv.split("=", 1)
            options[k.replace("-", "_")] = v
        else:
            options[kv.replace("-", "_")] = True
    script: str | None = None
    path: str | None = None
    if args.remote:
        path = args.script
    elif args.script == "-":
        script = sys.stdin.read()
    else:
        p = Path(args.script).expanduser()
        if p.exists():
            script = p.read_text()
        elif args.script.startswith(("#!", "#SBATCH")) or "\n" in args.script:
            script = args.script
        else:
            path = args.script
    if script is not None and not script.startswith("#!"):
        script = "#!/bin/bash\n" + script
    job = c.sweep(
        params,
        script=script,
        path=path,
        template=args.template,
        name=args.name,
        max_concurrent=args.max_concurrent,
        cwd=args.cwd,
        **options,
    )
    rec = c.registry.get(job.job_id)
    sweep_meta = (rec.meta.get("sweep") if rec else None) or {}
    st = job.status()
    d = st.to_dict()
    d["n"] = sweep_meta.get("n")
    d["params_path"] = sweep_meta.get("params_path")
    emit(
        args,
        d,
        lambda d: print(
            f"Submitted sweep {job.job_id} ({sweep_meta.get('n')} tasks, "
            f"array {rec.meta.get('array') if rec else '?'})\n"
            f"  params: {sweep_meta.get('params_path')}\n  script: {d.get('script_path')}"
        ),
    )
    return EXIT_OK


def cmd_templates(args: argparse.Namespace) -> int:
    cfg = Config.load(Path(args.config) if getattr(args, "config", None) else None)
    host = cfg.host(args.host)
    if args.show:
        t = host.resolve_template(args.show)  # ConfigError on unknown/cycle

        def show_human(d: dict[str, Any]) -> None:
            print(f"template {d['name']} (host {host.name}):")
            for k, v in d["options"].items():
                print(f"  --{k.replace('_', '-')}={v}")
            if d["preamble"]:
                print("  preamble:")
                for ln in d["preamble"].splitlines():
                    print(f"    {ln}")
            if d["epilogue"]:
                print("  epilogue:")
                for ln in d["epilogue"].splitlines():
                    print(f"    {ln}")

        emit(args, {"name": args.show, **t.summary()}, show_human)
        return EXIT_OK
    summaries = host.template_summaries()

    def human(d: dict[str, Any]) -> None:
        if not d["templates"]:
            print(f"no templates configured for host {host.name}")
            return
        for name, s in d["templates"].items():
            opts = " ".join(f"{k}={v}" for k, v in s["options"].items())
            inh = f" (inherits {s['inherit']})" if s.get("inherit") else ""
            pre = " +preamble" if s["preamble"] else ""
            print(f"{name:12} {opts}{inh}{pre}")

    emit(args, {"templates": summaries, "host": host.name, "count": len(summaries)}, human)
    return EXIT_OK


def cmd_notes(args: argparse.Namespace) -> int:
    from .jobs import read_learned_notes

    cfg = Config.load(Path(args.config) if getattr(args, "config", None) else None)
    host = cfg.host(args.host)
    learned = read_learned_notes(host.name)
    data = {"host": host.name, "notes": host.notes, "learned_notes": learned}

    def human(d: dict[str, Any]) -> None:
        if d["notes"]:
            print(d["notes"].rstrip("\n"))
        else:
            print(f'(no notes set for host {host.name}; add notes = "..." to the config)')
        if d["learned_notes"]:
            print("\nlearned from past rejections:")
            for ln in d["learned_notes"]:
                print(f"  - {ln}")

    emit(args, data, human)
    return EXIT_OK


def cmd_agent_guide(args: argparse.Namespace) -> int:
    from .guide import AGENT_GUIDE

    if args.json:
        print(json.dumps({"guide": AGENT_GUIDE}, indent=2))
    else:
        print(AGENT_GUIDE, end="" if AGENT_GUIDE.endswith("\n") else "\n")
    return EXIT_OK


def cmd_diagnose(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    d = c.diagnose(args.job_id, tail=args.lines)

    def human(d: dict[str, Any]) -> None:
        print(d["verdict"])
        for h in d["hints"]:
            print(f"  - {h}")
        st = d.get("status", {})
        meta = f"state={st.get('state')}"
        if st.get("exit_code") is not None:
            meta += f" exit={st.get('exit_code')}"
        if st.get("reason"):
            meta += f" reason={st.get('reason')}"
        print(f"\n[{meta}]")
        if d.get("stderr_tail"):
            print("\n--- stderr (tail) ---")
            print(d["stderr_tail"].rstrip("\n"))
        if d.get("stdout_tail"):
            print("\n--- stdout (tail) ---")
            print(d["stdout_tail"].rstrip("\n"))
        if d.get("truncated"):
            print("\n[diagnose output truncated to fit the size cap]", file=sys.stderr)

    emit(args, d, human)
    return EXIT_OK


def array_tasks_cell(row: dict[str, Any]) -> str:
    """A compact ``87✓ 10▶ 3◦ 2✗`` summary of an array row's task states (empty if not an array)."""
    from . import slurm

    extra = row.get("extra") or {}
    if not extra.get("array"):
        return ""
    counts = extra.get("tasks") or {}
    completed = counts.get("COMPLETED", 0)
    pending = counts.get("PENDING", 0)
    cancelled = counts.get("CANCELLED", 0)
    running = sum(v for k, v in counts.items() if k not in slurm.TERMINAL_STATES and k != "PENDING")
    failed = sum(v for k, v in counts.items() if k in slurm.ARRAY_FAILED_STATES)
    parts = []
    if completed:
        parts.append(f"{completed}✓")  # ✓
    if running:
        parts.append(f"{running}▶")  # ▶
    if pending:
        parts.append(f"{pending}◦")  # ◦
    if failed:
        parts.append(f"{failed}✗")  # ✗
    if cancelled:
        parts.append(f"{cancelled}⊘")  # ⊘
    return " ".join(parts)


def _print_jobs(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("no jobs")
        return
    cols = ["job_id", "name", "state", "elapsed", "time_limit", "nodelist", "reason", "exit_code"]
    if any(r.get("tasks") for r in rows):
        cols.insert(3, "tasks")

    def cell(r: dict[str, Any], k: str) -> str:
        v = r.get(k)
        return "" if v is None else str(v)

    widths = {k: max(len(k), *(len(cell(r, k)) for r in rows)) for k in cols}
    print("  ".join(k.upper().ljust(widths[k]) for k in cols))
    for r in rows:
        print("  ".join(cell(r, k).ljust(widths[k]) for k in cols))


def cmd_jobs(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    pruned: dict[str, Any] | None = None
    if getattr(args, "prune", False):
        pruned = c.registry.prune(force=True)
        if not args.json:
            print(f"pruned {pruned['pruned']} stale record(s)", file=sys.stderr)
    sts = c.jobs(include_finished=not args.live, refresh=args.refresh)
    rows = [s.to_dict() for s in sts]
    for r in rows:
        cell = array_tasks_cell(r)
        if cell:
            r["tasks"] = cell
    out: dict[str, Any] = {"jobs": rows, "count": len(rows)}
    if pruned is not None:
        out["pruned"] = pruned
    emit(args, out, lambda d: _print_jobs(d["jobs"]))
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
            extra = d.get("extra") or {}
            if extra.get("array"):
                _print_array_status(d, extra, show_tasks=args.tasks)
                continue
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
            if extra.get("params"):
                print(
                    f"  {'params':12} " + " ".join(f"{k}={v}" for k, v in extra["params"].items())
                )

    emit(args, out if len(out) > 1 else out[0], lambda d: human(d if isinstance(d, list) else [d]))
    return EXIT_OK


def _print_array_status(d: dict[str, Any], extra: dict[str, Any], *, show_tasks: bool) -> None:
    cell = array_tasks_cell(d)
    print(f"  {'tasks':12} {extra.get('n_tasks', 0)} total  {cell}")
    failed = extra.get("failed_tasks") or []
    if failed:
        print(f"  {'failed':12} " + ", ".join(str(t) for t in failed))
    fparams = extra.get("failed_task_params") or {}
    for t in failed:
        p = fparams.get(t) or fparams.get(str(t))
        if p:
            print(f"    task {t}: " + " ".join(f"{k}={v}" for k, v in p.items()))
    if d.get("name"):
        print(f"  {'name':12} {d['name']}")
    if show_tasks:
        states = extra.get("task_states") or {}
        for t in sorted(states, key=lambda x: int(x)):
            print(f"    {str(t):>6}  {states[t]}")


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
    # For an array, succeed only if every task COMPLETED (aggregate COMPLETED can still hide a
    # cancelled task); for a single job, COMPLETED is enough.
    extra = st.extra or {}
    if extra.get("array"):
        ok = set(extra.get("tasks") or {}) == {"COMPLETED"}
    else:
        ok = st.state == "COMPLETED"
    return EXIT_OK if ok else EXIT_ERROR


def _confirm_gate(args: argparse.Namespace, c: Cluster, op: str, what: str) -> bool:
    """CLI-side confirmation for a gated op: True to proceed, False if the user declined.

    ``--yes`` (or the op not being in the host's ``confirm`` list) proceeds silently; otherwise
    prompt ``y/N`` on a tty (a non-interactive stdin counts as "no").
    """
    if op not in c.host.confirm or getattr(args, "yes", False):
        return True
    try:
        ans = input(f"{what}? [y/N] ")
    except EOFError:
        ans = ""
    return ans.strip().lower() in ("y", "yes")


def cmd_cancel(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    if not _confirm_gate(args, c, "cancel", f"cancel {', '.join(args.job_id)}"):
        print("aborted", file=sys.stderr)
        return EXIT_ERROR
    r = c.cancel(args.job_id, confirm=True)
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


def cmd_queue(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    q = c.queue_info()

    def human(q: dict[str, Any]) -> None:
        parts = q.get("partitions") or []
        print("PARTITIONS")
        if not parts:
            print("  (sinfo unavailable)")
        for p in parts:
            name = p.get("partition", "?") + ("*" if p.get("default") else "")
            print(
                f"  {name:16} {p.get('avail', ''):5} {str(p.get('time_limit', '')):10} "
                f"nodes {p.get('nodes_total', 0):>4} (idle {p.get('nodes_idle', 0)})"
            )
        accounts = q.get("accounts") or []
        print("\nMY ACCOUNTS / QOS")
        if not accounts:
            print("  (sacctmgr unavailable)")
        for a in accounts:
            bits = [f"account={a.get('account')}"]
            if a.get("partition"):
                bits.append(f"partition={a['partition']}")
            if a.get("qos"):
                bits.append(f"qos={a['qos']}")
            if a.get("max_jobs"):
                bits.append(f"maxjobs={a['max_jobs']}")
            print("  " + "  ".join(bits))
        qos = q.get("qos") or []
        if qos:
            print("\nQOS LIMITS")
            for x in qos:
                print(
                    f"  {str(x.get('name')):12} maxwall={x.get('max_wall') or '-'}  "
                    f"maxjobs/user={x.get('max_jobs_pu') or '-'}  "
                    f"priority={x.get('priority') or '-'}"
                )
        fair = q.get("fairshare") or []
        if fair:
            print("\nFAIR-SHARE")
            for f in fair:
                if not f.get("account"):
                    continue
                print(
                    f"  {str(f.get('account')):12} norm_shares={f.get('norm_shares')}  "
                    f"usage={f.get('effective_usage')}  fairshare={f.get('fair_share')}"
                )
        pending = q.get("pending") or []
        print("\nMY PENDING JOBS")
        if not pending:
            print("  (none)")
        for j in pending:
            line = f"  {j.get('job_id'):>10}  {j.get('name') or ''}"
            if j.get("est_start"):
                line += f"  est_start={j['est_start']}"
            if j.get("reason"):
                line += f"  ({j['reason']})"
            print(line)

    emit(args, q, human)
    return EXIT_OK


def cmd_quota(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    q = c.quota()

    def human(q: dict[str, Any]) -> None:
        if not q.get("available"):
            print(f"disk usage unavailable ({q.get('source')})", file=sys.stderr)
            if q.get("stderr"):
                print(q["stderr"].rstrip("\n"), file=sys.stderr)
            return
        if q.get("source") == "df":
            print(f"{'FILESYSTEM':24} {'SIZE':>6} {'USED':>6} {'AVAIL':>6} {'USE%':>5}  MOUNT")
            for u in q.get("usage", []):
                print(
                    f"{str(u.get('filesystem'))[:24]:24} {str(u.get('size') or ''):>6} "
                    f"{str(u.get('used') or ''):>6} {str(u.get('avail') or ''):>6} "
                    f"{str(u.get('use_pct') or ''):>5}  {u.get('mounted_on') or ''}"
                )
        else:
            for u in q.get("usage", []):
                line = f"  {str(u.get('description')):32}"
                if u.get("used") or u.get("limit"):
                    line += f" {u.get('used') or '?'}/{u.get('limit') or '?'}"
                if u.get("files_used") or u.get("files_limit"):
                    line += f"   files {u.get('files_used') or '?'}/{u.get('files_limit') or '?'}"
                print(line)

    emit(args, q, human)
    return EXIT_OK


def _completed_ok(st: Any) -> bool:
    """True iff a job (or every array task) COMPLETED — the ``watch``/``wait`` success rule."""
    extra = st.extra or {}
    if extra.get("array"):
        return set(extra.get("tasks") or {}) == {"COMPLETED"}
    return bool(st.state == "COMPLETED")


def cmd_watch(args: argparse.Namespace) -> int:
    from . import slurm, watch

    c = get_cluster(args)
    host = c.host.name
    if args.all:
        rows = c.squeue(refresh=True)
        ids = sorted(
            {r["array_base"] or r["job_id"] for r in rows},
            key=lambda j: int(j.split("_")[0]),
        )
        if not ids:
            print("no jobs currently in the queue", file=sys.stderr)
            return EXIT_OK
    else:
        ids = list(args.job_id)
    for j in ids:
        slurm.parse_job_id(j)
    poll = max(2.0, args.poll)
    last_state: dict[str, str] = {}
    done: dict[str, Any] = {}
    t0 = time.time()
    all_ok = True
    try:
        while len(done) < len(ids):
            for j in ids:
                if j in done:
                    continue
                st = c.job_status(j, refresh=True)
                prev = last_state.get(j)
                if st.state != prev:
                    stamp = time.strftime("%H:%M:%S")
                    extra = f" ({st.reason})" if st.reason else ""
                    if not args.json:
                        print(f"{stamp}  {j}: {prev or '-'} -> {st.state}{extra}")
                    last_state[j] = st.state
                if st.terminal:
                    done[j] = st
                    ev = {
                        "t": time.time(),
                        "job_id": j,
                        "state": st.state,
                        "exit_code": st.exit_code,
                        "name": st.name,
                    }
                    watch.append_event(host, ev)
                    if not _completed_ok(st):
                        all_ok = False
                    if args.notify:
                        watch.notify(c.host, f"{j} {st.state}")
            if len(done) >= len(ids):
                break
            if args.timeout is not None and time.time() - t0 > args.timeout:
                print(f"timeout after {args.timeout}s", file=sys.stderr)
                all_ok = False
                break
            time.sleep(poll)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130
    if args.json:
        emit(args, {"watched": ids, "results": {j: s.to_dict() for j, s in done.items()}})
    return EXIT_OK if all_ok else EXIT_ERROR


def cmd_events(args: argparse.Namespace) -> int:
    from . import watch

    cfg = Config.load(Path(args.config) if getattr(args, "config", None) else None)
    host = cfg.host(args.host).name
    evs = watch.drain_events(host, since=args.since, all=args.all)

    def human(d: dict[str, Any]) -> None:
        if not d["events"]:
            print("no new events")
            return
        for e in d["events"]:
            stamp = fmt_time(e.get("t"))
            exit_s = f" exit={e['exit_code']}" if e.get("exit_code") is not None else ""
            name = f" {e['name']}" if e.get("name") else ""
            print(f"{stamp}  {e.get('job_id')}: {e.get('state')}{exit_s}{name}")

    emit(args, {"events": evs, "count": len(evs), "host": host}, human)
    return EXIT_OK


def cmd_forget(args: argparse.Namespace) -> int:
    from .jobs import JobRegistry

    cfg = Config.load(Path(args.config) if getattr(args, "config", None) else None)
    host = cfg.host(args.host).name
    ok = JobRegistry(host).forget(args.job_id)
    emit(
        args,
        {"job_id": args.job_id, "forgotten": ok},
        lambda d: print(f"forgot {args.job_id}" if ok else f"{args.job_id} not in the registry"),
    )
    return EXIT_OK if ok else EXIT_ERROR


def cmd_clean(args: argparse.Namespace) -> int:
    c = get_cluster(args)
    r = c.clean(older_than_days=args.older_than_days, dry_run=args.dry_run)

    def human(r: dict[str, Any]) -> None:
        pre = "[dry-run] would remove" if r["dry_run"] else "removed"
        if not r["removed"]:
            print(f"nothing to clean ({r['kept']} file(s) kept)")
            return
        print(f"{pre} {r['count']} file(s) ({r['kept']} kept):")
        for p in r["removed"]:
            print(f"  {p}")

    emit(args, r, human)
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

    sp = add(
        "run",
        cmd_run,
        "run a command on the login node (or a compute node with --compute)",
    )
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
    sp.add_argument(
        "--compute", action="store_true", help="run on a compute node via srun (queues for a node)"
    )
    sp.add_argument("--template", help="config template for --compute resources (options only)")
    sp.add_argument("-p", "--partition", help="--compute partition")
    sp.add_argument("-t", "--time", help="--compute walltime (e.g. 00:10:00)")
    sp.add_argument("-c", "--cpus", type=int, help="--compute cpus per task")
    sp.add_argument("--mem", help="--compute memory (e.g. 8G)")
    sp.add_argument("--gpus", type=int, help="--compute GPUs (--gres=gpu:N)")
    sp.add_argument(
        "--queue-timeout",
        type=int,
        default=600,
        help="--compute: seconds to wait for an allocation before giving up",
    )

    sp = add(
        "put", cmd_put, "upload a local file (small via stub; --rsync or large/dirs via rsync)"
    )
    sp.add_argument("src")
    sp.add_argument("dest", help="[HOST:]PATH (trailing / = into directory)")
    sp.add_argument("--rsync", action="store_true")
    sp.add_argument("--force", action="store_true", help="override the protected-path guard")

    sp = add(
        "get", cmd_get, "download a remote file (small via stub; --rsync or large/dirs via rsync)"
    )
    sp.add_argument("src", help="[HOST:]PATH")
    sp.add_argument("dest", nargs="?", default=".")
    sp.add_argument("--rsync", action="store_true")

    sp = add("edit", cmd_edit, "replace an exact string in a remote text file (atomic)")
    sp.add_argument("target", help="[HOST:]PATH")
    sp.add_argument("--old", help="exact string to replace")
    sp.add_argument("--new", help="replacement string")
    sp.add_argument("--old-file", metavar="FILE", help="read OLD from a local file (multi-line)")
    sp.add_argument("--new-file", metavar="FILE", help="read NEW from a local file (multi-line)")
    sp.add_argument("--all", action="store_true", help="replace every occurrence")
    sp.add_argument("--expect", type=int, default=1, help="required occurrence count (default 1)")
    sp.add_argument("--force", action="store_true", help="override the protected-path guard")

    sp = add(
        "diff", cmd_diff, "unified diff of a remote file vs a local file (exit 1 if different)"
    )
    sp.add_argument("target", help="[HOST:]PATH")
    sp.add_argument("localfile", nargs="?", help="local file to compare against")
    sp.add_argument(
        "--remote", metavar="PATH_B", help="compare against another remote path instead"
    )
    sp.add_argument("-C", "--context", type=int, default=3)
    sp.add_argument("--max-lines", type=int, default=500)

    sp = add("sync", cmd_sync, "rsync a configured project to (or from) the cluster")
    sp.add_argument("project", nargs="?", help="project name (default: the one containing CWD)")
    sp.add_argument("--pull", action="store_true", help="remote -> local instead of push")
    sp.add_argument("-n", "--dry-run", action="store_true", help="show changes without applying")
    sp.add_argument(
        "--delete",
        action="store_true",
        help="delete files missing from the source (project must also set delete = true)",
    )
    sp.add_argument("--force", action="store_true", help="skip the size guard")
    sp.add_argument("--timeout", type=int, default=1800, help="rsync timeout in seconds")

    sp = add("projects", cmd_projects, "list the host's configured sync projects")
    sp.add_argument(
        "-v", "--verbose", action="store_true", help="also read each remote sync marker (slower)"
    )

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
        "--template", help="config template name (see `rslurm templates`): options + preamble"
    )
    sp.add_argument(
        "--array", metavar="SPEC", help="submit a job array, e.g. 0-9 or 0-9%%4 (throttle)"
    )
    sp.add_argument("--dependency", metavar="SPEC", help="sbatch --dependency, e.g. afterok:123")
    sp.add_argument(
        "--force-preamble",
        action="store_true",
        help="apply a template's options to a --remote script even though its preamble is skipped",
    )
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
        "sweep",
        cmd_sweep,
        "submit a parameter sweep as a job array (-P NAME=v1,v2 per parameter)",
    )
    sp.add_argument(
        "script",
        help="local script path, '-' (stdin), inline script text, or remote path (with --remote)",
    )
    sp.add_argument("--remote", action="store_true", help="SCRIPT is a path on the cluster")
    sp.add_argument(
        "-P",
        "--param",
        action="append",
        metavar="NAME=v1,v2",
        help="a sweep parameter and its values (repeatable; the Cartesian product is taken)",
    )
    sp.add_argument("-n", "--name", default="sweep", help="job/array name (default: sweep)")
    sp.add_argument("--cwd", help="remote working directory for the job")
    sp.add_argument("--template", help="config template name (options + preamble)")
    sp.add_argument(
        "--max-concurrent", type=int, help="cap simultaneously running tasks (array %%N throttle)"
    )
    sp.add_argument(
        "-o", "--opt", action="append", metavar="KEY=VAL", help="any sbatch option, e.g. -o mem=8G"
    )

    sp = add("templates", cmd_templates, "list submit templates (or --show NAME)")
    sp.add_argument("--show", metavar="NAME", help="show one template's resolved options/preamble")

    add("notes", cmd_notes, "print the host's cluster notes and any learned policy notes")

    add("agent-guide", cmd_agent_guide, "print the CLAUDE.md/AGENTS.md block for coding agents")

    sp = add("diagnose", cmd_diagnose, "explain a finished/stuck job (verdict, hints, log tails)")
    sp.add_argument("job_id")
    sp.add_argument("-n", "--lines", type=int, default=60, help="log tail lines to include")

    sp = add("jobs", cmd_jobs, "list my jobs (queue + recently submitted)", aliases=["squeue"])
    sp.add_argument("--live", action="store_true", help="only jobs currently in the queue")
    sp.add_argument("--refresh", action="store_true", help="bypass the squeue cache")
    sp.add_argument(
        "--prune", action="store_true", help="drop long-finished registry records first"
    )

    sp = add("status", cmd_status, "detailed status of job(s)")
    sp.add_argument("job_id", nargs="+")
    sp.add_argument("--tasks", action="store_true", help="for an array, list every task's state")

    sp = add(
        "wait",
        cmd_wait,
        "block until a job finishes (exit 0 only if it — or every array task — COMPLETED)",
    )
    sp.add_argument("job_id")
    sp.add_argument("--poll", type=float, default=15.0)
    sp.add_argument("--timeout", type=float)
    sp.add_argument("-q", "--quiet", action="store_true")

    sp = add("cancel", cmd_cancel, "cancel job(s) (only your own)", aliases=["scancel"])
    sp.add_argument("job_id", nargs="+")
    sp.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="skip the confirmation prompt (if host requires it)",
    )

    sp = add("output", cmd_output, "show a job's stdout (or stderr) file", aliases=["out", "log"])
    sp.add_argument("job_id")
    sp.add_argument("-n", "--lines", type=int, default=100)
    sp.add_argument("--stderr", action="store_true")
    sp.add_argument("--max-bytes", type=int, default=65536)

    add("sinfo", cmd_sinfo, "partition summary", aliases=["partitions"])
    add("info", cmd_info, "remote user/home/env/slurm version")

    add(
        "queue",
        cmd_queue,
        "queue intelligence: partitions, my accounts/QOS, fair-share, pending start estimates",
    )

    add("quota", cmd_quota, "disk usage/quota (diskusage_report, else df -h)")

    sp = add(
        "watch",
        cmd_watch,
        "watch job(s) until they finish (exit 0 only if all COMPLETED); foreground",
    )
    sp.add_argument("job_id", nargs="*", help="job id(s) to watch (or use --all)")
    sp.add_argument("--all", action="store_true", help="watch everything currently in the queue")
    sp.add_argument("--notify", action="store_true", help="desktop notification on each finish")
    sp.add_argument("--poll", type=float, default=30.0, help="seconds between polls (default 30)")
    sp.add_argument("--timeout", type=float, help="give up after this many seconds")

    sp = add("events", cmd_events, "show job-finish events recorded by `watch` (unseen by default)")
    sp.add_argument("--since", help="only events at/after this ISO time (or epoch)")
    sp.add_argument("--all", action="store_true", help="show all events, not just unseen ones")

    sp = add("forget", cmd_forget, "remove a job from the local registry")
    sp.add_argument("job_id")

    sp = add(
        "clean", cmd_clean, "remove generated sbatch scripts/sweeps older than the cutoff (remote)"
    )
    sp.add_argument("-n", "--dry-run", action="store_true", help="list what would be removed")
    sp.add_argument(
        "--older-than-days", type=int, default=30, help="age cutoff in days (default 30)"
    )

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
