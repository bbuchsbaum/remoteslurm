"""``Cluster``: the public, synchronous, thread-safe API over one remote host."""

from __future__ import annotations

import base64
import fnmatch
import json
import os
import re
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

from . import slurm
from .config import Config, HostConfig
from .errors import (
    ConfirmationRequired,
    InvalidArgument,
    PermissionDenied,
    RemoteSlurmError,
)
from .jobs import SlurmOps, read_learned_notes
from .session import DEFAULT_TIMEOUT, Session
from .transport import LocalTransport, SSHTransport, Transport


def _expand_home(path: str, home: str | None) -> str:
    """Expand a leading ``~`` (and ``$HOME``) using the known remote home, if any."""
    if home and path == "~":
        return home
    if home and path.startswith("~/"):
        return home.rstrip("/") + "/" + path[2:]
    if home and path.startswith("$HOME"):
        return home.rstrip("/") + path[len("$HOME") :]
    return path


def _matches_protected(path: str, patterns: list[str], home: str | None) -> bool:
    """True if ``path`` matches any protected glob (raw or with ``~``/``$HOME`` expanded).

    ``fnmatch`` is used so ``*`` spans path separators, letting ``~/.ssh/**`` cover everything
    under it; both the pattern and the path are compared raw and home-expanded so a
    ``~``-relative arg matches an absolute pattern and vice-versa.
    """
    forms = {path, _expand_home(path, home)}
    for pat in patterns:
        pats = {pat, _expand_home(pat, home)}
        for p in forms:
            for q in pats:
                if fnmatch.fnmatch(p, q):
                    return True
    return False


_registry_lock = threading.Lock()
_clusters: dict[str, Cluster] = {}


class Cluster(SlurmOps):
    """A view onto one remote login node.

    Construct with :meth:`Cluster.connect` (uses config + ssh) or :meth:`Cluster.local`
    (runs the stub locally; used by tests).
    """

    def __init__(self, host: HostConfig, transport: Transport, session: Any = None) -> None:
        self.host = host
        self.transport = transport
        self.session = session if session is not None else Session(transport)
        self._info: dict[str, Any] | None = None
        self._squeue_cache = None
        self._squeue_lock = threading.Lock()
        self._registry = None

    # -- constructors --------------------------------------------------------------------
    @classmethod
    def connect(cls, name: str | None = None, *, config: Config | None = None) -> Cluster:
        """Return the (cached) cluster for ``name`` and make sure the stub is running."""
        config = config or Config.load()
        host = config.host(name)
        with _registry_lock:
            c = _clusters.get(host.name)
            if c is None:
                c = cls(host, cls._transport_for(host))
                _clusters[host.name] = c
        c.session.start()
        return c

    @staticmethod
    def _transport_for(host: HostConfig) -> Transport:
        if host.ssh == "local" or host.extra.get("transport") == "local":
            env = host.extra.get("env")
            return LocalTransport(env={str(k): str(v) for k, v in env.items()} if env else None)
        return SSHTransport(
            alias=host.ssh,
            mfa=host.mfa,
            python=host.python,
            install_dir=host.install_dir,
            control_path=host.control_path,
            control_persist=host.control_persist,
            extra_ssh_opts=list(host.ssh_opts),
        )

    @classmethod
    def local(cls, name: str = "local", **host_kw: Any) -> Cluster:
        host = HostConfig(name=name, ssh="local", mfa=False, **host_kw)
        c = cls(host, LocalTransport())
        c.session.start()
        return c

    def close(self) -> None:
        self.session.close()
        with _registry_lock:
            if _clusters.get(self.host.name) is self:
                del _clusters[self.host.name]

    def __enter__(self) -> Cluster:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- low level -------------------------------------------------------------------------
    def call(
        self,
        op: str,
        *,
        _timeout: float | None = DEFAULT_TIMEOUT,
        _cancel_on_timeout: bool = False,
        **args: Any,
    ) -> Any:
        return self.session.call(op, args, timeout=_timeout, cancel_on_timeout=_cancel_on_timeout)

    # -- basics ------------------------------------------------------------------------------
    def ping(self) -> dict[str, Any]:
        return self.call("ping", _timeout=15)

    def info(self, refresh: bool = False) -> dict[str, Any]:
        if self._info is None or refresh:
            base = dict(self.call("info", _timeout=30))
            # Local (client-side) facts an agent should read before submitting.
            base["notes"] = self.host.notes
            base["templates"] = self.host.template_summaries()
            base["learned_notes"] = read_learned_notes(self.host.name)
            self._info = base
        return self._info

    @property
    def user(self) -> str:
        return str(self.info()["user"])

    @property
    def home(self) -> str:
        return str(self.info()["home"])

    # -- safety rails (E2) --------------------------------------------------------------------
    def _cached_home(self) -> str | None:
        """The remote home for protected-path matching (cached ``info``; never raises)."""
        if self._info is not None:
            return self._info.get("home")
        try:
            return str(self.info().get("home"))
        except RemoteSlurmError:
            return None

    def _check_protected(self, path: str, *, force: bool = False, action: str = "write") -> None:
        """Refuse to touch a configured protected path unless ``force``."""
        patterns = self.host.protected_paths
        if force or not patterns:
            return
        if _matches_protected(path, patterns, self._cached_home()):
            raise PermissionDenied(
                f"{path} is a protected path on host {self.host.name} (refusing to {action})",
                action="pass force=True (library/MCP) or --force (CLI) to override",
                path=path,
            )

    def _enforce_run_policy(self, cmd: str | list[str]) -> None:
        """Apply ``allow_run`` to a ``run``/``srun`` request (client-side gate).

        ``true`` allows anything; ``false`` disables running; ``"safe"`` requires an argv list
        whose ``argv[0]`` basename matches ``run_allowlist``.
        """
        mode = self.host.allow_run
        if mode is False:
            raise PermissionDenied(
                f"`run` is disabled for host {self.host.name}",
                action='set allow_run = true (or "safe") in the host config',
            )
        if isinstance(mode, str) and mode == "safe":
            if isinstance(cmd, str):
                raise PermissionDenied(
                    f'allow_run = "safe" on host {self.host.name} forbids shell-string commands',
                    action="pass the command as an argv list, e.g. run(['python3', '-c', '...'])",
                )
            exe = os.path.basename(cmd[0]) if cmd else ""
            allow = self.host.run_allowlist
            if not any(fnmatch.fnmatch(exe, pat) for pat in allow):
                raise PermissionDenied(
                    f"{exe!r} is not in run_allowlist on host {self.host.name}",
                    action="add it to run_allowlist, or use a permitted executable; "
                    f"allowed: {', '.join(sorted(allow))}",
                )
            # A shell with -c/-lc is arbitrary execution: it defeats the allow-list even
            # though the shell is listed (so `bash script.sh` is allowed, `bash -c '…'` is not).
            if exe in ("bash", "sh", "zsh", "dash", "ksh") and any(
                a in ("-c", "-lc", "-ic", "-lic") or (a.startswith("-") and "c" in a[1:])
                for a in cmd[1:]
            ):
                raise PermissionDenied(
                    f'allow_run = "safe" forbids `{exe} -c` (arbitrary shell) on '
                    f"host {self.host.name}",
                    action="run the target executable directly as argv, e.g. "
                    "['python3', 'script.py']",
                )

    def _require_confirm(self, op: str, confirm: bool, what: str) -> None:
        """Raise ``ConfirmationRequired`` if ``op`` is gated by ``host.confirm`` and unconfirmed."""
        if op in self.host.confirm and not confirm:
            raise ConfirmationRequired(
                f"{op} needs confirmation on host {self.host.name}: {what}",
                what=what,
                op=op,
            )

    # -- filesystem ---------------------------------------------------------------------------
    def ls(
        self, path: str = "~", *, limit: int = 200, token: str | None = None, hidden: bool = True
    ) -> dict[str, Any]:
        return self.call("ls", path=path, limit=limit, token=token, hidden=hidden)

    def stat(self, path: str) -> dict[str, Any]:
        return self.call("stat", path=path)

    def exists(self, path: str) -> bool:
        from .errors import NotFound

        try:
            self.stat(path)
            return True
        except NotFound:
            return False

    def read(
        self,
        path: str,
        *,
        max_bytes: int = 65536,
        offset: int = 0,
        head: int | None = None,
        tail: int | None = None,
    ) -> dict[str, Any]:
        """Read a bounded slice of a file. Text files return ``content``; binary ``content_b64``."""
        return self.call(
            "read",
            path=path,
            max_bytes=max_bytes,
            offset=offset,
            head=head,
            tail=tail,
            _timeout=120,
        )

    def read_text(self, path: str, *, max_bytes: int = 65536) -> str:
        r = self.read(path, max_bytes=max_bytes)
        if r.get("binary"):
            raise InvalidArgument(f"{path} is binary", path=path)
        return str(r["content"])

    def read_bytes(self, path: str, *, max_bytes: int = 4 * 1024 * 1024) -> bytes:
        r = self.read(path, max_bytes=max_bytes)
        if r.get("binary"):
            return base64.b64decode(r["content_b64"])
        return str(r["content"]).encode("utf-8")

    def write(
        self,
        path: str,
        content: str | bytes,
        *,
        append: bool = False,
        mkdirs: bool = True,
        mode: int | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        self._check_protected(path, force=force, action="write")
        args: dict[str, Any] = {"path": path, "append": append, "mkdirs": mkdirs, "mode": mode}
        if isinstance(content, bytes):
            args["content_b64"] = base64.b64encode(content).decode("ascii")
        else:
            args["content"] = content
        return self.call("write", _timeout=120, **args)

    def edit(
        self,
        path: str,
        old: str,
        new: str,
        *,
        expect: int = 1,
        all: bool = False,
        force: bool = False,
    ) -> dict[str, Any]:
        """Replace exact occurrences of ``old`` with ``new`` in a remote text file.

        By default the occurrence count must equal ``expect`` (1); with ``all=True``
        every occurrence is replaced. Returns ``replacements``, ``first_line`` and a
        unified-diff ``preview``. Line endings and file mode are preserved; the
        replacement is atomic (the inode changes, hard links are not preserved).
        A protected path (``protected_paths``) is refused unless ``force=True``.
        """
        self._check_protected(path, force=force, action="edit")
        r = self.call("edit", path=path, old=old, new=new, expect=expect, all=all, _timeout=120)
        self.registry.audit("edit", path=r["path"], replacements=r["replacements"])
        return r

    def diff(
        self,
        path: str,
        content: str | bytes | None = None,
        *,
        path_b: str | None = None,
        context: int = 3,
        max_lines: int = 500,
    ) -> dict[str, Any]:
        """Unified diff of a remote text file against ``content`` or another remote file."""
        args: dict[str, Any] = {
            "path": path,
            "path_b": path_b,
            "context": context,
            "max_lines": max_lines,
        }
        if isinstance(content, bytes):
            args["content_b64"] = base64.b64encode(content).decode("ascii")
        elif content is not None:
            args["content"] = content
        return self.call("diff", _timeout=120, **args)

    def mkdir(self, path: str) -> dict[str, Any]:
        return self.call("mkdir", path=path)

    def rm(
        self,
        path: str,
        *,
        recursive: bool = False,
        force: bool = False,
        confirm: bool = False,
    ) -> dict[str, Any]:
        """Remove a remote file (or, with ``recursive``, a directory tree).

        Refuses a configured protected path unless ``force=True``; when ``rm`` is listed in the
        host's ``confirm``, requires ``confirm=True`` (else raises ``ConfirmationRequired``). The
        stub adds its own guards (shared roots, shallow recursive deletes).
        """
        self._check_protected(path, force=force, action="remove")
        what = f"remove {'-r ' if recursive else ''}{path}"
        self._require_confirm("rm", confirm, what)
        r = self.call("rm", path=path, recursive=recursive, _timeout=300)
        self.registry.audit("rm", path=path, recursive=recursive)
        return r

    def glob(
        self,
        path: str,
        pattern: str = "*",
        *,
        limit: int = 500,
        max_depth: int = 10,
        type: str | None = None,
        hidden: bool = False,
    ) -> dict[str, Any]:
        return self.call(
            "glob",
            path=path,
            pattern=pattern,
            limit=limit,
            max_depth=max_depth,
            type=type,
            hidden=hidden,
            _timeout=300,
        )

    def grep(
        self,
        pattern: str,
        path: str,
        *,
        glob: str | None = None,
        max_matches: int = 200,
        ignore_case: bool = False,
        max_depth: int = 10,
        context: int = 0,
        hidden: bool = False,
    ) -> dict[str, Any]:
        return self.call(
            "grep",
            pattern=pattern,
            path=path,
            glob=glob,
            max_matches=max_matches,
            ignore_case=ignore_case,
            max_depth=max_depth,
            context=context,
            hidden=hidden,
            _timeout=300,
        )

    # -- commands -------------------------------------------------------------------------------
    def run(
        self,
        cmd: str | list[str],
        *,
        cwd: str | None = None,
        timeout: int = 60,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
        login: bool = False,
        max_output: int = 65536,
        cancel_on_timeout: bool = True,
        stream: bool = False,
        on_chunk: Callable[[str, str], None] | None = None,
        compute: bool = False,
        template: str | None = None,
        partition: str | None = None,
        time: str | None = None,
        cpus: int | None = None,
        mem: str | None = None,
        gpus: int | None = None,
        account: str | None = None,
        queue_timeout: int = 600,
    ) -> dict[str, Any]:
        """Run a command on the login node (bounded output), or on a compute node with
        ``compute=True`` (via ``srun``).

        ``cmd`` may be an argv list or a shell string. When ``cancel_on_timeout`` (the default)
        the remote process group is killed if the client-side call times out (or is
        interrupted), so nothing lingers.

        With ``stream=True`` (or an ``on_chunk`` callback) the login-node run streams output as
        it arrives: ``on_chunk(stream, text)`` is invoked for each ``stdout``/``stderr`` piece
        and the final result dict (rc + bounded captured output) is returned. Streaming is
        login-node only; it is ignored when ``compute=True``.

        With ``compute=True`` the command runs under ``srun`` on an allocated node. Resources
        come from ``template`` (a config template) then the explicit ``partition``/``time``/
        ``cpus``/``mem``/``gpus``/``account`` kwargs (account falls back to the host default).
        ``queue_timeout`` bounds how long to wait for the allocation; the result is
        ``{started: true, rc, stdout, stderr, node, elapsed}`` for a run that got a node, or
        ``{started: false, reason}`` if it never left the queue. ``allow_run`` is enforced
        exactly as for a login-node ``run``.
        """
        self._enforce_run_policy(cmd)
        if compute:
            return self._run_compute(
                cmd,
                template=template,
                partition=partition,
                time=time,
                cpus=cpus,
                mem=mem,
                gpus=gpus,
                account=account,
                queue_timeout=queue_timeout,
                cwd=cwd,
                env=env,
                stdin=stdin,
                login=login,
                max_output=max_output,
                cancel_on_timeout=cancel_on_timeout,
            )
        args: dict[str, Any] = {
            "cwd": cwd,
            "timeout": timeout,
            "env": env,
            "stdin": stdin,
            "max_output": max_output,
        }
        if isinstance(cmd, str):
            args["cmd"] = cmd
            args["login"] = login
        else:
            args["argv"] = list(cmd)
        if stream or on_chunk is not None:
            return self._run_stream(args, timeout=timeout, on_chunk=on_chunk)
        return self.call("run", _timeout=timeout + 15, _cancel_on_timeout=cancel_on_timeout, **args)

    def _run_stream(
        self,
        args: dict[str, Any],
        *,
        timeout: int,
        on_chunk: Callable[[str, str], None] | None,
    ) -> dict[str, Any]:
        """Drive a streaming login-node ``run``: forward each chunk to ``on_chunk`` and return
        the final result dict (rc + bounded captured output)."""
        result: dict[str, Any] = {}
        gen = self.session.call_stream("run", args, timeout=timeout + 15)
        try:
            for frame in gen:
                if frame.get("done"):
                    result = frame.get("result") or {}
                    break
                chunk = frame.get("chunk") or {}
                if on_chunk is not None:
                    on_chunk(str(chunk.get("stream", "stdout")), str(chunk.get("data", "")))
        finally:
            gen.close()  # break/interrupt -> cancel the remote run, never leak the rid
        return result

    def follow(
        self,
        path: str,
        *,
        offset: int = 0,
        idle_timeout: int = 60,
        max_bytes_per_chunk: int = 65536,
        timeout: float | None = None,
    ) -> Iterator[str]:
        """Tail a remote file, yielding appended text as it arrives (via the stub ``follow`` op).

        Seeks to ``offset`` (negative = from the end), then yields each appended piece until
        ``idle_timeout`` seconds pass with no new data or the caller stops iterating (which
        cancels the remote tail promptly). ``timeout`` bounds the whole client-side stream
        (``None`` = follow until idle/cancel, as ``tail -f`` wants).
        """
        gen = self.session.call_stream(
            "follow",
            {
                "path": path,
                "offset": offset,
                "idle_timeout": idle_timeout,
                "max_bytes_per_chunk": max_bytes_per_chunk,
            },
            timeout=timeout,
        )
        try:
            for frame in gen:
                if frame.get("done"):
                    return
                data = str((frame.get("chunk") or {}).get("data", ""))
                if data:
                    yield data
        finally:
            gen.close()

    def _resolve_compute_resources(
        self,
        *,
        template: str | None,
        partition: str | None,
        time: str | None,
        cpus: int | None,
        mem: str | None,
        gpus: int | None,
        account: str | None,
    ) -> dict[str, Any]:
        """Merge template options (< explicit kwargs) into srun resource fields."""
        res: dict[str, Any] = {}
        if template is not None:
            opts = self.host.resolve_template(template).options  # ConfigError if unknown/cyclic
            res["partition"] = opts.get("partition")
            res["time"] = opts.get("time")
            res["cpus"] = opts.get("cpus_per_task", opts.get("cpus"))
            res["mem"] = opts.get("mem")
            res["gpus"] = opts.get("gpus_per_node", opts.get("gpus"))
            res["account"] = opts.get("account")
        for k, v in (
            ("partition", partition),
            ("time", time),
            ("cpus", cpus),
            ("mem", mem),
            ("gpus", gpus),
            ("account", account),
        ):
            if v is not None:
                res[k] = v
        if not res.get("account") and self.host.account:
            res["account"] = self.host.account
        return {k: v for k, v in res.items() if v is not None}

    def _run_compute(
        self,
        cmd: str | list[str],
        *,
        template: str | None,
        partition: str | None,
        time: str | None,
        cpus: int | None,
        mem: str | None,
        gpus: int | None,
        account: str | None,
        queue_timeout: int,
        cwd: str | None,
        env: dict[str, str] | None,
        stdin: str | None,
        login: bool,
        max_output: int,
        cancel_on_timeout: bool,
    ) -> dict[str, Any]:
        res = self._resolve_compute_resources(
            template=template,
            partition=partition,
            time=time,
            cpus=cpus,
            mem=mem,
            gpus=gpus,
            account=account,
        )
        walltime = slurm.walltime_to_seconds(res.get("time")) or 3600
        total = int(queue_timeout) + int(walltime) + 30
        args: dict[str, Any] = dict(res)
        args.update(
            {
                "cwd": cwd,
                "env": env,
                "stdin": stdin,
                "queue_timeout": queue_timeout,
                "timeout": total,
                "max_output": max_output,
                "login": login,  # honoured for both cmd and argv forms (module loads)
            }
        )
        if isinstance(cmd, str):
            args["cmd"] = cmd
        else:
            args["argv"] = list(cmd)
        r = self.call("srun", _timeout=total + 30, _cancel_on_timeout=cancel_on_timeout, **args)
        self.registry.audit(
            "srun",
            started=r.get("started"),
            node=r.get("node"),
            rc=r.get("rc"),
            partition=res.get("partition"),
        )
        return r

    # -- diagnose -------------------------------------------------------------------------------
    def _safe_user(self) -> str | None:
        try:
            return self.user
        except RemoteSlurmError:
            return None

    def diagnose(self, job_id: str, *, tail: int = 60) -> dict[str, Any]:
        """Explain what happened to a job in one call: status, tails, steps, sync marker,
        a plain-English ``verdict`` and actionable ``hints``.

        Gathers the merged status, the submit script (<=16 KB), stdout/stderr tails, the
        sacct step table and any ``.remoteslurm-sync.json`` marker, runs the pure
        :data:`~remoteslurm.slurm.DIAGNOSTICS` rules, and caps the whole payload at
        ~64 KB (``truncated`` flags a cut).
        """
        slurm.parse_job_id(job_id)
        st = self.job_status(job_id, refresh=True)
        rec = self.registry.get(job_id)

        script: str | None = None
        script_path = st.script_path or (rec.script_path if rec else None)
        if script_path:
            try:
                r = self.read(script_path, max_bytes=16 * 1024)
                script = None if r.get("binary") else r.get("content")
            except RemoteSlurmError:
                script = None

        stdout_tail = self._tail_or_empty(job_id, tail, "stdout")
        stderr_tail = ""
        if st.stderr_path and st.stderr_path != st.stdout_path:
            stderr_tail = self._tail_or_empty(job_id, tail, "stderr")

        steps: list[dict[str, Any]] = []
        req_mem: int | None = None
        max_rss = st.max_rss
        state_raw: str | None = None
        try:
            acct = self.sacct([job_id], all_steps=True)  # steps carry MaxRSS for the OOM rule
            a = acct.get(job_id.split("_")[0]) or acct.get(job_id)
            if a:
                steps = a.get("steps", []) or []
                req_mem = slurm._parse_mem(a.get("req_mem") or "")
                if a.get("max_rss") is not None:
                    max_rss = a["max_rss"]
                state_raw = a.get("state_raw")
        except RemoteSlurmError:
            pass

        sync = self._read_sync_marker(st.workdir)

        cancelled_by: str | None = None
        if state_raw:
            m = re.search(r"CANCELLED by (\S+)", state_raw)
            if m:
                cancelled_by = m.group(1)

        ctx = slurm.DiagContext(
            state=st.state,
            exit_code=st.exit_code,
            exit_code_raw=st.exit_code_raw,
            reason=st.reason,
            max_rss=max_rss,
            req_mem=req_mem,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            cancelled_by=cancelled_by,
            whoami=self._safe_user(),
            elapsed=st.elapsed,
            time_limit=st.time_limit,
        )
        verdict, hints = slurm.diagnose_job(ctx)
        if st.state == "PENDING":
            try:
                est = self.estimate_start(job_id)
            except RemoteSlurmError:
                est = None
            if est and est.get("est_start"):
                hint = f"estimated start: {est['est_start']}"
                if est.get("reason"):
                    hint += f" (scheduler reason: {est['reason']})"
                hints.append(hint)
        if sync:
            hints.append(_sync_hint(sync))

        out: dict[str, Any] = {
            "job_id": job_id,
            "status": st.to_dict(),
            "script": script,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "steps": steps,
            "sync": sync,
            "verdict": verdict,
            "hints": hints,
            "truncated": False,
        }
        return slurm.cap_diagnostic_fields(out)

    def _tail_or_empty(self, job_id: str, tail: int, stream: str) -> str:
        try:
            return str(self.job_output(job_id, tail=tail, stream=stream).get("content", ""))
        except RemoteSlurmError:
            return ""

    # -- queue intelligence (F1) --------------------------------------------------------------
    def estimate_start(self, job_id: str) -> dict[str, Any] | None:
        """The scheduler's start-time estimate for a pending job (``{job_id, est_start, reason}``).

        Returns ``None`` when the tool is unavailable, the job is not pending, or no row matches.
        """
        slurm.parse_job_id(job_id)
        res = self.call("squeue_start", jobs=[job_id])
        if res.get("rc") != 0:
            return None
        base = job_id.split("_")[0]
        for r in slurm.parse_squeue_start(res.get("stdout", "")):
            if r["job_id"] == job_id or r["job_id"].split("_")[0] == base:
                return r
        return None

    def _fairshare(self) -> list[dict[str, Any]]:
        res = self.call("sshare")
        if res.get("rc") != 0:
            return []
        return slurm.parse_sshare(res.get("stdout", ""))

    def _qos(self) -> list[dict[str, Any]]:
        res = self.call("qos")
        if res.get("rc") != 0:
            return []
        return slurm.parse_qos(res.get("stdout", ""))

    def _assoc(self) -> list[dict[str, Any]]:
        res = self.call("assoc", user=self._safe_user())
        if res.get("rc") != 0:
            return []
        return slurm.parse_assoc(res.get("stdout", ""))

    def _pending_estimates(self) -> list[dict[str, Any]]:
        """My PENDING jobs with the scheduler's start estimate (one batched squeue_start)."""
        try:
            rows = self.squeue(refresh=True)
        except RemoteSlurmError:
            return []
        # Collapse a pending array to one entry keyed by its base id (parse_squeue expands a
        # collapsed `123_[0-9999]` bracket into one pseudo-row per task, which would otherwise
        # emit thousands of rows and thousands of squeue_start ids).
        seen: set[str] = set()
        pending: list[dict[str, Any]] = []
        for r in rows:
            if r.get("state") != "PENDING":
                continue
            key = r.get("array_base") or r["job_id"]
            if key in seen:
                continue
            seen.add(key)
            r = dict(r)
            r["job_id"] = key  # query/report the base id for an array
            pending.append(r)
        if not pending:
            return []
        est: dict[str, dict[str, Any]] = {}
        try:
            res = self.call("squeue_start", jobs=[r["job_id"] for r in pending])
            if res.get("rc") == 0:
                for e in slurm.parse_squeue_start(res.get("stdout", "")):
                    est[e["job_id"]] = e
        except RemoteSlurmError:
            pass
        out: list[dict[str, Any]] = []
        for r in pending:
            e = est.get(r["job_id"], {})
            reason = e.get("reason") or (
                r["reason"] if r.get("reason") not in ("None", "", None) else None
            )
            out.append(
                {
                    "job_id": r["job_id"],
                    "name": r.get("name") or None,
                    "est_start": e.get("est_start"),
                    "reason": reason,
                }
            )
        return out

    def queue_info(self) -> dict[str, Any]:
        """One-call snapshot for ``rslurm queue``: partitions, my accounts/QOS + limits,
        fair-share, and my pending jobs with start estimates.

        Every section degrades to an empty list when its Slurm tool is missing (site formats
        vary); nothing here raises for an absent ``sshare``/``sacctmgr``.
        """
        try:
            partitions = self.sinfo()
        except RemoteSlurmError:
            partitions = []
        return {
            "partitions": partitions,
            "fairshare": self._fairshare(),
            "qos": self._qos(),
            "accounts": self._assoc(),
            "pending": self._pending_estimates(),
        }

    def _quota_paths(self) -> list[str]:
        """Filesystems to ``df`` when there is no ``quota_command``: home + scratch/project."""
        info: dict[str, Any] = {}
        try:
            info = self.info()
        except RemoteSlurmError:
            pass
        env = info.get("env", {}) if info else {}
        paths: list[str] = []
        for key in ("HOME", "SCRATCH", "PROJECT"):
            v = env.get(key)
            if v and v not in paths:
                paths.append(v)
        home = info.get("home")
        if not paths and home:
            paths.append(home)
        return paths

    def quota(self) -> dict[str, Any]:
        """Disk usage/quota. Uses the host's ``quota_command`` (Alliance: ``diskusage_report
        --per_user``) when set, else ``df -h`` of home/scratch/project.

        Returns ``{available, source, usage, raw, ...}``; ``available`` is false when the tool is
        missing (``usage`` empty) so an agent can fall back gracefully.
        """
        cmd = self.host.quota_command
        if cmd:
            # Run in a login shell so module-provided wrappers (Alliance's `diskusage_report`
            # is a shell function) resolve; the bare binary can report different numbers.
            res = self.call("quota", command_shell=cmd, _timeout=120)
            available = res.get("rc") == 0 and not res.get("missing")
            usage = slurm.parse_diskusage_report(res.get("stdout", "")) if available else []
            return {
                "available": available,
                "source": "command",
                "command": cmd,
                "usage": usage,
                "raw": res.get("stdout", ""),
                "stderr": "" if available else res.get("stderr", ""),
            }
        paths = self._quota_paths()
        res = self.call("quota", paths=paths, _timeout=90)
        usage = slurm.parse_df(res.get("stdout", ""))
        return {
            "available": bool(usage),
            "source": "df",
            "paths": paths,
            "usage": usage,
            "raw": res.get("stdout", ""),
            "stderr": "" if usage else res.get("stderr", ""),
        }

    # -- housekeeping (F3) --------------------------------------------------------------------
    def clean(self, *, older_than_days: int = 30, dry_run: bool = False) -> dict[str, Any]:
        """Remove generated sbatch scripts / sweep files older than the cutoff.

        Scans the host ``script_dir`` (default ``~/.remoteslurm/scripts`` and
        ``~/.remoteslurm/sweeps``), deleting files whose mtime is older than
        ``older_than_days`` via the stub's ``glob`` + ``rm``. Protected paths are skipped
        (counted under ``kept``). ``dry_run`` reports what *would* be removed without touching
        anything.
        """
        from .errors import NotFound

        cutoff = time.time() - older_than_days * 86400
        dirs = (
            [self.host.script_dir]
            if self.host.script_dir
            else ["~/.remoteslurm/scripts", "~/.remoteslurm/sweeps"]
        )
        removed: list[str] = []
        kept = 0
        scanned: list[str] = []
        for d in dirs:
            try:
                g = self.glob(d, "*", limit=10000, max_depth=20, hidden=False, type="file")
            except NotFound:
                continue
            except RemoteSlurmError:
                continue
            if g.get("root"):
                scanned.append(g["root"])
            for m in g.get("matches", []):
                path = m.get("path")
                mtime = m.get("mtime")
                if not path or mtime is None or mtime >= cutoff:
                    kept += 1
                    continue
                if _matches_protected(path, self.host.protected_paths, self._cached_home()):
                    kept += 1
                    continue
                if dry_run:
                    removed.append(path)
                    continue
                try:
                    self.rm(path)
                    removed.append(path)
                except RemoteSlurmError:
                    kept += 1
        self.registry.audit(
            "clean", dry_run=dry_run, removed=len(removed), older_than_days=older_than_days
        )
        return {
            "dry_run": dry_run,
            "removed": removed,
            "count": len(removed),
            "kept": kept,
            "dirs": scanned,
            "older_than_days": older_than_days,
        }

    def _read_sync_marker(self, workdir: str | None) -> dict[str, Any] | None:
        if not workdir:
            return None
        try:
            text = self.read_text(workdir.rstrip("/") + "/.remoteslurm-sync.json", max_bytes=8192)
            return dict(json.loads(text))
        except (RemoteSlurmError, ValueError):
            return None


def _sync_hint(marker: dict[str, Any]) -> str:
    rev = marker.get("local_git_rev") or "?"
    rev = rev[:12] if isinstance(rev, str) else "?"
    dirty = " (working tree was dirty)" if marker.get("local_dirty") else ""
    when = marker.get("pushed_at") or "?"
    return f"code was pushed {when} at git rev {rev}{dirty}; re-sync if you changed files since"
