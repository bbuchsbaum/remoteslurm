"""``Cluster``: the public, synchronous, thread-safe API over one remote host."""

from __future__ import annotations

import base64
import json
import re
import threading
from typing import Any

from . import slurm
from .config import Config, HostConfig
from .errors import InvalidArgument, PermissionDenied, RemoteSlurmError
from .jobs import SlurmOps, read_learned_notes
from .session import DEFAULT_TIMEOUT, Session
from .transport import LocalTransport, SSHTransport, Transport

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
    def call(self, op: str, *, _timeout: float | None = DEFAULT_TIMEOUT, **args: Any) -> Any:
        return self.session.call(op, args, timeout=_timeout)

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
    ) -> dict[str, Any]:
        args: dict[str, Any] = {"path": path, "append": append, "mkdirs": mkdirs, "mode": mode}
        if isinstance(content, bytes):
            args["content_b64"] = base64.b64encode(content).decode("ascii")
        else:
            args["content"] = content
        return self.call("write", _timeout=120, **args)

    def edit(
        self, path: str, old: str, new: str, *, expect: int = 1, all: bool = False
    ) -> dict[str, Any]:
        """Replace exact occurrences of ``old`` with ``new`` in a remote text file.

        By default the occurrence count must equal ``expect`` (1); with ``all=True``
        every occurrence is replaced. Returns ``replacements``, ``first_line`` and a
        unified-diff ``preview``. Line endings and file mode are preserved; the
        replacement is atomic (the inode changes, hard links are not preserved).
        """
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

    def rm(self, path: str, *, recursive: bool = False) -> dict[str, Any]:
        return self.call("rm", path=path, recursive=recursive, _timeout=300)

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
    ) -> dict[str, Any]:
        """Run a command on the login node (bounded output).

        ``cmd`` may be an argv list or a shell string.
        """
        if not self.host.allow_run:
            raise PermissionDenied(
                f"`run` is disabled for host {self.host.name}",
                action="set allow_run = true in the host config",
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
        return self.call("run", _timeout=timeout + 15, **args)

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
            acct = self.sacct([job_id])
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
