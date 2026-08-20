"""Project sync: rsync a configured local project tree to/from the cluster.

This is a *client-side* module — rsync runs on the laptop, talking to the remote over the
user's ssh alias (a fresh channel with ``ControlMaster=no``; the master keeps MFA warm).
Nothing here is a stub op.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import HostConfig, ProjectConfig
from .errors import ConfigError, InvalidArgument, RemoteSlurmError, RemoteTimeout, TooLarge

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .cluster import Cluster

ENV_SSH = "REMOTESLURM_SYNC_SSH"
MARKER = ".remoteslurm-sync.json"
DEFAULT_SSH_ARGS = ["ssh", "-o", "ControlMaster=no", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR"]
DEFAULT_SSH = " ".join(DEFAULT_SSH_ARGS)
MIN_RSYNC = (3, 1)
BUILTIN_EXCLUDES = [".git/", ".venv/", "__pycache__/", "*.pyc", ".DS_Store", MARKER]


# ----------------------------------------------------------------------- project resolution
def resolve_project(host: HostConfig, name: str | None, cwd: Path) -> ProjectConfig:
    """Pick a project: explicit ``name``, else the one whose ``local`` contains ``cwd``."""
    available = ", ".join(sorted(host.projects)) or "none"
    if name is not None:
        if name in host.projects:
            return host.projects[name]
        raise ConfigError(
            f"unknown project {name!r} for host {host.name}",
            action=f"available projects: {available} "
            f"(define [hosts.{host.name}.projects.{name}] in the config)",
        )
    if not host.projects:
        raise ConfigError(
            f"no projects configured for host {host.name}",
            action=f"add [hosts.{host.name}.projects.NAME] with local/remote to the config",
        )
    try:
        cwd_r = cwd.resolve()
    except OSError:
        cwd_r = cwd
    best: ProjectConfig | None = None
    best_depth = -1
    for p in host.projects.values():
        try:
            lp = p.local_path().resolve()
        except OSError:  # pragma: no cover - unresolvable local path
            continue
        if cwd_r == lp or cwd_r.is_relative_to(lp):
            if len(lp.parts) > best_depth:
                best, best_depth = p, len(lp.parts)
    if best is None:
        raise ConfigError(
            f"current directory is not inside any project of host {host.name}",
            action=f"pass a project name; available projects: {available}",
        )
    return best


# ----------------------------------------------------------------------- local rsync lookup
def _version_of(rsync: str) -> tuple[int, int] | None:
    try:
        out = subprocess.run(
            [rsync, "--version"], capture_output=True, text=True, timeout=10
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    m = re.search(r"version\s+(\d+)\.(\d+)", out)
    return (int(m.group(1)), int(m.group(2))) if m else None


@lru_cache(maxsize=1)
def find_rsync() -> tuple[str, tuple[int, int]] | None:
    """Best local rsync as ``(path, (major, minor))``, preferring >= 3.1.

    macOS ships openrsync/2.6.9 in ``/usr/bin``; a Homebrew rsync further down (or off)
    ``$PATH`` is preferred when the one on ``$PATH`` is too old.
    """
    candidates = []
    if w := shutil.which("rsync"):
        candidates.append(w)
    for p in ("/opt/homebrew/bin/rsync", "/usr/local/bin/rsync"):
        if p not in candidates and os.path.exists(p):
            candidates.append(p)
    fallback: tuple[str, tuple[int, int]] | None = None
    for c in candidates:
        v = _version_of(c)
        if v is None:
            continue
        if v >= MIN_RSYNC:
            return (c, v)
        fallback = fallback or (c, v)
    return fallback


def rsync_version() -> tuple[int, int] | None:
    """Version of the rsync :func:`sync` would use, or ``None`` if none is installed."""
    r = find_rsync()
    return r[1] if r else None


def _require_rsync() -> str:
    r = find_rsync()
    if r is None or r[1] < MIN_RSYNC:
        found = f"found {r[0]} ({r[1][0]}.{r[1][1]})" if r else "none found"
        raise InvalidArgument(
            f"sync needs local rsync >= {MIN_RSYNC[0]}.{MIN_RSYNC[1]} ({found})",
            action="brew install rsync",
        )
    return r[0]


# ----------------------------------------------------------------------- remote expansion
def transport_ssh_opts(cluster: Cluster) -> list[str] | None:
    """Reconstruct rsync's ``-e`` ssh args from the cluster's transport so rsync reuses the
    same ControlMaster (and any ProxyJump/extra opts) remoteslurm itself uses, instead of
    depending solely on the user's ~/.ssh/config."""
    t = cluster.transport
    alias = getattr(t, "alias", None)
    if alias is None:
        return None
    opts = list(DEFAULT_SSH_ARGS)
    control_path = getattr(t, "control_path", None)
    if control_path:
        opts += ["-o", f"ControlPath={control_path}"]
    for extra in getattr(t, "extra_ssh_opts", None) or []:
        opts.append(extra)
    return opts


def expand_remote(cluster: Cluster, path: str) -> str:
    """Expand ``~``/``$VARS`` on the remote, shell-free (no command substitution/globbing)."""
    return str(cluster.call("expandpath", path=path)["path"])


# ----------------------------------------------------------------------- command / parsing
def build_rsync_cmd(
    rsync: str,
    local: str,
    remote: str,
    alias: str,
    *,
    pull: bool = False,
    dry_run: bool = False,
    delete: bool = False,
    excludes: list[str] | None = None,
    ssh_cmd: list[str] | None = None,
    ssh_opts: list[str] | None = None,
) -> list[str]:
    """The full rsync argv (contents of ``local`` <-> contents of ``remote``)."""
    cmd = [rsync, "-az", "-s", "--itemize-changes", "--stats"]
    if dry_run:
        cmd.append("-n")
    if delete:
        cmd.append("--delete")
    for pat in [*BUILTIN_EXCLUDES, *(excludes or [])]:
        cmd.append(f"--filter=- {pat}")
    if ssh_cmd is not None:
        rsh = shlex.join(ssh_cmd)
    elif env_rsh := os.environ.get(ENV_SSH):
        rsh = env_rsh
    else:
        rsh = shlex.join(ssh_opts or DEFAULT_SSH_ARGS)
    cmd += ["-e", rsh]
    src_local = str(local).rstrip("/") + "/"
    remote_spec = f"{alias}:{remote.rstrip('/') or '/'}"
    if pull:
        cmd += [remote_spec + "/", str(local)]
    else:
        cmd += [src_local, remote_spec]
    return cmd


_ITEMIZE = re.compile(r"^([<>ch.])([fdLDS])([.+cstpoguaxbn?]{9}) (.+)$")
_ITEMIZE_ISH = re.compile(r"^[<>ch.][fdLDS][.+]")
_STATS_FILES = re.compile(r"^Number of (?:regular )?files transferred:\s*([\d,]+)")
_STATS_BYTES = re.compile(r"^Total transferred file size:\s*([\d,]+)")


def parse_itemize(text: str) -> dict[str, Any] | None:
    """Parse ``--itemize-changes --stats`` output into counts.

    Returns ``{created, updated, deleted, files, bytes}`` or ``None`` when the output is not
    recognizably rsync >= 3.x (e.g. macOS openrsync/2.6.9, whose itemize field is 9 characters
    instead of 11, or arbitrary garbage). Parse failures must degrade, never raise.
    """
    created = updated = deleted = 0
    files: int | None = None
    nbytes: int | None = None
    for raw in text.splitlines():
        line = raw.rstrip("\r")
        if not line.strip():
            continue
        if line.startswith("*deleting"):
            deleted += 1
            continue
        if m := _STATS_FILES.match(line):
            files = int(m.group(1).replace(",", ""))
            continue
        if m := _STATS_BYTES.match(line):
            nbytes = int(m.group(1).replace(",", ""))
            continue
        if m := _ITEMIZE.match(line):
            yx, attrs = m.group(1) + m.group(2), m.group(3)
            if attrs == "+++++++++":
                created += 1
            elif yx in (">f", "<f"):
                updated += 1
            continue
        if _ITEMIZE_ISH.match(line):
            # Looks like an itemize line but not the 11-char >=3.x format (2.6.9 is 9 chars).
            return None
    if files is None or nbytes is None:
        return None
    return {
        "created": created,
        "updated": updated,
        "deleted": deleted,
        "files": files,
        "bytes": nbytes,
    }


# ----------------------------------------------------------------------- git (best effort)
def _git_info(local: Path) -> tuple[str | None, bool | None]:
    """(HEAD rev, dirty?) of the local tree, or (None, None) when not a git repo/no git."""
    try:
        rev = subprocess.run(
            ["git", "-C", str(local), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if rev.returncode != 0:
            return None, None
        status = subprocess.run(
            ["git", "-C", str(local), "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        dirty = bool(status.stdout.strip()) if status.returncode == 0 else None
        return rev.stdout.strip(), dirty
    except (OSError, subprocess.SubprocessError):
        return None, None


# ----------------------------------------------------------------------- the sync itself
def _run_rsync(cmd: list[str], timeout: float) -> tuple[int, str, str]:
    # LC_ALL=C keeps the --stats block in English so parse_itemize (and thus the size guard)
    # works under any user locale.
    env = dict(os.environ, LC_ALL="C", LANG="C")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=False,
        env=env,
    )
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover - stubborn rsync
            proc.kill()
        raise RemoteTimeout(
            f"rsync did not finish within {timeout:.0f} s",
            action="re-run with a larger --timeout, or tighten excludes",
        ) from None
    except KeyboardInterrupt:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover
            proc.kill()
        raise
    return proc.returncode, out, err


def sync(
    cluster: Cluster,
    project: ProjectConfig,
    *,
    pull: bool = False,
    dry_run: bool = False,
    delete: bool = False,
    force: bool = False,
    timeout: float = 1800,
    ssh_cmd: list[str] | None = None,
) -> dict[str, Any]:
    """rsync ``project`` between the laptop and ``cluster``; returns a summary dict.

    Push by default (``pull=True`` reverses direction). A non-dry run without ``force``
    first does a dry run and refuses (``too_large``) when it would move more than the host's
    ``max_sync_files``/``max_sync_bytes``. ``delete`` needs *both* ``delete=True`` here and
    ``delete = true`` on the project (double opt-in). After a successful push a
    ``.remoteslurm-sync.json`` marker (git rev, time, counts) is written to the remote root.
    """
    rsync_bin = _require_rsync()
    if delete and not project.delete:
        raise InvalidArgument(
            f"project {project.name!r} does not allow --delete",
            action=f"set delete = true under [hosts.{cluster.host.name}.projects."
            f"{project.name}] and pass --delete again",
        )
    alias = getattr(cluster.transport, "alias", None)
    ssh_opts = transport_ssh_opts(cluster)
    if not alias:
        raise InvalidArgument(
            "sync needs an ssh host (the transport has no ssh alias)",
            action="configure the host with an ssh alias; sync cannot run on transport "
            f"{cluster.transport.describe()!r}",
        )
    local = project.local_path()
    if pull:
        local.mkdir(parents=True, exist_ok=True)
    elif not local.is_dir():
        raise InvalidArgument(
            f"local project directory not found: {local}",
            action=f"check [hosts.{cluster.host.name}.projects.{project.name}] local =",
        )
    remote = expand_remote(cluster, project.remote)
    if not pull and not dry_run:
        cluster.mkdir(remote)

    def cmd_for(dry: bool) -> list[str]:
        return build_rsync_cmd(
            rsync_bin,
            str(local),
            remote,
            str(alias),
            pull=pull,
            dry_run=dry,
            delete=delete,
            excludes=list(project.exclude),
            ssh_cmd=ssh_cmd,
            ssh_opts=ssh_opts,
        )

    # Size guard: probe with a dry run before moving anything for real.
    if not dry_run and not force:
        rc, out, err = _run_rsync(cmd_for(True), timeout)
        if rc == 0:
            probe = parse_itemize(out)
            if probe is not None and (
                probe["files"] > cluster.host.max_sync_files
                or probe["bytes"] > cluster.host.max_sync_bytes
            ):
                raise TooLarge(
                    f"sync would transfer {probe['files']} files / {probe['bytes']} bytes "
                    f"(host limits: {cluster.host.max_sync_files} files / "
                    f"{cluster.host.max_sync_bytes} bytes)",
                    action="run with --dry-run to inspect, tighten exclude patterns, "
                    "or pass --force",
                    files=probe["files"],
                    bytes=probe["bytes"],
                )

    cmd = cmd_for(dry_run)
    rc, out, err = _run_rsync(cmd, timeout)
    if rc not in (0, 24):  # 24: source files vanished mid-transfer (benign)
        tail = "\n".join(err.strip().splitlines()[-5:])
        raise RemoteSlurmError(
            f"rsync failed (exit {rc}): {tail or 'no stderr'}",
            action="check the remote path and ssh alias; re-run with --dry-run",
            rc=rc,
            command=cmd,
        )
    counts = parse_itemize(out)

    marker: dict[str, Any] | None = None
    if not pull and not dry_run:
        rev, dirty = _git_info(local)
        marker = {
            "pushed_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "local_git_rev": rev,
            "local_dirty": dirty,
            "files": counts["files"] if counts else None,
            "bytes": counts["bytes"] if counts else None,
            "project": project.name,
        }
        cluster.write(remote.rstrip("/") + "/" + MARKER, json.dumps(marker, indent=2) + "\n")

    return {
        "project": project.name,
        "direction": "pull" if pull else "push",
        "dry_run": dry_run,
        "local": str(local),
        "remote": remote,
        "counts": counts,
        "files": counts["files"] if counts else None,
        "bytes": counts["bytes"] if counts else None,
        "rc": rc,
        "command": cmd,
        "marker": marker,
    }


def read_marker(cluster: Cluster, project: ProjectConfig) -> dict[str, Any] | None:
    """Best-effort read of a project's remote sync marker (``None`` when absent)."""
    try:
        remote = expand_remote(cluster, project.remote)
        text = cluster.read_text(remote.rstrip("/") + "/" + MARKER)
        return dict(json.loads(text))
    except (RemoteSlurmError, ValueError):
        return None


__all__ = [
    "BUILTIN_EXCLUDES",
    "DEFAULT_SSH",
    "DEFAULT_SSH_ARGS",
    "transport_ssh_opts",
    "ENV_SSH",
    "MARKER",
    "build_rsync_cmd",
    "expand_remote",
    "find_rsync",
    "parse_itemize",
    "read_marker",
    "resolve_project",
    "rsync_version",
    "sync",
]
