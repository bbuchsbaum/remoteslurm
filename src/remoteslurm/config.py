"""User configuration: ``~/.config/remoteslurm/config.toml``.

A host that is not in the config is still usable: it is treated as a bare SSH alias with the
conservative ``mfa = true`` default. Users can run ``remoteslurm connect <host>`` explicitly to
establish any authentication or ControlMaster session required by their site.
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import ConfigError

ENV_CONFIG = "REMOTESLURM_CONFIG"
ENV_DEFAULT_HOST = "REMOTESLURM_DEFAULT_HOST"
ENV_STATE_DIR = "REMOTESLURM_STATE_DIR"
QUOTA_FORMATS = {"raw", "pairs", "df"}
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def config_path() -> Path:
    if p := os.environ.get(ENV_CONFIG):
        return Path(p).expanduser()
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".config"
    return base / "remoteslurm" / "config.toml"


def state_dir() -> Path:
    if p := os.environ.get(ENV_STATE_DIR):
        return Path(p).expanduser()
    xdg = os.environ.get("XDG_STATE_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".local" / "state"
    return base / "remoteslurm"


@dataclass
class ProjectConfig:
    """A local↔remote directory pair kept in sync with rsync (``rslurm sync``)."""

    name: str
    local: str
    remote: str
    exclude: list[str] = field(default_factory=list)  # rsync filter patterns
    delete: bool = False  # allow --delete on push (still needs delete=True on the call)

    @classmethod
    def from_dict(cls, name: str, d: dict[str, Any]) -> ProjectConfig:
        known = {f for f in cls.__dataclass_fields__ if f != "name"}
        unknown = set(d) - known
        if unknown:
            raise ConfigError(f"unknown key(s) in project {name!r}: {sorted(unknown)}")
        try:
            return cls(name=name, **d)
        except TypeError as e:
            raise ConfigError(f"bad config for project {name!r}: {e}") from e

    def local_path(self) -> Path:
        return Path(os.path.expandvars(self.local)).expanduser()


# Keys inside a ``[hosts.X.templates.NAME]`` table that are *not* sbatch options.
_TEMPLATE_META_KEYS = ("preamble", "epilogue", "inherit")

# Files/globs `write`/`edit`/`rm`/`put`/`sync --delete` refuse to touch without an explicit
# force. Matched client-side against both the raw path and its `~`-expansion (see
# ``Cluster._check_protected``).
DEFAULT_PROTECTED_PATHS = [
    "~/.ssh/**",
    "~/.bashrc",
    "~/.bash_profile",
    "~/.cache/remoteslurm/**",
]

# When ``allow_run = "safe"`` only argv whose ``argv[0]`` basename matches one of these
# (fnmatch) patterns may run. Deliberately covers the common scientific / cluster tools.
DEFAULT_RUN_ALLOWLIST = [
    "python*",
    "python3",
    "Rscript",
    "git",
    "ls",
    "cat",
    "head",
    "tail",
    "wc",
    "du",
    "df",
    "rsync",
    "module",
    "squeue",
    "sacct",
    "sinfo",
    "scontrol",
    "scancel",
    "bash",
    "sh",
    "echo",
    "cp",
    "mv",
    "mkdir",
]


def _normalize_allow_run(v: Any, host: str) -> bool | str:
    """Accept ``true``/``false`` (bool) or the string ``"safe"`` (also ``"true"``/``"false"``)."""
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s == "safe":
            return "safe"
        if s in ("true", "yes", "on"):
            return True
        if s in ("false", "no", "off"):
            return False
    raise ConfigError(
        f'[hosts.{host}] allow_run must be true, false or "safe", not {v!r}',
        action='use allow_run = true | false | "safe"',
    )


@dataclass
class Template:
    """A named bundle of sbatch options plus a script preamble/epilogue.

    Everything in the ``[hosts.X.templates.NAME]`` table except ``preamble``,
    ``epilogue`` and ``inherit`` is treated as an sbatch option (``time``,
    ``partition``, ``cpus_per_task`` …). ``inherit`` names another template whose
    options/preamble/epilogue this one layers on top of (resolved with
    :meth:`HostConfig.resolve_template`).
    """

    name: str
    options: dict[str, Any] = field(default_factory=dict)
    preamble: str = ""
    epilogue: str = ""
    inherit: str | None = None

    @classmethod
    def from_dict(cls, name: str, d: dict[str, Any]) -> Template:
        options: dict[str, Any] = {}
        preamble = ""
        epilogue = ""
        inherit: str | None = None
        for k, v in d.items():
            if k == "preamble":
                if not isinstance(v, str):
                    raise ConfigError(f"template {name!r}: preamble must be a string")
                preamble = v
            elif k == "epilogue":
                if not isinstance(v, str):
                    raise ConfigError(f"template {name!r}: epilogue must be a string")
                epilogue = v
            elif k == "inherit":
                if not isinstance(v, str):
                    raise ConfigError(f"template {name!r}: inherit must be a template name")
                inherit = v
            else:
                options[k] = v
        return cls(
            name=name, options=options, preamble=preamble, epilogue=epilogue, inherit=inherit
        )

    def summary(self) -> dict[str, Any]:
        """A compact, JSON-safe view (options + whether it has a preamble/epilogue)."""
        return {
            "options": dict(self.options),
            "preamble": self.preamble,
            "epilogue": self.epilogue,
            "inherit": self.inherit,
        }


_DURATION_UNITS = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(v: str | int) -> int:
    """Seconds in an OpenSSH-style time value: ``3600``, ``90m``, ``12h``, ``1h30m``, ``2d``."""
    if isinstance(v, bool) or not isinstance(v, (int, str)):
        raise ValueError(f"expected a duration like '24h', not {v!r}")
    if isinstance(v, int):
        secs = v
    else:
        s = v.strip().lower()
        if not re.fullmatch(r"(\d+[smhdw]?)+", s):
            raise ValueError(f"expected a duration like '24h' or '1h30m', not {v!r}")
        secs = sum(int(n) * _DURATION_UNITS[u] for n, u in re.findall(r"(\d+)([smhdw]?)", s))
    if secs <= 0:
        raise ValueError(f"duration must be positive, not {v!r}")
    return secs


@dataclass
class HostConfig:
    name: str
    ssh: str  # ssh alias / hostname as understood by the user's ssh config
    mfa: bool = True  # conservative default: never try to auto-auth unless told it's safe
    python: str = "python3"
    account: str | None = None
    partition: str | None = None
    install_dir: str | None = None
    control_path: str | None = None
    control_persist: str = "12h"
    # The longest an ssh connection may live if the site cuts it after a fixed time (e.g. "24h").
    # Lets `connection` report expires_at and warn before the cut; None = no known limit.
    session_lifetime: str | int | None = None
    allow_run: bool | str = True  # true | false | "safe" (argv-only + run_allowlist)
    script_dir: str | None = None  # where generated sbatch scripts are written (remote)
    task_dir: str | None = None  # durable ensure records; default ~/.remoteslurm/tasks
    ssh_opts: list[str] = field(default_factory=list)
    defaults: dict[str, Any] = field(
        default_factory=dict
    )  # default sbatch args, e.g. {"time": "1:00:00"}
    projects: dict[str, ProjectConfig] = field(default_factory=dict)
    notes: str = ""  # free-form cluster rules shown by `info`/`notes` (agents read this first)
    templates: dict[str, Template] = field(default_factory=dict)
    # Extra login-environment variables to expose through `info` (e.g. WORK or LAB_STORAGE).
    env_vars: list[str] = field(default_factory=list)
    # Remote paths used by the portable `df -h` quota fallback. Expanded on the remote host.
    quota_paths: list[str] = field(default_factory=lambda: ["~"])
    # Optional site command and parser: raw (default), pairs (<used>/<limit>), or df.
    quota_command: str | None = None
    quota_format: str = "raw"
    # Extra remote roots that recursive `rm` must never remove wholesale. Home is always guarded.
    protected_roots: list[str] = field(default_factory=list)
    # F2: command run by `watch --notify` on a job's terminal state (MSG is substituted with the
    # message). None -> a platform default at runtime (osascript on macOS, notify-send on Linux).
    notify_command: str | None = None
    max_sync_files: int = 50_000  # sync guard: max files on a non-dry push
    max_sync_bytes: int = 2 * 1024**3  # sync guard: max bytes on a non-dry push
    # Safety rails (E2): paths `write`/`edit`/`rm`/`put`/`sync --delete` refuse without force,
    # op names that need an explicit confirmation, and the `allow_run = "safe"` executable list.
    protected_paths: list[str] = field(default_factory=lambda: list(DEFAULT_PROTECTED_PATHS))
    confirm: list[str] = field(default_factory=list)
    run_allowlist: list[str] = field(default_factory=lambda: list(DEFAULT_RUN_ALLOWLIST))
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.quota_format, str) or self.quota_format not in QUOTA_FORMATS:
            raise ConfigError(
                f"[hosts.{self.name}] quota_format must be one of {sorted(QUOTA_FORMATS)}, "
                f"not {self.quota_format!r}"
            )
        if self.session_lifetime is not None:
            try:
                parse_duration(self.session_lifetime)
            except ValueError as e:
                raise ConfigError(f"[hosts.{self.name}] session_lifetime: {e}") from e
        for field_name in ("env_vars", "quota_paths", "protected_roots"):
            values = getattr(self, field_name)
            if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
                raise ConfigError(f"[hosts.{self.name}] {field_name} must be a list of strings")
            if any(not v.strip() or "\x00" in v for v in values):
                raise ConfigError(
                    f"[hosts.{self.name}] {field_name} entries must be non-empty and contain no NUL"
                )
        if len(self.env_vars) > 64:
            raise ConfigError(f"[hosts.{self.name}] env_vars cannot contain more than 64 names")
        bad_env = [name for name in self.env_vars if not _ENV_NAME.fullmatch(name)]
        if bad_env:
            raise ConfigError(
                f"[hosts.{self.name}] env_vars contains invalid environment names: {bad_env}"
            )

    @classmethod
    def from_dict(cls, name: str, d: dict[str, Any]) -> HostConfig:
        explicit = ("name", "extra", "projects", "templates")
        known = {f for f in cls.__dataclass_fields__ if f not in explicit}
        kw = {k: v for k, v in d.items() if k in known}
        extra = {k: v for k, v in d.items() if k not in known and k not in explicit}
        projects: dict[str, ProjectConfig] = {}
        for pname, pd in (d.get("projects") or {}).items():
            if not isinstance(pd, dict):
                raise ConfigError(f"[hosts.{name}.projects.{pname}] must be a table")
            projects[pname] = ProjectConfig.from_dict(pname, pd)
        kw["projects"] = projects
        templates: dict[str, Template] = {}
        for tname, td in (d.get("templates") or {}).items():
            if not isinstance(td, dict):
                raise ConfigError(f"[hosts.{name}.templates.{tname}] must be a table")
            templates[tname] = Template.from_dict(tname, td)
        kw["templates"] = templates
        kw.setdefault("ssh", name)
        if "allow_run" in kw:
            kw["allow_run"] = _normalize_allow_run(kw["allow_run"], name)
        try:
            return cls(name=name, extra=extra, **kw)
        except TypeError as e:
            raise ConfigError(f"bad config for host {name!r}: {e}") from e

    def resolve_template(self, name: str) -> Template:
        """Return ``name`` with its ``inherit`` chain flattened (child overrides parent).

        Raises :class:`ConfigError` for an unknown template or an inheritance cycle.
        Options merge parent-first (child wins); ``preamble``/``epilogue`` take the
        nearest non-empty value down the chain.
        """
        chain: list[str] = []
        seen: set[str] = set()
        cur: str | None = name
        while cur is not None:
            if cur in seen:
                raise ConfigError(
                    f"template inheritance cycle on host {self.name!r}: "
                    + " -> ".join([*chain, cur])
                )
            seen.add(cur)
            chain.append(cur)
            t = self.templates.get(cur)
            if t is None:
                where = f"inherited by {chain[-2]!r} " if len(chain) > 1 else ""
                raise ConfigError(
                    f"unknown template {cur!r} {where}on host {self.name!r}",
                    action="available templates: " + (", ".join(sorted(self.templates)) or "none"),
                )
            cur = t.inherit
        options: dict[str, Any] = {}
        preamble = ""
        epilogue = ""
        for tname in reversed(chain):  # root -> child
            t = self.templates[tname]
            options.update(t.options)
            if t.preamble:
                preamble = t.preamble
            if t.epilogue:
                epilogue = t.epilogue
        return Template(
            name=name, options=options, preamble=preamble, epilogue=epilogue, inherit=None
        )

    def template_summaries(self) -> dict[str, dict[str, Any]]:
        """Best-effort resolved summaries of every template (cycles fall back to raw)."""
        out: dict[str, dict[str, Any]] = {}
        for name in sorted(self.templates):
            try:
                out[name] = self.resolve_template(name).summary()
            except ConfigError:
                out[name] = self.templates[name].summary()
        return out


@dataclass
class Config:
    hosts: dict[str, HostConfig] = field(default_factory=dict)
    default_host: str | None = None
    path: Path | None = None

    @classmethod
    def load(cls, path: Path | None = None) -> Config:
        path = path or config_path()
        cfg = cls(path=path)
        if path.exists():
            try:
                data = tomllib.loads(path.read_text("utf-8"))
            except (OSError, tomllib.TOMLDecodeError) as e:
                raise ConfigError(f"cannot read {path}: {e}") from e
            for name, hd in (data.get("hosts") or {}).items():
                if not isinstance(hd, dict):
                    raise ConfigError(f"[hosts.{name}] must be a table")
                cfg.hosts[name] = HostConfig.from_dict(name, hd)
            cfg.default_host = data.get("default_host")
        if env_default := os.environ.get(ENV_DEFAULT_HOST):
            cfg.default_host = env_default
        return cfg

    def host(self, name: str | None) -> HostConfig:
        if name is None:
            name = self.default_host
        if name is None:
            if len(self.hosts) == 1:
                return next(iter(self.hosts.values()))
            raise ConfigError(
                "no host given and no default_host configured",
                action=f"pass a host, or set default_host in {self.path} or ${ENV_DEFAULT_HOST}",
            )
        if name in self.hosts:
            return self.hosts[name]
        # Unknown host: use as bare ssh alias. Assume MFA (conservative) so we never hang.
        return HostConfig(name=name, ssh=name, mfa=True)


EXAMPLE_CONFIG = """\
# ~/.config/remoteslurm/config.toml
default_host = "mycluster"

[hosts.mycluster]
ssh = "mycluster"           # alias from ~/.ssh/config (ControlMaster recommended)
mfa = true                  # interactive auth: run `remoteslurm connect mycluster` once
# account = "research"      # optional default --account for sbatch
# partition = "standard"   # optional default --partition for sbatch
# python = "python3"        # remote interpreter for the stub
# control_persist = "12h"
# session_lifetime = "24h"  # only if the site cuts ssh connections after a fixed time
# allow_run = true          # true | false | "safe" (argv-only + run_allowlist) for `run`/srun
# task_dir = "$WORK/.remoteslurm/tasks"  # durable `ensure` records; shared home by default
# env_vars = ["WORK", "LAB_STORAGE"]              # extra variables returned by `info`
# quota_paths = ["~", "$WORK", "$LAB_STORAGE"]  # portable `df -h` fallback
# protected_roots = ["$WORK", "$LAB_STORAGE"]    # recursive rm refuses these roots
# quota_command = "my-quota-command --user"        # optional site-specific command
# quota_format = "raw"                             # raw | pairs | df
# notify_command = "osascript -e 'display notification \\"MSG\\" with title \\"remoteslurm\\"'"

# Safety rails (all optional; sensible defaults shown):
# protected_paths = ["~/.ssh/**", "~/.bashrc", "~/.bash_profile", "~/.cache/remoteslurm/**"]
# confirm = ["rm", "cancel", "proc_kill"]  # ops needing --yes (CLI) / confirm=true (lib, MCP)
# run_allowlist = ["python*", "Rscript", "git", "ls", "cat"]   # only used when allow_run = "safe"
# [hosts.mycluster.defaults]
# time = "1:00:00"

# Cluster rules an agent should read before submitting (shown by `info`/`rslurm notes`).
# Use a single-line string, or TOML's triple-quoted multi-line string for several lines:
# notes = "Use the short partition only for jobs under 30 minutes."

# Submit templates bundle sbatch options + a script preamble/epilogue; use with
# `rslurm submit --template cpu` or MCP submit(template="cpu"). `inherit` layers one on
# another. Any key other than preamble/epilogue/inherit is an sbatch option.
# [hosts.mycluster.templates.cpu]
# partition = "standard"
# time = "01:00:00"
# cpus_per_task = 4
# mem = "16G"
# preamble = "module load python/3.11\\nsource $WORK/venvs/project/bin/activate\\n"
#
# [hosts.mycluster.templates.short]
# inherit = "cpu"           # take cpu's options + preamble, then override below
# partition = "short"
# time = "00:10:00"

# A project is a local<->remote directory pair for `rslurm sync` (rsync):
# [hosts.mycluster.projects.analysis]
# local   = "~/code/analysis"
# remote  = "$WORK/analysis"       # expanded on the remote side
# exclude = [".git", "__pycache__", "*.nii.gz", "results/"]
# delete  = false                  # allow --delete on push (still needs --delete on the call)
"""
