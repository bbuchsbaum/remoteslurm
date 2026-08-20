"""User configuration: ``~/.config/remoteslurm/config.toml``.

A host that is not in the config is still usable: it is treated as a bare ssh alias with
``mfa = false`` semantics *disabled* (we assume MFA-less hosts can auto-connect) — except that
we only auto-connect when the alias is not obviously an MFA host. Users can always run
``remoteslurm connect <host>`` explicitly.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import ConfigError

ENV_CONFIG = "REMOTESLURM_CONFIG"
ENV_DEFAULT_HOST = "REMOTESLURM_DEFAULT_HOST"
ENV_STATE_DIR = "REMOTESLURM_STATE_DIR"


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
    allow_run: bool = True
    script_dir: str | None = None  # where generated sbatch scripts are written (remote)
    ssh_opts: list[str] = field(default_factory=list)
    defaults: dict[str, Any] = field(
        default_factory=dict
    )  # default sbatch args, e.g. {"time": "1:00:00"}
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, name: str, d: dict[str, Any]) -> HostConfig:
        known = {f for f in cls.__dataclass_fields__ if f not in ("name", "extra")}
        kw = {k: v for k, v in d.items() if k in known}
        extra = {k: v for k, v in d.items() if k not in known}
        kw.setdefault("ssh", name)
        try:
            return cls(name=name, extra=extra, **kw)
        except TypeError as e:
            raise ConfigError(f"bad config for host {name!r}: {e}") from e


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
default_host = "trillium"

[hosts.trillium]
ssh = "trillium"            # alias from ~/.ssh/config (ControlMaster recommended)
mfa = true                  # Duo/MFA: you must run `remoteslurm connect trillium` once
account = "rrg-someone"     # default --account for sbatch
# partition = "compute"
# python = "python3"        # remote interpreter for the stub
# control_persist = "12h"
# allow_run = true          # enable the `run` (arbitrary command) tool
# [hosts.trillium.defaults]
# time = "1:00:00"
"""
