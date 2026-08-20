"""remoteslurm — fast, agent-friendly control of a remote Slurm login node."""

from .cluster import Cluster
from .config import Config, HostConfig
from .errors import (
    AuthRequired,
    ConfigError,
    InvalidArgument,
    NotConnected,
    NotFound,
    PermissionDenied,
    RemoteSlurmError,
    RemoteTimeout,
    SessionDied,
    SlurmError,
    TooLarge,
)

__version__ = "0.1.0"

__all__ = [
    "Cluster",
    "Config",
    "HostConfig",
    "AuthRequired",
    "ConfigError",
    "InvalidArgument",
    "NotConnected",
    "NotFound",
    "PermissionDenied",
    "RemoteSlurmError",
    "RemoteTimeout",
    "SessionDied",
    "SlurmError",
    "TooLarge",
    "__version__",
]
