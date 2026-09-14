"""remoteslurm — fast, agent-friendly control of a remote Slurm login node."""

from .cluster import Cluster
from .config import Config, HostConfig
from .errors import (
    AuthRequired,
    ConfigError,
    ExecutionMismatch,
    InvalidArgument,
    NotConnected,
    NotFound,
    PermissionDenied,
    RegistryUnavailable,
    RemoteSlurmError,
    RemoteTimeout,
    SessionDied,
    SlurmError,
    TaskBusy,
    TooLarge,
)
from .tasks import TaskSpec

__version__ = "0.2.0"

__all__ = [
    "Cluster",
    "Config",
    "HostConfig",
    "TaskSpec",
    "AuthRequired",
    "ConfigError",
    "ExecutionMismatch",
    "InvalidArgument",
    "NotConnected",
    "NotFound",
    "PermissionDenied",
    "RegistryUnavailable",
    "RemoteSlurmError",
    "RemoteTimeout",
    "SessionDied",
    "SlurmError",
    "TaskBusy",
    "TooLarge",
    "__version__",
]
