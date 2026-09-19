"""remoteslurm — fast, agent-friendly control of a remote Slurm login node."""

from .campaigns import (
    CampaignDefinition,
    CampaignManager,
    compile_campaign,
    evaluate_output,
    load_campaign,
    stage_contract,
)
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
    StoreConflict,
    TaskBusy,
    TooLarge,
)
from .tasks import TaskSpec

__version__ = "0.2.0"

__all__ = [
    "Cluster",
    "CampaignDefinition",
    "CampaignManager",
    "compile_campaign",
    "evaluate_output",
    "load_campaign",
    "stage_contract",
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
    "StoreConflict",
    "TaskBusy",
    "TooLarge",
    "__version__",
]
