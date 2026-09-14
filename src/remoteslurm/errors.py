"""Error taxonomy shared by the library, CLI and MCP server.

Every error carries a stable machine-readable ``code`` so agents can branch on it,
plus an optional ``action`` string telling a human/agent exactly what to do next.
"""

from __future__ import annotations

from typing import Any


class RemoteSlurmError(Exception):
    code = "error"

    def __init__(self, message: str, *, action: str | None = None, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.action = action
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"error": self.code, "message": self.message}
        if self.action:
            d["action"] = self.action
        if self.details:
            d["details"] = self.details
        return d

    def __str__(self) -> str:  # pragma: no cover - trivial
        s = f"[{self.code}] {self.message}"
        if self.action:
            s += f"\n  -> {self.action}"
        return s


class NotConnected(RemoteSlurmError):
    """No usable SSH master connection; user must (re)authenticate."""

    code = "not_connected"


class AuthRequired(NotConnected):
    code = "auth_required"


class RemoteTimeout(RemoteSlurmError):
    code = "timeout"


class NotFound(RemoteSlurmError):
    code = "not_found"


class PermissionDenied(RemoteSlurmError):
    code = "permission"


class TooLarge(RemoteSlurmError):
    code = "too_large"


class SlurmError(RemoteSlurmError):
    code = "slurm_error"


class SessionDied(RemoteSlurmError):
    code = "session_died"


class InvalidArgument(RemoteSlurmError):
    code = "invalid_arg"


class ConfigError(RemoteSlurmError):
    code = "config_error"


class ExecutionMismatch(RemoteSlurmError):
    """The client, local daemon, and remote stub are not the same execution build."""

    code = "execution_mismatch"


class TaskBusy(RemoteSlurmError):
    """Another process currently holds the short remote durable-task lease."""

    code = "task_busy"


class Cancelled(RemoteSlurmError):
    """The caller abandoned the call (e.g. an MCP client cancelled or timed out the tool call)."""

    code = "cancelled"


class ConfirmationRequired(RemoteSlurmError):
    """A destructive op (``rm``/``cancel``/…) needs an explicit ``confirm=True``.

    ``what`` is a short human-readable summary of what would happen; the CLI turns this into
    a ``y/N`` prompt and the MCP layer into a ``{needs_confirmation: true, what: ...}`` reply.
    """

    code = "confirmation_required"

    def __init__(self, message: str, *, what: str | None = None, **kw: Any) -> None:
        super().__init__(message, **kw)
        self.what = what or message


_BY_CODE: dict[str, type[RemoteSlurmError]] = {
    cls.code: cls
    for cls in (
        RemoteSlurmError,
        NotConnected,
        AuthRequired,
        RemoteTimeout,
        NotFound,
        PermissionDenied,
        TooLarge,
        SlurmError,
        SessionDied,
        InvalidArgument,
        ConfigError,
        ExecutionMismatch,
        TaskBusy,
        Cancelled,
        ConfirmationRequired,
    )
}


def from_stub_error(err: dict[str, Any]) -> RemoteSlurmError:
    """Convert a stub error payload ``{"code", "message", ...}`` into an exception."""
    code = str(err.get("code", "error"))
    cls = _BY_CODE.get(code, RemoteSlurmError)
    extra = {k: v for k, v in err.items() if k not in ("code", "message", "action")}
    return cls(str(err.get("message", code)), action=err.get("action"), **extra)
