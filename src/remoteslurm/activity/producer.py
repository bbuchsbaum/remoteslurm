"""Best-effort shared operation recording. Recording never controls remote execution."""

from __future__ import annotations

import contextvars
import functools
import hashlib
import inspect
import logging
import posixpath
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict
from types import SimpleNamespace
from typing import Any, Literal, ParamSpec, TypeVar, cast

from .model import EvidenceRef, SemanticRef, Subject
from .store import ActivityStore, encoded, stamp

CLIENT: contextvars.ContextVar[str] = contextvars.ContextVar("activity_client", default="python")
OPERATION: contextvars.ContextVar[str | None] = contextvars.ContextVar("activity_op", default=None)
CORRELATION: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "activity_correlation", default=None
)
CAUSE: contextvars.ContextVar[str | None] = contextvars.ContextVar("activity_cause", default=None)
P = ParamSpec("P")
R = TypeVar("R")
log = logging.getLogger(__name__)


def target_id(host: Any) -> str:
    return hashlib.sha256(
        encoded(
            [
                host.ssh,
                host.ssh_opts,
                host.control_path,
                host.task_dir,
                host.campaign_dir,
            ]
        ).encode()
    ).hexdigest()


def within(path: str | None, root: str) -> bool:
    return bool(
        path
        and path.startswith("/")
        and (
            posixpath.normpath(path) == root
            or posixpath.normpath(path).startswith(root.rstrip("/") + "/")
        )
    )


def workspace_for(store: ActivityStore, target: str, path: str | None) -> str | None:
    matches = [
        w
        for w in store.documents("workspace", limit=1000)
        if w["target_id"] == target and within(path, w["remote_root"])
    ]
    return (
        max(matches, key=lambda w: (len(w["remote_root"]), w["created_at"]))["workspace_id"]
        if matches
        else None
    )


def capture(
    cluster: Any,
    kind: str,
    payload: dict,
    *,
    job: str | None = None,
    source_key: str | None = None,
    provenance: str = "observation",
    authority: Literal[
        "local_request", "scheduler", "filesystem", "execution_ledger", "campaign", "validation"
    ] = "local_request",
    documents: tuple = (),
    subject: Subject | None = None,
    evidence: tuple[EvidenceRef, ...] | None = None,
    occurred_at: str | None = None,
) -> bool:
    try:
        store = ActivityStore()
        target = target_id(cluster.host)
        workspace = workspace_for(store, target, payload.get("workdir") or payload.get("path"))
        op = OPERATION.get()
        if subject is None and kind.startswith("campaign.") and payload.get("run_id"):
            from .semantic import key

            campaign = payload.get("campaign") or payload.get("name")
            card = store.get(
                "campaign_unit", key(target, campaign, payload["run_id"], payload.get("unit_id"))
            )
            subject = Subject(
                target,
                cluster.host.name,
                campaign=campaign,
                run_id=payload["run_id"],
                unit_id=payload.get("unit_id"),
                stage=payload.get("stage"),
                attempt_id=(card or {}).get("attempt_id"),
                semantic_ref=SemanticRef(**card["semantic_ref"]) if card else None,
            )
        store.append(
            subject=subject
            or Subject(
                target,
                cluster.host.name,
                job_id=job,
                submit_time=payload.get("submit_time"),
                identity_confidence="recorded" if job else "unknown",
            ),
            kind=kind,
            source="remoteslurm." + CLIENT.get(),
            payload=payload,
            actor=CLIENT.get(),
            operation_id=op,
            correlation_id=CORRELATION.get() or op,
            causation_id=CAUSE.get(),
            occurred_at=occurred_at,
            source_key=source_key,
            workspace=workspace,
            provenance=provenance,
            evidence=evidence or (EvidenceRef(authority, "remoteslurm", op or uuid.uuid4().hex),),
            documents=documents,
        )
        return True
    except Exception as exc:
        # Do not expose arbitrary command output/credentials in diagnostics.
        log.warning(
            "Activity recording unavailable (%s); remote outcome unchanged", type(exc).__name__
        )
        return False


def accepted(cluster: Any, job_id: str, *, cwd: str | None, name: str | None) -> bool:
    """Capture the acknowledged job id before optional scontrol/registry enrichment."""
    return capture(
        cluster,
        "submission.accepted",
        {
            "job_id": job_id,
            "workdir": cwd,
            "name": name,
            "state": "ACCEPTED",
            "accepted_at": stamp(),
            "identity_basis": "sbatch acknowledgement",
        },
        job=job_id,
        provenance="remote_record",
        authority="scheduler",
    )


def record_job(cluster: Any, job: Any, *, record: Any = None) -> bool:
    try:
        if not job.recorded:
            # An old registry row with the same scheduler id cannot identify this attempt.
            return capture(
                cluster, "job.registration.unavailable", {"job_id": job.job_id}, job=job.job_id
            )
        record = record or cluster.registry.get(job.job_id)
        if record is None:
            return False
        data = asdict(record) if record else {"job_id": job.job_id}
        data.pop("sbatch_args", None)
        data.pop("meta", None)
        data["target_id"] = target_id(cluster.host)
        data["host"] = cluster.host.name
        data["work_id"] = hashlib.sha256(
            encoded(
                [
                    data["target_id"],
                    job.job_id,
                    data.get("submit_time"),
                ]
            ).encode()
        ).hexdigest()
        data["observed_at"] = time.time()
        data["operation_id"] = OPERATION.get()
        data["correlation_id"] = CORRELATION.get()
        # Registration is not a scheduler observation or output validation.
        data["execution"] = "UNKNOWN"
        data["validation"] = "NOT_RUN"
        data["artifacts"] = "UNCHECKED"
        return capture(
            cluster,
            "job.registered",
            {
                "job_id": job.job_id,
                "name": data.get("name"),
                "workdir": data.get("workdir"),
            },
            job=job.job_id,
            documents=(("work", data["work_id"], data),),
        )
    except Exception as exc:
        log.warning("Activity job registration unavailable (%s)", type(exc).__name__)
        return False


def operation(kind: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorate domain entry points; kwargs and results use an explicit metadata allowlist."""

    def decorate(fn: Callable[P, R]) -> Callable[P, R]:
        signature = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            owner = args[0]
            cluster = getattr(owner, "cluster", owner)
            if OPERATION.get() and kind.startswith("file."):
                return fn(*args, **kwargs)
            values = signature.bind(*args, **kwargs).arguments
            fields = {
                key: value
                for key, value in values.items()
                if key
                in {
                    "path",
                    "cwd",
                    "name",
                    "job_id",
                    "run_id",
                    "stage",
                    "unit_id",
                    "src",
                    "dest",
                    "to_remote",
                    "dry_run",
                    "pull",
                }
                and isinstance(value, (str, int))
            }
            fields = {k: v[:4096] if isinstance(v, str) else v for k, v in fields.items()}
            if "cwd" in fields:
                fields["workdir"] = fields.pop("cwd")
            if kind.startswith("transfer."):
                project = values.get("project")
                if project is not None:
                    fields.update(project=project.name, workdir=project.remote)
                elif "src" in fields and "dest" in fields:
                    fields["path"] = fields["dest"] if values.get("to_remote") else fields["src"]
            parent = OPERATION.get()
            token = OPERATION.set(uuid.uuid4().hex)
            correlation = CORRELATION.set(CORRELATION.get() or OPERATION.get())
            cause = CAUSE.set(parent)
            recorded = capture(cluster, kind + ".requested", fields, provenance="request")
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:
                if kind.startswith("transfer."):
                    fields["transfer_outcome"] = "partial_or_unknown"
                capture(
                    cluster,
                    kind + ".outcome_unknown"
                    if getattr(exc, "code", "")
                    in {
                        "timeout",
                        "session_died",
                    }
                    or isinstance(exc, (KeyboardInterrupt, SystemExit))
                    else kind + ".failed",
                    {**fields, "error_code": getattr(exc, "code", type(exc).__name__)},
                )
                raise
            else:
                if hasattr(result, "job_id") and hasattr(result, "submission"):
                    recorded = record_job(cluster, result) and recorded
                    cast(Any, result).activity_recorded = recorded and getattr(
                        result, "activity_recorded", True
                    )
                    fields["job_id"] = result.job_id
                elif isinstance(result, dict):
                    if kind == "task.ensure" and result.get("job_id"):
                        recorded = (
                            record_job(
                                cluster,
                                SimpleNamespace(job_id=str(result["job_id"]), recorded=True),
                            )
                            and recorded
                        )
                    fields.update(
                        {
                            key: result[key]
                            for key in (
                                "path",
                                "job_id",
                                "state",
                                "rc",
                                "campaign",
                                "run_id",
                                "direction",
                                "remote",
                                "local",
                                "files",
                                "bytes",
                                "dry_run",
                            )
                            if key in result
                            and isinstance(result[key], (str, int, bool, type(None)))
                        }
                    )
                    if "remote" in fields:
                        fields["workdir"] = fields["remote"]
                elif isinstance(result, int):
                    fields["rc"] = result
                if kind.startswith("transfer."):
                    fields["transfer_outcome"] = (
                        "partial_or_unknown" if fields.get("rc", 0) else "returned_successfully"
                    )
                ok = capture(cluster, kind + ".returned", fields)
                if isinstance(result, dict) and not (ok and recorded):
                    result["activity_recorded"] = False
                elif hasattr(result, "activity_recorded"):
                    cast(Any, result).activity_recorded = cast(Any, result).activity_recorded and ok
                return result
            finally:
                OPERATION.reset(token)
                CORRELATION.reset(correlation)
                CAUSE.reset(cause)

        return wrapped

    return decorate
