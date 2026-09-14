"""Durable, content-addressed single-job execution contracts."""

from __future__ import annotations

import hashlib
import json
import time
import tomllib
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import slurm
from .errors import ExecutionMismatch, InvalidArgument, RemoteSlurmError
from .identity import control_identity
from .jobs import JobRecord, append_learned_notes, compose_templated_script

if TYPE_CHECKING:
    from .cluster import Cluster


TASK_VERSION = 1
_ALLOWED_KEYS = {
    "version",
    "name",
    "host",
    "script",
    "script_inline",
    "cwd",
    "template",
    "inputs",
    "outputs",
    "validate",
    "validation_timeout",
    "resources",
    "environment",
}


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _paths(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise InvalidArgument(f"{field_name} must be an array of remote file paths")
    out: list[str] = []
    for item in value:
        if isinstance(item, str):
            path = item
        elif isinstance(item, dict) and set(item) <= {"path", "fingerprint"}:
            raw_path = item.get("path")
            if item.get("fingerprint", "sha256") != "sha256":
                raise InvalidArgument(f"{field_name} supports fingerprint = 'sha256' only")
            path = raw_path if isinstance(raw_path, str) else ""
        else:
            raise InvalidArgument(
                f"each {field_name} entry must be a path string or {{path, fingerprint}} table"
            )
        if not isinstance(path, str) or not path.strip() or "\x00" in path:
            raise InvalidArgument(f"{field_name} paths must be non-empty strings without NUL")
        out.append(path)
    if len(set(out)) != len(out):
        raise InvalidArgument(f"{field_name} contains a duplicate path")
    return out


def _command(value: Any) -> tuple[list[str], int]:
    timeout = 300
    if isinstance(value, dict):
        unknown = set(value) - {"command", "timeout"}
        if unknown:
            raise InvalidArgument(f"validate contains unknown keys: {sorted(unknown)}")
        command = value.get("command")
        timeout = value.get("timeout", timeout)
    else:
        command = value
    if not isinstance(command, list) or not command or not all(
        isinstance(part, str) and part for part in command
    ):
        raise InvalidArgument("validate must be a non-empty argv array, not a shell string")
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= 3600:
        raise InvalidArgument("validation timeout must be an integer from 1 to 3600 seconds")
    return list(command), timeout


@dataclass(frozen=True)
class TaskSpec:
    """A parsed durable-task manifest before host defaults and fingerprints are resolved."""

    script: str
    outputs: list[str]
    validate: list[str]
    name: str = "task"
    host: str | None = None
    cwd: str | None = None
    template: str | None = None
    inputs: list[str] = field(default_factory=list)
    resources: dict[str, Any] = field(default_factory=dict)
    environment: dict[str, Any] = field(default_factory=dict)
    validation_timeout: int = 300
    source: str | None = None

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        base_dir: Path | None = None,
        source: str | None = None,
    ) -> TaskSpec:
        data = dict(value)
        unknown = set(data) - _ALLOWED_KEYS
        if unknown:
            raise InvalidArgument(f"task manifest contains unknown keys: {sorted(unknown)}")
        version = data.get("version", TASK_VERSION)
        if version != TASK_VERSION:
            raise InvalidArgument(f"unsupported task manifest version {version!r}; expected 1")
        script_path = data.get("script")
        script_inline = data.get("script_inline")
        if (script_path is None) == (script_inline is None):
            raise InvalidArgument("provide exactly one of script or script_inline")
        if script_inline is not None:
            if not isinstance(script_inline, str) or not script_inline.strip():
                raise InvalidArgument("script_inline must be non-empty text")
            script = script_inline
        else:
            if not isinstance(script_path, str) or not script_path:
                raise InvalidArgument("script must be a local file path")
            path = Path(script_path).expanduser()
            if not path.is_absolute() and base_dir is not None:
                path = base_dir / path
            try:
                script = path.read_text("utf-8")
            except OSError as e:
                raise InvalidArgument(f"cannot read task script {path}: {e}") from e
        if not script.startswith("#!"):
            script = "#!/bin/bash\n" + script
        if not script.endswith("\n"):
            script += "\n"
        outputs = _paths(data.get("outputs"), "outputs")
        if not outputs:
            raise InvalidArgument("outputs must declare at least one result file")
        validate, nested_timeout = _command(data.get("validate"))
        timeout = data.get("validation_timeout", nested_timeout)
        if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= 3600:
            raise InvalidArgument("validation_timeout must be an integer from 1 to 3600 seconds")
        inputs = _paths(data.get("inputs", []), "inputs")
        resources = data.get("resources", {})
        environment = data.get("environment", {})
        if not isinstance(resources, dict):
            raise InvalidArgument("resources must be a table")
        if not isinstance(environment, dict):
            raise InvalidArgument("environment must be a table")
        unsupported = {key for key in resources if key.replace("-", "_") in {"array", "dependency"}}
        if unsupported:
            raise InvalidArgument(
                f"durable task version 1 does not support resource keys: {sorted(unsupported)}"
            )
        try:
            _canonical({"resources": resources, "environment": environment})
        except (TypeError, ValueError) as e:
            raise InvalidArgument(
                "resources and environment must contain JSON-compatible values: " + str(e)
            ) from e
        text_fields = (
            ("name", data.get("name", "task")),
            ("host", data.get("host")),
            ("cwd", data.get("cwd")),
            ("template", data.get("template")),
        )
        for label, item in text_fields:
            if item is not None and (not isinstance(item, str) or not item.strip()):
                raise InvalidArgument(f"{label} must be a non-empty string")
        return cls(
            script=script,
            outputs=outputs,
            validate=validate,
            name=str(data.get("name", "task")),
            host=data.get("host"),
            cwd=data.get("cwd"),
            template=data.get("template"),
            inputs=inputs,
            resources=dict(resources),
            environment=dict(environment),
            validation_timeout=timeout,
            source=source,
        )

    @classmethod
    def load(cls, path: str | Path) -> TaskSpec:
        manifest = Path(path).expanduser().resolve()
        try:
            data = tomllib.loads(manifest.read_text("utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as e:
            raise InvalidArgument(f"cannot read task manifest {manifest}: {e}") from e
        return cls.from_mapping(data, base_dir=manifest.parent, source=str(manifest))


def _submission(cluster: Cluster, spec: TaskSpec) -> tuple[str, dict[str, Any], list[str]]:
    opts: dict[str, Any] = dict(cluster.host.defaults or {})
    if cluster.host.account:
        opts.setdefault("account", cluster.host.account)
    if cluster.host.partition:
        opts.setdefault("partition", cluster.host.partition)
    script = spec.script
    if spec.template is not None:
        template = cluster.host.resolve_template(spec.template)
        opts.update(template.options)
        script = compose_templated_script(
            script, template, name=spec.template, merged_options={**opts, **spec.resources}
        )
    opts.update(spec.resources)
    # The durable attempt marker owns JobName so reconciliation never depends on a display name.
    opts.pop("job_name", None)
    flags = slurm.sbatch_args_from_options(dict(sorted(opts.items())))
    return script, opts, flags


def _control(cluster: Cluster) -> dict[str, Any]:
    expected = control_identity()
    info = cluster.info(refresh=True)
    actual_sha = info.get("stub_sha")
    if actual_sha != expected["stub_sha"]:
        raise ExecutionMismatch(
            "the loaded remote stub does not match the requesting client",
            action="close the host session and repeat ensure so the current stub is installed",
            client_stub_sha=expected["stub_sha"],
            remote_stub_sha=actual_sha,
            remote_stub=info.get("stub"),
        )
    return {
        **expected,
        "remote_protocol": info.get("protocol"),
        "remote_python": info.get("python"),
        "remote_stub": info.get("stub"),
    }


def _resolve(cluster: Cluster, spec: TaskSpec) -> dict[str, Any]:
    script, resources, flags = _submission(cluster, spec)
    info = cluster.info()
    cwd = cluster.call("expandpath", path=spec.cwd or info.get("home"))["path"]
    input_files = cluster.call(
        "task_fingerprint",
        paths=spec.inputs,
        required=True,
        hash=True,
        base=cwd,
        _timeout=3600,
    )["files"]
    output_files = cluster.call(
        "task_fingerprint",
        paths=spec.outputs,
        required=False,
        hash=False,
        base=cwd,
        _timeout=300,
    )["files"]
    declared_paths = {
        **dict(zip(spec.inputs, (item["path"] for item in input_files), strict=True)),
        **dict(zip(spec.outputs, (item["path"] for item in output_files), strict=True)),
    }
    validate = [declared_paths.get(part, part) for part in spec.validate]
    contract = {
        "schema": TASK_VERSION,
        "name": spec.name,
        "cluster": info.get("env", {}).get("SLURM_CLUSTER_NAME") or cluster.host.name,
        "script_sha256": hashlib.sha256(script.encode("utf-8")).hexdigest(),
        "inputs": sorted(input_files, key=lambda item: item["path"]),
        "outputs": sorted(item["path"] for item in output_files),
        "cwd": cwd,
        "resources": resources,
        "environment": spec.environment,
        "validate": {"command": validate, "timeout": spec.validation_timeout},
    }
    task_id = hashlib.sha256(_canonical(contract).encode("utf-8")).hexdigest()
    limitations: list[str] = []
    if not spec.inputs:
        limitations.append("no input files were declared")
    else:
        limitations.append(
            "remote inputs are fingerprinted but not staged; they must remain immutable "
            "during execution"
        )
    limitations.append("environment identity is user-declared rather than measured at runtime")
    container = spec.environment.get("container")
    if not (isinstance(container, str) and "@sha256:" in container):
        limitations.append("the execution environment is not pinned by container digest")
    return {
        "task_id": task_id,
        "contract": contract,
        "script": script,
        "flags": flags,
        "cwd": contract["cwd"],
        "limitations": limitations,
    }


def _current(record: Mapping[str, Any]) -> dict[str, Any] | None:
    wanted = record.get("current_attempt")
    for attempt in reversed(record.get("attempts") or []):
        if attempt.get("attempt_id") == wanted:
            return dict(attempt)
    return None


def _remember_job(cluster: Cluster, record: Mapping[str, Any], attempt: Mapping[str, Any]) -> None:
    job_id = attempt.get("job_id")
    if not job_id or cluster.registry.get(str(job_id)) is not None:
        return
    rec = JobRecord(
        job_id=str(job_id),
        name=str(record.get("name") or "task"),
        script_path=attempt.get("script_path"),
        submit_time=float(attempt.get("accepted_at") or attempt.get("created_at") or time.time()),
        last_state=str(attempt.get("state") or "PENDING"),
        last_seen=time.time(),
        meta={
            "durable_task_id": record.get("task_id"),
            "durable_attempt_id": attempt.get("attempt_id"),
        },
    )
    try:
        sc = cluster.scontrol_job(str(job_id))
        rec.stdout_path = sc.get("StdOut") or None
        rec.stderr_path = sc.get("StdErr") or None
        rec.workdir = sc.get("WorkDir") or None
    except RemoteSlurmError:
        pass
    cluster.registry.put(rec)


def _update(
    cluster: Cluster,
    resolved: Mapping[str, Any],
    attempt_id: str,
    state: str,
    **fields: Any,
) -> dict[str, Any]:
    result = cluster.call(
        "task_update",
        task_id=resolved["task_id"],
        task_dir=cluster.host.task_dir,
        attempt_id=attempt_id,
        state=state,
        _timeout=60,
        **fields,
    )
    return dict(result["record"])


def ensure_task(
    cluster: Cluster,
    spec: TaskSpec | Mapping[str, Any] | str | Path,
    *,
    retry: bool = False,
    retry_unknown: bool = False,
) -> dict[str, Any]:
    """Recover, submit, or verify one durable task without silently duplicating attempts."""
    if isinstance(spec, (str, Path)):
        parsed = TaskSpec.load(spec)
    elif isinstance(spec, TaskSpec):
        parsed = spec
    elif isinstance(spec, Mapping):
        parsed = TaskSpec.from_mapping(spec)
    else:
        raise InvalidArgument("ensure expects a TaskSpec, manifest mapping, or TOML path")
    if parsed.host is not None and parsed.host != cluster.host.name:
        raise InvalidArgument(
            f"task manifest selects host {parsed.host!r}, but cluster "
            f"{cluster.host.name!r} is connected"
        )
    control = _control(cluster)
    resolved = _resolve(cluster, parsed)
    remote = cluster.call(
        "task_ensure",
        task_id=resolved["task_id"],
        task_dir=cluster.host.task_dir,
        contract=resolved["contract"],
        script=resolved["script"],
        args=resolved["flags"],
        cwd=resolved["cwd"],
        name=parsed.name,
        attempt_id=uuid.uuid4().hex,
        retry=retry,
        retry_unknown=retry_unknown,
        control=control,
        _timeout=180,
    )
    record = dict(remote["record"])
    attempt = _current(record)
    base = {
        "task_id": resolved["task_id"],
        "name": parsed.name,
        "state": record.get("state"),
        "task_dir": remote.get("task_dir"),
        "submitted": bool(remote.get("submitted")),
        "limitations": resolved["limitations"],
        "attempts": record.get("attempts", []),
        "reason": record.get("reason"),
    }
    if attempt is None or not attempt.get("job_id"):
        if record.get("state") == "REJECTED":
            append_learned_notes(
                cluster.host.name,
                slurm.match_policy_lines(str(record.get("reason") or "")),
            )
        if record.get("state") == "UNKNOWN":
            base["action"] = (
                "inspect squeue/sacct for the attempt marker; use retry_unknown=True only after "
                "accepting that duplicate execution may result"
            )
        elif record.get("state") in ("FAILED", "INVALID", "REJECTED"):
            base["action"] = "repeat ensure with retry=True to create a new visible attempt"
        return base

    job_id = str(attempt["job_id"])
    stored_scheduler = attempt.get("scheduler")
    if isinstance(stored_scheduler, dict) and stored_scheduler.get("terminal"):
        scheduler = dict(stored_scheduler)
        base.update(job_id=job_id, attempt_id=attempt["attempt_id"], scheduler=scheduler)
        if scheduler.get("state") != "COMPLETED":
            reason = f"scheduler attempt ended in {scheduler.get('state')}"
            base.update(state="FAILED", reason=reason)
            base["action"] = "repeat ensure with retry=True to create a new visible attempt"
            return base
    else:
        status = cluster.job_status(job_id, refresh=True)
        scheduler = status.to_dict()
        base.update(job_id=job_id, attempt_id=attempt["attempt_id"], scheduler=scheduler)
        if status.state == "UNKNOWN" or status.source == "registry":
            record = _update(
                cluster,
                resolved,
                attempt["attempt_id"],
                "UNKNOWN",
                scheduler=scheduler,
                reason="the accepted job is no longer visible in squeue, scontrol, or sacct",
            )
            base.update(state="UNKNOWN", reason=record.get("reason"))
            base["action"] = (
                "inspect scheduler history before using retry_unknown=True; "
                "the original job may exist"
            )
            return base
        _remember_job(cluster, record, attempt)
        if not status.terminal:
            state = "RUNNING" if status.state == "RUNNING" else "PENDING"
            _update(cluster, resolved, attempt["attempt_id"], state, scheduler=scheduler)
            base["state"] = state
            return base
        if status.state != "COMPLETED":
            reason = f"scheduler attempt ended in {status.state}"
            _update(
                cluster,
                resolved,
                attempt["attempt_id"],
                "FAILED",
                scheduler=scheduler,
                reason=reason,
            )
            base.update(state="FAILED", reason=reason)
            base["action"] = "repeat ensure with retry=True to create a new visible attempt"
            return base

    _update(cluster, resolved, attempt["attempt_id"], "COMPLETED", scheduler=scheduler)
    validation = cluster.run(
        resolved["contract"]["validate"]["command"],
        cwd=str(resolved["cwd"]),
        timeout=parsed.validation_timeout,
        max_output=65536,
    )
    evidence = {
        "command": resolved["contract"]["validate"]["command"],
        "checked_at": time.time(),
        "rc": validation.get("rc"),
        "stdout": str(validation.get("stdout") or "")[-4000:],
        "stderr": str(validation.get("stderr") or "")[-4000:],
    }
    if validation.get("rc") != 0:
        reason = f"validation command exited {validation.get('rc')}"
        _update(
            cluster,
            resolved,
            attempt["attempt_id"],
            "INVALID",
            scheduler=scheduler,
            validation=evidence,
            reason=reason,
        )
        base.update(state="INVALID", reason=reason, validation=evidence)
        base["action"] = "repair the outputs or repeat ensure with retry=True"
        return base
    try:
        outputs = cluster.call(
            "task_fingerprint",
            paths=resolved["contract"]["outputs"],
            required=True,
            hash=True,
            _timeout=3600,
        )["files"]
    except RemoteSlurmError as e:
        reason = f"declared output evidence is unavailable: {e.message}"
        _update(
            cluster,
            resolved,
            attempt["attempt_id"],
            "INVALID",
            scheduler=scheduler,
            validation=evidence,
            reason=reason,
        )
        base.update(state="INVALID", reason=reason, validation=evidence)
        base["action"] = "repair the outputs or repeat ensure with retry=True"
        return base
    outputs = sorted(outputs, key=lambda item: item["path"])
    prior = record.get("receipt")
    if prior is not None and prior.get("outputs") != outputs:
        reason = "declared output fingerprints changed after verification"
        _update(
            cluster,
            resolved,
            attempt["attempt_id"],
            "INVALID",
            scheduler=scheduler,
            validation=evidence,
            reason=reason,
        )
        base.update(state="INVALID", reason=reason, validation=evidence, outputs=outputs)
        base["action"] = "restore the verified outputs or repeat ensure with retry=True"
        return base
    receipt = prior or {
        "schema": 1,
        "task_id": resolved["task_id"],
        "attempt_id": attempt["attempt_id"],
        "job_id": job_id,
        "verified_at": time.time(),
        "control": control,
        "contract": resolved["contract"],
        "limitations": resolved["limitations"],
        "scheduler": scheduler,
        "validation": evidence,
        "outputs": outputs,
    }
    _update(
        cluster,
        resolved,
        attempt["attempt_id"],
        "VERIFIED",
        scheduler=scheduler,
        validation=evidence,
        receipt=receipt,
    )
    base.update(state="VERIFIED", receipt=receipt, validation=evidence, outputs=outputs)
    return base
