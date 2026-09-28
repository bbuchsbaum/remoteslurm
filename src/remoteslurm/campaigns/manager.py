"""Observation-first campaign lifecycle and aggregation."""

from __future__ import annotations

import getpass
import hashlib
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from .. import slurm
from ..activity.attention import project_attention
from ..attempts import (
    ensure_campaign_attempt,
    submission_control,
    update_campaign_attempt,  # noqa: F401 - retained as a compatibility/monkeypatch seam
)
from ..errors import InvalidArgument, RemoteSlurmError, StoreConflict
from ..evidence import ObservationIdentity, observed, unavailable
from ..reconcile import TERMINAL_STATES, reconcile_job
from .contracts import (
    MAX_VALIDATOR_OUTPUT,
    artifact_signature,
    evaluate_output,
    fixture_evidence,
    observation_request,
    render_validator,
    stage_contract,
    unit_contract,
)
from .execution import (
    array_spec,
    pending_retry_authorization,
    plan_job_groups,
    render_group_script,
    retry_eligible,
)
from .model import CampaignDefinition, Stage, WorkUnit
from .spec import canonical_json, pilot_alternative_coverage, pilot_units
from .store import CampaignStore

if TYPE_CHECKING:
    from ..cluster import Cluster

EXECUTION_STATES = (
    "UNBOUND",
    "INTENDED",
    "PENDING",
    "RUNNING",
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "UNKNOWN",
)
ARTIFACT_STATES = ("UNCHECKED", "SETTLING", "PRESENT", "MISSING", "CHANGED", "ERROR")
VALIDATION_STATES = ("NOT_RUN", "PASSED", "FAILED", "STALE", "ERROR")
DEPENDENCY_STATES = ("NOT_APPLICABLE", "BLOCKED", "SATISFIED", "CONFLICT")
FRESHNESS_STATES = ("FRESH", "STALE", "PARTIAL", "UNAVAILABLE", "CONFLICT")
ACTIVE_EXECUTION = frozenset({"INTENDED", "PENDING", "RUNNING", "UNKNOWN"})
FAILED_EXECUTION = frozenset(TERMINAL_STATES - {"COMPLETED", "CANCELLED"})
# Sources that observed the job itself; "scancel" and "registry" only restate our own request.
SCHEDULER_SOURCES = frozenset({"squeue", "sacct", "scontrol"})


def _duplicate_risk(execution: Mapping[str, Any]) -> bool:
    """Whether replacing this attempt could run alongside a still-live original job."""
    state = execution.get("state")
    if state == "UNKNOWN":
        return True
    if state != "CANCELLED":
        return False
    scheduler = execution.get("scheduler") or {}
    return not (
        scheduler.get("terminal") and execution.get("scheduler_source") in SCHEDULER_SOURCES
    )


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _event(snapshot: Mapping[str, Any], event_type: str, **payload: Any) -> dict[str, Any]:
    return {
        "schema": 1,
        "event_id": uuid.uuid4().hex,
        "type": event_type,
        "recorded_at": _now(),
        "actor": getpass.getuser(),
        "campaign": snapshot["campaign"],
        "run_id": snapshot["run_id"],
        "definition_id": snapshot["definition_id"],
        **payload,
    }


def _execution_projection(state: str) -> str:
    state = slurm.normalize_state(state)
    if state == "COMPLETED":
        return "COMPLETED"
    if state == "CANCELLED":
        return "CANCELLED"
    if state in FAILED_EXECUTION:
        return "FAILED"
    if state == "PENDING" or state in {"CONFIGURING", "REQUEUED", "REQUEUE_HOLD"}:
        return "PENDING"
    if state in slurm.ACTIVE_STATES:
        return "RUNNING"
    return "UNKNOWN"


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


class CampaignManager:
    """High-level campaign API. Status and refresh never mutate workloads."""

    def __init__(self, cluster: Cluster) -> None:
        self.cluster = cluster
        self.store = CampaignStore(cluster)

    def list_campaigns(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        return self.store.list_campaigns(limit=limit, offset=offset)

    def runs(
        self,
        name: str,
        *,
        include_archived: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        raw = self.store.list_runs(name, limit=limit, offset=offset)
        records: list[dict[str, Any]] = []
        for item in raw["items"]:
            record = dict(item["run"])
            summary = item.get("summary")
            if isinstance(summary, Mapping):
                record["lifecycle"] = summary.get("lifecycle", "OPEN")
                record["revision"] = item.get("head", {}).get("revision", 0)
            else:
                record["lifecycle"] = "UNINITIALIZED"
                record["revision"] = item.get("head", {}).get("revision", 0)
            if include_archived or record["lifecycle"] != "ARCHIVED":
                records.append(record)
        return {**raw, "items": records, "count": len(records)}

    def _resolve_run(self, name: str, run_id: str | None) -> str:
        if run_id:
            return run_id
        candidates = self.runs(name, include_archived=False, limit=100)["items"]
        open_runs = [record["run_id"] for record in candidates if record["lifecycle"] == "OPEN"]
        if len(open_runs) != 1:
            raise InvalidArgument(
                f"campaign {name!r} has {len(open_runs)} open runs; pass run_id explicitly",
                candidates=open_runs or [record["run_id"] for record in candidates],
            )
        return str(open_runs[0])

    @staticmethod
    def _dependency_requirements(
        definition: CampaignDefinition, stage: Stage, unit: WorkUnit
    ) -> list[dict[str, str]]:
        stage_by_unit = {candidate.unit_id: candidate.stage for candidate in definition.units}
        require_by_stage = {need.stage: need.require for need in stage.needs}
        return [
            {"unit_id": dependency, "require": require_by_stage[stage_by_unit[dependency]]}
            for dependency in unit.dependencies
        ]

    @staticmethod
    def _initial_unit(
        definition: CampaignDefinition, stage: Stage, unit: WorkUnit
    ) -> dict[str, Any]:
        contract = unit_contract(stage, unit)
        expected_outputs = contract["outputs"]
        requirements = CampaignManager._dependency_requirements(definition, stage, unit)
        return {
            "unit_id": unit.unit_id,
            "stage": unit.stage,
            "ordinal": unit.ordinal,
            "keys": unit.keys,
            "dependency_requirements": requirements,
            "dependency": {
                "state": "BLOCKED" if requirements else "NOT_APPLICABLE",
            },
            "execution": {"state": "UNBOUND", "attempts": []},
            "artifacts": {"state": "UNCHECKED", "outputs": expected_outputs},
            "validation": {"state": "NOT_RUN", "contract_id": contract["contract_id"]},
            "contract": contract,
            "freshness": {"state": "STALE", "reason": "not observed"},
        }

    @staticmethod
    def _stage_graph(definition: CampaignDefinition) -> list[dict[str, Any]]:
        return [
            {
                "name": stage.name,
                "foreach": stage.foreach,
                "needs": [
                    {"stage": need.stage, "on": list(need.on), "require": need.require}
                    for need in stage.needs
                ],
            }
            for stage in definition.stages
        ]

    @staticmethod
    def _stage_specs(definition: CampaignDefinition) -> dict[str, dict[str, Any]]:
        specs: dict[str, dict[str, Any]] = {}
        for stage in definition.stages:
            environment = dict(definition.environments.get(stage.environment or "", {}))
            template = dict(environment.get("resolved_template", {}))
            preamble = "\n".join(
                item
                for item in (
                    str(template.get("preamble") or ""),
                    str(environment.get("preamble") or ""),
                )
                if item
            )
            epilogue = "\n".join(
                item
                for item in (
                    str(environment.get("epilogue") or ""),
                    str(template.get("epilogue") or ""),
                )
                if item
            )
            specs[stage.name] = {
                "stage": stage.name,
                "script": stage.script,
                "script_sha256": stage.script_sha256,
                "script_content": stage.script_content,
                "execution": dict(stage.execution),
                "resources": dict(stage.resources),
                "environment_name": stage.environment,
                "environment": environment,
                "preamble": preamble,
                "epilogue": epilogue,
                "contract_id": stage_contract(stage)["contract_id"],
                "validators": [dict(validator) for validator in stage.validators],
            }
        return specs

    def start(
        self,
        definition: CampaignDefinition,
        *,
        run_id: str | None = None,
        parent_run: str | None = None,
    ) -> dict[str, Any]:
        if definition.host != self.cluster.host.name:
            raise InvalidArgument(
                f"campaign targets host {definition.host!r}, connected host is "
                f"{self.cluster.host.name!r}"
            )
        self.store.put_definition(definition)
        run_result = self.store.put_run(definition, run_id=run_id, parent_run=parent_run)
        run = run_result["run"]
        stage_map = {stage.name: stage for stage in definition.stages}
        units = [
            self._initial_unit(definition, stage_map[unit.stage], unit) for unit in definition.units
        ]
        snapshot: dict[str, Any] = {
            "schema": 1,
            "campaign": definition.name,
            "definition_id": definition.definition_id,
            "run_id": run["run_id"],
            "host": definition.host,
            "workspace": dict(definition.workspace),
            "lifecycle": "OPEN",
            "created_at": run["created_at"],
            "refreshed_at": None,
            "stage_graph": self._stage_graph(definition),
            "stage_specs": self._stage_specs(definition),
            "pilots": {name: dict(value) for name, value in definition.pilots.items()},
            "source_availability": {},
            "telemetry": self._empty_telemetry(),
            "limitations": [],
            "units": units,
        }
        self._summarize(snapshot)
        event = _event(snapshot, "run_started")
        snapshot["revision"] = 1
        result = self.store.commit(
            definition.name,
            run["run_id"],
            expected_revision=0,
            events=[event],
            documents=self.store.snapshot_documents(snapshot),
        )
        snapshot["store"] = result["head"]
        return self._page(snapshot, include_units=False)

    @staticmethod
    def _empty_telemetry() -> dict[str, Any]:
        return {
            "active_allocations": 0,
            "allocated_cpus": 0,
            "effective_cpus": None,
            "estimated_rss_bytes": None,
            "cpu_coverage": {"sampled": 0, "eligible": 0},
            "rss_coverage": {"sampled": 0, "eligible": 0},
        }

    def _commit_snapshot(
        self, snapshot: dict[str, Any], events: list[dict[str, Any]]
    ) -> dict[str, Any]:
        expected = int(snapshot.get("store", {}).get("revision", snapshot.get("revision", 0)))
        snapshot["revision"] = expected + 1
        snapshot.pop("store", None)
        result = self.store.commit(
            snapshot["campaign"],
            snapshot["run_id"],
            expected_revision=expected,
            events=events,
            documents=self.store.snapshot_documents(snapshot),
        )
        snapshot["store"] = result["head"]
        return result

    def adopt(
        self,
        name: str,
        stage: str,
        *,
        run_id: str | None = None,
        job_id: str | None = None,
        array_job_id: str | None = None,
        unit_jobs: Mapping[str, str] | None = None,
        output_only: bool = False,
    ) -> dict[str, Any]:
        run_id = self._resolve_run(name, run_id)
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot.get("lifecycle") != "OPEN":
            raise InvalidArgument("adoption requires an open campaign run")
        modes = sum(
            int(value)
            for value in (
                job_id is not None,
                array_job_id is not None,
                unit_jobs is not None,
                output_only,
            )
        )
        if modes != 1:
            raise InvalidArgument(
                "choose exactly one of job_id, array_job_id, unit_jobs, or output_only"
            )
        selected = sorted(
            (unit for unit in snapshot["units"] if unit["stage"] == stage),
            key=lambda unit: int(unit["ordinal"]),
        )
        if not selected:
            raise InvalidArgument(f"campaign has no stage {stage!r}")
        if job_id is not None and len(selected) != 1:
            raise InvalidArgument("job_id adoption requires a one-unit stage; use array_job_id")
        mapping: dict[str, str] = {}
        if job_id is not None:
            slurm.parse_job_id(job_id)
            mapping[selected[0]["unit_id"]] = job_id
        elif array_job_id is not None:
            base, task = slurm.parse_job_id(array_job_id)
            if task is not None:
                raise InvalidArgument("array_job_id must be a bare Slurm array id")
            mapping = {unit["unit_id"]: f"{base}_{unit['ordinal']}" for unit in selected}
        elif unit_jobs is not None:
            unknown = set(unit_jobs) - {unit["unit_id"] for unit in selected}
            if unknown:
                raise InvalidArgument(f"unit_jobs contains unknown stage units: {sorted(unknown)}")
            if set(unit_jobs) != {unit["unit_id"] for unit in selected}:
                raise InvalidArgument("unit_jobs must bind every selected stage unit")
            for unit_id, bound_job in unit_jobs.items():
                slurm.parse_job_id(bound_job)
                mapping[unit_id] = bound_job

        adopted_at = _now()
        events: list[dict[str, Any]] = []
        for unit in selected:
            if output_only:
                unit["adoption"] = {"mode": "output_only", "adopted_at": adopted_at}
                events.append(
                    _event(
                        snapshot,
                        "job_adopted",
                        stage=stage,
                        unit_id=unit["unit_id"],
                        mode="output_only",
                    )
                )
                continue
            bound_job = mapping[unit["unit_id"]]
            attempts = unit["execution"].setdefault("attempts", [])
            if attempts:
                raise InvalidArgument(f"unit {unit['unit_id']} already has a bound attempt")
            attempt = {
                "attempt_id": uuid.uuid4().hex,
                "job_id": bound_job,
                "group_job_id": array_job_id or job_id or bound_job,
                "adopted_at": adopted_at,
                "identity_confidence": "weak",
            }
            attempts.append(attempt)
            unit["execution"].update(state="UNKNOWN", current_attempt=attempt)
            events.append(
                _event(
                    snapshot,
                    "job_adopted",
                    stage=stage,
                    unit_id=unit["unit_id"],
                    attempt_id=attempt["attempt_id"],
                    job_id=bound_job,
                    identity_confidence="weak",
                )
            )
        self._summarize(snapshot)
        self._commit_snapshot(snapshot, events)
        return self._page(snapshot, include_units=False)

    def status(
        self,
        name: str,
        *,
        run_id: str | None = None,
        refresh: bool = False,
        include_units: bool = False,
        offset: int = 0,
        limit: int = 200,
        stage: str | None = None,
    ) -> dict[str, Any]:
        run_id = self._resolve_run(name, run_id)
        if refresh:
            snapshot = self._refresh(name, run_id)
        else:
            # Attention needs all evidence axes, including their current age.
            snapshot = self.store.read_complete_snapshot(name, run_id)
        if stage is not None:
            snapshot["units"] = [unit for unit in snapshot["units"] if unit["stage"] == stage]
        return self._page(snapshot, include_units=include_units, offset=offset, limit=limit)

    @staticmethod
    def _attempt_state(state: str) -> str:
        return {
            "INTENDED": "INTENDED",
            "SUBMITTING": "INTENDED",
            "ACCEPTED": "PENDING",
            "PENDING": "PENDING",
            "RUNNING": "RUNNING",
            "COMPLETED": "COMPLETED",
            "CANCELLED": "CANCELLED",
            "FAILED": "FAILED",
            "INVALID": "FAILED",
            "REJECTED": "FAILED",
            "UNKNOWN": "UNKNOWN",
        }.get(state, "UNKNOWN")

    @staticmethod
    def _attempt_for(record: Mapping[str, Any]) -> dict[str, Any] | None:
        current = record.get("current_attempt")
        return next(
            (
                dict(attempt)
                for attempt in reversed(record.get("attempts", []))
                if attempt.get("attempt_id") == current
            ),
            None,
        )

    def _merge_submission_outcomes(
        self,
        name: str,
        run_id: str,
        outcomes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """CAS-merge remotely durable outcomes without replacing attempt history."""

        for _ in range(8):
            snapshot = self.store.read_complete_snapshot(name, run_id)
            by_id = {unit["unit_id"]: unit for unit in snapshot["units"]}
            events: list[dict[str, Any]] = []
            for outcome in outcomes:
                group = outcome["group"]
                record = outcome["record"]
                attempt = self._attempt_for(record)
                if attempt is None:
                    continue
                group_id = str(group["group_id"])
                group_new = False
                for item in group["mapping"]:
                    unit = by_id.get(item["unit_id"])
                    if unit is None:
                        continue
                    base_job = attempt.get("job_id")
                    job_id = None
                    if base_job:
                        job_id = (
                            str(base_job)
                            if group["mode"] == "single"
                            else f"{base_job}_{int(item['index'])}"
                        )
                    unit_attempt = {
                        "attempt_id": attempt["attempt_id"],
                        "group_id": group_id,
                        "group_job_id": base_job,
                        "job_id": job_id,
                        "mode": group["mode"],
                        "array_index": int(item["index"]),
                        "marker": attempt.get("marker"),
                        "state": attempt.get("state"),
                        "script_sha256": attempt.get("script_sha256"),
                        "accepted_at": attempt.get("accepted_at"),
                        "recovered": bool(attempt.get("recovered")),
                    }
                    if group["mode"] == "pack":
                        unit_attempt["slot"] = int(item["slot"])
                        unit_attempt["marker_dir"] = (
                            f"{group['marker_root'].rstrip('/')}/{group_id}/"
                            f"{attempt['attempt_id']}/{item['unit_id']}"
                        )
                    execution = unit["execution"]
                    history = execution.setdefault("attempts", [])
                    existing = next(
                        (
                            old
                            for old in history
                            if old.get("attempt_id") == attempt["attempt_id"]
                            and old.get("group_id") == group_id
                        ),
                        None,
                    )
                    is_new_attempt = existing is None
                    group_new = group_new or is_new_attempt
                    if existing is None:
                        history.append(unit_attempt)
                        existing = unit_attempt
                    else:
                        existing.update(
                            {key: value for key, value in unit_attempt.items() if value is not None}
                        )
                    execution["current_attempt"] = existing
                    new_state = self._attempt_state(str(record.get("state")))
                    if is_new_attempt:
                        for key in (
                            "scheduler_state",
                            "scheduler_source",
                            "scheduler_observed_epoch",
                            "scheduler",
                            "marker_evidence",
                            "marker_status",
                            "identity_confidence",
                        ):
                            execution.pop(key, None)
                        execution["state"] = new_state
                        pending = pending_retry_authorization(execution)
                        if pending is not None:
                            pending["consumed_by"] = attempt["attempt_id"]
                            pending["consumed_at"] = _now()
                    elif execution.get("state") not in {"COMPLETED", "CANCELLED"} or new_state in {
                        "COMPLETED",
                        "CANCELLED",
                    }:
                        execution["state"] = new_state
                if group_new:
                    events.append(
                        _event(
                            snapshot,
                            "attempt_accepted" if attempt.get("job_id") else "submission_intended",
                            stage=group["stage"],
                            group_id=group_id,
                            attempt_id=attempt["attempt_id"],
                            group_job_id=attempt.get("job_id"),
                            state=record.get("state"),
                            unit_count=len(group["mapping"]),
                            unit_examples=[item["unit_id"] for item in group["mapping"][:20]],
                        )
                    )
            self._derive_dependencies(snapshot)
            self._summarize(snapshot)
            if not events:
                return snapshot
            try:
                self._commit_snapshot(snapshot, events)
                return snapshot
            except StoreConflict:
                continue
        raise StoreConflict("campaign changed repeatedly while merging submission outcomes")

    def apply(
        self,
        name: str,
        *,
        run_id: str | None = None,
        stage: str | None = None,
        unit_id: str | None = None,
        max_groups: int = 100,
        require_preflight: str | None = None,
        _unit_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        """Submit one bounded deterministic pass of currently eligible work."""

        if max_groups < 1 or max_groups > 1000:
            raise InvalidArgument("max_groups must be from 1 to 1000")
        run_id = self._resolve_run(name, run_id)
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot.get("lifecycle") != "OPEN":
            raise InvalidArgument("campaign apply requires an open run")
        pending_before = {
            str(pending["authorization_id"])
            for unit in snapshot["units"]
            if (pending := pending_retry_authorization(unit["execution"])) is not None
        }
        try:
            snapshot = self._refresh(name, run_id)
        except StoreConflict:
            # A concurrent observer/apply already committed newer evidence. Re-plan from HEAD.
            snapshot = self.store.read_complete_snapshot(name, run_id)
        if stage is not None and stage not in snapshot.get("stage_specs", {}):
            raise InvalidArgument(f"campaign has no stage {stage!r}")
        selected_ids = set(_unit_ids) if _unit_ids is not None else None
        if unit_id is not None:
            matches = [unit["unit_id"] for unit in snapshot["units"] if unit["unit_id"] == unit_id]
            if not matches:
                raise InvalidArgument(f"campaign has no unit {unit_id!r}")
            selected_ids = {unit_id} if selected_ids is None else selected_ids & {unit_id}
        preflight = None
        if require_preflight is not None:
            preflight = self._require_snapshot_preflight(snapshot, require_preflight or None)
        campaign_root = self.cluster.call(
            "expandpath",
            path=self.cluster.host.campaign_dir or "~/.remoteslurm/campaigns",
            _timeout=30,
        )["path"]
        marker_root = f"{campaign_root.rstrip('/')}/{name}/runs/{run_id}/markers"
        groups = plan_job_groups(
            snapshot,
            stage=stage,
            unit_ids=selected_ids,
            marker_root=marker_root,
        )
        planned = len(groups)
        groups = groups[:max_groups]
        voided_retries = sorted(
            unit["unit_id"]
            for unit in snapshot["units"]
            if (authorization := (unit["execution"].get("retry_authorizations") or [None])[-1])
            and authorization.get("authorization_id") in pending_before
            and authorization.get("voided_at")
        )
        control = submission_control(self.cluster) if groups else None
        outcomes_by_attempt: dict[tuple[str, str], dict[str, Any]] = {}
        by_id = {unit["unit_id"]: unit for unit in snapshot["units"]}
        for group in groups:
            attempt_id = uuid.uuid4().hex
            stage_spec = snapshot["stage_specs"][group["stage"]]
            script = render_group_script(group, stage_spec, attempt_id=attempt_id)
            flags = slurm.sbatch_args_from_options(dict(sorted(group["resources"].items())))
            array = array_spec(group)
            if array is not None:
                flags.append("--array=" + array)
            retry = False
            retry_unknown = False
            retry_claims: list[dict[str, Any]] = []
            for item in group["mapping"]:
                execution = by_id[item["unit_id"]]["execution"]
                authorization = pending_retry_authorization(execution)
                if authorization:
                    retry = retry or execution.get("state") != "UNKNOWN"
                    retry_unknown = retry_unknown or execution.get("state") == "UNKNOWN"
                    retry_claims.append(
                        {
                            "unit_id": item["unit_id"],
                            "authorization_id": authorization["authorization_id"],
                            "expected_group_id": authorization.get("previous_group_id"),
                            "expected_attempt_id": authorization.get("previous_attempt_id"),
                            "accept_duplicate_risk": bool(
                                authorization.get("accept_duplicate_risk")
                            ),
                        }
                    )
            contract = {key: value for key, value in group.items() if key != "group_id"}
            remote = ensure_campaign_attempt(
                self.cluster,
                campaign=name,
                run_id=run_id,
                group_id=group["group_id"],
                contract=contract,
                script=script,
                flags=flags,
                cwd=str(group["cwd"]),
                retry=retry,
                retry_unknown=retry_unknown,
                retry_claims=retry_claims,
                attempt_id=attempt_id,
                control=control,
            )
            remote_outcomes = remote.get("outcomes")
            if not isinstance(remote_outcomes, list):
                remote_outcomes = [
                    {
                        "group": remote.get("group") or group,
                        "record": remote["record"],
                        "submitted": bool(remote.get("submitted")),
                    }
                ]
            for outcome in remote_outcomes:
                actual_group = dict(outcome.get("group") or group)
                record = dict(outcome["record"])
                actual_attempt = self._attempt_for(record) or {}
                key = (str(actual_group["group_id"]), str(actual_attempt.get("attempt_id")))
                normalized = {
                    "group": actual_group,
                    "record": record,
                    "submitted": bool(outcome.get("submitted")),
                }
                old = outcomes_by_attempt.get(key)
                if old is None or normalized["submitted"]:
                    outcomes_by_attempt[key] = normalized
        outcomes = list(outcomes_by_attempt.values())
        if outcomes:
            merged = self._merge_submission_outcomes(name, run_id, outcomes)
        else:
            merged = snapshot
        return {
            "campaign": name,
            "run_id": run_id,
            "definition_id": snapshot["definition_id"],
            "planned_groups": planned,
            "applied_groups": len(outcomes),
            "remaining_groups": max(0, planned - len(outcomes)),
            "voided_retry_units": voided_retries,
            "submitted_groups": sum(outcome["submitted"] for outcome in outcomes),
            "recovered_groups": sum(
                bool((self._attempt_for(outcome["record"]) or {}).get("recovered"))
                for outcome in outcomes
            ),
            "groups": [
                {
                    "group_id": outcome["group"]["group_id"],
                    "stage": outcome["group"]["stage"],
                    "mode": outcome["group"]["mode"],
                    "units": len(outcome["group"]["mapping"]),
                    "state": outcome["record"].get("state"),
                    "submitted": outcome["submitted"],
                    "attempt": self._attempt_for(outcome["record"]),
                }
                for outcome in outcomes
            ],
            "preflight_receipt": preflight.get("receipt_id") if preflight else None,
            "next_action": merged.get("next_action"),
        }

    @staticmethod
    def _unit_match(snapshot: Mapping[str, Any], selector: str) -> str:
        exact = [unit["unit_id"] for unit in snapshot["units"] if unit["unit_id"] == selector]
        if exact:
            return str(exact[0])
        prefixes = [
            unit["unit_id"]
            for unit in snapshot["units"]
            if unit["unit_id"].startswith(selector)
            or unit["unit_id"].partition(".")[2].startswith(selector)
        ]
        if len(prefixes) == 1:
            return str(prefixes[0])
        if not prefixes:
            raise InvalidArgument(f"campaign has no unit matching {selector!r}")
        raise InvalidArgument(
            f"unit prefix {selector!r} is ambiguous",
            candidates=prefixes[:20],
            candidate_count=len(prefixes),
        )

    def retry(
        self,
        name: str,
        *,
        run_id: str | None = None,
        stage: str | None = None,
        unit_id: str | None = None,
        where: Mapping[str, str] | None = None,
        states: Mapping[str, str] | None = None,
        reason: str,
        accept_duplicate_risk: bool = False,
        dry_run: bool = False,
        apply: bool = False,
        require_preflight: str | None = None,
        max_groups: int = 100,
    ) -> dict[str, Any]:
        """Append explicit retry authorization, optionally followed by one apply pass."""

        if not reason or not reason.strip():
            raise InvalidArgument("retry requires a non-empty reason")
        if dry_run and apply:
            raise InvalidArgument("retry dry_run cannot be combined with apply")
        if not any((stage, unit_id, where, states)):
            raise InvalidArgument(
                "retry requires an explicit stage, unit, where, or state selector"
            )
        run_id = self._resolve_run(name, run_id)
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot.get("lifecycle") != "OPEN":
            raise InvalidArgument("campaign retry requires an open run")
        exact_unit = self._unit_match(snapshot, unit_id) if unit_id else None
        valid_axes = {
            "execution": "execution",
            "artifact": "artifacts",
            "artifacts": "artifacts",
            "validation": "validation",
            "dependency": "dependency",
            "freshness": "freshness",
        }
        requested_states: dict[str, str] = {}
        for axis, state in (states or {}).items():
            if axis not in valid_axes:
                raise InvalidArgument(f"retry state axis must be one of {sorted(valid_axes)}")
            requested_states[valid_axes[axis]] = state.upper()
        candidates = []
        for unit in snapshot["units"]:
            if stage and unit["stage"] != stage:
                continue
            if exact_unit and unit["unit_id"] != exact_unit:
                continue
            if where and any(
                str(unit.get("keys", {}).get(key)) != value for key, value in where.items()
            ):
                continue
            if any(unit[axis].get("state") != state for axis, state in requested_states.items()):
                continue
            if retry_eligible(unit):
                candidates.append(unit)
        if not candidates:
            raise InvalidArgument(
                "retry selection contains no failed, cancelled, invalid, or unresolved units"
            )
        unknown = [unit["unit_id"] for unit in candidates if _duplicate_risk(unit["execution"])]
        if dry_run:
            return {
                "campaign": name,
                "run_id": run_id,
                "authorized": 0,
                "selected": len(candidates),
                "unit_ids": [unit["unit_id"] for unit in candidates],
                "duplicate_risk_units": unknown,
                "dry_run": True,
                "applied": False,
            }
        if unknown and not accept_duplicate_risk:
            raise InvalidArgument(
                "UNKNOWN attempts and cancellations not yet confirmed by the scheduler require "
                "explicit duplicate-risk authorization",
                action=(
                    "refresh the run to confirm cancellations from squeue/sacct, or repeat with "
                    "accept_duplicate_risk=True after checking scheduler evidence"
                ),
                units=unknown[:20],
                unit_count=len(unknown),
            )
        authorization_id = uuid.uuid4().hex
        authorized_at = _now()
        for unit in candidates:
            current = unit["execution"].get("current_attempt") or {}
            unit["execution"].setdefault("retry_authorizations", []).append(
                {
                    "authorization_id": authorization_id,
                    "authorized_at": authorized_at,
                    "reason": reason.strip(),
                    "accept_duplicate_risk": _duplicate_risk(unit["execution"]),
                    "execution_state": unit["execution"]["state"],
                    "validation_state": unit["validation"]["state"],
                    "previous_attempt_id": current.get("attempt_id"),
                    "previous_group_id": current.get("group_id"),
                }
            )
        self._summarize(snapshot)
        self._commit_snapshot(
            snapshot,
            [
                _event(
                    snapshot,
                    "retry_authorized",
                    authorization_id=authorization_id,
                    reason=reason.strip(),
                    unit_ids=[unit["unit_id"] for unit in candidates],
                    accept_duplicate_risk=bool(unknown),
                )
            ],
        )
        result: dict[str, Any] = {
            "campaign": name,
            "run_id": run_id,
            "authorization_id": authorization_id,
            "authorized": len(candidates),
            "unit_ids": [unit["unit_id"] for unit in candidates],
            "dry_run": False,
            "applied": False,
        }
        if apply:
            result["apply"] = self.apply(
                name,
                run_id=run_id,
                max_groups=max_groups,
                require_preflight=require_preflight,
                _unit_ids=set(result["unit_ids"]),
            )
            result["applied"] = True
        return result

    def cancel(
        self,
        name: str,
        *,
        run_id: str | None = None,
        stage: str | None = None,
        unit_id: str | None = None,
        all_active: bool = False,
        apply: bool = False,
        confirm: bool = False,
    ) -> dict[str, Any]:
        """Plan or execute bounded cancellation of recorded active allocations."""

        if not any((stage, unit_id, all_active)):
            raise InvalidArgument("cancel requires stage, unit, or all_active")
        if all_active and (stage or unit_id):
            raise InvalidArgument("all_active cannot be combined with stage or unit")
        run_id = self._resolve_run(name, run_id)
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot.get("lifecycle") == "ARCHIVED":
            raise InvalidArgument("archived runs are read-only; restore before cancellation")
        exact_unit = self._unit_match(snapshot, unit_id) if unit_id else None
        active_states = {"INTENDED", "PENDING", "RUNNING", "UNKNOWN"}
        selected = [
            unit
            for unit in snapshot["units"]
            if unit["execution"]["state"] in active_states
            and (all_active or stage is None or unit["stage"] == stage)
            and (exact_unit is None or unit["unit_id"] == exact_unit)
            and unit["execution"].get("current_attempt", {}).get("job_id")
        ]
        job_ids = sorted({str(unit["execution"]["current_attempt"]["job_id"]) for unit in selected})
        if len(job_ids) > 1000:
            raise InvalidArgument("cancel selection exceeds the 1000-job bound")
        # Packed siblings share an allocation job ID. A sibling that already finished keeps its
        # terminal state; only work that could still be running becomes CANCELLED.
        affected = [
            unit
            for unit in snapshot["units"]
            if str(unit["execution"].get("current_attempt", {}).get("job_id")) in job_ids
            and unit["execution"]["state"] in active_states
        ]
        result: dict[str, Any] = {
            "campaign": name,
            "run_id": run_id,
            "job_ids": job_ids,
            "selected_units": [unit["unit_id"] for unit in selected],
            "affected_units": [unit["unit_id"] for unit in affected],
            "applied": False,
        }
        if not apply or not job_ids:
            return result
        result["scheduler"] = self.cluster.cancel(job_ids, confirm=confirm)
        cancelled_at = _now()
        cancelled_epoch = time.time()
        for unit in affected:
            execution = unit["execution"]
            attempt = execution.get("current_attempt")
            job_id = str(attempt.get("job_id")) if attempt else None
            previous_scheduler = execution.get("scheduler", {})
            scheduler_record = {
                "job_id": job_id,
                "state": "CANCELLED",
                "source": "scancel",
                "terminal": True,
                "accounting_pending": False,
                "allocated_cpus": previous_scheduler.get("allocated_cpus"),
                "elapsed": previous_scheduler.get("elapsed"),
                "submit_time": previous_scheduler.get("submit_time"),
                "evidence": [
                    {
                        "source": "scancel",
                        "available": True,
                        "matched": True,
                        "observed_at": cancelled_epoch,
                        "durable": True,
                    }
                ],
            }
            if attempt is not None:
                history_attempt = next(
                    (
                        item
                        for item in reversed(execution.get("attempts", []))
                        if item.get("attempt_id") == attempt.get("attempt_id")
                        and item.get("group_id") == attempt.get("group_id")
                    ),
                    None,
                )
                for attempt_record in (attempt, history_attempt):
                    if attempt_record is not None:
                        attempt_record.update(
                            scheduler_state="CANCELLED",
                            scheduler_source="scancel",
                            scheduler_observed_epoch=cancelled_epoch,
                            scheduler=scheduler_record,
                        )
            execution.update(
                state="CANCELLED",
                cancelled_at=cancelled_at,
                scheduler_state="CANCELLED",
                scheduler_source="scancel",
                scheduler_observed_epoch=cancelled_epoch,
                scheduler=scheduler_record,
            )
        self._summarize(snapshot)
        self._commit_snapshot(
            snapshot,
            [
                _event(
                    snapshot,
                    "attempts_cancelled",
                    job_ids=job_ids,
                    unit_ids=[unit["unit_id"] for unit in affected],
                )
            ],
        )
        result["applied"] = True
        return result

    def drive(
        self,
        name: str,
        *,
        run_id: str | None = None,
        max_passes: int = 20,
        interval: float = 10.0,
        max_groups: int = 100,
        require_preflight: str | None = None,
    ) -> dict[str, Any]:
        """Run bounded attached refresh/apply passes without automatic validation or retry."""

        if max_passes < 1 or max_passes > 100:
            raise InvalidArgument("max_passes must be from 1 to 100")
        if interval < 0 or interval > 3600:
            raise InvalidArgument("interval must be from 0 to 3600 seconds")
        run_id = self._resolve_run(name, run_id)
        history: list[dict[str, Any]] = []
        for pass_number in range(1, max_passes + 1):
            applied = self.apply(
                name,
                run_id=run_id,
                max_groups=max_groups,
                require_preflight=require_preflight,
            )
            refreshed = self.status(name, run_id=run_id, refresh=True)
            active = sum(
                stage["execution"]["INTENDED"]
                + stage["execution"]["PENDING"]
                + stage["execution"]["RUNNING"]
                for stage in refreshed["stages"]
            )
            history.append(
                {
                    "pass": pass_number,
                    "applied_groups": applied["applied_groups"],
                    "remaining_groups": applied["remaining_groups"],
                    "active_units": active,
                    "next_action": refreshed["next_action"],
                }
            )
            if applied["remaining_groups"] == 0 and applied["applied_groups"] == 0 and active == 0:
                break
            if pass_number < max_passes and interval:
                time.sleep(interval)
        return {
            "campaign": name,
            "run_id": run_id,
            "passes": len(history),
            "history": history,
            "next_action": history[-1]["next_action"],
        }

    def _scheduler_observations(
        self, snapshot: dict[str, Any]
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
        job_ids = sorted(
            {
                unit.get("execution", {}).get("current_attempt", {}).get("job_id")
                for unit in snapshot["units"]
                if unit.get("execution", {}).get("current_attempt", {}).get("job_id")
            }
        )
        availability: dict[str, Any] = {}
        queue: dict[str, dict[str, Any]] = {}
        accounting: dict[str, dict[str, Any]] = {}
        sampled_at = _now()
        try:
            rows = self.cluster.squeue(refresh=True)
            queue = {row["job_id"]: row for row in rows if row["job_id"] in job_ids}
            availability["squeue"] = {"available": True, "observed_at": sampled_at}
        except RemoteSlurmError as exc:
            availability["squeue"] = {
                "available": False,
                "observed_at": sampled_at,
                "reason": exc.message,
            }
        accounting_errors: list[str] = []
        for start in range(0, len(job_ids), 1000):
            try:
                accounting.update(self.cluster.sacct(job_ids[start : start + 1000]))
            except RemoteSlurmError as exc:
                accounting_errors.append(exc.message)
        availability["sacct"] = {
            "available": not accounting_errors,
            "observed_at": sampled_at,
        }
        if accounting_errors:
            availability["sacct"]["reason"] = "; ".join(accounting_errors[:3])
        return queue, accounting, availability

    def _artifact_observations(
        self, snapshot: dict[str, Any]
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        outputs = [
            output
            for unit in snapshot["units"]
            for output in unit.get("artifacts", {}).get("outputs", [])
        ]
        records: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        sampled_at = _now()
        for start in range(0, len(outputs), 1024):
            batch = outputs[start : start + 1024]
            try:
                result = self.cluster.call(
                    "contract_observe",
                    items=[observation_request(output) for output in batch],
                    _timeout=300,
                )
                for output, record in zip(batch, result.get("items", []), strict=False):
                    records[str(output["path"])] = record
            except RemoteSlurmError as exc:
                errors.append(exc.message)
        availability: dict[str, Any] = {
            "available": not errors,
            "observed_at": sampled_at,
            "queried": len(outputs),
        }
        if errors:
            availability["reason"] = "; ".join(errors[:3])
        return records, availability

    def _marker_observations(
        self, snapshot: Mapping[str, Any]
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        paths = sorted(
            {
                str(attempt["marker_dir"])
                for unit in snapshot.get("units", [])
                if (attempt := unit.get("execution", {}).get("current_attempt"))
                and attempt.get("mode") == "pack"
                and attempt.get("marker_dir")
            }
        )
        records: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        for start in range(0, len(paths), 1024):
            try:
                result = self.cluster.call(
                    "campaign_markers",
                    campaign_dir=self.cluster.host.campaign_dir,
                    paths=paths[start : start + 1024],
                    _timeout=120,
                )
                records.update({str(item["path"]): item for item in result.get("items", [])})
            except RemoteSlurmError as exc:
                errors.append(exc.message)
        availability: dict[str, Any] = {
            "available": not errors,
            "observed_at": _now(),
            "queried": len(paths),
        }
        if errors:
            availability["reason"] = "; ".join(errors[:3])
        return records, availability

    def _refresh(self, name: str, run_id: str) -> dict[str, Any]:
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot.get("lifecycle") == "ARCHIVED":
            raise InvalidArgument("archived runs are read-only; restore the run before refresh")
        now_epoch = time.time()
        queue, accounting, availability = self._scheduler_observations(snapshot)
        path_records, artifact_availability = self._artifact_observations(snapshot)
        marker_records, marker_availability = self._marker_observations(snapshot)
        availability["filesystem"] = artifact_availability
        availability["pack_markers"] = marker_availability
        scheduler_observation_count = 0
        artifact_observation_count = 0

        for unit in snapshot["units"]:
            execution = unit["execution"]
            attempt = execution.get("current_attempt")
            scheduler_available = bool(
                availability["squeue"]["available"] or availability["sacct"]["available"]
            )
            if attempt and attempt.get("job_id"):
                job_id = str(attempt["job_id"])
                observations = []
                if availability["squeue"]["available"]:
                    row = queue.get(job_id)
                    observations.append(
                        observed(
                            "squeue",
                            now_epoch,
                            {"state": row["state"]} if row else None,
                            identity=(
                                ObservationIdentity(
                                    cluster=self.cluster.host.name,
                                    job_id=job_id,
                                    submit_time=row.get("submit_time") or None,
                                )
                                if row
                                else None
                            ),
                        )
                    )
                else:
                    observations.append(
                        unavailable(
                            "squeue",
                            now_epoch,
                            availability["squeue"].get("reason", "unavailable"),
                        )
                    )
                if availability["sacct"]["available"]:
                    acct = accounting.get(job_id)
                    observations.append(
                        observed(
                            "sacct",
                            now_epoch,
                            {"state": acct["state"]} if acct else None,
                            identity=(
                                ObservationIdentity(
                                    cluster=self.cluster.host.name,
                                    job_id=job_id,
                                    submit_time=acct.get("submit_time") or None,
                                )
                                if acct
                                else None
                            ),
                            durable=True,
                        )
                    )
                else:
                    observations.append(
                        unavailable(
                            "sacct",
                            now_epoch,
                            availability["sacct"].get("reason", "unavailable"),
                        )
                    )
                previous_state = attempt.get("scheduler_state")
                previous_at = attempt.get("scheduler_observed_epoch")
                if previous_state and previous_at:
                    previous_scheduler = attempt.get("scheduler", {})
                    observations.append(
                        observed(
                            "registry",
                            float(previous_at),
                            {"state": previous_state, "last_seen": float(previous_at)},
                            identity=ObservationIdentity(
                                cluster=self.cluster.host.name,
                                job_id=job_id,
                                submit_time=previous_scheduler.get("submit_time") or None,
                            ),
                            durable=True,
                        )
                    )
                reconciled = reconcile_job(job_id, observations, now=now_epoch, grace_seconds=600.0)
                scheduler = queue.get(job_id) or accounting.get(job_id) or {}
                scheduler_record = {
                    "job_id": job_id,
                    "state": reconciled.state,
                    "source": reconciled.source,
                    "terminal": reconciled.terminal,
                    "accounting_pending": reconciled.accounting_pending,
                    "allocated_cpus": _int_or_none(scheduler.get("alloc_cpus")),
                    "elapsed": scheduler.get("elapsed") or scheduler.get("time_used"),
                    "submit_time": scheduler.get("submit_time"),
                    "evidence": list(reconciled.sources),
                }
                history_attempt = next(
                    (
                        item
                        for item in reversed(execution.get("attempts", []))
                        if item.get("attempt_id") == attempt.get("attempt_id")
                        and item.get("group_id") == attempt.get("group_id")
                    ),
                    None,
                )
                for attempt_record in (attempt, history_attempt):
                    if attempt_record is not None:
                        attempt_record.update(
                            scheduler_state=reconciled.state,
                            scheduler_source=reconciled.source,
                            scheduler_observed_epoch=reconciled.observed_at,
                            scheduler=scheduler_record,
                        )
                execution.update(
                    state=_execution_projection(reconciled.state),
                    scheduler_state=reconciled.state,
                    scheduler_source=reconciled.source,
                    scheduler_observed_epoch=reconciled.observed_at,
                    scheduler=scheduler_record,
                )
                scheduler_observation_count += 1

            if attempt and attempt.get("mode") == "pack" and attempt.get("marker_dir"):
                marker_path = str(attempt["marker_dir"])
                marker = marker_records.get(marker_path, {"path": marker_path})
                execution["marker_evidence"] = marker
                finished = marker.get("finished")
                if isinstance(finished, Mapping):
                    exit_code = _int_or_none(finished.get("exit_code"))
                    execution["state"] = "COMPLETED" if exit_code == 0 else "FAILED"
                    execution["identity_confidence"] = "per_unit_marker"
                    execution["marker_status"] = "finished"
                elif isinstance(marker.get("started"), Mapping):
                    scheduler = execution.get("scheduler", {})
                    if scheduler.get("terminal"):
                        execution["state"] = "UNKNOWN"
                        execution["identity_confidence"] = "incomplete_marker"
                        execution["marker_status"] = "started_without_finish"
                    else:
                        execution["state"] = "RUNNING"
                        execution["identity_confidence"] = "per_unit_marker"
                        execution["marker_status"] = "started"
                elif marker_availability["available"] and attempt.get("job_id"):
                    scheduler = execution.get("scheduler", {})
                    if scheduler.get("terminal"):
                        execution["state"] = "UNKNOWN"
                    elif execution.get("state") in {"PENDING", "RUNNING"}:
                        execution["state"] = "PENDING"
                    execution["identity_confidence"] = "allocation_only"
                    execution["marker_status"] = "not_observed"

            artifacts = unit["artifacts"]
            output_results: list[dict[str, Any]] = []
            for output in artifacts.get("outputs", []):
                current = path_records.get(output["path"])
                result = dict(output)
                if current is not None:
                    result["observation"] = current
                    evaluation = evaluate_output(
                        output,
                        current,
                        prior_stability=output.get("stability"),
                        observed_epoch=float(current.get("observed_at", now_epoch)),
                    )
                    result["contract_evidence"] = evaluation
                    result["stability"] = evaluation.get("stability")
                output_results.append(result)
            artifacts["outputs"] = output_results
            if not output_results:
                artifacts["state"] = "PRESENT"
            elif not artifact_availability["available"]:
                artifacts["state"] = "ERROR"
            elif any(
                result.get("contract_evidence", {}).get("state") == "ERROR"
                for result in output_results
            ):
                artifacts["state"] = "ERROR"
            elif any(
                result.get("contract_evidence", {}).get("state") == "MISSING"
                for result in output_results
            ):
                artifacts["state"] = "MISSING"
            elif any(
                result.get("contract_evidence", {}).get("state") == "SETTLING"
                for result in output_results
            ):
                artifacts["state"] = "SETTLING"
            else:
                artifacts["state"] = "PRESENT"
            current_observations = [
                output["observation"]
                for output in output_results
                if isinstance(output.get("observation"), Mapping)
            ]
            current_signature = artifact_signature(current_observations)
            artifacts["signature"] = current_signature
            verified_signature = unit["validation"].get("artifact_signature")
            if (
                verified_signature
                and current_signature != verified_signature
                and unit["validation"]["state"] in {"PASSED", "STALE"}
            ):
                artifacts["state"] = "CHANGED"
            artifacts["observed_at"] = artifact_availability["observed_at"]
            if unit["validation"]["state"] == "PASSED" and artifacts["state"] == "CHANGED":
                unit["validation"]["state"] = "STALE"
            available_axes = int(scheduler_available or not attempt) + int(
                artifact_availability["available"]
            )
            if execution.get("scheduler_source") == "conflict":
                unit["freshness"] = {"state": "CONFLICT", "observed_at": _now()}
            elif available_axes == 2:
                unit["freshness"] = {"state": "FRESH", "observed_at": _now()}
            elif available_axes == 1:
                unit["freshness"] = {"state": "PARTIAL", "observed_at": _now()}
            else:
                unit["freshness"] = {"state": "UNAVAILABLE", "observed_at": _now()}
            artifact_observation_count += len(output_results)

        self._derive_dependencies(snapshot)
        voided = self._void_stale_retry_authorizations(snapshot)
        snapshot["telemetry"] = self._sample_telemetry(snapshot)
        snapshot["source_availability"] = availability
        snapshot["refreshed_at"] = _now()
        self._summarize(snapshot)
        events = [
            _event(
                snapshot,
                "scheduler_observed",
                observation_count=scheduler_observation_count,
                source_availability={
                    "squeue": availability["squeue"],
                    "sacct": availability["sacct"],
                },
                evidence_location="committed unit view",
            ),
            _event(
                snapshot,
                "artifact_observed",
                observation_count=artifact_observation_count,
                source_availability=availability["filesystem"],
                evidence_location="committed unit view",
            ),
        ]
        if voided:
            events.append(
                _event(
                    snapshot,
                    "retry_authorizations_voided",
                    unit_ids=voided,
                    reason="refreshed evidence shows the attempt live or valid",
                )
            )
        self._commit_snapshot(snapshot, events)
        return snapshot

    @staticmethod
    def _void_stale_retry_authorizations(snapshot: dict[str, Any]) -> list[str]:
        """Void pending authorizations whose unit no longer needs replacing.

        An authorization approves replacing the attempt as it looked then. If fresh evidence
        shows that attempt live again or its output valid, a later failure must be authorized
        anew rather than resubmitted by a stale approval (for example, during ``drive``).
        """
        voided: list[str] = []
        voided_at = _now()
        for unit in snapshot["units"]:
            pending = pending_retry_authorization(unit["execution"])
            if pending is None or retry_eligible(unit):
                continue
            pending["voided_at"] = voided_at
            pending["voided_reason"] = (
                f"execution {unit['execution'].get('state')}, "
                f"validation {unit['validation'].get('state')}"
            )
            voided.append(str(unit["unit_id"]))
        return voided

    def _observe_outputs(
        self, outputs: list[dict[str, Any]]
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        records: dict[str, dict[str, Any]] = {}
        limitations: list[str] = []
        for start in range(0, len(outputs), 1024):
            batch = outputs[start : start + 1024]
            try:
                observed_batch = self.cluster.call(
                    "contract_observe",
                    items=[observation_request(output) for output in batch],
                    _timeout=300,
                )
                for output, evidence in zip(batch, observed_batch.get("items", []), strict=False):
                    records[str(output["path"])] = evidence
            except RemoteSlurmError as exc:
                limitations.append(exc.message)
                for output in batch:
                    records[str(output["path"])] = {
                        "path": output["path"],
                        "matches": [],
                        "error": {"code": exc.code, "message": exc.message},
                        "observed_at": time.time(),
                    }
        return records, limitations

    @staticmethod
    def _control_identity(info: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "hostname": info.get("hostname"),
            "user": info.get("user"),
            "python": info.get("python"),
            "protocol": info.get("protocol"),
            "stub_sha": info.get("stub_sha"),
            "slurm_version": info.get("slurm_version"),
            "slurm_tools": info.get("slurm_tools"),
            "env": info.get("env", {}),
        }

    def _validate_units(
        self,
        units: list[dict[str, Any]],
        *,
        remote_root: str,
        settle: bool,
        control: dict[str, Any],
        receipt_context: tuple[str, str] | None,
    ) -> list[dict[str, Any]]:
        """Run bounded metadata checks and audited argv validators for selected units."""

        outputs = [
            output for unit in units for output in unit.get("artifacts", {}).get("outputs", [])
        ]
        started = time.monotonic()
        limitations: list[str] = []
        while True:
            records, observed_limitations = self._observe_outputs(outputs)
            limitations.extend(observed_limitations)
            now_epoch = time.time()
            pending: list[dict[str, Any]] = []
            for output in outputs:
                observation = records[str(output["path"])]
                evaluation = evaluate_output(
                    output,
                    observation,
                    prior_stability=output.get("stability"),
                    observed_epoch=float(observation.get("observed_at", now_epoch)),
                )
                output["observation"] = observation
                output["contract_evidence"] = evaluation
                output["stability"] = evaluation.get("stability")
                if evaluation["state"] == "SETTLING":
                    elapsed = time.monotonic() - started
                    timeout = float(output.get("settle_timeout", 0))
                    if not settle:
                        continue
                    if elapsed >= timeout:
                        output["contract_evidence"] = {
                            **evaluation,
                            "state": "ERROR",
                            "passed": False,
                            "failure_codes": ["settling_timeout"],
                            "settling_timeout": timeout,
                            "elapsed": elapsed,
                        }
                    else:
                        pending.append(output)
            if not pending or not settle:
                break
            interval = min(float(output.get("settle_interval", 0.2)) for output in pending)
            remaining = min(
                float(output.get("settle_timeout", 0)) - (time.monotonic() - started)
                for output in pending
            )
            time.sleep(max(0.0, min(interval, remaining)))

        results: list[dict[str, Any]] = []
        for unit in units:
            unit_outputs = unit.get("artifacts", {}).get("outputs", [])
            metadata_passed = all(
                output.get("contract_evidence", {}).get("passed") for output in unit_outputs
            )
            metadata_states = [
                output.get("contract_evidence", {}).get("state", "ERROR") for output in unit_outputs
            ]
            validator_evidence: list[dict[str, Any]] = []
            validator_failed = False
            validator_error = False
            if metadata_passed:
                for validator in unit.get("contract", {}).get("validators", []):
                    checked_at = _now()
                    try:
                        rendered = render_validator(validator, unit, remote_root=remote_root)
                        response = self.cluster.run(
                            rendered["argv"],
                            cwd=rendered["cwd"],
                            env=rendered["env"],
                            timeout=rendered["timeout"],
                            max_output=MAX_VALIDATOR_OUTPUT,
                        )
                        evidence = {
                            "kind": "command",
                            "checked_at": checked_at,
                            **rendered,
                            "rc": response.get("rc"),
                            "stdout": str(response.get("stdout") or ""),
                            "stderr": str(response.get("stderr") or ""),
                            "stdout_truncated": bool(response.get("stdout_truncated", False)),
                            "stderr_truncated": bool(response.get("stderr_truncated", False)),
                            "status": "PASSED" if response.get("rc") == 0 else "FAILED",
                        }
                        if response.get("rc") != 0:
                            evidence["failure_code"] = "validator_exit"
                            validator_failed = True
                    except InvalidArgument as exc:
                        evidence = {
                            "kind": "command",
                            "checked_at": checked_at,
                            "status": "ERROR",
                            "failure_code": "validator_contract_error",
                            "error": exc.to_dict(),
                        }
                        validator_error = True
                    except RemoteSlurmError as exc:
                        failure_code = (
                            "validator_timeout" if exc.code == "timeout" else "validator_crash"
                        )
                        evidence = {
                            "kind": "command",
                            "checked_at": checked_at,
                            "status": "ERROR",
                            "failure_code": failure_code,
                            "error": exc.to_dict(),
                        }
                        validator_error = True
                    validator_evidence.append(evidence)

            if not metadata_passed:
                state = (
                    "ERROR"
                    if "ERROR" in metadata_states or "SETTLING" in metadata_states
                    else "FAILED"
                )
            elif validator_error:
                state = "ERROR"
            elif validator_failed:
                state = "FAILED"
            else:
                state = "PASSED"
            observations = [output.get("observation", {}) for output in unit_outputs]
            signature = artifact_signature(observations)
            receipt = {
                "schema": 1,
                "kind": "validation",
                "campaign": receipt_context[0] if receipt_context else None,
                "run_id": receipt_context[1] if receipt_context else None,
                "unit_id": unit["unit_id"],
                "stage": unit["stage"],
                "contract_id": unit.get("contract", {}).get("contract_id"),
                "verified_at": _now(),
                "result": state,
                "expanded_outputs": unit_outputs,
                "artifact_signature": signature,
                "validators": validator_evidence,
                "control": control,
                "limitations": sorted(set(limitations)),
            }
            receipt_id = hashlib.sha256(canonical_json(receipt)).hexdigest()
            if receipt_context is not None:
                stored = self.store.put_receipt(
                    receipt_context[0],
                    "validation",
                    receipt,
                    run_id=receipt_context[1],
                )
                receipt_id = str(stored["receipt_id"])
            prior_validation = dict(unit.get("validation", {}))
            history = list(prior_validation.get("receipt_history", []))
            if prior_validation.get("receipt_id") and prior_validation["receipt_id"] != receipt_id:
                history.append(prior_validation["receipt_id"])
            unit["validation"] = {
                "state": state,
                "contract_id": unit.get("contract", {}).get("contract_id"),
                "receipt_id": receipt_id,
                "receipt_history": list(dict.fromkeys(history)),
                "verified_at": receipt["verified_at"],
                "artifact_signature": signature,
            }
            artifact_states = [
                output.get("contract_evidence", {}).get("state", "ERROR") for output in unit_outputs
            ]
            if not artifact_states or all(item == "PRESENT" for item in artifact_states):
                unit["artifacts"]["state"] = "PRESENT"
            elif "ERROR" in artifact_states:
                unit["artifacts"]["state"] = "ERROR"
            elif "SETTLING" in artifact_states:
                unit["artifacts"]["state"] = "SETTLING"
            else:
                unit["artifacts"]["state"] = "MISSING"
            unit["artifacts"]["signature"] = signature
            results.append(
                {
                    "unit_id": unit["unit_id"],
                    "stage": unit["stage"],
                    "state": state,
                    "receipt_id": receipt_id,
                    "receipt": receipt,
                }
            )
        return results

    def verify(
        self,
        name: str,
        *,
        run_id: str | None = None,
        stage: str | None = None,
        unit_id: str | None = None,
        settle: bool = True,
        offset: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Validate a bounded set of production units and store immutable receipts."""

        if offset < 0 or limit < 1 or limit > 500:
            raise InvalidArgument("verification page requires offset >= 0 and limit 1..500")
        run_id = self._resolve_run(name, run_id)
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot.get("lifecycle") == "ARCHIVED":
            raise InvalidArgument("archived runs require restore before verification")
        candidates = snapshot["units"]
        if stage is not None:
            candidates = [unit for unit in candidates if unit["stage"] == stage]
        if unit_id is not None:
            candidates = [unit for unit in candidates if unit["unit_id"] == unit_id]
        total = len(candidates)
        selected = candidates[offset : offset + limit]
        if not selected:
            raise InvalidArgument("verification selection contains no campaign units")
        control = self._control_identity(self.cluster.info(refresh=True))
        results = self._validate_units(
            selected,
            remote_root=str(snapshot.get("workspace", {}).get("remote_root", ""))
            or str(self.cluster.home),
            settle=settle,
            control=control,
            receipt_context=(name, run_id),
        )
        self._derive_dependencies(snapshot)
        self._summarize(snapshot)
        counts = Counter(result["state"] for result in results)
        event = _event(
            snapshot,
            "validation_recorded",
            selected=len(selected),
            counts=dict(counts),
            receipt_ids=[result["receipt_id"] for result in results],
        )
        self._commit_snapshot(snapshot, [event])
        return {
            "campaign": name,
            "run_id": run_id,
            "definition_id": snapshot["definition_id"],
            "selected": len(selected),
            "total_matching": total,
            "counts": dict(counts),
            "results": [
                {key: value for key, value in result.items() if key != "receipt"}
                for result in results
            ],
            "next_offset": offset + len(selected) if offset + len(selected) < total else None,
        }

    def _remote_preflight(
        self, definition: CampaignDefinition
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        info = self.cluster.info(refresh=True)
        control = self._control_identity(info)
        roots = [
            str(definition.workspace["remote_root"]),
            str(definition.workspace["output_root"]),
        ]
        validator_tools: list[dict[str, Any]] = []
        try:
            root_records = self.cluster.call("path_stats", paths=roots, _timeout=60)["paths"]
            store = self.cluster.call(
                "campaign_check", campaign_dir=self.cluster.host.campaign_dir, _timeout=60
            )
            tool_names = sorted(
                {
                    str(validator["argv"][0])
                    for stage in definition.stages
                    for validator in stage.validators
                }
            )
            for start in range(0, len(tool_names), 64):
                validator_tools.extend(
                    self.cluster.call(
                        "tool_check", names=tool_names[start : start + 64], _timeout=30
                    ).get("tools", [])
                )
            checks = [
                {
                    "name": "remote_root",
                    "passed": bool(
                        root_records[0].get("exists")
                        and root_records[0].get("kind") == "directory"
                        and root_records[0].get("readable")
                        and root_records[0].get("executable")
                    ),
                    "evidence": root_records[0],
                },
                {
                    "name": "output_root",
                    "passed": bool(
                        (
                            root_records[1].get("exists")
                            and root_records[1].get("kind") == "directory"
                            and root_records[1].get("writable")
                        )
                        or (
                            not root_records[1].get("exists")
                            and root_records[1].get("parent_writable")
                        )
                    ),
                    "evidence": root_records[1],
                },
                {
                    "name": "campaign_store",
                    "passed": bool(store.get("writable") and store.get("atomic_rename")),
                    "evidence": store,
                },
                {
                    "name": "slurm_tools",
                    "passed": all(info.get("slurm_tools", {}).values()),
                    "evidence": info.get("slurm_tools", {}),
                },
                {
                    "name": "validator_tools",
                    "passed": all(tool.get("available") for tool in validator_tools),
                    "evidence": validator_tools,
                },
            ]
        except RemoteSlurmError as exc:
            checks = [
                {
                    "name": "remote_probe",
                    "passed": False,
                    "error": exc.to_dict(),
                }
            ]
            root_records = []
            store = {}
        identity_roots = [
            {
                key: record.get(key)
                for key in (
                    "path",
                    "exists",
                    "kind",
                    "readable",
                    "writable",
                    "executable",
                    "nearest_parent",
                    "parent_writable",
                    "error",
                )
                if key in record
            }
            for record in root_records
        ]
        environment = {
            "control": control,
            "roots": identity_roots,
            "store": store,
            "validator_tools": validator_tools,
        }
        return {
            "status": "PASSED" if all(check["passed"] for check in checks) else "FAILED",
            "checks": checks,
        }, environment

    @staticmethod
    def _preflight_key(
        definition: CampaignDefinition,
        against: str | None,
        environment: Mapping[str, Any],
    ) -> str:
        material = {
            "definition_id": definition.definition_id,
            "contract_ids": [stage_contract(stage)["contract_id"] for stage in definition.stages],
            "against": against,
            "pilot": definition.pilots.get(against) if against else None,
            "environment": environment,
        }
        return hashlib.sha256(canonical_json(material)).hexdigest()

    def preflight(
        self,
        definition: CampaignDefinition,
        *,
        against: str | None = None,
    ) -> dict[str, Any]:
        """Run static, remote, fixture, and optional named-pilot qualification."""

        if definition.host != self.cluster.host.name:
            raise InvalidArgument(
                f"campaign targets host {definition.host!r}, connected host is "
                f"{self.cluster.host.name!r}"
            )
        if against is not None and against not in definition.pilots:
            raise InvalidArgument(f"campaign has no pilot {against!r}")
        self.store.put_definition(definition)
        contracts = [stage_contract(stage) for stage in definition.stages]
        static = {
            "status": "PASSED",
            "definition_id": definition.definition_id,
            "unit_count": len(definition.units),
            "stage_count": len(definition.stages),
            "contract_ids": [contract["contract_id"] for contract in contracts],
            "checks": [
                "schema",
                "canonicalization",
                "inventory_identity",
                "dependency_dag",
                "path_alternatives",
                "output_collisions",
                "validator_identity",
            ],
        }
        remote, environment = self._remote_preflight(definition)
        fixture_results = fixture_evidence()
        fixtures = {
            "status": "PASSED" if all(item["passed"] for item in fixture_results) else "FAILED",
            "results": fixture_results,
        }
        if against is None:
            pilot_section: dict[str, Any] = {
                "status": "SKIPPED",
                "reason": "no named pilot requested",
            }
        else:
            coverage = pilot_alternative_coverage(definition, against)
            selected_units = pilot_units(definition, against)
            stage_map = {stage.name: stage for stage in definition.stages}
            transient = [
                self._initial_unit(definition, stage_map[unit.stage], unit)
                for unit in selected_units
            ]
            pilot_results = self._validate_units(
                transient,
                remote_root=str(definition.workspace["remote_root"]),
                settle=True,
                control=dict(environment["control"]),
                receipt_context=None,
            )
            coverage_passed = all(not item["missing"] for item in coverage)
            validation_passed = all(item["state"] == "PASSED" for item in pilot_results)
            pilot_section = {
                "status": "PASSED" if coverage_passed and validation_passed else "FAILED",
                "name": against,
                "selection": definition.pilots[against]["select"],
                "output_root": definition.pilots[against]["output_root"],
                "unit_count": len(transient),
                "alternative_coverage": coverage,
                "results": [
                    {key: value for key, value in result.items() if key != "receipt"}
                    for result in pilot_results
                ],
                "production_evidence": False,
            }
        sections = {
            "static": static,
            "remote": remote,
            "fixtures": fixtures,
            "pilot": pilot_section,
        }
        passed = all(section["status"] in {"PASSED", "SKIPPED"} for section in sections.values())
        receipt = {
            "schema": 1,
            "kind": "preflight",
            "campaign": definition.name,
            "definition_id": definition.definition_id,
            "against": against,
            "checked_at": _now(),
            "preflight_key": self._preflight_key(definition, against, environment),
            "result": "PASSED" if passed else "FAILED",
            "contracts": contracts,
            "environment": environment,
            "sections": sections,
            "limitations": [],
        }
        stored = self.store.put_receipt(definition.name, "preflight", receipt)
        return {"receipt_id": stored["receipt_id"], **receipt}

    def require_preflight(
        self, definition: CampaignDefinition, *, against: str | None = None
    ) -> dict[str, Any]:
        """Return the current passing receipt or fail before any scheduler mutation."""

        _remote, environment = self._remote_preflight(definition)
        expected = self._preflight_key(definition, against, environment)
        offset = 0
        observed_receipts = 0
        while observed_receipts < 5000:
            records = self.store.receipts(definition.name, "preflight", limit=500, offset=offset)
            for record in records.get("items", []):
                receipt = record.get("value", {})
                if receipt.get("preflight_key") == expected and receipt.get("result") == "PASSED":
                    return {"receipt_id": record["receipt_id"], **receipt}
            observed_receipts += len(records.get("items", []))
            next_offset = records.get("next_offset")
            if next_offset is None:
                break
            offset = int(next_offset)
        raise InvalidArgument(
            "no current passing preflight receipt matches this definition and environment",
            expected_preflight_key=expected,
            observed_receipts=observed_receipts,
        )

    def _snapshot_preflight_environment(self, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        info = self.cluster.info(refresh=True)
        roots = [
            str(snapshot["workspace"]["remote_root"]),
            str(snapshot["workspace"]["output_root"]),
        ]
        root_records = self.cluster.call("path_stats", paths=roots, _timeout=60)["paths"]
        store = self.cluster.call(
            "campaign_check", campaign_dir=self.cluster.host.campaign_dir, _timeout=60
        )
        tool_names = sorted(
            {
                str(validator["argv"][0])
                for spec in snapshot.get("stage_specs", {}).values()
                for validator in spec.get("validators", [])
            }
        )
        validator_tools: list[dict[str, Any]] = []
        for start in range(0, len(tool_names), 64):
            validator_tools.extend(
                self.cluster.call(
                    "tool_check", names=tool_names[start : start + 64], _timeout=30
                ).get("tools", [])
            )
        identity_roots = [
            {
                key: record.get(key)
                for key in (
                    "path",
                    "exists",
                    "kind",
                    "readable",
                    "writable",
                    "executable",
                    "nearest_parent",
                    "parent_writable",
                    "error",
                )
                if key in record
            }
            for record in root_records
        ]
        return {
            "control": self._control_identity(info),
            "roots": identity_roots,
            "store": store,
            "validator_tools": validator_tools,
        }

    def _require_snapshot_preflight(
        self, snapshot: Mapping[str, Any], against: str | None
    ) -> dict[str, Any]:
        if against is not None and against not in snapshot.get("pilots", {}):
            raise InvalidArgument(f"campaign has no pilot {against!r}")
        environment = self._snapshot_preflight_environment(snapshot)
        material = {
            "definition_id": snapshot["definition_id"],
            "contract_ids": [
                snapshot["stage_specs"][item["name"]]["contract_id"]
                for item in snapshot["stage_graph"]
            ],
            "against": against,
            "pilot": snapshot.get("pilots", {}).get(against) if against else None,
            "environment": environment,
        }
        expected = hashlib.sha256(canonical_json(material)).hexdigest()
        offset = 0
        observed = 0
        while observed < 5000:
            records = self.store.receipts(
                str(snapshot["campaign"]), "preflight", limit=500, offset=offset
            )
            for record in records.get("items", []):
                receipt = record.get("value", {})
                if receipt.get("preflight_key") == expected and receipt.get("result") == "PASSED":
                    return {"receipt_id": record["receipt_id"], **receipt}
            observed += len(records.get("items", []))
            if records.get("next_offset") is None:
                break
            offset = int(records["next_offset"])
        raise InvalidArgument(
            "no current passing preflight receipt matches this run and environment",
            expected_preflight_key=expected,
            observed_receipts=observed,
        )

    def receipts(
        self,
        name: str,
        kind: str,
        *,
        run_id: str | None = None,
        receipt_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        return self.store.receipts(
            name,
            kind,
            run_id=run_id,
            receipt_id=receipt_id,
            limit=limit,
            offset=offset,
        )

    def _sample_telemetry(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        active_units = [
            unit
            for unit in snapshot["units"]
            if unit["execution"]["state"] == "RUNNING" and unit["execution"].get("current_attempt")
        ]
        jobs: dict[str, dict[str, Any]] = {}
        for unit in active_units:
            attempt = unit["execution"]["current_attempt"]
            jobs.setdefault(str(attempt["job_id"]), unit)
        usages: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        job_ids = sorted(jobs)
        for start in range(0, len(job_ids), 1000):
            batch = job_ids[start : start + 1000]
            try:
                response = self.cluster.call(
                    "sstat",
                    fields=slurm.SSTAT_USAGE_FIELDS,
                    jobs=[job_id + ".batch" for job_id in batch],
                    _timeout=60,
                )
                lines: dict[str, list[str]] = defaultdict(list)
                for line in response.get("stdout", "").splitlines():
                    job_step = line.split("|", 1)[0]
                    job_id = job_step.rsplit(".", 1)[0]
                    lines[job_id].append(line)
                for job_id, job_lines in lines.items():
                    scheduler = jobs[job_id]["execution"].get("scheduler", {})
                    usage = slurm.parse_sstat_usage(
                        "\n".join(job_lines),
                        elapsed=scheduler.get("elapsed"),
                        allocated_cpus=scheduler.get("allocated_cpus"),
                    )
                    if usage:
                        usages[job_id] = usage
            except RemoteSlurmError as exc:
                errors.append(exc.message)
        allocated = 0
        effective = 0.0
        rss = 0
        cpu_sampled = 0
        rss_sampled = 0
        for job_id, unit in jobs.items():
            usage = usages.get(job_id)
            scheduler = unit["execution"].get("scheduler", {})
            cpus = scheduler.get("allocated_cpus") or (usage or {}).get("allocated_cpus") or 0
            allocated += int(cpus)
            if usage and usage.get("effective_cpus") is not None:
                effective += float(usage["effective_cpus"])
                cpu_sampled += 1
            if usage and usage.get("estimated_total_rss_bytes") is not None:
                rss += int(usage["estimated_total_rss_bytes"])
                rss_sampled += 1
            if usage:
                unit["execution"]["usage"] = usage
        result: dict[str, Any] = {
            "sampled_at": _now(),
            "active_allocations": len(jobs),
            "allocated_cpus": allocated,
            "effective_cpus": round(effective, 2) if cpu_sampled else None,
            "estimated_rss_bytes": rss if rss_sampled else None,
            "cpu_coverage": {"sampled": cpu_sampled, "eligible": len(jobs)},
            "rss_coverage": {"sampled": rss_sampled, "eligible": len(jobs)},
        }
        if errors:
            result["limitations"] = errors[:3]
        return result

    @staticmethod
    def _derive_dependencies(snapshot: dict[str, Any]) -> None:
        by_id = {unit["unit_id"]: unit for unit in snapshot["units"]}
        for unit in snapshot["units"]:
            requirements = unit.get("dependency_requirements", [])
            if not requirements:
                unit["dependency"] = {"state": "NOT_APPLICABLE"}
                continue
            satisfied = True
            blockers: list[str] = []
            for requirement in requirements:
                upstream = by_id[requirement["unit_id"]]
                kind = requirement["require"]
                accepted = (
                    (kind == "completed" and upstream["execution"]["state"] == "COMPLETED")
                    or (kind == "outputs_present" and upstream["artifacts"]["state"] == "PRESENT")
                    or (kind == "verified" and upstream["validation"]["state"] == "PASSED")
                )
                if not accepted:
                    satisfied = False
                    blockers.append(upstream["unit_id"])
            unit["dependency"] = {
                "state": "SATISFIED" if satisfied else "BLOCKED",
                "blockers": blockers[:20],
                "blocker_count": len(blockers),
            }

    @staticmethod
    def _summarize(snapshot: dict[str, Any]) -> None:
        stage_units: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for unit in snapshot["units"]:
            stage_units[unit["stage"]].append(unit)
        stages: list[dict[str, Any]] = []
        for stage in [item["name"] for item in snapshot["stage_graph"]]:
            units = stage_units.get(stage, [])
            stage_spec = snapshot.get("stage_specs", {}).get(stage, {})
            execution = Counter(unit["execution"]["state"] for unit in units)
            artifacts = Counter(unit["artifacts"]["state"] for unit in units)
            validation = Counter(unit["validation"]["state"] for unit in units)
            dependency = Counter(unit["dependency"]["state"] for unit in units)
            freshness = Counter(unit["freshness"]["state"] for unit in units)
            ready = sum(
                unit["execution"]["state"] == "UNBOUND"
                and unit["dependency"]["state"] in {"SATISFIED", "NOT_APPLICABLE"}
                for unit in units
            )
            stages.append(
                {
                    "name": stage,
                    "executor": stage_spec.get("execution", {}).get("mode", "adopted"),
                    "units": len(units),
                    "ready": ready,
                    "execution": {state: execution.get(state, 0) for state in EXECUTION_STATES},
                    "artifacts": {state: artifacts.get(state, 0) for state in ARTIFACT_STATES},
                    "validation": {state: validation.get(state, 0) for state in VALIDATION_STATES},
                    "dependency": {state: dependency.get(state, 0) for state in DEPENDENCY_STATES},
                    "freshness": {state: freshness.get(state, 0) for state in FRESHNESS_STATES},
                }
            )
        snapshot["stages"] = stages
        snapshot["unit_count"] = len(snapshot["units"])
        snapshot["next_action"] = CampaignManager._next_action(snapshot)

    @staticmethod
    def _next_action(snapshot: Mapping[str, Any]) -> dict[str, Any]:
        return project_attention(snapshot)

    @staticmethod
    def _page(
        snapshot: dict[str, Any], *, include_units: bool, offset: int = 0, limit: int = 200
    ) -> dict[str, Any]:
        result = dict(snapshot)
        # Cached observations age even when no refresh/observer is running.
        result["next_action"] = CampaignManager._next_action(snapshot)
        result.pop("stage_specs", None)
        result.pop("pilots", None)
        units = result.pop("units", [])
        if include_units:
            if offset < 0 or limit < 1 or limit > 500:
                raise InvalidArgument("unit page requires offset >= 0 and limit 1..500")
            result["units"] = units[offset : offset + limit]
            result["unit_page"] = {
                "offset": offset,
                "count": len(result["units"]),
                "total": len(units),
                "next_offset": offset + limit if offset + limit < len(units) else None,
            }
        return result

    def close(
        self,
        name: str,
        *,
        run_id: str | None = None,
        allow_active: bool = False,
        reason: str | None = None,
    ) -> dict[str, Any]:
        run_id = self._resolve_run(name, run_id)
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot["lifecycle"] == "CLOSED":
            return self._page(snapshot, include_units=False)
        if snapshot["lifecycle"] != "OPEN":
            raise InvalidArgument("only an open run can be closed")
        unresolved = [
            unit for unit in snapshot["units"] if unit["execution"]["state"] in ACTIVE_EXECUTION
        ]
        if unresolved and not allow_active:
            raise InvalidArgument(
                f"run has {len(unresolved)} active or unresolved attempts; pass allow_active=True",
                examples=[unit["unit_id"] for unit in unresolved[:10]],
            )
        snapshot["lifecycle"] = "CLOSED"
        snapshot["closed_at"] = _now()
        snapshot["closure"] = {
            "reason": reason,
            "unresolved_count": len(unresolved),
            "unresolved_examples": [unit["unit_id"] for unit in unresolved[:20]],
        }
        event = _event(
            snapshot,
            "run_closed",
            reason=reason,
            allow_active=allow_active,
            unresolved_count=len(unresolved),
        )
        self._commit_snapshot(snapshot, [event])
        return self._page(snapshot, include_units=False)

    def archive(
        self,
        name: str,
        *,
        run_id: str,
        accept_unresolved: bool = False,
    ) -> dict[str, Any]:
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot["lifecycle"] == "ARCHIVED":
            return self._page(snapshot, include_units=False)
        if snapshot["lifecycle"] != "CLOSED":
            raise InvalidArgument("archive requires a closed run")
        active = [
            unit
            for unit in snapshot["units"]
            if unit["execution"]["state"] in {"INTENDED", "PENDING", "RUNNING"}
        ]
        unknown = [unit for unit in snapshot["units"] if unit["execution"]["state"] == "UNKNOWN"]
        if active:
            raise InvalidArgument(f"cannot archive while {len(active)} attempts remain active")
        if unknown and not accept_unresolved:
            raise InvalidArgument(
                f"run has {len(unknown)} unresolved attempts; pass accept_unresolved=True"
            )
        snapshot["lifecycle"] = "ARCHIVED"
        snapshot["archived_at"] = _now()
        if unknown:
            snapshot.setdefault("limitations", []).append(
                f"archived with {len(unknown)} unresolved execution attempts"
            )
        event = _event(
            snapshot,
            "run_archived",
            accept_unresolved=accept_unresolved,
            unresolved_count=len(unknown),
        )
        self._commit_snapshot(snapshot, [event])
        return self._page(snapshot, include_units=False)

    def restore(self, name: str, *, run_id: str) -> dict[str, Any]:
        snapshot = self.store.read_complete_snapshot(name, run_id)
        if snapshot["lifecycle"] == "CLOSED":
            return self._page(snapshot, include_units=False)
        if snapshot["lifecycle"] != "ARCHIVED":
            raise InvalidArgument("restore requires an archived run")
        snapshot["lifecycle"] = "CLOSED"
        snapshot["restored_at"] = _now()
        event = _event(snapshot, "run_restored", lifecycle="CLOSED")
        self._commit_snapshot(snapshot, [event])
        return self._page(snapshot, include_units=False)

    def failures(self, name: str, *, run_id: str | None = None, limit: int = 200) -> dict[str, Any]:
        run_id = self._resolve_run(name, run_id)
        snapshot = self.store.read_complete_snapshot(name, run_id)
        failures = [
            unit
            for unit in snapshot["units"]
            if unit["execution"]["state"] in {"FAILED", "CANCELLED", "UNKNOWN"}
            or unit["artifacts"]["state"] in {"MISSING", "CHANGED", "ERROR"}
            or unit["validation"]["state"] in {"FAILED", "STALE", "ERROR"}
        ]
        return {
            "campaign": name,
            "run_id": run_id,
            "count": len(failures),
            "failures": failures[:limit],
            "truncated": len(failures) > limit,
        }

    def events(
        self,
        name: str,
        *,
        run_id: str | None = None,
        cursor: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        run_id = self._resolve_run(name, run_id)
        result = self.store.events(name, run_id, cursor=cursor, limit=limit)
        result["campaign"] = name
        result["run_id"] = run_id
        return result

    # ``list`` is the concise public spelling; assign it after annotated methods
    # so it cannot shadow the built-in generic while the class body is compiled.
    list = list_campaigns
