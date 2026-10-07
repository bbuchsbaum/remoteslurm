"""Read-only source adapters with bounded pages and independently persisted cadence."""

from __future__ import annotations

import json
import time
from typing import Any

from .. import slurm
from ..campaigns.contracts import artifact_signature, evaluate_output, observation_request
from ..errors import RemoteSlurmError
from .model import EvidenceRef, SemanticRef, Subject
from .producer import workspace_for
from .semantic import key
from .store import ActivityStore, stamp

KINDS = ("recovery", "scheduler", "artifacts", "usage")


def work_page(store: ActivityStore, subscription: dict, kind: str) -> tuple[list[dict], str | None]:
    checkpoint = key(subscription["id"], kind, "page")
    after = (store.get("checkpoint", checkpoint) or {}).get("after", "")
    with store.connection() as db:
        rows = db.execute(
            "SELECT key,data FROM documents WHERE namespace='work' AND key>? "
            "AND json_extract(data,'$.target_id')=? AND json_extract(data,'$.campaign')=? "
            "AND json_extract(data,'$.run_id')=? ORDER BY key LIMIT 1001",
            (after, subscription["target_id"], subscription["campaign"], subscription["run_id"]),
        ).fetchall()
    return [json.loads(row[1]) for row in rows[:1000]], rows[999][0] if len(rows) > 1000 else None


def publish(
    store: ActivityStore,
    cluster: Any,
    work: dict,
    kind: str,
    facts: dict,
    authority: Any,
    now: float,
) -> None:
    checkpoint = key("source-fact", kind, work["work_id"])
    previous = store.get("checkpoint", checkpoint) or {}
    # Material state transitions matter; refreshing an unchanged sample only updates freshness.
    material = {k: v for k, v in facts.items() if k not in {"observed_at", "sampled_at"}}
    if isinstance(material.get("scheduler"), dict):
        material["scheduler"] = {
            k: v for k, v in material["scheduler"].items() if k != "observed_at"
        }
    digest = key(material)
    value = {**work, **facts, "observed_at": now, "projection_lag": True}
    value["source_health"] = {
        **work.get("source_health", {}),
        kind: {"observed_at": now, "freshness": facts.get("source_freshness", "FRESH")},
    }
    if digest == previous.get("digest"):
        store.save("work", work["work_id"], value)
        return
    raw = dict(work.get("subject") or {})
    if work.get("semantic_ref"):
        raw["semantic_ref"] = SemanticRef(**work["semantic_ref"])
    subject = Subject(**raw)
    ordinal = previous.get("ordinal", 0) + 1
    store.append(
        subject=subject,
        kind=kind + ".observed",
        source="campaign.observer",
        payload=facts,
        source_key=key(checkpoint, ordinal, digest),
        evidence=(EvidenceRef(authority, kind, digest),),
        observed_at=stamp(now),
        workspace=workspace_for(store, work["target_id"], work.get("workdir")),
        operation_id=work.get("operation_id"),
        correlation_id=work.get("correlation_id"),
        checkpoint=(checkpoint, {"digest": digest, "ordinal": ordinal}),
        documents=(("work", work["work_id"], value),),
    )


def sample(store: ActivityStore, cluster: Any, subscription: dict, kind: str) -> dict:
    from ..job_monitor import shared_collect
    from .monitor import policy

    POLICY = policy()
    work, after = work_page(store, subscription, kind)
    now, calls = time.time(), 0
    coverage = "partial" if after else "complete"
    if kind == "scheduler":
        unresolved = [w for w in work if not w.get("job_id") and w.get("marker")]
        if unresolved:
            checkpoint = key(subscription["id"], "identity-page")
            last = (store.get("checkpoint", checkpoint) or {}).get("after", "")
            candidate = next((w for w in unresolved if w["work_id"] > last), unresolved[0])
            match = cluster.call(
                "attempt_matches",
                marker=candidate["marker"],
                since=candidate["submit_time"],
                _timeout=10,
            )
            calls += 2
            matches = match["matches"]
            if len(matches) == 1:
                parent = matches[0]["job_id"]
                for w in unresolved:
                    if (w["attempt_id"], w["group_id"]) == (
                        candidate["attempt_id"],
                        candidate["group_id"],
                    ):
                        index = w["subject"].get("array_task_id")
                        subject = {
                            **w["subject"],
                            "job_id": parent,
                            "identity_confidence": "unique_scheduler_marker",
                        }
                        w.update(
                            job_id=parent + "_" + index if index is not None else parent,
                            subject=subject,
                        )
                        publish(
                            store,
                            cluster,
                            w,
                            "scheduler",
                            {
                                "execution": "UNKNOWN",
                                "identity_basis": (
                                    "unique scheduler marker; original acceptance time unknown"
                                ),
                                "source_freshness": "PARTIAL",
                            },
                            "scheduler",
                            match["observed_at"],
                        )
            else:
                coverage = "partial"
            store.save("checkpoint", checkpoint, {"after": candidate["work_id"]})
        selected = [
            w for w in work if w.get("job_id") and not w.get("scheduler", {}).get("terminal")
        ]
        ids = sorted({w["job_id"] for w in selected})
        scheduler_samples = shared_collect(cluster, ids) if ids else {}
        calls += 2 if ids else 0
        for w in selected:
            observed = scheduler_samples[w["job_id"]]
            name = observed.get("name")
            matches = name == w.get("marker") if w.get("marker") else False
            facts = {
                "scheduler": observed,
                "execution": observed["state"] if matches else "UNKNOWN",
                "identity_conflict": bool(name) and not matches,
                "source_freshness": "FRESH" if matches else "CONFLICT" if name else "UNAVAILABLE",
            }
            if w.get("marker_dir"):
                facts["execution"] = (
                    w["execution"]
                    if w.get("marker_evidence", {}).get("finished_at")
                    else "UNKNOWN"
                    if observed.get("terminal")
                    else facts["execution"]
                )
            publish(
                store,
                cluster,
                w,
                "scheduler",
                facts,
                "scheduler",
                observed.get("observed_at") or now,
            )
    elif kind == "artifacts":
        selected = []
        requests: list[dict] = []
        for w in work:
            contracts = w.get("output_contracts", [])
            if w.get("historical"):
                continue
            if len(requests) + len(contracts) > POLICY["max_outputs"]:
                coverage = "partial"
                after = selected[-1]["work_id"] if selected else None
                break
            requests.extend(observation_request(o) for o in contracts)
            selected.append(w)
        response = (
            cluster.call(
                "contract_observe",
                items=requests,
                max_hash_bytes=POLICY["max_hash_bytes"],
                _timeout=10,
            )
            if requests
            else {"items": []}
        )
        calls += bool(requests)
        observed_paths = {o["path"]: o for o in response["items"]}
        marker_paths = [w["marker_dir"] for w in work if w.get("marker_dir")][:1024]
        markers = (
            cluster.call(
                "campaign_markers",
                campaign_dir=cluster.host.campaign_dir,
                paths=marker_paths,
                _timeout=10,
            )
            if marker_paths
            else {"items": []}
        )
        calls += bool(marker_paths)
        by_marker = {m["path"]: m for m in markers["items"]}
        for w in selected:
            observations = [observed_paths[o["path"]] for o in w.get("output_contracts", [])]
            checks = [
                evaluate_output(
                    o,
                    v,
                    observed_epoch=now,
                    prior_stability=w.get("output_stability", {}).get(o["path"]),
                )
                for o, v in zip(w.get("output_contracts", []), observations, strict=True)
            ]
            artifact = (
                "UNCHECKED"
                if not checks
                else "ERROR"
                if any(c["state"] == "ERROR" for c in checks)
                else "PRESENT"
                if all(c["passed"] for c in checks)
                else "SETTLING"
                if any(c["state"] == "SETTLING" for c in checks)
                else "MISSING"
            )
            signature = artifact_signature(observations)
            validation = w.get("validation", "NOT_RUN")
            if (
                w.get("validation_artifact_signature")
                and observations
                and signature != w["validation_artifact_signature"]
            ):
                artifact = "CHANGED"
                validation = "STALE" if validation == "PASSED" else validation
            facts = {
                "artifacts": artifact,
                "validation": validation,
                "artifact_signature": signature,
                "output_count": len(observations),
                "output_stability": {
                    o["path"]: c.get("stability")
                    for o, c in zip(w.get("output_contracts", []), checks, strict=True)
                },
                "source_freshness": "PARTIAL"
                if w.get("outputs_omitted") or artifact == "ERROR"
                else "FRESH",
            }
            store.save(
                "evidence", signature, {"source": "filesystem", "observations": observations}
            )
            marker = by_marker.get(w.get("marker_dir"), {})
            finished = marker.get("finished")
            if finished:
                if finished.get("unit_id") != w["unit_id"]:
                    facts.update(
                        execution="UNKNOWN", identity_conflict=True, source_freshness="CONFLICT"
                    )
                else:
                    facts["execution"] = "COMPLETED" if finished.get("exit_code") == 0 else "FAILED"
                    facts["marker_evidence"] = {
                        "path": w["marker_dir"],
                        "exit_code": finished.get("exit_code"),
                        "finished_at": finished.get("finished_at"),
                    }
            publish(store, cluster, w, "artifacts", facts, "filesystem", now)
    elif kind == "usage":
        selected = [w for w in work if w.get("execution") == "RUNNING" and w.get("job_id")]
        ids = sorted({w["job_id"] for w in selected})
        response = (
            cluster.call(
                "sstat",
                jobs=[jid + ".batch" for jid in ids],
                fields=slurm.SSTAT_USAGE_FIELDS,
                _timeout=10,
            )
            if ids
            else {"stdout": "", "rc": 0}
        )
        calls += bool(ids)
        if response.get("rc") or response.get("stdout_truncated"):
            raise RemoteSlurmError("usage source unavailable or truncated")
        samples: dict[str, list[str]] = {}
        for line in response["stdout"].splitlines():
            samples.setdefault(line.split("|", 1)[0].rsplit(".", 1)[0], []).append(line)
        for w in selected:
            scheduler = w.get("scheduler", {})
            usage = slurm.parse_sstat_usage(
                "\n".join(samples.get(w["job_id"], [])),
                elapsed=scheduler.get("elapsed"),
                allocated_cpus=scheduler.get("allocated_cpus"),
            )
            publish(
                store,
                cluster,
                w,
                "resources",
                {
                    "usage": usage,
                    "usage_basis": "lifetime average",
                    "source_freshness": "FRESH" if usage else "UNAVAILABLE",
                },
                "scheduler",
                now,
            )
    with store.transaction() as db:
        store.put(db, "checkpoint", key(subscription["id"], kind, "page"), {"after": after or ""})
    return {
        "work_count": len(work),
        "coverage": coverage,
        "next_page": after,
        "remote_calls": calls,
    }
