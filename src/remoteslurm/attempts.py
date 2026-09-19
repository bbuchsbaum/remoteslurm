"""Shared durable-submission control and campaign attempt client."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from .errors import ExecutionMismatch
from .identity import control_identity

if TYPE_CHECKING:
    from .cluster import Cluster


def submission_control(cluster: Cluster) -> dict[str, Any]:
    """Prove that the requesting client and loaded submission stub are identical."""

    expected = control_identity()
    info = cluster.info(refresh=True)
    actual_sha = info.get("stub_sha")
    if actual_sha != expected["stub_sha"]:
        raise ExecutionMismatch(
            "the loaded remote stub does not match the requesting client",
            action=(
                "close the host session and repeat the operation so the current stub is installed"
            ),
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


def ensure_campaign_attempt(
    cluster: Cluster,
    *,
    campaign: str,
    run_id: str,
    group_id: str,
    contract: Mapping[str, Any],
    script: str,
    flags: list[str],
    cwd: str,
    retry: bool = False,
    retry_unknown: bool = False,
    retry_claims: Sequence[Mapping[str, Any]] | None = None,
    attempt_id: str | None = None,
    control: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Recover or create one remotely journaled campaign job-group attempt."""

    return dict(
        cluster.call(
            "campaign_attempt_ensure",
            campaign_dir=cluster.host.campaign_dir,
            name=campaign,
            run_id=run_id,
            group_id=group_id,
            contract=dict(contract),
            script=script,
            script_sha256=hashlib.sha256(script.encode("utf-8")).hexdigest(),
            args=flags,
            cwd=cwd,
            attempt_id=attempt_id or uuid.uuid4().hex,
            retry=retry,
            retry_unknown=retry_unknown,
            retry_claims=[dict(claim) for claim in (retry_claims or [])],
            control=dict(control or submission_control(cluster)),
            _timeout=180,
        )
    )


def update_campaign_attempt(
    cluster: Cluster,
    *,
    campaign: str,
    run_id: str,
    group_id: str,
    attempt_id: str,
    state: str,
) -> dict[str, Any]:
    """Project reconciled campaign state into the remote attempt record."""

    return dict(
        cluster.call(
            "campaign_attempt_update",
            campaign_dir=cluster.host.campaign_dir,
            name=campaign,
            run_id=run_id,
            group_id=group_id,
            attempt_id=attempt_id,
            state=state,
        )
    )
