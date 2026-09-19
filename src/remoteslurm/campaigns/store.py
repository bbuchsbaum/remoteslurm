"""Client interface to the durable remote campaign transaction store."""

from __future__ import annotations

import getpass
import hashlib
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ..errors import InvalidArgument
from .model import CampaignDefinition
from .spec import canonical_json

if TYPE_CHECKING:
    from ..cluster import Cluster

UNIT_PAGE_SIZE = 500


def new_run_id(now: datetime | None = None) -> str:
    instant = now or datetime.now(UTC)
    return instant.strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:12]


def new_transaction_id() -> str:
    return uuid.uuid4().hex


class CampaignStore:
    """Bounded remote reads and compare-and-swap transactions."""

    def __init__(self, cluster: Cluster) -> None:
        self.cluster = cluster
        self.campaign_dir = cluster.host.campaign_dir

    def _base_args(self) -> dict[str, Any]:
        return {"campaign_dir": self.campaign_dir}

    def put_definition(self, definition: CampaignDefinition) -> dict[str, Any]:
        value = definition.to_dict()
        # Expanded units are stored once in the run's paged immutable views. The
        # definition retains every identity-bearing input without duplicating that
        # potentially large projection in a single remote object.
        value.pop("units", None)
        value["unit_count"] = len(definition.units)
        return dict(
            self.cluster.call(
                "campaign_put_immutable",
                **self._base_args(),
                name=definition.name,
                kind="definition",
                object_id=definition.definition_id,
                value=value,
            )
        )

    def put_run(
        self,
        definition: CampaignDefinition,
        *,
        run_id: str | None = None,
        parent_run: str | None = None,
    ) -> dict[str, Any]:
        run_id = run_id or new_run_id()
        created_at = datetime.now(UTC).isoformat()
        record: dict[str, Any] = {
            "schema": 1,
            "campaign": definition.name,
            "run_id": run_id,
            "definition_id": definition.definition_id,
            "created_at": created_at,
            "creator": getpass.getuser(),
            "host": definition.host,
            "control_plane": "remoteslurm",
        }
        if parent_run:
            record["parent_run"] = parent_run
        result = dict(
            self.cluster.call(
                "campaign_put_immutable",
                **self._base_args(),
                name=definition.name,
                kind="run",
                object_id=run_id,
                value=record,
            )
        )
        result["run"] = record
        return result

    def put_receipt(
        self,
        name: str,
        kind: str,
        value: dict[str, Any],
        *,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Write one content-addressed immutable preflight or validation receipt."""

        if kind not in {"preflight", "validation"}:
            raise InvalidArgument("receipt kind must be preflight or validation")
        if kind == "validation" and not run_id:
            raise InvalidArgument("validation receipts require run_id")
        receipt_id = hashlib.sha256(canonical_json(value)).hexdigest()
        result = dict(
            self.cluster.call(
                "campaign_put_immutable",
                _timeout=120,
                **self._base_args(),
                name=name,
                kind=kind + "_receipt",
                object_id=receipt_id,
                run_id=run_id,
                value=value,
            )
        )
        result["receipt_id"] = receipt_id
        return result

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
        return dict(
            self.cluster.call(
                "campaign_receipts",
                **self._base_args(),
                name=name,
                kind=kind,
                run_id=run_id,
                receipt_id=receipt_id,
                limit=limit,
                offset=offset,
            )
        )

    def commit(
        self,
        name: str,
        run_id: str,
        *,
        expected_revision: int,
        events: list[dict[str, Any]],
        documents: dict[str, Any],
        transaction_id: str | None = None,
    ) -> dict[str, Any]:
        return dict(
            self.cluster.call(
                "campaign_commit",
                _timeout=120,
                **self._base_args(),
                name=name,
                run_id=run_id,
                expected_revision=expected_revision,
                transaction_id=transaction_id or new_transaction_id(),
                events=events,
                documents=documents,
            )
        )

    def read_document(
        self, name: str, run_id: str, document: str = "summary.json"
    ) -> dict[str, Any]:
        return dict(
            self.cluster.call(
                "campaign_read",
                **self._base_args(),
                name=name,
                run_id=run_id,
                document=document,
            )
        )

    def events(
        self,
        name: str,
        run_id: str,
        *,
        cursor: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        return dict(
            self.cluster.call(
                "campaign_events",
                **self._base_args(),
                name=name,
                run_id=run_id,
                cursor=cursor,
                limit=limit,
            )
        )

    def list_campaigns(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        return dict(
            self.cluster.call(
                "campaign_list",
                **self._base_args(),
                limit=limit,
                offset=offset,
            )
        )

    def list_runs(self, name: str, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        return dict(
            self.cluster.call(
                "campaign_list",
                **self._base_args(),
                name=name,
                limit=limit,
                offset=offset,
            )
        )

    @staticmethod
    def snapshot_documents(snapshot: dict[str, Any]) -> dict[str, Any]:
        """Split unit details into bounded pages while retaining an exact summary."""

        units = snapshot.get("units", [])
        if not isinstance(units, list):
            raise InvalidArgument("campaign snapshot units must be an array")
        summary = {key: value for key, value in snapshot.items() if key != "units"}
        documents: dict[str, Any] = {"summary.json": summary}
        pages: list[dict[str, Any]] = []
        for page_number, start in enumerate(range(0, len(units), UNIT_PAGE_SIZE)):
            name = f"units/{page_number:08d}.json"
            page = units[start : start + UNIT_PAGE_SIZE]
            documents[name] = {
                "offset": start,
                "count": len(page),
                "units": page,
            }
            pages.append({"document": name, "offset": start, "count": len(page)})
        documents["index.json"] = {
            "unit_count": len(units),
            "page_size": UNIT_PAGE_SIZE,
            "pages": pages,
        }
        return documents

    def read_snapshot(
        self,
        name: str,
        run_id: str,
        *,
        include_units: bool = False,
        offset: int = 0,
        limit: int = 200,
    ) -> dict[str, Any]:
        summary_result = self.read_document(name, run_id, "summary.json")
        snapshot = dict(summary_result["value"])
        snapshot["store"] = summary_result["head"]
        if not include_units:
            return snapshot
        if offset < 0 or limit < 1 or limit > UNIT_PAGE_SIZE:
            raise InvalidArgument(f"unit page requires offset >= 0 and limit 1..{UNIT_PAGE_SIZE}")
        index = self.read_document(name, run_id, "index.json")["value"]
        selected: list[dict[str, Any]] = []
        stop = offset + limit
        for page in index.get("pages", []):
            page_start = int(page["offset"])
            page_stop = page_start + int(page["count"])
            if page_stop <= offset or page_start >= stop:
                continue
            value = self.read_document(name, run_id, str(page["document"]))["value"]
            for absolute, unit in enumerate(value.get("units", []), start=page_start):
                if offset <= absolute < stop:
                    selected.append(unit)
        snapshot["units"] = selected
        snapshot["unit_page"] = {
            "offset": offset,
            "count": len(selected),
            "total": int(index.get("unit_count", 0)),
            "next_offset": stop if stop < int(index.get("unit_count", 0)) else None,
        }
        return snapshot

    def read_complete_snapshot(
        self, name: str, run_id: str, *, max_units: int = 50_000
    ) -> dict[str, Any]:
        """Read every bounded unit page, enforcing the definition-wide ceiling."""

        summary_result = self.read_document(name, run_id, "summary.json")
        snapshot = dict(summary_result["value"])
        snapshot["store"] = summary_result["head"]
        index = self.read_document(name, run_id, "index.json")["value"]
        total = int(index.get("unit_count", 0))
        if total > max_units:
            raise InvalidArgument(
                f"campaign has {total} units, above this client's max_units={max_units}"
            )
        units: list[dict[str, Any]] = []
        for page in index.get("pages", []):
            value = self.read_document(name, run_id, str(page["document"]))["value"]
            page_units = value.get("units", [])
            if not isinstance(page_units, list):
                raise InvalidArgument("campaign unit page is malformed")
            units.extend(page_units)
        if len(units) != total:
            raise InvalidArgument(
                f"campaign unit index claims {total} units but pages contain {len(units)}"
            )
        snapshot["units"] = units
        return snapshot
