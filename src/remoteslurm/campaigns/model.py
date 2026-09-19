"""Stable domain records for campaign definitions.

Attempt, receipt, and mutable snapshot records intentionally live outside this
module until their owning lifecycle packages define their semantics.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, NewType

from ..errors import InvalidArgument

SCHEMA_VERSION = 1
NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
CampaignName = NewType("CampaignName", str)


def checked_name(value: Any, path: str) -> str:
    if not isinstance(value, str) or not NAME_PATTERN.fullmatch(value):
        raise InvalidArgument(
            f"{path} must match {NAME_PATTERN.pattern}",
            path=path,
        )
    return value


@dataclass(frozen=True)
class Inventory:
    name: str
    key: tuple[str, ...]
    rows: tuple[dict[str, str | int | float | bool], ...]
    source: str | None = None
    source_sha256: str | None = None


@dataclass(frozen=True)
class DependencyJoin:
    stage: str
    on: tuple[str, ...]
    require: str = "verified"
    allow_empty: bool = False


@dataclass(frozen=True)
class OutputAlternative:
    path: str
    when_present: tuple[str, ...] = ()
    when_absent: tuple[str, ...] = ()
    external: bool = False


@dataclass(frozen=True)
class OutputDeclaration:
    name: str
    kind: str
    alternatives: tuple[OutputAlternative, ...]
    contract: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Stage:
    name: str
    foreach: str
    script: str
    script_sha256: str
    script_content: str
    environment: str | None
    needs: tuple[DependencyJoin, ...]
    outputs: tuple[OutputDeclaration, ...]
    execution: dict[str, Any] = field(default_factory=dict)
    resources: dict[str, Any] = field(default_factory=dict)
    validators: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class WorkUnit:
    unit_id: str
    stage: str
    ordinal: int
    keys: dict[str, str | int | float | bool]
    dependencies: tuple[str, ...]
    outputs: dict[str, str]


@dataclass(frozen=True)
class CampaignDefinition:
    schema: int
    name: str
    host: str
    project: str | None
    resolved_host: dict[str, Any]
    workspace: dict[str, Any]
    inventories: tuple[Inventory, ...]
    environments: dict[str, dict[str, Any]]
    stages: tuple[Stage, ...]
    units: tuple[WorkUnit, ...]
    pilots: dict[str, dict[str, Any]]
    definition_id: str
    source_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def stage(self, name: str) -> Stage:
        for stage in self.stages:
            if stage.name == name:
                return stage
        raise InvalidArgument(f"unknown campaign stage {name!r}", stage=name)

    def inventory(self, name: str) -> Inventory:
        for inventory in self.inventories:
            if inventory.name == name:
                return inventory
        raise InvalidArgument(f"unknown campaign inventory {name!r}", inventory=name)
