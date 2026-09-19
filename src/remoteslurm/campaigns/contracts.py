"""Pure output-contract normalization, identity, and evaluation.

Remote code gathers bounded filesystem facts.  This module owns the policy that
turns those facts into artifact and validation evidence, so the same rules are
used by status, verification, fixtures, and pilot preflight.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

from ..errors import InvalidArgument
from .model import Stage, WorkUnit
from .spec import canonical_json

MIN_SETTLE_INTERVAL = 0.2
MAX_SETTLE_TIMEOUT = 3600.0
MAX_VALIDATOR_TIMEOUT = 3600
MAX_VALIDATOR_OUTPUT = 8_192
_GLOB_MAGIC = re.compile(r"[*?[]")
_PLACEHOLDER = re.compile(r"\{([A-Za-z][A-Za-z0-9_.-]{0,63}|output:[A-Za-z][A-Za-z0-9_.-]{0,63})\}")


def stage_contract(stage: Stage) -> dict[str, Any]:
    """Return the canonical reusable contract and its content identity."""

    body = {
        "schema": 1,
        "stage": stage.name,
        "outputs": [
            {
                "name": output.name,
                "kind": output.kind,
                "alternatives": [
                    {
                        "path": alternative.path,
                        "when_present": list(alternative.when_present),
                        "when_absent": list(alternative.when_absent),
                        "external": alternative.external,
                    }
                    for alternative in output.alternatives
                ],
                **normalized_predicates(output.contract),
            }
            for output in stage.outputs
        ],
        "validators": [dict(validator) for validator in stage.validators],
    }
    return {
        "contract_id": hashlib.sha256(canonical_json(body)).hexdigest(),
        **body,
    }


def normalized_predicates(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Fill contract defaults without losing explicitly declared constraints."""

    required = bool(raw.get("required", True))
    cardinality = raw.get("cardinality")
    exact_matches = raw.get("exact_matches")
    min_matches = raw.get("min_matches")
    max_matches = raw.get("max_matches")
    if cardinality == "exactly_one":
        exact_matches = 1
    elif cardinality == "zero_or_one":
        min_matches, max_matches = 0, 1
    elif cardinality == "one_or_more":
        min_matches = 1
    if exact_matches is None and min_matches is None and max_matches is None:
        if required:
            exact_matches = 1
        else:
            min_matches, max_matches = 0, 1
    result: dict[str, Any] = {
        "required": required,
        "exact_matches": exact_matches,
        "min_matches": min_matches,
        "max_matches": max_matches,
        "exact_bytes": raw.get("exact_bytes"),
        "min_bytes": raw.get("min_bytes"),
        "max_bytes": raw.get("max_bytes"),
        "sha256": raw.get("sha256", False),
        "max_matches_scanned": int(raw.get("max_matches_scanned", 1024)),
        "settle_timeout": float(raw.get("settle_timeout", 0)),
        "settle_interval": max(
            MIN_SETTLE_INTERVAL, float(raw.get("settle_interval", MIN_SETTLE_INTERVAL))
        ),
        "stable_for": float(raw.get("stable_for", 0)),
        "stable_dimensions": list(raw.get("stable_dimensions", ["existence", "size", "mtime"])),
    }
    return {key: value for key, value in result.items() if value is not None}


def unit_contract(stage: Stage, unit: WorkUnit | Mapping[str, Any]) -> dict[str, Any]:
    """Bind a reusable stage contract to one unit's exact output paths."""

    outputs = unit.outputs if isinstance(unit, WorkUnit) else unit.get("outputs", {})
    keys = unit.keys if isinstance(unit, WorkUnit) else unit.get("keys", {})
    output_by_name = {output.name: output for output in stage.outputs}
    reusable = stage_contract(stage)
    bound = []
    for name, path in outputs.items():
        declaration = output_by_name[name]
        bound.append(
            {
                "name": name,
                "path": path,
                "kind": declaration.kind,
                "glob": bool(_GLOB_MAGIC.search(path)),
                **normalized_predicates(declaration.contract),
            }
        )
    return {
        "contract_id": reusable["contract_id"],
        "stage": stage.name,
        "keys": dict(keys),
        "outputs": bound,
        "validators": [dict(validator) for validator in stage.validators],
    }


def observation_request(output: Mapping[str, Any]) -> dict[str, Any]:
    """Project a bound output into the small remote collector grammar."""

    sha = output.get("sha256", False)
    dimensions = output.get("stable_dimensions", [])
    return {
        "path": str(output["path"]),
        "glob": bool(output.get("glob", False)),
        "max_matches": int(output.get("max_matches_scanned", 1024)),
        "sha256": bool(sha) or "sha256" in dimensions,
    }


def artifact_signature(
    observations: list[Mapping[str, Any]], dimensions: list[str] | tuple[str, ...] | None = None
) -> str:
    """Hash stable, ordered filesystem evidence for mutation and settling checks."""

    dims = tuple(dimensions or ("existence", "size", "mtime", "sha256"))
    normalized = []
    for observation in observations:
        matches = []
        for match in observation.get("matches", []):
            item: dict[str, Any] = {"path": match.get("path")}
            if "existence" in dims:
                item["exists"] = match.get("exists")
            for dimension in ("kind", "size", "mtime", "sha256"):
                if dimension in dims:
                    item[dimension] = match.get(dimension)
            if match.get("error"):
                item["error"] = match["error"]
            matches.append(item)
        normalized.append(
            {
                "path": observation.get("path"),
                "matches": matches,
                "truncated": bool(observation.get("truncated", False)),
                "error": observation.get("error"),
            }
        )
    return hashlib.sha256(canonical_json(normalized)).hexdigest()


def evaluate_output(
    output: Mapping[str, Any],
    observation: Mapping[str, Any],
    *,
    prior_stability: Mapping[str, Any] | None = None,
    observed_epoch: float,
) -> dict[str, Any]:
    """Evaluate one built-in contract and retain explicit settling evidence."""

    predicates = normalized_predicates(output)
    checks: list[dict[str, Any]] = []
    errors = [match.get("error") for match in observation.get("matches", []) if match.get("error")]
    if observation.get("error"):
        errors.insert(0, observation["error"])
    if errors:
        code = (
            "permission_denied"
            if any(e.get("code") == "permission" for e in errors)
            else "io_error"
        )
        return {
            "state": "ERROR",
            "passed": False,
            "checks": [{"code": code, "passed": False, "evidence": errors[:5]}],
            "stability": None,
        }
    if observation.get("truncated"):
        return {
            "state": "ERROR",
            "passed": False,
            "checks": [
                {
                    "code": "scan_truncated",
                    "passed": False,
                    "observed": len(observation.get("matches", [])),
                    "limit": output.get("max_matches_scanned", 1024),
                }
            ],
            "stability": None,
        }

    matches = [match for match in observation.get("matches", []) if match.get("exists")]
    count = len(matches)
    exact = predicates.get("exact_matches")
    minimum = predicates.get("min_matches")
    maximum = predicates.get("max_matches")
    if exact is not None:
        checks.append(
            {
                "code": "cardinality_exact",
                "passed": count == int(exact),
                "expected": int(exact),
                "observed": count,
            }
        )
    if minimum is not None:
        checks.append(
            {
                "code": "cardinality_min",
                "passed": count >= int(minimum),
                "expected": int(minimum),
                "observed": count,
            }
        )
    if maximum is not None:
        checks.append(
            {
                "code": "cardinality_max",
                "passed": count <= int(maximum),
                "expected": int(maximum),
                "observed": count,
            }
        )
    for match in matches:
        checks.append(
            {
                "code": "kind",
                "passed": match.get("kind") == output.get("kind", "file"),
                "expected": output.get("kind", "file"),
                "observed": match.get("kind"),
                "path": match.get("path"),
            }
        )
        for key, relation in (
            ("exact_bytes", lambda size, expected: size == expected),
            ("min_bytes", lambda size, expected: size >= expected),
            ("max_bytes", lambda size, expected: size <= expected),
        ):
            expected = predicates.get(key)
            if expected is not None and match.get("kind") == "file":
                size = int(match.get("size", 0))
                checks.append(
                    {
                        "code": key,
                        "passed": relation(size, int(expected)),
                        "expected": int(expected),
                        "observed": size,
                        "path": match.get("path"),
                    }
                )
        expected_hash = predicates.get("sha256")
        if isinstance(expected_hash, str):
            checks.append(
                {
                    "code": "sha256",
                    "passed": match.get("sha256") == expected_hash,
                    "expected": expected_hash,
                    "observed": match.get("sha256"),
                    "path": match.get("path"),
                }
            )

    failed = [check for check in checks if not check["passed"]]
    if failed:
        missing = count == 0 and any(check["code"].startswith("cardinality") for check in failed)
        return {
            "state": "MISSING" if missing else "ERROR",
            "passed": False,
            "checks": checks,
            "failure_codes": [check["code"] for check in failed],
            "stability": None,
        }

    stable_for = float(predicates.get("stable_for", 0))
    signature = artifact_signature([observation], list(predicates.get("stable_dimensions", [])))
    if stable_for > 0:
        prior = dict(prior_stability or {})
        stable_since = (
            float(prior["stable_since"])
            if prior.get("signature") == signature and prior.get("stable_since") is not None
            else observed_epoch
        )
        duration = max(0.0, observed_epoch - stable_since)
        stability = {
            "signature": signature,
            "stable_since": stable_since,
            "observed_at": observed_epoch,
            "stable_for": stable_for,
            "duration": duration,
            "dimensions": list(predicates.get("stable_dimensions", [])),
        }
        if duration < stable_for:
            return {
                "state": "SETTLING",
                "passed": False,
                "checks": checks,
                "stability": stability,
                "failure_codes": ["unstable"],
            }
    else:
        stability = {
            "signature": signature,
            "stable_since": observed_epoch,
            "observed_at": observed_epoch,
            "stable_for": 0.0,
            "duration": 0.0,
            "dimensions": list(predicates.get("stable_dimensions", [])),
        }
    return {"state": "PRESENT", "passed": True, "checks": checks, "stability": stability}


def render_validator(
    validator: Mapping[str, Any], unit: Mapping[str, Any], *, remote_root: str
) -> dict[str, Any]:
    """Expand only enumerated unit/output placeholders into an argv validator."""

    keys = unit.get("keys", {})
    outputs = {
        output["name"]: output["path"]
        for output in unit.get("artifacts", {}).get("outputs", unit.get("outputs", []))
    }

    def render(text: str) -> str:
        cursor = 0
        chunks: list[str] = []
        for match in _PLACEHOLDER.finditer(text):
            literal = text[cursor : match.start()]
            if "{" in literal or "}" in literal:
                raise InvalidArgument(f"invalid validator placeholder in {text!r}")
            name = match.group(1)
            if name.startswith("output:"):
                output_name = name.split(":", 1)[1]
                if output_name not in outputs:
                    raise InvalidArgument(f"validator references unknown output {output_name!r}")
                chunks.append(literal + str(outputs[output_name]))
            else:
                if name not in keys:
                    raise InvalidArgument(f"validator references absent unit field {name!r}")
                chunks.append(literal + str(keys[name]))
            cursor = match.end()
        tail = text[cursor:]
        if "{" in tail or "}" in tail:
            raise InvalidArgument(f"invalid validator placeholder in {text!r}")
        chunks.append(tail)
        return "".join(chunks)

    cwd = render(str(validator.get("cwd", remote_root)))
    env = {str(key): render(str(value)) for key, value in validator.get("env", {}).items()}
    return {
        "argv": [render(str(item)) for item in validator["argv"]],
        "cwd": cwd,
        "env": env,
        "timeout": int(validator.get("timeout", 300)),
    }


def fixture_evidence() -> list[dict[str, Any]]:
    """Exercise each negative built-in path without touching user data."""

    now = 1000.0
    fixtures: list[tuple[str, dict[str, Any], dict[str, Any], str]] = [
        (
            "missing",
            {"path": "/fixture/missing", "kind": "file"},
            {"path": "/fixture/missing", "matches": [], "truncated": False},
            "cardinality_exact",
        ),
        (
            "empty",
            {"path": "/fixture/empty", "kind": "file", "min_bytes": 1},
            {
                "path": "/fixture/empty",
                "matches": [
                    {
                        "path": "/fixture/empty",
                        "exists": True,
                        "kind": "file",
                        "size": 0,
                        "mtime": 1,
                    }
                ],
            },
            "min_bytes",
        ),
        (
            "partial",
            {"path": "/fixture/*.dat", "kind": "file", "min_matches": 2, "max_matches": 2},
            {
                "path": "/fixture/*.dat",
                "matches": [
                    {
                        "path": "/fixture/a.dat",
                        "exists": True,
                        "kind": "file",
                        "size": 1,
                        "mtime": 1,
                    }
                ],
            },
            "cardinality_min",
        ),
        (
            "corrupt",
            {"path": "/fixture/data", "kind": "file", "sha256": "a" * 64},
            {
                "path": "/fixture/data",
                "matches": [
                    {
                        "path": "/fixture/data",
                        "exists": True,
                        "kind": "file",
                        "size": 1,
                        "mtime": 1,
                        "sha256": "b" * 64,
                    }
                ],
            },
            "sha256",
        ),
        (
            "changing",
            {"path": "/fixture/data", "kind": "file", "stable_for": 5},
            {
                "path": "/fixture/data",
                "matches": [
                    {"path": "/fixture/data", "exists": True, "kind": "file", "size": 1, "mtime": 2}
                ],
            },
            "unstable",
        ),
        (
            "permission_denied",
            {"path": "/fixture/private", "kind": "file"},
            {"path": "/fixture/private", "matches": [], "error": {"code": "permission"}},
            "permission_denied",
        ),
    ]
    results = []
    for name, contract, observation, expected in fixtures:
        result = evaluate_output(contract, observation, observed_epoch=now)
        codes = list(result.get("failure_codes", [])) + [
            check["code"] for check in result.get("checks", []) if not check.get("passed")
        ]
        results.append(
            {
                "name": name,
                "passed": not result["passed"] and expected in codes,
                "expected_failure": expected,
                "observed_state": result["state"],
                "evidence": result,
            }
        )
    return results
