"""Strict schema-v1 campaign parsing and deterministic compilation."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import tomllib
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

from ..errors import InvalidArgument
from .model import (
    SCHEMA_VERSION,
    CampaignDefinition,
    DependencyJoin,
    Inventory,
    OutputAlternative,
    OutputDeclaration,
    Stage,
    WorkUnit,
    checked_name,
)

MAX_UNITS = 50_000
MAX_OUTPUTS_PER_STAGE = 256
MAX_VALIDATORS_PER_STAGE = 16
MAX_CONTRACT_MATCHES = 1024
MAX_STAGES = 128
MAX_SCRIPT_BYTES = 1024 * 1024
MAX_TOTAL_SCRIPT_BYTES = 2 * 1024 * 1024
_PLACEHOLDER = re.compile(r"\{([A-Za-z][A-Za-z0-9_.-]{0,63}|output:[A-Za-z][A-Za-z0-9_.-]{0,63})\}")
_SCALAR = (str, int, float, bool)
_ROOT_KEYS = {
    "schema",
    "name",
    "host",
    "project",
    "workspace",
    "inventories",
    "environments",
    "stages",
    "pilots",
}
_WORKSPACE_KEYS = {"remote_root", "output_root", "deployment"}
_INVENTORY_KEYS = {"source", "format", "key", "rows"}
_ENVIRONMENT_KEYS = {
    "template",
    "declaration",
    "options",
    "preamble",
    "epilogue",
    "container",
    "workdir",
    "env",
}
_STAGE_KEYS = {
    "foreach",
    "script",
    "environment",
    "needs",
    "outputs",
    "execution",
    "resources",
    "validators",
}
_NEED_KEYS = {"stage", "on", "require", "allow_empty"}
_OUTPUT_KEYS = {
    "name",
    "kind",
    "path",
    "alternatives",
    "external",
    "cardinality",
    "required",
    "exact_matches",
    "min_matches",
    "max_matches",
    "max_matches_scanned",
    "exact_bytes",
    "min_bytes",
    "max_bytes",
    "sha256",
    "settle_timeout",
    "settle_interval",
    "stable_for",
    "stable_dimensions",
}
_ALTERNATIVE_KEYS = {"path", "when_present", "when_absent", "external"}
_PILOT_KEYS = {"inventory", "select", "output_root"}
_VALIDATOR_KEYS = {"kind", "argv", "timeout", "cwd", "env", "files"}
_EXECUTION_KEYS = {
    "mode",
    "max_concurrent",
    "max_array_size",
    "max_processes",
    "units_per_allocation",
}


def canonical_json(value: Any) -> bytes:
    """Return the byte representation used for campaign identities."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _fail(path: str, message: str) -> NoReturn:
    raise InvalidArgument(f"{path}: {message}", path=path)


def _table(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        _fail(path, "must be a table")
    return dict(value)


def _strict(value: Mapping[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        _fail(path, f"unknown key(s): {unknown}")


def _text(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        _fail(path, "must be non-empty text without NUL")
    return value


def _names(value: Any, path: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        _fail(path, "must be an array of names")
    names = tuple(checked_name(item, f"{path}[{i}]") for i, item in enumerate(value))
    if len(set(names)) != len(names):
        _fail(path, "contains duplicate names")
    return names


def _scalar(value: Any, path: str) -> str | int | float | bool:
    if not isinstance(value, _SCALAR) or value is None:
        _fail(path, "inventory values must be strings, integers, finite floats, or booleans")
    if isinstance(value, float) and not math.isfinite(value):
        _fail(path, "inventory floats must be finite")
    return value


def _normalize_row(value: Any, path: str) -> dict[str, str | int | float | bool]:
    row = _table(value, path)
    result: dict[str, str | int | float | bool] = {}
    for key, item in row.items():
        checked_name(key, f"{path}.{key}")
        if item is not None:  # JSON null is the absent optional dimension.
            result[key] = _scalar(item, f"{path}.{key}")
    return result


def _read_inventory(source: Path, fmt: str, path: str) -> tuple[list[dict[str, Any]], str]:
    try:
        raw = source.read_bytes()
    except OSError as exc:
        _fail(path, f"cannot read {source}: {exc}")
    digest = hashlib.sha256(raw).hexdigest()
    if fmt == "tsv":
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            _fail(path, f"is not UTF-8: {exc}")
        rows = [
            {key: value for key, value in row.items() if value not in (None, "")}
            for row in csv.DictReader(text.splitlines(), delimiter="\t")
        ]
    elif fmt == "json":
        try:
            parsed = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            _fail(path, f"is not valid JSON: {exc}")
        if not isinstance(parsed, list):
            _fail(path, "JSON inventory must contain an array of objects")
        rows = parsed
    else:
        _fail(path, "format must be 'tsv' or 'json'")
    return rows, digest


def _parse_inventories(raw: Any, base: Path) -> tuple[tuple[Inventory, ...], dict[str, Inventory]]:
    tables = _table(raw, "inventories")
    result: list[Inventory] = []
    by_name: dict[str, Inventory] = {}
    for name, value in sorted(tables.items()):
        checked_name(name, f"inventories.{name}")
        path = f"inventories.{name}"
        table = _table(value, path)
        _strict(table, _INVENTORY_KEYS, path)
        key = _names(table.get("key"), f"{path}.key")
        has_source = "source" in table
        has_rows = "rows" in table
        if has_source == has_rows:
            _fail(path, "requires exactly one of source or rows")
        source_text: str | None = None
        source_sha: str | None = None
        if has_source:
            source_text = _text(table["source"], f"{path}.source")
            fmt = table.get("format") or Path(source_text).suffix.lstrip(".").lower()
            raw_rows, source_sha = _read_inventory(
                (base / source_text).resolve(), str(fmt), f"{path}.source"
            )
        else:
            if "format" in table:
                _fail(f"{path}.format", "is only valid with source")
            if not isinstance(table["rows"], list):
                _fail(f"{path}.rows", "must be an array of tables")
            raw_rows = table["rows"]
        rows = [_normalize_row(row, f"{path}.rows[{index}]") for index, row in enumerate(raw_rows)]
        rows.sort(key=lambda row: canonical_json(row))
        seen: set[bytes] = set()
        for row in rows:
            identity = canonical_json({field: row.get(field) for field in key})
            if identity in seen:
                _fail(path, f"duplicate inventory key {identity.decode('utf-8')}")
            seen.add(identity)
        inventory = Inventory(
            name=name,
            key=key,
            rows=tuple(rows),
            source=source_text,
            source_sha256=source_sha,
        )
        result.append(inventory)
        by_name[name] = inventory
    if not result:
        _fail("inventories", "must define at least one inventory")
    return tuple(result), by_name


def _parse_alternative(value: Any, path: str) -> OutputAlternative:
    table = _table(value, path)
    _strict(table, _ALTERNATIVE_KEYS, path)
    return OutputAlternative(
        path=_text(table.get("path"), f"{path}.path"),
        when_present=_names(table.get("when_present", []), f"{path}.when_present"),
        when_absent=_names(table.get("when_absent", []), f"{path}.when_absent"),
        external=bool(table.get("external", False)),
    )


def _parse_output(value: Any, path: str) -> OutputDeclaration:
    table = _table(value, path)
    _strict(table, _OUTPUT_KEYS, path)
    name = checked_name(table.get("name"), f"{path}.name")
    kind = table.get("kind", "file")
    if kind not in {"file", "directory", "symlink"}:
        _fail(f"{path}.kind", "must be file, directory, or symlink")
    if ("path" in table) == ("alternatives" in table):
        _fail(path, "requires exactly one of path or alternatives")
    if "path" in table:
        alternatives: tuple[OutputAlternative, ...] = (
            OutputAlternative(
                path=_text(table["path"], f"{path}.path"),
                external=bool(table.get("external", False)),
            ),
        )
    else:
        raw = table["alternatives"]
        if not isinstance(raw, list) or not raw:
            _fail(f"{path}.alternatives", "must be a non-empty array")
        alternatives = tuple(
            _parse_alternative(item, f"{path}.alternatives[{i}]") for i, item in enumerate(raw)
        )
    cardinality = table.get("cardinality")
    if cardinality is not None and cardinality not in {
        "exactly_one",
        "zero_or_one",
        "one_or_more",
    }:
        _fail(f"{path}.cardinality", "must be exactly_one, zero_or_one, or one_or_more")
    required = table.get("required")
    if required is not None and not isinstance(required, bool):
        _fail(f"{path}.required", "must be true or false")
    integer_fields = (
        "exact_matches",
        "min_matches",
        "max_matches",
        "max_matches_scanned",
        "exact_bytes",
        "min_bytes",
        "max_bytes",
    )
    for key in integer_fields:
        item = table.get(key)
        minimum = 1 if key == "max_matches_scanned" else 0
        if item is not None and (
            isinstance(item, bool) or not isinstance(item, int) or item < minimum
        ):
            _fail(f"{path}.{key}", f"must be an integer >= {minimum}")
    if int(table.get("max_matches_scanned", MAX_CONTRACT_MATCHES)) > MAX_CONTRACT_MATCHES:
        _fail(
            f"{path}.max_matches_scanned",
            f"must be <= {MAX_CONTRACT_MATCHES}",
        )
    if "exact_matches" in table and any(
        key in table for key in ("cardinality", "min_matches", "max_matches")
    ):
        _fail(f"{path}.exact_matches", "cannot be combined with other cardinality fields")
    if "cardinality" in table and any(key in table for key in ("min_matches", "max_matches")):
        _fail(f"{path}.cardinality", "cannot be combined with min_matches or max_matches")
    if table.get("min_matches", 0) > table.get("max_matches", float("inf")):
        _fail(path, "min_matches cannot exceed max_matches")
    scan_limit = int(table.get("max_matches_scanned", MAX_CONTRACT_MATCHES))
    if (
        int(table.get("exact_matches", 0)) > scan_limit
        or int(table.get("min_matches", 0)) > scan_limit
    ):
        _fail(path, "required match cardinality exceeds max_matches_scanned")
    if "exact_bytes" in table and any(key in table for key in ("min_bytes", "max_bytes")):
        _fail(f"{path}.exact_bytes", "cannot be combined with min_bytes or max_bytes")
    if table.get("min_bytes", 0) > table.get("max_bytes", float("inf")):
        _fail(path, "min_bytes cannot exceed max_bytes")
    sha256 = table.get("sha256")
    if sha256 is not None and not (
        isinstance(sha256, bool)
        or (isinstance(sha256, str) and re.fullmatch(r"[0-9a-f]{64}", sha256))
    ):
        _fail(f"{path}.sha256", "must be true, false, or a lowercase SHA-256 digest")
    if kind == "directory" and sha256:
        _fail(f"{path}.sha256", "directory hashing is not supported in schema v1")
    if kind != "file" and any(key in table for key in ("exact_bytes", "min_bytes", "max_bytes")):
        _fail(path, "byte-size predicates require kind = 'file'")
    for key in ("settle_timeout", "settle_interval", "stable_for"):
        item = table.get(key)
        if item is not None and (
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(float(item))
            or item < 0
        ):
            _fail(f"{path}.{key}", "must be a non-negative number")
    if float(table.get("settle_timeout", 0)) > 3600:
        _fail(f"{path}.settle_timeout", "must be <= 3600 seconds")
    if float(table.get("stable_for", 0)) > float(table.get("settle_timeout", 0)):
        _fail(path, "stable_for requires settle_timeout >= stable_for")
    stable_dimensions = table.get("stable_dimensions")
    if stable_dimensions is not None:
        allowed_dimensions = {"existence", "kind", "size", "mtime", "sha256"}
        if (
            not isinstance(stable_dimensions, list)
            or not stable_dimensions
            or not all(
                isinstance(item, str) and item in allowed_dimensions for item in stable_dimensions
            )
            or len(set(stable_dimensions)) != len(stable_dimensions)
        ):
            _fail(
                f"{path}.stable_dimensions",
                "must be a unique non-empty array of existence, kind, size, mtime, or sha256",
            )
    contract = {
        key: table[key]
        for key in _OUTPUT_KEYS - {"name", "kind", "path", "alternatives", "external"}
        if key in table
    }
    return OutputDeclaration(name=name, kind=kind, alternatives=alternatives, contract=contract)


def _file_digest(path: Path, location: str) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        _fail(location, f"cannot read {path}: {exc}")


def _read_script(path: Path, location: str) -> tuple[str, str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        _fail(location, f"cannot read {path}: {exc}")
    if len(raw) > MAX_SCRIPT_BYTES:
        _fail(location, f"script exceeds {MAX_SCRIPT_BYTES} bytes")
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        _fail(location, f"script is not UTF-8: {exc}")
    if not content.strip():
        _fail(location, "script must not be empty")
    return content, hashlib.sha256(raw).hexdigest()


def _parse_execution(value: Any, path: str) -> dict[str, Any]:
    execution = _table(value, path)
    _strict(execution, _EXECUTION_KEYS, path)
    mode = execution.get("mode", "single")
    if mode not in {"single", "array", "pack"}:
        _fail(f"{path}.mode", "must be single, array, or pack")
    result: dict[str, Any] = {"mode": mode}
    limits = {
        "max_concurrent": 100_000,
        "max_array_size": 10_000,
        "max_processes": 1024,
        "units_per_allocation": 10_000,
    }
    for key, maximum in limits.items():
        if key not in execution:
            continue
        item = execution[key]
        if isinstance(item, bool) or not isinstance(item, int) or not 1 <= item <= maximum:
            _fail(f"{path}.{key}", f"must be an integer from 1 to {maximum}")
        result[key] = item
    array_only = {"max_concurrent", "max_array_size"}
    pack_only = {"max_processes", "units_per_allocation"}
    if mode == "single" and any(key in execution for key in array_only | pack_only):
        _fail(path, "single execution does not accept array or pack limits")
    if mode == "array" and any(key in execution for key in pack_only):
        _fail(path, "array execution does not accept pack limits")
    if mode == "pack":
        result.setdefault("max_processes", 1)
        result.setdefault("units_per_allocation", result["max_processes"])
        result.setdefault("max_array_size", 1000)
    elif mode == "array":
        result.setdefault("max_array_size", 1000)
    return result


def _parse_validator(value: Any, path: str, base: Path) -> dict[str, Any]:
    validator = _table(value, path)
    _strict(validator, _VALIDATOR_KEYS, path)
    kind = validator.get("kind")
    if kind != "command":
        _fail(f"{path}.kind", "schema v1 supports only 'command'")
    argv = validator.get("argv")
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(item, str) and item and "\x00" not in item for item in argv)
    ):
        _fail(f"{path}.argv", "must be a non-empty array of strings without NUL")
    for index, argument in enumerate(argv):
        if "{" in _PLACEHOLDER.sub("", argument) or "}" in _PLACEHOLDER.sub("", argument):
            _fail(f"{path}.argv[{index}]", "contains an invalid placeholder")
    if _PLACEHOLDER.search(argv[0]):
        _fail(f"{path}.argv[0]", "the validator executable cannot be a placeholder")
    files = validator.get("files", [])
    if not isinstance(files, list) or not all(isinstance(item, str) and item for item in files):
        _fail(f"{path}.files", "must be an array of local paths")
    references = list(files)
    for item in argv:
        if "{" in item or "}" in item or item in references:
            continue
        candidate = (base / item).resolve()
        if not Path(item).is_absolute() and candidate.is_file():
            references.append(item)
    timeout = validator.get("timeout", 300)
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= 3600:
        _fail(f"{path}.timeout", "must be an integer from 1 to 3600")
    cwd = validator.get("cwd")
    if cwd is not None:
        _text(cwd, f"{path}.cwd")
    env = validator.get("env", {})
    if not isinstance(env, Mapping) or not all(
        isinstance(key, str)
        and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key)
        and isinstance(item, str)
        and "\x00" not in item
        for key, item in env.items()
    ):
        _fail(f"{path}.env", "must map environment names to strings without NUL")
    normalized = dict(validator)
    normalized["timeout"] = timeout
    normalized["env"] = dict(env)
    normalized["referenced_sha256"] = {
        item: _file_digest((base / item).resolve(), f"{path}.files") for item in references
    }
    return normalized


def _parse_stages(
    raw: Any, base: Path, inventories: Mapping[str, Inventory], environments: Mapping[str, Any]
) -> tuple[Stage, ...]:
    tables = _table(raw, "stages")
    stages: list[Stage] = []
    for name, value in sorted(tables.items()):
        checked_name(name, f"stages.{name}")
        path = f"stages.{name}"
        table = _table(value, path)
        _strict(table, _STAGE_KEYS, path)
        foreach = checked_name(table.get("foreach"), f"{path}.foreach")
        if foreach not in inventories:
            _fail(f"{path}.foreach", f"unknown inventory {foreach!r}")
        script = _text(table.get("script"), f"{path}.script")
        script_content, script_sha = _read_script((base / script).resolve(), f"{path}.script")
        environment = table.get("environment")
        if environment is not None:
            environment = checked_name(environment, f"{path}.environment")
            if environment not in environments:
                _fail(f"{path}.environment", f"unknown environment {environment!r}")
        raw_needs = table.get("needs", [])
        if not isinstance(raw_needs, list):
            _fail(f"{path}.needs", "must be an array of tables")
        needs: list[DependencyJoin] = []
        for index, item in enumerate(raw_needs):
            need_path = f"{path}.needs[{index}]"
            need = _table(item, need_path)
            _strict(need, _NEED_KEYS, need_path)
            upstream = checked_name(need.get("stage"), f"{need_path}.stage")
            require = need.get("require", "verified")
            if require not in {"completed", "outputs_present", "verified"}:
                _fail(
                    f"{need_path}.require",
                    "must be completed, outputs_present, or verified",
                )
            allow_empty = need.get("allow_empty", False)
            if not isinstance(allow_empty, bool):
                _fail(f"{need_path}.allow_empty", "must be true or false")
            needs.append(
                DependencyJoin(
                    stage=upstream,
                    on=_names(need.get("on", []), f"{need_path}.on"),
                    require=require,
                    allow_empty=allow_empty,
                )
            )
        raw_outputs = table.get("outputs", [])
        if not isinstance(raw_outputs, list):
            _fail(f"{path}.outputs", "must be an array of tables")
        if len(raw_outputs) > MAX_OUTPUTS_PER_STAGE:
            _fail(f"{path}.outputs", f"must contain at most {MAX_OUTPUTS_PER_STAGE} outputs")
        outputs = tuple(
            _parse_output(item, f"{path}.outputs[{index}]")
            for index, item in enumerate(raw_outputs)
        )
        output_names = [output.name for output in outputs]
        if len(output_names) != len(set(output_names)):
            _fail(f"{path}.outputs", "contains duplicate output names")
        validators = table.get("validators", [])
        if not isinstance(validators, list) or not all(isinstance(v, Mapping) for v in validators):
            _fail(f"{path}.validators", "must be an array of tables")
        if len(validators) > MAX_VALIDATORS_PER_STAGE:
            _fail(
                f"{path}.validators",
                f"must contain at most {MAX_VALIDATORS_PER_STAGE} validators",
            )
        normalized_validators = tuple(
            _parse_validator(validator, f"{path}.validators[{index}]", base)
            for index, validator in enumerate(validators)
        )
        inventory_fields = {key for row in inventories[foreach].rows for key in row}
        valid_outputs = set(output_names)
        for validator_index, validator in enumerate(normalized_validators):
            for argv_index, argument in enumerate(validator["argv"]):
                for match in _PLACEHOLDER.finditer(argument):
                    placeholder = match.group(1)
                    location = f"{path}.validators[{validator_index}].argv[{argv_index}]"
                    if placeholder.startswith("output:"):
                        output_name = placeholder.split(":", 1)[1]
                        if output_name not in valid_outputs:
                            _fail(location, f"references unknown output {output_name!r}")
                    elif placeholder not in inventory_fields:
                        _fail(location, f"references unknown inventory field {placeholder!r}")
        execution = _parse_execution(table.get("execution", {}), f"{path}.execution")
        resources = dict(_table(table.get("resources", {}), f"{path}.resources"))
        if any(key.replace("-", "_") in {"array", "dependency", "job_name"} for key in resources):
            _fail(
                f"{path}.resources",
                "array, dependency, and job_name are owned by campaign execution",
            )
        try:
            canonical_json(resources)
        except (TypeError, ValueError) as exc:
            _fail(f"{path}.resources", f"must contain JSON-compatible values: {exc}")
        stages.append(
            Stage(
                name=name,
                foreach=foreach,
                script=script,
                script_sha256=script_sha,
                script_content=script_content,
                environment=environment,
                needs=tuple(needs),
                outputs=outputs,
                execution=execution,
                resources=resources,
                validators=normalized_validators,
            )
        )
    if not stages:
        _fail("stages", "must define at least one stage")
    if len(stages) > MAX_STAGES:
        _fail("stages", f"must define at most {MAX_STAGES} stages")
    if sum(len(stage.script_content.encode("utf-8")) for stage in stages) > MAX_TOTAL_SCRIPT_BYTES:
        _fail("stages", f"combined script content exceeds {MAX_TOTAL_SCRIPT_BYTES} bytes")
    return tuple(stages)


def _topological(stages: Sequence[Stage]) -> tuple[Stage, ...]:
    by_name = {stage.name: stage for stage in stages}
    incoming = {stage.name: 0 for stage in stages}
    outgoing: dict[str, list[str]] = defaultdict(list)
    for stage in stages:
        for need in stage.needs:
            if need.stage not in by_name:
                _fail(f"stages.{stage.name}.needs", f"unknown upstream stage {need.stage!r}")
            incoming[stage.name] += 1
            outgoing[need.stage].append(stage.name)
    ready = deque(sorted(name for name, count in incoming.items() if count == 0))
    ordered: list[Stage] = []
    while ready:
        name = ready.popleft()
        ordered.append(by_name[name])
        for downstream in sorted(outgoing[name]):
            incoming[downstream] -= 1
            if incoming[downstream] == 0:
                ready.append(downstream)
    if len(ordered) != len(stages):
        cycle = sorted(name for name, count in incoming.items() if count)
        _fail("stages", f"dependency cycle involving {cycle}")
    return tuple(ordered)


def _safe_path_value(value: Any, path: str) -> str:
    text = str(value)
    if not text or text in {".", ".."} or any(ch in text for ch in ("/", "\\", "\x00")):
        _fail(path, f"{text!r} is not one safe path segment")
    return text


def _render_path(template: str, row: Mapping[str, Any], path: str) -> str:
    cursor = 0
    chunks: list[str] = []
    for match in _PLACEHOLDER.finditer(template):
        literal = template[cursor : match.start()]
        if "{" in literal or "}" in literal:
            _fail(path, "contains an invalid placeholder")
        name = match.group(1)
        if name.startswith("output:"):
            _fail(path, "output placeholders are not valid inside output paths")
        if name not in row:
            _fail(path, f"unresolved placeholder {{{name}}}")
        chunks.append(literal)
        chunks.append(_safe_path_value(row[name], path))
        cursor = match.end()
    tail = template[cursor:]
    if "{" in tail or "}" in tail:
        _fail(path, "contains an invalid placeholder")
    chunks.append(tail)
    return "".join(chunks)


def _resolve_output(
    output: OutputDeclaration, row: Mapping[str, Any], output_root: str, path: str
) -> str:
    applicable = [
        alt
        for alt in output.alternatives
        if all(name in row for name in alt.when_present)
        and all(name not in row for name in alt.when_absent)
    ]
    if len(applicable) != 1:
        _fail(path, f"expected exactly one applicable path alternative, found {len(applicable)}")
    alternative = applicable[0]
    rendered = _render_path(alternative.path, row, path)
    rendered_path = PurePosixPath(rendered)
    if rendered_path.is_absolute():
        if not alternative.external:
            _fail(path, "absolute output path requires external = true")
        return str(rendered_path)
    if ".." in rendered_path.parts and not alternative.external:
        _fail(path, "output path escapes output_root; declare external = true")
    return str(PurePosixPath(output_root) / rendered_path)


def _compile_units(
    stages: Sequence[Stage], inventories: Mapping[str, Inventory], output_root: str, max_units: int
) -> tuple[WorkUnit, ...]:
    units: list[WorkUnit] = []
    by_stage: dict[str, list[WorkUnit]] = {}
    paths: dict[str, str] = {}
    for stage in stages:
        stage_units: list[WorkUnit] = []
        inventory = inventories[stage.foreach]
        for ordinal, row in enumerate(inventory.rows):
            dependencies: list[str] = []
            for need_index, need in enumerate(stage.needs):
                upstream = by_stage[need.stage]
                matches = [
                    unit
                    for unit in upstream
                    if all(unit.keys.get(key) == row.get(key) for key in need.on)
                ]
                if not matches and not need.allow_empty:
                    _fail(
                        f"stages.{stage.name}.needs[{need_index}]",
                        f"row {row!r} matches no unit in stage {need.stage!r}",
                    )
                dependencies.extend(unit.unit_id for unit in matches)
            identity = hashlib.sha256(
                canonical_json({"stage": stage.name, "keys": row})
            ).hexdigest()[:24]
            unit_id = f"{stage.name}.{identity}"
            outputs: dict[str, str] = {}
            for index, output in enumerate(stage.outputs):
                location = f"stages.{stage.name}.outputs[{index}]"
                resolved = _resolve_output(output, row, output_root, location)
                prior = paths.get(resolved)
                if prior is not None:
                    _fail(location, f"output path collides with unit {prior}: {resolved}")
                paths[resolved] = unit_id
                outputs[output.name] = resolved
            unit = WorkUnit(
                unit_id=unit_id,
                stage=stage.name,
                ordinal=ordinal,
                keys=dict(row),
                dependencies=tuple(sorted(set(dependencies))),
                outputs=outputs,
            )
            units.append(unit)
            stage_units.append(unit)
            if len(units) > max_units:
                _fail("stages", f"campaign expands beyond max_units={max_units}")
        by_stage[stage.name] = stage_units
    return tuple(units)


def pilot_units(definition: CampaignDefinition, pilot_name: str) -> tuple[WorkUnit, ...]:
    """Compile the named pilot selection against its own output root."""

    pilot = definition.pilots.get(pilot_name)
    if pilot is None:
        _fail("pilots", f"unknown pilot {pilot_name!r}")
    inventory_name = str(pilot["inventory"])
    selection = pilot.get("select", {})
    selected_rows = [
        row
        for row in definition.inventory(inventory_name).rows
        if all(row.get(field) in values for field, values in selection.items())
    ]
    selected_keys = {canonical_json(row) for row in selected_rows}
    output_root = str(pilot["output_root"])
    stages = {stage.name: stage for stage in definition.stages}
    units: list[WorkUnit] = []
    for unit in definition.units:
        stage = stages[unit.stage]
        if stage.foreach != inventory_name or canonical_json(unit.keys) not in selected_keys:
            continue
        outputs = {
            output.name: _resolve_output(
                output,
                unit.keys,
                output_root,
                f"pilots.{pilot_name}.stages.{stage.name}.outputs.{output.name}",
            )
            for output in stage.outputs
        }
        units.append(replace(unit, outputs=outputs))
    return tuple(units)


def pilot_alternative_coverage(
    definition: CampaignDefinition, pilot_name: str
) -> list[dict[str, Any]]:
    """Report whether selected pilot rows exercise every declared path layout."""

    pilot = definition.pilots.get(pilot_name)
    if pilot is None:
        _fail("pilots", f"unknown pilot {pilot_name!r}")
    inventory_name = str(pilot["inventory"])
    selection = pilot.get("select", {})
    rows = [
        row
        for row in definition.inventory(inventory_name).rows
        if all(row.get(field) in values for field, values in selection.items())
    ]
    coverage: list[dict[str, Any]] = []
    for stage in definition.stages:
        if stage.foreach != inventory_name:
            continue
        for output in stage.outputs:
            covered = []
            for index, alternative in enumerate(output.alternatives):
                if any(
                    all(name in row for name in alternative.when_present)
                    and all(name not in row for name in alternative.when_absent)
                    for row in rows
                ):
                    covered.append(index)
            coverage.append(
                {
                    "stage": stage.name,
                    "output": output.name,
                    "alternatives": len(output.alternatives),
                    "covered": covered,
                    "missing": sorted(set(range(len(output.alternatives))) - set(covered)),
                }
            )
    return coverage


def compile_campaign(
    value: Mapping[str, Any],
    *,
    base_dir: Path | None = None,
    source_path: Path | None = None,
    max_units: int = MAX_UNITS,
    host_config: Any | None = None,
) -> CampaignDefinition:
    """Compile a parsed schema-v1 mapping into an immutable definition."""

    root = _table(value, "campaign")
    _strict(root, _ROOT_KEYS, "campaign")
    if root.get("schema") != SCHEMA_VERSION:
        _fail("schema", f"must be {SCHEMA_VERSION}")
    name = checked_name(root.get("name"), "name")
    host = checked_name(root.get("host"), "host")
    project = root.get("project")
    if project is not None:
        project = checked_name(project, "project")
    workspace = _table(root.get("workspace"), "workspace")
    _strict(workspace, _WORKSPACE_KEYS, "workspace")
    for required in ("remote_root", "output_root"):
        workspace[required] = _text(workspace.get(required), f"workspace.{required}")
    deployment = workspace.get("deployment", "snapshot")
    if deployment not in {"snapshot", "mutable"}:
        _fail("workspace.deployment", "must be snapshot or mutable")
    workspace["deployment"] = deployment
    base = base_dir or Path.cwd()
    inventories, inventory_map = _parse_inventories(root.get("inventories"), base)
    environments = _table(root.get("environments", {}), "environments")
    normalized_environments: dict[str, dict[str, Any]] = {}
    for environment_name, environment_value in sorted(environments.items()):
        checked_name(environment_name, f"environments.{environment_name}")
        environment = _table(environment_value, f"environments.{environment_name}")
        _strict(environment, _ENVIRONMENT_KEYS, f"environments.{environment_name}")
        declaration = environment.get("declaration")
        if declaration is not None:
            declaration = _table(declaration, f"environments.{environment_name}.declaration")
            lockfile = declaration.get("lockfile")
            if lockfile is not None:
                lockfile = _text(lockfile, f"environments.{environment_name}.declaration.lockfile")
                declaration = dict(declaration)
                declaration["lockfile_sha256"] = _file_digest(
                    (base / lockfile).resolve(),
                    f"environments.{environment_name}.declaration.lockfile",
                )
                environment["declaration"] = declaration
        if host_config is not None and environment.get("template"):
            template = host_config.resolve_template(str(environment["template"]))
            environment["resolved_template"] = {
                "name": template.name,
                "options": template.options,
                "preamble": template.preamble,
                "epilogue": template.epilogue,
            }
        normalized_environments[environment_name] = environment
    stages = _topological(
        _parse_stages(root.get("stages"), base, inventory_map, normalized_environments)
    )
    resolved_host: dict[str, Any] = {}
    if host_config is not None:
        resolved_host = {
            "name": host_config.name,
            "account": host_config.account,
            "partition": host_config.partition,
            "defaults": dict(host_config.defaults),
        }
        resolved_stages: list[Stage] = []
        for stage in stages:
            resources = dict(host_config.defaults)
            environment = normalized_environments.get(stage.environment or "", {})
            template = environment.get("resolved_template", {})
            resources.update(template.get("options", {}))
            resources.update(environment.get("options", {}))
            resources.update(stage.resources)
            resources.setdefault("account", host_config.account)
            resources.setdefault("partition", host_config.partition)
            resources = {key: value for key, value in resources.items() if value is not None}
            resolved_stages.append(replace(stage, resources=resources))
        stages = tuple(resolved_stages)
    pilots = _table(root.get("pilots", {}), "pilots")
    normalized_pilots: dict[str, dict[str, Any]] = {}
    for pilot_name, pilot_value in sorted(pilots.items()):
        checked_name(pilot_name, f"pilots.{pilot_name}")
        pilot = _table(pilot_value, f"pilots.{pilot_name}")
        _strict(pilot, _PILOT_KEYS, f"pilots.{pilot_name}")
        inventory_name = checked_name(pilot.get("inventory"), f"pilots.{pilot_name}.inventory")
        if inventory_name not in inventory_map:
            _fail(f"pilots.{pilot_name}.inventory", f"unknown inventory {inventory_name!r}")
        selection = _table(pilot.get("select", {}), f"pilots.{pilot_name}.select")
        known_fields = {key for row in inventory_map[inventory_name].rows for key in row}
        normalized_selection: dict[str, list[str | int | float | bool]] = {}
        for field, raw_values in sorted(selection.items()):
            checked_name(field, f"pilots.{pilot_name}.select.{field}")
            if field not in known_fields:
                _fail(f"pilots.{pilot_name}.select.{field}", "unknown inventory field")
            if not isinstance(raw_values, list) or not raw_values:
                _fail(f"pilots.{pilot_name}.select.{field}", "must be a non-empty array")
            normalized_selection[field] = [
                _scalar(item, f"pilots.{pilot_name}.select.{field}[{index}]")
                for index, item in enumerate(raw_values)
            ]
        output_root = _text(pilot.get("output_root"), f"pilots.{pilot_name}.output_root")
        matching = [
            row
            for row in inventory_map[inventory_name].rows
            if all(row.get(field) in values for field, values in normalized_selection.items())
        ]
        if not matching:
            _fail(f"pilots.{pilot_name}.select", "matches no inventory rows")
        normalized_pilots[pilot_name] = {
            "inventory": inventory_name,
            "select": normalized_selection,
            "output_root": output_root,
        }
    units = _compile_units(stages, inventory_map, workspace["output_root"], max_units)
    identity_value = {
        "schema": SCHEMA_VERSION,
        "name": name,
        "host": host,
        "project": project,
        "resolved_host": resolved_host,
        "workspace": workspace,
        "inventories": inventories,
        "environments": normalized_environments,
        "stages": stages,
        "units": units,
        "pilots": normalized_pilots,
    }
    identity_material = dict(identity_value)
    identity_material["inventories"] = [
        {
            "name": inventory.name,
            "key": inventory.key,
            "rows": inventory.rows,
            "source": inventory.source,
            "normalized_sha256": hashlib.sha256(canonical_json(inventory.rows)).hexdigest(),
        }
        for inventory in inventories
    ]
    identity = hashlib.sha256(canonical_json(_jsonable(identity_material))).hexdigest()
    return CampaignDefinition(
        schema=SCHEMA_VERSION,
        name=name,
        host=host,
        project=project,
        resolved_host=resolved_host,
        workspace=workspace,
        inventories=inventories,
        environments=normalized_environments,
        stages=stages,
        units=units,
        pilots=normalized_pilots,
        definition_id=identity,
        source_path=str(source_path) if source_path else None,
    )


def load_campaign(
    path: str | Path,
    *,
    max_units: int = MAX_UNITS,
    host_config: Any | None = None,
) -> CampaignDefinition:
    """Load and compile one checked-in TOML campaign file."""

    source = Path(path).expanduser().resolve()
    try:
        with source.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise InvalidArgument(f"cannot load campaign {source}: {exc}", path=str(source)) from exc
    return compile_campaign(
        value,
        base_dir=source.parent,
        source_path=source,
        max_units=max_units,
        host_config=host_config,
    )
