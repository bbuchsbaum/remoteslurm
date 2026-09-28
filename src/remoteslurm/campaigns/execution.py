"""Deterministic campaign job-group planning and wrapper generation."""

from __future__ import annotations

import hashlib
import json
import shlex
from collections.abc import Iterable, Mapping
from typing import Any

from ..errors import InvalidArgument
from .spec import canonical_json

WRAPPER_SCHEMA = 1


def _chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _unit_entry(unit: Mapping[str, Any], *, index: int, slot: int | None = None) -> dict[str, Any]:
    entry = {
        "index": index,
        "unit_id": str(unit["unit_id"]),
        "ordinal": int(unit["ordinal"]),
        "keys": dict(unit.get("keys", {})),
        "outputs": {
            str(output["name"]): str(output["path"])
            for output in unit.get("artifacts", {}).get("outputs", [])
        },
    }
    if slot is not None:
        entry["slot"] = slot
    return entry


def _group(
    snapshot: Mapping[str, Any],
    stage: str,
    spec: Mapping[str, Any],
    mode: str,
    mapping: list[dict[str, Any]],
    *,
    marker_root: str,
) -> dict[str, Any]:
    contract = {
        "schema": 1,
        "wrapper_schema": WRAPPER_SCHEMA,
        "campaign": snapshot["campaign"],
        "run_id": snapshot["run_id"],
        "definition_id": snapshot["definition_id"],
        "stage": stage,
        "mode": mode,
        "script_sha256": spec["script_sha256"],
        "execution": dict(spec.get("execution", {})),
        "resources": dict(spec.get("resources", {})),
        "environment": dict(spec.get("environment", {})),
        "cwd": str(
            spec.get("environment", {}).get("workdir") or snapshot["workspace"]["remote_root"]
        ),
        "mapping": mapping,
        "marker_root": marker_root if mode == "pack" else None,
    }
    group_id = hashlib.sha256(canonical_json(contract)).hexdigest()
    return {"group_id": group_id, **contract}


def plan_job_groups(
    snapshot: Mapping[str, Any],
    *,
    stage: str | None = None,
    unit_ids: set[str] | None = None,
    marker_root: str,
) -> list[dict[str, Any]]:
    """Plan stable groups from the snapshot's currently eligible units."""

    specs = snapshot.get("stage_specs")
    if not isinstance(specs, Mapping):
        raise InvalidArgument(
            "campaign run lacks execution specifications; start a new run from the current "
            "definition"
        )
    selected: dict[str, list[Mapping[str, Any]]] = {}
    for unit in snapshot.get("units", []):
        if stage is not None and unit.get("stage") != stage:
            continue
        if unit_ids is not None and unit.get("unit_id") not in unit_ids:
            continue
        execution = unit.get("execution", {})
        retry_authorizations = execution.get("retry_authorizations", [])
        retry_ready = bool(retry_authorizations and not retry_authorizations[-1].get("consumed_by"))
        if execution.get("state") != "UNBOUND" and not retry_ready:
            continue
        if unit.get("dependency", {}).get("state") not in {"SATISFIED", "NOT_APPLICABLE"}:
            continue
        selected.setdefault(str(unit["stage"]), []).append(unit)

    groups: list[dict[str, Any]] = []
    for stage_name in sorted(selected):
        stage_spec = specs.get(stage_name)
        if not isinstance(stage_spec, Mapping):
            raise InvalidArgument(f"campaign run lacks execution specification for {stage_name!r}")
        units = sorted(
            selected[stage_name], key=lambda item: (int(item["ordinal"]), item["unit_id"])
        )
        execution = dict(stage_spec.get("execution", {}))
        mode = str(execution.get("mode", "single"))
        if mode == "single":
            groups.extend(
                _group(
                    snapshot,
                    stage_name,
                    stage_spec,
                    mode,
                    [_unit_entry(unit, index=0)],
                    marker_root=marker_root,
                )
                for unit in units
            )
        elif mode == "array":
            max_array = int(execution.get("max_array_size", 1000))
            for chunk in _chunks(units, max_array):
                mapping = [_unit_entry(unit, index=index) for index, unit in enumerate(chunk)]
                groups.append(
                    _group(
                        snapshot,
                        stage_name,
                        stage_spec,
                        mode,
                        mapping,
                        marker_root=marker_root,
                    )
                )
        elif mode == "pack":
            units_per_allocation = int(execution.get("units_per_allocation", 1))
            max_array = int(execution.get("max_array_size", 1000))
            allocations = list(_chunks(units, units_per_allocation))
            for allocation_chunk in _chunks(allocations, max_array):
                mapping = [
                    _unit_entry(unit, index=array_index, slot=slot)
                    for array_index, allocation in enumerate(allocation_chunk)
                    for slot, unit in enumerate(allocation)
                ]
                groups.append(
                    _group(
                        snapshot,
                        stage_name,
                        stage_spec,
                        mode,
                        mapping,
                        marker_root=marker_root,
                    )
                )
        else:  # compiled schema should make this unreachable
            raise InvalidArgument(f"unsupported campaign execution mode {mode!r}")
    return groups


def array_spec(group: Mapping[str, Any]) -> str | None:
    mode = group["mode"]
    if mode == "single":
        return None
    indexes = sorted({int(item["index"]) for item in group["mapping"]})
    if not indexes:
        raise InvalidArgument("array or pack group has no indexes")
    spec = f"0-{max(indexes)}"
    maximum = group.get("execution", {}).get("max_concurrent")
    if maximum is not None:
        spec += f"%{int(maximum)}"
    return spec


def _shell_json(value: Any) -> str:
    return shlex.quote(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _stage_function(stage_spec: Mapping[str, Any], attempt_id: str) -> list[str]:
    epilogue = str(stage_spec.get("epilogue") or "")
    lines = [
        f'rs_stage_file="${{TMPDIR:-/tmp}}/rslurm-stage-{attempt_id}.$$"',
        'tail -n +__RS_PAYLOAD_LINE__ "$0" > "$rs_stage_file"',
        'chmod 700 "$rs_stage_file"',
        "rs_stage_body() {",
        "  rs_first_line=",
        '  IFS= read -r rs_first_line < "$rs_stage_file" || true',
        '  case "$rs_first_line" in',
        '    \\#!*) "$rs_stage_file" ;;',
        '    *) /bin/bash "$rs_stage_file" ;;',
        "  esac",
        "  rs_stage_rc=$?",
    ]
    if epilogue:
        lines.extend("  " + line for line in epilogue.splitlines())
    lines.extend(['  return "$rs_stage_rc"', "}"])
    return lines


def _exports(group: Mapping[str, Any], item: Mapping[str, Any], attempt_id: str) -> list[str]:
    return [
        f"export RS_CAMPAIGN={shlex.quote(str(group['campaign']))}",
        f"export RS_RUN_ID={shlex.quote(str(group['run_id']))}",
        f"export RS_STAGE={shlex.quote(str(group['stage']))}",
        f"export RS_UNIT_ID={shlex.quote(str(item['unit_id']))}",
        f"export RS_ATTEMPT_ID={shlex.quote(attempt_id)}",
        f"export RS_PARAMS_JSON={_shell_json(item.get('keys', {}))}",
        f"export RS_OUTPUTS_JSON={_shell_json(item.get('outputs', {}))}",
    ]


def render_group_script(
    group: Mapping[str, Any], stage_spec: Mapping[str, Any], *, attempt_id: str
) -> str:
    """Render exact submitted bytes for one single, array, or packed attempt."""

    lines = ["#!/bin/bash", "set -u"]
    environment = stage_spec.get("environment", {})
    for key, value in sorted(environment.get("env", {}).items()):
        lines.append(f"export {key}={shlex.quote(str(value))}")
    preamble = str(stage_spec.get("preamble") or "")
    if preamble:
        lines.append(preamble)
    lines.extend(_stage_function(stage_spec, attempt_id))
    mode = str(group["mode"])
    mapping = list(group["mapping"])
    if mode == "single":
        lines.extend(_exports(group, mapping[0], attempt_id))
        lines.extend(
            ["set +e", "rs_stage_body", "rs_rc=$?", 'rm -f "$rs_stage_file"', 'exit "$rs_rc"']
        )
    elif mode == "array":
        lines.extend(['case "${SLURM_ARRAY_TASK_ID:?}" in'])
        for item in mapping:
            lines.append(f"  {int(item['index'])})")
            lines.extend("    " + line for line in _exports(group, item, attempt_id))
            lines.append("    ;;")
        lines.extend(["  *) echo 'invalid campaign array index' >&2; exit 64 ;;", "esac"])
        lines.extend(
            ["set +e", "rs_stage_body", "rs_rc=$?", 'rm -f "$rs_stage_file"', 'exit "$rs_rc"']
        )
    elif mode == "pack":
        marker_base = f"{group['marker_root'].rstrip('/')}/{group['group_id']}/{attempt_id}"
        lines.extend(
            [
                "rs_run_unit() {",
                "  rs_unit_id=$1",
                "  rs_params=$2",
                "  rs_outputs=$3",
                f"  rs_marker_base={shlex.quote(marker_base)}",
                '  rs_marker_dir="$rs_marker_base/$rs_unit_id"',
                '  mkdir -p "$rs_marker_dir"',
                '  rs_tmp="$rs_marker_dir/.started.$$"',
                "  printf '"
                + '{"schema":1,"unit_id":"%s","started_at":%s}'
                + '\\n\' "$rs_unit_id" "$(date +%s)" > "$rs_tmp"',
                '  mv "$rs_tmp" "$rs_marker_dir/started.json"',
                "  (",
                '    export RS_UNIT_ID="$rs_unit_id"',
                '    export RS_PARAMS_JSON="$rs_params"',
                '    export RS_OUTPUTS_JSON="$rs_outputs"',
                "    set +e",
                '    rs_stage_body >"$rs_marker_dir/stdout.log" 2>"$rs_marker_dir/stderr.log"',
                "    rs_rc=$?",
                '    rs_tmp="$rs_marker_dir/.finished.$$"',
                "    printf '"
                + '{"schema":1,"unit_id":"%s","finished_at":%s,"exit_code":%s,'
                + '"stdout":"stdout.log","stderr":"stderr.log"}'
                + '\\n\' "$rs_unit_id" "$(date +%s)" "$rs_rc" > "$rs_tmp"',
                '    mv "$rs_tmp" "$rs_marker_dir/finished.json"',
                '    exit "$rs_rc"',
                "  )",
                "}",
                f"export RS_CAMPAIGN={shlex.quote(str(group['campaign']))}",
                f"export RS_RUN_ID={shlex.quote(str(group['run_id']))}",
                f"export RS_STAGE={shlex.quote(str(group['stage']))}",
                f"export RS_ATTEMPT_ID={shlex.quote(attempt_id)}",
                "rs_group_rc=0",
                'case "${SLURM_ARRAY_TASK_ID:?}" in',
            ]
        )
        max_processes = int(group.get("execution", {}).get("max_processes", 1))
        by_index: dict[int, list[dict[str, Any]]] = {}
        for item in mapping:
            by_index.setdefault(int(item["index"]), []).append(item)
        for index, allocation in sorted(by_index.items()):
            lines.append(f"  {index})")
            for wave in _chunks(allocation, max_processes):
                lines.append("    rs_pids=()")
                for item in wave:
                    lines.append(
                        "    rs_run_unit "
                        + shlex.quote(str(item["unit_id"]))
                        + " "
                        + _shell_json(item.get("keys", {}))
                        + " "
                        + _shell_json(item.get("outputs", {}))
                        + ' & rs_pids+=("$!")'
                    )
                lines.append(
                    '    for rs_pid in "${rs_pids[@]}"; do wait "$rs_pid" || rs_group_rc=1; done'
                )
            lines.append("    ;;")
        lines.extend(
            [
                "  *) echo 'invalid campaign pack index' >&2; exit 64 ;;",
                "esac",
                'rm -f "$rs_stage_file"',
                'exit "$rs_group_rc"',
            ]
        )
    else:
        raise InvalidArgument(f"unsupported campaign execution mode {mode!r}")
    wrapper = "\n".join(lines) + "\n"
    payload_line = wrapper.count("\n") + 1
    wrapper = wrapper.replace("__RS_PAYLOAD_LINE__", str(payload_line), 1)
    return wrapper + str(stage_spec["script_content"])
