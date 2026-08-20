# Changelog

## 0.2.0 (in progress)

v2 "workflow layer" — see docs/plans/v2-plan.md.

- Project sync: `rslurm sync`/`projects` (rsync push/pull with excludes, dry-run, size guard,
  double-opt-in `--delete`, sync marker); MCP `sync`/`projects`. Uses the transport's ControlMaster.
- Remote editing: stub `edit`/`diff` ops, `rslurm edit`/`diff`, MCP `edit`/`diff` (exact-string
  replacement with occurrence checks; whole-file write no longer needed for small changes).
- Cluster knowledge: per-host `notes` + submit `templates` (preamble/epilogue, `inherit`); auto-learned
  policy notes from sbatch rejections; `rslurm notes`/`templates`/`submit --template`.
- Failure triage: `rslurm diagnose JOB` / MCP `diagnose` — merged status + script + log tails + a
  verdict and hints (OOM, timeout, node failure, missing module/path, pending reasons).
- Agent guidance: `rslurm agent-guide`, MCP resource `remoteslurm://guide`; MCP tool sets
  `REMOTESLURM_MCP_TOOLS=core|all` (default core).

## 0.1.0 — 2026-08-20

Initial release: persistent-ssh transport (ControlMaster, MFA-safe), stdlib-only remote stub,
bounded filesystem/Slurm operations, job registry, CLI (`remoteslurm`/`rslurm`), MCP server,
local session daemon. Verified live on trillium.alliancecan.ca (Slurm 25.11).
