# Synthesized Slurm ~20.02-era fixtures

**These files are hand-built, NOT captured from a real cluster.** They approximate the output an
older Slurm (roughly the 20.02 / 20.11 generation, e.g. Alliance Cedar/Graham of that era) would
produce for the exact `-o`/`-P`/`-h` invocations remoteslurm's stub uses. They exist so the parsers
in `remoteslurm.slurm` are exercised against a second, older output shape in addition to the
trillium (Slurm 25.11) fixtures in the parent directory.

Deliberate differences from the 25.11 fixtures, to catch version drift:

- `sacct_completed.txt` uses the **old `ReqMem` form with a per-node/per-core suffix** (`4Gc` =
  4 GiB per core). Slurm < 21 always appended `n`/`c`; `_parse_mem` must tolerate the trailing
  letter. (The 25.11 fixtures use the newer plain `767000M` form.)
- `scontrol_job.txt` uses `TRES=`/`MinMemoryCPU=` and omits fields that only newer Slurm emits
  (`AccrueTime`, `LastSchedEval`, `AllocTRES`, `ReqTRES`), to confirm the generic key=value parser
  does not depend on any 25.x-only key.
- `squeue_running.txt` / `sinfo.txt` use the same delimited `-o` format (Slurm 20.02 already
  supported every format code we request), with older-style account/partition/node names.

If a real Nibi or other older cluster is captured later, add its output under a sibling directory
(e.g. `nibi/`) rather than editing these synthesized files.
