// Example data shaped like the planned /api/overview + /api/work payloads.
// Times are seconds relative to NOW (negative = past). The UI labels this as example data.
window.FIXTURE = (function () {
  const m = 60, h = 3600;
  // observedAge: seconds since the daemon's last scheduler read for this host.
  // alloc: how the cluster allocates (trillium = whole nodes, nibi = per core).
  const hosts = [
    { id: "trillium", login: "tri-login03", observedAge: 4, accounting: "ok", alloc: "node", coresPerNode: 192 },
    { id: "nibi", login: "l3", observedAge: 12, accounting: "ok", accountingSince: null, alloc: "core" },
  ];
  // fit_sweep: 200 tasks, %16 throttle. Per-task state; failed tasks carry exit codes.
  const sweep = [];
  for (let i = 0; i < 200; i++) sweep.push(i < 141 ? "COMPLETED" : i < 157 ? "RUNNING" : "PENDING");
  sweep[63] = "FAILED"; sweep[117] = "FAILED";
  const jobs = [
    {
      id: "4412702", name: "fit_sweep", host: "trillium", partition: "compute", tracked: true,
      state: "RUNNING", array: { n: 200, tasks: sweep, limit: 16, exits: { 63: "1:0", 117: "1:0" }, pendingReason: "JobArrayTaskLimit",
        taskInfo: { 63: { elapsed: 52 * m, maxrss: "212G", node: "tri-c0533", at: -3.3 * h }, 117: { elapsed: 2 * h + 6 * m, maxrss: "198G", node: "tri-c0610", at: -0.9 * h } } },
      submit: -5.2 * h, start: -4.9 * h, timelimit: 3 * h, perTask: "1 node · 192 cores", mem: "750G",
      workdir: "/scratch/bbuchs/mvpa-rsa/sweeps/fit", stdout: "logs/fit_sweep_%A_%a.out", stderr: null,
      campaign: "rsa-sweep · run 7", sweepParam: "alpha × roi", artifacts: "UNCHECKED", validation: null,
    },
    {
      id: "4412766", name: "fmriprep_sub-14", host: "trillium", partition: "compute", tracked: true,
      state: "RUNNING", submit: -1.6 * h, start: -1.2 * h, timelimit: 8 * h, nodes: 1, nodelist: "tri-c0412",
      cpus: 192, mem: "750G", workdir: "/scratch/bbuchs/ds-recall/derivatives", stdout: "logs/fmriprep_sub-14.out",
      stderr: null, artifacts: "SETTLING", validation: null, logRead: -6,
    },
    {
      id: "4412794", name: "searchlight_perm", host: "trillium", partition: "compute", tracked: true,
      state: "RUNNING", submit: -0.8 * h, start: -0.3 * h - 41, timelimit: 1 * h, nodes: 4, nodelist: "tri-c[0201-0204]",
      cpus: 768, mem: "3000G", workdir: "/scratch/bbuchs/mvpa-rsa/perm", stdout: "logs/perm.out", artifacts: "UNCHECKED", validation: null,
    },
    {
      id: "4412871", name: "mvpa_perm_null", host: "trillium", partition: "compute", tracked: true,
      state: "PENDING", submit: -0.4 * h, reason: "Priority", estStart: 1.4 * h, timelimit: 6 * h,
      nodes: 8, cpus: 1536, mem: "6000G", workdir: "/scratch/bbuchs/mvpa-rsa/perm", stdout: "logs/null_%j.out",
    },
    {
      id: "4412880", name: "rsa_bootstrap", host: "trillium", partition: "compute", tracked: true,
      state: "PENDING", submit: -0.35 * h, reason: "QOSMaxJobsPerUserLimit", estStart: null, timelimit: 2 * h,
      nodes: 1, cpus: 192, mem: "750G", workdir: "/scratch/bbuchs/mvpa-rsa/boot", stdout: "logs/boot.out",
    },
    {
      id: "8829911", name: "freesurfer_recon", host: "nibi", partition: "cpubase_bycore_b3", tracked: true,
      state: "RUNNING", submit: -7.5 * h, start: -6.8 * h - 300, timelimit: 12 * h, nodes: 1, nodelist: "c1137",
      cpus: 8, mem: "32G", workdir: "/project/def-bbuchs/ds-recall/fs", stdout: "recon.log", artifacts: "UNCHECKED", validation: null,
    },
    {
      id: "8830330", name: "glm_firstlevel", host: "nibi", partition: "cpubase_bycore_b2", tracked: false,
      state: "PENDING", submit: -0.2 * h, reason: "Resources", estStart: 0.5 * h, timelimit: 3 * h,
      nodes: 1, cpus: 16, mem: "64G", workdir: "/project/def-bbuchs/ds-recall/glm", stdout: null,
    },
    {
      id: "8830331", name: "glm_firstlevel", host: "nibi", partition: "cpubase_bycore_b2", tracked: false,
      state: "PENDING", submit: -0.2 * h, reason: "Resources", estStart: 0.5 * h, timelimit: 3 * h,
      nodes: 1, cpus: 16, mem: "64G", workdir: "/project/def-bbuchs/ds-recall/glm", stdout: null,
    },
    {
      id: "8830187", name: "fit_sub-07", host: "nibi", partition: "cpubase_bycore_b3", tracked: true,
      state: "OUT_OF_MEMORY", submit: -3.1 * h, start: -2.6 * h, end: -0.07 * h, timelimit: 4 * h,
      nodes: 1, nodelist: "c1042", cpus: 8, mem: "32G", maxrss: "31.8G", workdir: "/project/def-bbuchs/mvpa-rsa/subj",
      stdout: "logs/fit_sub-07.out", stderr: "logs/fit_sub-07.err", exit: "0:125", artifacts: "MISSING", validation: null,
      hint: "Killed at the 32G request while filling a 40 GiB permutation buffer (output log lines 7–8). The buffer alone is larger than the request, so it needs well over 40G plus what the job already held, or fewer permutations per job.",
      logRead: -250,
    },
    {
      id: "4412812", name: "qc_report", host: "trillium", partition: "compute", tracked: true,
      state: "COMPLETED", submit: -0.6 * h, start: -0.02 * h - 862, end: -0.02 * h, timelimit: 1 * h,
      nodes: 1, nodelist: "tri-c0118", cpus: 192, mem: "750G", workdir: "/scratch/bbuchs/ds-recall/qc", stdout: "qc.out",
      exit: "0:0", artifacts: "PRESENT", validation: "PASSED",
    },
    {
      id: "4412611", name: "fmriprep_sub-13", host: "trillium", partition: "compute", tracked: true,
      state: "COMPLETED", submit: -9.5 * h, start: -9 * h, end: -2.1 * h, timelimit: 8 * h,
      nodes: 1, nodelist: "tri-c0412", cpus: 192, mem: "750G", workdir: "/scratch/bbuchs/ds-recall/derivatives",
      stdout: "logs/fmriprep_sub-13.out", exit: "0:0", artifacts: "PRESENT", validation: "NOT_RUN",
    },
    {
      id: "4412455", name: "decode_timegen", host: "trillium", partition: "compute", tracked: true,
      state: "TIMEOUT", submit: -14 * h, start: -13 * h, end: -11 * h, timelimit: 2 * h,
      nodes: 2, cpus: 384, mem: "1500G", workdir: "/scratch/bbuchs/mvpa-rsa/timegen", stdout: "tg.out", stderr: "tg.err",
      exit: "0:15", artifacts: "MISSING", validation: null,
      hint: "Reached its 2:00:00 limit. The last checkpoint in its log was written at 02:54, eight minutes before the kill.",
      logRead: -10.9 * h,
    },
    {
      id: "8829990", name: "bids_validate", host: "nibi", partition: "cpubase_bycore_b1", tracked: true,
      state: "COMPLETED", submit: -5 * h, start: -4.8 * h - 340, end: -4.8 * h, timelimit: 0.5 * h,
      nodes: 1, cpus: 2, mem: "4G", workdir: "/project/def-bbuchs/ds-recall", stdout: "bv.out", exit: "0:0",
      artifacts: "PRESENT", validation: "PASSED",
    },
    {
      id: "8829702", name: "mriqc_group", host: "nibi", partition: "cpubase_bycore_b2", tracked: false,
      state: "COMPLETED", submit: -12 * h, start: -11.5 * h, end: -10.2 * h, timelimit: 3 * h,
      nodes: 1, cpus: 16, mem: "64G", workdir: "/project/def-bbuchs/ds-recall/mriqc", stdout: null, exit: "0:0",
    },
    {
      id: "4412390", name: "smoke_test", host: "trillium", partition: "debug", tracked: true,
      state: "CANCELLED", submit: -16 * h, start: -15.9 * h, end: -15.8 * h, timelimit: 0.25 * h,
      nodes: 1, cpus: 192, mem: "750G", workdir: "/scratch/bbuchs/mvpa-rsa", stdout: "smoke.out", exit: "0:15",
      cancelledBy: "you (scancel)", logEmpty: true, logRead: -15.7 * h,
    },
  ];
  const activity = [
    { t: -0.02 * h, kind: "ok", host: "trillium", text: "qc_report completed", job: "4412812", detail: "exit 0:0 · 14m 22s" },
    { t: -0.07 * h, kind: "fail", host: "nibi", text: "fit_sub-07 ran out of memory", job: "8830187", detail: "MaxRSS 31.8G of 32G" },
    { t: -0.2 * h, kind: "pend", host: "nibi", text: "2 × glm_firstlevel queued", job: "8830330", detail: "untracked · Resources" },
    { t: -0.3 * h - 41, kind: "run", host: "trillium", text: "searchlight_perm started", job: "4412794", detail: "4 nodes" },
    { t: -0.35 * h, kind: "pend", host: "trillium", text: "rsa_bootstrap submitted", job: "4412880", detail: "QOSMaxJobsPerUserLimit" },
    { t: -0.4 * h, kind: "pend", host: "trillium", text: "mvpa_perm_null submitted", job: "4412871", detail: "Priority · est. start 15:26" },
    { t: -0.9 * h, kind: "fail", host: "trillium", text: "fit_sweep task 117 failed", job: "4412702", detail: "exit 1:0" },
    { t: -1.2 * h, kind: "run", host: "trillium", text: "fmriprep_sub-14 started", job: "4412766", detail: "tri-c0412" },
    { t: -2.1 * h, kind: "ok", host: "trillium", text: "fmriprep_sub-13 completed", job: "4412611", detail: "exit 0:0 · 6h 54m" },
    { t: -3.3 * h, kind: "fail", host: "trillium", text: "fit_sweep task 63 failed", job: "4412702", detail: "exit 1:0" },
    { t: -4.8 * h, kind: "ok", host: "nibi", text: "bids_validate completed", job: "8829990", detail: "validation passed" },
    { t: -4.9 * h, kind: "run", host: "trillium", text: "fit_sweep started", job: "4412702", detail: "200 tasks, 16 at a time" },
    { t: -11 * h, kind: "fail", host: "trillium", text: "decode_timegen hit its time limit", job: "4412455", detail: "2:00:00" },
  ];
  const logs = {
    "4412455": {
      out: [
        "[01:02:41] decode_timegen: 384 cores, 41 sessions, train × test grid 120 × 120",
        "[01:38:09] checkpoint 1 written: timegen_ckpt_001.npz (sessions 1–14)",
        "[02:16:30] checkpoint 2 written: timegen_ckpt_002.npz (sessions 15–28)",
        "[02:54:12] checkpoint 3 written: timegen_ckpt_003.npz (sessions 29–39)",
        "[02:58:47] session 40/41 …",
      ],
      err: ["slurmstepd: error: *** JOB 4412455 ON tri-c0281 CANCELLED AT 2026-11-05T03:02:00 DUE TO TIME LIMIT ***"],
    },
    "8830187": {
      out: [
        "[2026-11-05 11:26:09] fit_sub-07: loading ds-recall/sub-07 (4 runs, 812 TRs)",
        "[2026-11-05 11:26:41] masking with roi=hippocampus_bilat (2,318 voxels)",
        "[2026-11-05 11:32:57] building design: 96 conditions × 4 runs",
        "[2026-11-05 12:11:30] fold 1/8  rsa r=0.214  elapsed 45m",
        "[2026-11-05 12:50:12] fold 2/8  rsa r=0.198  elapsed 1h 24m",
        "[2026-11-05 13:22:03] fold 3/8  rsa r=0.231  elapsed 1h 56m",
        "[2026-11-05 13:55:40] allocating permutation buffer: 1000 × 2318 × 2318 float64 (40.0 GiB)",
        "[2026-11-05 13:56:02] filling permutations 1–1000",
      ],
      err: [
        "/var/spool/slurmd/job8830187/slurm_script: line 14: 1882047 Killed                  python fit.py --subject sub-07 --n-perm 1000",
        "slurmstepd: error: Detected 1 oom_kill event in StepId=8830187.batch. Some of the step tasks have been OOM Killed.",
      ],
    },
    "4412766": {
      out: [
        "261105-12:50:11,322 nipype.workflow INFO: [Node] Finished \"bold_std_trans_wf.bold_to_std_transform\"",
        "261105-12:52:40,018 nipype.workflow INFO: [Node] Setting-up \"_autobbreg_bbregister\" in \"/tmp/work/fmriprep_25_wf\"",
        "261105-13:04:12,551 nipype.workflow INFO: [Node] Finished \"_autobbreg_bbregister\", elapsed time 692.5s.",
        "261105-13:20:02,907 nipype.workflow INFO: [Node] Setting-up \"ica_aroma_wf.melodic\"",
        "261105-13:41:37,114 nipype.workflow INFO: [Node] Finished \"ica_aroma_wf.melodic\", elapsed time 1294.2s.",
        "261105-13:58:20,440 nipype.workflow INFO: [Node] Setting-up \"bold_confounds_wf.acompcor\"",
        "261105-14:01:51,002 nipype.workflow INFO: [Node] Finished \"bold_confounds_wf.acompcor\", elapsed time 210.6s.",
      ],
      err: null,
    },
  };
  return { hosts, jobs, activity, logs };
})();
