# Alex full-frame pipeline recovery

The 30 predictions were left over from the successful preview. The first
88-frame-block full run extracted all 517 frames, but seven Hub cancellation
requests stopped it before classification. MATLAB returned exit code zero;
the worker incorrectly marked that cancelled result done. Hub commit 538aa23
rejects cancelled/failed results independently of the exit code and records
the authenticated cancellation requester. Commit 4c714c6 reserves GPU memory
for external processors that explicitly declare GPU execution.

Alex independently resubmitted job b38095c1-ec3a-403f-8787-5588e6e030ae from
GC-DELL01-308. It processes FOV 1 / ROI 1 / all 517 frames, cut 88, and is the
current attempt. Do not create a second job while it runs.

The campaign state is stored in
`X:/Alexander/nobackup/analysis/anof_071026_X54445_LOGEB/pipeline/monitor_recovery_20261014/campaign.json`.
The same path is `/data/Alexander/...` on Linux. The Codex heartbeat attached
to this task runs every 30 minutes. It uses
`scripts/monitor_pipeline_campaign.py <campaign.json>` for Hub status, then
corroborates progression with the actual worker logs, process activity,
cgroup memory pressure, and GPU activity. A quiet classifier log alone does
not establish a stall.

Technical failures may be corrected and retried under Gilles's explicit
authorization. Preserve the selected FOV/ROI and all 517 frames, 24-GiB job
allocation, classifier models, scientific calculations, and universal ROI
save implementation. Back up outputs before replacements. Validate a code
repair with a regression test, commit/push, and deploy it without interrupting
other jobs. Record diagnoses, patches, versions and submissions in the campaign.
Use at most three further automatic submissions before reporting a persistent
blocker. Never override a subsequent authenticated human cancellation.

The retry command calls the official pipeline-run creation service, including
project preflight and locks, checks for existing attempts before insertion,
uses a fresh deterministic run ID and stop/result paths, preserves authorship
as alexander, and reserves 24576 MiB before commit. Example:

```powershell
.\.venv\Scripts\python.exe scripts/monitor_pipeline_campaign.py <campaign.json> --retry --classification-only
```

Classification-only recovery requires proven intact extracted data and
`extraction_verified=true` in the campaign. It explicitly declares GPU use.
For a demonstrated extraction-memory failure, a full rerun may use fixed
`--cut 44` or `--cut 22`, after stopping only the failed campaign job and
confirming project availability. Do not split the temporal LSTM sequence or
change scientific thresholds to conceal a failure.

Success requires a successful MATLAB run/report and fresh-session inspection:
517 predictions/labels, probability 6 x 517, fluorescence 3 x 517, images
171 x 171 x 7 x 517, unique channel mapping 1:7, unchanged six raw channels,
and exact sidecar restoration into roi.results through both data-only and
full load. The verifier and report must identify the actual job and core SHA.
Only then mark the campaign verified and pause its heartbeat.
