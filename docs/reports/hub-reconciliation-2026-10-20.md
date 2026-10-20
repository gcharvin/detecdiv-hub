# Hub repository reconciliation, 20 October 2026

Reviewed from `222dea1ba6842c63df28d4c5ca96f8225b4e59ba` on `master`.
The original backlog was 26 modified tracked files and 489 untracked files,
totalling about 31 MB. Most untracked files were historical observations,
diagnostic scripts and preserved deployment copies, rather than product code.
See the [515-file inventory](hub-reconciliation-2026-10-20.files.json) for
initial/current SHA256 fingerprints and each disposition.

## Existing commits and the apparent backlog

Earlier work was committed: examples include `b4906d9` (Windows GPU validation,
16 October) and `222dea1` (automatic extraction memory, 19 October). The latter
was already on GitHub; GitLab was one fast-forward commit behind. No branch
divergence required rewriting published history.

Twenty-three pending files have contents already present elsewhere in regular
branch history. They are mostly preserved deployment copies. Their inventory
entries name the earlier blob path. Only regular branch/remote history was
counted; internal recovery refs were excluded. These archives do not represent
new implementations of previously committed features.

## Coherent workstreams

| Commit | Scope |
| --- | --- |
| `3366f31` | Repair stale scheduler/indexing/archive test fixtures and portable preview expectations. |
| `fdb21b2` | Preserve real backup path case and distinct case-sensitive files during project deletion. |
| `64776be` | Audited force-stop API, schema migration, isolated Windows/Linux job trees, UI and historical rollout evidence. |
| `844024e` | Validated project resource defaults, legacy processor GPU admission, exact staged NVIDIA runtime selection and recovery/study evidence. |
| `0ce2398` | Unlimited Windows poller task duration, operating guidance and historical recovery/GPU evidence. |
| `9b80f02` | Pinned Qwen parser/readiness settings, preserved comparison outputs and guardrail proposal. |
| `6aa44eb` | English Hub interface messages and asset cache versions. |
| `58c9a16` | Historical storage inventories, monitoring observations, proxy files and explicitly superseded SQL drafts. |

The final documentation commit records this audit and the pending deployment
notes. Historical scripts are retained as evidence and were not replayed.
Frozen diagnostic diffs/logs retain their original whitespace.

Two local raw files are deliberately ignored while their evidence is preserved:

- `reports/alex-memory-recovery-20261020/monitor_state.json` is the mutable
  cursor of a still-active monitor. Its frozen review copy is
  `monitor-state-review-20261020.json` in the same directory, SHA256
  `316efb89eac5dd60d57f00be1f912ebe2c5c9384e0a5fddf15223e688ae0a6c5`.
- The historical `api-intended/pipeline_run_executor.py.conflict` is preserved
  byte-for-byte inside `reports/job-force-stop-deployment-20261020/premerge-snapshot.zip`.
  The member SHA256 is
  `db7aa0b1b38cae2a06ec21aa59088e6bb8f7cd2dc22895ddef185dafa642cd5f`.
  Current executable Python has resolved code and parses successfully.

Both raw originals remain at their original local paths; no source data was
deleted. Concurrent recovery-helper updates were preserved and parsed.

## Validation

- Windows, Python 3.13.9: **381 passed**, zero failures.
- Linux, Python 3.12.3: **379 passed, two skipped**, zero failures.
  The skipped cases exercise the real Windows PowerShell process guard.
- Node: all **three** shipped force-stop UI tests passed; `app.js` and
  `assistant.js` passed syntax checks.
- All changed Python files, including archived source snapshots, parse.
  All 21 pending PowerShell files parse without executing their operations.
- All pending JSON parsed; relative links in the edited documentation resolved.
  The Qwen corpus SHA256 matches the historical comparison manifest:
  `e3fc6d927b24cbb18bf2b936d7405e4bd7b42eadee0be76c2dbc6ea216e276ce`.
- Credential-pattern review found placeholders/test examples rather than live
  credentials in the pending sources. The proxy environment file contains
  listener/target configuration, not a database password or private key.
- Strict whitespace checks passed for active source/docs. Archived patches
  retain historical whitespace rather than altering their recorded contents.

The full Python command on each platform was:

```text
python -m pytest -q tests reports/resource-allocation-study-20261015/test_simulator.py reports/resource-allocation-study-20261015/test_hub_policy_probe.py
node --test tests/test_force_stop_ui.cjs
```

Thirteen cases cover the offline allocation study; no scientific simulation,
model inference, GPU service or Hub job was launched. The Linux suite ran in
`/tmp/detecdiv-hub-review-20261020-7a836ea448`, separate from the deployed checkout.
Pytest was installed only into that directory's test dependency folder.

The first Windows run had eight failures. All eight were reproduced against
an unchanged export of the base commit before fixing the obsolete fixtures.
Linux additionally exposed two nonportable expected paths and a real old bug:
backup paths were returned lowercased instead of preserving actual filenames.
The latter received its own source fix and regression. Windows reports 14
Starlette deprecation warnings in the resource-default error tests.

## Preservation of ongoing work

Read-only before/after Hub snapshots confirmed all seven poller PID/code
fingerprint pairs unchanged. All three active jobs retained their IDs, running
status, start times and parameter hashes. The published MATLAB default stayed
`6ed9c3e62c10a6bab3d1fa361adbfe54f9355c6b`.

This task publishes repository commits. It does not apply a database migration,
restart API/workers, change allocations, cancel jobs or deploy a different
runtime. Existing host adaptations and historical operational evidence remain.
