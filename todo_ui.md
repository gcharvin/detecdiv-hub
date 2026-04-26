# UI TODO

This file tracks future UI improvements for `detecdiv-hub`.

## Worker Administration

- Make active worker instances the primary operational control in the UI.
  The practical concurrency limit is currently the number of active
  `detecdiv-worker@N` services. The UI should therefore emphasize active workers
  and desired workers before exposing lower-level capacity fields.

- Treat `max_concurrent_jobs` as an advanced guardrail, not the main worker
  scaling control.
  The effective concurrency is `min(active_workers, max_concurrent_jobs)`, but
  for normal operation this distinction is mostly confusing. Show it only in an
  advanced section or as explanatory text.

- Improve multi-worker health display.
  The UI should show each worker instance separately, for example `@1`, `@2`,
  `@3`, with state, last heartbeat, current job, and last error.

- Add a clear mismatch warning.
  Warn when `worker_instances_desired`, active systemd workers, registered
  heartbeats, and `max_concurrent_jobs` disagree.

- Add a safe worker scaling action.
  Provide UI actions for increasing/decreasing the desired number of workers,
  while making it explicit that this affects systemd services on the compute
  host, not the VM API container.

## Deployment Awareness

- Surface the current deployment target in the admin UI.
  Show that the primary API/DB runs on `webserver-labo` and compute workers run
  on `detecdiv-server`.

- Add a health panel for the split deployment.
  Include API health, PostgreSQL status, execution target status, worker
  heartbeat freshness, and last job claim time.

## Raw Dataset Positions And Movie Preview

- Fix the MP4 preview viewer regression after the migration to `webserver-labo`.
  Preview videos that were available before migration should still be playable
  from the VM-hosted UI. Check whether the breakage comes from URL generation,
  reverse proxy paths, static/file serving, MIME type, or the VM not seeing
  storage paths directly.

- Redesign the positions display to avoid excessive horizontal and vertical
  scrolling.
  The positions table should be much narrower, with only the operational columns
  needed to select a position and see its status.

- Give the positions table its own scroll container.
  Scrolling through positions should not move the whole page or hide the movie
  preview.

- Display the selected position movie to the right of the table.
  The desired layout is a two-pane view: compact positions list on the left,
  MP4/movie preview panel on the right.

- Keep the selected position visible and visually obvious.
  Selection state should remain clear while the preview is playing, without
  requiring the user to scroll back to understand which position is displayed.
