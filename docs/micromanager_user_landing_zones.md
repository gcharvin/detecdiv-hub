# Micro-Manager User Landing Zones

The hub supports per-user Micro-Manager landing zones under each provisioned
DetecdivHub home:

```text
<user-home>/DetecdivHub/landing
<user-home>/DetecdivHub/raw/YYYYMMDD/<dataset>
```

Examples from the live layout:

```text
/data/Antoine/DetecdivHub/landing
/data/Antoine/DetecdivHub/raw
/homes/Gilles/DetecdivHub/landing
/homes/Gilles/DetecdivHub/raw
```

The important invariant is that `landing` and `raw` are siblings inside the
same user home. The ingest worker can then promote a stable acquisition with a
filesystem `rename()` rather than a copy/delete cycle.

`landing` is now part of the canonical user home directory layout created by
home-storage preparation jobs, together with:

```text
landing/
projects/
raw/
artifacts/
exports/
```

Existing live user homes were backfilled with a `landing` directory on
2026-05-18. For users under `/homes`, the landing and raw directories remain on
the `/homes` mount; for users under `/data`, they remain on the `/data` mount.
This keeps promotion intra-filesystem for each user even though `/homes` and
`/data` are separate CIFS mountpoints on `detecdiv-server`.

## Hub Behavior

`GET /micromanager-ingest/status` now reports:

- the default landing root for the connected user
- all ready user landing roots visible from storage accounts
- the legacy configured landing root
- a preview of stable candidate datasets visible from the API host

`POST /micromanager-ingest/run` accepts `landing_root_key`:

- `user:<user_key_slug>` scans that user's landing zone
- `all_user` scans all ready user landing zones
- `configured` scans the legacy configured root

If `landing_root_override` is provided, it still takes precedence and behaves
as an explicit manual folder scan.

Scheduled worker ingestion has no connected user, so it scans all ready user
landing zones plus the legacy configured root.

## Widget Contract

The microscope widget should stop hard-coding `/data/microscope/landing` for
new acquisitions. Instead, after the operator is identified, it should ask the
hub for the current user's landing root and write the acquisition session there.

For now, the widget can read the default root from:

```http
GET /micromanager-ingest/status
```

Use:

```json
{
  "default_landing_root": {
    "root_key": "user:gilles",
    "path": "/homes/Gilles/DetecdivHub/landing"
  }
}
```

The widget should create one acquisition/session subfolder under that root,
write the dataset and `detecdiv_acquisition_manifest.json`, and leave promotion
from `landing` to `raw/YYYYMMDD` to the hub ingest worker.

The legacy `/data/microscope/landing` root remains supported for old widget
versions and manual recovery.
