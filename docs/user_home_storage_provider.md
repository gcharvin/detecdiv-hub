# User Home Storage Provider

## Purpose

This note defines the target direction for moving DetecDiv Hub storage from a
single shared `/data` area toward per-user home storage, while keeping the
architecture generic enough to work outside a Synology NAS ecosystem.

The immediate deployment assumption is:

- `detecdiv-server` has a permanent server-side mount exposing user homes as
  `/homes/<username>`;
- existing legacy data may remain under `/data` for a transition period;
- new users and new data should be able to use `/homes/<username>/DetecDiv`
  before existing data is migrated.

Synology DSM APIs may be used for provisioning and quota enforcement, but the
hub data model must not become Synology-specific.

## Design Goals

- Keep DetecDiv Hub as the storage governance authority.
- Keep storage paths represented as `storage_roots` plus relative paths.
- Support a simple POSIX-only deployment with a configured mount point.
- Support Synology as an optional provider for user provisioning and quotas.
- Avoid storing or handling individual Synology user passwords in the hub.
- Allow `/data` and `/homes` to coexist during development and migration.
- Make migration preview-first, auditable, and reversible where practical.

## Non-Goals

- Do not move DetecDiv MATLAB project internals into the hub.
- Do not make Synology DSM a required runtime dependency.
- Do not require interactive NAS credentials for worker jobs.
- Do not assume Windows client drive letters are canonical storage paths.
- Do not perform destructive cleanup as part of the first migration pass.

## Storage Provider Model

The hub should introduce a provider-level concept above `storage_roots`.

Example provider kinds:

- `posix_mount`
  - A plain mounted filesystem visible to the worker.
  - The hub can create folders and measure usage.
  - Quotas are not enforced by the provider, or are enforced externally.
- `synology_dsm`
  - A mounted filesystem visible to the worker.
  - DSM APIs can create or inspect users, home directories, and quotas.
  - The worker still performs filesystem operations through the mount.
- future kinds such as `smb_share`, `nfs`, or another NAS backend.

The provider abstraction should describe capabilities, not business rules:

- can list or provision storage users
- can read quota
- can set quota
- can report provider health
- can validate that a user home exists

Project ownership, access control, ingestion, indexing, deletion, and migration
remain hub responsibilities.

## Canonical Path Model

For the current `/homes/<username>` mount, the recommended root is:

```text
storage_roots:
  name = user-homes
  root_type = user_home_root
  host_scope = detecdiv-server
  path_prefix = /homes
```

A project location should then be stored as:

```text
project_locations:
  storage_root = user-homes
  relative_path = <username>/DetecDiv/projects/<project-folder>
```

A raw dataset location should be stored as:

```text
raw_dataset_locations:
  storage_root = user-homes
  relative_path = <username>/DetecDiv/raw/<dataset-folder>
```

This avoids storing `/homes/florian/...` repeatedly as an absolute canonical
path. If the mount changes later, only the storage root needs to change.

Legacy data can continue to use a separate root:

```text
storage_roots:
  name = legacy-data
  root_type = legacy_root
  host_scope = detecdiv-server
  path_prefix = /data
```

## User Storage Accounts

The existing `users` table should remain the hub identity table. A separate
storage mapping should link a hub user to the storage provider account.

Proposed table:

```text
user_storage_accounts
  id
  user_id
  provider_key
  provider_user_key
  home_storage_root_id
  home_relative_path
  quota_bytes
  quota_status
  provisioning_status
  last_synced_at
  metadata_json
  created_at
  updated_at
```

Example:

```text
user_id = <hub user id>
provider_key = synology-main
provider_user_key = florian
home_storage_root_id = user-homes
home_relative_path = florian/DetecDiv
quota_bytes = 2000000000000
```

`users.default_path` can remain a convenience field for UI or legacy display,
but it should not be the durable source of truth for storage placement.

## Password And Authentication Policy

The hub should not ask for or store individual Synology user passwords for
normal storage access.

The intended model is:

- users authenticate to DetecDiv Hub through the hub authentication layer;
- `detecdiv-server` keeps a permanent mount of the storage namespace;
- worker jobs access files through `/homes/<username>/...`;
- DSM credentials used by the hub, if any, are service credentials stored in
  deployment secrets;
- DSM user passwords remain outside the hub.

This is required because worker jobs, indexing, preview generation, migrations,
and MATLAB batch execution must run non-interactively.

Synology still remains useful as the enforcement layer for quotas and storage
accounts, but it should not become the hub's per-request authentication layer.

## Quota Model

Use two complementary quota views:

- provider-enforced quota
  - Synology or another backend applies the hard limit;
  - the hub can read and update this value when the provider supports it.
- hub-measured usage
  - the hub computes project, raw dataset, artifact, and total owned bytes;
  - this remains useful even when the provider does not expose quotas.

Proposed audit/snapshot table:

```text
storage_quota_snapshots
  id
  user_id
  provider_key
  provider_user_key
  quota_bytes
  provider_used_bytes
  hub_project_bytes
  hub_raw_bytes
  hub_artifact_bytes
  measured_at
  metadata_json
```

The UI should clearly distinguish "NAS quota" from "hub-indexed usage", because
they will not always match during migration or when unindexed files exist.

## API And Worker Responsibilities

FastAPI should expose administrative and status endpoints:

- list storage providers
- list user storage mappings
- provision or sync a user's storage account
- read quota and usage status
- update desired quota
- preview migration from legacy roots to user homes
- queue migration jobs

Provider-changing and filesystem-changing operations should be job-backed:

- create or validate user home folders
- move or copy legacy data
- update preferred project/raw locations
- rescan sizes after migration
- optionally schedule cleanup of old locations

The worker on `detecdiv-server` should perform physical filesystem operations,
because that host has direct storage visibility.

The first worker-backed operation is:

```text
job_kind = prepare_user_home_storage
```

It is queued through `POST /storage/user-accounts/{account_id}/prepare` and
creates or validates the mounted home layout:

```text
<storage_root>/<home_relative_path>/
  projects/
  raw/
  artifacts/
  exports/
```

This keeps the FastAPI VM out of direct filesystem mutation while still giving
admins a concrete way to prepare new users on the storage-visible worker host.

## Synology Adapter

For Synology deployments, implement a provider adapter that can:

- authenticate to DSM using service credentials;
- discover available DSM APIs at startup or sync time;
- map hub users to DSM users;
- verify that user homes are enabled and reachable;
- read quota and usage information when available;
- set quota for a provider user when configured;
- record errors in provisioning events.

The DSM API integration should be isolated in a small service module, for
example:

```text
api/services/storage_providers/base.py
api/services/storage_providers/posix_mount.py
api/services/storage_providers/synology_dsm.py
```

Callers should depend on provider capabilities rather than Synology method
names.

The first DSM adapter layer is intentionally limited to login, API discovery,
and admin-only probing:

```text
POST /storage/providers/{provider_key}/synology/discover
POST /storage/providers/{provider_key}/synology/login-check
POST /storage/providers/{provider_key}/synology/probe
GET  /storage/providers/{provider_key}/synology/users
GET  /storage/providers/{provider_key}/synology/user-home
GET  /storage/user-accounts/{account_id}/synology/quota
```

The required deployment settings are:

```text
DETECDIV_HUB_SYNOLOGY_DSM_BASE_URL=https://nas.example.org:5001
DETECDIV_HUB_SYNOLOGY_DSM_ACCOUNT=<service-account>
DETECDIV_HUB_SYNOLOGY_DSM_PASSWORD=<secret>
DETECDIV_HUB_SYNOLOGY_DSM_SESSION=DetecDivHub
DETECDIV_HUB_SYNOLOGY_DSM_VERIFY_TLS=true
```

The probe endpoint exists to validate the exact DSM API surface on the live NAS
before hardcoding quota or user-management calls. The DSM login guide documents
`SYNO.API.Info` discovery and `SYNO.API.Auth` login, but DSM core APIs such as
quota and user home management vary enough across versions that the hub should
discover capabilities first and only then promote confirmed methods into stable
service functions.

On the current NAS, the server is visible in DSM as `SRV-DATA-GS01` at
`10.20.11.250:5000`, but that short name does not currently resolve from the
Windows development machine. Using `https://10.20.11.250:5001` works with
`DETECDIV_HUB_SYNOLOGY_DSM_VERIFY_TLS=false`; with TLS verification enabled,
the certificate is rejected because it is not valid for the IP address. A DNS
name matching the DSM certificate should be preferred before production use.

The current NAS accepts:

```text
SYNO.Core.User.list
SYNO.Core.User.Home.get
SYNO.Core.Quota.get with name=<provider_user_key>
```

For a user without an explicit quota, `SYNO.Core.Quota.get` may return an empty
`user_quota` list. That is treated as a successful read with unknown quota bytes,
not as a transport or authentication failure.

On DSM 7.2, the user edit screen can show a user quota for the `homes` shared
folder while `SYNO.Core.Quota.get name=<provider_user_key>` still returns an
empty `user_quota` list. In that case the hub response keeps the provider quota
as `null`, marks `provider_reported=false`, and exposes the cataloged desired
quota as `effective_quota_bytes` with `quota_source=hub_desired`. This prevents
callers from mistaking "DSM did not report a quota entry" for "no quota should
exist".

## Provisioning Flow For New Users

Initial rollout should target new users first:

1. Create or update the hub `users` row.
2. Create or sync `user_storage_accounts`.
3. If provider kind is `synology_dsm`, verify or create the DSM user according
   to the lab policy.
4. Set or verify quota if the provider supports it.
5. Ensure `/homes/<username>/DetecDiv` and expected subdirectories exist.
6. Set new project/raw default placement to the user home root.

Suggested user home layout:

```text
/homes/<username>/DetecDiv/
  projects/
  raw/
  artifacts/
  exports/
```

## Migration Strategy

Migration should be delayed until the new-user path is stable.

During the transition:

- existing projects and raw datasets can remain under `/data`;
- new data can land under `/homes/<username>/DetecDiv`;
- both roots can be indexed and browsed by the hub;
- UI filters should expose storage root and owner to make mixed state visible.

Migration should be per owner, per project, or per batch:

1. Build a migration preview.
2. Resolve owner and destination.
3. Estimate bytes and compare with quota.
4. Detect ambiguous ownership or shared raw data.
5. Queue a worker job.
6. Move or copy data.
7. Verify result.
8. Add new location rows and mark them preferred.
9. Keep old locations as non-preferred/read-only until cleanup.
10. Clean up old data only through a separate audited deletion workflow.

## Move Versus Copy

Even if `/data` and `/homes` are on the same physical NAS, the migration code
must not assume that a move is metadata-only.

If source and destination are on the same filesystem or compatible subvolume,
a rename can be fast and atomic. If they are different mounts, shared folders,
subvolumes, or quota domains, a move may degrade to copy plus delete.

The worker should therefore:

- detect whether an atomic rename is possible;
- attempt rename only when safe;
- fall back to copy plus verification when required;
- avoid deleting the source until verification succeeds;
- record which strategy was used.

Operational checks before implementation:

```bash
df -T /data /homes/<username>
stat -c '%d %m %n' /data /homes/<username>
```

These checks inform the migration strategy but should not replace defensive
runtime handling.

## Rollout Plan

1. Add provider and user storage account schema.
2. Add a POSIX mount provider using `/homes` without Synology-specific logic.
3. Add admin UI/API for mapping hub users to storage homes.
4. Route new users and new data into `/homes/<username>/DetecDiv`.
5. Add quota snapshots based on hub-measured usage.
6. Add Synology DSM adapter for quota and account synchronization.
7. Add migration preview from `/data` to `/homes`.
8. Add worker-backed migration execution.
9. Add cleanup workflows for legacy `/data` locations.

This order keeps the deployment usable if DSM API details change or are harder
than expected.

## Open Questions

- What is the exact DSM API surface available on the current NAS version?
- Should DSM users be created by the hub, or only linked to pre-existing NAS
  accounts?
- What quota defaults should apply by role, group, or lab status?
- Should raw acquisition data live in each user's home, or should some
  microscope landing zones remain shared and be reassigned after ingest?
- How should shared projects count against quota when ownership and ACL differ?
- What grace period should old `/data` locations keep before cleanup?
