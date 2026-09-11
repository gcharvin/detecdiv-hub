from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Callable

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import Session

from api.models import LabguruYeastStrain
from api.services.external_eln_clients import LabguruClient, LabguruInventoryItem, html_to_text


SEARCH_SCOPES = ("name", "identity", "genetics", "description", "people", "metadata")
_GENETICS_TERMS = (
    "allele",
    "background",
    "gene",
    "genotype",
    "mating",
    "mutation",
    "parent",
    "phenotype",
    "ploidy",
    "strain",
)
_IDENTITY_TERMS = ("barcode", "identifier", "sys_id", "sysid", "system_id")
_PEOPLE_TERMS = ("author", "created_by", "creator", "member", "owner", "produced_by")
_DESCRIPTION_TERMS = ("comment", "description", "note", "remark")


def sync_labguru_yeast_strains(
    session: Session,
    *,
    client: LabguruClient,
    collection_name: str,
    since: datetime | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    synced_at = datetime.now(timezone.utc)
    sync_mode = "incremental" if since is not None else "full"
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "connecting",
                "sync_mode": sync_mode,
                "processed_count": 0,
                "total_count": None,
                "created_count": 0,
                "updated_count": 0,
            }
        )
    items = client.list_yeast_strains(
        collection_name=collection_name,
        since=since,
        progress_callback=progress_callback,
    )
    existing = {
        record.external_id: record
        for record in session.scalars(select(LabguruYeastStrain)).all()
    }
    seen_ids: set[str] = set()
    created_count = 0
    updated_count = 0

    unchanged_count = 0
    for index, item in enumerate(items, start=1):
        if item.external_id in seen_ids:
            continue
        seen_ids.add(item.external_id)
        record = existing.get(item.external_id)
        if record is None:
            record = LabguruYeastStrain(external_id=item.external_id, name=item.name)
            session.add(record)
            existing[item.external_id] = record
            created_count += 1
        elif inventory_item_needs_sync(record, item=item, since=since):
            updated_count += 1
        else:
            unchanged_count += 1
            if progress_callback is not None and (index % 50 == 0 or index == len(items)):
                progress_callback(
                    sync_progress_payload(
                        phase="synchronizing",
                        sync_mode=sync_mode,
                        processed_count=index,
                        total_count=len(items),
                        created_count=created_count,
                        updated_count=updated_count,
                    )
                )
            continue
        apply_inventory_item(record, item=item, synced_at=synced_at)
        if progress_callback is not None and (index % 50 == 0 or index == len(items)):
            progress_callback(
                sync_progress_payload(
                    phase="synchronizing",
                    sync_mode=sync_mode,
                    processed_count=index,
                    total_count=len(items),
                    created_count=created_count,
                    updated_count=updated_count,
                )
            )

    deactivated_count = 0
    if sync_mode == "full":
        for external_id, record in existing.items():
            if external_id in seen_ids or not record.is_active:
                continue
            record.is_active = False
            record.missing_since = synced_at
            record.updated_at = synced_at
            deactivated_count += 1

    session.flush()
    return {
        "system_key": "labguru",
        "collection_name": collection_name,
        "sync_mode": sync_mode,
        "imported_count": len(seen_ids),
        "processed_count": len(seen_ids),
        "total_count": len(seen_ids),
        "created_count": created_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "deactivated_count": deactivated_count,
        "phase": "completed",
        "progress_percent": 100,
        "synced_at": synced_at.isoformat(),
    }


def inventory_item_needs_sync(
    record: LabguruYeastStrain,
    *,
    item: LabguruInventoryItem,
    since: datetime | None,
) -> bool:
    if not record.is_active:
        return True
    if record.created_external_at is None and item.created_external_at is not None:
        return True
    if (record.payload_json or {}) != (item.payload_json or {}):
        return True
    if since is None:
        return True
    comparison_time = ensure_aware(item.updated_external_at or item.created_external_at)
    return comparison_time is not None and comparison_time >= ensure_aware(since)


def sync_progress_payload(
    *,
    phase: str,
    sync_mode: str,
    processed_count: int,
    total_count: int | None,
    created_count: int,
    updated_count: int,
) -> dict[str, Any]:
    progress_percent = None
    if total_count:
        progress_percent = min(100, round(processed_count * 100 / total_count))
    return {
        "phase": phase,
        "sync_mode": sync_mode,
        "processed_count": processed_count,
        "total_count": total_count,
        "created_count": created_count,
        "updated_count": updated_count,
        "progress_percent": progress_percent,
    }


def apply_inventory_item(
    record: LabguruYeastStrain,
    *,
    item: LabguruInventoryItem,
    synced_at: datetime,
) -> None:
    search_fields = build_search_fields(item.payload_json, item=item)
    record.name = item.name
    record.sys_id = item.sys_id
    record.description = item.description
    record.owner_name = item.owner_name
    record.external_url = item.external_url
    record.search_fields_json = search_fields
    record.payload_json = item.payload_json
    record.search_text = normalize_search_text(" ".join(search_fields.values()))
    record.is_active = True
    record.created_external_at = item.created_external_at
    record.updated_external_at = item.updated_external_at
    record.last_synced_at = synced_at
    record.missing_since = None
    record.updated_at = synced_at


def build_search_fields(payload: dict[str, Any], *, item: LabguruInventoryItem) -> dict[str, str]:
    grouped: dict[str, list[str]] = {scope: [] for scope in SEARCH_SCOPES}
    add_unique(grouped["name"], item.name)
    add_unique(grouped["identity"], item.external_id)
    add_unique(grouped["identity"], item.sys_id)
    add_unique(grouped["description"], item.description)
    add_unique(grouped["people"], item.owner_name)

    for label, value in flatten_labguru_fields(payload):
        scope = search_scope_for_label(label)
        add_unique(grouped[scope], value)
    return {scope: normalize_search_text(" | ".join(values)) for scope, values in grouped.items()}


def flatten_labguru_fields(value: Any, *, prefix: str = "") -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    if isinstance(value, dict):
        label = first_text(value, "name", "label", "field_name", "title")
        field_value = first_scalar(value, "value", "text", "content", "data")
        handled_pair = bool(label and field_value is not None and len(value) <= 12)
        if handled_pair:
            clean_value = clean_field_value(field_value)
            if clean_value:
                fields.append((label, clean_value))
        for key, child in value.items():
            if key in {"token", "api_token"}:
                continue
            if handled_pair and key in {"name", "label", "field_name", "title", "value", "text", "content", "data"}:
                continue
            child_prefix = humanize_field_name(key) if not prefix else f"{prefix} · {humanize_field_name(key)}"
            fields.extend(flatten_labguru_fields(child, prefix=child_prefix))
        return deduplicate_fields(fields)
    if isinstance(value, list):
        for child in value[:500]:
            fields.extend(flatten_labguru_fields(child, prefix=prefix))
        return deduplicate_fields(fields)
    clean_value = clean_field_value(value)
    if prefix and clean_value:
        fields.append((prefix, clean_value))
    return fields


def search_scope_for_label(label: str) -> str:
    normalized = normalize_search_text(label).replace(" ", "_")
    if normalized in {"name", "title"} or normalized.endswith("_name"):
        return "name"
    if any(term in normalized for term in _IDENTITY_TERMS) or normalized == "id":
        return "identity"
    if any(term in normalized for term in _GENETICS_TERMS):
        return "genetics"
    if any(term in normalized for term in _DESCRIPTION_TERMS):
        return "description"
    if any(term in normalized for term in _PEOPLE_TERMS):
        return "people"
    return "metadata"


def search_labguru_yeast_strains(
    session: Session,
    *,
    query: str | None,
    scopes: list[str] | None,
    match: str,
    limit: int,
    offset: int,
) -> tuple[int, list[LabguruYeastStrain]]:
    selected_scopes = normalize_scopes(scopes)
    tokens = search_tokens(query or "")
    match_mode = "any" if str(match or "").lower() == "any" else "all"
    conditions = []
    for token in tokens:
        scope_conditions = [
            func.coalesce(LabguruYeastStrain.search_fields_json[scope].as_string(), "").ilike(f"%{token}%")
            for scope in selected_scopes
        ]
        conditions.append(or_(*scope_conditions))

    where_clauses = [LabguruYeastStrain.is_active.is_(True)]
    if conditions:
        where_clauses.append(or_(*conditions) if match_mode == "any" else and_(*conditions))

    count = int(
        session.scalar(select(func.count(LabguruYeastStrain.id)).where(*where_clauses)) or 0
    )
    stmt = select(LabguruYeastStrain).where(*where_clauses)
    creation_order = func.coalesce(
        LabguruYeastStrain.created_external_at,
        LabguruYeastStrain.updated_external_at,
        LabguruYeastStrain.created_at,
    )
    if tokens:
        normalized_query = " ".join(tokens)
        relevance = case(
            (func.lower(LabguruYeastStrain.name) == normalized_query, 100),
            (func.lower(func.coalesce(LabguruYeastStrain.sys_id, "")) == normalized_query, 90),
            (LabguruYeastStrain.name.ilike(f"{normalized_query}%"), 70),
            (LabguruYeastStrain.name.ilike(f"%{normalized_query}%"), 50),
            else_=0,
        )
        stmt = stmt.order_by(
            relevance.desc(),
            creation_order.desc(),
            LabguruYeastStrain.name.asc(),
        )
    else:
        stmt = stmt.order_by(creation_order.desc(), LabguruYeastStrain.name.asc())
    stmt = stmt.offset(max(0, offset)).limit(min(max(limit, 1), 200))
    return count, list(session.scalars(stmt))


def yeast_strain_search_result(
    record: LabguruYeastStrain,
    *,
    query: str | None,
    include_payload: bool = False,
) -> dict[str, Any]:
    tokens = search_tokens(query or "")
    flattened = flatten_labguru_fields(record.payload_json or {})
    contexts = []
    preferred = []
    for label, value in flattened:
        normalized_value = normalize_search_text(value)
        context = {"label": label, "value": truncate(value, 360)}
        if tokens and any(token in normalized_value for token in tokens):
            contexts.append(context)
        elif search_scope_for_label(label) in {"genetics", "description"}:
            preferred.append(context)
    if not contexts:
        contexts = preferred
    return {
        "id": record.id,
        "external_id": record.external_id,
        "name": record.name,
        "sys_id": record.sys_id,
        "description": record.description,
        "owner_name": record.owner_name,
        "external_url": record.external_url,
        "search_fields_json": record.search_fields_json or {},
        "context": contexts[:6],
        "payload_json": (record.payload_json or {}) if include_payload else {},
        "created_external_at": record.created_external_at
        or external_created_at_from_payload(record.payload_json or {})
        or record.created_at,
        "updated_external_at": record.updated_external_at,
        "last_synced_at": record.last_synced_at,
    }


def external_created_at_from_payload(payload: dict[str, Any]) -> datetime | None:
    value = payload.get("created_at") or payload.get("created_on") or payload.get("creation_date")
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def ensure_aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def normalize_scopes(scopes: list[str] | None) -> list[str]:
    requested = [str(scope or "").strip().lower() for scope in (scopes or [])]
    selected = [scope for scope in SEARCH_SCOPES if scope in requested]
    return selected or list(SEARCH_SCOPES)


def search_tokens(value: str) -> list[str]:
    normalized = normalize_search_text(value)
    return list(dict.fromkeys(re.findall(r"[a-z0-9]+", normalized)))[:12]


def normalize_search_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.casefold().split())


def clean_field_value(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return "" if value is None else str(value).lower()
    if isinstance(value, (dict, list)):
        return ""
    return truncate(html_to_text(value), 2_000)


def humanize_field_name(value: str) -> str:
    text = re.sub(r"[_\-]+", " ", str(value or "")).strip()
    return text[:1].upper() + text[1:] if text else "Field"


def first_text(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def first_scalar(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None and not isinstance(value, (dict, list)):
            return value
    return None


def add_unique(values: list[str], value: Any) -> None:
    clean = clean_field_value(value)
    if clean and clean not in values:
        values.append(clean)


def deduplicate_fields(fields: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    result = []
    for field in fields:
        if field in seen:
            continue
        seen.add(field)
        result.append(field)
    return result


def truncate(value: str, length: int) -> str:
    text = str(value or "").strip()
    return text if len(text) <= length else text[: length - 1].rstrip() + "…"
