from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, timezone
from typing import Any, Callable

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import Session, selectinload

from api.models import (
    LabguruStorageBox,
    LabguruStorageLocation,
    LabguruYeastStock,
    LabguruYeastStrain,
)
from api.services.external_eln_clients import (
    LabguruClient,
    LabguruInventoryItem,
    html_to_text,
    normalize_external_url,
    parse_datetime,
)


SEARCH_SCOPES = ("name", "identity", "genetics", "description", "people", "storage", "metadata")
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
_HIDDEN_CONTEXT_LABELS = {
    "api url",
    "external uuid",
    "export path",
    "id",
    "labguru id",
    "member id",
    "member url",
    "owner id",
    "owner url",
    "url",
    "user",
    "uuid",
}
_BIOLOGY_FIELD_ALIASES = {
    "genotype": ("genotype", "transgenic features"),
    "auxotrophies": ("auxotrophies", "auxotrophy", "auxotrophic markers"),
    "mating_type": ("mating type", "reproduction"),
    "background": ("background", "genetic background"),
    "source": ("source",),
}


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
        progress_callback=progress_callback,
    )
    stock_payloads = client.list_stocks(progress_callback=progress_callback)
    storage_payloads = client.list_storage_locations(progress_callback=progress_callback)
    box_payloads = client.list_storage_boxes(progress_callback=progress_callback)
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
    storage_locations = sync_storage_locations(
        session,
        payloads=storage_payloads,
        synced_at=synced_at,
        base_url=client.base_url,
    )
    storage_boxes = sync_storage_boxes(
        session,
        payloads=box_payloads,
        synced_at=synced_at,
        base_url=client.base_url,
    )
    stock_result = sync_yeast_stocks(
        session,
        payloads=stock_payloads,
        strains_by_external_id={external_id: existing[external_id] for external_id in seen_ids},
        locations_by_external_id=storage_locations,
        boxes_by_external_id=storage_boxes,
        synced_at=synced_at,
        base_url=client.base_url,
        progress_callback=progress_callback,
        sync_mode=sync_mode,
        collection_name=collection_name,
    )
    session.flush()
    refresh_storage_search_fields(
        existing.values(),
        stocks=session.scalars(select(LabguruYeastStock)).all(),
    )
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
        "stock_count": stock_result["stock_count"],
        "stock_created_count": stock_result["created_count"],
        "stock_updated_count": stock_result["updated_count"],
        "stock_deactivated_count": stock_result["deactivated_count"],
        "storage_location_count": len(storage_locations),
        "storage_box_count": len(storage_boxes),
        "phase": "completed",
        "progress_percent": 100,
        "synced_at": synced_at.isoformat(),
    }


def sync_storage_locations(
    session: Session,
    *,
    payloads: list[dict[str, Any]],
    synced_at: datetime,
    base_url: str,
) -> dict[str, LabguruStorageLocation]:
    existing = {
        record.external_id: record
        for record in session.scalars(select(LabguruStorageLocation)).all()
    }
    seen_ids: set[str] = set()
    for payload in payloads:
        external_id = payload_external_id(payload)
        if not external_id:
            continue
        seen_ids.add(external_id)
        record = existing.get(external_id)
        if record is None:
            record = LabguruStorageLocation(external_id=external_id, name=payload_name(payload, external_id))
            session.add(record)
            existing[external_id] = record
        storage_type = payload.get("storage_type")
        record.parent_external_id = optional_text(payload.get("parent_id"))
        record.name = payload_name(payload, external_id)
        record.location_type = nested_name(storage_type)
        record.external_url = normalize_external_url(
            optional_text(payload.get("url")) or f"/storage/storages/{external_id}",
            base_url=base_url,
        )
        record.name_with_hierarchy = optional_text(payload.get("name_with_hierarchy"))
        record.payload_json = payload
        record.is_active = payload.get("deleted_at") in (None, "") and not bool(payload.get("archived"))
        record.last_synced_at = synced_at
        record.missing_since = None
        record.updated_at = synced_at
    deactivate_missing_records(existing, seen_ids=seen_ids, synced_at=synced_at)
    session.flush()
    return existing


def sync_storage_boxes(
    session: Session,
    *,
    payloads: list[dict[str, Any]],
    synced_at: datetime,
    base_url: str,
) -> dict[str, LabguruStorageBox]:
    existing = {
        record.external_id: record
        for record in session.scalars(select(LabguruStorageBox)).all()
    }
    seen_ids: set[str] = set()
    for payload in payloads:
        external_id = payload_external_id(payload)
        if not external_id:
            continue
        seen_ids.add(external_id)
        record = existing.get(external_id)
        if record is None:
            record = LabguruStorageBox(external_id=external_id, name=payload_name(payload, external_id))
            session.add(record)
            existing[external_id] = record
        record.storage_external_id = optional_text(payload.get("storage_id"))
        record.name = payload_name(payload, external_id)
        record.external_url = normalize_external_url(
            optional_text(payload.get("url")) or f"/storage/boxes/{external_id}",
            base_url=base_url,
        )
        record.rows = optional_int(payload.get("rows"))
        record.cols = optional_int(payload.get("cols"))
        record.payload_json = payload
        record.is_active = payload.get("deleted_at") in (None, "")
        record.last_synced_at = synced_at
        record.missing_since = None
        record.updated_at = synced_at
    deactivate_missing_records(existing, seen_ids=seen_ids, synced_at=synced_at)
    session.flush()
    return existing


def sync_yeast_stocks(
    session: Session,
    *,
    payloads: list[dict[str, Any]],
    strains_by_external_id: dict[str, LabguruYeastStrain],
    locations_by_external_id: dict[str, LabguruStorageLocation],
    boxes_by_external_id: dict[str, LabguruStorageBox],
    synced_at: datetime,
    base_url: str,
    progress_callback: Callable[[dict[str, Any]], None] | None,
    sync_mode: str,
    collection_name: str,
) -> dict[str, int]:
    existing = {
        record.external_id: record
        for record in session.scalars(select(LabguruYeastStock)).all()
    }
    yeast_payloads = [
        payload
        for payload in payloads
        if optional_text(payload.get("stockable_id") or payload.get("sample_id")) in strains_by_external_id
        and stock_matches_collection(payload, collection_name=collection_name)
    ]
    seen_ids: set[str] = set()
    created_count = 0
    updated_count = 0
    for index, payload in enumerate(yeast_payloads, start=1):
        external_id = payload_external_id(payload)
        strain_external_id = optional_text(payload.get("stockable_id") or payload.get("sample_id"))
        strain = strains_by_external_id.get(strain_external_id or "")
        if not external_id or strain is None:
            continue
        seen_ids.add(external_id)
        record = existing.get(external_id)
        if record is None:
            record = LabguruYeastStock(
                external_id=external_id,
                yeast_strain_id=strain.id,
                name=payload_name(payload, external_id),
            )
            session.add(record)
            existing[external_id] = record
            created_count += 1
        elif record.payload_json != payload or not record.is_active:
            updated_count += 1
        apply_stock_payload(
            record,
            payload=payload,
            strain=strain,
            locations_by_external_id=locations_by_external_id,
            boxes_by_external_id=boxes_by_external_id,
            synced_at=synced_at,
            base_url=base_url,
        )
        if progress_callback is not None and (index % 50 == 0 or index == len(yeast_payloads)):
            progress_callback(
                {
                    **sync_progress_payload(
                        phase="synchronizing_stocks",
                        sync_mode=sync_mode,
                        processed_count=index,
                        total_count=len(yeast_payloads),
                        created_count=created_count,
                        updated_count=updated_count,
                    ),
                    "stock_count": len(yeast_payloads),
                }
            )
    deactivated_count = deactivate_missing_records(existing, seen_ids=seen_ids, synced_at=synced_at)
    return {
        "stock_count": len(seen_ids),
        "created_count": created_count,
        "updated_count": updated_count,
        "deactivated_count": deactivated_count,
    }


def apply_stock_payload(
    record: LabguruYeastStock,
    *,
    payload: dict[str, Any],
    strain: LabguruYeastStrain,
    locations_by_external_id: dict[str, LabguruStorageLocation],
    boxes_by_external_id: dict[str, LabguruStorageBox],
    synced_at: datetime,
    base_url: str,
) -> None:
    box_payload = payload.get("box") if isinstance(payload.get("box"), dict) else {}
    storage_type = optional_text(payload.get("storage_type")) or ""
    is_box_storage = storage_type.endswith("::Box") or bool(box_payload)
    box_external_id = (
        optional_text(payload.get("storage_id") or box_payload.get("id"))
        if is_box_storage
        else None
    )
    box = boxes_by_external_id.get(box_external_id or "")
    box_name = optional_text(box_payload.get("name")) or (box.name if box is not None else None)
    box_url = optional_text(box_payload.get("url")) or (box.external_url if box is not None else None)
    stored_by = payload.get("stored_by")
    owner = payload.get("owner")
    record.yeast_strain = strain
    record.name = payload_name(payload, record.external_id)
    record.container_type = optional_text(payload.get("container_type"))
    record.box_external_id = box_external_id
    record.box_name = box_name
    record.box_url = normalize_external_url(box_url, base_url=base_url) if box_url else None
    record.position = optional_text(payload.get("position")) or optional_text(box_payload.get("location_in_box"))
    record.owner_name = nested_name(owner)
    record.stored_by_name = nested_name(stored_by)
    record.stored_on = parse_date(payload.get("stored_on"))
    record.external_url = normalize_external_url(
        optional_text(payload.get("url")) or f"/storage/stocks/{record.external_id}",
        base_url=base_url,
    )
    if box is not None:
        record.storage_path_json = storage_path_for_box(
            box,
            locations_by_external_id=locations_by_external_id,
        )
    else:
        record.storage_path_json = storage_path_for_location(
            optional_text(payload.get("storage_id")),
            locations_by_external_id=locations_by_external_id,
        )
    record.payload_json = payload
    record.is_active = payload.get("deleted_at") in (None, "") and payload.get("archived_at") in (None, "")
    record.updated_external_at = parse_datetime(payload.get("updated_at"))
    record.last_synced_at = synced_at
    record.missing_since = None
    record.updated_at = synced_at


def storage_path_for_box(
    box: LabguruStorageBox | None,
    *,
    locations_by_external_id: dict[str, LabguruStorageLocation],
) -> list[dict[str, str | None]]:
    if box is None:
        return []
    path = storage_path_for_location(
        box.storage_external_id,
        locations_by_external_id=locations_by_external_id,
    )
    path.append({"name": box.name, "type": "Box", "url": box.external_url})
    return path


def storage_path_for_location(
    location_external_id: str | None,
    *,
    locations_by_external_id: dict[str, LabguruStorageLocation],
) -> list[dict[str, str | None]]:
    path: list[dict[str, str | None]] = []
    seen: set[str] = set()
    while location_external_id and location_external_id not in seen:
        seen.add(location_external_id)
        location = locations_by_external_id.get(location_external_id)
        if location is None:
            break
        path.append(
            {
                "name": location.name,
                "type": location.location_type,
                "url": location.external_url,
            }
        )
        location_external_id = location.parent_external_id
    path.reverse()
    return path


def refresh_storage_search_fields(records: Any, *, stocks: list[LabguruYeastStock]) -> None:
    stocks_by_strain_id: dict[Any, list[LabguruYeastStock]] = {}
    for stock in stocks:
        stocks_by_strain_id.setdefault(stock.yeast_strain_id, []).append(stock)
    for record in records:
        storage_values: list[str] = []
        for stock in stocks_by_strain_id.get(record.id, []):
            if not stock.is_active:
                continue
            add_unique(storage_values, stock.name)
            add_unique(storage_values, stock.container_type)
            add_unique(storage_values, stock.box_name)
            add_unique(storage_values, stock.position)
            add_unique(storage_values, stock.owner_name)
            add_unique(storage_values, stock.stored_by_name)
            for segment in stock.storage_path_json or []:
                if isinstance(segment, dict):
                    add_unique(storage_values, segment.get("name"))
                    add_unique(storage_values, segment.get("type"))
        search_fields = dict(record.search_fields_json or {})
        search_fields["storage"] = normalize_search_text(" | ".join(storage_values))
        record.search_fields_json = search_fields
        record.search_text = normalize_search_text(" ".join(search_fields.values()))


def deactivate_missing_records(records: dict[str, Any], *, seen_ids: set[str], synced_at: datetime) -> int:
    deactivated_count = 0
    for external_id, record in records.items():
        if external_id in seen_ids or not record.is_active:
            continue
        record.is_active = False
        record.missing_since = synced_at
        record.updated_at = synced_at
        deactivated_count += 1
    return deactivated_count


def payload_external_id(payload: dict[str, Any]) -> str:
    return optional_text(payload.get("id") or payload.get("external_id")) or ""


def stock_matches_collection(payload: dict[str, Any], *, collection_name: str) -> bool:
    displayed_type = optional_text(payload.get("content_type_for_display"))
    if not displayed_type:
        return True
    return normalize_search_text(displayed_type).replace(" ", "") == normalize_search_text(
        collection_name
    ).replace(" ", "")


def payload_name(payload: dict[str, Any], fallback: str) -> str:
    return optional_text(payload.get("name") or payload.get("title")) or fallback


def nested_name(value: Any) -> str | None:
    if isinstance(value, dict):
        return optional_text(value.get("name") or value.get("title"))
    return optional_text(value)


def optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_date(value: Any) -> date | None:
    text = optional_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


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
    stmt = select(LabguruYeastStrain).options(selectinload(LabguruYeastStrain.stocks)).where(*where_clauses)
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
    biology = yeast_biology_fields(record.payload_json or {}, flattened=flattened)
    contexts = []
    preferred = []
    for label, value in flattened:
        if hidden_technical_context(label):
            continue
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
        **biology,
        "stocks": [
            yeast_stock_summary(stock)
            for stock in sorted(
                (stock for stock in record.stocks if stock.is_active),
                key=lambda stock: ((stock.box_name or "").casefold(), stock.position or "", stock.name.casefold()),
            )
        ],
        "search_fields_json": record.search_fields_json or {},
        "context": contexts[:6],
        "payload_json": (record.payload_json or {}) if include_payload else {},
        "created_external_at": record.created_external_at
        or external_created_at_from_payload(record.payload_json or {})
        or record.created_at,
        "updated_external_at": record.updated_external_at,
        "last_synced_at": record.last_synced_at,
    }


def yeast_stock_summary(stock: LabguruYeastStock) -> dict[str, Any]:
    return {
        "name": stock.name,
        "container_type": stock.container_type,
        "box_name": stock.box_name,
        "box_url": stock.box_url,
        "position": stock.position,
        "owner_name": stock.owner_name,
        "stored_by_name": stock.stored_by_name,
        "stored_on": stock.stored_on,
        "external_url": stock.external_url,
        "storage_path": stock.storage_path_json or [],
    }


def yeast_biology_fields(
    payload: dict[str, Any],
    *,
    flattened: list[tuple[str, str]] | None = None,
) -> dict[str, str | None]:
    flattened_fields = flattened if flattened is not None else flatten_labguru_fields(payload)
    values_by_label: dict[str, str] = {}
    for label, value in flattened_fields:
        normalized_label = normalize_search_text(label)
        if normalized_label and value and normalized_label not in values_by_label:
            values_by_label[normalized_label] = value

    result: dict[str, str | None] = {}
    for field_name, aliases in _BIOLOGY_FIELD_ALIASES.items():
        result[field_name] = next(
            (values_by_label[alias] for alias in aliases if values_by_label.get(alias)),
            None,
        )
    return result


def hidden_technical_context(label: str) -> bool:
    normalized = normalize_search_text(label)
    return normalized in _HIDDEN_CONTEXT_LABELS or normalized.endswith(" uuid")


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
