"""Memory admission for the standard, streaming ROI extractor.

This is a scheduling contract, not an implementation of MATLAB's block planner.
Unknown modules and runs containing other processing steps keep fixed budgets.
"""
from __future__ import annotations

from typing import Any

ROI_EXTRACT_MIN_MEMORY_MB = 24 * 1024
ROI_EXTRACT_PREFERRED_MEMORY_MB = 48 * 1024
ROI_EXTRACT_MEMORY_MODE = "roi_extract_auto_v1"


def inspect_roi_extraction(pipeline: dict, payload: dict) -> dict[str, Any]:
    request = payload.get("run_request") or {}
    if request.get("ingest_raw_dataset"):
        return {"eligible": False, "reason": "raw_ingestion_in_run"}
    if str(request.get("roi_cache_policy") or "auto").lower() not in {"auto", "disk"}:
        return {"eligible": False, "reason": "unbounded_roi_cache"}
    selection = request.get("selected_nodes")
    if selection is not None and not isinstance(selection, list):
        return {"eligible": False, "reason": "invalid_selection"}
    selected = {str(value) for value in selection or []}
    nodes = pipeline.get("nodes") or []
    known = {str(node.get("id") or f"node_{i + 1}") for i, node in enumerate(nodes)
             if isinstance(node, dict)}
    if selected - known:
        return {"eligible": False, "reason": "unknown_selected_nodes"}
    active = []
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            return {"eligible": False, "reason": "unknown_node"}
        node_id = str(node.get("id") or f"node_{i + 1}")
        if selected and node_id not in selected:
            continue
        if node.get("enabled") is False:
            continue
        # Do not opt an external processor or a mixed pipeline into a contract
        # that only roiExtract.process implements. In particular, historical
        # extractors with fixed cut/block lengths cannot borrow this policy.
        if (str(node.get("type") or "").lower() != "roiextract"
                or node.get("pkg", "roiExtract") != "roiExtract"
                or node.get("func", "roiExtract.process") != "roiExtract.process"):
            return {"eligible": False, "reason": "non_adaptive_selected_node"}
        active.append(node_id)
    if not active:
        return {"eligible": False, "reason": "no_extraction_nodes"}
    return {"eligible": True, "selected_node_ids": active}


def minimum_allocation(allocation: dict) -> dict:
    """Reserve the safe floor, never the optional surplus, for a waiting job."""
    if allocation.get("memory_allocation_mode") != ROI_EXTRACT_MEMORY_MODE:
        return allocation
    return _with_memory(allocation, allocation["memory_min_mb"])


def fit_memory_to_capacity(
    allocation: dict, *, totals: dict, policy: dict, reserved_allocation: dict | None = None,
) -> dict:
    """Choose a budget for one new claim; never mutate an active allocation.

    Returning the minimum when it does not fit lets the ordinary admission
    checks report the shortage and keep the job queued.
    """
    if allocation.get("memory_allocation_mode") != ROI_EXTRACT_MEMORY_MODE:
        return allocation
    minimum = allocation["memory_min_mb"]
    preferred = allocation["memory_preferred_mb"]
    reserved = minimum_allocation(reserved_allocation or {}).get("memory_mb", 0)
    bounds = [preferred]
    if totals.get("memory_capacity_mb") is not None:
        bounds.append(totals["memory_capacity_mb"] - totals.get("memory_mb", 0) - reserved)
    if policy.get("worker_limit_mb") is not None:
        bounds.append(policy["worker_limit_mb"])
    # Respect pressure from non-Hub processes too. Do not count another job's
    # unused reservation as free memory in the shared admission budget.
    bounds.append(policy["available_mb"] - policy["headroom_mb"] - reserved)
    memory = max(minimum, min(bounds))
    result = _with_memory(allocation, memory)
    result["memory_sizing_reason"] = "preferred" if memory == preferred else "available_capacity"
    return result


def _with_memory(allocation: dict, memory_mb: int) -> dict:
    result = {**allocation, "memory_mb": memory_mb}
    pool = allocation["memory_pool_mb"]
    result["swap_limit_mb"] = allocation["memory_pool_swap_mb"] * memory_mb // max(1, pool)
    return result
