from fastapi import APIRouter

from api.schemas import IndexRequest


router = APIRouter(prefix="/indexing", tags=["indexing"])


@router.post("")
def request_index(payload: IndexRequest) -> dict:
    # Placeholder endpoint. The intended implementation is:
    # 1. validate the requested source path or storage root
    # 2. create a queued indexing job in the catalog
    # 3. let a worker pick it up asynchronously
    return {
        "status": "accepted",
        "source_kind": payload.source_kind,
        "source_path": payload.source_path,
        "message": "Indexing job creation is not implemented yet.",
    }

