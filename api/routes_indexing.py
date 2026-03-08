from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.db import get_db
from api.schemas import IndexRequest, IndexResponse
from api.services.project_indexing import index_project_root


router = APIRouter(prefix="/indexing", tags=["indexing"])


@router.post("", response_model=IndexResponse)
def request_index(payload: IndexRequest, db: Session = Depends(get_db)) -> IndexResponse:
    if payload.source_kind != "project_root":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only source_kind=project_root is implemented.",
        )

    try:
        result = index_project_root(
            db,
            root_path=payload.source_path,
            storage_root_name=payload.storage_root_name,
            host_scope=payload.host_scope,
            root_type=payload.root_type,
            clear_existing_for_root=payload.clear_existing_for_root,
        )
        db.commit()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception:
        db.rollback()
        raise

    return IndexResponse(
        status="completed",
        source_kind=payload.source_kind,
        source_path=result.root_path,
        storage_root_name=result.storage_root_name,
        scanned_projects=result.scanned_projects,
        indexed_projects=result.indexed_projects,
        deleted_projects=result.deleted_projects,
        message="Project root indexed successfully.",
    )
