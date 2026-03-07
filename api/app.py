from fastapi import FastAPI

from api.config import get_settings
from api.routes_indexing import router as indexing_router
from api.routes_jobs import router as jobs_router
from api.routes_projects import router as projects_router
from api.schemas import HealthResponse


settings = get_settings()
app = FastAPI(title=settings.app_name, version="0.1.0")
app.include_router(projects_router)
app.include_router(jobs_router)
app.include_router(indexing_router)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse()

