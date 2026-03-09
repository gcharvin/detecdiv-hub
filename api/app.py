from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from api.config import get_settings
from api.routes_dashboard import router as dashboard_router
from api.routes_indexing import router as indexing_router
from api.routes_jobs import router as jobs_router
from api.routes_pipelines import router as pipelines_router
from api.routes_projects import groups_router, router as projects_router, storage_roots_router, users_router
from api.schemas import HealthResponse


settings = get_settings()
app = FastAPI(title=settings.app_name, version="0.1.0")
app.include_router(projects_router)
app.include_router(groups_router)
app.include_router(users_router)
app.include_router(storage_roots_router)
app.include_router(jobs_router)
app.include_router(pipelines_router)
app.include_router(indexing_router)
app.include_router(dashboard_router)

STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/web", StaticFiles(directory=STATIC_DIR, html=True), name="web")


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/web/")
