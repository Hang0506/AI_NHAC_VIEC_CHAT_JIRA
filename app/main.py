from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import settings
from core.logging import configure_logging
from services.scheduler import get_scheduler, start_scheduler, shutdown_scheduler
from app.api.v1.routes import api_router

configure_logging()

app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    docs_url="/docs",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api/v1")


@app.on_event("startup")
async def on_startup() -> None:
    scheduler = get_scheduler()
    start_scheduler(scheduler)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    scheduler = get_scheduler()
    shutdown_scheduler(scheduler)
