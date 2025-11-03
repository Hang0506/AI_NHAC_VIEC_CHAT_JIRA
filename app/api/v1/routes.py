from fastapi import APIRouter

from app.api.v1.endpoints.health import router as health_router
from app.api.v1.endpoints.reminders import router as reminders_router
from app.api.v1.endpoints.scheduler import router as scheduler_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(reminders_router, prefix="/reminders", tags=["reminders"])
api_router.include_router(scheduler_router, prefix="/scheduler", tags=["scheduler"])
