from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.config import settings
from services.scheduler import get_scheduler, start_scheduler, schedule_job, get_job_info


router = APIRouter()


class SchedulerConfigIn(BaseModel):
    enabled: Optional[bool] = Field(default=None, description="Enable/disable scheduler")


class SchedulerConfigOut(BaseModel):
    enabled: bool
    running: bool
    jobs: List[dict]


class JobConfigIn(BaseModel):
    cron: str = Field(description="Cron expression, e.g. */5 * * * *")


class JobConfigOut(BaseModel):
    job_id: str
    cron: str
    exists: bool
    next_run_time: Optional[datetime]


@router.get("/", response_model=SchedulerConfigOut, summary="Lấy cấu hình scheduler", description="Xem trạng thái scheduler và danh sách jobs")
async def get_scheduler_config() -> SchedulerConfigOut:
    scheduler = get_scheduler()
    jobs = []
    
    # Get info for all jobs
    for job_id in ["send_reminders", "legacy_bot"]:
        job_info = get_job_info(job_id)
        jobs.append(job_info)
    
    return SchedulerConfigOut(
        enabled=settings.scheduler_enabled,
        running=scheduler.running,
        jobs=jobs,
    )


@router.put("/", response_model=SchedulerConfigOut, summary="Cập nhật cấu hình scheduler", description="Chỉ bật/tắt scheduler, không set cron (dùng /jobs/{job_id} để set cron riêng)")
async def update_scheduler_config(payload: SchedulerConfigIn) -> SchedulerConfigOut:
    scheduler = get_scheduler()

    # Update in-memory settings
    if payload.enabled is not None:
        settings.scheduler_enabled = payload.enabled  # type: ignore[attr-defined]

    if not settings.scheduler_enabled:
        if scheduler.running:
            scheduler.shutdown(wait=False)
        return await get_scheduler_config()

    # Ensure scheduler is started (schedules all jobs with their individual cron)
    if not scheduler.running:
        start_scheduler(scheduler)

    return await get_scheduler_config()


@router.get("/jobs", summary="Danh sách tất cả jobs", description="Xem thông tin tất cả jobs đang schedule")
async def list_jobs() -> List[JobConfigOut]:
    jobs = []
    for job_id in ["send_reminders", "legacy_bot"]:
        job_info = get_job_info(job_id)
        jobs.append(JobConfigOut(**job_info))
    return jobs


@router.get("/jobs/{job_id}", response_model=JobConfigOut, summary="Xem thông tin job", description="Xem thông tin và schedule của một job cụ thể")
async def get_job_config(job_id: str) -> JobConfigOut:
    if job_id not in ["send_reminders", "legacy_bot"]:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    job_info = get_job_info(job_id)
    return JobConfigOut(**job_info)


@router.put("/jobs/{job_id}", response_model=JobConfigOut, summary="Cập nhật schedule cho job", description="Set cron riêng cho một job. Ví dụ: '*/5 * * * *' (mỗi 5 phút), '0 9 * * 1-5' (9h sáng thứ 2-6)")
async def update_job_config(job_id: str, payload: JobConfigIn) -> JobConfigOut:
    if job_id not in ["send_reminders", "legacy_bot"]:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    scheduler = get_scheduler()
    if not scheduler.running:
        raise HTTPException(status_code=400, detail="Scheduler is not running. Enable it first via PUT /scheduler")
    
    # Update config
    if job_id == "send_reminders":
        settings.schedule_cron_reminders = payload.cron  # type: ignore[attr-defined]
    elif job_id == "legacy_bot":
        settings.schedule_cron_legacy = payload.cron  # type: ignore[attr-defined]
    
    # Schedule/reschedule the job
    schedule_job(job_id, payload.cron)
    
    job_info = get_job_info(job_id)
    return JobConfigOut(**job_info)


@router.post("/run-now", summary="Chạy job ngay lập tức", description="Trigger job gửi reminders ngay lập tức mà không cần chờ đến lịch chạy")
async def run_job_now() -> dict:
    from services.scheduler import _job_send_reminders  # local import to avoid cycle at import time

    _job_send_reminders()
    return {"status": "triggered"}


@router.post("/run-legacy", summary="Chạy legacy reminder_bot", description="Gọi reminder_bot.run_once() để kiểm tra nhanh logic cũ")
async def run_legacy_bot() -> dict:
    from services.scheduler import _job_run_legacy_bot  # local import to avoid cycle at import time

    _job_run_legacy_bot()
    return {"status": "legacy_triggered"}


