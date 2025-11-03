from __future__ import annotations
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from core.config import settings
from services.reminder_service import ReminderService
from services.jira_service import JiraService
from services.chat_service import ChatService

_scheduler: BackgroundScheduler | None = None


def get_scheduler() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(timezone=settings.timezone)
    return _scheduler


def _job_send_reminders() -> None:
    logger.info("Running scheduled reminders job")
    service = ReminderService(jira_service=JiraService(), chat_service=ChatService())
    # Fire and forget; underlying operations are synchronous within service methods
    import asyncio
    asyncio.run(service.send_due_reminders())


def _job_run_legacy_bot() -> None:
    """Run legacy reminder_bot one-off cycle for testing via API."""
    logger.info("Running legacy reminder_bot.run_once()")
    try:
        # Import locally to avoid import cost/cycles at module import time
        from reminder_bot import run_once
        run_once()
    except Exception as ex:
        logger.exception("Legacy bot run failed: {}", ex)


def start_scheduler(scheduler: BackgroundScheduler) -> None:
    if not settings.scheduler_enabled:
        logger.info("Scheduler disabled by settings")
        return
    if not scheduler.running:
        # Schedule reminders job
        trigger_reminders = CronTrigger.from_crontab(settings.schedule_cron_reminders)
        scheduler.add_job(_job_send_reminders, trigger_reminders, id="send_reminders", replace_existing=True)
        logger.info("Scheduled reminders job with cron: {}", settings.schedule_cron_reminders)
        
        # Schedule legacy bot job
        trigger_legacy = CronTrigger.from_crontab(settings.schedule_cron_legacy)
        scheduler.add_job(_job_run_legacy_bot, trigger_legacy, id="legacy_bot", replace_existing=True)
        logger.info("Scheduled legacy bot job with cron: {}", settings.schedule_cron_legacy)
        
        scheduler.start()
        logger.info("Scheduler started")


def schedule_job(job_id: str, cron: str) -> None:
    """Schedule or reschedule a specific job with given cron."""
    scheduler = get_scheduler()
    if not scheduler.running:
        logger.warning("Scheduler not running, cannot schedule job")
        return
    
    trigger = CronTrigger.from_crontab(cron)
    
    if job_id == "send_reminders":
        func = _job_send_reminders
    elif job_id == "legacy_bot":
        func = _job_run_legacy_bot
    else:
        logger.error(f"Unknown job_id: {job_id}")
        return
    
    job = scheduler.get_job(job_id)
    if job is None:
        scheduler.add_job(func, trigger, id=job_id, replace_existing=True)
        logger.info(f"Scheduled job {job_id} with cron: {cron}")
    else:
        scheduler.reschedule_job(job_id, trigger=trigger)
        logger.info(f"Rescheduled job {job_id} with cron: {cron}")


def get_job_info(job_id: str) -> dict:
    """Get info about a specific job."""
    scheduler = get_scheduler()
    job = scheduler.get_job(job_id) if scheduler.running else None
    
    if job_id == "send_reminders":
        cron = settings.schedule_cron_reminders
    elif job_id == "legacy_bot":
        cron = settings.schedule_cron_legacy
    else:
        return {"job_id": job_id, "exists": False}
    
    return {
        "job_id": job_id,
        "cron": cron,
        "exists": job is not None,
        "next_run_time": job.next_run_time if job else None,
    }


def shutdown_scheduler(scheduler: BackgroundScheduler) -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown")
