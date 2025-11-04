import os
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from logger import get_logger
from services.reminder_bot import run_once

logger = get_logger()

def main():
    load_dotenv()
    cron_expr = os.getenv("SCHEDULE_CRON", "").strip()
    minutes = int(os.getenv("SCHEDULE_INTERVAL_MINUTES", "15"))
    scheduler = BlockingScheduler()

    if cron_expr:
        logger.info(f"Starting scheduler with CRON: {cron_expr}")
        scheduler.add_job(run_once, CronTrigger.from_crontab(cron_expr), max_instances=1, coalesce=True)
    else:
        logger.info(f"Starting scheduler with interval: {minutes} minutes")
        scheduler.add_job(run_once, IntervalTrigger(minutes=minutes), max_instances=1, coalesce=True)
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    main()
