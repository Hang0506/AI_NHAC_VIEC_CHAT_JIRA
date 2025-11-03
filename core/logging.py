import logging
from loguru import logger
import sys
import structlog

from core.config import settings


def _get_numeric_level(level_name: str) -> int:
    try:
        return int(level_name)
    except Exception:
        return getattr(logging, str(level_name).upper(), logging.INFO)


def configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=settings.log_level,
        backtrace=False,
        diagnose=False,
        enqueue=True,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    )

    numeric_level = _get_numeric_level(settings.log_level)

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
