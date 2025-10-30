from loguru import logger
import sys

# Configure logger once at import
logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    enqueue=True,
    backtrace=False,
    diagnose=False,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)


def get_logger():
    return logger

