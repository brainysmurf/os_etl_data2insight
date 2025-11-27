from .sheets import GSheet
from dotenv import load_dotenv
import logging, os


__all__ = ["GSheet"]

load_dotenv()

if level := os.environ.get("DATA2INSIGHTS_LOGLEVEL"):
    logger = logging.getLogger("data2insights")
    logger.setLevel(level)
