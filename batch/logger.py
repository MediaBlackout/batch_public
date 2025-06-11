import os
import logging
from logging.handlers import RotatingFileHandler


LOG_FILE_NAME = "batch.log"


def _build_handler(logs_dir: str) -> RotatingFileHandler:
    """Create a rotating file handler (10 MB * 5 backups)."""

    file_path = os.path.join(logs_dir, LOG_FILE_NAME)
    handler = RotatingFileHandler(
        file_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    return handler


def get_logger(name: str) -> logging.Logger:
    """Return a module-level logger configured for the batch system.

    The first time this is called a *logs/* directory is created next to this
    file, a rotating file handler is attached as well as a stream handler so
    logs are also echoed to stdout. Subsequent calls simply return the same
    configured logger instance so we never add duplicate handlers.
    """

    base_dir = os.path.dirname(__file__)
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured by an earlier call.
        return logger

    logger.setLevel(logging.INFO)

    # File handler (rotating).
    logger.addHandler(_build_handler(logs_dir))

    # Console handler.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(console_handler)

    # Avoid double logging if the root logger is configured elsewhere.
    logger.propagate = False

    return logger
