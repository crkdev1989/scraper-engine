from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any


LOGGER_NAME = "scraper_engine"


def configure_logging(level: str = "INFO", log_file: Path | None = None) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level.upper())
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level.upper())
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level.upper())
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def log_event(
    logger: logging.Logger | None,
    level: int,
    event: str,
    message: str,
    **details: Any,
) -> None:
    if logger is None:
        return

    detail_parts = [
        f"{key}={value}"
        for key, value in details.items()
        if value is not None and value != ""
    ]
    suffix = f" | {' | '.join(detail_parts)}" if detail_parts else ""
    logger.log(level, "%s | %s%s", event, message, suffix)
