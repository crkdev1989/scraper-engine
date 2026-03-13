from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

from scraper_engine.utils.logging_utils import log_event


T = TypeVar("T")


def retry_call(
    func: Callable[[], T],
    attempts: int,
    backoff_seconds: float,
    handled_exceptions: tuple[type[BaseException], ...],
    logger=None,
) -> T:
    last_error: BaseException | None = None

    for attempt in range(1, attempts + 1):
        try:
            return func()
        except handled_exceptions as error:
            last_error = error
            log_event(
                logger,
                logging.WARNING,
                "RETRY ATTEMPT",
                "Request attempt failed.",
                attempt=attempt,
                max_attempts=attempts,
                error=error,
            )
            if attempt == attempts:
                break
            time.sleep(backoff_seconds * attempt)

    assert last_error is not None
    raise last_error
