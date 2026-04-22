from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar
from typing import Optional


T = TypeVar("T")


def with_retry(fn: Callable[[], T], retries: int, delay_sec: float) -> T:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(delay_sec)
    if last_error is not None:
        raise last_error
    raise RuntimeError("Retry failed with unknown error")
