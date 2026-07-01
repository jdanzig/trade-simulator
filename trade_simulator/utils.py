from __future__ import annotations

import json
import logging
import math
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime
from typing import Any, TypeVar

T = TypeVar("T")


def chunked(values: Iterable[T], size: int) -> Iterator[list[T]]:
    batch: list[T] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _retryable_status(result: Any) -> int | None:
    """If the operation returned an HTTP response with a retryable status
    (429 or 5xx), return that status; otherwise None.

    Call sites use the pattern `response = with_retry(lambda: get(...))`
    followed by raise_for_status, so a 429/5xx response never raises inside
    the retry loop — without this check rate limits were never retried.
    """
    status = getattr(result, "status_code", None)
    if status is not None and (status == 429 or status >= 500):
        return int(status)
    return None


def _retry_after_seconds(result: Any) -> float | None:
    headers = getattr(result, "headers", None)
    if not headers:
        return None
    raw = headers.get("Retry-After")
    if not raw:
        return None
    try:
        return min(float(raw), 60.0)
    except (TypeError, ValueError):
        return None


def with_retry(
    operation: Callable[[], T],
    *,
    component: str,
    logger: logging.Logger,
    retries: int = 3,
    base_delay_seconds: float = 1.0,
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            result = operation()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= retries:
                break
            delay = base_delay_seconds * (2 ** (attempt - 1))
            logger.warning(
                "%s attempt %s/%s failed: %s. Retrying in %.1fs",
                component,
                attempt,
                retries,
                exc,
                delay,
            )
            time.sleep(delay)
            continue
        status = _retryable_status(result)
        if status is None:
            return result
        if attempt >= retries:
            return result  # let the caller's raise_for_status surface it
        delay = _retry_after_seconds(result) or base_delay_seconds * (2 ** (attempt - 1))
        logger.warning(
            "%s attempt %s/%s got HTTP %s. Retrying in %.1fs",
            component,
            attempt,
            retries,
            status,
            delay,
        )
        time.sleep(delay)
    assert last_error is not None
    raise last_error


def estimate_token_count(text: str) -> int:
    return int(math.ceil(len(text) / 4))


def extract_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    decoder = json.JSONDecoder()
    obj, _ = decoder.raw_decode(stripped)
    if not isinstance(obj, dict):
        raise ValueError("Claude response did not decode to a JSON object.")
    return obj


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def from_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


def utc_now() -> datetime:
    return datetime.utcnow()
