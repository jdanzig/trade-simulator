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
            return operation()
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
