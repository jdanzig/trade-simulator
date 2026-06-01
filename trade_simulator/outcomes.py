from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from .database import Database
from .market import EASTERN

# Map config window strings ("1h", "30m", "1d") to seconds.
_WINDOW_SUFFIX_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_window(label: str) -> int:
    """Convert a window label like '1h' or '30m' to seconds."""
    label = label.strip().lower()
    if not label or label[-1] not in _WINDOW_SUFFIX_SECONDS:
        raise ValueError(f"Invalid outcome window: {label!r} (expected like '1h', '30m', '1d')")
    return int(label[:-1]) * _WINDOW_SUFFIX_SECONDS[label[-1]]


class OutcomeService:
    """Computes realized returns for news events over configured windows.

    For each (news_event, window) pair:
      anchor_time  = news_event.published_at (or fetched_at as fallback)
      future_time  = anchor_time + window
      anchor_price = first Alpaca trade at-or-after anchor_time
      future_price = first Alpaca trade at-or-after future_time
      return_pct   = (future_price - anchor_price) / anchor_price * 100

    Out-of-market hours roll forward to the next available bar, so a news
    event published Friday 8pm with a 1h window measures the gap between
    Monday's opening print and the print one hour later. Slightly noisy
    for after-hours events but consistent and reproducible.
    """

    def __init__(self, db: Database, market_data_client, logger: logging.Logger):
        self.db = db
        self.market_data_client = market_data_client
        self.logger = logger

    def compute_outcomes(self, windows: list[str], now: datetime) -> int:
        """Process every news event whose window has closed but lacks an outcome.

        Returns the number of outcomes inserted.
        """
        inserted = 0
        for label in windows:
            seconds = parse_window(label)
            pending = self.db.list_news_events_needing_outcome(label, seconds, now)
            for event in pending:
                if self._compute_single(event, label, seconds, now):
                    inserted += 1
        return inserted

    def _compute_single(
        self,
        event: dict[str, Any],
        window_label: str,
        window_seconds: int,
        now: datetime,
    ) -> bool:
        ticker = event["ticker"]
        anchor_raw = event.get("published_at") or event["fetched_at"]
        try:
            anchor_time = datetime.fromisoformat(anchor_raw)
        except ValueError:
            self.logger.warning("Bad anchor timestamp on news_event %s: %r", event["id"], anchor_raw)
            return False
        if anchor_time.tzinfo is None:
            anchor_time = anchor_time.replace(tzinfo=EASTERN)
        future_time = anchor_time + timedelta(seconds=window_seconds)
        try:
            anchor_price = self.market_data_client.fetch_price_at(ticker, anchor_time)
            future_price = self.market_data_client.fetch_price_at(ticker, future_time)
        except Exception as exc:  # noqa: BLE001
            self.db.log_error("outcomes", f"price fetch failed for {ticker}", repr(exc))
            self.logger.exception("Price fetch failed for %s window=%s", ticker, window_label)
            return False
        if not anchor_price or not future_price:
            return False  # Alpaca had no bars in the search window; try again later
        return_pct = round(((future_price - anchor_price) / anchor_price) * 100, 4)
        self.db.insert_news_outcome(
            news_event_id=event["id"],
            window_label=window_label,
            window_seconds=window_seconds,
            anchor_time=anchor_time,
            anchor_price=anchor_price,
            future_time=future_time,
            future_price=future_price,
            return_pct=return_pct,
            computed_at=now,
        )
        return True
