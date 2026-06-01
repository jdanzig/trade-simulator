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


class TriggerOutcomeService:
    """Computes the counterfactual outcome of every trigger by running it
    through the strategy's actual exit rules on realized prices — no
    hindsight. This labels false negatives (passed triggers that would have
    hit target) and true negatives, completing the classifier scorecard the
    live position tracker can't see.

    Entry is anchored at recheck_scheduled_at (the moment a real buy would
    fill at the second pass) and the +target / max-hold-days exit is
    evaluated on daily CLOSES, mirroring the once-daily eod_update_job —
    not intraday highs, which would overstate capturable gains.
    """

    def __init__(
        self,
        db: Database,
        market_data_client,
        logger: logging.Logger,
        *,
        target_return_pct: float,
        max_hold_days: int,
    ):
        self.db = db
        self.market_data_client = market_data_client
        self.logger = logger
        self.target_return_pct = target_return_pct
        self.max_hold_days = max_hold_days

    def compute_outcomes(self, now: datetime) -> int:
        """Finalize outcomes for any trigger whose verdict is now knowable.

        A winner is finalized as soon as a daily close crosses target; a
        non-winner only once the full max-hold window has elapsed. Triggers
        that are neither yet are left for a future run.
        """
        finalized = 0
        for trigger in self.db.list_triggers_needing_trigger_outcome():
            try:
                if self._compute_single(trigger, now):
                    finalized += 1
            except Exception as exc:  # noqa: BLE001
                self.db.log_error(
                    "trigger_outcomes", f"compute failed for {trigger.get('ticker')}", repr(exc)
                )
                self.logger.exception("Trigger outcome compute failed for %s", trigger.get("ticker"))
        return finalized

    def _compute_single(self, trigger: dict[str, Any], now: datetime) -> bool:
        entry_time = trigger.get("recheck_scheduled_at") or trigger.get("triggered_at")
        if entry_time is None:
            return False
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=EASTERN)
        if entry_time >= now:
            return False  # entry hasn't happened yet
        ticker = trigger["ticker"]
        window_end = entry_time + timedelta(days=self.max_hold_days)
        entry_price = self.market_data_client.fetch_price_at(ticker, entry_time)
        if not entry_price:
            return False  # no price at entry yet; retry later
        closes = self.market_data_client.fetch_daily_closes(ticker, entry_time, min(now, window_end))
        if not closes:
            return False
        target_price = entry_price * (1 + self.target_return_pct / 100)
        max_close = max(price for _, price in closes)
        max_close_return = round(((max_close - entry_price) / entry_price) * 100, 4)

        # First daily close at or above target → the strategy would have sold.
        for ts, price in closes:
            if price >= target_price:
                self._finalize(
                    trigger["id"], entry_time, entry_price, ts, price,
                    hit_target=True, exit_reason="target_reached",
                    max_close_return=max_close_return, now=now,
                )
                return True

        # No target hit. Only finalize once the full window has closed.
        if now >= window_end:
            exit_ts, exit_price = closes[-1]
            ret = round(((exit_price - entry_price) / entry_price) * 100, 4)
            self._finalize(
                trigger["id"], entry_time, entry_price, exit_ts, exit_price,
                hit_target=False, exit_reason="max_hold_exceeded",
                max_close_return=max_close_return, now=now, return_pct=ret,
            )
            return True
        return False  # still open, no verdict yet

    def _finalize(
        self, trigger_id, entry_time, entry_price, exit_time, exit_price,
        *, hit_target, exit_reason, max_close_return, now, return_pct=None,
    ) -> None:
        if return_pct is None:
            return_pct = round(((exit_price - entry_price) / entry_price) * 100, 4)
        self.db.insert_trigger_outcome(
            trigger_id=trigger_id,
            entry_time=entry_time,
            entry_price=entry_price,
            exit_time=exit_time,
            exit_price=exit_price,
            return_pct=return_pct,
            hit_target=hit_target,
            exit_reason=exit_reason,
            max_close_return_pct=max_close_return,
            computed_at=now,
        )
