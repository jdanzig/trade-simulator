from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from .config import AppConfig
from .database import Database


class SimulationService:
    def __init__(self, config: AppConfig, db: Database, market_data_client, logger: logging.Logger, ntfy=None):
        self.config = config
        self.db = db
        self.market_data_client = market_data_client
        self.logger = logger
        self.ntfy = ntfy

    def maybe_open_position(
        self,
        trigger: dict[str, Any],
        second_pass: dict[str, Any],
        now: datetime,
        market_is_open: bool,
    ) -> None:
        if second_pass["recommendation"] != "buy_candidate":
            return
        ticker = trigger["ticker"]
        try:
            if market_is_open:
                entry_price = self.market_data_client.fetch_latest_price(ticker)
                self.db.create_position(trigger["id"], ticker, entry_price, now, status="open")
            else:
                # Queue for fill at next market open — matches what a real retail
                # trader would do (market-on-open order) rather than chasing
                # thin after-hours liquidity.
                self.db.create_position(trigger["id"], ticker, 0.0, now, status="pending")
                if self.ntfy:
                    self.ntfy._post(
                        title=f"{ticker} queued for next open",
                        message=f"Market closed at classification. Will fill at next open.",
                        tags=["hourglass_flowing_sand"],
                    )
        except Exception as exc:  # noqa: BLE001
            self.db.log_error("simulation", f"Failed to create position for {ticker}", repr(exc))
            self.logger.exception("Failed to create position for %s", ticker)

    def fill_pending_positions(self, prices: dict[str, float], now: datetime) -> None:
        for position in self.db.list_pending_positions():
            ticker = position["ticker"]
            if ticker not in prices:
                continue
            entry_price = prices[ticker]
            self.db.fill_pending_position(position["id"], entry_price, now)
            self.logger.info("Filled pending position %s at %.2f", ticker, entry_price)
            if self.ntfy:
                self.ntfy._post(
                    title=f"{ticker} filled at ${entry_price:.2f}",
                    message="Queued buy executed at market open.",
                    tags=["white_check_mark"],
                )

    def refresh_open_position_prices(self, prices: dict[str, float], now: datetime) -> None:
        """Update current_price and P&L for open positions using already-fetched prices.

        Unlike update_positions, this does NOT check exit conditions or write
        daily snapshots — it's meant for cheap intraday refreshes during the
        price monitor poll.
        """
        for position in self.db.list_open_positions():
            ticker = position["ticker"]
            if ticker not in prices:
                continue
            current_price = prices[ticker]
            entry_price = float(position["hypothetical_entry_price"])
            pnl_pct = round(((current_price - entry_price) / entry_price) * 100, 2)
            entry_timestamp = datetime.fromisoformat(position["entry_timestamp"])
            days_held = max((now.date() - entry_timestamp.date()).days, 0)
            self.db.update_position(
                position["id"],
                current_price=current_price,
                pnl_pct=pnl_pct,
                days_held=days_held,
                status="open",
            )

    def update_positions(self, trading_date: date) -> None:
        open_positions = self.db.list_open_positions()
        if not open_positions:
            return
        prices = self.market_data_client.fetch_eod_prices(
            [position["ticker"] for position in open_positions],
            trading_date,
        )
        for position in open_positions:
            ticker = position["ticker"]
            if ticker not in prices:
                continue
            current_price = prices[ticker]
            entry_price = float(position["hypothetical_entry_price"])
            pnl_pct = round(((current_price - entry_price) / entry_price) * 100, 2)
            entry_timestamp = datetime.fromisoformat(position["entry_timestamp"])
            days_held = max((trading_date - entry_timestamp.date()).days, 0)
            exit_price = None
            exit_reason = None
            status = "open"
            if pnl_pct >= self.config.target_return_pct:
                status = "closed"
                exit_price = current_price
                exit_reason = "target_reached"
            elif days_held >= self.config.max_hold_days:
                status = "closed"
                exit_price = current_price
                exit_reason = "max_hold_exceeded"
            self.db.update_position(
                position["id"],
                current_price=current_price,
                pnl_pct=pnl_pct,
                days_held=days_held,
                status=status,
                exit_price=exit_price,
                exit_reason=exit_reason,
            )
            if status == "closed" and self.ntfy:
                self.ntfy.notify_position_closed(
                    ticker=ticker,
                    pnl_pct=pnl_pct,
                    exit_reason=exit_reason,
                    days_held=days_held,
                )
            self.db.save_daily_snapshot(
                position["id"],
                trading_date,
                price=current_price,
                pnl_pct=pnl_pct,
                days_held=days_held,
            )
