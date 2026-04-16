from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from .config import AppConfig
from .database import Database


class SimulationService:
    def __init__(self, config: AppConfig, db: Database, market_data_client, logger: logging.Logger):
        self.config = config
        self.db = db
        self.market_data_client = market_data_client
        self.logger = logger

    def maybe_open_position(self, trigger: dict[str, Any], second_pass: dict[str, Any], now: datetime) -> None:
        if second_pass["recommendation"] != "buy_candidate":
            return
        entry_price = self.market_data_client.fetch_latest_price(trigger["ticker"])
        try:
            self.db.create_position(trigger["id"], trigger["ticker"], entry_price, now)
        except Exception as exc:  # noqa: BLE001
            self.db.log_error("simulation", f"Failed to create position for {trigger['ticker']}", repr(exc))
            self.logger.exception("Failed to create position for %s", trigger["ticker"])

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
            self.db.save_daily_snapshot(
                position["id"],
                trading_date,
                price=current_price,
                pnl_pct=pnl_pct,
                days_held=days_held,
            )
