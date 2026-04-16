from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .market import EASTERN
from .utils import from_json, to_json


def _as_eastern_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=EASTERN)
    return value.astimezone(EASTERN).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


@dataclass(slots=True)
class TriggerCandidate:
    ticker: str
    drop_pct: float
    intraday_high: float
    trigger_price: float
    triggered_at: datetime
    budget_status: str
    recheck_scheduled_at: datetime | None


class Database:
    def __init__(self, path: Path):
        self.path = path

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(self.path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS universe_tickers (
                    ticker TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    universe TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    refreshed_at TEXT NOT NULL,
                    PRIMARY KEY (ticker, universe)
                );

                CREATE TABLE IF NOT EXISTS triggers (
                    id TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    triggered_at TEXT NOT NULL,
                    drop_pct REAL NOT NULL,
                    intraday_high REAL NOT NULL,
                    trigger_price REAL NOT NULL,
                    budget_status TEXT NOT NULL,
                    recheck_scheduled_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS classifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trigger_id TEXT NOT NULL,
                    pass_number INTEGER NOT NULL,
                    news_maturity TEXT NOT NULL,
                    sources_used TEXT NOT NULL,
                    cause_summary TEXT NOT NULL,
                    cause_category TEXT NOT NULL,
                    affects_cash_flows INTEGER NOT NULL,
                    affects_cash_flows_reasoning TEXT NOT NULL,
                    reversible INTEGER NOT NULL,
                    reversible_reasoning TEXT NOT NULL,
                    retail_sentiment TEXT NOT NULL,
                    retail_sentiment_reasoning TEXT NOT NULL,
                    smart_money_signal TEXT NOT NULL,
                    overreaction_score INTEGER NOT NULL,
                    overreaction_reasoning TEXT NOT NULL,
                    recommendation TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    recheck_scheduled_at TEXT,
                    full_claude_response TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(trigger_id, pass_number),
                    FOREIGN KEY (trigger_id) REFERENCES triggers(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS hypothetical_positions (
                    id TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    trigger_id TEXT NOT NULL,
                    hypothetical_entry_price REAL NOT NULL,
                    entry_timestamp TEXT NOT NULL,
                    current_price REAL NOT NULL,
                    hypothetical_pnl_pct REAL NOT NULL,
                    days_held INTEGER NOT NULL,
                    exit_price REAL,
                    exit_reason TEXT,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(trigger_id),
                    FOREIGN KEY (trigger_id) REFERENCES triggers(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS daily_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    price REAL NOT NULL,
                    pnl_pct REAL NOT NULL,
                    days_held INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(position_id, snapshot_date),
                    FOREIGN KEY (position_id) REFERENCES hypothetical_positions(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS system_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS cooldowns (
                    ticker TEXT PRIMARY KEY,
                    expires_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    component TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    raw_exception TEXT
                );
                """
            )

    def log_error(self, component: str, error_message: str, raw_exception: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO errors (timestamp, component, error_message, raw_exception)
                VALUES (?, ?, ?, ?)
                """,
                (_as_eastern_iso(datetime.now(EASTERN)), component, error_message, raw_exception),
            )

    def set_state(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO system_state (state_key, state_value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (key, value),
            )

    def get_state(self, key: str, default: str | None = None) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT state_value FROM system_state WHERE state_key = ?",
                (key,),
            ).fetchone()
        return row["state_value"] if row else default

    def get_today_api_usage(self, today: date) -> int:
        stored_date = self.get_state("api_call_usage_date")
        if stored_date != today.isoformat():
            self.set_state("api_call_usage_date", today.isoformat())
            self.set_state("api_call_usage_count", "0")
            return 0
        return int(self.get_state("api_call_usage_count", "0") or "0")

    def increment_today_api_usage(self, today: date, amount: int = 1) -> int:
        current = self.get_today_api_usage(today) + amount
        self.set_state("api_call_usage_date", today.isoformat())
        self.set_state("api_call_usage_count", str(current))
        return current

    def upsert_universe(self, entries: list[dict[str, str]], universe: str, refreshed_at: datetime) -> None:
        timestamp = _as_eastern_iso(refreshed_at)
        with self.connect() as conn:
            conn.execute("UPDATE universe_tickers SET active = 0 WHERE universe = ?", (universe,))
            for entry in entries:
                conn.execute(
                    """
                    INSERT INTO universe_tickers (ticker, company_name, universe, active, refreshed_at)
                    VALUES (?, ?, ?, 1, ?)
                    ON CONFLICT(ticker, universe) DO UPDATE SET
                        company_name = excluded.company_name,
                        active = 1,
                        refreshed_at = excluded.refreshed_at
                    """,
                    (entry["ticker"], entry["company_name"], universe, timestamp),
                )

    def list_universe(self, universe: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT ticker, company_name, refreshed_at
                FROM universe_tickers
                WHERE universe = ? AND active = 1
                ORDER BY ticker
                """,
                (universe,),
            ).fetchall()
        return [dict(row) for row in rows]

    def create_trigger(self, candidate: TriggerCandidate) -> str:
        trigger_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO triggers (
                    id, ticker, triggered_at, drop_pct, intraday_high, trigger_price,
                    budget_status, recheck_scheduled_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trigger_id,
                    candidate.ticker,
                    _as_eastern_iso(candidate.triggered_at),
                    candidate.drop_pct,
                    candidate.intraday_high,
                    candidate.trigger_price,
                    candidate.budget_status,
                    _as_eastern_iso(candidate.recheck_scheduled_at),
                ),
            )
        return trigger_id

    def get_trigger(self, trigger_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM triggers WHERE id = ?", (trigger_id,)).fetchone()
        if not row:
            return None
        payload = dict(row)
        payload["triggered_at"] = _parse_iso(payload["triggered_at"])
        payload["recheck_scheduled_at"] = _parse_iso(payload["recheck_scheduled_at"])
        return payload

    def set_cooldown(self, ticker: str, expires_at: datetime) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO cooldowns (ticker, expires_at)
                VALUES (?, ?)
                ON CONFLICT(ticker) DO UPDATE SET expires_at = excluded.expires_at
                """,
                (ticker, _as_eastern_iso(expires_at)),
            )

    def is_in_cooldown(self, ticker: str, now: datetime) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT expires_at FROM cooldowns WHERE ticker = ?",
                (ticker,),
            ).fetchone()
        if not row:
            return False
        expires_at = _parse_iso(row["expires_at"])
        return bool(expires_at and expires_at > now)

    def save_classification(self, trigger_id: str, payload: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO classifications (
                    trigger_id, pass_number, news_maturity, sources_used, cause_summary,
                    cause_category, affects_cash_flows, affects_cash_flows_reasoning, reversible,
                    reversible_reasoning, retail_sentiment, retail_sentiment_reasoning,
                    smart_money_signal, overreaction_score, overreaction_reasoning,
                    recommendation, confidence, recheck_scheduled_at, full_claude_response
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trigger_id, pass_number) DO UPDATE SET
                    news_maturity = excluded.news_maturity,
                    sources_used = excluded.sources_used,
                    cause_summary = excluded.cause_summary,
                    cause_category = excluded.cause_category,
                    affects_cash_flows = excluded.affects_cash_flows,
                    affects_cash_flows_reasoning = excluded.affects_cash_flows_reasoning,
                    reversible = excluded.reversible,
                    reversible_reasoning = excluded.reversible_reasoning,
                    retail_sentiment = excluded.retail_sentiment,
                    retail_sentiment_reasoning = excluded.retail_sentiment_reasoning,
                    smart_money_signal = excluded.smart_money_signal,
                    overreaction_score = excluded.overreaction_score,
                    overreaction_reasoning = excluded.overreaction_reasoning,
                    recommendation = excluded.recommendation,
                    confidence = excluded.confidence,
                    recheck_scheduled_at = excluded.recheck_scheduled_at,
                    full_claude_response = excluded.full_claude_response
                """,
                (
                    trigger_id,
                    int(payload["pass"]),
                    payload["news_maturity"],
                    to_json(payload["sources_used"]),
                    payload["cause_summary"],
                    payload["cause_category"],
                    1 if payload["affects_cash_flows"] else 0,
                    payload["affects_cash_flows_reasoning"],
                    1 if payload["reversible"] else 0,
                    payload["reversible_reasoning"],
                    payload["retail_sentiment"],
                    payload["retail_sentiment_reasoning"],
                    payload["smart_money_signal"],
                    int(payload["overreaction_score"]),
                    payload["overreaction_reasoning"],
                    payload["recommendation"],
                    payload["confidence"],
                    payload.get("recheck_scheduled_at"),
                    to_json(payload["full_claude_response"]),
                ),
            )

    def get_classification(self, trigger_id: str, pass_number: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM classifications
                WHERE trigger_id = ? AND pass_number = ?
                """,
                (trigger_id, pass_number),
            ).fetchone()
        if not row:
            return None
        payload = dict(row)
        payload["sources_used"] = from_json(payload["sources_used"], [])
        payload["affects_cash_flows"] = bool(payload["affects_cash_flows"])
        payload["reversible"] = bool(payload["reversible"])
        payload["full_claude_response"] = from_json(payload["full_claude_response"], {})
        return payload

    def list_pending_rechecks(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT t.*
                FROM triggers t
                LEFT JOIN classifications c2
                  ON c2.trigger_id = t.id
                 AND c2.pass_number = 2
                WHERE t.budget_status = 'classified'
                  AND c2.id IS NULL
                ORDER BY t.triggered_at
                """
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["triggered_at"] = _parse_iso(payload["triggered_at"])
            payload["recheck_scheduled_at"] = _parse_iso(payload["recheck_scheduled_at"])
            results.append(payload)
        return results

    def create_position(self, trigger_id: str, ticker: str, entry_price: float, entry_timestamp: datetime) -> str:
        position_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO hypothetical_positions (
                    id, ticker, trigger_id, hypothetical_entry_price, entry_timestamp,
                    current_price, hypothetical_pnl_pct, days_held, status
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'open')
                ON CONFLICT(trigger_id) DO NOTHING
                """,
                (
                    position_id,
                    ticker,
                    trigger_id,
                    entry_price,
                    _as_eastern_iso(entry_timestamp),
                    entry_price,
                ),
            )
        return position_id

    def list_open_positions(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM hypothetical_positions
                WHERE status = 'open'
                ORDER BY entry_timestamp
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def update_position(
        self,
        position_id: str,
        *,
        current_price: float,
        pnl_pct: float,
        days_held: int,
        status: str,
        exit_price: float | None = None,
        exit_reason: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE hypothetical_positions
                SET current_price = ?,
                    hypothetical_pnl_pct = ?,
                    days_held = ?,
                    status = ?,
                    exit_price = COALESCE(?, exit_price),
                    exit_reason = COALESCE(?, exit_reason),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (current_price, pnl_pct, days_held, status, exit_price, exit_reason, position_id),
            )

    def save_daily_snapshot(
        self,
        position_id: str,
        snapshot_date: date,
        *,
        price: float,
        pnl_pct: float,
        days_held: int,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_snapshots (position_id, snapshot_date, price, pnl_pct, days_held)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(position_id, snapshot_date) DO UPDATE SET
                    price = excluded.price,
                    pnl_pct = excluded.pnl_pct,
                    days_held = excluded.days_held
                """,
                (position_id, snapshot_date.isoformat(), price, pnl_pct, days_held),
            )

    def list_triggers_for_date(self, current_date: date) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM triggers
                WHERE date(triggered_at) = ?
                ORDER BY triggered_at
                """,
                (current_date.isoformat(),),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_closed_positions_for_date(self, current_date: date) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM hypothetical_positions
                WHERE status = 'closed'
                  AND date(updated_at) = ?
                ORDER BY updated_at DESC
                """,
                (current_date.isoformat(),),
            ).fetchall()
        return [dict(row) for row in rows]

    def count_positions_entered_on(self, current_date: date) -> int:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS position_count
                FROM hypothetical_positions
                WHERE date(entry_timestamp) = ?
                """,
                (current_date.isoformat(),),
            ).fetchone()
        return int(row["position_count"])

    def count_errors_for_date(self, current_date: date) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS error_count FROM errors WHERE date(timestamp) = ?",
                (current_date.isoformat(),),
            ).fetchone()
        return int(row["error_count"])

    def get_classifier_rollups(self) -> dict[str, dict[str, dict[str, float | int]]]:
        with self.connect() as conn:
            confidence_rows = conn.execute(
                """
                SELECT c.confidence, COUNT(*) AS total,
                       SUM(CASE WHEN p.hypothetical_pnl_pct > 0 THEN 1 ELSE 0 END) AS profitable
                FROM classifications c
                JOIN hypothetical_positions p ON p.trigger_id = c.trigger_id
                WHERE c.pass_number = 2
                GROUP BY c.confidence
                """
            ).fetchall()
            category_rows = conn.execute(
                """
                SELECT c.cause_category, COUNT(*) AS total,
                       SUM(CASE WHEN p.hypothetical_pnl_pct > 0 THEN 1 ELSE 0 END) AS profitable
                FROM classifications c
                JOIN hypothetical_positions p ON p.trigger_id = c.trigger_id
                WHERE c.pass_number = 2
                GROUP BY c.cause_category
                """
            ).fetchall()
        by_confidence: dict[str, dict[str, float | int]] = {}
        for row in confidence_rows:
            total = int(row["total"])
            profitable = int(row["profitable"] or 0)
            by_confidence[row["confidence"]] = {
                "total": total,
                "profitable_at_30_days": profitable,
                "win_rate_pct": round((profitable / total) * 100, 1) if total else 0.0,
            }
        by_category: dict[str, dict[str, float | int]] = {}
        for row in category_rows:
            total = int(row["total"])
            profitable = int(row["profitable"] or 0)
            by_category[row["cause_category"]] = {
                "total": total,
                "win_rate_pct": round((profitable / total) * 100, 1) if total else 0.0,
            }
        return {
            "by_confidence": by_confidence,
            "by_cause_category": by_category,
        }

    def portfolio_performance(self) -> dict[str, float]:
        with self.connect() as conn:
            open_positions = conn.execute(
                """
                SELECT AVG(hypothetical_pnl_pct) AS avg_open
                FROM hypothetical_positions
                WHERE status = 'open'
                """
            ).fetchone()
            all_positions = conn.execute(
                """
                SELECT AVG(hypothetical_pnl_pct) AS avg_all
                FROM hypothetical_positions
                """
            ).fetchone()
        return {
            "today": round(float(open_positions["avg_open"] or 0.0), 2),
            "inception": round(float(all_positions["avg_all"] or 0.0), 2),
        }

    def list_closed_positions_since(self, start_date: date) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT p.*, c.cause_category, c.confidence, c.news_maturity
                FROM hypothetical_positions p
                JOIN classifications c
                  ON c.trigger_id = p.trigger_id
                 AND c.pass_number = 2
                WHERE p.status = 'closed'
                  AND date(p.updated_at) >= ?
                ORDER BY p.updated_at DESC
                """,
                (start_date.isoformat(),),
            ).fetchall()
        return [dict(row) for row in rows]
