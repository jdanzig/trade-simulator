"""Backfill benchmark_return_pct (SPY over the same holding window) for
existing trigger_outcomes rows, then report average alpha.

Uses a single SPY daily-bars fetch spanning the whole outcome history and
slices it per row by session date — no per-row API calls.

Usage:
    python3 scripts/backfill_benchmark_returns.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trade_simulator.config import AppPaths, load_config  # noqa: E402
from trade_simulator.database import Database  # noqa: E402
from trade_simulator.providers import AlpacaDataClient  # noqa: E402


def main() -> int:
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger("backfill_benchmark")

    paths = AppPaths.from_base_dir(Path(__file__).resolve().parent.parent)
    config = load_config(paths)
    db = Database(paths.database_path)
    db.initialize()  # idempotent; applies the benchmark_return_pct migration
    market = AlpacaDataClient(config, logger)

    with db.connect() as conn:
        rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT trigger_id, entry_time, exit_time, return_pct, hit_target
                FROM trigger_outcomes
                WHERE benchmark_return_pct IS NULL
                """
            ).fetchall()
        ]
    if not rows:
        print("Nothing to backfill.")
        return 0

    entries = [datetime.fromisoformat(r["entry_time"]) for r in rows]
    exits = [datetime.fromisoformat(r["exit_time"]) for r in rows]
    span_start, span_end = min(entries), max(exits) + timedelta(days=1)
    spy = market.fetch_daily_closes("SPY", span_start, span_end)
    spy_by_date = {ts.date(): price for ts, price in spy}
    spy_dates = sorted(spy_by_date)

    def close_on_or_after(day):
        for d in spy_dates:
            if d >= day:
                return spy_by_date[d]
        return None

    def close_on_or_before(day):
        for d in reversed(spy_dates):
            if d <= day:
                return spy_by_date[d]
        return None

    updated = 0
    with db.connect() as conn:
        for row, entry_dt, exit_dt in zip(rows, entries, exits, strict=True):
            first = close_on_or_after(entry_dt.date())
            last = close_on_or_before(exit_dt.date())
            if not first or not last:
                continue
            bench = round(((last - first) / first) * 100, 4)
            conn.execute(
                "UPDATE trigger_outcomes SET benchmark_return_pct = ? WHERE trigger_id = ?",
                (bench, row["trigger_id"]),
            )
            row["bench"] = bench
            updated += 1
    print(f"Backfilled {updated}/{len(rows)} rows.\n")

    scored = [r for r in rows if "bench" in r]
    if scored:
        avg_ret = sum(r["return_pct"] for r in scored) / len(scored)
        avg_bench = sum(r["bench"] for r in scored) / len(scored)
        print(f"Avg trade return:  {avg_ret:+.2f}%")
        print(f"Avg SPY same-window: {avg_bench:+.2f}%")
        print(f"Avg alpha:         {avg_ret - avg_bench:+.2f}% per trade")
    return 0


if __name__ == "__main__":
    sys.exit(main())
