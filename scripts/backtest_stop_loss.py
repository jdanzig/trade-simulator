"""Backtest stop-loss levels against realized trigger outcomes.

For every finalized row in trigger_outcomes, refetches the daily closes
over the actual holding window and replays the strategy's exit rules with
a stop-loss added: first daily close at/below the stop exits the trade,
first close at/above the target wins — whichever comes first. Reports,
per stop level, how many eventual winners the stop would have killed vs
how many losers it cut short, and the net effect on average return.

Read-only: touches no live behavior, costs no Claude calls (market data
only, paced for Alpaca's free tier).

Usage:
    python3 scripts/backtest_stop_loss.py [--levels -8,-10,-12,-15]
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trade_simulator.config import AppPaths, load_config  # noqa: E402
from trade_simulator.database import Database  # noqa: E402
from trade_simulator.providers import AlpacaDataClient  # noqa: E402


def simulate(holding: list[float], entry: float, target_pct: float, stop_pct: float) -> tuple[str, float]:
    """Walk daily closes; return (exit_reason, return_pct) with a stop applied."""
    ret = 0.0
    for price in holding:
        ret = ((price - entry) / entry) * 100
        if ret >= target_pct:
            return "target", round(ret, 2)
        if ret <= stop_pct:
            return "stopped", round(ret, 2)
    return "max_hold", round(ret, 2)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--levels", default="-8,-10,-12,-15", help="Comma-separated stop levels in percent.")
    args = parser.parse_args()
    levels = [float(x) for x in args.levels.split(",")]

    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger("backtest_stop")

    paths = AppPaths.from_base_dir(Path(__file__).resolve().parent.parent)
    config = load_config(paths)
    db = Database(paths.database_path)
    market = AlpacaDataClient(config, logger)
    target = config.target_return_pct

    with db.connect() as conn:
        rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT o.trigger_id, t.ticker, o.entry_time, o.entry_price,
                       o.exit_time, o.return_pct, o.hit_target
                FROM trigger_outcomes o JOIN triggers t ON t.id = o.trigger_id
                ORDER BY o.entry_time
                """
            ).fetchall()
        ]
    print(f"Backtesting {len(rows)} resolved triggers, stops {levels}, target +{target}%...\n")

    paths_data = []
    for i, row in enumerate(rows, 1):
        entry_time = datetime.fromisoformat(row["entry_time"])
        exit_time = datetime.fromisoformat(row["exit_time"])
        try:
            closes = market.fetch_daily_closes(
                row["ticker"], entry_time, exit_time + timedelta(days=1)
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  skip {row['ticker']}: {exc}")
            continue
        if len(closes) < 2:
            continue
        holding = [price for _, price in closes[1:]]
        paths_data.append((row, holding))
        if i % 25 == 0:
            print(f"  fetched {i}/{len(rows)}...")
        time.sleep(0.4)

    n = len(paths_data)
    if not n:
        print("No price paths available.")
        return 1
    baseline_avg = sum(r["return_pct"] for r, _ in paths_data) / n
    print(f"\nPaths replayed: {n}   baseline avg return: {baseline_avg:+.2f}%\n")
    print(f"{'stop':>6} {'avg ret':>9} {'delta':>8} {'winners killed':>15} {'losers cut':>11}")
    for stop in sorted(levels, reverse=True):
        total = 0.0
        killed = cut = 0
        for row, holding in paths_data:
            reason, ret = simulate(holding, float(row["entry_price"]), target, stop)
            total += ret
            if reason == "stopped":
                if row["hit_target"]:
                    killed += 1  # would have recovered to target; stop ate it
                else:
                    cut += 1  # genuine loser exited early
        avg = total / n
        print(f"{stop:>5.0f}% {avg:>8.2f}% {avg - baseline_avg:>+7.2f}% {killed:>15} {cut:>11}")
    print(
        "\nwinners killed = trades that hit +{:.0f}% in reality but the stop fired first\n"
        "losers cut     = trades that never hit target; stop exited them early".format(target)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
