"""One-shot CLI: compute realized outcomes for every news_event whose
window has already closed but doesn't have a row in news_outcomes yet.

Same logic as the hourly compute_outcomes_job in the daemon — this just
lets you catch up the whole corpus at once after enabling Phase 2 or
after adding a new window to outcome_windows.

Usage:
    python3 scripts/backfill_news_outcomes.py [--windows 1h,4h,1d]
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Make the daemon package importable when running as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trade_simulator.config import AppPaths, load_config  # noqa: E402
from trade_simulator.database import Database  # noqa: E402
from trade_simulator.market import EASTERN  # noqa: E402
from trade_simulator.outcomes import OutcomeService  # noqa: E402
from trade_simulator.providers import AlpacaDataClient  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--windows",
        default=None,
        help="Comma-separated window labels (e.g. 1h,4h,1d). Defaults to config.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("backfill_outcomes")

    paths = AppPaths.from_base_dir(Path(__file__).resolve().parent.parent)
    config = load_config(paths)
    db = Database(paths.database_path)
    market = AlpacaDataClient(config, logger)
    outcomes = OutcomeService(db, market, logger)

    windows = args.windows.split(",") if args.windows else config.outcome_windows
    now = datetime.now(EASTERN)
    logger.info("Backfilling outcomes for windows: %s", windows)
    n = outcomes.compute_outcomes(windows, now)
    logger.info("Done. Inserted %s outcome rows.", n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
