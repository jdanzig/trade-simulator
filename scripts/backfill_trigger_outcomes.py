"""Compute counterfactual outcomes for triggers (the classifier scorecard).

By default fills outcomes for triggers that don't have one yet — same as
the daily eod job, just on demand. With --recompute it first deletes all
existing outcomes, so a change to the outcome logic (e.g. close-to-close
entry pricing) can be reapplied to the entire history.

Usage:
    python3 scripts/backfill_trigger_outcomes.py [--recompute]
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trade_simulator.config import AppPaths, load_config  # noqa: E402
from trade_simulator.database import Database  # noqa: E402
from trade_simulator.market import EASTERN  # noqa: E402
from trade_simulator.outcomes import TriggerOutcomeService  # noqa: E402
from trade_simulator.providers import AlpacaDataClient  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--recompute",
        action="store_true",
        help="Delete all existing trigger outcomes first, then recompute from scratch.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("backfill_trigger_outcomes")

    paths = AppPaths.from_base_dir(Path(__file__).resolve().parent.parent)
    config = load_config(paths)
    db = Database(paths.database_path)
    market = AlpacaDataClient(config, logger)
    svc = TriggerOutcomeService(
        db,
        market,
        logger,
        target_return_pct=config.target_return_pct,
        max_hold_days=config.max_hold_days,
    )

    if args.recompute:
        removed = db.clear_trigger_outcomes()
        logger.info("Cleared %s existing trigger outcomes", removed)

    now = datetime.now(EASTERN)
    n = svc.compute_outcomes(now)
    logger.info("Finalized %s trigger outcomes.", n)
    logger.info("Scorecard: %s", db.classifier_scorecard())
    return 0


if __name__ == "__main__":
    sys.exit(main())
