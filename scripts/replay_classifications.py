"""Phase 5: retrieval A/B validation harness.

Replays past triggers through the classifier twice on identical
reconstructed input — retrieval OFF vs ON — and reports whether the
historical-analog prompt changes recommendations and how each path's
buy decisions line up with realized outcomes (the trigger_outcomes
scorecard).

Leakage safety: each trigger is replayed as-of its own decision
timestamp (recheck_scheduled_at), so retrieval can only ever see
neighbors that were ingested and matured before that moment — the exact
same rule the live path enforces. Early triggers therefore cold-start
(no qualifying neighbors yet) and are reported separately.

Cost: each trigger with active retrieval costs 2 Claude calls (off + on);
cold-start triggers cost 1 (the paths are identical, so the on-call is
skipped). Use --limit to bound spend.

Usage:
    python3 scripts/replay_classifications.py [--limit N] [--k K]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trade_simulator.classifier import ClassifierService  # noqa: E402
from trade_simulator.config import AppPaths, load_config  # noqa: E402
from trade_simulator.database import Database  # noqa: E402
from trade_simulator.embedding import EmbeddingProvider  # noqa: E402
from trade_simulator.news import NewsFetcher  # noqa: E402
from trade_simulator.providers import AnthropicClassifierClient  # noqa: E402
from trade_simulator.retrieval import RetrievalService, build_query_text  # noqa: E402


def reconstruct_payload(trigger: dict, news_rows: list[dict]) -> dict:
    """Rebuild the news_payload a trigger was classified on, from the
    persisted news_events. Sentiment/smart-money hints weren't persisted
    and don't affect the A/B (identical across both paths), so they're set
    to neutral placeholders."""
    tier1, tier2, sources = [], [], set()
    for r in news_rows:
        sources.add(r["source"])
        item = {"title": r["title"] or "", "source": r["source"], "published_at": r["published_at"] or ""}
        if r["tier"] == 1:
            item["description"] = r["body"] or ""
            tier1.append(item)
        else:
            item["body"] = r["body"] or ""
            tier2.append(item)
    return {
        "ticker": trigger["ticker"],
        "triggered_at": trigger["triggered_at"].isoformat(),
        "tier1": tier1,
        "tier2": tier2,
        "sources_used": sorted(sources),
        "retail_sentiment_hint": "unavailable",
        "smart_money_signal": "unavailable",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=25, help="Max triggers to replay (0 = all).")
    parser.add_argument("--k", type=int, default=None, help="Override news_retrieval_k for the test.")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute retrieval active/cold counts only — no Claude calls, no cost.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("replay")

    paths = AppPaths.from_base_dir(Path(__file__).resolve().parent.parent)
    config = load_config(paths)
    if args.k is not None:
        config.news_retrieval_k = args.k
    k = config.news_retrieval_k

    db = Database(paths.database_path)
    embedder = EmbeddingProvider(config.embedding_model, logger)
    retrieval = RetrievalService(db, embedder, logger)
    classifier = None
    if not args.dry_run:
        anthropic = AnthropicClassifierClient(config, logger)
        classifier = ClassifierService(
            config, paths.classifier_prompt_path, anthropic, logger, retrieval_service=retrieval
        )

    triggers = db.list_scored_triggers(limit=args.limit or None)
    mode = "DRY RUN (no Claude calls)" if args.dry_run else "replaying"
    print(f"{mode}: {len(triggers)} scored triggers (k={k})...\n")

    rows = []
    for i, trig in enumerate(triggers, 1):
        news_rows = db.list_news_events_for_trigger(trig["id"])
        if not news_rows:
            continue  # no persisted news to replay (pre-Phase-1 trigger)
        payload = reconstruct_payload(trig, news_rows)
        formatted = NewsFetcher.format_for_classifier(payload)
        decision_time = trig["recheck_scheduled_at"] or trig["triggered_at"]

        # Is retrieval active for this trigger as-of its decision time?
        neighbors = retrieval.find_neighbors(
            query_text=build_query_text(payload),
            query_timestamp=decision_time,
            ticker=trig["ticker"],
            k=k,
            exclude_trigger_id=trig["id"],
        )
        active = len(neighbors) >= k

        if args.dry_run:
            rows.append({"ticker": trig["ticker"], "hit": int(trig["hit_target"]),
                         "active": active, "off": "", "on": "", "neighbors": len(neighbors)})
            print(f"  [{i}/{len(triggers)}] {trig['ticker']:6} hit={int(trig['hit_target'])} "
                  f"neighbors={len(neighbors):3} {'(active)' if active else '(cold)'}")
            continue

        def run(flag: bool) -> str:
            classifier.config.news_retrieval_enabled = flag
            res = classifier.classify(
                trigger=trig, pass_number=2, news_maturity="settled",
                news_payload=payload, formatted_context=formatted, now=decision_time,
            )
            return res.get("recommendation", "")

        rec_off = run(False)
        rec_on = run(True) if active else rec_off  # cold path is identical, skip the call

        rows.append({
            "ticker": trig["ticker"], "hit": int(trig["hit_target"]),
            "active": active, "off": rec_off, "on": rec_on,
        })
        flag = "*" if (active and rec_on != rec_off) else " "
        print(f"  [{i}/{len(triggers)}] {flag} {trig['ticker']:6} hit={int(trig['hit_target'])} "
              f"off={rec_off:13} on={rec_on:13} {'(active)' if active else '(cold)'}")

    if args.dry_run:
        active = sum(1 for r in rows if r["active"])
        active_winners = sum(1 for r in rows if r["active"] and r["hit"])
        print("\n" + "=" * 64)
        print(f"Replayable (have persisted news): {len(rows)}")
        print(f"Retrieval would be ACTIVE: {active}  (of which winners: {active_winners})")
        print(f"Cold-start: {len(rows) - active}")
        est_calls = (len(rows) - active) * 1 + active * 2
        print(f"\nA real run would cost ~{est_calls} Claude calls.")
        if active < k:
            print("Too few active triggers to validate meaningfully yet — let the corpus mature.")
        print("=" * 64)
        return 0

    _report(rows, k)
    return 0


def _confusion(rows, key):
    """TP/FP/FN/TN treating recommendation buy_candidate as a buy."""
    tp = fp = fn = tn = 0
    for r in rows:
        buy = r[key] == "buy_candidate"
        if buy and r["hit"]:
            tp += 1
        elif buy and not r["hit"]:
            fp += 1
        elif not buy and r["hit"]:
            fn += 1
        else:
            tn += 1
    return tp, fp, fn, tn


def _report(rows, k):
    print("\n" + "=" * 64)
    if not rows:
        print("No replayable triggers (need persisted news + outcome).")
        return
    active = [r for r in rows if r["active"]]
    changed = [r for r in active if r["on"] != r["off"]]
    print(f"Replayed: {len(rows)}  |  retrieval active: {len(active)}  |  cold-start: {len(rows)-len(active)}")
    print(f"Label changes (active only): {len(changed)} of {len(active)}")
    toward_buy = sum(1 for r in changed if r["on"] == "buy_candidate" and r["off"] != "buy_candidate")
    away_buy = sum(1 for r in changed if r["off"] == "buy_candidate" and r["on"] != "buy_candidate")
    print(f"  toward buy: {toward_buy}   away from buy: {away_buy}   other: {len(changed)-toward_buy-away_buy}")

    print("\nOutcome alignment (buy = buy_candidate):")
    off = _confusion(rows, "off")
    on = _confusion(rows, "on")
    print(f"  {'':9}{'caught':>8}{'bad buys':>10}{'missed':>8}")
    print(f"  flag OFF {off[0]:>8}{off[1]:>10}{off[2]:>8}")
    print(f"  flag ON  {on[0]:>8}{on[1]:>10}{on[2]:>8}")
    print(f"  delta    {on[0]-off[0]:>+8}{on[1]-off[1]:>+10}{on[2]-off[2]:>+8}")
    print("=" * 64)
    if len(active) == 0:
        print("\nNote: retrieval was cold for every trigger — corpus too thin")
        print("at these triggers' decision times. Re-run later as it matures.")


if __name__ == "__main__":
    sys.exit(main())
