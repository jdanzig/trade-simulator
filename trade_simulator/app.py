from __future__ import annotations

import logging
import signal
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .classifier import ClassificationError, ClassifierService
from .config import AppPaths, ConfigError, load_config
from .dashboard import DashboardServer
from .database import Database, TriggerCandidate
from .embedding import EmbeddingProvider
from .outcomes import OutcomeService, TriggerOutcomeService
from .retrieval import RetrievalService
from .market import EASTERN, MarketClock
from .news import NewsFetcher
from .providers import (
    AlpacaDataClient,
    AnthropicClassifierClient,
    NewsApiClient,
    NtfyClient,
    UniverseProvider,
)
from .simulation import SimulationService


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger("trade_simulator")


def build_default_classifier_prompt() -> str:
    return """# Dip Classifier Instructions

You classify whether an intraday selloff looks like an overreaction suitable for a hypothetical paper-trade entry.

Rules:
- Never recommend a live trade or mention execution.
- Focus on whether the root cause appears temporary, non-fundamental, and reversible.
- The thesis is buying overreactions, so a genuine overreaction should reach `buy_candidate`. Treat sentiment-driven, sector-contagion, or macro/index-wide drops as candidate overreactions and lean `buy_candidate` when the company's own fundamentals look intact.
- Reserve `avoid` for catalysts that LIKELY cause material, lasting impairment to future cash flows — fraud, a guidance cut, regulatory/legal action, a failed product or trial, or a structural demand shock. A merely plausible or speculative cash-flow worry is not enough.
- Use `monitor` only when the cause is genuinely unclear or the evidence is directly conflicting — not merely because breaking news is still developing.
- Only return JSON matching the requested schema.

## Learned Adjustments

Add manual notes here as the system learns. The application reads this file at runtime but will never overwrite it.
"""


class TradeSimulatorApp:
    def __init__(self, base_dir: Path):
        self.paths = AppPaths.from_base_dir(base_dir)
        self.logger = configure_logging()
        self.shutdown_event = threading.Event()
        self.clock = MarketClock()
        self.scheduler = BackgroundScheduler(timezone=EASTERN.key)

        self.paths.data_dir.mkdir(parents=True, exist_ok=True)
        if not self.paths.classifier_prompt_path.exists():
            self.paths.classifier_prompt_path.write_text(build_default_classifier_prompt())

        self.config = load_config(self.paths)
        self.db = Database(self.paths.database_path)
        self.db.initialize()

        self.universe_provider = UniverseProvider(self.logger)
        self.market_data = AlpacaDataClient(self.config, self.logger)
        self.newsapi = NewsApiClient(self.config, self.logger)
        self.anthropic_client = AnthropicClassifierClient(self.config, self.logger)
        self.ntfy = NtfyClient(self.config.ntfy_topic, self.logger)
        self.embedding_provider = EmbeddingProvider(self.config.embedding_model, self.logger)
        self.news_fetcher = NewsFetcher(self.config, self.db, self.logger, self.embedding_provider)
        self.retrieval = RetrievalService(self.db, self.embedding_provider, self.logger)
        self.classifier = ClassifierService(
            self.config,
            self.paths.classifier_prompt_path,
            self.anthropic_client,
            self.logger,
            retrieval_service=self.retrieval,
        )
        self.simulation = SimulationService(self.config, self.db, self.market_data, self.logger, ntfy=self.ntfy)
        self.outcomes = OutcomeService(self.db, self.market_data, self.logger)
        self.trigger_outcomes = TriggerOutcomeService(
            self.db,
            self.market_data,
            self.logger,
            target_return_pct=self.config.target_return_pct,
            max_hold_days=self.config.max_hold_days,
        )
        self.dashboard = DashboardServer(self.db, self.config.dashboard_port)

    def validate_startup(self) -> None:
        self.logger.info("Validating configured providers")
        self.market_data.validate()
        self.newsapi.validate()
        self.anthropic_client.validate()

    def run(self) -> None:
        self.validate_startup()
        self.refresh_universe()
        self.dashboard.start()
        self._register_jobs()
        self._resume_pending_rechecks()
        self.scheduler.start()
        self.logger.info(
            "Trade simulator started. Dashboard available at http://127.0.0.1:%s",
            self.config.dashboard_port,
        )
        # If the previous run never marked a clean shutdown, it crashed (or
        # was killed) — flag that in the startup ping so unattended restarts
        # under launchd are visible.
        crashed = self.db.get_state("clean_shutdown") == "0"
        self.db.set_state("clean_shutdown", "0")
        self.ntfy.notify_startup(
            universe=self.config.universe,
            ticker_count=len(self.db.list_universe(self.config.universe)),
            dashboard_port=self.config.dashboard_port,
            summary=self.db.eod_summary(self.clock.now().date()),
            recovered_from_crash=crashed,
        )
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        self.shutdown_event.wait()

    def _handle_signal(self, signum, frame) -> None:  # noqa: ANN001, ARG002
        self.logger.info("Received signal %s, shutting down.", signum)
        self.db.set_state("clean_shutdown", "1")
        self.scheduler.shutdown(wait=False)
        self.shutdown_event.set()

    def _register_jobs(self) -> None:
        self.scheduler.add_job(
            self._safe_run,
            IntervalTrigger(minutes=self.config.poll_interval_minutes, timezone=EASTERN.key),
            kwargs={"component": "price_monitor", "fn": self.price_monitor_job},
            id="price_monitor",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._safe_run,
            CronTrigger(day_of_week="mon-fri", hour=16, minute=5, timezone=EASTERN.key),
            kwargs={"component": "eod_update", "fn": self.eod_update_job},
            id="eod_update",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._safe_run,
            CronTrigger(day_of_week="sun", hour=17, minute=0, timezone=EASTERN.key),
            kwargs={"component": "refresh_universe", "fn": self.refresh_universe},
            id="refresh_universe",
            replace_existing=True,
        )
        # Run hourly to compute realized outcomes for any news_events whose
        # window has just closed. Cheap if the queue is empty.
        self.scheduler.add_job(
            self._safe_run,
            IntervalTrigger(hours=1, timezone=EASTERN.key),
            kwargs={"component": "compute_outcomes", "fn": self.compute_outcomes_job},
            id="compute_outcomes",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._safe_run,
            CronTrigger(day_of_week="mon-fri", hour=9, minute=0, timezone=EASTERN.key),
            kwargs={"component": "morning_brief", "fn": self.morning_brief_job},
            id="morning_brief",
            replace_existing=True,
        )

    def _resume_pending_rechecks(self) -> None:
        now = self.clock.now()
        for trigger in self.db.list_pending_rechecks():
            run_at = trigger["recheck_scheduled_at"] or now
            if run_at < now:
                run_at = now + timedelta(seconds=5)
            self._schedule_second_pass(trigger["id"], run_at)

    def _safe_run(self, *, component: str, fn) -> None:  # noqa: ANN001
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            self.db.log_error(component, f"{component} job failed", repr(exc))
            self.logger.exception("%s job failed", component)

    def refresh_universe(self) -> None:
        entries = self.universe_provider.fetch(self.config.universe)
        self.db.upsert_universe(entries, self.config.universe, self.clock.now())
        self.db.set_state("last_universe_refresh", self.clock.now().isoformat())
        self.logger.info("Universe refreshed with %s tickers", len(entries))

    def price_monitor_job(self) -> None:
        now = self.clock.now()
        if not self.clock.market_is_open(now):
            return
        session_bounds = self.clock.session_bounds(now.date())
        if session_bounds is None:
            return
        market_open, _ = session_bounds
        universe_rows = self.db.list_universe(self.config.universe)
        tickers = [
            row["ticker"]
            for row in universe_rows
            if row["ticker"] not in set(ticker.upper() for ticker in self.config.blocklist)
        ]
        intraday = self.market_data.fetch_intraday_state(
            tickers,
            session_start=market_open,
            session_end=now,
        )
        # Refresh P&L on open positions and re-classify any pending entries before filling
        price_map = {t: m["current_price"] for t, m in intraday.items() if m.get("current_price")}
        self._process_pending_positions(price_map, now)
        self.simulation.refresh_open_position_prices(price_map, now)
        todays_usage = self.db.get_today_api_usage(now.date())
        # Collect qualifying dips first, then spend the classification budget
        # on the biggest drops — a single morning of small dips shouldn't
        # exhaust the budget before a larger afternoon capitulation is seen.
        candidates = []
        for ticker, metrics in intraday.items():
            current_price = metrics["current_price"]
            intraday_high = metrics["intraday_high"]
            if intraday_high <= 0:
                continue
            drop_pct = round(((intraday_high - current_price) / intraday_high) * 100, 2)
            if drop_pct < self.config.drop_threshold_pct:
                continue
            if self.db.is_in_cooldown(ticker, now):
                continue
            candidates.append((ticker, drop_pct, intraday_high, current_price))
        candidates.sort(key=lambda c: c[1], reverse=True)
        for ticker, drop_pct, intraday_high, current_price in candidates:
            recheck_time = self.clock.recheck_time(now)
            budget_status = "classified"
            if todays_usage + 2 > self.config.daily_api_call_budget:
                budget_status = "budget_exhausted"
            candidate = TriggerCandidate(
                ticker=ticker,
                drop_pct=drop_pct,
                intraday_high=intraday_high,
                trigger_price=current_price,
                triggered_at=now,
                budget_status=budget_status,
                recheck_scheduled_at=recheck_time,
            )
            trigger_id = self.db.create_trigger(candidate)
            self.db.set_cooldown(ticker, now + timedelta(hours=self.config.trigger_cooldown_hours))
            if budget_status == "budget_exhausted":
                self.logger.info("Budget exhausted. Logged trigger for %s without classification", ticker)
                continue
            self.db.increment_today_api_usage(now.date(), 2)
            todays_usage += 2
            first_pass = self._run_first_pass(trigger_id)
            # Early exit: if pass 1 says "avoid" with high confidence, the
            # answer is already decisive — skip pass 2 to save a Claude call.
            if (
                first_pass
                and first_pass.get("recommendation") == "avoid"
                and first_pass.get("confidence") == "high"
            ):
                self.db.increment_today_api_usage(now.date(), -1)  # refund pass 2
                todays_usage -= 1
                self.logger.info(
                    "Skipping pass 2 for %s — pass 1 returned high-confidence avoid", ticker
                )
            else:
                self._schedule_second_pass(trigger_id, recheck_time)
        self.db.set_state("last_successful_monitor_run", now.isoformat())

    def _process_pending_positions(self, prices: dict[str, float], now) -> None:
        """Re-classify each pending position with fresh morning news before filling.

        Matches the existing two-pass philosophy: don't trust an after-hours
        classification once the overnight news cycle has run. If Claude still
        says buy_candidate, fill at the current open price. Otherwise cancel.
        """
        pending = self.db.list_pending_positions()
        if not pending:
            return
        for position in pending:
            ticker = position["ticker"]
            if ticker not in prices:
                continue
            trigger = self.db.get_trigger(position["trigger_id"])
            if not trigger:
                self.db.cancel_pending_position(position["id"], "trigger_missing")
                continue
            news_payload = self.news_fetcher.gather(ticker, trigger["triggered_at"], trigger_id=trigger["id"])
            formatted_context = self.news_fetcher.format_for_classifier(news_payload)
            try:
                third_pass = self.classifier.classify(
                    trigger=trigger,
                    pass_number=3,
                    news_maturity="overnight_settled",
                    news_payload=news_payload,
                    formatted_context=formatted_context,
                    now=now,
                )
            except ClassificationError as exc:
                self.db.log_error("classifier", "pass3_classification_failed", repr(exc))
                self.logger.exception("Pass 3 classification failed for %s", ticker)
                continue
            self.db.save_classification(position["trigger_id"], third_pass)
            self.db.increment_today_api_usage(now.date(), 1)
            if third_pass.get("recommendation") == "buy_candidate":
                self.simulation.fill_pending_position(position["id"], ticker, prices[ticker], now)
            else:
                self.simulation.cancel_pending_position(
                    position["id"],
                    ticker,
                    reason=f"thesis_changed_to_{third_pass.get('recommendation', 'unknown')}",
                    summary=third_pass.get("cause_summary", ""),
                )

    def _run_first_pass(self, trigger_id: str) -> dict | None:
        trigger = self.db.get_trigger(trigger_id)
        if not trigger:
            return None
        news_payload = self.news_fetcher.gather(trigger["ticker"], trigger["triggered_at"], trigger_id=trigger_id)
        formatted_context = self.news_fetcher.format_for_classifier(news_payload)
        try:
            classification = self.classifier.classify(
                trigger=trigger,
                pass_number=1,
                news_maturity="breaking",
                news_payload=news_payload,
                formatted_context=formatted_context,
            )
            self.db.save_classification(trigger_id, classification)
            return classification
        except ClassificationError as exc:
            self.db.log_error("classifier", "classification_failed", repr(exc))
            self.logger.exception("Pass 1 classification failed for %s", trigger["ticker"])
            return None

    def _schedule_second_pass(self, trigger_id: str, run_at: datetime) -> None:
        self.scheduler.add_job(
            self._safe_run,
            DateTrigger(run_date=run_at, timezone=EASTERN.key),
            kwargs={
                "component": f"second_pass_{trigger_id}",
                "fn": lambda trigger_id=trigger_id: self.second_pass_job(trigger_id),
            },
            id=f"second_pass_{trigger_id}",
            replace_existing=True,
        )

    def second_pass_job(self, trigger_id: str) -> None:
        trigger = self.db.get_trigger(trigger_id)
        if not trigger:
            return
        now = self.clock.now()
        news_payload = self.news_fetcher.gather(trigger["ticker"], trigger["triggered_at"], trigger_id=trigger_id)
        formatted_context = self.news_fetcher.format_for_classifier(news_payload)
        try:
            second_pass = self.classifier.classify(
                trigger=trigger,
                pass_number=2,
                news_maturity="settled",
                news_payload=news_payload,
                formatted_context=formatted_context,
                now=now,
            )
        except ClassificationError as exc:
            self.db.log_error("classifier", "classification_failed", repr(exc))
            self.logger.exception("Pass 2 classification failed for %s", trigger["ticker"])
            return

        first_pass = self.db.get_classification(trigger_id, 1)
        finalized = self.classifier.finalize_second_pass(first_pass, second_pass, now)
        self.db.save_classification(trigger_id, finalized)
        self.simulation.maybe_open_position(trigger, finalized, now, market_is_open=self.clock.market_is_open(now))
        self.ntfy.notify_trigger(
            ticker=trigger["ticker"],
            drop_pct=float(trigger["drop_pct"]),
            recommendation=finalized.get("recommendation", ""),
            confidence=finalized.get("confidence", ""),
            summary=finalized.get("cause_summary", ""),
        )

    def morning_brief_job(self) -> None:
        now = self.clock.now()
        today = now.date()
        if not self.clock.is_trading_day(today):
            return
        yesterday_triggers = []
        # Walk back to the previous trading day so Monday reports Friday.
        probe = today - timedelta(days=1)
        for _ in range(7):
            if self.clock.is_trading_day(probe):
                yesterday_triggers = self.db.list_triggers_for_date(probe)
                break
            probe -= timedelta(days=1)
        self.ntfy.notify_morning_brief(
            today.isoformat(),
            open_positions=self.db.list_open_positions(),
            pending_positions=self.db.list_pending_positions(),
            triggers_yesterday=len(yesterday_triggers),
            classified_yesterday=sum(
                1 for t in yesterday_triggers if t["budget_status"] == "classified"
            ),
        )

    def compute_outcomes_job(self) -> None:
        now = self.clock.now()
        n = self.outcomes.compute_outcomes(self.config.outcome_windows, now)
        if n:
            self.logger.info("Computed %s news outcomes", n)

    def eod_update_job(self) -> None:
        now = self.clock.now()
        today = now.date()
        if not self.clock.is_trading_day(today):
            return
        self.simulation.update_positions(today)
        n = self.trigger_outcomes.compute_outcomes(now)
        if n:
            self.logger.info("Finalized %s trigger outcomes", n)
        self.ntfy.notify_eod_summary(today.isoformat(), self.db.eod_summary(today))
        self.logger.info("EOD position update complete for %s", today)


def create_app(base_dir: Path) -> TradeSimulatorApp:
    try:
        return TradeSimulatorApp(base_dir)
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc
