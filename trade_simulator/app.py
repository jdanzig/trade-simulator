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
from .market import EASTERN, MarketClock
from .news import NewsFetcher
from .providers import (
    AlpacaDataClient,
    AnthropicClassifierClient,
    GmailClient,
    NewsApiClient,
    UniverseProvider,
)
from .reporting import ReportingService
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
- Prefer `avoid` when the catalyst plausibly impairs future cash flows.
- Prefer `monitor` when facts are incomplete or conflicting.
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
        if not self.paths.findings_path.exists():
            self.paths.findings_path.write_text("# Weekly Findings\n")

        self.config = load_config(self.paths)
        self.db = Database(self.paths.database_path)
        self.db.initialize()

        self.universe_provider = UniverseProvider(self.logger)
        self.market_data = AlpacaDataClient(self.config, self.logger)
        self.newsapi = NewsApiClient(self.config, self.logger)
        self.anthropic_client = AnthropicClassifierClient(self.config, self.logger)
        self.gmail = GmailClient(self.config, self.logger)
        self.news_fetcher = NewsFetcher(self.config, self.db, self.logger)
        self.classifier = ClassifierService(
            self.config,
            self.paths.classifier_prompt_path,
            self.anthropic_client,
            self.logger,
        )
        self.simulation = SimulationService(self.config, self.db, self.market_data, self.logger)
        self.reporting = ReportingService(
            self.db,
            self.gmail,
            self.paths.classifier_prompt_path,
            self.paths.findings_path,
        )
        self.dashboard = DashboardServer(self.db, self.config.dashboard_port)

    def validate_startup(self) -> None:
        self.logger.info("Validating configured providers")
        self.market_data.validate()
        self.newsapi.validate()
        self.anthropic_client.validate()
        self.gmail.validate()

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
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        self.shutdown_event.wait()

    def _handle_signal(self, signum, frame) -> None:  # noqa: ANN001, ARG002
        self.logger.info("Received signal %s, shutting down.", signum)
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
            kwargs={"component": "daily_report", "fn": self.daily_report_job},
            id="daily_report",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._safe_run,
            CronTrigger(day_of_week="sun", hour=17, minute=0, timezone=EASTERN.key),
            kwargs={"component": "weekly_findings", "fn": self.weekly_findings_job},
            id="weekly_findings",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._safe_run,
            CronTrigger(day_of_week="sun", hour=17, minute=0, timezone=EASTERN.key),
            kwargs={"component": "refresh_universe", "fn": self.refresh_universe},
            id="refresh_universe",
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
        todays_usage = self.db.get_today_api_usage(now.date())
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
            self._run_first_pass(trigger_id)
            self._schedule_second_pass(trigger_id, recheck_time)
        self.db.set_state("last_successful_monitor_run", now.isoformat())

    def _run_first_pass(self, trigger_id: str) -> None:
        trigger = self.db.get_trigger(trigger_id)
        if not trigger:
            return
        news_payload = self.news_fetcher.gather(trigger["ticker"], trigger["triggered_at"])
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
        except ClassificationError as exc:
            self.db.log_error("classifier", "classification_failed", repr(exc))
            self.logger.exception("Pass 1 classification failed for %s", trigger["ticker"])

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
        news_payload = self.news_fetcher.gather(trigger["ticker"], trigger["triggered_at"])
        formatted_context = self.news_fetcher.format_for_classifier(news_payload)
        try:
            second_pass = self.classifier.classify(
                trigger=trigger,
                pass_number=2,
                news_maturity="settled",
                news_payload=news_payload,
                formatted_context=formatted_context,
            )
        except ClassificationError as exc:
            self.db.log_error("classifier", "classification_failed", repr(exc))
            self.logger.exception("Pass 2 classification failed for %s", trigger["ticker"])
            return

        first_pass = self.db.get_classification(trigger_id, 1)
        finalized = self.classifier.finalize_second_pass(first_pass, second_pass, now)
        self.db.save_classification(trigger_id, finalized)
        self.simulation.maybe_open_position(trigger, finalized, now)

    def daily_report_job(self) -> None:
        today = self.clock.now().date()
        if not self.clock.is_trading_day(today):
            return
        self.simulation.update_positions(today)
        self.reporting.send_daily_report(today)
        self.db.set_state("last_daily_report_date", today.isoformat())
        self.logger.info("Daily report sent for %s", today)

    def weekly_findings_job(self) -> None:
        today = self.clock.now().date()
        findings_block = self.reporting.append_weekly_findings(today)
        self.reporting.send_weekly_findings_email(today, findings_block)
        self.db.set_state("last_findings_run", today.isoformat())
        self.logger.info("Weekly findings written and emailed for %s", today)


def create_app(base_dir: Path) -> TradeSimulatorApp:
    try:
        return TradeSimulatorApp(base_dir)
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc
