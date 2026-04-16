from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from .config import AppConfig
from .database import Database
from .providers import (
    EdgarClient,
    GoogleNewsClient,
    NewsApiClient,
    RedditClient,
    StocktwitsClient,
    summarize_retail_sentiment,
)


class NewsFetcher:
    def __init__(self, config: AppConfig, db: Database, logger: logging.Logger):
        self.config = config
        self.db = db
        self.logger = logger
        self.google_news = GoogleNewsClient(logger)
        self.newsapi = NewsApiClient(config, logger)
        self.edgar = EdgarClient(config, logger)
        self.stocktwits = StocktwitsClient(logger)
        self.reddit = RedditClient(config, logger)

    def gather(self, ticker: str, triggered_at: datetime) -> dict[str, Any]:
        tier1: list[dict[str, str]] = []
        tier2: list[dict[str, str]] = []
        sources_used: list[str] = []
        for source_name, fetcher, bucket in (
            ("google_news", self.google_news.fetch, tier1),
            ("newsapi", self.newsapi.fetch, tier1),
            ("sec_edgar", self.edgar.fetch, tier1),
            ("stocktwits", self.stocktwits.fetch, tier2),
            ("reddit", self.reddit.fetch, tier2),
        ):
            try:
                items = fetcher(ticker)
            except Exception as exc:  # noqa: BLE001
                self.db.log_error("news_fetcher", f"{source_name} failed for {ticker}", repr(exc))
                self.logger.exception("%s failed for %s", source_name, ticker)
                items = []
            if items:
                sources_used.append(source_name)
                bucket.extend(items)

        smart_money_signal = "unavailable"
        if self.config.unusual_whales_enabled:
            self.db.log_error(
                "news_fetcher",
                "Unusual Whales is enabled but the provider integration is not implemented.",
                None,
            )

        return {
            "ticker": ticker,
            "triggered_at": triggered_at.isoformat(),
            "tier1": tier1,
            "tier2": tier2,
            "sources_used": sources_used,
            "retail_sentiment_hint": summarize_retail_sentiment(tier2),
            "smart_money_signal": smart_money_signal,
        }

    @staticmethod
    def format_for_classifier(payload: dict[str, Any]) -> str:
        lines = [
            f"Ticker: {payload['ticker']}",
            f"Triggered at: {payload['triggered_at']}",
            f"Sources used: {', '.join(payload['sources_used']) if payload['sources_used'] else 'none'}",
            f"Retail sentiment hint: {payload['retail_sentiment_hint']}",
            f"Smart money signal: {payload['smart_money_signal']}",
            "",
            "Tier 1 news and filings:",
        ]
        for item in payload["tier1"]:
            lines.append(
                f"- [{item.get('source', 'unknown')}] {item.get('published_at', '')}: "
                f"{item.get('title', '')} {item.get('description', '')}".strip()
            )
        if not payload["tier1"]:
            lines.append("- None")
        lines.append("")
        lines.append("Tier 2 sentiment:")
        for item in payload["tier2"]:
            text = item.get("title") or item.get("body") or ""
            lines.append(
                f"- [{item.get('source', 'unknown')}] {item.get('published_at', '')}: "
                f"{text[:400]}".strip()
            )
        if not payload["tier2"]:
            lines.append("- None")
        return "\n".join(lines)
