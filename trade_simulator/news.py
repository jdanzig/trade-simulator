from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from .config import AppConfig
from .database import Database
from .embedding import EmbeddingProvider
from .providers import (
    EdgarClient,
    GoogleNewsClient,
    NewsApiClient,
    RedditClient,
    StocktwitsClient,
    summarize_retail_sentiment,
)

# Cap body text fed to the embedder. Long bodies dilute the title signal
# and slow encoding; first 500 chars usually captures the lede.
EMBED_BODY_CHARS = 500


class NewsFetcher:
    def __init__(
        self,
        config: AppConfig,
        db: Database,
        logger: logging.Logger,
        embedding_provider: EmbeddingProvider | None = None,
    ):
        self.config = config
        self.db = db
        self.logger = logger
        self.embedding_provider = embedding_provider
        self.google_news = GoogleNewsClient(logger)
        self.newsapi = NewsApiClient(config, logger)
        self.edgar = EdgarClient(config, logger)
        self.stocktwits = StocktwitsClient(logger)
        self.reddit = RedditClient(config, logger)

    def gather(self, ticker: str, triggered_at: datetime, trigger_id: str | None = None) -> dict[str, Any]:
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

        if trigger_id and self.embedding_provider is not None:
            self._persist_with_embeddings(
                trigger_id=trigger_id,
                ticker=ticker,
                triggered_at=triggered_at,
                tier1=tier1,
                tier2=tier2,
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
    def _item_title_body(item: dict[str, str]) -> tuple[str, str]:
        title = (item.get("title") or "").strip()
        body = (item.get("description") or item.get("body") or "").strip()
        return title, body

    def _persist_with_embeddings(
        self,
        *,
        trigger_id: str,
        ticker: str,
        triggered_at: datetime,
        tier1: list[dict[str, str]],
        tier2: list[dict[str, str]],
    ) -> None:
        """Insert each fetched item into news_events + its embedding.

        Failures here are non-fatal — classification must continue even if
        we can't build the corpus. Duplicates (same trigger+source+text)
        are silently ignored by the UNIQUE constraint.
        """
        try:
            items: list[tuple[dict[str, str], int]] = [(i, 1) for i in tier1] + [(i, 2) for i in tier2]
            texts: list[str] = []
            payloads: list[tuple[dict[str, str], int, str, str]] = []
            for item, tier in items:
                title, body = self._item_title_body(item)
                if not title and not body:
                    continue
                texts.append(f"{title} {body[:EMBED_BODY_CHARS]}".strip())
                payloads.append((item, tier, title, body))
            if not payloads:
                return
            vectors = self.embedding_provider.embed_batch(texts)
            for (item, tier, title, body), vec in zip(payloads, vectors, strict=True):
                self.db.insert_news_event(
                    trigger_id=trigger_id,
                    ticker=ticker,
                    source=item.get("source", "unknown"),
                    tier=tier,
                    published_at=item.get("published_at") or None,
                    fetched_at=triggered_at,
                    title=title,
                    body=body,
                    embedding=vec,
                )
        except Exception as exc:  # noqa: BLE001
            self.db.log_error("news_persistence", f"persist failed for {ticker}", repr(exc))
            self.logger.exception("News persistence failed for %s", ticker)

    # Caps tuned to keep input context lean. News sources are highly
    # redundant — the same story shows up across NewsAPI, Reddit, etc. —
    # so capping totals + truncating bodies removes duplication, not signal.
    TIER1_MAX_ITEMS = 8
    TIER2_MAX_ITEMS = 6
    TIER1_TEXT_CHARS = 200
    TIER2_TEXT_CHARS = 200

    @classmethod
    def format_for_classifier(cls, payload: dict[str, Any]) -> str:
        lines = [
            f"Ticker: {payload['ticker']}",
            f"Triggered at: {payload['triggered_at']}",
            f"Sources used: {', '.join(payload['sources_used']) if payload['sources_used'] else 'none'}",
            f"Retail sentiment hint: {payload['retail_sentiment_hint']}",
            f"Smart money signal: {payload['smart_money_signal']}",
            "",
            "Tier 1 news and filings:",
        ]
        for item in payload["tier1"][: cls.TIER1_MAX_ITEMS]:
            text = f"{item.get('title', '')} {item.get('description', '')}".strip()
            lines.append(
                f"- [{item.get('source', 'unknown')}] {item.get('published_at', '')}: "
                f"{text[: cls.TIER1_TEXT_CHARS]}".strip()
            )
        if not payload["tier1"]:
            lines.append("- None")
        lines.append("")
        lines.append("Tier 2 sentiment:")
        for item in payload["tier2"][: cls.TIER2_MAX_ITEMS]:
            text = item.get("title") or item.get("body") or ""
            lines.append(
                f"- [{item.get('source', 'unknown')}] {item.get('published_at', '')}: "
                f"{text[: cls.TIER2_TEXT_CHARS]}".strip()
            )
        if not payload["tier2"]:
            lines.append("- None")
        return "\n".join(lines)
