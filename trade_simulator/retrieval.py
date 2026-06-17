from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from .database import Database
from .embedding import EmbeddingProvider

# Cap on how much current-event text we embed as the retrieval query, and
# how much of each neighbor's headline we show back in the prompt.
_QUERY_TEXT_CHARS = 1500
_NEIGHBOR_TITLE_CHARS = 140


def build_query_text(news_payload: dict[str, Any]) -> str:
    """Collapse the current trigger's news items into a single query string.

    The corpus embeds one vector per item; the query is one vector over the
    whole current situation, so we concatenate item headlines (plus a short
    body) the same way the corpus text was built, capped to keep the query
    focused on the lede.
    """
    parts: list[str] = []
    for item in [*news_payload.get("tier1", []), *news_payload.get("tier2", [])]:
        title = (item.get("title") or "").strip()
        body = (item.get("description") or item.get("body") or "").strip()
        text = f"{title} {body[:200]}".strip()
        if text:
            parts.append(text)
    return " | ".join(parts)[:_QUERY_TEXT_CHARS]


def format_precedent_block(neighbors: list[dict[str, Any]]) -> str:
    """Render retrieved neighbors as a prompt section: each headline plus
    the stock's realized move over each matured window."""
    lines: list[str] = []
    for n in neighbors:
        title = (n.get("title") or "(no headline)").strip()[:_NEIGHBOR_TITLE_CHARS]
        moves = ", ".join(
            f"{o['window_label']}: {float(o['return_pct']):+.1f}%"
            for o in sorted(n.get("outcomes", []), key=lambda o: o["window_seconds"])
        )
        lines.append(f"- [{n['ticker']}] \"{title}\" -> {moves or 'n/a'}")
    return "\n".join(lines)


class RetrievalService:
    """Finds the most similar past news events to a query, with their
    realized outcomes, enforcing the no-time-leakage rule.

    Cross-ticker retrieval is allowed (a "China export ban" overreaction on
    one chipmaker is informative about another), but same-ticker neighbors
    get a rank boost since a stock's own history is usually most relevant.
    The boost is a multiplicative discount on the vector distance — lower
    distance ranks higher, so same-ticker matches are pulled toward the top
    without excluding cross-ticker ones.
    """

    SAME_TICKER_DISTANCE_FACTOR = 0.85

    def __init__(
        self,
        db: Database,
        embedding_provider: EmbeddingProvider,
        logger: logging.Logger,
    ):
        self.db = db
        self.embedding_provider = embedding_provider
        self.logger = logger

    def find_neighbors(
        self,
        *,
        query_text: str,
        query_timestamp: datetime,
        ticker: str,
        k: int = 5,
        exclude_trigger_id: str | None = None,
        overfetch: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return up to k prior similar events (each with an 'outcomes' list).

        Over-fetches from the vector index because the leakage + mature-
        outcome filters drop many raw KNN hits, especially early on when
        few outcomes have matured.
        """
        if not query_text.strip():
            return []
        if overfetch is None:
            overfetch = max(k * 40, 200)
        try:
            query_embedding = self.embedding_provider.embed(query_text)
        except Exception as exc:  # noqa: BLE001
            self.db.log_error("retrieval", "embed failed", repr(exc))
            self.logger.exception("Retrieval embedding failed for %s", ticker)
            return []
        candidates = self.db.knn_news_neighbors(
            query_embedding=query_embedding,
            query_timestamp=query_timestamp,
            overfetch=overfetch,
            exclude_trigger_id=exclude_trigger_id,
        )
        for candidate in candidates:
            factor = self.SAME_TICKER_DISTANCE_FACTOR if candidate["ticker"] == ticker else 1.0
            candidate["score"] = candidate["distance"] * factor
        candidates.sort(key=lambda c: c["score"])
        return candidates[:k]
