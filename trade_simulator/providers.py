from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from io import StringIO
from typing import Any

import pandas as pd
import requests
from anthropic import Anthropic
from requests import HTTPError

from .config import AppConfig
from .market import EASTERN
from .utils import chunked, with_retry


def _raise_for_status(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except HTTPError as exc:
        body = response.text[:500]
        raise HTTPError(
            f"{exc} — response body: {body!r}",
            response=response,
        ) from exc


class UniverseProvider:
    SOURCES = {
        "sp500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "nasdaq100": "https://en.wikipedia.org/wiki/Nasdaq-100",
    }

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "trade-simulator/1.0"})

    def fetch(self, universe: str) -> list[dict[str, str]]:
        if universe == "both":
            seen: set[str] = set()
            combined: list[dict[str, str]] = []
            for u in ("sp500", "nasdaq100"):
                for entry in self.fetch(u):
                    if entry["ticker"] not in seen:
                        seen.add(entry["ticker"])
                        combined.append(entry)
            return combined
        if universe not in self.SOURCES:
            raise ValueError(f"Unsupported universe: {universe}")
        response = with_retry(
            lambda: self.session.get(self.SOURCES[universe], timeout=30),
            component="universe_fetch",
            logger=self.logger,
        )
        _raise_for_status(response)
        tables = pd.read_html(StringIO(response.text))
        if universe == "sp500":
            table = tables[0]
            sectors = table["GICS Sector"] if "GICS Sector" in table.columns else [""] * len(table)
            return [
                {
                    "ticker": ticker,
                    "company_name": security,
                    "sector": str(sector) if pd.notna(sector) else "",
                }
                for ticker, security, sector in zip(
                    table["Symbol"], table["Security"], sectors, strict=True
                )
            ]
        for table in tables:
            if {"Ticker", "Company"}.issubset(set(table.columns)):
                sectors = table["GICS Sector"] if "GICS Sector" in table.columns else [""] * len(table)
                return [
                    {
                        "ticker": ticker,
                        "company_name": company,
                        "sector": str(sector) if pd.notna(sector) else "",
                    }
                    for ticker, company, sector in zip(
                        table["Ticker"], table["Company"], sectors, strict=True
                    )
                ]
        raise RuntimeError("Unable to parse Nasdaq-100 constituents from source page.")


class AlpacaDataClient:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.logger = logger
        self.base_url = "https://data.alpaca.markets/v2/stocks"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "APCA-API-KEY-ID": config.alpaca_api_key,
                "APCA-API-SECRET-KEY": config.alpaca_secret_key,
            }
        )

    def validate(self) -> None:
        response = self.session.get(
            f"{self.base_url}/bars/latest",
            params={"symbols": "AAPL", "feed": "iex"},
            timeout=30,
        )
        _raise_for_status(response)

    def fetch_intraday_state(
        self,
        tickers: list[str],
        *,
        session_start: datetime,
        session_end: datetime,
    ) -> dict[str, dict[str, float]]:
        start = session_start.astimezone(EASTERN).isoformat()
        end = session_end.astimezone(EASTERN).isoformat()
        results: dict[str, dict[str, float]] = {}
        for batch in chunked(tickers, 100):
            response = with_retry(
                lambda batch=batch: self.session.get(
                    f"{self.base_url}/bars",
                    params={
                        "symbols": ",".join(batch),
                        "timeframe": "1Min",
                        "start": start,
                        "end": end,
                        "adjustment": "all",
                        "feed": "iex",
                        "limit": 10000,
                    },
                    timeout=60,
                ),
                component="alpaca_intraday",
                logger=self.logger,
            )
            _raise_for_status(response)
            bars = response.json().get("bars", {})
            for ticker, ticker_bars in bars.items():
                if not ticker_bars:
                    continue
                intraday_high = max(float(bar["h"]) for bar in ticker_bars)
                current_price = float(ticker_bars[-1]["c"])
                results[ticker] = {
                    "intraday_high": intraday_high,
                    "current_price": current_price,
                }
        return results

    def fetch_latest_price(self, ticker: str) -> float:
        response = with_retry(
            lambda: self.session.get(
                f"{self.base_url}/bars/latest",
                params={"symbols": ticker, "feed": "iex"},
                timeout=30,
            ),
            component="alpaca_latest_price",
            logger=self.logger,
        )
        _raise_for_status(response)
        payload = response.json().get("bars", {})
        if ticker not in payload:
            raise RuntimeError(f"No latest price returned for {ticker}")
        return float(payload[ticker]["c"])

    def fetch_price_at(
        self,
        ticker: str,
        target_time: datetime,
        *,
        search_hours: int = 96,
    ) -> float | None:
        """Return the first trade close price at or after target_time.

        If target_time falls outside market hours (overnight, weekend,
        holiday), this rolls forward to the next available bar. Returns
        None if no bar can be found within search_hours.
        """
        start = target_time.astimezone(EASTERN)
        end = start + timedelta(hours=search_hours)
        response = with_retry(
            lambda: self.session.get(
                f"{self.base_url}/{ticker}/bars",
                params={
                    "timeframe": "1Min",
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "adjustment": "all",
                    "feed": "iex",
                    "limit": 1,
                    "sort": "asc",
                },
                timeout=30,
            ),
            component="alpaca_price_at",
            logger=self.logger,
        )
        _raise_for_status(response)
        bars = response.json().get("bars", [])
        if not bars:
            return None
        return float(bars[0]["c"])

    def fetch_daily_closes(
        self, ticker: str, start: datetime, end: datetime
    ) -> list[tuple[datetime, float]]:
        """Daily (timestamp, close) bars for one ticker over [start, end],
        ascending. Used to walk a counterfactual position forward through
        the strategy's daily-close exit rules.
        """
        response = with_retry(
            lambda: self.session.get(
                f"{self.base_url}/{ticker}/bars",
                params={
                    "timeframe": "1Day",
                    "start": start.astimezone(EASTERN).isoformat(),
                    "end": end.astimezone(EASTERN).isoformat(),
                    "adjustment": "all",
                    "feed": "iex",
                    "limit": 10000,
                    "sort": "asc",
                },
                timeout=60,
            ),
            component="alpaca_daily_closes",
            logger=self.logger,
        )
        _raise_for_status(response)
        bars = response.json().get("bars", [])
        closes: list[tuple[datetime, float]] = []
        for bar in bars:
            ts = datetime.fromisoformat(bar["t"].replace("Z", "+00:00")).astimezone(EASTERN)
            closes.append((ts, float(bar["c"])))
        return closes

    def fetch_eod_prices(self, tickers: list[str], trading_date: date) -> dict[str, float]:
        start = datetime.combine(trading_date - timedelta(days=5), datetime.min.time(), tzinfo=EASTERN)
        end = datetime.combine(trading_date + timedelta(days=1), datetime.min.time(), tzinfo=EASTERN)
        results: dict[str, float] = {}
        for batch in chunked(tickers, 100):
            response = with_retry(
                lambda batch=batch: self.session.get(
                    f"{self.base_url}/bars",
                    params={
                        "symbols": ",".join(batch),
                        "timeframe": "1Day",
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "adjustment": "all",
                        "feed": "iex",
                        "limit": 10,
                    },
                    timeout=60,
                ),
                component="alpaca_eod_prices",
                logger=self.logger,
            )
            _raise_for_status(response)
            bars = response.json().get("bars", {})
            for ticker, ticker_bars in bars.items():
                if ticker_bars:
                    results[ticker] = float(ticker_bars[-1]["c"])
        return results


class GoogleNewsClient:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "trade-simulator/1.0"})

    def fetch(self, ticker: str) -> list[dict[str, str]]:
        import xml.etree.ElementTree as ET

        response = with_retry(
            lambda: self.session.get(
                "https://news.google.com/rss/search",
                params={"q": f"{ticker} stock", "hl": "en-US", "gl": "US", "ceid": "US:en"},
                timeout=30,
            ),
            component="google_news",
            logger=self.logger,
        )
        _raise_for_status(response)
        root = ET.fromstring(response.text)
        entries: list[dict[str, str]] = []
        for item in root.findall(".//item")[:5]:
            entries.append(
                {
                    "title": item.findtext("title", default=""),
                    "link": item.findtext("link", default=""),
                    "published_at": item.findtext("pubDate", default=""),
                    "source": "google_news",
                }
            )
        return entries


class NewsApiClient:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.logger = logger
        self.session = requests.Session()
        self.api_key = config.newsapi_key

    def validate(self) -> None:
        response = self.session.get(
            "https://newsapi.org/v2/everything",
            params={"q": "AAPL", "pageSize": 1, "apiKey": self.api_key},
            timeout=30,
        )
        _raise_for_status(response)
        payload = response.json()
        if payload.get("status") != "ok":
            raise RuntimeError(f"NewsAPI validation failed: {payload}")

    def fetch(self, ticker: str) -> list[dict[str, str]]:
        response = with_retry(
            lambda: self.session.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": f"{ticker} stock",
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 5,
                    "apiKey": self.api_key,
                },
                timeout=30,
            ),
            component="newsapi_fetch",
            logger=self.logger,
        )
        _raise_for_status(response)
        payload = response.json()
        articles = payload.get("articles", [])
        return [
            {
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "link": article.get("url", ""),
                "published_at": article.get("publishedAt", ""),
                "source": "newsapi",
            }
            for article in articles[:5]
        ]


class EdgarClient:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.logger = logger
        self.session = requests.Session()
        contact = config.sec_contact_email or "contact@example.com"
        self.session.headers.update({"User-Agent": f"trade-simulator/1.0 {contact}"})
        self._ticker_map: dict[str, str] | None = None

    def _load_mapping(self) -> dict[str, str]:
        if self._ticker_map is not None:
            return self._ticker_map
        response = with_retry(
            lambda: self.session.get("https://www.sec.gov/files/company_tickers.json", timeout=30),
            component="sec_mapping",
            logger=self.logger,
        )
        _raise_for_status(response)
        payload = response.json()
        self._ticker_map = {
            value["ticker"].upper(): str(value["cik_str"]).zfill(10)
            for value in payload.values()
        }
        return self._ticker_map

    def fetch(self, ticker: str) -> list[dict[str, str]]:
        cik = self._load_mapping().get(ticker.upper())
        if not cik:
            return []
        response = with_retry(
            lambda: self.session.get(f"https://data.sec.gov/submissions/CIK{cik}.json", timeout=30),
            component="sec_filings",
            logger=self.logger,
        )
        _raise_for_status(response)
        payload = response.json()
        recent = payload.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        items: list[dict[str, str]] = []
        for form, filed_at, accession, primary_doc in list(
            zip(forms, dates, accessions, primary_docs, strict=False)
        )[:5]:
            accession_slug = accession.replace("-", "")
            items.append(
                {
                    "title": f"SEC {form} filing",
                    "link": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_slug}/{primary_doc}",
                    "published_at": filed_at,
                    "source": "sec_edgar",
                }
            )
        return items


class StocktwitsClient:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "trade-simulator/1.0"})

    def fetch(self, ticker: str) -> list[dict[str, str]]:
        response = with_retry(
            lambda: self.session.get(
                f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json",
                timeout=30,
            ),
            component="stocktwits_fetch",
            logger=self.logger,
        )
        _raise_for_status(response)
        payload = response.json()
        messages = payload.get("messages", [])
        return [
            {
                "body": message.get("body", ""),
                "sentiment": (
                    (message.get("entities", {}) or {}).get("sentiment") or {}
                ).get("basic", ""),
                "published_at": message.get("created_at", ""),
                "source": "stocktwits",
            }
            for message in messages[:10]
        ]


class RedditClient:
    SUBREDDITS = ("stocks", "investing", "wallstreetbets")

    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.logger = logger
        self.client_id = config.reddit_client_id
        self.client_secret = config.reddit_client_secret
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "trade-simulator/1.0"})
        self._token: str | None = None
        self._token_expires_at: datetime | None = None

    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def _get_token(self) -> str:
        if self._token and self._token_expires_at and self._token_expires_at > datetime.now(EASTERN):
            return self._token
        response = with_retry(
            lambda: self.session.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=(self.client_id, self.client_secret),
                data={"grant_type": "client_credentials"},
                timeout=30,
            ),
            component="reddit_token",
            logger=self.logger,
        )
        _raise_for_status(response)
        payload = response.json()
        self._token = payload["access_token"]
        self._token_expires_at = datetime.now(EASTERN) + timedelta(seconds=int(payload["expires_in"]) - 60)
        return self._token

    def fetch(self, ticker: str) -> list[dict[str, str]]:
        if not self.configured():
            return []
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}", "User-Agent": "trade-simulator/1.0"}
        results: list[dict[str, str]] = []
        for subreddit in self.SUBREDDITS:
            response = with_retry(
                lambda subreddit=subreddit: self.session.get(
                    f"https://oauth.reddit.com/r/{subreddit}/search",
                    headers=headers,
                    params={
                        "q": ticker,
                        "restrict_sr": 1,
                        "sort": "new",
                        "limit": 3,
                        "t": "week",
                    },
                    timeout=30,
                ),
                component=f"reddit_fetch_{subreddit}",
                logger=self.logger,
            )
            _raise_for_status(response)
            posts = response.json().get("data", {}).get("children", [])
            for post in posts:
                data = post.get("data", {})
                results.append(
                    {
                        "title": data.get("title", ""),
                        "body": data.get("selftext", ""),
                        "score": str(data.get("score", "")),
                        "published_at": datetime.fromtimestamp(
                            data.get("created_utc", 0), tz=EASTERN
                        ).isoformat(),
                        "source": "reddit",
                    }
                )
        return results


class AnthropicClassifierClient:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.logger = logger
        self.model = config.anthropic_model
        self.api_key = config.anthropic_api_key
        self.client = Anthropic(api_key=config.anthropic_api_key)

    def validate(self) -> None:
        response = requests.get(
            "https://api.anthropic.com/v1/models",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
            },
            timeout=30,
        )
        _raise_for_status(response)

    def classify(self, *, system_prompt: str, user_prompt: str) -> str:
        # Mark the system prompt as cacheable — it's identical across every
        # classification call so we pay the full input cost only on the first
        # request in each 5-minute cache window, then ~10% of that for hits.
        response = with_retry(
            lambda: self.client.messages.create(
                model=self.model,
                max_tokens=1200,
                temperature=0,
                system=[
                    {
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_prompt}],
            ),
            component="anthropic_classification",
            logger=self.logger,
        )
        text_chunks = []
        for block in response.content:
            if getattr(block, "type", None) == "text":
                text_chunks.append(block.text)
        return "\n".join(text_chunks).strip()


class NtfyClient:
    BASE_URL = "https://ntfy.sh"

    def __init__(self, topic: str, logger: logging.Logger):
        self.topic = topic
        self.logger = logger
        self.session = requests.Session()

    def _post(self, title: str, message: str, tags: list[str] | None = None, priority: str = "default") -> None:
        if not self.topic:
            return
        # HTTP headers default to latin-1; strip non-ASCII from title to be safe
        safe_title = title.encode("ascii", errors="ignore").decode("ascii")
        headers: dict[str, str] = {
            "Title": safe_title,
            "Priority": priority,
        }
        if tags:
            headers["Tags"] = ",".join(tags)
        try:
            response = self.session.post(
                f"{self.BASE_URL}/{self.topic}",
                data=message.encode("utf-8"),
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("ntfy notification failed: %s", exc)

    def notify_trigger(self, ticker: str, drop_pct: float, recommendation: str, confidence: str, summary: str) -> None:
        title = f"{ticker} dropped {drop_pct:.1f}%"
        message = f"{recommendation.upper()} ({confidence} confidence)\n{summary}"
        tag = {"buy_candidate": "green_circle", "monitor": "yellow_circle", "avoid": "red_circle"}.get(recommendation, "white_circle")
        priority = "high" if recommendation == "buy_candidate" and confidence == "high" else "default"
        self._post(title, message, tags=[tag, "chart_with_downwards_trend"], priority=priority)

    @staticmethod
    def _format_summary_lines(summary: dict) -> list[str]:
        open_positions = summary.get("open_positions", [])
        closed_today = summary.get("closed_today", [])
        lines = [
            f"Triggers: {summary.get('triggers_today', 0)}  |  "
            f"Opened: {summary.get('opened_today', 0)}  |  Closed: {len(closed_today)}",
            "",
            f"P&L today: {summary.get('pnl_today_sum', 0):+.2f}%",
            f"P&L all-time: {summary.get('pnl_all_time_sum', 0):+.2f}% ({summary.get('total_closed', 0)} closed)",
        ]
        if closed_today:
            lines.append("")
            lines.append("Closed today:")
            for p in closed_today:
                lines.append(f"  {p['ticker']}: {float(p['hypothetical_pnl_pct']):+.2f}% ({p.get('exit_reason') or 'n/a'})")
        if open_positions:
            lines.append("")
            lines.append(f"Holdings ({len(open_positions)}):")
            for p in open_positions:
                lines.append(f"  {p['ticker']}: {float(p['hypothetical_pnl_pct']):+.2f}% ({int(p['days_held'])}d)")
        else:
            lines.append("")
            lines.append("No open positions.")
        pending = summary.get("pending_positions", [])
        if pending:
            lines.append("")
            lines.append(f"Pending fill at next open ({len(pending)}):")
            for p in pending:
                lines.append(f"  {p['ticker']}")
        corpus = summary.get("corpus")
        if corpus:
            lines.append("")
            lines.append(
                f"Corpus: {corpus.get('news_events', 0)} events, "
                f"{corpus.get('labeled_events', 0)} labeled"
            )
        sc = summary.get("scorecard")
        if sc and (sc.get("evaluated") or sc.get("budget_skipped")):
            lines.append("")
            lines.append(
                f"Classifier ({sc.get('evaluated', 0)} eval): "
                f"missed {sc.get('false_negatives', 0)} winners, "
                f"caught {sc.get('true_positives', 0)}, "
                f"bad buys {sc.get('false_positives', 0)}"
            )
            if sc.get("budget_skipped_winners"):
                lines.append(
                    f"  +{sc['budget_skipped_winners']} winners never classified (budget)"
                )
        return lines

    def notify_startup(
        self,
        universe: str,
        ticker_count: int,
        dashboard_port: int,
        summary: dict,
        recovered_from_crash: bool = False,
    ) -> None:
        lines = [
            f"Universe: {universe} ({ticker_count} tickers)",
            f"Dashboard: http://127.0.0.1:{dashboard_port}",
            "",
        ]
        if recovered_from_crash:
            lines.insert(0, "Previous run did not shut down cleanly (crash or kill).")
            lines.insert(1, "")
        lines.extend(self._format_summary_lines(summary))
        title = "Trade simulator restarted after crash" if recovered_from_crash else "Trade simulator started"
        tags = ["warning"] if recovered_from_crash else ["rocket"]
        self._post(title=title, message="\n".join(lines), tags=tags)

    def notify_eod_summary(self, today: str, summary: dict) -> None:
        pnl = summary.get("pnl_today_sum", 0)
        tag = "chart_with_upwards_trend" if pnl > 0 else "chart_with_downwards_trend" if pnl < 0 else "bar_chart"
        self._post(
            title=f"EOD Summary — {today}",
            message="\n".join(self._format_summary_lines(summary)),
            tags=[tag],
        )

    def notify_position_closed(self, ticker: str, pnl_pct: float, exit_reason: str, days_held: int) -> None:
        title = f"{ticker} closed {pnl_pct:+.2f}%"
        reason_label = {"target_reached": "hit target", "max_hold_exceeded": "max hold exceeded"}.get(exit_reason, exit_reason)
        message = f"{reason_label} after {days_held}d"
        tags = ["white_check_mark", "moneybag"] if pnl_pct > 0 else ["x"]
        self._post(title, message, tags=tags)


def summarize_retail_sentiment(items: list[dict[str, str]]) -> str:
    counts = defaultdict(int)
    for item in items:
        sentiment = item.get("sentiment", "").lower()
        if sentiment in {"bullish", "bearish"}:
            counts[sentiment] += 1
    if not counts:
        return "insufficient_data"
    if counts["bullish"] and counts["bearish"]:
        return "mixed"
    if counts["bullish"]:
        return "bullish"
    return "bearish"
