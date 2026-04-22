from __future__ import annotations

import base64
import logging
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.message import EmailMessage
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
            return [
                {
                    "ticker": ticker,
                    "company_name": security,
                }
                for ticker, security in zip(table["Symbol"], table["Security"], strict=True)
            ]
        for table in tables:
            if {"Ticker", "Company"}.issubset(set(table.columns)):
                return [
                    {
                        "ticker": ticker,
                        "company_name": company,
                    }
                    for ticker, company in zip(table["Ticker"], table["Company"], strict=True)
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
        contact = config.report_email or "contact@example.com"
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
                "sentiment": (message.get("entities", {}) or {})
                .get("sentiment", {})
                .get("basic", ""),
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
        response = with_retry(
            lambda: self.client.messages.create(
                model=self.model,
                max_tokens=1200,
                temperature=0,
                system=system_prompt,
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


class GmailClient:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.logger = logger
        self.report_email = config.report_email
        self.from_email = config.gmail_sender_email or config.report_email
        self.client_id = config.gmail_client_id
        self.client_secret = config.gmail_client_secret
        self.refresh_token = config.gmail_refresh_token
        self.session = requests.Session()
        self._access_token: str | None = None
        self._access_token_expires_at = 0.0

    def _refresh_access_token(self) -> str:
        if self._access_token and time.time() < self._access_token_expires_at - 60:
            return self._access_token
        response = with_retry(
            lambda: self.session.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                    "grant_type": "refresh_token",
                },
                timeout=30,
            ),
            component="gmail_refresh_token",
            logger=self.logger,
        )
        _raise_for_status(response)
        payload = response.json()
        self._access_token = payload["access_token"]
        self._access_token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._access_token

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._refresh_access_token()}",
            "Content-Type": "application/json",
        }

    def validate(self) -> None:
        # `gmail.send` is sufficient for delivery but not for reading the mailbox profile.
        # Startup only needs to confirm that the OAuth credentials can mint an access token.
        self._refresh_access_token()

    def send_markdown(self, subject: str, body: str) -> None:
        message = EmailMessage()
        message["To"] = self.report_email
        message["From"] = self.from_email
        message["Subject"] = subject
        message.set_content(body)
        payload = {
            "raw": base64.urlsafe_b64encode(message.as_bytes()).decode("ascii"),
        }
        response = with_retry(
            lambda: self.session.post(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
                headers=self._auth_headers(),
                json=payload,
                timeout=30,
            ),
            component="gmail_send",
            logger=self.logger,
        )
        if response.status_code >= 300:
            raise RuntimeError(f"Gmail send failed: {response.status_code} {response.text}")


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
