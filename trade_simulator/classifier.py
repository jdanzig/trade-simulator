from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import AppConfig
from .utils import estimate_token_count, extract_json_object


class ClassificationError(RuntimeError):
    """Raised when the Claude response is malformed."""


BASE_FIELDS = {
    "cause_category": "unclear",
    "affects_cash_flows": False,
    "affects_cash_flows_reasoning": "",
    "reversible": False,
    "reversible_reasoning": "",
    "retail_sentiment": "insufficient_data",
    "retail_sentiment_reasoning": "",
    "smart_money_signal": "unavailable",
    "overreaction_score": 0,
    "overreaction_reasoning": "",
    "recommendation": "monitor",
    "confidence": "low",
}


class ClassifierService:
    def __init__(
        self,
        config: AppConfig,
        prompt_path: Path,
        anthropic_client,
        logger: logging.Logger,
    ):
        self.config = config
        self.prompt_path = prompt_path
        self.anthropic_client = anthropic_client
        self.logger = logger

    def _load_prompt(self) -> str:
        prompt = self.prompt_path.read_text()
        if estimate_token_count(prompt) > self.config.max_learned_prompt_tokens:
            raise ClassificationError(
                "classifier_prompt.md exceeds max_learned_prompt_tokens and must be shortened."
            )
        return prompt

    def classify(
        self,
        *,
        trigger: dict[str, Any],
        pass_number: int,
        news_maturity: str,
        news_payload: dict[str, Any],
        formatted_context: str,
    ) -> dict[str, Any]:
        system_prompt = self._load_prompt()
        user_prompt = (
            "Return JSON only. No markdown.\n\n"
            f"Ticker: {trigger['ticker']}\n"
            f"Triggered at: {trigger['triggered_at'].isoformat()}\n"
            f"Drop pct from intraday high: {-abs(float(trigger['drop_pct'])):.2f}\n"
            f"Pass: {pass_number}\n"
            f"News maturity: {news_maturity}\n\n"
            "Required JSON schema keys:\n"
            "ticker, triggered_at, drop_pct_from_intraday_high, news_maturity, pass, sources_used, "
            "cause_summary, cause_category, affects_cash_flows, affects_cash_flows_reasoning, reversible, "
            "reversible_reasoning, retail_sentiment, retail_sentiment_reasoning, smart_money_signal, "
            "overreaction_score, overreaction_reasoning, recommendation, confidence, recheck_scheduled_at.\n\n"
            f"Context:\n{formatted_context}"
        )
        raw_text = self.anthropic_client.classify(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        try:
            parsed = extract_json_object(raw_text)
        except Exception as exc:  # noqa: BLE001
            raise ClassificationError(f"Malformed Claude response: {exc}") from exc

        normalized = dict(BASE_FIELDS)
        normalized.update(parsed)
        normalized["ticker"] = trigger["ticker"]
        normalized["triggered_at"] = trigger["triggered_at"].isoformat()
        normalized["drop_pct_from_intraday_high"] = -abs(float(trigger["drop_pct"]))
        normalized["news_maturity"] = news_maturity
        normalized["pass"] = pass_number
        normalized["sources_used"] = news_payload["sources_used"]
        normalized["smart_money_signal"] = parsed.get(
            "smart_money_signal", news_payload["smart_money_signal"]
        )
        if pass_number == 1:
            normalized["recheck_scheduled_at"] = trigger["recheck_scheduled_at"].isoformat()
        else:
            normalized["recheck_scheduled_at"] = None
        normalized["full_claude_response"] = parsed
        return normalized

    @staticmethod
    def finalize_second_pass(
        first_pass: dict[str, Any] | None,
        second_pass: dict[str, Any],
        current_time: datetime,
    ) -> dict[str, Any]:
        if not first_pass:
            return second_pass
        if first_pass["recommendation"] == second_pass["recommendation"]:
            return second_pass
        second_pass["recommendation"] = "monitor"
        second_pass["confidence"] = second_pass.get("confidence", "low")
        second_pass["overreaction_reasoning"] = (
            f"{second_pass['overreaction_reasoning']} "
            f"Pass disagreement detected at {current_time.isoformat()}, so recommendation was normalized to monitor."
        ).strip()
        return second_pass
