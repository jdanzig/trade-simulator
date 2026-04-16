from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


class ConfigError(RuntimeError):
    """Raised when config.yaml is missing or invalid."""


@dataclass(slots=True)
class AppPaths:
    base_dir: Path
    config_path: Path
    data_dir: Path
    database_path: Path
    classifier_prompt_path: Path
    findings_path: Path

    @classmethod
    def from_base_dir(cls, base_dir: Path) -> "AppPaths":
        data_dir = base_dir / "data"
        return cls(
            base_dir=base_dir,
            config_path=base_dir / "config.yaml",
            data_dir=data_dir,
            database_path=data_dir / "trade_simulator.sqlite3",
            classifier_prompt_path=base_dir / "classifier_prompt.md",
            findings_path=base_dir / "findings.md",
        )


@dataclass(slots=True)
class AppConfig:
    anthropic_api_key: str
    alpaca_api_key: str
    alpaca_secret_key: str
    newsapi_key: str
    report_email: str
    gmail_client_id: str
    gmail_client_secret: str
    gmail_refresh_token: str
    gmail_sender_email: str = ""
    stocktwits_api_key: str = ""
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    anthropic_model: str = "claude-3-5-haiku-latest"
    universe: str = "sp500"
    blocklist: list[str] = field(default_factory=list)
    drop_threshold_pct: float = 7.0
    trigger_cooldown_hours: int = 4
    poll_interval_minutes: int = 5
    max_hold_days: int = 30
    target_return_pct: float = 15.0
    daily_api_call_budget: int = 20
    dashboard_port: int = 8080
    max_learned_prompt_tokens: int = 2000
    unusual_whales_enabled: bool = False

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "AppConfig":
        try:
            return cls(**payload)
        except TypeError as exc:
            raise ConfigError(f"Invalid config.yaml contents: {exc}") from exc

    def validate(self) -> None:
        required = {
            "anthropic_api_key": self.anthropic_api_key,
            "alpaca_api_key": self.alpaca_api_key,
            "alpaca_secret_key": self.alpaca_secret_key,
            "newsapi_key": self.newsapi_key,
            "report_email": self.report_email,
            "gmail_client_id": self.gmail_client_id,
            "gmail_client_secret": self.gmail_client_secret,
            "gmail_refresh_token": self.gmail_refresh_token,
        }
        missing = [name for name, value in required.items() if not str(value).strip()]
        if missing:
            joined = ", ".join(missing)
            raise ConfigError(f"Missing required config values: {joined}")
        if self.universe not in {"sp500", "nasdaq100"}:
            raise ConfigError("universe must be one of: sp500, nasdaq100")
        if self.poll_interval_minutes <= 0:
            raise ConfigError("poll_interval_minutes must be positive")
        if self.daily_api_call_budget <= 0:
            raise ConfigError("daily_api_call_budget must be positive")


def load_config(paths: AppPaths) -> AppConfig:
    if not paths.config_path.exists():
        raise ConfigError(
            f"Missing config file at {paths.config_path}. Fill out config.yaml before first run."
        )
    payload = yaml.safe_load(paths.config_path.read_text()) or {}
    if not isinstance(payload, dict):
        raise ConfigError("config.yaml must contain a mapping/object at the top level.")
    config = AppConfig.from_dict(payload)
    config.validate()
    return config
