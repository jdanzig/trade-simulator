from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal


EASTERN = ZoneInfo("America/New_York")


@dataclass(slots=True)
class MarketClock:
    timezone: ZoneInfo = EASTERN
    calendar: Any = field(init=False)

    def __post_init__(self) -> None:
        self.calendar = mcal.get_calendar("XNYS")

    def now(self) -> datetime:
        return datetime.now(self.timezone)

    def is_trading_day(self, current_date: date) -> bool:
        schedule = self.calendar.schedule(start_date=current_date, end_date=current_date)
        return not schedule.empty

    def session_for_date(self, current_date: date):
        schedule = self.calendar.schedule(start_date=current_date, end_date=current_date)
        if schedule.empty:
            return None
        return schedule.iloc[0]

    def market_is_open(self, current_time: datetime | None = None) -> bool:
        current_time = current_time.astimezone(self.timezone) if current_time else self.now()
        session = self.session_for_date(current_time.date())
        if session is None:
            return False
        market_open = session["market_open"].tz_convert(self.timezone).to_pydatetime()
        market_close = session["market_close"].tz_convert(self.timezone).to_pydatetime()
        return market_open <= current_time <= market_close

    def session_bounds(self, current_date: date | None = None) -> tuple[datetime, datetime] | None:
        current_date = current_date or self.now().date()
        session = self.session_for_date(current_date)
        if session is None:
            return None
        market_open = session["market_open"].tz_convert(self.timezone).to_pydatetime()
        market_close = session["market_close"].tz_convert(self.timezone).to_pydatetime()
        return market_open, market_close

    def scheduled_report_time(self, current_date: date) -> datetime:
        return datetime.combine(current_date, time(hour=16, minute=5), tzinfo=self.timezone)

    def weekly_findings_time(self, current_date: date) -> datetime:
        return datetime.combine(current_date, time(hour=17, minute=0), tzinfo=self.timezone)

    def recheck_time(self, triggered_at: datetime) -> datetime:
        return triggered_at.astimezone(self.timezone) + timedelta(hours=2)
