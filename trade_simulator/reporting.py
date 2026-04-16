from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .database import Database


class ReportingService:
    def __init__(self, db: Database, mail_client, prompt_path: Path, findings_path: Path):
        self.db = db
        self.mail_client = mail_client
        self.prompt_path = prompt_path
        self.findings_path = findings_path

    def build_daily_report_payload(self, report_date: date) -> dict[str, Any]:
        triggers = self.db.list_triggers_for_date(report_date)
        open_positions = self.db.list_open_positions()
        closed_positions = self.db.list_closed_positions_for_date(report_date)
        performance = self.db.portfolio_performance()
        return {
            "report_date": report_date.isoformat(),
            "triggers_today": len(triggers),
            "budget_exhausted": any(t["budget_status"] == "budget_exhausted" for t in triggers),
            "buy_candidates_today": self.db.count_positions_entered_on(report_date),
            "open_positions": [
                {
                    "ticker": position["ticker"],
                    "entry_date": position["entry_timestamp"][:10],
                    "entry_price": round(float(position["hypothetical_entry_price"]), 2),
                    "current_price": round(float(position["current_price"]), 2),
                    "pnl_pct": round(float(position["hypothetical_pnl_pct"]), 2),
                    "days_held": int(position["days_held"]),
                }
                for position in open_positions
            ],
            "closed_positions_today": [
                {
                    "ticker": position["ticker"],
                    "exit_price": round(float(position["exit_price"] or 0), 2),
                    "exit_reason": position["exit_reason"],
                    "pnl_pct": round(float(position["hypothetical_pnl_pct"]), 2),
                }
                for position in closed_positions
            ],
            "portfolio_pnl_pct_today": performance["today"],
            "portfolio_pnl_pct_inception": performance["inception"],
            "classifier_performance": {
                **self.db.get_classifier_rollups(),
                "prompt_last_modified": date.fromtimestamp(self.prompt_path.stat().st_mtime).isoformat(),
            },
            "errors_today": self.db.count_errors_for_date(report_date),
        }

    def render_daily_report_markdown(self, payload: dict[str, Any]) -> str:
        lines = [
            f"# Daily Report - {payload['report_date']}",
            "",
            f"- Triggers today: {payload['triggers_today']}",
            f"- Budget exhausted: {payload['budget_exhausted']}",
            f"- Buy candidates today: {payload['buy_candidates_today']}",
            f"- Portfolio P&L today: {payload['portfolio_pnl_pct_today']}%",
            f"- Portfolio P&L since inception: {payload['portfolio_pnl_pct_inception']}%",
            f"- Errors today: {payload['errors_today']}",
            "",
            "## Open Positions",
        ]
        if payload["open_positions"]:
            for position in payload["open_positions"]:
                lines.append(
                    f"- {position['ticker']}: entry {position['entry_price']}, current {position['current_price']}, "
                    f"P&L {position['pnl_pct']}%, held {position['days_held']} days"
                )
        else:
            lines.append("- None")
        lines.extend(["", "## Closed Positions Today"])
        if payload["closed_positions_today"]:
            for position in payload["closed_positions_today"]:
                lines.append(
                    f"- {position['ticker']}: exit {position['exit_price']}, "
                    f"P&L {position['pnl_pct']}%, reason {position['exit_reason']}"
                )
        else:
            lines.append("- None")
        lines.extend(["", "## Classifier Performance"])
        for confidence, stats in payload["classifier_performance"]["by_confidence"].items():
            lines.append(
                f"- Confidence {confidence}: {stats['total']} total, "
                f"{stats['profitable_at_30_days']} profitable, win rate {stats['win_rate_pct']}%"
            )
        for category, stats in payload["classifier_performance"]["by_cause_category"].items():
            lines.append(
                f"- Category {category}: {stats['total']} total, win rate {stats['win_rate_pct']}%"
            )
        lines.append(
            f"- Prompt last modified: {payload['classifier_performance']['prompt_last_modified']}"
        )
        return "\n".join(lines)

    def send_daily_report(self, report_date: date) -> dict[str, Any]:
        payload = self.build_daily_report_payload(report_date)
        markdown = self.render_daily_report_markdown(payload)
        self.mail_client.send_markdown(
            subject=f"Daily Dip Classifier Report - {report_date.isoformat()}",
            body=markdown,
        )
        return payload

    def build_weekly_findings(self, today: date) -> str:
        since = today - timedelta(days=30)
        rows = self.db.list_closed_positions_since(since)
        if not rows:
            return f"## Findings for {today.isoformat()}\n\nNo closed positions were available for the last 30 days.\n"

        by_category: dict[str, list[float]] = defaultdict(list)
        by_confidence: dict[str, list[float]] = defaultdict(list)
        by_maturity: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            pnl = float(row["hypothetical_pnl_pct"])
            by_category[row["cause_category"]].append(pnl)
            by_confidence[row["confidence"]].append(pnl)
            by_maturity[row["news_maturity"]].append(pnl)

        lines = [f"## Findings for {today.isoformat()}", ""]
        for label, groups in (
            ("Cause category", by_category),
            ("Confidence", by_confidence),
            ("News maturity", by_maturity),
        ):
            lines.append(f"{label}:")
            for key, values in sorted(groups.items()):
                win_rate = round((sum(1 for value in values if value > 0) / len(values)) * 100, 1)
                avg_return = round(sum(values) / len(values), 2)
                lines.append(
                    f"- {key}: {len(values)} closed positions, {win_rate}% profitable, average return {avg_return}%"
                )
            lines.append("")
        return "\n".join(lines).strip() + "\n"

    def append_weekly_findings(self, today: date) -> str:
        block = self.build_weekly_findings(today)
        prefix = "" if not self.findings_path.exists() or self.findings_path.stat().st_size == 0 else "\n\n"
        self.findings_path.write_text(
            self.findings_path.read_text() + prefix + block if self.findings_path.exists() else block
        )
        return block

    def send_weekly_findings_email(self, today: date, findings_block: str) -> None:
        self.mail_client.send_markdown(
            subject=f"Weekly Dip Classifier Findings - {today.isoformat()}",
            body=findings_block,
        )
