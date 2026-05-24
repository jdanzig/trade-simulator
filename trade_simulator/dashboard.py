from __future__ import annotations

import threading

from flask import Flask, render_template_string

from .database import Database

_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="60">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Dip Classifier</title>
  <style>
    :root {
      --bg: #f5efe3;
      --card: rgba(255, 252, 245, 0.92);
      --ink: #1f2933;
      --muted: #6b7280;
      --accent: #0f766e;
      --accent-soft: #d7f3ef;
      --red: #dc2626;
      --red-soft: #fee2e2;
      --green: #16a34a;
      --green-soft: #dcfce7;
      --border: rgba(31, 41, 51, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      font-size: 14px;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.18), transparent 28%),
        linear-gradient(135deg, #f7f1e6 0%, #efe6d6 50%, #f6f2eb 100%);
      min-height: 100vh;
    }
    main { max-width: 1200px; margin: 0 auto; padding: 32px 20px 60px; }
    h1 { margin: 0 0 4px; font-size: 1.5rem; }
    h2 { font-size: 1rem; margin: 0 0 14px; color: var(--muted); font-weight: normal; letter-spacing: .04em; text-transform: uppercase; }
    .subtitle { color: var(--muted); margin: 0 0 28px; font-size: 0.85rem; }

    /* Stats strip */
    .stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 12px;
      margin-bottom: 28px;
    }
    .stat {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px 18px;
    }
    .stat label { display: block; font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
    .stat .val { font-size: 1.6rem; font-weight: bold; line-height: 1; }
    .stat .val.pos { color: var(--green); }
    .stat .val.neg { color: var(--red); }
    .stat .val.neutral { color: var(--ink); }

    /* Section cards */
    .section {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 22px 24px;
      margin-bottom: 22px;
      overflow: hidden;
    }

    /* Tables */
    .tbl-wrap { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
    th {
      text-align: left;
      padding: 8px 12px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: .05em;
      white-space: nowrap;
    }
    td { padding: 10px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(15, 118, 110, 0.04); }
    .empty td { color: var(--muted); font-style: italic; }

    /* Badges */
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 0.72rem;
      font-family: system-ui, sans-serif;
      font-weight: 600;
      white-space: nowrap;
    }
    .badge-buy { background: var(--green-soft); color: var(--green); }
    .badge-avoid { background: var(--red-soft); color: var(--red); }
    .badge-monitor { background: #fef9c3; color: #92400e; }
    .badge-high { background: var(--accent-soft); color: var(--accent); }
    .badge-medium { background: #e0e7ff; color: #3730a3; }
    .badge-low { background: #f3f4f6; color: #6b7280; }

    .pos-pnl { color: var(--green); font-weight: bold; }
    .neg-pnl { color: var(--red); font-weight: bold; }
    .muted { color: var(--muted); }

    /* Classifier perf grid */
    .perf-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
    .perf-table th { background: #f3f4f6; color: var(--muted); }
    @media (max-width: 640px) { .perf-grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
<main>
  <h1>News-Driven Dip Classifier</h1>
  <p class="subtitle">Paper trading dashboard &mdash; refreshes every 60 s</p>

  <!-- Stats strip -->
  <div class="stats">
    <div class="stat">
      <label>Open positions</label>
      <div class="val neutral">{{ summary.open_count }}</div>
    </div>
    <div class="stat">
      <label>Total trades</label>
      <div class="val neutral">{{ summary.total_trades }}</div>
    </div>
    <div class="stat">
      <label>Win rate</label>
      {% if summary.win_rate_pct is not none %}
        <div class="val {{ 'pos' if summary.win_rate_pct >= 50 else 'neg' }}">{{ summary.win_rate_pct }}%</div>
      {% else %}
        <div class="val muted">—</div>
      {% endif %}
    </div>
    <div class="stat">
      <label>Avg closed P&amp;L</label>
      <div class="val {{ 'pos' if summary.avg_closed_pnl > 0 else 'neg' if summary.avg_closed_pnl < 0 else 'neutral' }}">
        {{ "%+.2f"|format(summary.avg_closed_pnl) }}%
      </div>
    </div>
    <div class="stat">
      <label>Avg open P&amp;L</label>
      <div class="val {{ 'pos' if summary.avg_open_pnl > 0 else 'neg' if summary.avg_open_pnl < 0 else 'neutral' }}">
        {{ "%+.2f"|format(summary.avg_open_pnl) }}%
      </div>
    </div>
    <div class="stat">
      <label>Best trade</label>
      <div class="val pos">{{ "%+.2f"|format(summary.best_trade) }}%</div>
    </div>
    <div class="stat">
      <label>Worst trade</label>
      <div class="val neg">{{ "%+.2f"|format(summary.worst_trade) }}%</div>
    </div>
  </div>

  <!-- Open positions -->
  <div class="section">
    <h2>Open positions</h2>
    <div class="tbl-wrap">
      <table>
        <thead>
          <tr>
            <th>Ticker</th><th>Entry date</th><th>Entry price</th>
            <th>Current price</th><th>P&amp;L</th><th>Days held</th>
            <th>Category</th><th>Confidence</th><th>Summary</th>
          </tr>
        </thead>
        <tbody>
          {% if open_positions %}
            {% for p in open_positions %}
            <tr>
              <td><strong>{{ p.ticker }}</strong></td>
              <td class="muted">{{ p.entry_timestamp[:10] }}</td>
              <td>${{ "%.2f"|format(p.hypothetical_entry_price) }}</td>
              <td>${{ "%.2f"|format(p.current_price) }}</td>
              <td class="{{ 'pos-pnl' if p.hypothetical_pnl_pct > 0 else 'neg-pnl' }}">
                {{ "%+.2f"|format(p.hypothetical_pnl_pct) }}%
              </td>
              <td>{{ p.days_held }}d</td>
              <td>{{ p.cause_category or "—" }}</td>
              <td>
                {% if p.confidence %}
                  <span class="badge badge-{{ p.confidence }}">{{ p.confidence }}</span>
                {% else %}—{% endif %}
              </td>
              <td class="muted" style="max-width:260px;font-size:0.8rem">{{ p.cause_summary or "—" }}</td>
            </tr>
            {% endfor %}
          {% else %}
            <tr class="empty"><td colspan="9">No open positions.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>

  <!-- Closed trades -->
  <div class="section">
    <h2>Closed trades (last 50)</h2>
    <div class="tbl-wrap">
      <table>
        <thead>
          <tr>
            <th>Ticker</th><th>Entry date</th><th>Entry price</th>
            <th>Exit price</th><th>P&amp;L</th><th>Days held</th>
            <th>Exit reason</th><th>Category</th><th>Confidence</th>
          </tr>
        </thead>
        <tbody>
          {% if closed_positions %}
            {% for p in closed_positions %}
            <tr>
              <td><strong>{{ p.ticker }}</strong></td>
              <td class="muted">{{ p.entry_timestamp[:10] }}</td>
              <td>${{ "%.2f"|format(p.hypothetical_entry_price) }}</td>
              <td>${{ "%.2f"|format(p.exit_price or 0) }}</td>
              <td class="{{ 'pos-pnl' if p.hypothetical_pnl_pct > 0 else 'neg-pnl' }}">
                {{ "%+.2f"|format(p.hypothetical_pnl_pct) }}%
              </td>
              <td>{{ p.days_held }}d</td>
              <td class="muted">{{ p.exit_reason or "—" }}</td>
              <td>{{ p.cause_category or "—" }}</td>
              <td>
                {% if p.confidence %}
                  <span class="badge badge-{{ p.confidence }}">{{ p.confidence }}</span>
                {% else %}—{% endif %}
              </td>
            </tr>
            {% endfor %}
          {% else %}
            <tr class="empty"><td colspan="9">No closed trades yet.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>

  <!-- Recent triggers -->
  <div class="section">
    <h2>Recent triggers (last 50)</h2>
    <div class="tbl-wrap">
      <table>
        <thead>
          <tr>
            <th>Time</th><th>Ticker</th><th>Drop</th><th>Trigger price</th>
            <th>Recommendation</th><th>Score</th><th>Confidence</th>
            <th>Category</th><th>Position P&amp;L</th>
          </tr>
        </thead>
        <tbody>
          {% if triggers %}
            {% for t in triggers %}
            <tr>
              <td class="muted" style="white-space:nowrap">{{ t.triggered_at[:16].replace("T"," ") if t.triggered_at else "—" }}</td>
              <td><strong>{{ t.ticker }}</strong></td>
              <td class="neg-pnl">-{{ "%.1f"|format(t.drop_pct) }}%</td>
              <td>${{ "%.2f"|format(t.trigger_price) }}</td>
              <td>
                {% if t.recommendation %}
                  <span class="badge badge-{{ t.recommendation }}">{{ t.recommendation }}</span>
                {% elif t.budget_status == "budget_exhausted" %}
                  <span class="badge badge-low">budget exhausted</span>
                {% else %}
                  <span class="muted">pending</span>
                {% endif %}
              </td>
              <td>{{ t.overreaction_score if t.overreaction_score is not none else "—" }}</td>
              <td>
                {% if t.confidence %}
                  <span class="badge badge-{{ t.confidence }}">{{ t.confidence }}</span>
                {% else %}—{% endif %}
              </td>
              <td>{{ t.cause_category or "—" }}</td>
              <td>
                {% if t.position_pnl_pct is not none %}
                  <span class="{{ 'pos-pnl' if t.position_pnl_pct > 0 else 'neg-pnl' }}">
                    {{ "%+.2f"|format(t.position_pnl_pct) }}%
                  </span>
                {% else %}—{% endif %}
              </td>
            </tr>
            {% endfor %}
          {% else %}
            <tr class="empty"><td colspan="9">No triggers yet.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>

  <!-- Classifier performance -->
  <div class="section">
    <h2>Classifier performance</h2>
    <div class="perf-grid">
      <div>
        <table class="perf-table">
          <thead><tr><th>Confidence</th><th>Trades</th><th>Win rate</th></tr></thead>
          <tbody>
            {% if classifier.by_confidence %}
              {% for conf, stats in classifier.by_confidence.items() %}
              <tr>
                <td><span class="badge badge-{{ conf }}">{{ conf }}</span></td>
                <td>{{ stats.total }}</td>
                <td class="{{ 'pos-pnl' if stats.win_rate_pct >= 50 else 'neg-pnl' }}">{{ stats.win_rate_pct }}%</td>
              </tr>
              {% endfor %}
            {% else %}
              <tr class="empty"><td colspan="3">No data yet.</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
      <div>
        <table class="perf-table">
          <thead><tr><th>Category</th><th>Trades</th><th>Win rate</th></tr></thead>
          <tbody>
            {% if classifier.by_cause_category %}
              {% for cat, stats in classifier.by_cause_category.items() %}
              <tr>
                <td>{{ cat }}</td>
                <td>{{ stats.total }}</td>
                <td class="{{ 'pos-pnl' if stats.win_rate_pct >= 50 else 'neg-pnl' }}">{{ stats.win_rate_pct }}%</td>
              </tr>
              {% endfor %}
            {% else %}
              <tr class="empty"><td colspan="3">No data yet.</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
    </div>
  </div>

</main>
</body>
</html>
"""


class DashboardServer:
    def __init__(self, db: Database, port: int):
        self.db = db
        self.port = port
        self.app = Flask(__name__)
        self._configure_routes()

    def _configure_routes(self) -> None:
        @self.app.route("/")
        def index():
            return render_template_string(
                _TEMPLATE,
                summary=self.db.portfolio_summary(),
                open_positions=self.db.list_open_positions_with_classification(),
                closed_positions=self.db.list_recent_closed_positions(50),
                triggers=self.db.list_recent_triggers(50),
                classifier=self.db.get_classifier_rollups(),
            )

    def start(self) -> threading.Thread:
        thread = threading.Thread(
            target=self.app.run,
            kwargs={"host": "127.0.0.1", "port": self.port, "debug": False, "use_reloader": False},
            daemon=True,
        )
        thread.start()
        return thread
