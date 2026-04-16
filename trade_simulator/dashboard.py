from __future__ import annotations

import threading

from flask import Flask, render_template_string

from .database import Database


class DashboardServer:
    def __init__(self, db: Database, port: int):
        self.db = db
        self.port = port
        self.app = Flask(__name__)
        self._configure_routes()

    def _configure_routes(self) -> None:
        @self.app.route("/")
        def index():
            open_positions = self.db.list_open_positions()
            performance = self.db.portfolio_performance()
            return render_template_string(
                """
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta http-equiv="refresh" content="60">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>Dip Classifier Dashboard</title>
                  <style>
                    :root {
                      --bg: #f5efe3;
                      --card: rgba(255, 252, 245, 0.92);
                      --ink: #1f2933;
                      --accent: #0f766e;
                      --accent-soft: #d7f3ef;
                      --border: rgba(31, 41, 51, 0.12);
                    }
                    body {
                      margin: 0;
                      font-family: Georgia, "Times New Roman", serif;
                      color: var(--ink);
                      background:
                        radial-gradient(circle at top left, rgba(15, 118, 110, 0.18), transparent 28%),
                        linear-gradient(135deg, #f7f1e6 0%, #efe6d6 50%, #f6f2eb 100%);
                      min-height: 100vh;
                    }
                    main {
                      max-width: 1080px;
                      margin: 0 auto;
                      padding: 32px 20px 60px;
                    }
                    .hero {
                      padding: 24px;
                      border-radius: 24px;
                      background: var(--card);
                      border: 1px solid var(--border);
                      box-shadow: 0 20px 60px rgba(15, 23, 42, 0.08);
                    }
                    .stats {
                      display: grid;
                      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                      gap: 16px;
                      margin-top: 24px;
                    }
                    .stat, table {
                      background: var(--card);
                      border: 1px solid var(--border);
                      border-radius: 18px;
                    }
                    .stat {
                      padding: 18px;
                    }
                    .stat strong {
                      display: block;
                      font-size: 1.8rem;
                      margin-top: 8px;
                    }
                    table {
                      width: 100%;
                      margin-top: 24px;
                      border-collapse: collapse;
                      overflow: hidden;
                    }
                    th, td {
                      padding: 14px 16px;
                      text-align: left;
                      border-bottom: 1px solid var(--border);
                    }
                    th {
                      background: var(--accent-soft);
                    }
                    tr:last-child td {
                      border-bottom: none;
                    }
                  </style>
                </head>
                <body>
                  <main>
                    <section class="hero">
                      <h1>News-Driven Dip Classifier</h1>
                      <p>Read-only paper trading dashboard. Refreshes every 60 seconds.</p>
                    </section>
                    <section class="stats">
                      <article class="stat">
                        <span>Open positions</span>
                        <strong>{{ open_positions|length }}</strong>
                      </article>
                      <article class="stat">
                        <span>Avg open P&amp;L</span>
                        <strong>{{ performance.today }}%</strong>
                      </article>
                      <article class="stat">
                        <span>Inception P&amp;L</span>
                        <strong>{{ performance.inception }}%</strong>
                      </article>
                    </section>
                    <table>
                      <thead>
                        <tr>
                          <th>Ticker</th>
                          <th>Entry Price</th>
                          <th>Current Price</th>
                          <th>P&amp;L</th>
                          <th>Days Held</th>
                        </tr>
                      </thead>
                      <tbody>
                        {% if open_positions %}
                          {% for position in open_positions %}
                            <tr>
                              <td>{{ position.ticker }}</td>
                              <td>{{ "%.2f"|format(position.hypothetical_entry_price) }}</td>
                              <td>{{ "%.2f"|format(position.current_price) }}</td>
                              <td>{{ "%.2f"|format(position.hypothetical_pnl_pct) }}%</td>
                              <td>{{ position.days_held }}</td>
                            </tr>
                          {% endfor %}
                        {% else %}
                          <tr>
                            <td colspan="5">No open hypothetical positions.</td>
                          </tr>
                        {% endif %}
                      </tbody>
                    </table>
                  </main>
                </body>
                </html>
                """,
                open_positions=open_positions,
                performance=performance,
            )

    def start(self) -> threading.Thread:
        thread = threading.Thread(
            target=self.app.run,
            kwargs={"host": "127.0.0.1", "port": self.port, "debug": False, "use_reloader": False},
            daemon=True,
        )
        thread.start()
        return thread
