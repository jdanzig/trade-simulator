# News-Driven Dip Classifier

Long-running paper trading simulation that watches the S&P 500 or Nasdaq-100 during market hours, detects sharp intraday drawdowns, classifies the news catalyst with Claude, tracks hypothetical positions, sends email reports, and serves a local read-only dashboard.

## Setup

1. Create a virtual environment and install dependencies:
   `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
2. Fill out `config.yaml` with your API keys, Gmail OAuth values, and report email.
3. If you do not already have a Gmail refresh token, run:
   `python3 gmail_oauth_setup.py`
   If Google reports a redirect mismatch, add `http://127.0.0.1:8765/` as an allowed redirect URI for that OAuth client.
4. Start the daemon:
   `python3 main.py`

## Runtime behavior

- The same entry point handles first run and subsequent runs.
- On first run the app creates the SQLite database, creates `classifier_prompt.md`, creates `findings.md`, validates the configured providers, refreshes the universe, starts the dashboard, and starts the scheduler.
- The dashboard is available at `http://127.0.0.1:8080` by default.
- The system never places trades. All positions are hypothetical and stored in SQLite.
- Gmail report delivery uses OAuth refresh tokens so the daemon can keep sending email unattended.

## Files created at runtime

- `data/trade_simulator.sqlite3`
- `classifier_prompt.md`
- `findings.md`
