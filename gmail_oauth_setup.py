from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests
import yaml


REDIRECT_HOST = "127.0.0.1"
REDIRECT_PORT = 8765
REDIRECT_PATH = "/"
REDIRECT_URI = f"http://{REDIRECT_HOST}:{REDIRECT_PORT}{REDIRECT_PATH}"
GMAIL_SCOPE = "https://www.googleapis.com/auth/gmail.send"


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != REDIRECT_PATH:
            self.send_response(404)
            self.end_headers()
            return

        params = parse_qs(parsed.query)
        self.server.auth_code = params.get("code", [None])[0]  # type: ignore[attr-defined]
        self.server.auth_error = params.get("error", [None])[0]  # type: ignore[attr-defined]

        body = (
            "Authorization received. You can close this tab and return to the terminal."
            if self.server.auth_code  # type: ignore[attr-defined]
            else "Authorization failed. Return to the terminal for details."
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def load_partial_config() -> dict[str, str]:
    config_path = Path(__file__).resolve().parent / "config.yaml"
    if not config_path.exists():
        raise SystemExit(f"config.yaml not found at {config_path}")

    payload = yaml.safe_load(config_path.read_text()) or {}
    if not isinstance(payload, dict):
        raise SystemExit("config.yaml must contain a top-level mapping/object.")

    required = {
        "gmail_client_id": payload.get("gmail_client_id", ""),
        "gmail_client_secret": payload.get("gmail_client_secret", ""),
    }
    missing = [name for name, value in required.items() if not str(value).strip()]
    if missing:
        raise SystemExit(
            "Missing required config values for Gmail OAuth setup: " + ", ".join(missing)
        )
    return payload


def main() -> None:
    payload = load_partial_config()
    query = urlencode(
        {
            "client_id": payload["gmail_client_id"],
            "redirect_uri": REDIRECT_URI,
            "response_type": "code",
            "scope": GMAIL_SCOPE,
            "access_type": "offline",
            "prompt": "consent",
        }
    )
    auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{query}"

    print("Open this URL in your browser and approve Gmail send access:")
    print()
    print(auth_url)
    print()
    print(
        "If Google reports a redirect URI mismatch, add this redirect URI to your OAuth client:"
    )
    print(REDIRECT_URI)
    print()
    print(f"Waiting for the redirect on {REDIRECT_URI} ...")

    server = HTTPServer((REDIRECT_HOST, REDIRECT_PORT), OAuthCallbackHandler)
    server.auth_code = None  # type: ignore[attr-defined]
    server.auth_error = None  # type: ignore[attr-defined]
    server.handle_request()

    if server.auth_error:  # type: ignore[attr-defined]
        raise SystemExit(f"Google returned an authorization error: {server.auth_error}")  # type: ignore[attr-defined]
    if not server.auth_code:  # type: ignore[attr-defined]
        raise SystemExit("No authorization code was returned.")

    response = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": payload["gmail_client_id"],
            "client_secret": payload["gmail_client_secret"],
            "code": server.auth_code,  # type: ignore[attr-defined]
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI,
        },
        timeout=30,
    )
    response.raise_for_status()
    tokens = response.json()
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise SystemExit(
            "Google did not return a refresh token. Remove the app's access, then run this helper again."
        )

    print()
    print("Paste this value into config.yaml as gmail_refresh_token:")
    print()
    print(refresh_token)


if __name__ == "__main__":
    main()
