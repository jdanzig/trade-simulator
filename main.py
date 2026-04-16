from __future__ import annotations

from pathlib import Path

from trade_simulator.app import create_app


def main() -> None:
    app = create_app(Path(__file__).resolve().parent)
    app.run()


if __name__ == "__main__":
    main()
