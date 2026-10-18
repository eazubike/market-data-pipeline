"""
Lambda: check-market-hours

Determines which exchanges are currently in their trading session and
returns the list so the Step Functions Map state knows what to collect.

Input:
    { "trigger_time": "2026-07-24T14:30:00Z" }   (optional; defaults to now)

Output:
    {
      "open_exchanges": ["NASDAQ", "NYSE", "LSE"],
      "chunks": [{"exchange": "NASDAQ", "offset": 0, "limit": 400}, ...]
    }

Chunks split each exchange's ticker list into CHUNK_SIZE pieces so every
collect-stocks invocation finishes in a minute or two instead of risking the
Lambda timeout (and a full re-run of the exchange on retry).
"""
from __future__ import annotations

import sys
import os

# Allow imports from the shared/ directory (bundled alongside handler.py)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

from datetime import datetime
from zoneinfo import ZoneInfo

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from config_loader import load_tickers
from market_hours import get_open_exchanges

logger = Logger()

# Must stay a multiple of collect_stocks BATCH_SIZE (50) so batch file
# numbering never collides between chunks of the same exchange.
CHUNK_SIZE = 400


def _plan_chunks(open_exchanges: list[str]) -> list[dict]:
    chunks: list[dict] = []
    for exchange in open_exchanges:
        total = len(load_tickers(exchange))
        for offset in range(0, total, CHUNK_SIZE):
            chunks.append(
                {"exchange": exchange, "offset": offset, "limit": CHUNK_SIZE}
            )
    return chunks


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    trigger_time_str: str | None = event.get("trigger_time")

    if trigger_time_str:
        # Normalise from ISO-8601 (Step Functions passes with or without Z)
        trigger_time_str = trigger_time_str.replace("Z", "+00:00")
        now_utc = datetime.fromisoformat(trigger_time_str).replace(
            tzinfo=ZoneInfo("UTC")
        )
    else:
        now_utc = datetime.now(tz=ZoneInfo("UTC"))

    open_exchanges = get_open_exchanges(now_utc)
    chunks = _plan_chunks(open_exchanges)

    logger.info(
        "Market hours check complete",
        extra={
            "now_utc": now_utc.isoformat(),
            "open_exchanges": open_exchanges,
            "count": len(open_exchanges),
            "chunk_count": len(chunks),
        },
    )

    return {"open_exchanges": open_exchanges, "chunks": chunks}
