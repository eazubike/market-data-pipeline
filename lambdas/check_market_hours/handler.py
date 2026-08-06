"""
Lambda: check-market-hours

Determines which exchanges are currently in their trading session and
returns the list so the Step Functions Map state knows what to collect.

Input:
    { "trigger_time": "2026-07-24T14:30:00Z" }   (optional; defaults to now)

Output:
    { "open_exchanges": ["NASDAQ", "NYSE", "LSE"] }
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

from market_hours import get_open_exchanges

logger = Logger()


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

    logger.info(
        "Market hours check complete",
        extra={
            "now_utc": now_utc.isoformat(),
            "open_exchanges": open_exchanges,
            "count": len(open_exchanges),
        },
    )

    return {"open_exchanges": open_exchanges}
