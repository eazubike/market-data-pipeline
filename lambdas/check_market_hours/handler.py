"""
Lambda: check-market-hours

Plans the daily-bar collection and records which exchange-days are done.
Runs every 2 hours; most runs find nothing ready and return no chunks.

Plan (default action):
    Input:  { "trigger_time": "...Z" (optional),
              "backfill_start": "2025-10-22" (optional, one-off: all
                exchanges from that date, ignoring readiness/state) }
    Output: {
      "open_exchanges": ["TSE", ...],          # exchanges to collect now
      "ready": [{"exchange": "TSE", "trade_date": "2026-10-22",
                 "start_date": "2026-10-22"}, ...],
      "chunks": [{"exchange": "TSE", "offset": 0, "limit": 400,
                  "start_date": "...", "end_date": "..."}, ...]
    }
    An exchange is ready once its trading day is finished: in its own
    timezone, a trading day, >= 2h after close, and not yet collected
    (see market_hours.get_exchanges_ready_for_daily).

Mark (action="mark", called after collection):
    Input:  { "action": "mark", "ready": [...], "stocks_results": [...] }
    Records trade_date as collected for every ready exchange whose chunks
    all succeeded, in s3://<bucket>/state/stocks_daily.json. An exchange
    with a failed or incomplete chunk stays pending and the next run retries.
"""
from __future__ import annotations

import json
import os
import sys

# Allow imports from the shared/ directory (bundled alongside handler.py)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from config_loader import load_exchanges, load_tickers
from market_hours import get_exchanges_ready_for_daily

logger = Logger()
s3 = boto3.client("s3")

DATA_BUCKET = os.environ["DATA_BUCKET"]
STATE_KEY = "state/stocks_daily.json"

# Must stay a multiple of collect_stocks BATCH_SIZE (50) so batch file
# numbering never collides between chunks of the same exchange.
CHUNK_SIZE = 400


def _load_state() -> dict[str, str]:
    """{exchange: last collected trade_date}."""
    try:
        body = s3.get_object(Bucket=DATA_BUCKET, Key=STATE_KEY)["Body"].read()
        return json.loads(body)
    except s3.exceptions.NoSuchKey:
        return {}


def _plan_chunks(ready: list[dict]) -> list[dict]:
    chunks: list[dict] = []
    for r in ready:
        total = len(load_tickers(r["exchange"]))
        for offset in range(0, total, CHUNK_SIZE):
            chunks.append(
                {
                    "exchange": r["exchange"],
                    "offset": offset,
                    "limit": CHUNK_SIZE,
                    "start_date": r["start_date"],
                    "end_date": r["trade_date"],
                }
            )
    return chunks


def _plan(event: dict) -> dict:
    trigger_time_str: str | None = event.get("trigger_time")
    if trigger_time_str:
        now_utc = datetime.fromisoformat(trigger_time_str.replace("Z", "+00:00"))
    else:
        now_utc = datetime.now(tz=ZoneInfo("UTC"))

    backfill_start = event.get("backfill_start")
    if backfill_start:
        # One-off history load: every exchange, from backfill_start up to its
        # last weekday *before* today (local). Today may still be trading; the
        # normal 2-hourly check collects it once it's final.
        ready = []
        for code, cfg in load_exchanges().items():
            last_done = now_utc.astimezone(ZoneInfo(cfg["timezone"])).date() - timedelta(days=1)
            while last_done.weekday() >= 5:
                last_done -= timedelta(days=1)
            ready.append(
                {
                    "exchange": code,
                    "trade_date": last_done.isoformat(),
                    "start_date": backfill_start,
                }
            )
    else:
        ready = get_exchanges_ready_for_daily(now_utc, _load_state())

    chunks = _plan_chunks(ready)
    logger.info(
        "Daily collection plan",
        extra={
            "now_utc": now_utc.isoformat(),
            "backfill_start": backfill_start,
            "ready": ready,
            "chunk_count": len(chunks),
        },
    )
    return {
        "open_exchanges": [r["exchange"] for r in ready],
        "ready": ready,
        "chunks": chunks,
    }


def _mark(event: dict) -> dict:
    """Record exchanges whose chunks all succeeded (and none ran out of time)."""
    failed: set[str] = set()
    for item in event.get("stocks_results", []):
        payload = (item.get("stock_result") or {}).get("Payload")
        if (
            not isinstance(payload, dict)
            or payload.get("incomplete")
            # A chunk that got nothing at all (e.g. Yahoo outage) must not
            # mark the day as done, or that day would never be fetched.
            or (payload.get("tickers_ok", 0) == 0 and payload.get("tickers_failed", 0) > 0)
        ):
            failed.add(item.get("exchange", ""))

    state = _load_state()
    marked = []
    for r in event.get("ready", []):
        if r["exchange"] in failed:
            continue
        if state.get(r["exchange"], "") < r["trade_date"]:
            state[r["exchange"]] = r["trade_date"]
            marked.append(r["exchange"])

    s3.put_object(
        Bucket=DATA_BUCKET,
        Key=STATE_KEY,
        Body=json.dumps(state, indent=1, sort_keys=True).encode(),
        ContentType="application/json",
    )
    logger.info(
        "Daily collection marked",
        extra={"marked": marked, "pending_retry": sorted(failed - {""})},
    )
    return {"marked": marked, "pending_retry": sorted(failed - {""})}


@logger.inject_lambda_context(log_event=False)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    if event.get("action") == "mark":
        return _mark(event)
    return _plan(event)
