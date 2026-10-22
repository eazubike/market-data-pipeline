"""
Lambda: collect-stocks

Pulls daily OHLCV bars (start_date..end_date, exchange-local dates) for one
chunk of an exchange's tickers using yfinance, and writes one Parquet file per
bar date: stocks_daily/date=<bar date>/exchange=<X>/part-<offset>.parquet.
File names are fixed per chunk, so a retry overwrites instead of duplicating.

A normal run fetches just the finished trading day (start = end = trade
date); after a missed run start is the day after the last collected day; the
one-off backfill uses start = a year ago. Same request count either way.

Data source: Yahoo Finance via the yfinance library (no API key required).
Rate limiting is handled with per-batch sleeps and exponential backoff on 429s.

Input:
    {
      "exchange": "NASDAQ",
      "start_date": "2026-10-22",
      "end_date": "2026-10-22",
      "offset": 0,      (optional) first ticker index of this chunk
      "limit": 400      (optional) number of tickers in this chunk
    }

Output:
    {
      "exchange": "NASDAQ",
      "tickers_ok": 390,
      "tickers_failed": 10,
      "rows_written": 390,
      "dates_written": 1,
      "incomplete": false
    }
"""

from __future__ import annotations

import gc
import io
import os
import resource
import sys
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import yfinance as yf
from yfinance.exceptions import YFRateLimitError

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

from config_loader import load_tickers, load_exchanges
from parquet_writer import write_parquet

logger = Logger()
metrics = Metrics(namespace="MarketData")

BATCH_SIZE = 50
BATCH_SLEEP_S = 0.5
MAX_RETRY = 2
RETRY_BASE_S = 3
# Per-ticker fetches run on our own pool instead of yf.download(threads=True),
# whose wait loop has no timeout and hung whole invocations until Lambda killed them.
FETCH_WORKERS = 4  # ~old per-Lambda request rate; 8 tripped Yahoo's rate limit in testing
TICKER_TIMEOUT_S = 10  # per HTTP request
BATCH_DEADLINE_S = 60  # hard cap for one 50-ticker batch; stragglers are abandoned
# Stop starting new batches when less than this much time is left, so the
# invocation returns normally (partial data already in S3) instead of timing out.
MIN_REMAINING_MS = 45_000


# ── Parquet schema ─────────────────────────────────────────────────────────────

# Raw OHLC as traded plus adj_close (split/dividend adjusted) for returns.
DAILY_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("trade_date", pa.date32()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("adj_close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("currency", pa.string()),
    ]
)


# ── Helpers ────────────────────────────────────────────────────────────────────


def _yahoo_symbol(ticker: str) -> str:
    """Map exchange-style symbols to Yahoo's: 'BRK/A' → 'BRK-A', 'TRTN^A' → 'TRTN-PA'."""
    if "^" in ticker:
        base, cls = ticker.split("^", 1)
        return f"{base}-P{cls}"
    return ticker.replace("/", "-")


def _fetch_one(ticker: str, start: str, end_exclusive: str) -> pd.DataFrame:
    return yf.Ticker(_yahoo_symbol(ticker)).history(
        start=start,
        end=end_exclusive,
        interval="1d",
        auto_adjust=False,  # keep raw OHLC and the separate Adj Close
        actions=False,
        timeout=TICKER_TIMEOUT_S,
        raise_errors=True,
    )


def _fetch_batch(
    tickers: list[str], start: str, end_exclusive: str, deadline_s: float
) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    """
    Fetch daily bars [start, end_exclusive) for each ticker concurrently, never
    waiting past deadline_s. Rate-limited tickers are retried with backoff.

    Returns (frames by ticker, error by failed ticker).
    """
    frames: dict[str, pd.DataFrame] = {}
    errors: dict[str, str] = {}
    pending = list(tickers)
    deadline = time.monotonic() + deadline_s

    for attempt in range(MAX_RETRY + 1):
        rate_limited: list[str] = []
        pool = ThreadPoolExecutor(max_workers=FETCH_WORKERS)
        futures = {pool.submit(_fetch_one, t, start, end_exclusive): t for t in pending}
        try:
            for fut in as_completed(futures, timeout=max(deadline - time.monotonic(), 0)):
                ticker = futures[fut]
                try:
                    frames[ticker] = fut.result()
                except YFRateLimitError:
                    rate_limited.append(ticker)
                except Exception as exc:
                    errors[ticker] = type(exc).__name__
        except FuturesTimeout:
            for fut, ticker in futures.items():
                if not fut.done():
                    errors[ticker] = "BatchDeadline"
            logger.warning(
                "Batch deadline hit, abandoning slow tickers",
                extra={"abandoned": sum(1 for f in futures if not f.done())},
            )
            rate_limited = []
        finally:
            # Don't block on stragglers; each request ends on its own HTTP timeout.
            pool.shutdown(wait=False, cancel_futures=True)

        if not rate_limited:
            break
        sleep_s = RETRY_BASE_S * (2**attempt) + random.uniform(0, 3)
        if attempt == MAX_RETRY or time.monotonic() + sleep_s >= deadline:
            errors.update({t: "YFRateLimitError" for t in rate_limited})
            break
        logger.warning(
            "Rate limited by Yahoo Finance, backing off",
            extra={"attempt": attempt + 1, "tickers": len(rate_limited), "sleep_s": sleep_s},
        )
        time.sleep(sleep_s)
        pending = rate_limited

    return frames, errors


def _memory_mb() -> tuple[float, float]:
    """(current RSS, peak RSS) of this process in MB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB on Linux
    try:
        with open("/proc/self/statm") as f:
            current = int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1024**2
    except OSError:
        current = -1.0
    return round(current, 1), round(peak, 1)


def _daily_rows(ticker_df: pd.DataFrame, start: str, end: str) -> list[dict]:
    """
    One row per daily bar in [start, end]. The index is midnight in the
    exchange's timezone, so its date is the exchange-local trading date.
    """
    rows: list[dict] = []
    df = ticker_df.dropna(subset=["Close"])
    for ts, bar in df.iterrows():
        trade_date = pd.Timestamp(ts).date()
        if not (start <= trade_date.isoformat() <= end):
            continue  # Yahoo can add an in-progress bar outside the range
        close = float(bar["Close"])
        rows.append(
            {
                "trade_date": trade_date,
                "open": float(bar["Open"]),
                "high": float(bar["High"]),
                "low": float(bar["Low"]),
                "close": close,
                "adj_close": float(bar.get("Adj Close", close)),
                "volume": int(bar["Volume"]) if pd.notna(bar["Volume"]) else 0,
            }
        )
    return rows


# ── Handler ────────────────────────────────────────────────────────────────────


@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    exchange: str = event["exchange"]
    start_date: str = event["start_date"]
    end_date: str = event["end_date"]
    end_exclusive = (
        datetime.fromisoformat(end_date) + timedelta(days=1)
    ).date().isoformat()

    # Get currency from exchange config
    exchanges_config = load_exchanges()
    currency = exchanges_config.get(exchange, {}).get("currency", "")

    tickers = load_tickers(exchange)

    # Chunk support: process only [offset, offset+limit) of the exchange.
    offset: int = int(event.get("offset", 0) or 0)
    limit = event.get("limit")
    if limit:
        tickers = tickers[offset : offset + int(limit)]
    elif offset:
        tickers = tickers[offset:]
    if not tickers:
        logger.warning("No tickers found for exchange", extra={"exchange": exchange})
        return {
            "exchange": exchange,
            "tickers_ok": 0,
            "tickers_failed": 0,
            "rows_written": 0,
            "dates_written": 0,
            "incomplete": False,
        }

    logger.info(
        "Starting daily collection",
        extra={
            "exchange": exchange,
            "ticker_count": len(tickers),
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    tickers_ok = 0
    tickers_failed = 0
    chunk_rows: list[dict] = []

    batches = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    first_batch_no = offset // BATCH_SIZE
    incomplete = False

    for batch_idx, batch in enumerate(batches):
        if context.get_remaining_time_in_millis() < MIN_REMAINING_MS:
            incomplete = True
            logger.warning(
                "Low remaining time, stopping early",
                extra={"exchange": exchange, "next_batch": batch_idx, "of": len(batches)},
            )
            break

        batch_started = time.monotonic()
        # Leave ~20s after the batch for the S3 write and a clean return.
        deadline_s = min(
            BATCH_DEADLINE_S, context.get_remaining_time_in_millis() / 1000 - 20
        )
        frames, errors = _fetch_batch(batch, start_date, end_exclusive, deadline_s)

        batch_ok = 0
        for ticker in batch:
            df = frames.get(ticker)
            rows = _daily_rows(df, start_date, end_date) if df is not None else []
            if not rows:
                tickers_failed += 1
                continue
            for r in rows:
                r.update(symbol=ticker, exchange=exchange, currency=currency)
            chunk_rows.extend(rows)
            batch_ok += 1
        tickers_ok += batch_ok

        del frames
        gc.collect()
        rss_mb, peak_mb = _memory_mb()
        logger.info(
            "Batch complete",
            extra={
                "exchange": exchange,
                "batch": first_batch_no + batch_idx + 1,
                "duration_s": round(time.monotonic() - batch_started, 1),
                "ok": batch_ok,
                "failed": len(batch) - batch_ok,
                "failed_sample": dict(list(errors.items())[:10]),
                "rss_mb": rss_mb,
                "peak_rss_mb": peak_mb,
            },
        )

        # Respect rate limits between batches
        if batch_idx < len(batches) - 1:
            time.sleep(BATCH_SLEEP_S)

    # ── Write one file per bar date (fixed name per chunk: retries overwrite) ──
    dates_written = 0
    if chunk_rows:
        all_df = pd.DataFrame(chunk_rows)
        for trade_date, day_df in all_df.groupby("trade_date"):
            key = (
                f"stocks_daily/date={trade_date.isoformat()}/exchange={exchange}/"
                f"part-{offset:05d}.parquet"
            )
            write_parquet(day_df, key, schema=DAILY_SCHEMA)
            dates_written += 1

    # ── Emit CloudWatch metrics ───────────────────────────────────────────────
    metrics.add_dimension(name="Exchange", value=exchange)
    metrics.add_metric(name="StocksCollected", unit=MetricUnit.Count, value=tickers_ok)
    metrics.add_metric(name="StocksFailed", unit=MetricUnit.Count, value=tickers_failed)

    logger.info(
        "Daily collection complete",
        extra={
            "exchange": exchange,
            "tickers_ok": tickers_ok,
            "tickers_failed": tickers_failed,
            "rows_written": len(chunk_rows),
            "dates_written": dates_written,
            "incomplete": incomplete,
        },
    )

    return {
        "exchange": exchange,
        "tickers_ok": tickers_ok,
        "tickers_failed": tickers_failed,
        "rows_written": len(chunk_rows),
        "dates_written": dates_written,
        "incomplete": incomplete,
    }
