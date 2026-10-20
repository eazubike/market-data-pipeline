"""
Lambda: collect-stocks

Pulls OHLCV price snapshots for every ticker on a given exchange using
yfinance (Yahoo Finance), then writes Parquet to S3 per-batch so partial
progress is preserved even if the Lambda times out.

Fundamentals are collected separately by an ECS Fargate task (once daily).

Data source: Yahoo Finance via the yfinance library (no API key required).
Rate limiting is handled with per-batch sleeps and exponential backoff on 429s.

Input:
    {
      "exchange": "NASDAQ",
      "run_timestamp": "2026-07-24T14:30:00Z",
      "date": "2026-07-24",
      "offset": 0,      (optional) first ticker index of this chunk
      "limit": 400      (optional) number of tickers in this chunk
    }

Output:
    {
      "exchange": "NASDAQ",
      "tickers_ok": 3412,
      "tickers_failed": 8,
      "stocks_s3_prefix": "stocks/date=.../run_timestamp=.../exchange=NASDAQ/"
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
from datetime import datetime, timezone
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

STOCKS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("price", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
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


def _fetch_one(ticker: str) -> pd.DataFrame:
    return yf.Ticker(_yahoo_symbol(ticker)).history(
        period="1d",
        interval="30m",
        auto_adjust=True,
        timeout=TICKER_TIMEOUT_S,
        raise_errors=True,
    )


def _fetch_batch(
    tickers: list[str], deadline_s: float
) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    """
    Fetch 1-day 30-minute bars for each ticker concurrently, never waiting past
    deadline_s. Rate-limited tickers are retried with backoff.

    Returns (frames by ticker, error by failed ticker).
    """
    frames: dict[str, pd.DataFrame] = {}
    errors: dict[str, str] = {}
    pending = list(tickers)
    deadline = time.monotonic() + deadline_s

    for attempt in range(MAX_RETRY + 1):
        rate_limited: list[str] = []
        pool = ThreadPoolExecutor(max_workers=FETCH_WORKERS)
        futures = {pool.submit(_fetch_one, t): t for t in pending}
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


def _extract_latest_row(ticker_df: pd.DataFrame) -> dict | None:
    """Pull the most recent bar from a single ticker's history df."""
    try:
        ticker_df = ticker_df.dropna(how="all")
        if ticker_df.empty:
            return None
        row = ticker_df.iloc[-1]
        ts = pd.Timestamp(ticker_df.index[-1])
        ts = ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")
        return {
            "price": float(row.get("Close", row.get("close", float("nan")))),
            "open": float(row.get("Open", row.get("open", float("nan")))),
            "high": float(row.get("High", row.get("high", float("nan")))),
            "low": float(row.get("Low", row.get("low", float("nan")))),
            "close": float(row.get("Close", row.get("close", float("nan")))),
            "volume": int(row.get("Volume", row.get("volume", 0))),
            "timestamp": ts.to_pydatetime(),
        }
    except Exception:
        return None


def _run_timestamp_to_s3_safe(ts: str) -> str:
    """Convert '2026-07-24T14:30:00Z' → '2026-07-24T14-30-00Z' (S3-key safe)."""
    return ts.replace(":", "-").rstrip("Z") + "Z"


# ── Handler ────────────────────────────────────────────────────────────────────


@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    exchange: str = event["exchange"]
    run_timestamp: str = event["run_timestamp"]  # ISO-8601 UTC
    date_str: str = run_timestamp[:10]  # "2026-07-24"
    run_ts_safe = _run_timestamp_to_s3_safe(run_timestamp)

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
            "stocks_s3_key": "",
        }

    logger.info(
        "Starting price collection",
        extra={"exchange": exchange, "ticker_count": len(tickers)},
    )

    s3_prefix = (
        f"stocks/date={date_str}/run_timestamp={run_ts_safe}/exchange={exchange}/"
    )
    tickers_ok = 0
    tickers_failed = 0
    batches_written = 0

    batches = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    # Global batch numbering keeps file names unique across chunks that share
    # the same S3 prefix (offset is a multiple of BATCH_SIZE).
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
        frames, errors = _fetch_batch(batch, deadline_s)

        batch_rows: list[dict] = []
        for ticker in batch:
            df = frames.get(ticker)
            row = _extract_latest_row(df) if df is not None else None
            if row is None:
                tickers_failed += 1
                continue

            batch_rows.append(
                {
                    "symbol": ticker,
                    "exchange": exchange,
                    "timestamp": row["timestamp"],
                    "price": row["price"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "currency": currency,
                }
            )
            tickers_ok += 1

        # Write this batch to S3 immediately
        if batch_rows:
            batch_df = pd.DataFrame(batch_rows)
            batch_key = f"{s3_prefix}batch_{first_batch_no + batch_idx + 1:04d}.parquet"
            write_parquet(batch_df, batch_key, schema=STOCKS_SCHEMA)
            batches_written += 1

        batch_ok = len(batch_rows)
        del frames, batch_rows
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

    # ── Emit CloudWatch metrics ───────────────────────────────────────────────
    metrics.add_dimension(name="Exchange", value=exchange)
    metrics.add_metric(name="StocksCollected", unit=MetricUnit.Count, value=tickers_ok)
    metrics.add_metric(name="StocksFailed", unit=MetricUnit.Count, value=tickers_failed)

    logger.info(
        "Price collection complete",
        extra={
            "exchange": exchange,
            "tickers_ok": tickers_ok,
            "tickers_failed": tickers_failed,
            "batches_written": batches_written,
            "s3_prefix": s3_prefix,
        },
    )

    return {
        "exchange": exchange,
        "tickers_ok": tickers_ok,
        "tickers_failed": tickers_failed,
        "stocks_s3_prefix": s3_prefix,
        "incomplete": incomplete,
    }
