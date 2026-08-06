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
      "date": "2026-07-24"
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

import io
import os
import sys
import time
import random
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import yfinance as yf

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

from config_loader import load_tickers, load_exchanges
from parquet_writer import write_parquet

logger = Logger()
metrics = Metrics(namespace="MarketData")

BATCH_SIZE = 50
BATCH_SLEEP_S = 0.5
MAX_RETRY = 3
RETRY_BASE_S = 5


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


def _fetch_batch_with_retry(tickers: list[str]) -> pd.DataFrame | None:
    """Download 1-day 30-minute bars for a batch of tickers with retry on 429."""
    ticker_str = " ".join(tickers)
    for attempt in range(MAX_RETRY):
        try:
            df = yf.download(
                ticker_str,
                period="1d",
                interval="30m",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            return df
        except Exception as exc:
            err_str = str(exc).lower()
            if "429" in err_str or "rate" in err_str:
                sleep_s = RETRY_BASE_S * (2**attempt) + random.uniform(0, 3)
                logger.warning(
                    "Rate limited by Yahoo Finance, backing off",
                    extra={"attempt": attempt + 1, "sleep_s": sleep_s},
                )
                time.sleep(sleep_s)
            else:
                logger.error("yfinance download error", extra={"error": str(exc)})
                return None
    return None


def _extract_latest_row(df: pd.DataFrame, ticker: str) -> dict | None:
    """Pull the most recent bar for a single ticker from a multi-ticker df."""
    try:
        if isinstance(df.columns, pd.MultiIndex):
            ticker_df = df[ticker].dropna(how="all")
        else:
            ticker_df = df.dropna(how="all")
        if ticker_df.empty:
            return None
        row = ticker_df.iloc[-1]
        return {
            "price": float(row.get("Close", row.get("close", float("nan")))),
            "open": float(row.get("Open", row.get("open", float("nan")))),
            "high": float(row.get("High", row.get("high", float("nan")))),
            "low": float(row.get("Low", row.get("low", float("nan")))),
            "close": float(row.get("Close", row.get("close", float("nan")))),
            "volume": int(row.get("Volume", row.get("volume", 0))),
            "timestamp": ticker_df.index[-1]
            .to_pydatetime()
            .replace(tzinfo=timezone.utc),
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

    for batch_idx, batch in enumerate(batches):
        df = _fetch_batch_with_retry(batch)

        batch_rows: list[dict] = []
        for ticker in batch:
            row = _extract_latest_row(df, ticker) if df is not None else None
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
            batch_key = f"{s3_prefix}batch_{batch_idx+1:04d}.parquet"
            write_parquet(batch_df, batch_key, schema=STOCKS_SCHEMA)
            batches_written += 1

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
    }
