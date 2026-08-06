"""
Lambda: collect-corporate-actions

Runs ONCE per day at market open for each exchange.
Pulls stock splits, dividends, and spinoff/merger events for every
ticker on the exchange using yfinance, then writes Parquet to S3.

Why this matters
----------------
- Stock splits change the share count and price per share overnight.
  Without tracking them you cannot distinguish a genuine price crash
  from a 4-for-1 split.
- Dividends are real cash returned to shareholders. They are essential
  for calculating total return (price appreciation + income).
- Knowing the event date lets you annotate price charts, avoid false
  anomaly alerts, and adjust historical data correctly.

Input:
    {
      "exchange": "NASDAQ",
      "date": "2026-07-24"
    }

Output:
    {
      "exchange": "NASDAQ",
      "date": "2026-07-24",
      "splits_found": 3,
      "dividends_found": 47,
      "tickers_failed": 2,
      "splits_s3_key": "corporate_actions/type=split/exchange=NASDAQ/date=2026-07-24/data.parquet",
      "dividends_s3_key": "corporate_actions/type=dividend/exchange=NASDAQ/date=2026-07-24/data.parquet"
    }
"""
from __future__ import annotations

import os
import sys
import time
import random
from datetime import date, datetime, timezone, timedelta
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import pandas as pd
import pyarrow as pa
import yfinance as yf

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

from config_loader import load_tickers
from parquet_writer import write_parquet

logger = Logger()
metrics = Metrics(namespace="MarketData")

# Only pull actions that occurred within the last N days on a daily run.
# On first-ever run for an exchange you may want to set this higher,
# but for the scheduled daily job 2 days gives a safe overlap buffer.
LOOKBACK_DAYS = 2
BATCH_SLEEP_S = 0.3
MAX_RETRY = 3
RETRY_BASE_S = 5


# ── Parquet schemas ────────────────────────────────────────────────────────────

SPLITS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("event_date", pa.date32()),       # date the split took effect
        pa.field("ratio", pa.float64()),            # e.g. 4.0 means 4-for-1
        pa.field("collection_date", pa.date32()),   # date we collected this record
    ]
)

DIVIDENDS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("ex_date", pa.date32()),           # ex-dividend date
        pa.field("amount", pa.float64()),           # dividend per share (in trading currency)
        pa.field("collection_date", pa.date32()),
    ]
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_float(v: Any) -> float:
    try:
        return float(v) if v is not None else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _fetch_actions_with_retry(ticker_symbol: str) -> tuple[pd.Series, pd.Series]:
    """
    Return (splits, dividends) Series for a single ticker.
    Both are indexed by the event date.
    Returns (empty, empty) on failure.
    """
    for attempt in range(MAX_RETRY):
        try:
            t = yf.Ticker(ticker_symbol)
            splits = t.splits        # pd.Series, index=DatetimeIndex, values=ratio
            dividends = t.dividends  # pd.Series, index=DatetimeIndex, values=amount
            return splits, dividends
        except Exception as exc:
            err_str = str(exc).lower()
            if "429" in err_str or "rate" in err_str:
                sleep_s = RETRY_BASE_S * (2 ** attempt) + random.uniform(0, 2)
                logger.warning(
                    "Rate limited fetching corporate actions, backing off",
                    extra={"ticker": ticker_symbol, "attempt": attempt + 1, "sleep_s": sleep_s},
                )
                time.sleep(sleep_s)
            else:
                logger.debug(
                    "Error fetching actions for ticker",
                    extra={"ticker": ticker_symbol, "error": str(exc)},
                )
                return pd.Series(dtype=float), pd.Series(dtype=float)
    return pd.Series(dtype=float), pd.Series(dtype=float)


def _filter_recent(series: pd.Series, lookback_days: int) -> pd.Series:
    """Keep only events within the last `lookback_days` days."""
    if series.empty:
        return series
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=lookback_days)
    # Some series have tz-aware index, some don't — normalise
    if series.index.tz is None:
        series.index = series.index.tz_localize("UTC")
    return series[series.index >= cutoff]


# ── Handler ────────────────────────────────────────────────────────────────────

@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    exchange: str = event["exchange"]
    date_str: str = event["date"][:10]
    collection_date = date.fromisoformat(date_str)

    tickers = load_tickers(exchange)
    if not tickers:
        logger.warning("No tickers for exchange", extra={"exchange": exchange})
        return {
            "exchange": exchange,
            "date": date_str,
            "splits_found": 0,
            "dividends_found": 0,
            "tickers_failed": 0,
            "splits_s3_key": "",
            "dividends_s3_key": "",
        }

    logger.info(
        "Starting corporate actions collection",
        extra={"exchange": exchange, "ticker_count": len(tickers), "lookback_days": LOOKBACK_DAYS},
    )

    split_rows: list[dict] = []
    dividend_rows: list[dict] = []
    tickers_failed = 0

    for idx, symbol in enumerate(tickers):
        splits, dividends = _fetch_actions_with_retry(symbol)

        # ── Splits ─────────────────────────────────────────────────────────
        recent_splits = _filter_recent(splits, LOOKBACK_DAYS)
        for event_ts, ratio in recent_splits.items():
            split_rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "event_date": event_ts.date(),
                    "ratio": _safe_float(ratio),
                    "collection_date": collection_date,
                }
            )
            logger.info(
                "Split detected",
                extra={"symbol": symbol, "ratio": ratio, "event_date": str(event_ts.date())},
            )

        # ── Dividends ───────────────────────────────────────────────────────
        recent_dividends = _filter_recent(dividends, LOOKBACK_DAYS)
        for event_ts, amount in recent_dividends.items():
            dividend_rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "ex_date": event_ts.date(),
                    "amount": _safe_float(amount),
                    "collection_date": collection_date,
                }
            )

        if splits is None and dividends is None:
            tickers_failed += 1

        # Polite pacing — one ticker at a time to avoid hammering Yahoo
        if idx < len(tickers) - 1:
            time.sleep(BATCH_SLEEP_S)

    # ── Write splits Parquet ──────────────────────────────────────────────────
    splits_s3_key = ""
    if split_rows:
        splits_df = pd.DataFrame(split_rows)
        splits_s3_key = (
            f"corporate_actions/type=split/exchange={exchange}/date={date_str}/data.parquet"
        )
        write_parquet(splits_df, splits_s3_key, schema=SPLITS_SCHEMA)
        logger.info(
            "Splits written to S3",
            extra={"key": splits_s3_key, "rows": len(split_rows)},
        )

    # ── Write dividends Parquet ───────────────────────────────────────────────
    dividends_s3_key = ""
    if dividend_rows:
        dividends_df = pd.DataFrame(dividend_rows)
        dividends_s3_key = (
            f"corporate_actions/type=dividend/exchange={exchange}/date={date_str}/data.parquet"
        )
        write_parquet(dividends_df, dividends_s3_key, schema=DIVIDENDS_SCHEMA)
        logger.info(
            "Dividends written to S3",
            extra={"key": dividends_s3_key, "rows": len(dividend_rows)},
        )

    # ── Metrics ───────────────────────────────────────────────────────────────
    metrics.add_dimension(name="Exchange", value=exchange)
    metrics.add_metric(name="SplitsFound", unit=MetricUnit.Count, value=len(split_rows))
    metrics.add_metric(name="DividendsFound", unit=MetricUnit.Count, value=len(dividend_rows))
    metrics.add_metric(name="CorporateActionsFailed", unit=MetricUnit.Count, value=tickers_failed)

    return {
        "exchange": exchange,
        "date": date_str,
        "splits_found": len(split_rows),
        "dividends_found": len(dividend_rows),
        "tickers_failed": tickers_failed,
        "splits_s3_key": splits_s3_key,
        "dividends_s3_key": dividends_s3_key,
    }
