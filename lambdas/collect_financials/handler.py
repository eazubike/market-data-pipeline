"""
Lambda: collect-financials

Runs once per WEEK (Sunday 06:00 UTC) per exchange.
Collects full financial statements for every ticker:

  - Income statement  (quarterly + annual)
  - Balance sheet     (quarterly + annual)
  - Cash flow         (quarterly + annual)

Why quarterly financial statements matter
-----------------------------------------
The .info dict gives headline numbers (revenue_ttm, net_income_ttm) but not
the underlying detail. Full statements let you compute:
  - Gross margin, operating margin, net margin
  - Current ratio, quick ratio (liquidity)
  - Free cash flow (operating CF - capex)
  - Return on equity, return on assets
  - Revenue growth rate QoQ and YoY

These are the numbers fundamental analysts use to value companies.
Running weekly is sufficient — financials are filed quarterly.

Input:
    { "exchange": "NASDAQ", "date": "2026-07-27" }

Output:
    {
      "exchange": "NASDAQ",
      "date": "2026-07-27",
      "income_ok": 3100,
      "balance_sheet_ok": 3100,
      "cashflow_ok": 3100,
      "tickers_failed": 20
    }
"""
from __future__ import annotations

import os
import sys
import time
import random
from datetime import date
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

BATCH_SLEEP_S = 0.5
MAX_RETRY = 3
RETRY_BASE_S = 5


# ── Parquet schema (shared for all three statement types) ──────────────────────
# We store statements in a long/tidy format:
#   one row per (symbol, period_type, period_end_date, line_item, value)
# This is more flexible than wide format — adding a new line item doesn't
# require a schema change.

STATEMENT_SCHEMA = pa.schema([
    pa.field("symbol",          pa.string()),
    pa.field("exchange",        pa.string()),
    pa.field("collection_date", pa.date32()),
    pa.field("statement_type",  pa.string()),   # "income" | "balance_sheet" | "cashflow"
    pa.field("period_type",     pa.string()),   # "quarterly" | "annual"
    pa.field("period_end_date", pa.date32()),   # date the reporting period ended
    pa.field("line_item",       pa.string()),   # e.g. "TotalRevenue", "NetIncome"
    pa.field("value",           pa.float64()),  # USD value (or ratio if applicable)
])


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_float(v: Any) -> float:
    try:
        return float(v) if v is not None else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _df_to_long(df: pd.DataFrame, symbol: str, exchange: str,
                statement_type: str, period_type: str,
                collection_date: date) -> list[dict]:
    """
    Convert a yfinance statement DataFrame (rows=line items, cols=period dates)
    into the long/tidy format used by our schema.
    """
    rows: list[dict] = []
    if df is None or df.empty:
        return rows
    for col in df.columns:
        # col is a Timestamp representing the period end date
        try:
            period_end = col.date() if hasattr(col, "date") else date.fromisoformat(str(col)[:10])
        except Exception:
            continue
        for line_item, raw_value in df[col].items():
            value = _safe_float(raw_value)
            if pd.isna(value):
                continue
            rows.append({
                "symbol":          symbol,
                "exchange":        exchange,
                "collection_date": collection_date,
                "statement_type":  statement_type,
                "period_type":     period_type,
                "period_end_date": period_end,
                "line_item":       str(line_item),
                "value":           value,
            })
    return rows


def _fetch_with_retry(symbol: str) -> yf.Ticker | None:
    for attempt in range(MAX_RETRY):
        try:
            return yf.Ticker(symbol)
        except Exception as exc:
            if "429" in str(exc):
                sleep_s = RETRY_BASE_S * (2 ** attempt) + random.uniform(0, 2)
                logger.warning("Rate limited", extra={"symbol": symbol, "sleep_s": sleep_s})
                time.sleep(sleep_s)
            else:
                return None
    return None


# ── Handler ────────────────────────────────────────────────────────────────────

@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    exchange:        str  = event["exchange"]
    date_str:        str  = event["date"][:10]  # extract YYYY-MM-DD from full timestamp
    collection_date: date = date.fromisoformat(date_str)

    tickers = load_tickers(exchange)
    if not tickers:
        logger.warning("No tickers", extra={"exchange": exchange})
        return {"exchange": exchange, "date": date_str,
                "income_ok": 0, "balance_sheet_ok": 0, "cashflow_ok": 0, "tickers_failed": 0}

    logger.info("Starting financials collection",
                extra={"exchange": exchange, "ticker_count": len(tickers)})

    all_rows: list[dict] = []
    income_ok = balance_ok = cashflow_ok = failed = 0

    for idx, symbol in enumerate(tickers):
        t = _fetch_with_retry(symbol)
        if t is None:
            failed += 1
            if idx < len(tickers) - 1:
                time.sleep(BATCH_SLEEP_S)
            continue

        try:
            # Quarterly statements (last 4 quarters)
            inc_q  = _df_to_long(t.quarterly_income_stmt,  symbol, exchange, "income",        "quarterly", collection_date)
            bal_q  = _df_to_long(t.quarterly_balance_sheet, symbol, exchange, "balance_sheet", "quarterly", collection_date)
            cf_q   = _df_to_long(t.quarterly_cashflow,     symbol, exchange, "cashflow",      "quarterly", collection_date)

            # Annual statements (last 4 years)
            inc_a  = _df_to_long(t.income_stmt,   symbol, exchange, "income",        "annual", collection_date)
            bal_a  = _df_to_long(t.balance_sheet,  symbol, exchange, "balance_sheet", "annual", collection_date)
            cf_a   = _df_to_long(t.cashflow,       symbol, exchange, "cashflow",      "annual", collection_date)

            all_rows.extend(inc_q + inc_a)
            all_rows.extend(bal_q + bal_a)
            all_rows.extend(cf_q  + cf_a)

            if inc_q or inc_a:   income_ok   += 1
            if bal_q or bal_a:   balance_ok  += 1
            if cf_q  or cf_a:    cashflow_ok += 1

        except Exception as exc:
            logger.warning("Failed to fetch financials",
                           extra={"symbol": symbol, "error": str(exc)})
            failed += 1

        if idx < len(tickers) - 1:
            time.sleep(BATCH_SLEEP_S)

        # Flush to S3 every 500 tickers to avoid holding too much in Lambda memory
        if len(all_rows) >= 50_000:
            _flush(all_rows, exchange, date_str)
            all_rows = []

    # Final flush
    if all_rows:
        _flush(all_rows, exchange, date_str)

    metrics.add_dimension(name="Exchange", value=exchange)
    metrics.add_metric(name="FinancialsIncomeOk",      unit=MetricUnit.Count, value=income_ok)
    metrics.add_metric(name="FinancialsBalanceSheetOk", unit=MetricUnit.Count, value=balance_ok)
    metrics.add_metric(name="FinancialsCashflowOk",    unit=MetricUnit.Count, value=cashflow_ok)
    metrics.add_metric(name="FinancialsFailed",        unit=MetricUnit.Count, value=failed)

    return {
        "exchange":       exchange,
        "date":           date_str,
        "income_ok":      income_ok,
        "balance_sheet_ok": balance_ok,
        "cashflow_ok":    cashflow_ok,
        "tickers_failed": failed,
    }


def _flush(rows: list[dict], exchange: str, date_str: str) -> None:
    """Write accumulated rows to S3 as a single Parquet file (append-friendly key)."""
    import hashlib, time as _time
    # Use a short hash of first row symbol + epoch to make key unique per flush
    suffix = hashlib.md5(f"{rows[0]['symbol']}{_time.time()}".encode()).hexdigest()[:8]
    key = f"financials/exchange={exchange}/date={date_str}/part-{suffix}.parquet"
    write_parquet(pd.DataFrame(rows), key, schema=STATEMENT_SCHEMA)
    logger.info("Financials flushed to S3", extra={"key": key, "rows": len(rows)})
