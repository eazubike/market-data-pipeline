"""
ECS Fargate Task: collect-financials

Collects full financial statements (income, balance sheet, cash flow)
for every ticker on a given exchange. Runs once weekly.

Environment variables:
    DATA_BUCKET   — S3 bucket name
    EXCHANGE      — single exchange to process
    AWS_REGION    — region (default: us-east-1)
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import time
import random
from datetime import date, datetime, timezone
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

DATA_BUCKET = os.environ["DATA_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
EXCHANGE = os.environ.get("EXCHANGE", "NASDAQ")

BATCH_SLEEP_S = 0.5
MAX_RETRY = 3
RETRY_BASE_S = 5

s3 = boto3.client("s3", region_name=AWS_REGION)

STATEMENT_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("collection_date", pa.date32()),
        pa.field("statement_type", pa.string()),
        pa.field("period_type", pa.string()),
        pa.field("period_end_date", pa.date32()),
        pa.field("line_item", pa.string()),
        pa.field("value", pa.float64()),
    ]
)


def load_tickers(exchange: str) -> list[str]:
    key = f"config/tickers/{exchange}.csv"
    try:
        resp = s3.get_object(Bucket=DATA_BUCKET, Key=key)
        lines = resp["Body"].read().decode("utf-8").splitlines()
    except Exception:
        return []
    return [l.split(",")[0].strip() for l in lines[1:] if l.split(",")[0].strip()]


def write_parquet_to_s3(df: pd.DataFrame, key: str) -> None:
    if df.empty:
        return
    table = pa.Table.from_pandas(df, schema=STATEMENT_SCHEMA, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", use_compliant_nested_type=True)
    buf.seek(0)
    s3.put_object(
        Bucket=DATA_BUCKET,
        Key=key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )


def _safe_float(v: Any) -> float:
    try:
        return float(v) if v is not None else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _df_to_long(df, symbol, exchange, statement_type, period_type, collection_date):
    rows = []
    if df is None or df.empty:
        return rows
    for col in df.columns:
        try:
            period_end = (
                col.date()
                if hasattr(col, "date")
                else date.fromisoformat(str(col)[:10])
            )
        except Exception:
            continue
        for line_item, raw_value in df[col].items():
            value = _safe_float(raw_value)
            if pd.isna(value):
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "collection_date": collection_date,
                    "statement_type": statement_type,
                    "period_type": period_type,
                    "period_end_date": period_end,
                    "line_item": str(line_item),
                    "value": value,
                }
            )
    return rows


def _fetch_with_retry(symbol: str):
    for attempt in range(MAX_RETRY):
        try:
            return yf.Ticker(symbol)
        except Exception as exc:
            if "429" in str(exc):
                time.sleep(RETRY_BASE_S * (2**attempt) + random.uniform(0, 2))
            else:
                return None
    return None


def main():
    run_date = datetime.now(timezone.utc).date()
    date_str = run_date.isoformat()
    exchange = EXCHANGE

    print(f"\n{'='*60}")
    print(f"  Financials Collection — ECS Fargate")
    print(f"  Date     : {date_str}")
    print(f"  Exchange : {exchange}")
    print(f"  Bucket   : {DATA_BUCKET}")
    print(f"{'='*60}\n")

    tickers = load_tickers(exchange)
    if not tickers:
        print(f"  No tickers for {exchange}")
        return

    print(f"  Processing {len(tickers)} tickers...")

    all_rows = []
    income_ok = balance_ok = cashflow_ok = failed = 0
    flush_count = 0

    for idx, symbol in enumerate(tickers):
        t = _fetch_with_retry(symbol)
        if t is None:
            failed += 1
            time.sleep(BATCH_SLEEP_S)
            continue

        try:
            inc_q = _df_to_long(
                t.quarterly_income_stmt,
                symbol,
                exchange,
                "income",
                "quarterly",
                run_date,
            )
            bal_q = _df_to_long(
                t.quarterly_balance_sheet,
                symbol,
                exchange,
                "balance_sheet",
                "quarterly",
                run_date,
            )
            cf_q = _df_to_long(
                t.quarterly_cashflow,
                symbol,
                exchange,
                "cashflow",
                "quarterly",
                run_date,
            )
            inc_a = _df_to_long(
                t.income_stmt, symbol, exchange, "income", "annual", run_date
            )
            bal_a = _df_to_long(
                t.balance_sheet, symbol, exchange, "balance_sheet", "annual", run_date
            )
            cf_a = _df_to_long(
                t.cashflow, symbol, exchange, "cashflow", "annual", run_date
            )

            all_rows.extend(inc_q + inc_a + bal_q + bal_a + cf_q + cf_a)
            if inc_q or inc_a:
                income_ok += 1
            if bal_q or bal_a:
                balance_ok += 1
            if cf_q or cf_a:
                cashflow_ok += 1
        except Exception:
            failed += 1

        time.sleep(BATCH_SLEEP_S)

        if len(all_rows) >= 50000:
            flush_count += 1
            suffix = hashlib.md5(f"{symbol}{time.time()}".encode()).hexdigest()[:8]
            key = (
                f"financials/date={date_str}/exchange={exchange}/part-{suffix}.parquet"
            )
            write_parquet_to_s3(pd.DataFrame(all_rows), key)
            print(f"    Flushed {len(all_rows)} rows → {key}")
            all_rows = []

        if (idx + 1) % 100 == 0:
            print(
                f"    {idx+1}/{len(tickers)} — income={income_ok} balance={balance_ok} cashflow={cashflow_ok} failed={failed}"
            )

    if all_rows:
        flush_count += 1
        suffix = hashlib.md5(f"final{time.time()}".encode()).hexdigest()[:8]
        key = f"financials/exchange={exchange}/date={date_str}/part-{suffix}.parquet"
        write_parquet_to_s3(pd.DataFrame(all_rows), key)
        print(f"    Final flush {len(all_rows)} rows → {key}")

    print(
        f"\n  DONE — income={income_ok} balance={balance_ok} cashflow={cashflow_ok} failed={failed}"
    )


if __name__ == "__main__":
    main()
