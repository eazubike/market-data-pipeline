"""
ECS Fargate Task: collect-corporate-actions

Collects stock splits and dividends for every ticker on a given exchange.
Runs once daily before market open.

Environment variables:
    DATA_BUCKET   — S3 bucket name
    EXCHANGE      — single exchange to process
    AWS_REGION    — region (default: us-east-1)
"""

from __future__ import annotations

import io
import os
import time
import random
from datetime import date, datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

DATA_BUCKET = os.environ["DATA_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
EXCHANGE = os.environ.get("EXCHANGE", "NASDAQ")

BATCH_SLEEP_S = 0.3
MAX_RETRY = 3
RETRY_BASE_S = 5
LOOKBACK_DAYS = 90

s3 = boto3.client("s3", region_name=AWS_REGION)

SPLITS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("event_date", pa.date32()),
        pa.field("ratio", pa.float64()),
        pa.field("collection_date", pa.date32()),
    ]
)

DIVIDENDS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("ex_date", pa.date32()),
        pa.field("amount", pa.float64()),
        pa.field("collection_date", pa.date32()),
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


def write_parquet_to_s3(df: pd.DataFrame, key: str, schema: pa.Schema) -> None:
    if df.empty:
        return
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", use_compliant_nested_type=True)
    buf.seek(0)
    s3.put_object(
        Bucket=DATA_BUCKET,
        Key=key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )


def _fetch_actions_with_retry(symbol: str):
    for attempt in range(MAX_RETRY):
        try:
            t = yf.Ticker(symbol)
            splits = t.splits if t.splits is not None else pd.Series(dtype=float)
            dividends = (
                t.dividends if t.dividends is not None else pd.Series(dtype=float)
            )
            return splits, dividends
        except Exception as exc:
            if "429" in str(exc):
                time.sleep(RETRY_BASE_S * (2**attempt) + random.uniform(0, 2))
            else:
                return pd.Series(dtype=float), pd.Series(dtype=float)
    return pd.Series(dtype=float), pd.Series(dtype=float)


def _filter_recent(series: pd.Series, lookback_days: int) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(dtype=float)
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=lookback_days)
    if series.index.tz is None:
        series.index = series.index.tz_localize("UTC")
    return series[series.index >= cutoff]


def main():
    run_date = datetime.now(timezone.utc).date()
    date_str = run_date.isoformat()
    exchange = EXCHANGE

    print(f"\n{'='*60}")
    print(f"  Corporate Actions — ECS Fargate")
    print(f"  Date     : {date_str}")
    print(f"  Exchange : {exchange}")
    print(f"  Bucket   : {DATA_BUCKET}")
    print(f"{'='*60}\n")

    tickers = load_tickers(exchange)
    if not tickers:
        print(f"  No tickers for {exchange}")
        return

    print(f"  Processing {len(tickers)} tickers (lookback={LOOKBACK_DAYS} days)...")

    split_rows = []
    dividend_rows = []
    failed = 0

    for idx, symbol in enumerate(tickers):
        splits, dividends = _fetch_actions_with_retry(symbol)

        recent_splits = _filter_recent(splits, LOOKBACK_DAYS)
        for event_ts, ratio in recent_splits.items():
            split_rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "event_date": event_ts.date(),
                    "ratio": float(ratio),
                    "collection_date": run_date,
                }
            )

        recent_divs = _filter_recent(dividends, LOOKBACK_DAYS)
        for ex_date_ts, amount in recent_divs.items():
            dividend_rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "ex_date": ex_date_ts.date(),
                    "amount": float(amount),
                    "collection_date": run_date,
                }
            )

        time.sleep(BATCH_SLEEP_S)

        if (idx + 1) % 100 == 0:
            print(
                f"    {idx+1}/{len(tickers)} — splits={len(split_rows)} dividends={len(dividend_rows)}"
            )

    # Write splits
    if split_rows:
        key = f"corporate_actions/type=split/date={date_str}/exchange={exchange}/data.parquet"
        write_parquet_to_s3(pd.DataFrame(split_rows), key, SPLITS_SCHEMA)
        print(f"  Splits: {len(split_rows)} → {key}")

    # Write dividends
    if dividend_rows:
        key = f"corporate_actions/type=dividend/date={date_str}/exchange={exchange}/data.parquet"
        write_parquet_to_s3(pd.DataFrame(dividend_rows), key, DIVIDENDS_SCHEMA)
        print(f"  Dividends: {len(dividend_rows)} → {key}")

    print(
        f"\n  DONE — splits={len(split_rows)} dividends={len(dividend_rows)} failed={failed}"
    )


if __name__ == "__main__":
    main()
