"""
ECS Fargate Task: collect-analyst-data

Collects analyst price targets, recommendations, and earnings dates
for every ticker on a given exchange. Runs once daily.

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
from typing import Any

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

s3 = boto3.client("s3", region_name=AWS_REGION)

PT_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("date", pa.date32()),
        pa.field("current_price", pa.float64()),
        pa.field("target_low", pa.float64()),
        pa.field("target_mean", pa.float64()),
        pa.field("target_high", pa.float64()),
        pa.field("target_median", pa.float64()),
        pa.field("number_of_analysts", pa.int32()),
        pa.field("upside_pct", pa.float64()),
    ]
)

REC_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("date", pa.date32()),
        pa.field("period", pa.string()),
        pa.field("strong_buy", pa.int32()),
        pa.field("buy", pa.int32()),
        pa.field("hold", pa.int32()),
        pa.field("sell", pa.int32()),
        pa.field("strong_sell", pa.int32()),
        pa.field("consensus", pa.string()),
    ]
)

EARN_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("collection_date", pa.date32()),
        pa.field("earnings_date", pa.timestamp("us", tz="UTC")),
        pa.field("eps_estimate", pa.float64()),
        pa.field("reported_eps", pa.float64()),
        pa.field("surprise_pct", pa.float64()),
        pa.field("is_future", pa.bool_()),
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


def _safe_float(v: Any) -> float:
    try:
        return float(v) if v is not None else float("nan")
    except (TypeError, ValueError):
        return float("nan")


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


def _collect_price_targets(t, symbol, exchange, collection_date):
    try:
        pt = t.analyst_price_targets
        if pt is None or not isinstance(pt, dict):
            return None
        current = _safe_float(pt.get("current"))
        low = _safe_float(pt.get("low"))
        mean = _safe_float(pt.get("mean"))
        high = _safe_float(pt.get("high"))
        median = _safe_float(pt.get("median"))
        n = int(pt.get("numberOfAnalysts", 0) or 0)
        upside = (
            ((mean / current) - 1) * 100
            if current and current > 0 and not pd.isna(mean)
            else float("nan")
        )
        return {
            "symbol": symbol,
            "exchange": exchange,
            "date": collection_date,
            "current_price": current,
            "target_low": low,
            "target_mean": mean,
            "target_high": high,
            "target_median": median,
            "number_of_analysts": n,
            "upside_pct": upside,
        }
    except Exception:
        return None


def _collect_recommendations(t, symbol, exchange, collection_date):
    rows = []
    try:
        rec = t.recommendations_summary
        if rec is None or rec.empty:
            return rows
        for _, row in rec.iterrows():
            period = str(row.get("period", ""))
            sb = int(row.get("strongBuy", 0) or 0)
            b = int(row.get("buy", 0) or 0)
            h = int(row.get("hold", 0) or 0)
            se = int(row.get("sell", 0) or 0)
            ss = int(row.get("strongSell", 0) or 0)
            total = sb + b + h + se + ss
            if total == 0:
                consensus = "N/A"
            elif (sb + b) > (se + ss + h):
                consensus = "Buy"
            elif (se + ss) > (sb + b + h):
                consensus = "Sell"
            else:
                consensus = "Hold"
            rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "date": collection_date,
                    "period": period,
                    "strong_buy": sb,
                    "buy": b,
                    "hold": h,
                    "sell": se,
                    "strong_sell": ss,
                    "consensus": consensus,
                }
            )
    except Exception:
        pass
    return rows


def _collect_earnings_dates(t, symbol, exchange, collection_date):
    rows = []
    try:
        ed = t.earnings_dates
        if ed is None or ed.empty:
            return rows
        now = pd.Timestamp.now(tz="UTC")
        for earn_ts, row in ed.iterrows():
            is_future = earn_ts > now if hasattr(earn_ts, "__gt__") else False
            rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "collection_date": collection_date,
                    "earnings_date": (
                        earn_ts.to_pydatetime()
                        if hasattr(earn_ts, "to_pydatetime")
                        else earn_ts
                    ),
                    "eps_estimate": _safe_float(row.get("EPS Estimate")),
                    "reported_eps": _safe_float(row.get("Reported EPS")),
                    "surprise_pct": _safe_float(row.get("Surprise(%)")),
                    "is_future": is_future,
                }
            )
    except Exception:
        pass
    return rows


def main():
    run_date = datetime.now(timezone.utc).date()
    date_str = run_date.isoformat()
    exchange = EXCHANGE

    print(f"\n{'='*60}")
    print(f"  Analyst Data — ECS Fargate")
    print(f"  Date     : {date_str}")
    print(f"  Exchange : {exchange}")
    print(f"  Bucket   : {DATA_BUCKET}")
    print(f"{'='*60}\n")

    tickers = load_tickers(exchange)
    if not tickers:
        print(f"  No tickers for {exchange}")
        return

    print(f"  Processing {len(tickers)} tickers...")

    pt_rows = []
    rec_rows = []
    earn_rows = []
    failed = 0

    for idx, symbol in enumerate(tickers):
        t = _fetch_with_retry(symbol)
        if t is None:
            failed += 1
            time.sleep(BATCH_SLEEP_S)
            continue

        pt = _collect_price_targets(t, symbol, exchange, run_date)
        if pt:
            pt_rows.append(pt)

        rec_rows.extend(_collect_recommendations(t, symbol, exchange, run_date))
        earn_rows.extend(_collect_earnings_dates(t, symbol, exchange, run_date))

        time.sleep(BATCH_SLEEP_S)

        if (idx + 1) % 100 == 0:
            print(
                f"    {idx+1}/{len(tickers)} — targets={len(pt_rows)} recs={len(rec_rows)} earnings={len(earn_rows)} failed={failed}"
            )

    # Write price targets
    if pt_rows:
        key = f"analyst/type=price_targets/date={date_str}/exchange={exchange}/data.parquet"
        write_parquet_to_s3(pd.DataFrame(pt_rows), key, PT_SCHEMA)
        print(f"  Price targets: {len(pt_rows)} → {key}")

    # Write recommendations
    if rec_rows:
        key = f"analyst/type=recommendations/date={date_str}/exchange={exchange}/data.parquet"
        write_parquet_to_s3(pd.DataFrame(rec_rows), key, REC_SCHEMA)
        print(f"  Recommendations: {len(rec_rows)} → {key}")

    # Write earnings dates
    if earn_rows:
        key = f"analyst/type=earnings_dates/date={date_str}/exchange={exchange}/data.parquet"
        write_parquet_to_s3(pd.DataFrame(earn_rows), key, EARN_SCHEMA)
        print(f"  Earnings dates: {len(earn_rows)} → {key}")

    print(
        f"\n  DONE — targets={len(pt_rows)} recs={len(rec_rows)} earnings={len(earn_rows)} failed={failed}"
    )


if __name__ == "__main__":
    main()
