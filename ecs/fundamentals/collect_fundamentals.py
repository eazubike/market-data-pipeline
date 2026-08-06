"""
ECS Fargate Task: collect-fundamentals

Pulls company fundamentals (PE, EPS, revenue, debt ratios, short interest, etc.)
for every ticker on every exchange. Runs once daily after market close.

Writes Parquet per-batch to S3 so partial progress is preserved, then compacts
into a single file per exchange/date.

Environment variables:
    DATA_BUCKET   — S3 bucket name
    EXCHANGES     — comma-separated list (default: all)
    AWS_REGION    — region (default: us-east-1)
"""

from __future__ import annotations

import io
import json
import os
import time
from datetime import datetime, timezone, date
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DATA_BUCKET = os.environ["DATA_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
EXCHANGES = os.environ.get(
    "EXCHANGES",
    "NASDAQ,NYSE,LSE,XETRA,EURONEXT,EURONEXT_AMS,EURONEXT_BRU,EURONEXT_LIS,BORSA_ITALIANA,TSE,HKEX",
).split(",")

BATCH_SIZE = 50
BATCH_SLEEP_S = 1.0

s3 = boto3.client("s3", region_name=AWS_REGION)

# ─────────────────────────────────────────────────────────────────────────────
# PARQUET SCHEMA
# ─────────────────────────────────────────────────────────────────────────────

FUNDAMENTALS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("date", pa.date32()),
        pa.field("pe_ratio", pa.float64()),
        pa.field("pb_ratio", pa.float64()),
        pa.field("eps", pa.float64()),
        pa.field("dividend_yield", pa.float64()),
        pa.field("revenue_ttm", pa.float64()),
        pa.field("net_income_ttm", pa.float64()),
        pa.field("debt_to_equity", pa.float64()),
        pa.field("shares_outstanding", pa.int64()),
        pa.field("week_52_high", pa.float64()),
        pa.field("week_52_low", pa.float64()),
        pa.field("avg_volume_30d", pa.int64()),
        pa.field("short_ratio", pa.float64()),
        pa.field("short_percent_of_float", pa.float64()),
        pa.field("market_cap", pa.float64()),
        pa.field("currency", pa.string()),
    ]
)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def _safe_float(v: Any) -> float:
    try:
        return float(v) if v is not None else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _safe_int(v: Any) -> int:
    try:
        return int(v) if v is not None else 0
    except (TypeError, ValueError):
        return 0


def load_tickers(exchange: str) -> list[str]:
    """Load ticker list from S3 config."""
    key = f"config/tickers/{exchange}.csv"
    try:
        resp = s3.get_object(Bucket=DATA_BUCKET, Key=key)
        lines = resp["Body"].read().decode("utf-8").splitlines()
    except Exception:
        return []
    tickers = []
    for line in lines[1:]:  # skip header
        parts = line.split(",")
        if parts and parts[0].strip():
            tickers.append(parts[0].strip())
    return [t for t in tickers if t]


def load_exchanges_config() -> dict:
    """Load exchanges.json from S3."""
    resp = s3.get_object(Bucket=DATA_BUCKET, Key="config/exchanges.json")
    return json.loads(resp["Body"].read())


def write_parquet_to_s3(df: pd.DataFrame, key: str) -> None:
    """Write DataFrame as Snappy-compressed Parquet to S3."""
    if df.empty:
        return
    table = pa.Table.from_pandas(df, schema=FUNDAMENTALS_SCHEMA, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(
        Bucket=DATA_BUCKET,
        Key=key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )


def fetch_fundamentals(ticker: str) -> dict:
    """Fetch .info for a single ticker."""
    try:
        info = yf.Ticker(ticker).info
        return {
            "pe_ratio": _safe_float(info.get("trailingPE")),
            "pb_ratio": _safe_float(info.get("priceToBook")),
            "eps": _safe_float(info.get("trailingEps")),
            "dividend_yield": _safe_float(info.get("dividendYield")),
            "revenue_ttm": _safe_float(info.get("totalRevenue")),
            "net_income_ttm": _safe_float(info.get("netIncomeToCommon")),
            "debt_to_equity": _safe_float(info.get("debtToEquity")),
            "shares_outstanding": _safe_int(info.get("sharesOutstanding")),
            "week_52_high": _safe_float(info.get("fiftyTwoWeekHigh")),
            "week_52_low": _safe_float(info.get("fiftyTwoWeekLow")),
            "avg_volume_30d": _safe_int(
                info.get("averageVolume30Day") or info.get("averageVolume")
            ),
            "market_cap": _safe_float(info.get("marketCap")),
            "currency": info.get("currency", ""),
            "short_ratio": _safe_float(info.get("shortRatio")),
            "short_percent_of_float": _safe_float(info.get("shortPercentOfFloat")),
        }
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def collect_exchange(exchange: str, run_date: date) -> dict:
    """Collect fundamentals for one exchange. Writes per-batch, then compacts."""
    tickers = load_tickers(exchange)
    if not tickers:
        print(f"  [{exchange}] No tickers found")
        return {"exchange": exchange, "tickers_ok": 0, "tickers_failed": 0}

    date_str = run_date.isoformat()
    s3_prefix = f"fundamentals/date={date_str}/exchange={exchange}/"

    print(f"  [{exchange}] Collecting fundamentals for {len(tickers)} tickers...")

    tickers_ok = 0
    tickers_failed = 0
    batches_written = 0

    batches = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        batch_rows: list[dict] = []

        for ticker in batch:
            fund_info = fetch_fundamentals(ticker)
            if fund_info and fund_info.get("market_cap"):
                batch_rows.append(
                    {
                        "symbol": ticker,
                        "exchange": exchange,
                        "date": run_date,
                        **{
                            k: fund_info[k]
                            for k in [
                                "pe_ratio",
                                "pb_ratio",
                                "eps",
                                "dividend_yield",
                                "revenue_ttm",
                                "net_income_ttm",
                                "debt_to_equity",
                                "shares_outstanding",
                                "week_52_high",
                                "week_52_low",
                                "avg_volume_30d",
                                "short_ratio",
                                "short_percent_of_float",
                                "market_cap",
                                "currency",
                            ]
                        },
                    }
                )
                tickers_ok += 1
            else:
                tickers_failed += 1

        # Write batch to S3 immediately
        if batch_rows:
            batch_key = f"{s3_prefix}batch_{batch_idx+1:04d}.parquet"
            write_parquet_to_s3(pd.DataFrame(batch_rows), batch_key)
            batches_written += 1

        if batch_idx < len(batches) - 1:
            time.sleep(BATCH_SLEEP_S)

        # Progress log every 10 batches
        if (batch_idx + 1) % 10 == 0:
            print(
                f"    [{exchange}] batch {batch_idx+1}/{len(batches)} — ok={tickers_ok} failed={tickers_failed}"
            )

    # ── Compact into single file ──────────────────────────────────────────────
    if batches_written > 0:
        print(f"  [{exchange}] Compacting {batches_written} batch files...")
        batch_dfs: list[pd.DataFrame] = []
        paginator = s3.get_paginator("list_objects_v2")
        batch_keys: list[str] = []

        for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if "batch_" in key and key.endswith(".parquet"):
                    resp = s3.get_object(Bucket=DATA_BUCKET, Key=key)
                    batch_dfs.append(pd.read_parquet(io.BytesIO(resp["Body"].read())))
                    batch_keys.append(key)

        if batch_dfs:
            merged_df = pd.concat(batch_dfs, ignore_index=True)
            final_key = f"{s3_prefix}data.parquet"
            write_parquet_to_s3(merged_df, final_key)
            print(f"  [{exchange}] Written {final_key} ({len(merged_df)} rows)")

            # Delete batch files
            for key in batch_keys:
                s3.delete_object(Bucket=DATA_BUCKET, Key=key)

    print(f"  [{exchange}] Done — ok={tickers_ok}  failed={tickers_failed}")
    return {
        "exchange": exchange,
        "tickers_ok": tickers_ok,
        "tickers_failed": tickers_failed,
    }


def main() -> None:
    run_date = datetime.now(timezone.utc).date()
    print(f"\n{'='*60}")
    print(f"  Fundamentals Collection — ECS Fargate Task")
    print(f"  Date      : {run_date.isoformat()}")
    print(f"  Bucket    : {DATA_BUCKET}")
    print(f"  Exchanges : {EXCHANGES}")
    print(f"{'='*60}\n")

    results: list[dict] = []
    for exchange in EXCHANGES:
        result = collect_exchange(exchange, run_date)
        results.append(result)

    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    total_ok = sum(r["tickers_ok"] for r in results)
    total_failed = sum(r["tickers_failed"] for r in results)
    for r in results:
        print(
            f"  {r['exchange']:20s}  ok={r['tickers_ok']:5d}  failed={r['tickers_failed']:5d}"
        )
    print(f"  {'TOTAL':20s}  ok={total_ok:5d}  failed={total_failed:5d}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
