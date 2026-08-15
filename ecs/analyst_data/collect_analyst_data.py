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

PROFILE_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("collection_date", pa.date32()),
        pa.field("company_name", pa.string()),
        pa.field("sector", pa.string()),
        pa.field("industry", pa.string()),
        pa.field("description", pa.string()),
        pa.field("country", pa.string()),
        pa.field("employees", pa.int64()),
        pa.field("website", pa.string()),
        pa.field("forward_pe", pa.float64()),
        pa.field("forward_eps", pa.float64()),
        pa.field("trailing_pe", pa.float64()),
        pa.field("price_to_sales", pa.float64()),
        pa.field("price_to_book", pa.float64()),
        pa.field("enterprise_value", pa.float64()),
        pa.field("ebitda", pa.float64()),
        pa.field("revenue_growth", pa.float64()),
        pa.field("earnings_growth", pa.float64()),
        pa.field("profit_margin", pa.float64()),
        pa.field("operating_margin", pa.float64()),
        pa.field("return_on_equity", pa.float64()),
        pa.field("return_on_assets", pa.float64()),
        pa.field("free_cash_flow", pa.float64()),
        pa.field("beta", pa.float64()),
    ]
)

INST_HOLDERS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("collection_date", pa.date32()),
        pa.field("holder", pa.string()),
        pa.field("shares", pa.int64()),
        pa.field("value", pa.float64()),
        pa.field("pct_held", pa.float64()),
        pa.field("date_reported", pa.date32()),
    ]
)

EARNINGS_HISTORY_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("collection_date", pa.date32()),
        pa.field("quarter_end", pa.date32()),
        pa.field("eps_estimate", pa.float64()),
        pa.field("eps_actual", pa.float64()),
        pa.field("revenue_estimate", pa.float64()),
        pa.field("revenue_actual", pa.float64()),
        pa.field("eps_surprise_pct", pa.float64()),
        pa.field("beat_eps", pa.bool_()),
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


def _collect_company_profile(t, symbol, exchange, collection_date):
    """Extract company profile, sector, industry, and key ratios from ticker.info."""
    try:
        info = t.info
        if not info or not isinstance(info, dict):
            return None
        return {
            "symbol": symbol,
            "exchange": exchange,
            "collection_date": collection_date,
            "company_name": info.get("longName") or info.get("shortName") or "",
            "sector": info.get("sector") or "",
            "industry": info.get("industry") or "",
            "description": (info.get("longBusinessSummary") or "")[:2000],
            "country": info.get("country") or "",
            "employees": int(info.get("fullTimeEmployees") or 0),
            "website": info.get("website") or "",
            "forward_pe": _safe_float(info.get("forwardPE")),
            "forward_eps": _safe_float(info.get("forwardEps")),
            "trailing_pe": _safe_float(info.get("trailingPE")),
            "price_to_sales": _safe_float(info.get("priceToSalesTrailing12Months")),
            "price_to_book": _safe_float(info.get("priceToBook")),
            "enterprise_value": _safe_float(info.get("enterpriseValue")),
            "ebitda": _safe_float(info.get("ebitda")),
            "revenue_growth": _safe_float(info.get("revenueGrowth")),
            "earnings_growth": _safe_float(info.get("earningsGrowth")),
            "profit_margin": _safe_float(info.get("profitMargins")),
            "operating_margin": _safe_float(info.get("operatingMargins")),
            "return_on_equity": _safe_float(info.get("returnOnEquity")),
            "return_on_assets": _safe_float(info.get("returnOnAssets")),
            "free_cash_flow": _safe_float(info.get("freeCashflow")),
            "beta": _safe_float(info.get("beta")),
        }
    except Exception:
        return None


def _collect_institutional_holders(t, symbol, exchange, collection_date):
    """Extract top institutional holders from ticker.institutional_holders."""
    rows = []
    try:
        holders = t.institutional_holders
        if holders is None or holders.empty:
            return rows
        for _, row in holders.iterrows():
            date_reported = None
            if pd.notna(row.get("Date Reported")):
                date_reported = pd.Timestamp(row["Date Reported"]).date()
            rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "collection_date": collection_date,
                    "holder": str(row.get("Holder", "")),
                    "shares": int(row.get("Shares", 0) or 0),
                    "value": _safe_float(row.get("Value")),
                    "pct_held": _safe_float(row.get("% Out")),
                    "date_reported": date_reported,
                }
            )
    except Exception:
        pass
    return rows


def _collect_earnings_history(t, symbol, exchange, collection_date):
    """
    Extract historical earnings beat/miss record.
    Uses earnings_dates (past entries) to build the history.
    """
    rows = []
    try:
        # Try earnings_history first (some yfinance versions)
        eh = getattr(t, "earnings_history", None)
        if eh is not None and not eh.empty:
            for _, row in eh.iterrows():
                eps_est = _safe_float(row.get("epsEstimate"))
                eps_act = _safe_float(row.get("epsActual"))
                surprise = _safe_float(row.get("surprisePercent"))
                quarter_end = None
                if pd.notna(row.get("quarter")):
                    quarter_end = pd.Timestamp(row["quarter"]).date()
                beat = (
                    eps_act > eps_est
                    if not (pd.isna(eps_act) or pd.isna(eps_est))
                    else None
                )
                rows.append(
                    {
                        "symbol": symbol,
                        "exchange": exchange,
                        "collection_date": collection_date,
                        "quarter_end": quarter_end,
                        "eps_estimate": eps_est,
                        "eps_actual": eps_act,
                        "revenue_estimate": float("nan"),
                        "revenue_actual": float("nan"),
                        "eps_surprise_pct": surprise,
                        "beat_eps": beat,
                    }
                )
            return rows

        # Fallback: derive from earnings_dates (past entries only)
        ed = t.earnings_dates
        if ed is None or ed.empty:
            return rows
        now = pd.Timestamp.now(tz="UTC")
        for earn_ts, row in ed.iterrows():
            if hasattr(earn_ts, "__gt__") and earn_ts > now:
                continue  # skip future dates
            eps_est = _safe_float(row.get("EPS Estimate"))
            eps_act = _safe_float(row.get("Reported EPS"))
            surprise = _safe_float(row.get("Surprise(%)"))
            beat = (
                eps_act > eps_est
                if not (pd.isna(eps_act) or pd.isna(eps_est))
                else None
            )
            quarter_end = earn_ts.date() if hasattr(earn_ts, "date") else None
            rows.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "collection_date": collection_date,
                    "quarter_end": quarter_end,
                    "eps_estimate": eps_est,
                    "eps_actual": eps_act,
                    "revenue_estimate": float("nan"),
                    "revenue_actual": float("nan"),
                    "eps_surprise_pct": surprise,
                    "beat_eps": beat,
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
    profile_rows = []
    inst_rows = []
    hist_rows = []
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

        profile = _collect_company_profile(t, symbol, exchange, run_date)
        if profile:
            profile_rows.append(profile)

        inst_rows.extend(_collect_institutional_holders(t, symbol, exchange, run_date))
        hist_rows.extend(_collect_earnings_history(t, symbol, exchange, run_date))

        time.sleep(BATCH_SLEEP_S)

        if (idx + 1) % 100 == 0:
            print(
                f"    {idx+1}/{len(tickers)} — targets={len(pt_rows)} recs={len(rec_rows)} "
                f"earnings={len(earn_rows)} profiles={len(profile_rows)} "
                f"holders={len(inst_rows)} history={len(hist_rows)} failed={failed}"
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

    # Write company profiles
    if profile_rows:
        key = f"analyst/type=company_profiles/date={date_str}/exchange={exchange}/data.parquet"
        write_parquet_to_s3(pd.DataFrame(profile_rows), key, PROFILE_SCHEMA)
        print(f"  Company profiles: {len(profile_rows)} → {key}")

    # Write institutional holders
    if inst_rows:
        key = f"analyst/type=institutional_holders/date={date_str}/exchange={exchange}/data.parquet"
        write_parquet_to_s3(pd.DataFrame(inst_rows), key, INST_HOLDERS_SCHEMA)
        print(f"  Institutional holders: {len(inst_rows)} → {key}")

    # Write earnings history (beat/miss record)
    if hist_rows:
        key = f"analyst/type=earnings_history/date={date_str}/exchange={exchange}/data.parquet"
        write_parquet_to_s3(pd.DataFrame(hist_rows), key, EARNINGS_HISTORY_SCHEMA)
        print(f"  Earnings history: {len(hist_rows)} → {key}")

    print(
        f"\n  DONE — targets={len(pt_rows)} recs={len(rec_rows)} earnings={len(earn_rows)} "
        f"profiles={len(profile_rows)} holders={len(inst_rows)} history={len(hist_rows)} failed={failed}"
    )


if __name__ == "__main__":
    main()
