"""
ECS Fargate Task: collect-insider-transactions

Collects insider trading activity for all exchanges. Runs once daily.

Sources:
  - US (NASDAQ, NYSE): SEC EDGAR full-text search API with date range
    → fetches all Form 4 filings for past 2 days in one call
    → parses each XML for transaction details (P/S/M/A codes)
  - Non-US (LSE, TSE, HKEX): yfinance per-ticker

Writes per-batch Parquet to S3, then compacts into single file per exchange/date.

Environment variables:
    DATA_BUCKET   — S3 bucket name
    EXCHANGES     — comma-separated (default: NASDAQ,NYSE,LSE,TSE,HKEX)
    AWS_REGION    — region (default: us-east-1)
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yfinance as yf

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DATA_BUCKET = os.environ["DATA_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
EXCHANGE = os.environ.get("EXCHANGE", "NASDAQ")  # single exchange per task

BATCH_SIZE = 50
BATCH_SLEEP_S = 0.4

s3 = boto3.client("s3", region_name=AWS_REGION)

SEC_HEADERS = {
    "User-Agent": "MarketDataPipeline research@example.com",
    "Accept-Encoding": "gzip, deflate",
}
EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

# ─────────────────────────────────────────────────────────────────────────────
# PARQUET SCHEMA
# ─────────────────────────────────────────────────────────────────────────────

INSIDER_SCHEMA = pa.schema(
    [
        pa.field("transaction_id", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("collection_date", pa.date32()),
        pa.field("transaction_date", pa.date32()),
        pa.field("insider_name", pa.string()),
        pa.field("insider_title", pa.string()),
        pa.field("transaction_type", pa.string()),
        pa.field("shares", pa.float64()),
        pa.field("price_per_share", pa.float64()),
        pa.field("total_value", pa.float64()),
        pa.field("shares_owned_after", pa.float64()),
        pa.field("acquisition_disposition", pa.string()),
        pa.field("source", pa.string()),
    ]
)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def _safe_float(v: Any) -> float:
    try:
        return float(str(v).replace(",", "")) if v is not None else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _safe_date(v: Any) -> date | None:
    if v is None:
        return None
    try:
        if isinstance(v, date):
            return v
        s = str(v)[:10]
        return date.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _transaction_id(
    symbol: str, insider: str, txn_date: Any, shares: Any, txn_type: str
) -> str:
    key = f"{symbol}|{insider}|{txn_date}|{shares}|{txn_type}"
    return hashlib.sha256(key.encode()).hexdigest()


def load_tickers(exchange: str) -> list[str]:
    key = f"config/tickers/{exchange}.csv"
    try:
        resp = s3.get_object(Bucket=DATA_BUCKET, Key=key)
        lines = resp["Body"].read().decode("utf-8").splitlines()
    except Exception:
        return []
    tickers = []
    for line in lines[1:]:
        parts = line.split(",")
        if parts and parts[0].strip():
            tickers.append(parts[0].strip())
    return [t for t in tickers if t]


def write_parquet_to_s3(df: pd.DataFrame, key: str) -> None:
    if df.empty:
        return
    table = pa.Table.from_pandas(df, schema=INSIDER_SCHEMA, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(
        Bucket=DATA_BUCKET,
        Key=key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )


# ─────────────────────────────────────────────────────────────────────────────
# SEC EDGAR — full-text search API with date range
# ─────────────────────────────────────────────────────────────────────────────


def _parse_form4_xml(xml_text: str, collection_date: date) -> list[dict]:
    """Parse a Form 4 XML and extract all non-derivative transactions."""
    import xml.etree.ElementTree as ET

    rows: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return rows

    # Get ticker
    ticker = ""
    for el in root.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
        if tag.lower() == "issuertradingsymbol":
            ticker = (el.text or "").strip().upper()
            break
    if not ticker:
        return rows

    # Get insider info
    insider_name = ""
    insider_title = ""
    for el in root.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
        if tag == "rptOwnerName" and not insider_name:
            insider_name = (el.text or "").strip()
        elif tag == "officerTitle" and not insider_title:
            insider_title = (el.text or "").strip()

    # Parse non-derivative transactions using iterparse
    in_txn = False
    txn_code = ""
    shares = float("nan")
    price = float("nan")
    txn_date_val = None
    owned_after = float("nan")
    acq_disp = ""

    for event, el in ET.iterparse(io.StringIO(xml_text), events=["start", "end"]):
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag

        if event == "start" and tag == "nonDerivativeTransaction":
            in_txn = True
            txn_code = ""
            shares = float("nan")
            price = float("nan")
            txn_date_val = None
            owned_after = float("nan")
            acq_disp = ""

        elif event == "end" and tag == "nonDerivativeTransaction":
            if in_txn and txn_code in ("P", "S", "A", "M", "F", "G", "X"):
                total_val = (
                    shares * price
                    if not (pd.isna(shares) or pd.isna(price))
                    else float("nan")
                )
                rows.append(
                    {
                        "symbol": ticker,
                        "collection_date": collection_date,
                        "transaction_date": txn_date_val or collection_date,
                        "insider_name": insider_name,
                        "insider_title": insider_title,
                        "transaction_type": txn_code,
                        "shares": shares,
                        "price_per_share": price,
                        "total_value": total_val,
                        "shares_owned_after": owned_after,
                        "acquisition_disposition": acq_disp,
                        "source": "sec_edgar",
                    }
                )
            in_txn = False

        elif in_txn and event == "end":
            if tag == "transactionCode":
                txn_code = (el.text or "").strip()
            elif tag == "transactionShares":
                val_el = el.find("value")
                if val_el is not None and val_el.text:
                    shares = _safe_float(val_el.text)
                elif el.text:
                    shares = _safe_float(el.text)
            elif tag == "transactionPricePerShare":
                val_el = el.find("value")
                if val_el is not None and val_el.text:
                    price = _safe_float(val_el.text)
                elif el.text:
                    price = _safe_float(el.text)
            elif tag == "transactionDate":
                val_el = el.find("value")
                if val_el is not None and val_el.text:
                    txn_date_val = _safe_date(val_el.text)
                elif el.text:
                    txn_date_val = _safe_date(el.text)
            elif tag == "sharesOwnedFollowingTransaction":
                val_el = el.find("value")
                if val_el is not None and val_el.text:
                    owned_after = _safe_float(val_el.text)
            elif tag == "transactionAcquiredDisposedCode":
                val_el = el.find("value")
                if val_el is not None and val_el.text:
                    acq_disp = (val_el.text or "").strip()
                elif el.text:
                    acq_disp = (el.text or "").strip()

    return rows


def collect_sec_edgar(run_date: date, lookback_days: int = 2) -> list[dict]:
    """Fetch all Form 4 filings from SEC EDGAR for the past N days."""
    start_date = (run_date - timedelta(days=lookback_days)).isoformat()
    end_date = run_date.isoformat()

    print(f"  [SEC EDGAR] Searching Form 4 filings from {start_date} to {end_date}...")

    all_rows: list[dict] = []
    offset = 0
    page_size = 100
    total_filings = 0

    while True:
        params = {
            "q": "",
            "forms": "4",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "from": offset,
            "size": page_size,
        }
        try:
            r = requests.get(
                EDGAR_SEARCH_URL, params=params, headers=SEC_HEADERS, timeout=20
            )
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            print(f"  [warn] SEC EDGAR search failed: {exc}")
            break

        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {}).get("value", 0)

        if not hits:
            break

        total_filings += len(hits)
        print(f"    Fetching {len(hits)} filings (offset={offset}, total={total})...")

        for hit in hits:
            doc_id = hit.get("_id", "")
            parts = doc_id.split(":")
            if len(parts) != 2:
                continue
            accession_raw = parts[0]
            filename = parts[1]

            source = hit.get("_source", {})
            ciks = source.get("ciks", [])
            if len(ciks) < 2:
                continue
            issuer_cik = ciks[1]

            accession_no_dashes = accession_raw.replace("-", "")
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{issuer_cik}/{accession_no_dashes}/{filename}"

            try:
                time.sleep(0.12)
                r2 = requests.get(xml_url, headers=SEC_HEADERS, timeout=15)
                if r2.status_code != 200:
                    continue
                rows = _parse_form4_xml(r2.text, run_date)
                all_rows.extend(rows)
            except Exception:
                continue

        offset += page_size
        if offset >= total or offset >= 1000:
            break

    print(f"  [SEC EDGAR] Done — {total_filings} filings, {len(all_rows)} transactions")
    return all_rows


# ─────────────────────────────────────────────────────────────────────────────
# YFINANCE — for non-US exchanges
# ─────────────────────────────────────────────────────────────────────────────


def collect_yfinance_insider(
    tickers: list[str], exchange: str, run_date: date
) -> list[dict]:
    """Collect insider transactions via yfinance for non-US exchanges."""
    print(f"  [{exchange}] yfinance insider for {len(tickers)} tickers...")
    all_rows: list[dict] = []

    for i, ticker in enumerate(tickers):
        try:
            df = yf.Ticker(ticker).insider_transactions
            if df is None or df.empty:
                continue
        except Exception:
            continue

        for _, row in df.iterrows():
            insider = str(row.get("Insider", "")).strip()
            title = str(row.get("Position", "")).strip()
            text_field = str(row.get("Text", "")).strip()
            shares = _safe_float(row.get("Shares"))
            value = _safe_float(row.get("Value"))
            price = (
                (value / shares)
                if shares and shares > 0 and not pd.isna(value)
                else float("nan")
            )
            txn_date = _safe_date(row.get("Start Date"))

            text_lower = text_field.lower()
            if "sale" in text_lower or "sell" in text_lower:
                txn_code = "S"
            elif "purchase" in text_lower or "buy" in text_lower:
                txn_code = "P"
            elif "option" in text_lower or "exercise" in text_lower:
                txn_code = "M"
            elif "gift" in text_lower:
                txn_code = "G"
            elif not pd.isna(value) and value > 0:
                txn_code = "S"
            else:
                txn_code = "A"

            all_rows.append(
                {
                    "symbol": ticker,
                    "collection_date": run_date,
                    "transaction_date": txn_date or run_date,
                    "insider_name": insider,
                    "insider_title": title,
                    "transaction_type": txn_code,
                    "shares": shares,
                    "price_per_share": price,
                    "total_value": value,
                    "shares_owned_after": float("nan"),
                    "acquisition_disposition": "",
                    "source": "yfinance",
                }
            )

        time.sleep(BATCH_SLEEP_S)
        if (i + 1) % 100 == 0:
            print(
                f"    [{exchange}] {i+1}/{len(tickers)} — {len(all_rows)} transactions"
            )

    print(f"  [{exchange}] yfinance done — {len(all_rows)} transactions")
    return all_rows


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def collect_exchange(
    exchange: str, run_date: date, edgar_cache: list[dict] | None = None
) -> dict:
    """Collect insider transactions for one exchange."""
    tickers = load_tickers(exchange)
    if not tickers:
        print(f"  [{exchange}] No tickers found")
        return {"exchange": exchange, "transactions": 0}

    date_str = run_date.isoformat()
    s3_prefix = f"insider_transactions/date={date_str}/exchange={exchange}/"

    seen_ids: set[str] = set()
    all_rows: list[dict] = []

    if exchange in ("NASDAQ", "NYSE"):
        # Use cached SEC EDGAR data
        exchange_tickers = set(tickers)
        for row in edgar_cache or []:
            if row["symbol"] not in exchange_tickers:
                continue
            tid = _transaction_id(
                row["symbol"],
                row["insider_name"],
                str(row["transaction_date"]),
                str(row["shares"]),
                row["transaction_type"],
            )
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            row["exchange"] = exchange
            row["transaction_id"] = tid
            all_rows.append(row)
        print(f"  [{exchange}] SEC EDGAR: {len(all_rows)} transactions matched")
    else:
        # Non-US: yfinance
        yf_rows = collect_yfinance_insider(tickers, exchange, run_date)
        for row in yf_rows:
            tid = _transaction_id(
                row["symbol"],
                row["insider_name"],
                str(row["transaction_date"]),
                str(row["shares"]),
                row["transaction_type"],
            )
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            row["exchange"] = exchange
            row["transaction_id"] = tid
            all_rows.append(row)

    # Write to S3
    if all_rows:
        df = pd.DataFrame(all_rows)
        for col in ("transaction_date", "collection_date"):
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        final_key = f"{s3_prefix}data.parquet"
        write_parquet_to_s3(df, final_key)
        print(f"  [{exchange}] Written {final_key} ({len(df)} rows)")

    return {"exchange": exchange, "transactions": len(all_rows)}


def main():
    run_date = datetime.now(timezone.utc).date()
    exchange = EXCHANGE
    print(f"\n{'='*60}")
    print(f"  Insider Transactions — ECS Fargate Task")
    print(f"  Date      : {run_date.isoformat()}")
    print(f"  Exchange  : {exchange}")
    print(f"  Bucket    : {DATA_BUCKET}")
    print(f"{'='*60}\n")

    # Fetch SEC EDGAR if US exchange
    edgar_data: list[dict] = []
    if exchange in ("NASDAQ", "NYSE"):
        edgar_data = collect_sec_edgar(run_date, lookback_days=2)

    result = collect_exchange(exchange, run_date, edgar_cache=edgar_data)

    print(f"\n{'='*60}")
    print(f"  DONE — {exchange}: {result['transactions']} transactions")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
