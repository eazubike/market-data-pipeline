"""
run_local.py — Local test runner for the market data pipeline.

Fetches ALL real tickers for each configured exchange directly from Yahoo Finance,
collects OHLCV snapshots + fundamentals, and writes CSV files locally under
./local_output/ (mirrors the S3 partition structure).

Usage:
    pip install yfinance pandas vaderSentiment feedparser requests
    python run_local.py

Options (edit at the top of the file):
    EXCHANGES       — list of exchanges to run (default: all)
    BATCH_SIZE      — tickers per yfinance download call
    BATCH_SLEEP_S   — sleep between batches (be kind to Yahoo)
    COLLECT_NEWS    — also run the news pipeline
    OUTPUT_DIR      — local folder for CSV output
"""
from __future__ import annotations

import io
import os
import re
import sys
import time
import random
import hashlib
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf
import boto3
import requests
import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — edit these to control the run
# ─────────────────────────────────────────────────────────────────────────────

EXCHANGES = []

BATCH_SIZE     = 50      # tickers per yf.download call
BATCH_SLEEP_S  = 1.0     # seconds between batches (rate-limit buffer)
COLLECT_NEWS   = False   # also run the news pipeline
COLLECT_FUNDAMENTALS = False  # separate from prices — run once daily, slow
COLLECT_INSIDER = True   # collect insider transactions
OUTPUT_DIR     = Path("local_output")

# Exchanges to collect insider transactions for (only those with data)
INSIDER_EXCHANGES = ["LSE", "TSE", "HKEX","NASDAQ", "NYSE"]

# S3 bucket for European exchange ticker lists (same as prod)
S3_BUCKET  = "market-data-082121306678-us-east-1"
AWS_REGION = "us-east-1"

# ─────────────────────────────────────────────────────────────────────────────
# EXCHANGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# nasdaq_api_name  → use NASDAQ screener API (US exchanges only)
# yf_exchange_code → use Yahoo Finance screener via curl_cffi session
# hkex             → pull from HKEX official XLSX
# jpx              → pull from JPX official XLS (TSE)
EXCHANGE_CONFIG: dict[str, dict] = {
    "NASDAQ":         {"source": "nasdaq_api",  "nasdaq_api_name": "NASDAQ", "suffix": "",    "currency": "USD"},
    "NYSE":           {"source": "nasdaq_api",  "nasdaq_api_name": "NYSE",   "suffix": "",    "currency": "USD"},
    "LSE":            {"source": "lse_xls",      "suffix": ".L",  "currency": "GBP"},
    "XETRA":          {"source": "companiesmarketcap", "country": "germany",         "suffix": ".DE", "currency": "EUR"},
    "EURONEXT":       {"source": "companiesmarketcap", "country": "france",           "suffix": ".PA", "currency": "EUR"},
    "EURONEXT_AMS":   {"source": "companiesmarketcap", "country": "the-netherlands",  "suffix": ".AS", "currency": "EUR"},
    "EURONEXT_BRU":   {"source": "companiesmarketcap", "country": "belgium",          "suffix": ".BR", "currency": "EUR"},
    "EURONEXT_LIS":   {"source": "companiesmarketcap", "country": "portugal",         "suffix": ".LS", "currency": "EUR"},
    "BORSA_ITALIANA": {"source": "companiesmarketcap", "country": "italy",            "suffix": ".MI", "currency": "EUR"},
    "TSE":            {"source": "jpx",          "suffix": ".T",  "currency": "JPY"},
    "HKEX":           {"source": "hkex",         "suffix": ".HK", "currency": "HKD"},
}

# ─────────────────────────────────────────────────────────────────────────────
# TICKER FETCHING — one authoritative public source per exchange, no AWS
# ─────────────────────────────────────────────────────────────────────────────

_HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}


def _fetch_nasdaq_api(exchange_name: str) -> list[str]:
    """
    NASDAQ + NYSE: pull full listing from NASDAQ's public screener API.
    Uses download=true to bypass pagination and return every listed equity.
    """
    r = requests.get(
        "https://api.nasdaq.com/api/screener/stocks",
        headers=_HTTP_HEADERS,
        params={"tableonly": "true", "exchange": exchange_name, "download": "true"},
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json().get("data", {}).get("rows", [])
    tickers = [row["symbol"] for row in rows if row.get("symbol")]
    print(f"  [tickers] {exchange_name}: {len(tickers)} tickers from NASDAQ API")
    return tickers


def _fetch_jpx() -> list[str]:
    """
    TSE: download the official JPX equity list (Excel), filter to domestic
    equities only, and return as XXXX.T symbols.
    """
    r = requests.get(
        "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
        headers=_HTTP_HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content))
    # Keep only rows where the market column contains domestic equity marker
    equities = df[df["市場・商品区分"].str.contains("内国株式", na=False)]
    tickers = [str(c).strip() + ".T" for c in equities["コード"]]
    print(f"  [tickers] TSE: {len(tickers)} tickers from JPX")
    return tickers


def _fetch_hkex() -> list[str]:
    """
    HKEX: download the official HKEX securities list (XLSX), filter to
    main board equities, and return as XXXX.HK symbols.
    """
    r = requests.get(
        "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
        headers=_HTTP_HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content), skiprows=2)
    equities = df[df["Category"] == "Equity"]
    tickers = [
        str(int(c)).zfill(4) + ".HK"
        for c in equities["Stock Code"]
        if str(c).strip().isdigit()
    ]
    print(f"  [tickers] HKEX: {len(tickers)} tickers from HKEX")
    return tickers


def _fetch_lse_xls() -> list[str]:
    """
    LSE: download the official SETS securities list from the London Stock Exchange.
    Returns ~999 tickers with .L suffix.
    """
    r = requests.get(
        "https://docs.londonstockexchange.com/sites/default/files/documents/List%20of%20SETS%20securities_0.xls",
        headers=_HTTP_HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content), skiprows=3)
    df = df.dropna(subset=["Mnemonic"])
    tickers = [str(m).strip() + ".L" for m in df["Mnemonic"]]
    print(f"  [tickers] LSE: {len(tickers)} tickers from LSE SETS official list")
    return tickers


def _fetch_companiesmarketcap(exchange: str, country: str, suffix: str) -> list[str]:
    """
    European exchanges: download CSV from companiesmarketcap.com.
    Filters to tickers with the correct Yahoo Finance suffix.
    """
    url = f"https://companiesmarketcap.com/{country}/largest-companies-in-{country}-by-market-cap/?download=csv"
    r = requests.get(url, headers=_HTTP_HEADERS, timeout=20)
    r.raise_for_status()

    import csv
    reader = csv.DictReader(io.StringIO(r.text))
    tickers = []
    for row in reader:
        sym = row.get("Symbol", "").strip()
        if not sym:
            continue
        if sym.endswith(suffix):
            tickers.append(sym)
        elif "." not in sym and len(sym) <= 5:
            tickers.append(sym + suffix)
    print(f"  [tickers] {exchange}: {len(tickers)} tickers from companiesmarketcap.com")
    return tickers


def _fetch_s3_csv(exchange: str) -> list[str]:
    """
    Fallback: pull ticker list from the prod S3 CSV.
    """
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    key = f"config/tickers/{exchange}.csv"
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        lines = resp["Body"].read().decode("utf-8").splitlines()
    except Exception as e:
        print(f"  [tickers] S3 read failed for {exchange}: {e}")
        return []
    tickers = []
    for line in lines[1:]:
        parts = line.split(",")
        if parts and parts[0].strip():
            tickers.append(parts[0].strip())
    print(f"  [tickers] {exchange}: {len(tickers)} tickers from S3")
    return tickers


def fetch_tickers(exchange: str) -> list[str]:
    """Dispatch to the correct source for each exchange."""
    cfg = EXCHANGE_CONFIG.get(exchange, {})
    source = cfg.get("source", "")
    try:
        if source == "nasdaq_api":
            return _fetch_nasdaq_api(cfg["nasdaq_api_name"])
        elif source == "lse_xls":
            return _fetch_lse_xls()
        elif source == "companiesmarketcap":
            return _fetch_companiesmarketcap(exchange, cfg["country"], cfg["suffix"])
        elif source == "jpx":
            return _fetch_jpx()
        elif source == "hkex":
            return _fetch_hkex()
        elif source == "s3_csv":
            return _fetch_s3_csv(exchange)
        else:
            print(f"  [tickers] No source configured for {exchange}")
            return []
    except Exception as e:
        print(f"  [tickers] ERROR fetching {exchange}: {e}")
        import traceback; traceback.print_exc()
        return []

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


def write_csv_local(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame as CSV to a local path, serialising list columns to strings."""
    if df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # List-valued columns (symbols_mentioned, topics) are stored as pipe-separated strings
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object and out[col].apply(lambda x: isinstance(x, list)).any():
            out[col] = out[col].apply(lambda x: "|".join(x) if isinstance(x, list) else x)
    out.to_csv(path, index=False)
    print(f"  [write] {path}  ({len(df)} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# STOCK COLLECTION
# ─────────────────────────────────────────────────────────────────────────────

MAX_RETRY    = 3
RETRY_BASE_S = 5


def _fetch_batch_with_retry(tickers: list[str]) -> pd.DataFrame | None:
    """Download last 5 days of daily bars for a batch of tickers, with retry on 429."""
    ticker_str = " ".join(tickers)
    for attempt in range(MAX_RETRY):
        try:
            df = yf.download(
                ticker_str,
                period="5d",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            return df
        except Exception as exc:
            err = str(exc).lower()
            if "429" in err or "rate" in err:
                sleep_s = RETRY_BASE_S * (2 ** attempt) + random.uniform(0, 3)
                print(f"  [warn] Rate-limited (attempt {attempt+1}), sleeping {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                print(f"  [error] yfinance download: {exc}")
                return None
    return None


def _extract_latest_row(df: pd.DataFrame, ticker: str) -> dict | None:
    """Pull the most recent bar for a single ticker from a multi-ticker DataFrame."""
    try:
        if isinstance(df.columns, pd.MultiIndex):
            ticker_df = df[ticker].dropna(how="all")
        else:
            ticker_df = df.dropna(how="all")
        if ticker_df.empty:
            return None
        row = ticker_df.iloc[-1]
        return {
            "price":  float(row.get("Close", row.get("close", float("nan")))),
            "open":   float(row.get("Open",  row.get("open",  float("nan")))),
            "high":   float(row.get("High",  row.get("high",  float("nan")))),
            "low":    float(row.get("Low",   row.get("low",   float("nan")))),
            "close":  float(row.get("Close", row.get("close", float("nan")))),
            "volume": int(row.get("Volume",  row.get("volume", 0))),
            "timestamp": ticker_df.index[-1].to_pydatetime().replace(tzinfo=timezone.utc),
        }
    except Exception:
        return None


def _fetch_fundamentals(ticker: str) -> dict:
    """Fetch .info for a single ticker. Returns empty dict on any failure."""
    try:
        info = yf.Ticker(ticker).info
        return {
            "pe_ratio":               _safe_float(info.get("trailingPE")),
            "pb_ratio":               _safe_float(info.get("priceToBook")),
            "eps":                    _safe_float(info.get("trailingEps")),
            "dividend_yield":         _safe_float(info.get("dividendYield")),
            "revenue_ttm":            _safe_float(info.get("totalRevenue")),
            "net_income_ttm":         _safe_float(info.get("netIncomeToCommon")),
            "debt_to_equity":         _safe_float(info.get("debtToEquity")),
            "shares_outstanding":     _safe_int(info.get("sharesOutstanding")),
            "week_52_high":           _safe_float(info.get("fiftyTwoWeekHigh")),
            "week_52_low":            _safe_float(info.get("fiftyTwoWeekLow")),
            "avg_volume_30d":         _safe_int(info.get("averageVolume30Day") or info.get("averageVolume")),
            "market_cap":             _safe_float(info.get("marketCap")),
            "currency":               info.get("currency", ""),
            "short_ratio":            _safe_float(info.get("shortRatio")),
            "short_percent_of_float": _safe_float(info.get("shortPercentOfFloat")),
        }
    except Exception:
        return {}

def collect_stocks(exchange: str, run_date: date, run_timestamp: str) -> dict:
    """Collect OHLCV prices for all tickers on an exchange. Appends to one file per batch."""
    tickers = fetch_tickers(exchange)
    if not tickers:
        print(f"  [warn] No tickers for {exchange}")
        return {"exchange": exchange, "tickers_ok": 0, "tickers_failed": 0}

    print(f"\n[{exchange}] Collecting {len(tickers)} tickers (prices only)...")

    date_str = run_date.isoformat()
    run_ts_safe = run_timestamp.replace(":", "-").rstrip("Z") + "Z"
    currency = EXCHANGE_CONFIG.get(exchange, {}).get("currency", "")
    tickers_ok = 0
    tickers_failed = 0

    # Single output file — appended to after each batch
    out_path = (
        OUTPUT_DIR / "stocks"
        / f"exchange={exchange}"
        / f"date={date_str}"
        / f"run_timestamp={run_ts_safe}"
        / "data.csv"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    header_written = out_path.exists()

    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        print(f"  [batch {batch_idx+1}/{len(batches)}] {batch[:5]}{'...' if len(batch) > 5 else ''}")
        df = _fetch_batch_with_retry(batch)

        batch_rows: list[dict] = []
        for ticker in batch:
            row = _extract_latest_row(df, ticker) if df is not None else None
            if row is None:
                tickers_failed += 1
                continue

            batch_rows.append({
                "symbol":     ticker,
                "exchange":   exchange,
                "timestamp":  row["timestamp"],
                "price":      row["price"],
                "open":       row["open"],
                "high":       row["high"],
                "low":        row["low"],
                "close":      row["close"],
                "volume":     row["volume"],
                "currency":   currency,
            })
            tickers_ok += 1

        # Append this batch to the single file
        if batch_rows:
            batch_df = pd.DataFrame(batch_rows)
            batch_df.to_csv(out_path, mode="a", index=False, header=not header_written)
            header_written = True
            print(f"    → appended {len(batch_rows)} rows to {out_path.name}  (total so far: {tickers_ok})")

        if batch_idx < len(batches) - 1:
            time.sleep(BATCH_SLEEP_S)

    print(f"  [{exchange}] Done — ok={tickers_ok}  failed={tickers_failed}")
    return {"exchange": exchange, "tickers_ok": tickers_ok, "tickers_failed": tickers_failed}


def collect_fundamentals(exchange: str, run_date: date) -> dict:
    """Collect fundamentals for all tickers on an exchange. Appends to one file per batch."""
    tickers = fetch_tickers(exchange)
    if not tickers:
        return {"exchange": exchange, "tickers_ok": 0, "tickers_failed": 0}

    print(f"\n[{exchange}] Collecting fundamentals for {len(tickers)} tickers...")

    date_str = run_date.isoformat()
    tickers_ok = 0
    tickers_failed = 0

    out_path = (
        OUTPUT_DIR / "fundamentals"
        / f"exchange={exchange}"
        / f"date={date_str}"
        / "data.csv"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    header_written = out_path.exists()

    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        print(f"  [fundamentals batch {batch_idx+1}/{len(batches)}] {batch[:5]}{'...' if len(batch) > 5 else ''}")
        batch_rows: list[dict] = []

        for ticker in batch:
            fund_info = _fetch_fundamentals(ticker)
            if fund_info and fund_info.get("market_cap"):
                batch_rows.append({
                    "symbol":   ticker,
                    "exchange": exchange,
                    "date":     run_date,
                    **{k: fund_info[k] for k in [
                        "pe_ratio","pb_ratio","eps","dividend_yield",
                        "revenue_ttm","net_income_ttm","debt_to_equity",
                        "shares_outstanding","week_52_high","week_52_low",
                        "avg_volume_30d","short_ratio","short_percent_of_float",
                    ]},
                    "market_cap": fund_info.get("market_cap", float("nan")),
                    "currency":   fund_info.get("currency", ""),
                })
                tickers_ok += 1
            else:
                tickers_failed += 1

        # Append this batch to the single file
        if batch_rows:
            batch_df = pd.DataFrame(batch_rows)
            batch_df.to_csv(out_path, mode="a", index=False, header=not header_written)
            header_written = True
            print(f"    → appended {len(batch_rows)} rows  (total so far: {tickers_ok})")

        if batch_idx < len(batches) - 1:
            time.sleep(BATCH_SLEEP_S)

    print(f"  [{exchange}] Fundamentals done — ok={tickers_ok}  failed={tickers_failed}")
    return {"exchange": exchange, "tickers_ok": tickers_ok, "tickers_failed": tickers_failed}

# ─────────────────────────────────────────────────────────────────────────────
# INSIDER TRANSACTIONS
# ─────────────────────────────────────────────────────────────────────────────

SEC_HEADERS = {"User-Agent": "MarketDataPipeline research@example.com", "Accept-Encoding": "gzip, deflate"}
EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"


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


def _transaction_id(symbol: str, insider: str, txn_date: Any,
                    shares: Any, txn_type: str) -> str:
    key = f"{symbol}|{insider}|{txn_date}|{shares}|{txn_type}"
    return hashlib.sha256(key.encode()).hexdigest()


def _parse_form4_xml(xml_text: str, collection_date: date) -> list[dict]:
    """Parse a Form 4 XML and extract all transactions."""
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

    # Parse non-derivative transactions
    in_non_deriv_txn = False
    txn_code = ""
    shares = float("nan")
    price = float("nan")
    txn_date_val = None
    owned_after = float("nan")
    acq_disp = ""

    for event, el in ET.iterparse(io.StringIO(xml_text), events=["start", "end"]):
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag

        if event == "start" and tag == "nonDerivativeTransaction":
            in_non_deriv_txn = True
            txn_code = ""
            shares = float("nan")
            price = float("nan")
            txn_date_val = None
            owned_after = float("nan")
            acq_disp = ""

        elif event == "end" and tag == "nonDerivativeTransaction":
            if in_non_deriv_txn and txn_code in ("P", "S", "A", "M", "F", "G", "X"):
                total_val = shares * price if not (pd.isna(shares) or pd.isna(price)) else float("nan")
                rows.append({
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
                })
            in_non_deriv_txn = False

        elif in_non_deriv_txn and event == "end":
            if tag == "transactionCode":
                txn_code = (el.text or "").strip()
            elif tag == "value":
                # Determine parent context
                parent_text = ""
                # value elements appear in transactionShares, transactionPricePerShare, etc.
                # We handle them by position — first value = shares or date
                pass
            elif tag == "transactionShares":
                # Look for nested value
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


def collect_insider_sec_edgar(run_date: date, lookback_days: int = 3) -> list[dict]:
    """
    Fetch ALL Form 4 filings from SEC EDGAR for the past N days using the
    full-text search API. Returns structured transactions across all US companies.
    """
    from datetime import timedelta

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
            r = requests.get(EDGAR_SEARCH_URL, params=params, headers=SEC_HEADERS, timeout=20)
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
            # doc_id format: "CIK-ACCESSION:filename.xml"
            # We need to construct the XML URL
            parts = doc_id.split(":")
            if len(parts) != 2:
                continue
            accession_raw = parts[0]  # e.g. "0001628280-26-049647"
            filename = parts[1]       # e.g. "wk-form4_1784928388.xml"

            # Get the issuer CIK from source
            source = hit.get("_source", {})
            ciks = source.get("ciks", [])
            if len(ciks) < 2:
                continue
            # Second CIK is typically the issuer (company)
            issuer_cik = ciks[1] if len(ciks) > 1 else ciks[0]

            # Construct XML URL
            accession_no_dashes = accession_raw.replace("-", "")
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{issuer_cik}/{accession_no_dashes}/{filename}"

            try:
                time.sleep(0.12)  # SEC rate limit
                r2 = requests.get(xml_url, headers=SEC_HEADERS, timeout=15)
                if r2.status_code != 200:
                    continue
                rows = _parse_form4_xml(r2.text, run_date)
                all_rows.extend(rows)
            except Exception:
                continue

        offset += page_size
        if offset >= total or offset >= 500:  # cap at 500 filings per run
            break

    print(f"  [SEC EDGAR] Done — {total_filings} filings processed, {len(all_rows)} transactions extracted")
    return all_rows


def collect_insider_yfinance(tickers: list[str], exchange: str, run_date: date) -> list[dict]:
    """Collect insider transactions via yfinance for non-US exchanges."""
    print(f"  [{exchange}] yfinance insider collection for {len(tickers)} tickers...")
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
            price = (value / shares) if shares and shares > 0 and not pd.isna(value) else float("nan")
            txn_date = _safe_date(row.get("Start Date"))
            ownership = str(row.get("Ownership", "D")).strip()

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

            all_rows.append({
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
            })

        time.sleep(0.4)
        if (i + 1) % 50 == 0:
            print(f"    [{exchange}] {i+1}/{len(tickers)} tickers processed, {len(all_rows)} transactions")

    print(f"  [{exchange}] yfinance done — {len(all_rows)} transactions")
    return all_rows


def collect_insider_transactions(exchange: str, run_date: date) -> dict:
    """
    Collect insider transactions:
    - US (NASDAQ/NYSE): SEC EDGAR search API (fast, date-range, all filings at once)
    - Non-US (LSE/TSE/HKEX): yfinance per-ticker
    """
    print(f"\n[{exchange}] Collecting insider transactions...")

    date_str = run_date.isoformat()
    out_path = (
        OUTPUT_DIR / "insider_transactions"
        / f"exchange={exchange}"
        / f"date={date_str}"
        / "data.csv"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    seen_ids: set[str] = set()

    if exchange in ("NASDAQ", "NYSE"):
        # SEC EDGAR — fast, all US filings in one go
        edgar_rows = collect_insider_sec_edgar(run_date, lookback_days=2)

        # Filter to tickers on this exchange
        exchange_tickers = set(fetch_tickers(exchange))
        for row in edgar_rows:
            if row["symbol"] not in exchange_tickers:
                continue
            tid = _transaction_id(row["symbol"], row["insider_name"],
                                  str(row["transaction_date"]), str(row["shares"]),
                                  row["transaction_type"])
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            row["exchange"] = exchange
            all_rows.append(row)

        print(f"  [{exchange}] SEC EDGAR: {len(all_rows)} transactions matched")

    else:
        # Non-US: yfinance
        tickers = fetch_tickers(exchange)
        if tickers:
            yf_rows = collect_insider_yfinance(tickers, exchange, run_date)
            for row in yf_rows:
                tid = _transaction_id(row["symbol"], row["insider_name"],
                                      str(row["transaction_date"]), str(row["shares"]),
                                      row["transaction_type"])
                if tid in seen_ids:
                    continue
                seen_ids.add(tid)
                row["exchange"] = exchange
                all_rows.append(row)

    # Write to CSV
    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_csv(out_path, index=False)
        print(f"  [write] {out_path}  ({len(df)} rows)")

    print(f"  [{exchange}] Insider done — {len(all_rows)} transactions")
    return {"exchange": exchange, "tickers_with_data": len(set(r["symbol"] for r in all_rows)), "transactions": len(all_rows)}


# ─────────────────────────────────────────────────────────────────────────────
# NEWS COLLECTION
# ─────────────────────────────────────────────────────────────────────────────

vader_analyzer = SentimentIntensityAnalyzer()

_TICKER_RE   = re.compile(r"\b([A-Z]{2,5})\b")
_COMMON_WORDS = {
    "THE","AND","FOR","WITH","FROM","THAT","THIS","WILL","HAVE","MORE","INTO",
    "THAN","THEY","BEEN","WERE","SAID","OVER","AFTER","ALSO","WHICH","WHEN",
    "YEAR","SAYS","STOCK","CORP","INC","LTD","CEO","CFO","IPO","ETF","GDP",
    "ECB","FED","SEC","FCA","BOE","NASDAQ","NYSE","LSE","USD","EUR","GBP","JPY",
}

TOPIC_KEYWORDS: dict[str, list[str]] = {
    "earnings":    ["earnings","revenue","profit","loss","eps","quarterly results"],
    "merger":      ["merger","acquisition","takeover","buyout","m&a","deal"],
    "ipo":         ["ipo","initial public offering","listing","float"],
    "central_bank":["federal reserve","fed","ecb","boe","interest rate","rate hike","rate cut"],
    "macro":       ["gdp","inflation","unemployment","recession","economic growth"],
    "dividend":    ["dividend","payout","yield"],
    "analyst":     ["upgrade","downgrade","price target","buy rating","sell rating"],
    "investment":  ["investment","venture capital","private equity","fund","stake"],
    "regulation":  ["sec","fca","regulation","fine","penalty","lawsuit","antitrust"],
    "geopolitical":["sanctions","tariff","trade war","conflict","election"],
}

RSS_FEEDS: dict[str, str] = {
    "reuters":             "https://feeds.reuters.com/reuters/businessNews",
    "marketwatch":         "https://feeds.marketwatch.com/marketwatch/topstories",
    "cnbc":                "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "ft":                  "https://www.ft.com/rss/home/uk",
    "sec_8k": (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        "?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom"
    ),
    "google_business":     "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB",
    "google_markets":      "https://news.google.com/rss/topics/CAAqKAgKIiJDQkFTRXdvSkwyMHZNR3gxZEhGZkVnSmxiaG9DVlZNb0FBUAE",
    "seekingalpha_market": "https://seekingalpha.com/market_currents.xml",
    "seekingalpha_news":   "https://seekingalpha.com/news.xml",
    "prnewswire_finance":  "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
    "prnewswire_earnings": "https://www.prnewswire.com/rss/earnings-latest-news/earnings-latest-news-list.rss",
    "businesswire":        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWQ==",
    "investing_news":      "https://www.investing.com/rss/news.rss",
    "investing_stock":     "https://www.investing.com/rss/news_14.rss",
}

_seen_ids: set[str] = set()  # in-memory dedup for local run


def _article_id(url: str) -> str:
    return hashlib.sha256(url.strip().lower().encode()).hexdigest()


def _detect_topics(text: str) -> list[str]:
    t = text.lower()
    return [topic for topic, kws in TOPIC_KEYWORDS.items() if any(kw in t for kw in kws)]


def _extract_tickers(text: str) -> list[str]:
    return list({t for t in _TICKER_RE.findall(text) if t not in _COMMON_WORDS})


def _sentiment(headline: str, summary: str) -> float:
    return round(vader_analyzer.polarity_scores(f"{headline} {summary}")["compound"], 4)


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def collect_rss(source: str, feed_url: str, collection_ts: datetime) -> list[dict]:
    articles = []
    try:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            url = entry.get("link", "")
            if not url:
                continue
            aid = _article_id(url)
            if aid in _seen_ids:
                continue
            _seen_ids.add(aid)
            headline  = entry.get("title", "")
            summary   = entry.get("summary", entry.get("description", ""))
            published = _parse_dt(entry.get("published") or entry.get("updated") or "")
            text = f"{headline} {summary}"
            articles.append({
                "article_id":           aid,
                "source":               source,
                "headline":             headline,
                "summary":              summary[:1000],
                "url":                  url,
                "published_at":         published,
                "symbols_mentioned":    _extract_tickers(text),
                "sentiment_score":      _sentiment(headline, summary),
                "topics":               _detect_topics(text),
                "collection_timestamp": collection_ts,
            })
    except Exception as exc:
        print(f"  [warn] RSS {source}: {exc}")
    return articles


def collect_gdelt(collection_ts: datetime) -> list[dict]:
    articles = []
    try:
        resp = requests.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={
                "mode": "artlist", "theme": "ECON_STOCKMARKET",
                "maxrecords": 75, "timespan": "30min", "format": "json",
            },
            timeout=20,
        )
        resp.raise_for_status()
        for item in resp.json().get("articles", []):
            url = item.get("url", "")
            if not url:
                continue
            aid = _article_id(url)
            if aid in _seen_ids:
                continue
            _seen_ids.add(aid)
            headline = item.get("title", "")
            published = _parse_dt(item.get("seendate", ""))
            articles.append({
                "article_id":           aid,
                "source":               "gdelt",
                "headline":             headline,
                "summary":              "",
                "url":                  url,
                "published_at":         published,
                "symbols_mentioned":    _extract_tickers(headline),
                "sentiment_score":      _sentiment(headline, ""),
                "topics":               _detect_topics(headline),
                "collection_timestamp": collection_ts,
            })
    except Exception as exc:
        print(f"  [warn] GDELT: {exc}")
    return articles


def collect_reddit(collection_ts: datetime) -> list[dict]:
    """Collect stock-related posts from finance subreddits via RSS."""
    articles = []
    reddit_feeds = {
        "reddit_stocks": "https://old.reddit.com/r/stocks/.rss",
        "reddit_wsb": "https://old.reddit.com/r/wallstreetbets/.rss",
        "reddit_investing": "https://old.reddit.com/r/investing/.rss",
    }
    for source, url in reddit_feeds.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                post_url = entry.get("link", "")
                if not post_url:
                    continue
                aid = _article_id(post_url)
                if aid in _seen_ids:
                    continue
                _seen_ids.add(aid)
                headline = entry.get("title", "")
                summary = (entry.get("summary", "") or "")[:1000]
                published = _parse_dt(entry.get("published") or entry.get("updated") or "")
                text = f"{headline} {summary}"
                articles.append({
                    "article_id":           aid,
                    "source":               source,
                    "headline":             headline,
                    "summary":              summary,
                    "url":                  post_url,
                    "published_at":         published,
                    "symbols_mentioned":    _extract_tickers(text),
                    "sentiment_score":      _sentiment(headline, summary),
                    "topics":               _detect_topics(text),
                    "collection_timestamp": collection_ts,
                })
        except Exception as exc:
            print(f"  [warn] Reddit {source}: {exc}")
    return articles


def collect_yfinance_news(collection_ts: datetime) -> list[dict]:
    """Pull news for top tickers via yfinance."""
    top_tickers = [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO",
        "JPM", "V", "UNH", "XOM", "WMT", "MA", "JNJ", "HD", "PG", "CVX",
        "MRK", "ABBV", "LLY", "BAC", "KO", "PEP", "NFLX", "AMD", "CRM",
        "COST", "ADBE", "TMO", "DIS", "INTC", "BA", "GS", "CAT", "NKE",
        "QCOM", "SBUX", "GE", "BLK", "AMGN", "PYPL", "BKNG", "TXN",
        "COIN", "PLTR", "SOFI", "RIVN", "ARM", "SMCI",
    ]
    articles = []
    for symbol in top_tickers:
        try:
            ticker = yf.Ticker(symbol)
            news_items = ticker.news or []
            for item in news_items:
                # yfinance 1.5.x returns nested structure: item['content']
                content = item.get("content", item)
                url = ""
                if isinstance(content, dict):
                    canon = content.get("canonicalUrl", {})
                    url = canon.get("url", "") if isinstance(canon, dict) else ""
                    if not url:
                        click = content.get("clickThroughUrl", {})
                        url = click.get("url", "") if isinstance(click, dict) else ""
                if not url:
                    url = item.get("link", item.get("url", ""))
                if not url:
                    continue
                aid = _article_id(url)
                if aid in _seen_ids:
                    continue
                _seen_ids.add(aid)
                headline = content.get("title", "") if isinstance(content, dict) else item.get("title", "")
                summary = (content.get("summary", "") if isinstance(content, dict) else item.get("summary", ""))[:1000]
                pub_raw = content.get("pubDate", content.get("displayTime", "")) if isinstance(content, dict) else item.get("providerPublishTime", 0)
                if isinstance(pub_raw, (int, float)) and pub_raw > 0:
                    published = datetime.fromtimestamp(pub_raw, tz=timezone.utc)
                else:
                    published = _parse_dt(pub_raw)
                provider = content.get("provider", {}) if isinstance(content, dict) else {}
                publisher = provider.get("displayName", "yahoo_finance") if isinstance(provider, dict) else "yahoo_finance"
                text = f"{headline} {summary}"
                articles.append({
                    "article_id":           aid,
                    "source":               f"yfinance_{publisher}".lower().replace(" ", "_"),
                    "headline":             headline,
                    "summary":              summary,
                    "url":                  url,
                    "published_at":         published,
                    "symbols_mentioned":    list(set([symbol] + _extract_tickers(text))),
                    "sentiment_score":      _sentiment(headline, summary),
                    "topics":               _detect_topics(text),
                    "collection_timestamp": collection_ts,
                })
        except Exception:
            pass
        time.sleep(0.2)
    return articles


def collect_news(run_date: date, run_timestamp: str) -> dict:
    print("\n[NEWS] Collecting from RSS + GDELT + Reddit + Yahoo Finance...")
    collection_ts = datetime.now(timezone.utc)
    all_articles: list[dict] = []

    for source, url in RSS_FEEDS.items():
        arts = collect_rss(source, url, collection_ts)
        print(f"  [rss] {source}: {len(arts)} new articles")
        all_articles.extend(arts)

    gdelt_arts = collect_gdelt(collection_ts)
    print(f"  [gdelt] {len(gdelt_arts)} new articles")
    all_articles.extend(gdelt_arts)

    reddit_arts = collect_reddit(collection_ts)
    print(f"  [reddit] {len(reddit_arts)} new articles")
    all_articles.extend(reddit_arts)

    yf_arts = collect_yfinance_news(collection_ts)
    print(f"  [yfinance news] {len(yf_arts)} new articles")
    all_articles.extend(yf_arts)

    if not all_articles:
        print("  [news] No articles collected")
        return {"articles_new": 0}

    date_str    = run_date.isoformat()
    run_ts_safe = run_timestamp.replace(":", "-").rstrip("Z") + "Z"
    df = pd.DataFrame(all_articles)

    for source_name, group_df in df.groupby("source"):
        news_path = (
            OUTPUT_DIR / "news"
            / f"source={source_name}"
            / f"date={date_str}"
            / f"run_timestamp={run_ts_safe}"
            / "data.csv"
        )
        write_csv_local(group_df.reset_index(drop=True), news_path)

    print(f"  [news] Done — {len(all_articles)} total articles")
    return {"articles_new": len(all_articles)}

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    now          = datetime.now(timezone.utc)
    run_date     = now.date()
    run_timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n{'='*60}")
    print(f"  Market Data Pipeline — Local Run")
    print(f"  Timestamp : {run_timestamp}")
    print(f"  Exchanges : {EXCHANGES}")
    print(f"  Output    : {OUTPUT_DIR.resolve()}")
    print(f"{'='*60}\n")

    stock_summary: list[dict] = []
    for exchange in EXCHANGES:
        result = collect_stocks(exchange, run_date, run_timestamp)
        stock_summary.append(result)

    # Fundamentals — separate pass, once daily (slow: 1 API call per ticker)
    fund_summary: list[dict] = []
    if COLLECT_FUNDAMENTALS:
        for exchange in EXCHANGES:
            result = collect_fundamentals(exchange, run_date)
            fund_summary.append(result)

    news_result: dict = {}
    if COLLECT_NEWS:
        news_result = collect_news(run_date, run_timestamp)

    # Insider transactions
    insider_summary: list[dict] = []
    if COLLECT_INSIDER:
        for exchange in INSIDER_EXCHANGES:
            result = collect_insider_transactions(exchange, run_date)
            insider_summary.append(result)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  RUN SUMMARY")
    print(f"{'='*60}")
    total_ok     = sum(r["tickers_ok"]     for r in stock_summary)
    total_failed = sum(r["tickers_failed"] for r in stock_summary)
    for r in stock_summary:
        status = "OK" if r["tickers_ok"] > 0 else "EMPTY (market closed?)"
        print(f"  {r['exchange']:20s}  ok={r['tickers_ok']:4d}  failed={r['tickers_failed']:4d}  [{status}]")
    print(f"  {'TOTAL':20s}  ok={total_ok:4d}  failed={total_failed:4d}")
    if COLLECT_NEWS:
        print(f"  {'NEWS':20s}  articles={news_result.get('articles_new', 0)}")
    if COLLECT_INSIDER:
        total_txns = sum(r["transactions"] for r in insider_summary)
        print(f"  {'INSIDER TXNS':20s}  transactions={total_txns}")
    print(f"\n  Output written to: {OUTPUT_DIR.resolve()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
