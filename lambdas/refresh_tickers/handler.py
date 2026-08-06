"""
Lambda: refresh-tickers

Refreshes ticker lists for all exchanges from live public sources and
uploads them to S3. Runs once daily before market open.

Sources:
  - NASDAQ/NYSE: NASDAQ screener API (full listings)
  - LSE: Official LSE SETS securities XLS
  - XETRA: companiesmarketcap.com Germany
  - Euronext Paris: companiesmarketcap.com France
  - Euronext Amsterdam: companiesmarketcap.com Netherlands
  - Euronext Brussels: companiesmarketcap.com Belgium
  - Euronext Lisbon: companiesmarketcap.com Portugal
  - Borsa Italiana: companiesmarketcap.com Italy
  - TSE: JPX official XLS
  - HKEX: HKEX official XLSX

Input: {} (no parameters needed)
Output: { "exchanges_updated": [...], "total_tickers": 12345 }
"""
from __future__ import annotations

import csv
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import boto3
import pandas as pd
import requests

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

DATA_BUCKET = os.environ["DATA_BUCKET"]
H = {"User-Agent": "Mozilla/5.0"}
s3 = boto3.client("s3")


# ── Upload helper ─────────────────────────────────────────────────────────────

def _upload_tickers(exchange: str, tickers: list[str]) -> int:
    """Upload ticker list as CSV to S3. Returns count."""
    lines = ["symbol,name,sector,industry"]
    for t in tickers:
        lines.append(f"{t},,,")
    body = "\n".join(lines)
    key = f"config/tickers/{exchange}.csv"
    s3.put_object(Bucket=DATA_BUCKET, Key=key, Body=body.encode(), ContentType="text/csv")
    logger.info("Uploaded tickers", extra={"exchange": exchange, "count": len(tickers), "key": key})
    return len(tickers)


# ── Fetchers ──────────────────────────────────────────────────────────────────

def _fetch_nasdaq_api(exchange_name: str) -> list[str]:
    """NASDAQ/NYSE: full listing from NASDAQ screener API."""
    r = requests.get(
        "https://api.nasdaq.com/api/screener/stocks",
        headers=H,
        params={"tableonly": "true", "exchange": exchange_name, "download": "true"},
        timeout=30,
    )
    r.raise_for_status()
    return [row["symbol"] for row in r.json()["data"]["rows"] if row.get("symbol")]


def _fetch_lse_xls() -> list[str]:
    """LSE: official SETS securities list."""
    r = requests.get(
        "https://docs.londonstockexchange.com/sites/default/files/documents/List%20of%20SETS%20securities_0.xls",
        headers=H,
        timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content), skiprows=3)
    df = df.dropna(subset=["Mnemonic"])
    return [str(m).strip() + ".L" for m in df["Mnemonic"]]


def _fetch_companiesmarketcap(country: str, suffix: str) -> list[str]:
    """European exchanges: companiesmarketcap.com CSV download."""
    url = f"https://companiesmarketcap.com/{country}/largest-companies-in-{country}-by-market-cap/?download=csv"
    r = requests.get(url, headers=H, timeout=20)
    r.raise_for_status()
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
    return tickers


def _fetch_jpx() -> list[str]:
    """TSE: JPX official XLS, domestic equities only."""
    r = requests.get(
        "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
        headers=H,
        timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content))
    equities = df[df["市場・商品区分"].str.contains("内国株式", na=False)]
    return [str(c).strip() + ".T" for c in equities["コード"]]


def _fetch_hkex() -> list[str]:
    """HKEX: official securities list XLSX."""
    r = requests.get(
        "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
        headers=H,
        timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content), skiprows=2)
    equities = df[df["Category"] == "Equity"]
    return [
        str(int(c)).zfill(4) + ".HK"
        for c in equities["Stock Code"]
        if str(c).strip().isdigit()
    ]


# ── Exchange config ───────────────────────────────────────────────────────────

EXCHANGE_SOURCES = [
    ("NASDAQ",         lambda: _fetch_nasdaq_api("NASDAQ")),
    ("NYSE",           lambda: _fetch_nasdaq_api("NYSE")),
    ("LSE",            _fetch_lse_xls),
    ("XETRA",         lambda: _fetch_companiesmarketcap("germany", ".DE")),
    ("EURONEXT",       lambda: _fetch_companiesmarketcap("france", ".PA")),
    ("EURONEXT_AMS",   lambda: _fetch_companiesmarketcap("the-netherlands", ".AS")),
    ("EURONEXT_BRU",   lambda: _fetch_companiesmarketcap("belgium", ".BR")),
    ("EURONEXT_LIS",   lambda: _fetch_companiesmarketcap("portugal", ".LS")),
    ("BORSA_ITALIANA", lambda: _fetch_companiesmarketcap("italy", ".MI")),
    ("TSE",            _fetch_jpx),
    ("HKEX",           _fetch_hkex),
]


# ── Handler ───────────────────────────────────────────────────────────────────

@logger.inject_lambda_context(log_event=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    exchanges_updated = []
    total_tickers = 0

    for exchange, fetcher in EXCHANGE_SOURCES:
        try:
            tickers = fetcher()
            if tickers:
                count = _upload_tickers(exchange, tickers)
                exchanges_updated.append(exchange)
                total_tickers += count
            else:
                logger.warning("No tickers returned", extra={"exchange": exchange})
        except Exception as e:
            logger.error("Failed to fetch tickers", extra={"exchange": exchange, "error": str(e)})

    logger.info("Ticker refresh complete", extra={
        "exchanges_updated": exchanges_updated,
        "total_tickers": total_tickers,
    })

    return {
        "exchanges_updated": exchanges_updated,
        "total_tickers": total_tickers,
    }
