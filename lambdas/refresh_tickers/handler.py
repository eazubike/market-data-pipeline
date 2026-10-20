"""
Lambda: refresh-tickers

Refreshes ticker lists for all exchanges from live public sources and
uploads them to S3. Runs once daily before market open.

Sources:
  - NASDAQ/NYSE: NASDAQ screener API (full listings; NASDAQ drops SPAC
    units/rights, which Trading 212 doesn't offer)
  - LSE: Yahoo Finance equity screener (the official SETS XLS stopped updating in 2020)
  - XETRA: Deutsche Börse official listed-companies XLSX (Prime, General, Scale,
    Basic Board) plus companiesmarketcap.com Germany. Not Yahoo's screener: its
    German venues also list thousands of foreign stocks (NVD.DE = Nvidia).
  - Euronext Paris/Amsterdam/Brussels/Lisbon, Borsa Italiana, TSE: Yahoo Finance
    equity screener (companiesmarketcap only listed the largest companies and
    its NL/PT pages and the JPX file now 404)
  - HKEX: HKEX official XLSX (equities + REITs)

Input: {} (no parameters needed)
Output: { "exchanges_updated": [...], "total_tickers": 12345 }
"""
from __future__ import annotations

import csv
import io
import os
import re
import sys
from urllib.parse import urljoin

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import time

import boto3
import pandas as pd
import requests
import yfinance as yf
from yfinance import EquityQuery

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

DATA_BUCKET = os.environ["DATA_BUCKET"]
H = {"User-Agent": "Mozilla/5.0"}
s3 = boto3.client("s3")

# Refuse to replace a list with one less than this fraction of its current size,
# so a broken or truncated source can't silently wipe out an exchange.
MIN_KEEP_RATIO = 0.5


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


def _existing_count(exchange: str) -> int:
    """Number of tickers in the current S3 list (0 if none)."""
    try:
        resp = s3.get_object(Bucket=DATA_BUCKET, Key=f"config/tickers/{exchange}.csv")
    except s3.exceptions.NoSuchKey:
        return 0
    lines = resp["Body"].read().decode("utf-8").splitlines()[1:]
    return sum(1 for line in lines if line.split(",")[0].strip())


# ── Fetchers ──────────────────────────────────────────────────────────────────

def _is_nasdaq_spac_security(symbol: str) -> bool:
    """
    NASDAQ's 5th-letter codes U = unit, R = right. Warrants (W) are kept:
    Trading 212 lists some (e.g. AACIW) but not units (AACOU) or rights (AACPR).
    """
    return len(symbol) == 5 and symbol[-1] in "UR"


def _fetch_nasdaq_api(exchange_name: str, drop_spac_securities: bool = False) -> list[str]:
    """NASDAQ/NYSE: full listing from NASDAQ screener API."""
    r = requests.get(
        "https://api.nasdaq.com/api/screener/stocks",
        headers=H,
        params={"tableonly": "true", "exchange": exchange_name, "download": "true"},
        timeout=30,
    )
    r.raise_for_status()
    symbols = [row["symbol"] for row in r.json()["data"]["rows"] if row.get("symbol")]
    if drop_spac_securities:
        symbols = [s for s in symbols if not _is_nasdaq_spac_security(s)]
    return symbols


DEUTSCHE_BOERSE_PAGE = (
    "https://www.cashmarket.deutsche-boerse.com/cash-en/Data-Tech/statistics/listed-companies"
)
DEUTSCHE_BOERSE_SEGMENTS = ["Prime Standard", "General Standard", "Scale", "Basic Board"]


def _fetch_deutsche_boerse() -> list[str]:
    """XETRA: Deutsche Börse's monthly listed-companies file, all four segments."""
    page = requests.get(DEUTSCHE_BOERSE_PAGE, headers=H, timeout=30)
    page.raise_for_status()
    # The file's URL carries a hash that changes with each monthly release.
    match = re.search(r'href="([^"]*Listed-companies\.xlsx)"', page.text)
    if not match:
        raise ValueError("Listed-companies.xlsx link not found on Deutsche Börse page")
    r = requests.get(urljoin(DEUTSCHE_BOERSE_PAGE, match.group(1)), headers=H, timeout=30)
    r.raise_for_status()

    book = pd.ExcelFile(io.BytesIO(r.content))
    tickers: list[str] = []
    for sheet in DEUTSCHE_BOERSE_SEGMENTS:
        raw = pd.read_excel(book, sheet, header=None)
        header_row = next(
            i for i in range(len(raw))
            if raw.iloc[i].astype(str).str.strip().eq("Trading Symbol").any()
        )
        df = raw.iloc[header_row + 1 :]
        df.columns = [str(c).strip() for c in raw.iloc[header_row]]
        tickers += [str(s).strip() + ".DE" for s in df["Trading Symbol"].dropna()]
    return tickers


def _fetch_xetra() -> list[str]:
    """
    Official Deutsche Börse list plus companiesmarketcap Germany, which adds
    German companies whose primary listing is abroad (e.g. BioNTech).
    Either source alone is enough for a refresh.
    """
    official: list[str] = []
    extra: list[str] = []
    try:
        official = _fetch_deutsche_boerse()
    except Exception as e:
        logger.warning("Deutsche Börse list failed", extra={"error": str(e)})
    try:
        extra = _fetch_companiesmarketcap("germany", ".DE")
    except Exception as e:
        logger.warning("companiesmarketcap Germany failed", extra={"error": str(e)})
    return list(dict.fromkeys(official + extra))


def _fetch_yahoo_screener(exchange_code: str, region: str) -> list[str]:
    """
    All equities Yahoo lists for an exchange, already in Yahoo symbol format.
    Keeps only listings with a market cap and recent volume, which drops
    preference shares and foreign stocks cross-traded on the exchange.
    """
    # The market-cap condition server-side keeps paging small (Milan alone lists
    # ~31k certificates/warrants with no market cap).
    query = EquityQuery(
        "and",
        [
            EquityQuery("eq", ["exchange", exchange_code]),
            EquityQuery("eq", ["region", region]),
            EquityQuery("gt", ["intradaymarketcap", 0]),
        ],
    )
    tickers: list[str] = []
    offset = 0
    while True:
        resp = yf.screen(query, offset=offset, size=250, sortField="intradaymarketcap", sortAsc=False)
        quotes = resp.get("quotes", [])
        tickers += [
            q["symbol"]
            for q in quotes
            if (q.get("marketCap") or 0) > 0 and (q.get("averageDailyVolume3Month") or 0) > 0
        ]
        offset += len(quotes)
        if not quotes or offset >= resp.get("total", 0):
            break
        time.sleep(0.5)
    return list(dict.fromkeys(tickers))


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


def _fetch_hkex() -> list[str]:
    """HKEX: official securities list XLSX."""
    r = requests.get(
        "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
        headers=H,
        timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content), skiprows=2)
    equities = df[df["Category"].isin(["Equity", "Real Estate Investment Trusts"])]
    return [
        str(int(c)).zfill(4) + ".HK"
        for c in equities["Stock Code"]
        if str(c).strip().isdigit()
    ]


# ── Exchange config ───────────────────────────────────────────────────────────

EXCHANGE_SOURCES = [
    ("NASDAQ",         lambda: _fetch_nasdaq_api("NASDAQ", drop_spac_securities=True)),
    ("NYSE",           lambda: _fetch_nasdaq_api("NYSE")),
    ("LSE",            lambda: _fetch_yahoo_screener("LSE", "gb")),
    ("XETRA",          _fetch_xetra),
    ("EURONEXT",       lambda: _fetch_yahoo_screener("PAR", "fr")),
    ("EURONEXT_AMS",   lambda: _fetch_yahoo_screener("AMS", "nl")),
    ("EURONEXT_BRU",   lambda: _fetch_yahoo_screener("BRU", "be")),
    ("EURONEXT_LIS",   lambda: _fetch_yahoo_screener("LIS", "pt")),
    ("BORSA_ITALIANA", lambda: _fetch_yahoo_screener("MIL", "it")),
    ("TSE",            lambda: _fetch_yahoo_screener("JPX", "jp")),
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
            existing = _existing_count(exchange)
            if tickers and len(tickers) < existing * MIN_KEEP_RATIO:
                logger.error(
                    "Refusing to shrink ticker list, keeping current one",
                    extra={"exchange": exchange, "fetched": len(tickers), "existing": existing},
                )
            elif tickers:
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
