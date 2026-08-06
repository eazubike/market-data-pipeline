"""
update_ticker_lists.py — Regenerate full ticker CSVs for all exchanges and upload to S3.

Sources:
  - NASDAQ/NYSE: NASDAQ screener API (full listings)
  - LSE: companiesmarketcap.com UK companies (add .L suffix)
  - XETRA: companiesmarketcap.com Germany (.DE tickers)
  - Euronext Paris: companiesmarketcap.com France (.PA tickers)
  - Euronext Amsterdam: companiesmarketcap.com Netherlands (.AS tickers)
  - Euronext Brussels: companiesmarketcap.com Belgium (.BR tickers)
  - Euronext Lisbon: companiesmarketcap.com Portugal (.LS tickers)
  - Borsa Italiana: companiesmarketcap.com Italy (.MI tickers)
  - TSE: JPX official Excel
  - HKEX: HKEX official XLSX

Usage:
    py -3 scripts/update_ticker_lists.py
"""
from __future__ import annotations

import csv
import io
import os
import sys
from pathlib import Path

import boto3
import pandas as pd
import requests

S3_BUCKET = "market-data-082121306678-us-east-1"
AWS_REGION = "us-east-1"
H = {"User-Agent": "Mozilla/5.0"}

s3 = boto3.client("s3", region_name=AWS_REGION)


def upload_tickers(exchange: str, tickers: list[str]) -> None:
    """Upload a ticker list as CSV to S3."""
    lines = ["symbol,name,sector,industry"]
    for t in tickers:
        lines.append(f"{t},,,")
    body = "\n".join(lines)
    key = f"config/tickers/{exchange}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode(), ContentType="text/csv")
    print(f"  Uploaded {exchange}: {len(tickers)} tickers → s3://{S3_BUCKET}/{key}")


def fetch_nasdaq_api(exchange: str) -> list[str]:
    r = requests.get(
        "https://api.nasdaq.com/api/screener/stocks",
        headers=H, params={"tableonly": "true", "exchange": exchange, "download": "true"},
        timeout=30,
    )
    r.raise_for_status()
    return [row["symbol"] for row in r.json()["data"]["rows"] if row.get("symbol")]


def fetch_companiesmarketcap(country_slug: str, suffix: str) -> list[str]:
    """Fetch tickers from companiesmarketcap.com for a given country."""
    url = f"https://companiesmarketcap.com/{country_slug}/largest-companies-in-{country_slug}-by-market-cap/?download=csv"
    r = requests.get(url, headers=H, timeout=20)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    tickers = []
    for row in reader:
        sym = row.get("Symbol", "").strip()
        if not sym:
            continue
        if suffix and sym.endswith(suffix):
            tickers.append(sym)
        elif suffix and not sym.endswith(suffix):
            # Some symbols don't have the suffix (US-listed ADRs) - add it
            # But only if it's a short symbol (likely local listing)
            if "." not in sym and len(sym) <= 5:
                tickers.append(sym + suffix)
    return tickers


def fetch_uk_tickers() -> list[str]:
    """UK companies - need to handle specially as symbols may be US ADRs."""
    url = "https://companiesmarketcap.com/united-kingdom/largest-companies-in-the-uk-by-market-cap/?download=csv"
    r = requests.get(url, headers=H, timeout=20)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    tickers = []
    for row in reader:
        sym = row.get("Symbol", "").strip()
        if not sym:
            continue
        # Already has .L suffix
        if sym.endswith(".L"):
            tickers.append(sym)
        # Has no dot - might be US ADR, add .L to check
        elif "." not in sym and len(sym) <= 5:
            tickers.append(sym + ".L")
    return tickers


def fetch_jpx() -> list[str]:
    r = requests.get(
        "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
        headers=H, timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content))
    equities = df[df["市場・商品区分"].str.contains("内国株式", na=False)]
    return [str(c).strip() + ".T" for c in equities["コード"]]


def fetch_hkex() -> list[str]:
    r = requests.get(
        "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
        headers=H, timeout=30,
    )
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content), skiprows=2)
    equities = df[df["Category"] == "Equity"]
    return [
        str(int(c)).zfill(4) + ".HK"
        for c in equities["Stock Code"]
        if str(c).strip().isdigit()
    ]


def main():
    print("=" * 60)
    print("  Updating Ticker Lists → S3")
    print("=" * 60)

    # US exchanges
    print("\n[NASDAQ]")
    nasdaq = fetch_nasdaq_api("NASDAQ")
    upload_tickers("NASDAQ", nasdaq)

    print("\n[NYSE]")
    nyse = fetch_nasdaq_api("NYSE")
    upload_tickers("NYSE", nyse)

    # UK / LSE
    print("\n[LSE]")
    lse = fetch_uk_tickers()
    upload_tickers("LSE", lse)

    # Germany / XETRA
    print("\n[XETRA]")
    xetra = fetch_companiesmarketcap("germany", ".DE")
    upload_tickers("XETRA", xetra)

    # France / Euronext Paris
    print("\n[EURONEXT]")
    euronext = fetch_companiesmarketcap("france", ".PA")
    upload_tickers("EURONEXT", euronext)

    # Netherlands / Euronext Amsterdam
    print("\n[EURONEXT_AMS]")
    ams = fetch_companiesmarketcap("the-netherlands", ".AS")
    upload_tickers("EURONEXT_AMS", ams)

    # Belgium / Euronext Brussels
    print("\n[EURONEXT_BRU]")
    bru = fetch_companiesmarketcap("belgium", ".BR")
    upload_tickers("EURONEXT_BRU", bru)

    # Portugal / Euronext Lisbon
    print("\n[EURONEXT_LIS]")
    lis = fetch_companiesmarketcap("portugal", ".LS")
    upload_tickers("EURONEXT_LIS", lis)

    # Italy / Borsa Italiana
    print("\n[BORSA_ITALIANA]")
    milan = fetch_companiesmarketcap("italy", ".MI")
    upload_tickers("BORSA_ITALIANA", milan)

    # Japan / TSE
    print("\n[TSE]")
    tse = fetch_jpx()
    upload_tickers("TSE", tse)

    # Hong Kong / HKEX
    print("\n[HKEX]")
    hkex = fetch_hkex()
    upload_tickers("HKEX", hkex)

    print("\n" + "=" * 60)
    print("  Done! All ticker lists updated in S3.")
    print("=" * 60)


if __name__ == "__main__":
    main()
