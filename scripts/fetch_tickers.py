"""
One-time setup script: downloads ticker lists for each exchange from
Wikipedia / yfinance and uploads CSV files to S3.

Usage:
    pip install yfinance pandas boto3 requests beautifulsoup4
    python scripts/fetch_tickers.py --bucket market-data-<account>-<region>

The CSV format written to S3:
    symbol,name,sector,industry

Run this once before deploying, then re-run periodically (e.g. quarterly)
to refresh the ticker lists.
"""
from __future__ import annotations

import argparse
import io
import logging
import time

import boto3
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)
s3 = boto3.client("s3")


# ── Exchange fetchers ──────────────────────────────────────────────────────────

def fetch_nasdaq() -> pd.DataFrame:
    """Fetch all NASDAQ-listed tickers via NASDAQ API."""
    log.info("Fetching NASDAQ tickers...")
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=NASDAQ"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()
    rows = resp.json()["data"]["table"]["rows"]
    df = pd.DataFrame(rows)
    # Normalise column names — API returns lowercase
    df.columns = [c.lower() for c in df.columns]
    available = df.columns.tolist()
    sym_col = next((c for c in ["symbol"] if c in available), available[0])
    name_col = next((c for c in ["name", "companyname"] if c in available), available[1] if len(available) > 1 else sym_col)
    sec_col = next((c for c in ["sector"] if c in available), None)
    ind_col = next((c for c in ["industry"] if c in available), None)
    df["symbol"] = df[sym_col].astype(str).str.strip()
    df["name"] = df[name_col].astype(str).str.strip()
    df["sector"] = df[sec_col].astype(str) if sec_col else ""
    df["industry"] = df[ind_col].astype(str) if ind_col else ""
    return df[["symbol", "name", "sector", "industry"]].dropna(subset=["symbol"])


def fetch_nyse() -> pd.DataFrame:
    """Fetch all NYSE-listed tickers via NASDAQ API."""
    log.info("Fetching NYSE tickers...")
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=NYSE"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()
    rows = resp.json()["data"]["table"]["rows"]
    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    available = df.columns.tolist()
    sym_col = next((c for c in ["symbol"] if c in available), available[0])
    name_col = next((c for c in ["name", "companyname"] if c in available), available[1] if len(available) > 1 else sym_col)
    sec_col = next((c for c in ["sector"] if c in available), None)
    ind_col = next((c for c in ["industry"] if c in available), None)
    df["symbol"] = df[sym_col].astype(str).str.strip()
    df["name"] = df[name_col].astype(str).str.strip()
    df["sector"] = df[sec_col].astype(str) if sec_col else ""
    df["industry"] = df[ind_col].astype(str) if ind_col else ""
    return df[["symbol", "name", "sector", "industry"]].dropna(subset=["symbol"])


def _wiki_table_to_df(wiki_url: str, ticker_suffix: str,
                      ticker_hints: list[str], name_hints: list[str]) -> pd.DataFrame:
    """
    Generic Wikipedia table scraper. Parses all wikitables and picks the one
    that has a column matching any of the ticker_hints keywords.
    Uses direct HTML parsing to handle tables that pd.read_html fails on.
    """
    resp = requests.get(wiki_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    soup = BeautifulSoup(resp.text, "html.parser")

    for tbl in soup.find_all("table", {"class": "wikitable"}):
        # Extract header row
        headers = []
        header_row = tbl.find("tr")
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

        if not headers:
            continue

        headers_lower = [h.lower() for h in headers]

        # Find which column index is the ticker
        ticker_idx = next(
            (i for hint in ticker_hints
             for i, h in enumerate(headers_lower) if hint in h), None
        )
        name_idx = next(
            (i for hint in name_hints
             for i, h in enumerate(headers_lower) if hint in h), 0
        )

        if ticker_idx is None:
            continue

        rows = []
        for tr in tbl.find_all("tr")[1:]:  # skip header
            cells = tr.find_all(["td", "th"])
            if len(cells) <= max(ticker_idx, name_idx):
                continue
            ticker_val = cells[ticker_idx].get_text(strip=True).upper()
            name_val = cells[name_idx].get_text(strip=True)
            if ticker_val and ticker_val not in ("TICKER", "SYMBOL", "CODE"):
                rows.append({
                    "symbol": ticker_val + ticker_suffix,
                    "name": name_val,
                    "sector": "",
                    "industry": "",
                })

        if rows:
            df = pd.DataFrame(rows)
            return df.drop_duplicates("symbol")

    return pd.DataFrame()


def fetch_lse() -> pd.DataFrame:
    """Fetch FTSE 100 + FTSE 250 from Wikipedia."""
    log.info("Fetching LSE tickers (FTSE 100 + 250)...")
    frames = []
    for url in ["https://en.wikipedia.org/wiki/FTSE_100_Index",
                "https://en.wikipedia.org/wiki/FTSE_250_Index"]:
        df = _wiki_table_to_df(url, ".L",
                               ["ticker", "epic", "symbol"],
                               ["company", "name"])
        if not df.empty:
            frames.append(df)
        time.sleep(1)
    return pd.concat(frames).drop_duplicates("symbol") if frames else pd.DataFrame()


def fetch_xetra() -> pd.DataFrame:
    """Fetch DAX 40 + MDAX from Wikipedia."""
    log.info("Fetching XETRA tickers (DAX 40 + MDAX)...")
    frames = []
    for url in ["https://en.wikipedia.org/wiki/DAX",
                "https://en.wikipedia.org/wiki/MDAX"]:
        df = _wiki_table_to_df(url, ".DE",
                               ["ticker", "symbol", "xetra"],
                               ["company", "name"])
        if not df.empty:
            frames.append(df)
        time.sleep(1)
    return pd.concat(frames).drop_duplicates("symbol") if frames else pd.DataFrame()


def fetch_euronext() -> pd.DataFrame:
    """Fetch CAC 40 from Wikipedia."""
    log.info("Fetching Euronext tickers (CAC 40)...")
    return _wiki_table_to_df(
        "https://en.wikipedia.org/wiki/CAC_40", ".PA",
        ["ticker", "symbol"], ["company", "name"]
    )


def fetch_tse() -> pd.DataFrame:
    """Fetch Nikkei 225 top constituents — hardcoded as Wikipedia has no parseable table."""
    log.info("Fetching TSE tickers (Nikkei 225 — top constituents)...")
    tickers = [
        ("7203.T","Toyota Motor"),("6758.T","Sony Group"),("8306.T","Mitsubishi UFJ"),
        ("6098.T","Recruit Holdings"),("6902.T","DENSO"),("8035.T","Tokyo Electron"),
        ("7267.T","Honda Motor"),("8316.T","Sumitomo Mitsui"),("6954.T","Fanuc"),
        ("4568.T","Daiichi Sankyo"),("6501.T","Hitachi"),("9984.T","SoftBank Group"),
        ("7751.T","Canon"),("8058.T","Mitsubishi"),("6367.T","Daikin Industries"),
        ("8031.T","Mitsui"),("4502.T","Takeda Pharmaceutical"),("8766.T","Tokio Marine"),
        ("8411.T","Mizuho Financial"),("9432.T","NTT"),("9433.T","KDDI"),
        ("7974.T","Nintendo"),("9020.T","East Japan Railway"),("6762.T","TDK"),
        ("5401.T","Nippon Steel"),("4063.T","Shin-Etsu Chemical"),("2914.T","JT"),
        ("8601.T","Daiwa Securities"),("7733.T","Olympus"),("6503.T","Mitsubishi Electric"),
        ("3382.T","Seven & i Holdings"),("4452.T","Kao"),("8053.T","Sumitomo"),
        ("6861.T","Keyence"),("7741.T","HOYA"),("6301.T","Komatsu"),
        ("4661.T","Oriental Land"),("9022.T","Central Japan Railway"),("2802.T","Ajinomoto"),
        ("7832.T","Bandai Namco"),("8802.T","Mitsubishi Estate"),("4911.T","Shiseido"),
        ("6645.T","Omron"),("7269.T","Suzuki Motor"),("9735.T","SECOM"),
        ("5802.T","Sumitomo Electric"),("6724.T","Seiko Epson"),("9613.T","NTT Data"),
        ("3407.T","Asahi Kasei"),("4578.T","Otsuka Holdings"),
    ]
    df = pd.DataFrame(tickers, columns=["symbol","name"])
    df["sector"] = ""
    df["industry"] = ""
    return df


def fetch_hkex() -> pd.DataFrame:
    """Fetch Hang Seng Index from Wikipedia."""
    log.info("Fetching HKEX tickers (Hang Seng Index)...")
    df = _wiki_table_to_df(
        "https://en.wikipedia.org/wiki/Hang_Seng_Index", "",
        ["code", "ticker", "symbol"], ["company", "name"]
    )
    if not df.empty:
        df["symbol"] = df["symbol"].str.replace(r"\.0$", "", regex=True).str.zfill(4) + ".HK"
    return df


# ── Main ───────────────────────────────────────────────────────────────────────

def fetch_euronext_amsterdam() -> pd.DataFrame:
    """Fetch AEX + AMX (Amsterdam) from Wikipedia."""
    log.info("Fetching Euronext Amsterdam tickers (AEX + AMX)...")
    frames = []
    for url in ["https://en.wikipedia.org/wiki/AEX_index",
                "https://en.wikipedia.org/wiki/AMX_index"]:
        df = _wiki_table_to_df(url, ".AS",
                               ["ticker", "symbol", "isin"],
                               ["company", "name"])
        if not df.empty:
            frames.append(df)
        time.sleep(1)
    return pd.concat(frames).drop_duplicates("symbol") if frames else pd.DataFrame()


def fetch_euronext_brussels() -> pd.DataFrame:
    """Fetch BEL 20 (Brussels) from Wikipedia."""
    log.info("Fetching Euronext Brussels tickers (BEL 20)...")
    return _wiki_table_to_df(
        "https://en.wikipedia.org/wiki/BEL_20", ".BR",
        ["ticker", "symbol"], ["company", "name"]
    )


def fetch_euronext_lisbon() -> pd.DataFrame:
    """Fetch PSI (Lisbon) — hardcoded list (Wikipedia page has no parseable wikitable)."""
    log.info("Fetching Euronext Lisbon tickers (PSI — hardcoded)...")
    tickers = [
        ("EDP.LS","EDP"),("EDPR.LS","EDP Renováveis"),("GALP.LS","Galp Energia"),
        ("BCP.LS","Banco Comercial Português"),("NOS.LS","NOS"),("JMT.LS","Jerónimo Martins"),
        ("SON.LS","Sonae"),("CTT.LS","CTT"),("ALTR.LS","Corticeira Amorim"),
        ("SEM.LS","Semapa"),("MOTA.LS","Mota-Engil"),("REN.LS","REN"),
        ("RAM.LS","Ramada Investimentos"),("PHR.LS","Pharol"),
        ("IPV.LS","Impresa"),("COR.LS","Corticeira Amorim"),
        ("F2I.LS","F2i"),("VIS.LS","Vidrala"),("NBA.LS","Novabase"),
        ("SCT.LS","Sonaecom"),
    ]
    df = pd.DataFrame(tickers, columns=["symbol","name"])
    df["sector"] = ""
    df["industry"] = ""
    return df


def fetch_borsa_italiana() -> pd.DataFrame:
    """Fetch FTSE MIB (Milan) from Wikipedia."""
    log.info("Fetching Borsa Italiana tickers (FTSE MIB)...")
    return _wiki_table_to_df(
        "https://en.wikipedia.org/wiki/FTSE_MIB", ".MI",
        ["ticker", "symbol"], ["company", "name"]
    )


FETCHERS = {
    "NASDAQ":          fetch_nasdaq,
    "NYSE":            fetch_nyse,
    "LSE":             fetch_lse,
    "XETRA":           fetch_xetra,
    "EURONEXT":        fetch_euronext,
    "EURONEXT_AMS":    fetch_euronext_amsterdam,
    "EURONEXT_BRU":    fetch_euronext_brussels,
    "EURONEXT_LIS":    fetch_euronext_lisbon,
    "BORSA_ITALIANA":  fetch_borsa_italiana,
    "TSE":             fetch_tse,
    "HKEX":            fetch_hkex,
}


def upload_df(df: pd.DataFrame, bucket: str, exchange: str) -> None:
    if df.empty:
        log.warning(f"No tickers for {exchange} — skipping upload")
        return
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    key = f"config/tickers/{exchange}.csv"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    log.info(f"Uploaded {len(df)} tickers for {exchange} → s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and upload ticker lists to S3")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--exchanges",
        nargs="+",
        default=list(FETCHERS.keys()),
        help="Exchanges to fetch (default: all)",
    )
    args = parser.parse_args()

    for exchange in args.exchanges:
        if exchange not in FETCHERS:
            log.warning(f"Unknown exchange: {exchange}")
            continue
        try:
            df = FETCHERS[exchange]()
            df = df.fillna("").drop_duplicates("symbol")
            df["symbol"] = df["symbol"].str.strip()
            upload_df(df, args.bucket, exchange)
        except Exception as exc:
            log.error(f"Failed to fetch {exchange}: {exc}")
        time.sleep(2)  # polite crawl delay


if __name__ == "__main__":
    main()
