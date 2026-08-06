"""
Lambda: collect-insider-transactions

Collects insider trading activity (Form 4 filings) from two sources:

Primary — SEC EDGAR Form 4 RSS feed (real-time, ~10 min delay after filing)
  Polls the EDGAR Atom feed for all Form 4 filings submitted in the last 24 h,
  fetches and parses each filing's XML, extracts non-derivative and derivative
  transactions, then writes to S3.

Secondary — yfinance per-ticker backfill (daily, catches anything missed)
  For each ticker on the exchange, calls ticker.insider_transactions to get
  Yahoo Finance's pre-parsed view of recent insider activity. Used to fill
  gaps where the EDGAR filing was amended or outside our RSS window.

Why insider transactions matter
--------------------------------
Corporate insiders (CEOs, CFOs, directors, 10%+ shareholders) must file
Form 4 with the SEC within 2 business days of any trade. Because they know
more about their company than anyone else, their buying/selling behaviour
is one of the strongest predictive signals in finance:

  - Cluster buying (multiple insiders buying at once) is strongly bullish
  - CEO/CFO purchases (not option exercises) on the open market are most
    significant — they are spending real money voluntarily
  - Sales are weaker signals (could be diversification, tax planning, 10b5-1)
  - Derivative transactions (option exercises) are routine and lower signal

Deduplication: article_id = SHA-256 of (symbol + insider_name + transaction_date
+ shares + transaction_type) to prevent double-counting across the two sources.

Input:
    { "exchange": "NASDAQ", "date": "2026-07-24" }

Output:
    {
      "exchange": "NASDAQ",
      "date": "2026-07-24",
      "edgar_transactions": 142,
      "yfinance_transactions": 89,
      "duplicates_skipped": 34,
      "tickers_failed": 5
    }
"""
from __future__ import annotations

import hashlib
import os
import re
import sys
import time
import random
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone, timedelta
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import feedparser
import pandas as pd
import pyarrow as pa
import requests
import yfinance as yf

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

from config_loader import load_tickers
from parquet_writer import write_parquet

logger = Logger()
metrics = Metrics(namespace="MarketData")

# SEC EDGAR Form 4 RSS — all Form 4 filings, most recent 40
EDGAR_FORM4_RSS = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type=4&dateb=&owner=include&count=40&output=atom"
)

# SEC EDGAR headers — SEC requires a descriptive User-Agent
SEC_HEADERS = {
    "User-Agent": "MarketDataPipeline research@example.com",
    "Accept-Encoding": "gzip, deflate",
}

BATCH_SLEEP_S = 0.4   # yfinance pacing
EDGAR_SLEEP_S = 0.15  # SEC asks for ≤10 req/sec; 0.15s gives ~6/sec — well within limit
MAX_RETRY = 3
RETRY_BASE_S = 5


# ── Parquet schema ─────────────────────────────────────────────────────────────

INSIDER_SCHEMA = pa.schema([
    pa.field("transaction_id",      pa.string()),    # SHA-256 dedup key
    pa.field("symbol",              pa.string()),
    pa.field("exchange",            pa.string()),
    pa.field("collection_date",     pa.date32()),
    pa.field("filing_date",         pa.date32()),    # date filed with SEC
    pa.field("transaction_date",    pa.date32()),    # date trade settled
    pa.field("insider_name",        pa.string()),
    pa.field("insider_title",       pa.string()),    # CEO / CFO / Director / etc.
    pa.field("transaction_type",    pa.string()),    # P=Purchase S=Sale A=Award F=Tax
    pa.field("is_derivative",       pa.bool_()),     # True = options/warrants
    pa.field("shares",              pa.float64()),   # shares bought/sold
    pa.field("price_per_share",     pa.float64()),   # execution price
    pa.field("total_value",         pa.float64()),   # shares × price
    pa.field("shares_owned_after",  pa.float64()),   # total position after trade
    pa.field("ownership_type",      pa.string()),    # D=Direct I=Indirect
    pa.field("source",              pa.string()),    # "edgar" | "yfinance"
])


# ── Helpers ────────────────────────────────────────────────────────────────────

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


def _transaction_id(symbol: str, insider: str, txn_date: Any,
                    shares: Any, txn_type: str) -> str:
    key = f"{symbol}|{insider}|{txn_date}|{shares}|{txn_type}"
    return hashlib.sha256(key.encode()).hexdigest()


def _get_xml_text(element: ET.Element | None, tag: str,
                  ns: dict | None = None) -> str:
    """Safely extract text from an XML element by tag name."""
    if element is None:
        return ""
    ns = ns or {}
    found = element.find(tag, ns)
    return (found.text or "").strip() if found is not None else ""


# ── SEC EDGAR RSS + Form 4 XML parser ─────────────────────────────────────────

def _fetch_edgar_rss_entries() -> list[dict]:
    """
    Poll the EDGAR Form 4 RSS feed.
    Returns list of dicts with keys: filing_url, company_name, ticker, filing_date.
    """
    entries: list[dict] = []
    try:
        feed = feedparser.parse(EDGAR_FORM4_RSS)
        for entry in feed.entries:
            title = entry.get("title", "")
            link  = entry.get("link", "")
            updated = entry.get("updated", "")

            # Title format: "4 - COMPANY NAME (TICKER) (CIK 0001234567)"
            # Extract ticker from parentheses before the CIK
            ticker_match = re.search(r"\(([A-Z]{1,5})\)\s*\(CIK", title)
            ticker = ticker_match.group(1) if ticker_match else ""

            filing_date = _safe_date(updated[:10]) if updated else None

            entries.append({
                "filing_url":   link,
                "company_name": title,
                "ticker":       ticker,
                "filing_date":  filing_date,
            })
    except Exception as exc:
        logger.warning("EDGAR RSS fetch failed", extra={"error": str(exc)})
    return entries


def _fetch_form4_xml(filing_index_url: str) -> str | None:
    """
    Given an EDGAR filing index URL, find and return the Form 4 XML content.
    EDGAR index pages list all documents in a filing; we need the .xml file.
    """
    for attempt in range(MAX_RETRY):
        try:
            # Convert index page URL → index JSON
            # e.g. https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&...
            # The link from RSS points to the filing index HTML
            # We convert to the filing-index JSON endpoint
            json_url = filing_index_url.replace(
                "/Archives/edgar/data/",
                "/cgi-bin/browse-edgar?action=getcompany&"
            )
            # Simpler: just fetch the index HTML and find the XML link
            resp = requests.get(filing_index_url, headers=SEC_HEADERS, timeout=15)
            resp.raise_for_status()

            # Find the .xml document link in the filing index
            xml_match = re.search(
                r'href="(/Archives/edgar/data/[^"]+\.xml)"',
                resp.text
            )
            if not xml_match:
                return None

            xml_url = "https://www.sec.gov" + xml_match.group(1)
            time.sleep(EDGAR_SLEEP_S)

            xml_resp = requests.get(xml_url, headers=SEC_HEADERS, timeout=15)
            xml_resp.raise_for_status()
            return xml_resp.text

        except Exception as exc:
            if "429" in str(exc) or "503" in str(exc):
                sleep_s = RETRY_BASE_S * (2 ** attempt) + random.uniform(0, 2)
                logger.warning("SEC rate limit", extra={"sleep_s": sleep_s})
                time.sleep(sleep_s)
            else:
                logger.debug("Form 4 fetch failed",
                             extra={"url": filing_index_url, "error": str(exc)})
                return None
    return None


def _parse_form4_xml(xml_content: str, ticker: str,
                     filing_date: date | None,
                     collection_date: date) -> list[dict]:
    """
    Parse a Form 4 XML document into a list of transaction dicts.
    Handles both nonDerivativeTable (actual shares) and derivativeTable (options).
    """
    rows: list[dict] = []
    if not xml_content:
        return rows

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as exc:
        logger.debug("Form 4 XML parse error", extra={"error": str(exc)})
        return rows

    # Insider identity
    rpt = root.find(".//reportingOwner")
    insider_name  = _get_xml_text(rpt, ".//rptOwnerName")
    insider_title = _get_xml_text(rpt, ".//officerTitle")
    if not insider_title:
        is_director = _get_xml_text(rpt, ".//isDirector")
        is_officer  = _get_xml_text(rpt, ".//isOfficer")
        if is_director == "1":
            insider_title = "Director"
        elif is_officer == "1":
            insider_title = "Officer"

    def _parse_transactions(table_tag: str, is_deriv: bool) -> None:
        table = root.find(f".//{table_tag}")
        if table is None:
            return
        for txn in table.findall(".//"):
            tag = txn.tag
            # Only process the transaction element, not its children
            if "Transaction" not in tag or tag.endswith("Amounts"):
                continue
            # Try to extract transaction amounts from child elements
            amounts = txn.find(".//transactionAmounts") or txn
            shares_el = (amounts.find("transactionShares") or
                         amounts.find("transactionTotalShares"))
            price_el  = amounts.find("transactionPricePerShare")
            type_el   = amounts.find("transactionCode") or txn.find("transactionCode")
            date_el   = txn.find(".//transactionDate/value") or txn.find("transactionDate")
            owned_el  = (txn.find(".//sharesOwnedFollowingTransaction/value") or
                         txn.find("sharesOwnedFollowingTransaction"))
            own_type_el = (txn.find(".//directOrIndirectOwnership/value") or
                           txn.find("directOrIndirectOwnership"))

            shares    = _safe_float(shares_el.text if shares_el is not None else None)
            price     = _safe_float(price_el.find("value").text
                                    if price_el is not None and price_el.find("value") is not None
                                    else (price_el.text if price_el is not None else None))
            txn_type  = (type_el.text or "").strip() if type_el is not None else ""
            txn_date  = _safe_date(date_el.text if date_el is not None else None)
            owned     = _safe_float(owned_el.text if owned_el is not None else None)
            own_type  = (own_type_el.text or "D").strip() if own_type_el is not None else "D"

            if not txn_type or txn_type not in ("P", "S", "A", "F", "M", "G", "X"):
                continue

            total_val = shares * price if not (
                pd.isna(shares) or pd.isna(price)
            ) else float("nan")

            tid = _transaction_id(ticker, insider_name,
                                  str(txn_date), str(shares), txn_type)
            rows.append({
                "transaction_id":     tid,
                "symbol":             ticker,
                "exchange":           "",   # filled in by caller
                "collection_date":    collection_date,
                "filing_date":        filing_date or collection_date,
                "transaction_date":   txn_date or collection_date,
                "insider_name":       insider_name,
                "insider_title":      insider_title,
                "transaction_type":   txn_type,
                "is_derivative":      is_deriv,
                "shares":             shares,
                "price_per_share":    price,
                "total_value":        total_val,
                "shares_owned_after": owned,
                "ownership_type":     own_type,
                "source":             "edgar",
            })

    _parse_transactions("nonDerivativeTable", is_deriv=False)
    _parse_transactions("derivativeTable",    is_deriv=True)
    return rows


# ── yfinance backfill ──────────────────────────────────────────────────────────

def _yfinance_insider_transactions(symbol: str, exchange: str,
                                   collection_date: date) -> list[dict]:
    """
    Pull insider_transactions DataFrame from yfinance for a single ticker.
    Used as a daily backfill alongside EDGAR to catch any missed filings.
    """
    rows: list[dict] = []
    for attempt in range(MAX_RETRY):
        try:
            df = yf.Ticker(symbol).insider_transactions
            if df is None or df.empty:
                return rows
            break
        except Exception as exc:
            if "429" in str(exc):
                sleep_s = RETRY_BASE_S * (2 ** attempt) + random.uniform(0, 2)
                time.sleep(sleep_s)
            else:
                return rows
    else:
        return rows

    for _, row in df.iterrows():
        # Column names vary slightly across yfinance versions — normalise
        insider  = str(row.get("Insider", row.get("insider", ""))).strip()
        title    = str(row.get("Position", row.get("position", ""))).strip()
        txn_type = str(row.get("Transaction", row.get("transaction", ""))).strip()
        shares   = _safe_float(row.get("Shares", row.get("shares")))
        value    = _safe_float(row.get("Value", row.get("value")))
        price    = (value / shares) if shares and shares > 0 and not pd.isna(value) else float("nan")
        txn_date = _safe_date(row.get("Date", row.get("date", row.get("Start Date"))))
        owned    = _safe_float(row.get("Ownership", row.get("ownership")))

        # Map yfinance text type to SEC codes
        type_map = {
            "Buy": "P", "Purchase": "P", "Sale": "S", "Sell": "S",
            "Option Exercise": "M", "Gift": "G", "Award": "A",
        }
        txn_code = type_map.get(txn_type, txn_type[:1].upper() if txn_type else "")

        tid = _transaction_id(symbol, insider, str(txn_date), str(shares), txn_code)
        rows.append({
            "transaction_id":     tid,
            "symbol":             symbol,
            "exchange":           exchange,
            "collection_date":    collection_date,
            "filing_date":        collection_date,
            "transaction_date":   txn_date or collection_date,
            "insider_name":       insider,
            "insider_title":      title,
            "transaction_type":   txn_code,
            "is_derivative":      "Option" in txn_type or "Derivative" in txn_type,
            "shares":             shares,
            "price_per_share":    price,
            "total_value":        value,
            "shares_owned_after": owned,
            "ownership_type":     "D",
            "source":             "yfinance",
        })
    return rows


# ── Handler ────────────────────────────────────────────────────────────────────

@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    exchange:        str  = event["exchange"]
    date_str:        str  = event["date"][:10]
    collection_date: date = date.fromisoformat(date_str)

    # Chunking: accept offset + limit to process a subset of tickers
    ticker_offset: int = event.get("ticker_offset", 0)
    ticker_limit:  int = event.get("ticker_limit", 500)

    seen_ids: set[str] = set()   # dedup across both sources within this run
    all_rows: list[dict] = []
    edgar_count = yf_count = dupe_count = failed = 0

    # Load full ticker list then slice to our chunk
    all_tickers = sorted(load_tickers(exchange))
    chunk_tickers = all_tickers[ticker_offset:ticker_offset + ticker_limit]
    chunk_set = set(chunk_tickers)

    logger.info("Starting insider collection",
                extra={"exchange": exchange, "total_tickers": len(all_tickers),
                       "chunk_offset": ticker_offset, "chunk_size": len(chunk_tickers)})

    # ── 1. EDGAR RSS — only on first chunk (offset=0) to avoid duplicate fetches
    if ticker_offset == 0:
        logger.info("Fetching EDGAR Form 4 RSS feed")
        rss_entries = _fetch_edgar_rss_entries()

        relevant_entries = [
            e for e in rss_entries
            if e["ticker"] and e["ticker"] in set(all_tickers)
        ]
        logger.info("EDGAR entries for exchange",
                    extra={"exchange": exchange, "total_rss": len(rss_entries),
                           "relevant": len(relevant_entries)})

        for entry in relevant_entries:
            xml_content = _fetch_form4_xml(entry["filing_url"])
            time.sleep(EDGAR_SLEEP_S)
            if not xml_content:
                continue

            rows = _parse_form4_xml(
                xml_content,
                ticker=entry["ticker"],
                filing_date=entry["filing_date"],
                collection_date=collection_date,
            )
            for row in rows:
                row["exchange"] = exchange
                tid = row["transaction_id"]
                if tid in seen_ids:
                    dupe_count += 1
                    continue
                seen_ids.add(tid)
                all_rows.append(row)
                edgar_count += 1

    # ── 2. yfinance backfill — only for our chunk of tickers ──────────────────
    logger.info("Starting yfinance insider backfill for chunk",
                extra={"exchange": exchange, "chunk_size": len(chunk_tickers)})

    for idx, symbol in enumerate(chunk_tickers):
        rows = _yfinance_insider_transactions(symbol, exchange, collection_date)
        for row in rows:
            tid = row["transaction_id"]
            if tid in seen_ids:
                dupe_count += 1
                continue
            seen_ids.add(tid)
            all_rows.append(row)
            yf_count += 1

        if idx < len(chunk_tickers) - 1:
            time.sleep(BATCH_SLEEP_S)

    # ── Write Parquet (per-chunk file) ────────────────────────────────────────
    s3_key = ""
    if all_rows:
        df = pd.DataFrame(all_rows)
        # Ensure date columns are Python date objects (not Timestamps)
        for col in ("filing_date", "transaction_date", "collection_date"):
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        s3_key = (
            f"insider_transactions/exchange={exchange}"
            f"/date={date_str}/chunk_{ticker_offset:05d}.parquet"
        )
        write_parquet(df, s3_key, schema=INSIDER_SCHEMA)
        logger.info("Insider transactions written",
                    extra={"key": s3_key, "rows": len(all_rows)})

    # ── Metrics ───────────────────────────────────────────────────────────────
    metrics.add_dimension(name="Exchange", value=exchange)
    metrics.add_metric(name="InsiderTransactionsEdgar",    unit=MetricUnit.Count, value=edgar_count)
    metrics.add_metric(name="InsiderTransactionsYFinance", unit=MetricUnit.Count, value=yf_count)
    metrics.add_metric(name="InsiderTransactionsDupes",    unit=MetricUnit.Count, value=dupe_count)
    metrics.add_metric(name="InsiderTransactionsFailed",   unit=MetricUnit.Count, value=failed)

    return {
        "exchange":              exchange,
        "date":                  date_str,
        "ticker_offset":         ticker_offset,
        "ticker_limit":          ticker_limit,
        "tickers_processed":     len(chunk_tickers),
        "edgar_transactions":    edgar_count,
        "yfinance_transactions": yf_count,
        "duplicates_skipped":    dupe_count,
        "tickers_failed":        failed,
        "s3_key":                s3_key,
    }
