"""
Lambda: collect-analyst-data

Runs once per day per exchange. Collects three things for every ticker:

1. Analyst price targets  — consensus low / mean / high / current target
2. Analyst recommendations — buy/hold/sell consensus and recent changes
3. Earnings dates          — next expected earnings release + EPS estimates

Why each matters
----------------
Price targets:  Wall Street's collective view on fair value. When the gap
                between current price and mean target is large, it signals
                under/over-valuation according to professional analysts.

Recommendations: Upgrades (e.g. Hold → Buy) or downgrades are one of the
                 strongest short-term price catalysts. Tracking the trend of
                 recommendation changes is more valuable than a single snapshot.

Earnings dates:  The single most predictable source of short-term volatility.
                 Knowing WHEN a company reports and WHAT the market expects
                 lets you anticipate price swings before they happen.

Data source: yfinance (free, no API key)
  ticker.analyst_price_targets  → dict
  ticker.recommendations        → DataFrame
  ticker.earnings_dates         → DataFrame

Input:
    { "exchange": "NASDAQ", "date": "2026-07-24" }

Output:
    {
      "exchange": "NASDAQ",
      "date": "2026-07-24",
      "price_targets_ok": 3100,
      "recommendations_ok": 2900,
      "earnings_dates_ok": 3200,
      "tickers_failed": 15
    }
"""
from __future__ import annotations

import os
import sys
import time
import random
from datetime import date, datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import pandas as pd
import pyarrow as pa
import yfinance as yf

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

from config_loader import load_tickers
from parquet_writer import write_parquet

logger = Logger()
metrics = Metrics(namespace="MarketData")

BATCH_SLEEP_S = 0.4
MAX_RETRY = 3
RETRY_BASE_S = 5


# ── Parquet schemas ────────────────────────────────────────────────────────────

PRICE_TARGETS_SCHEMA = pa.schema([
    pa.field("symbol",              pa.string()),
    pa.field("exchange",            pa.string()),
    pa.field("date",                pa.date32()),
    pa.field("current_price",       pa.float64()),   # price at time of collection
    pa.field("target_low",          pa.float64()),   # most bearish analyst target
    pa.field("target_mean",         pa.float64()),   # consensus target
    pa.field("target_high",         pa.float64()),   # most bullish analyst target
    pa.field("target_median",       pa.float64()),
    pa.field("number_of_analysts",  pa.int32()),
    pa.field("upside_pct",          pa.float64()),   # (mean_target / current_price - 1) * 100
])

RECOMMENDATIONS_SCHEMA = pa.schema([
    pa.field("symbol",          pa.string()),
    pa.field("exchange",        pa.string()),
    pa.field("date",            pa.date32()),        # collection date
    pa.field("period",          pa.string()),        # e.g. "0m" current, "-1m" last month
    pa.field("strong_buy",      pa.int32()),
    pa.field("buy",             pa.int32()),
    pa.field("hold",            pa.int32()),
    pa.field("sell",            pa.int32()),
    pa.field("strong_sell",     pa.int32()),
    pa.field("consensus",       pa.string()),        # derived: "BUY" / "HOLD" / "SELL"
])

EARNINGS_DATES_SCHEMA = pa.schema([
    pa.field("symbol",               pa.string()),
    pa.field("exchange",             pa.string()),
    pa.field("collection_date",      pa.date32()),
    pa.field("earnings_date",        pa.timestamp("us", tz="UTC")),
    pa.field("eps_estimate",         pa.float64()),  # consensus analyst EPS estimate
    pa.field("reported_eps",         pa.float64()),  # actual reported EPS (null if future)
    pa.field("surprise_pct",         pa.float64()),  # % beat/miss vs estimate
    pa.field("is_future",            pa.bool_()),    # True if earnings date is upcoming
])


# ── Helpers ────────────────────────────────────────────────────────────────────

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


def _consensus_label(strong_buy: int, buy: int, hold: int,
                     sell: int, strong_sell: int) -> str:
    """Map recommendation counts to a single BUY / HOLD / SELL label."""
    bullish = strong_buy + buy
    bearish = sell + strong_sell
    total = bullish + hold + bearish
    if total == 0:
        return "UNKNOWN"
    bull_pct = bullish / total
    bear_pct = bearish / total
    if bull_pct >= 0.5:
        return "BUY"
    if bear_pct >= 0.4:
        return "SELL"
    return "HOLD"


def _fetch_with_retry(symbol: str) -> yf.Ticker | None:
    """Return a yf.Ticker object, retrying on rate-limit errors."""
    for attempt in range(MAX_RETRY):
        try:
            return yf.Ticker(symbol)
        except Exception as exc:
            if "429" in str(exc):
                sleep_s = RETRY_BASE_S * (2 ** attempt) + random.uniform(0, 2)
                logger.warning("Rate limited", extra={"symbol": symbol, "sleep_s": sleep_s})
                time.sleep(sleep_s)
            else:
                return None
    return None


# ── Per-ticker collectors ─────────────────────────────────────────────────────

def _collect_price_targets(ticker: yf.Ticker, symbol: str,
                            exchange: str, collection_date: date) -> dict | None:
    try:
        pt = ticker.analyst_price_targets   # returns a dict
        if not pt:
            return None
        current = _safe_float(pt.get("current"))
        mean    = _safe_float(pt.get("mean"))
        upside  = ((mean / current) - 1) * 100 if current and current > 0 else float("nan")
        return {
            "symbol":             symbol,
            "exchange":           exchange,
            "date":               collection_date,
            "current_price":      current,
            "target_low":         _safe_float(pt.get("low")),
            "target_mean":        mean,
            "target_high":        _safe_float(pt.get("high")),
            "target_median":      _safe_float(pt.get("median")),
            "number_of_analysts": _safe_int(pt.get("numberOfAnalysts")),
            "upside_pct":         upside,
        }
    except Exception:
        return None


def _collect_recommendations(ticker: yf.Ticker, symbol: str,
                               exchange: str, collection_date: date) -> list[dict]:
    rows = []
    try:
        df = ticker.recommendations         # DataFrame: index=period, cols=strongBuy/buy/hold/sell/strongSell
        if df is None or df.empty:
            return rows
        # Keep the last 4 months only (current month + 3 prior)
        for period, row in df.head(4).iterrows():
            sb = _safe_int(row.get("strongBuy",   row.get("strong_buy",   0)))
            b  = _safe_int(row.get("buy",         0))
            h  = _safe_int(row.get("hold",        0))
            s  = _safe_int(row.get("sell",        0))
            ss = _safe_int(row.get("strongSell",  row.get("strong_sell",  0)))
            rows.append({
                "symbol":      symbol,
                "exchange":    exchange,
                "date":        collection_date,
                "period":      str(period),
                "strong_buy":  sb,
                "buy":         b,
                "hold":        h,
                "sell":        s,
                "strong_sell": ss,
                "consensus":   _consensus_label(sb, b, h, s, ss),
            })
    except Exception:
        pass
    return rows


def _collect_earnings_dates(ticker: yf.Ticker, symbol: str,
                              exchange: str, collection_date: date) -> list[dict]:
    rows = []
    try:
        df = ticker.earnings_dates          # DataFrame indexed by earnings datetime
        if df is None or df.empty:
            return rows
        now = pd.Timestamp.now(tz="UTC")
        # Keep 2 future + 4 past earnings events
        future = df[df.index >= now].tail(2)
        past   = df[df.index <  now].head(4)
        combined = pd.concat([future, past])
        for dt, row in combined.iterrows():
            eps_est  = _safe_float(row.get("EPS Estimate", row.get("eps_estimate")))
            rep_eps  = _safe_float(row.get("Reported EPS", row.get("reported_eps")))
            surprise = float("nan")
            if eps_est and eps_est != 0 and not pd.isna(rep_eps):
                surprise = ((rep_eps - eps_est) / abs(eps_est)) * 100
            # Normalise timezone
            if dt.tzinfo is None:
                dt = dt.tz_localize("UTC")
            rows.append({
                "symbol":          symbol,
                "exchange":        exchange,
                "collection_date": collection_date,
                "earnings_date":   dt.to_pydatetime(),
                "eps_estimate":    eps_est,
                "reported_eps":    rep_eps,
                "surprise_pct":    surprise,
                "is_future":       dt >= now,
            })
    except Exception:
        pass
    return rows


# ── Handler ────────────────────────────────────────────────────────────────────

@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    exchange:        str  = event["exchange"]
    date_str:        str  = event["date"][:10]
    collection_date: date = date.fromisoformat(date_str)

    tickers = load_tickers(exchange)
    if not tickers:
        logger.warning("No tickers", extra={"exchange": exchange})
        return {"exchange": exchange, "date": date_str,
                "price_targets_ok": 0, "recommendations_ok": 0,
                "earnings_dates_ok": 0, "tickers_failed": 0}

    logger.info("Starting analyst data collection",
                extra={"exchange": exchange, "ticker_count": len(tickers)})

    pt_rows:   list[dict] = []
    rec_rows:  list[dict] = []
    earn_rows: list[dict] = []
    failed = 0

    for idx, symbol in enumerate(tickers):
        t = _fetch_with_retry(symbol)
        if t is None:
            failed += 1
            if idx < len(tickers) - 1:
                time.sleep(BATCH_SLEEP_S)
            continue

        pt = _collect_price_targets(t, symbol, exchange, collection_date)
        if pt:
            pt_rows.append(pt)

        rec_rows.extend(
            _collect_recommendations(t, symbol, exchange, collection_date)
        )
        earn_rows.extend(
            _collect_earnings_dates(t, symbol, exchange, collection_date)
        )

        if idx < len(tickers) - 1:
            time.sleep(BATCH_SLEEP_S)

    # ── Write price targets ───────────────────────────────────────────────────
    pt_key = ""
    if pt_rows:
        pt_key = f"analyst/type=price_targets/exchange={exchange}/date={date_str}/data.parquet"
        write_parquet(pd.DataFrame(pt_rows), pt_key, schema=PRICE_TARGETS_SCHEMA)
        logger.info("Price targets written", extra={"key": pt_key, "rows": len(pt_rows)})

    # ── Write recommendations ─────────────────────────────────────────────────
    rec_key = ""
    if rec_rows:
        rec_key = f"analyst/type=recommendations/exchange={exchange}/date={date_str}/data.parquet"
        write_parquet(pd.DataFrame(rec_rows), rec_key, schema=RECOMMENDATIONS_SCHEMA)
        logger.info("Recommendations written", extra={"key": rec_key, "rows": len(rec_rows)})

    # ── Write earnings dates ──────────────────────────────────────────────────
    earn_key = ""
    if earn_rows:
        earn_key = f"analyst/type=earnings_dates/exchange={exchange}/date={date_str}/data.parquet"
        write_parquet(pd.DataFrame(earn_rows), earn_key, schema=EARNINGS_DATES_SCHEMA)
        logger.info("Earnings dates written", extra={"key": earn_key, "rows": len(earn_rows)})

    metrics.add_dimension(name="Exchange", value=exchange)
    metrics.add_metric(name="AnalystPriceTargetsOk",    unit=MetricUnit.Count, value=len(pt_rows))
    metrics.add_metric(name="AnalystRecommendationsOk", unit=MetricUnit.Count, value=len(rec_rows))
    metrics.add_metric(name="EarningsDatesOk",          unit=MetricUnit.Count, value=len(earn_rows))
    metrics.add_metric(name="AnalystDataFailed",        unit=MetricUnit.Count, value=failed)

    return {
        "exchange":          exchange,
        "date":              date_str,
        "price_targets_ok":  len(pt_rows),
        "recommendations_ok": len(rec_rows),
        "earnings_dates_ok": len(earn_rows),
        "tickers_failed":    failed,
    }
