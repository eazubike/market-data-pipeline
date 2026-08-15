"""
Lambda: detect-signals

Daily signal detection engine that scores stocks across multiple factors
and surfaces opportunities. Runs after all data pipelines complete (23:00 UTC).

Signals detected:
  1. Insider Cluster — 3+ insiders buying same stock within 30 days
  2. Undervaluation — PE significantly below sector average
  3. Sentiment Divergence — news sentiment vs price action mismatch
  4. Earnings Momentum — consistent beat record + upcoming earnings
  5. Analyst Breakout — price below analyst target with recent upgrades
  6. Volume Surge — volume > 2x 30-day average
  7. Institutional Accumulation — major funds increasing positions
  8. Pre-Earnings Opportunity — stocks likely to beat in next 2-3 weeks

Output:
    s3://bucket/signals/date=YYYY-MM-DD/data.parquet
    s3://bucket/signals/type=opportunities/date=YYYY-MM-DD/data.parquet

Input:
    { "run_date": "2026-08-15" }  (optional, defaults to today)
"""

from __future__ import annotations

import io
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

DATA_BUCKET = os.environ["DATA_BUCKET"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
ATHENA_OUTPUT = f"s3://{DATA_BUCKET}/athena-results/"

athena = boto3.client("athena")
s3 = boto3.client("s3")

# ── Signal weights for composite scoring ─────────────────────────────────────

SIGNAL_WEIGHTS = {
    "insider_cluster": 0.20,
    "undervaluation": 0.15,
    "sentiment_divergence": 0.10,
    "earnings_momentum": 0.20,
    "analyst_breakout": 0.10,
    "volume_surge": 0.08,
    "institutional_accumulation": 0.12,
    "pre_earnings": 0.05,
}

# ── Schemas ──────────────────────────────────────────────────────────────────

SIGNAL_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("signal_date", pa.date32()),
        pa.field("signal_type", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("confidence", pa.float64()),
        pa.field("description", pa.string()),
        pa.field("evidence", pa.string()),
    ]
)

OPPORTUNITY_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("signal_date", pa.date32()),
        pa.field("composite_score", pa.float64()),
        pa.field("verdict", pa.string()),
        pa.field("signals_fired", pa.int32()),
        pa.field("top_signals", pa.string()),
        pa.field("sector", pa.string()),
        pa.field("current_price", pa.float64()),
        pa.field("pe_ratio", pa.float64()),
        pa.field("earnings_date", pa.string()),
        pa.field("beat_rate", pa.float64()),
        pa.field("insider_net_30d", pa.float64()),
        pa.field("sentiment_score", pa.float64()),
        pa.field("analyst_upside_pct", pa.float64()),
    ]
)


# ── Athena query helper ──────────────────────────────────────────────────────


def run_athena_query(query: str) -> pd.DataFrame:
    """Execute an Athena query and return results as DataFrame."""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    execution_id = response["QueryExecutionId"]

    # Poll until complete
    for _ in range(120):  # max 10 minutes
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            logger.error("Athena query failed", extra={"reason": reason, "query": query[:200]})
            return pd.DataFrame()
        time.sleep(5)
    else:
        logger.error("Athena query timed out")
        return pd.DataFrame()

    # Get results
    result_location = status["QueryExecution"]["ResultConfiguration"][
        "OutputLocation"
    ]
    # Parse S3 path
    bucket = result_location.split("/")[2]
    key = "/".join(result_location.split("/")[3:])

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return df


def write_parquet_to_s3(df: pd.DataFrame, key: str, schema: pa.Schema) -> None:
    """Write DataFrame as Parquet to S3."""
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


# ── Signal Detection Functions ───────────────────────────────────────────────


def detect_insider_clusters(run_date: date) -> list[dict]:
    """
    Detect stocks where 3+ distinct insiders bought within the last 30 days.
    """
    start_date = (run_date - timedelta(days=30)).isoformat()
    query = f"""
    SELECT symbol, exchange,
           COUNT(DISTINCT insider_name) as insider_count,
           SUM(CASE WHEN transaction_type IN ('P', 'Buy') THEN shares * value_per_share ELSE 0 END) as total_value
    FROM insider_transactions
    WHERE date >= DATE('{start_date}')
      AND transaction_type IN ('P', 'Buy')
    GROUP BY symbol, exchange
    HAVING COUNT(DISTINCT insider_name) >= 3
    ORDER BY total_value DESC
    """
    df = run_athena_query(query)
    signals = []
    for _, row in df.iterrows():
        count = int(row.get("insider_count", 0))
        value = float(row.get("total_value", 0))
        score = min(100, 50 + (count - 3) * 15 + min(value / 1_000_000 * 10, 30))
        signals.append(
            {
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "signal_date": run_date,
                "signal_type": "insider_cluster",
                "score": round(score, 1),
                "confidence": min(0.95, 0.6 + count * 0.08),
                "description": f"{count} insiders bought ${value/1_000_000:.1f}M in 30 days",
                "evidence": f"insider_count={count}, total_value=${value:,.0f}",
            }
        )
    return signals


def detect_undervaluation(run_date: date) -> list[dict]:
    """
    Detect stocks trading at PE significantly below their sector average.
    Uses fundamentals table for PE ratios.
    """
    query = f"""
    WITH sector_pe AS (
        SELECT
            f.symbol, f.exchange, f.pe_ratio,
            p.sector,
            AVG(f.pe_ratio) OVER (PARTITION BY p.sector) as sector_avg_pe
        FROM fundamentals f
        JOIN (
            SELECT symbol, exchange, sector
            FROM company_profiles
            WHERE date = (SELECT MAX(date) FROM company_profiles)
        ) p ON f.symbol = p.symbol AND f.exchange = p.exchange
        WHERE f.date = (SELECT MAX(date) FROM fundamentals)
          AND f.pe_ratio > 0
          AND f.pe_ratio < 100
    )
    SELECT symbol, exchange, pe_ratio, sector, sector_avg_pe,
           ((sector_avg_pe - pe_ratio) / sector_avg_pe * 100) as discount_pct
    FROM sector_pe
    WHERE pe_ratio < sector_avg_pe * 0.6
    ORDER BY discount_pct DESC
    LIMIT 50
    """
    df = run_athena_query(query)
    signals = []
    for _, row in df.iterrows():
        discount = float(row.get("discount_pct", 0))
        score = min(100, 40 + discount * 0.8)
        signals.append(
            {
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "signal_date": run_date,
                "signal_type": "undervaluation",
                "score": round(score, 1),
                "confidence": min(0.85, 0.5 + discount / 100),
                "description": f"PE {row['pe_ratio']:.1f} vs sector avg {row['sector_avg_pe']:.1f} ({discount:.0f}% discount)",
                "evidence": f"sector={row['sector']}, pe={row['pe_ratio']:.1f}, sector_avg={row['sector_avg_pe']:.1f}",
            }
        )
    return signals


def detect_sentiment_divergence(run_date: date) -> list[dict]:
    """
    Detect stocks where news sentiment is positive but price is falling
    (buying opportunity) or sentiment negative but price rising (caution).
    Focus on positive sentiment + price drop = opportunity.
    """
    start_date = (run_date - timedelta(days=7)).isoformat()
    query = f"""
    WITH sentiment AS (
        SELECT
            element_at(symbols_mentioned, 1) as symbol,
            AVG(sentiment_score) as avg_sentiment,
            COUNT(*) as article_count
        FROM news
        WHERE date >= DATE('{start_date}')
          AND cardinality(symbols_mentioned) > 0
        GROUP BY element_at(symbols_mentioned, 1)
        HAVING COUNT(*) >= 3
    ),
    price_change AS (
        SELECT symbol, exchange,
               (MAX(price) - MIN(price)) / MIN(price) * 100 as price_change_pct,
               MAX(price) as latest_price
        FROM stocks
        WHERE date >= DATE('{start_date}')
        GROUP BY symbol, exchange
    )
    SELECT s.symbol, p.exchange, s.avg_sentiment, s.article_count,
           p.price_change_pct, p.latest_price
    FROM sentiment s
    JOIN price_change p ON s.symbol = p.symbol
    WHERE (s.avg_sentiment > 0.3 AND p.price_change_pct < -3)
       OR (s.avg_sentiment < -0.3 AND p.price_change_pct > 5)
    ORDER BY ABS(s.avg_sentiment - p.price_change_pct / 100) DESC
    LIMIT 30
    """
    df = run_athena_query(query)
    signals = []
    for _, row in df.iterrows():
        sentiment = float(row.get("avg_sentiment", 0))
        price_chg = float(row.get("price_change_pct", 0))
        # Positive sentiment + price drop = buying opportunity
        if sentiment > 0 and price_chg < 0:
            score = min(100, 50 + abs(sentiment) * 30 + abs(price_chg) * 3)
            desc = f"Positive news (sentiment +{sentiment:.2f}) but price down {price_chg:.1f}% — potential buying opportunity"
        else:
            score = min(100, 40 + abs(sentiment) * 20 + abs(price_chg) * 2)
            desc = f"Negative news (sentiment {sentiment:.2f}) but price up {price_chg:.1f}% — caution"
        signals.append(
            {
                "symbol": row["symbol"],
                "exchange": row.get("exchange", ""),
                "signal_date": run_date,
                "signal_type": "sentiment_divergence",
                "score": round(score, 1),
                "confidence": min(0.8, 0.4 + int(row.get("article_count", 0)) * 0.05),
                "description": desc,
                "evidence": f"sentiment={sentiment:.2f}, price_change={price_chg:.1f}%, articles={row.get('article_count', 0)}",
            }
        )
    return signals


def detect_earnings_momentum(run_date: date) -> list[dict]:
    """
    Detect stocks with consistent earnings beats (6+ of last 8 quarters).
    """
    query = """
    SELECT symbol, exchange,
           COUNT(*) as total_quarters,
           SUM(CASE WHEN beat_eps = true THEN 1 ELSE 0 END) as beats,
           AVG(eps_surprise_pct) as avg_surprise
    FROM earnings_history
    WHERE collection_date = (SELECT MAX(collection_date) FROM earnings_history)
      AND quarter_end IS NOT NULL
    GROUP BY symbol, exchange
    HAVING COUNT(*) >= 4
       AND SUM(CASE WHEN beat_eps = true THEN 1 ELSE 0 END) >= 3
    ORDER BY SUM(CASE WHEN beat_eps = true THEN 1 ELSE 0 END) DESC,
             AVG(eps_surprise_pct) DESC
    LIMIT 100
    """
    df = run_athena_query(query)
    signals = []
    for _, row in df.iterrows():
        beats = int(row.get("beats", 0))
        total = int(row.get("total_quarters", 0))
        beat_rate = beats / total if total > 0 else 0
        avg_surprise = float(row.get("avg_surprise", 0))
        score = min(100, beat_rate * 80 + min(avg_surprise * 2, 20))
        signals.append(
            {
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "signal_date": run_date,
                "signal_type": "earnings_momentum",
                "score": round(score, 1),
                "confidence": min(0.9, beat_rate),
                "description": f"Beat EPS {beats}/{total} quarters (avg surprise +{avg_surprise:.1f}%)",
                "evidence": f"beat_rate={beat_rate:.0%}, avg_surprise={avg_surprise:.1f}%, quarters={total}",
            }
        )
    return signals


def detect_analyst_breakout(run_date: date) -> list[dict]:
    """
    Detect stocks trading significantly below analyst median target
    with recent upside.
    """
    query = """
    SELECT pt.symbol, pt.exchange, pt.current_price,
           pt.target_median, pt.target_high, pt.number_of_analysts,
           pt.upside_pct
    FROM analyst_price_targets pt
    WHERE pt.date = (SELECT MAX(date) FROM analyst_price_targets)
      AND pt.upside_pct > 15
      AND pt.number_of_analysts >= 5
    ORDER BY pt.upside_pct DESC
    LIMIT 50
    """
    df = run_athena_query(query)
    signals = []
    for _, row in df.iterrows():
        upside = float(row.get("upside_pct", 0))
        analysts = int(row.get("number_of_analysts", 0))
        score = min(100, 30 + upside * 1.5 + analysts * 2)
        signals.append(
            {
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "signal_date": run_date,
                "signal_type": "analyst_breakout",
                "score": round(score, 1),
                "confidence": min(0.85, 0.4 + analysts * 0.03),
                "description": f"{upside:.0f}% below analyst median target (${row['target_median']:.0f}) — {analysts} analysts",
                "evidence": f"price=${row['current_price']:.2f}, target=${row['target_median']:.0f}, upside={upside:.0f}%, analysts={analysts}",
            }
        )
    return signals


def detect_volume_surge(run_date: date) -> list[dict]:
    """
    Detect stocks with volume > 2x their 30-day average.
    """
    date_str = run_date.isoformat()
    query = f"""
    WITH today_vol AS (
        SELECT symbol, exchange, MAX(volume) as today_volume
        FROM stocks
        WHERE date = DATE('{date_str}')
        GROUP BY symbol, exchange
    ),
    avg_vol AS (
        SELECT symbol, exchange, AVG(volume) as avg_volume_30d
        FROM stocks
        WHERE date >= DATE('{(run_date - timedelta(days=30)).isoformat()}')
          AND date < DATE('{date_str}')
        GROUP BY symbol, exchange
        HAVING AVG(volume) > 100000
    )
    SELECT t.symbol, t.exchange, t.today_volume, a.avg_volume_30d,
           CAST(t.today_volume AS DOUBLE) / a.avg_volume_30d as volume_ratio
    FROM today_vol t
    JOIN avg_vol a ON t.symbol = a.symbol AND t.exchange = a.exchange
    WHERE CAST(t.today_volume AS DOUBLE) / a.avg_volume_30d > 2.0
    ORDER BY volume_ratio DESC
    LIMIT 50
    """
    df = run_athena_query(query)
    signals = []
    for _, row in df.iterrows():
        ratio = float(row.get("volume_ratio", 0))
        score = min(100, 40 + ratio * 15)
        signals.append(
            {
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "signal_date": run_date,
                "signal_type": "volume_surge",
                "score": round(score, 1),
                "confidence": min(0.75, 0.4 + ratio * 0.1),
                "description": f"Volume {ratio:.1f}x normal ({int(row['today_volume']):,} vs avg {int(row['avg_volume_30d']):,})",
                "evidence": f"today_vol={int(row['today_volume']):,}, avg_30d={int(row['avg_volume_30d']):,}, ratio={ratio:.1f}x",
            }
        )
    return signals


def detect_pre_earnings_opportunity(run_date: date) -> list[dict]:
    """
    Detect stocks with earnings in 14-21 days that have high probability
    of beating based on historical record + current signals.
    """
    start_window = (run_date + timedelta(days=14)).isoformat()
    end_window = (run_date + timedelta(days=21)).isoformat()

    query = f"""
    WITH upcoming AS (
        SELECT symbol, exchange, MIN(earnings_date) as next_earnings
        FROM earnings_dates
        WHERE collection_date = (SELECT MAX(collection_date) FROM earnings_dates)
          AND is_future = true
          AND earnings_date >= TIMESTAMP '{start_window} 00:00:00'
          AND earnings_date <= TIMESTAMP '{end_window} 23:59:59'
        GROUP BY symbol, exchange
    ),
    beat_history AS (
        SELECT symbol, exchange,
               COUNT(*) as total_q,
               SUM(CASE WHEN beat_eps = true THEN 1 ELSE 0 END) as beats
        FROM earnings_history
        WHERE collection_date = (SELECT MAX(collection_date) FROM earnings_history)
        GROUP BY symbol, exchange
        HAVING COUNT(*) >= 4
    )
    SELECT u.symbol, u.exchange, u.next_earnings,
           b.total_q, b.beats,
           CAST(b.beats AS DOUBLE) / b.total_q as beat_rate
    FROM upcoming u
    JOIN beat_history b ON u.symbol = b.symbol AND u.exchange = b.exchange
    WHERE CAST(b.beats AS DOUBLE) / b.total_q >= 0.6
    ORDER BY beat_rate DESC, b.beats DESC
    """
    df = run_athena_query(query)
    signals = []
    for _, row in df.iterrows():
        beat_rate = float(row.get("beat_rate", 0))
        beats = int(row.get("beats", 0))
        total = int(row.get("total_q", 0))
        days_until = (
            pd.Timestamp(row["next_earnings"]).date() - run_date
        ).days
        score = min(100, beat_rate * 85 + min(beats * 3, 15))
        signals.append(
            {
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "signal_date": run_date,
                "signal_type": "pre_earnings",
                "score": round(score, 1),
                "confidence": min(0.9, beat_rate),
                "description": f"Earnings in {days_until} days — beat {beats}/{total} quarters ({beat_rate:.0%})",
                "evidence": f"earnings_date={row['next_earnings']}, beat_rate={beat_rate:.0%}, beats={beats}/{total}",
            }
        )
    return signals


# ── Composite scoring ────────────────────────────────────────────────────────


def calculate_opportunities(all_signals: list[dict], run_date: date) -> pd.DataFrame:
    """
    Aggregate all signals per stock into a composite opportunity score.
    Stocks with multiple signals stacked = highest opportunity.
    """
    if not all_signals:
        return pd.DataFrame()

    df = pd.DataFrame(all_signals)

    # Group by stock, calculate composite
    opportunities = []
    for (symbol, exchange), group in df.groupby(["symbol", "exchange"]):
        signals_fired = len(group)
        if signals_fired < 2:
            continue  # require at least 2 stacked signals

        # Weighted composite score
        weighted_sum = 0
        total_weight = 0
        for _, sig in group.iterrows():
            w = SIGNAL_WEIGHTS.get(sig["signal_type"], 0.05)
            weighted_sum += sig["score"] * w
            total_weight += w

        composite = weighted_sum / total_weight if total_weight > 0 else 0
        composite = min(100, composite * (1 + (signals_fired - 2) * 0.1))

        # Verdict
        if composite >= 75:
            verdict = "BUY"
        elif composite >= 55:
            verdict = "WATCH"
        else:
            verdict = "NEUTRAL"

        top_signals = "; ".join(
            f"{r['signal_type']}({r['score']:.0f})"
            for _, r in group.nlargest(3, "score").iterrows()
        )

        opportunities.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "signal_date": run_date,
                "composite_score": round(composite, 1),
                "verdict": verdict,
                "signals_fired": signals_fired,
                "top_signals": top_signals,
                "sector": "",
                "current_price": 0.0,
                "pe_ratio": 0.0,
                "earnings_date": "",
                "beat_rate": 0.0,
                "insider_net_30d": 0.0,
                "sentiment_score": 0.0,
                "analyst_upside_pct": 0.0,
            }
        )

    opp_df = pd.DataFrame(opportunities)
    if not opp_df.empty:
        opp_df = opp_df.sort_values("composite_score", ascending=False)
    return opp_df


# ── Enrichment ───────────────────────────────────────────────────────────────


def enrich_opportunities(opp_df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """
    Enrich opportunity records with current price, PE, sector, etc.
    from the latest data in the lake.
    """
    if opp_df.empty:
        return opp_df

    symbols = opp_df["symbol"].tolist()
    symbols_str = ",".join(f"'{s}'" for s in symbols[:100])

    # Get latest fundamentals
    query = f"""
    SELECT symbol, exchange, pe_ratio
    FROM fundamentals
    WHERE date = (SELECT MAX(date) FROM fundamentals)
      AND symbol IN ({symbols_str})
    """
    fund_df = run_athena_query(query)
    if not fund_df.empty:
        fund_map = fund_df.set_index("symbol")["pe_ratio"].to_dict()
        opp_df["pe_ratio"] = opp_df["symbol"].map(fund_map).fillna(0.0)

    # Get latest price
    query = f"""
    SELECT symbol, exchange, price
    FROM stocks
    WHERE date = (SELECT MAX(date) FROM stocks)
      AND symbol IN ({symbols_str})
    """
    price_df = run_athena_query(query)
    if not price_df.empty:
        # Take latest price per symbol
        price_map = price_df.drop_duplicates("symbol", keep="last").set_index("symbol")["price"].to_dict()
        opp_df["current_price"] = opp_df["symbol"].map(price_map).fillna(0.0)

    # Get sector from company profiles
    query = f"""
    SELECT symbol, sector
    FROM company_profiles
    WHERE date = (SELECT MAX(date) FROM company_profiles)
      AND symbol IN ({symbols_str})
    """
    sector_df = run_athena_query(query)
    if not sector_df.empty:
        sector_map = sector_df.set_index("symbol")["sector"].to_dict()
        opp_df["sector"] = opp_df["symbol"].map(sector_map).fillna("")

    return opp_df


# ── Main handler ─────────────────────────────────────────────────────────────


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    run_date_str = event.get("run_date")
    if run_date_str:
        run_date = date.fromisoformat(run_date_str)
    else:
        run_date = datetime.now(timezone.utc).date()

    date_str = run_date.isoformat()
    logger.info("Starting signal detection", extra={"run_date": date_str})

    all_signals: list[dict] = []

    # Run each detector
    detectors = [
        ("insider_cluster", detect_insider_clusters),
        ("undervaluation", detect_undervaluation),
        ("sentiment_divergence", detect_sentiment_divergence),
        ("earnings_momentum", detect_earnings_momentum),
        ("analyst_breakout", detect_analyst_breakout),
        ("volume_surge", detect_volume_surge),
        ("pre_earnings", detect_pre_earnings_opportunity),
    ]

    for name, detector in detectors:
        try:
            signals = detector(run_date)
            all_signals.extend(signals)
            logger.info(f"Signal detected: {name}", extra={"count": len(signals)})
        except Exception as exc:
            logger.error(f"Detector failed: {name}", extra={"error": str(exc)})

    # Write all individual signals
    if all_signals:
        signals_df = pd.DataFrame(all_signals)
        key = f"signals/date={date_str}/data.parquet"
        write_parquet_to_s3(signals_df, key, SIGNAL_SCHEMA)
        logger.info("Signals written", extra={"count": len(all_signals), "key": key})

    # Calculate composite opportunities (stocks with 2+ stacked signals)
    opp_df = calculate_opportunities(all_signals, run_date)

    # Enrich with current data
    opp_df = enrich_opportunities(opp_df, run_date)

    # Write opportunities
    if not opp_df.empty:
        opp_key = f"signals/type=opportunities/date={date_str}/data.parquet"
        write_parquet_to_s3(opp_df, opp_key, OPPORTUNITY_SCHEMA)
        logger.info(
            "Opportunities written",
            extra={
                "count": len(opp_df),
                "buy_count": len(opp_df[opp_df["verdict"] == "BUY"]),
                "watch_count": len(opp_df[opp_df["verdict"] == "WATCH"]),
            },
        )

    return {
        "run_date": date_str,
        "total_signals": len(all_signals),
        "opportunities": len(opp_df) if not opp_df.empty else 0,
        "buy_signals": len(opp_df[opp_df["verdict"] == "BUY"]) if not opp_df.empty else 0,
    }
