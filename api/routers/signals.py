"""Signals & Opportunities endpoints."""

from typing import Optional

from fastapi import APIRouter, Query

from api.services.athena_client import query_athena

router = APIRouter()


@router.get("")
async def get_signals(
    signal_type: Optional[str] = Query(default=None),
    min_score: float = Query(default=0),
    limit: int = Query(default=50, le=200),
):
    """
    Get today's signals, optionally filtered by type and minimum score.
    Signal types: insider_cluster, undervaluation, sentiment_divergence,
                  earnings_momentum, analyst_breakout, volume_surge, pre_earnings
    """
    type_filter = f"AND signal_type = '{signal_type}'" if signal_type else ""

    data = await query_athena(f"""
        SELECT symbol, exchange, signal_type, score, confidence, description, evidence
        FROM signals
        WHERE date = (SELECT MAX(date) FROM signals)
          AND score >= {min_score}
          {type_filter}
        ORDER BY score DESC
        LIMIT {limit}
    """)

    return {"signals": data, "count": len(data)}


@router.get("/opportunities")
async def get_opportunities(
    verdict: Optional[str] = Query(default=None),
    min_score: float = Query(default=0),
    sector: Optional[str] = Query(default=None),
    limit: int = Query(default=20, le=100),
):
    """
    Get top scored opportunities (stocks with 2+ stacked signals).
    Filter by verdict (BUY, WATCH, NEUTRAL), min score, or sector.
    """
    filters = [f"composite_score >= {min_score}"]
    if verdict:
        filters.append(f"verdict = '{verdict.upper()}'")
    if sector:
        filters.append(f"sector = '{sector}'")

    where_clause = " AND ".join(filters)

    data = await query_athena(f"""
        SELECT symbol, exchange, composite_score, verdict, signals_fired,
               top_signals, sector, current_price, pe_ratio,
               earnings_date, beat_rate, insider_net_30d,
               sentiment_score, analyst_upside_pct
        FROM opportunities
        WHERE date = (SELECT MAX(date) FROM opportunities)
          AND {where_clause}
        ORDER BY composite_score DESC
        LIMIT {limit}
    """)

    return {"opportunities": data, "count": len(data)}


@router.get("/pre-earnings")
async def get_pre_earnings_opportunities(
    min_beat_rate: float = Query(default=0.6),
    days_ahead: int = Query(default=21),
):
    """
    Stocks with earnings coming in the next N days that have high
    beat probability based on historical record + current signals.
    """
    data = await query_athena(f"""
        SELECT symbol, exchange, score, confidence, description, evidence
        FROM signals
        WHERE date = (SELECT MAX(date) FROM signals)
          AND signal_type = 'pre_earnings'
          AND score >= 50
        ORDER BY score DESC
        LIMIT 30
    """)

    return {"pre_earnings_opportunities": data, "count": len(data)}


@router.get("/history/{symbol}")
async def get_signal_history(symbol: str, days: int = Query(default=30)):
    """Historical signals for a specific stock."""
    symbol = symbol.upper()

    data = await query_athena(f"""
        SELECT signal_date, signal_type, score, confidence, description
        FROM signals
        WHERE symbol = '{symbol}'
        ORDER BY signal_date DESC
        LIMIT 100
    """)

    return {"symbol": symbol, "signal_history": data}
