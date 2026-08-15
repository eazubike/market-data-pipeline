"""Screener endpoint — filter stocks by multiple criteria."""

from typing import Optional

from fastapi import APIRouter, Query

from api.services.athena_client import query_athena

router = APIRouter()


@router.get("")
async def screen_stocks(
    max_pe: Optional[float] = Query(default=None, description="Maximum PE ratio"),
    min_pe: Optional[float] = Query(default=None, description="Minimum PE ratio"),
    sector: Optional[str] = Query(default=None, description="Sector filter"),
    min_dividend_yield: Optional[float] = Query(default=None),
    min_signal_score: Optional[float] = Query(default=None, description="Minimum opportunity score"),
    insider_buying: Optional[bool] = Query(default=None, description="Has insider buying in last 30d"),
    min_beat_rate: Optional[float] = Query(default=None, description="Min earnings beat rate (0-1)"),
    exchange: Optional[str] = Query(default=None),
    sort_by: str = Query(default="composite_score", description="Sort field"),
    limit: int = Query(default=50, le=200),
):
    """
    Multi-factor stock screener. Joins fundamentals, signals, and earnings
    to find stocks matching specified criteria.
    """
    # Build dynamic WHERE clauses
    conditions = ["1=1"]  # base

    if max_pe is not None:
        conditions.append(f"f.pe_ratio <= {max_pe}")
    if min_pe is not None:
        conditions.append(f"f.pe_ratio >= {min_pe}")
    if min_dividend_yield is not None:
        conditions.append(f"f.dividend_yield >= {min_dividend_yield}")
    if exchange is not None:
        conditions.append(f"f.exchange = '{exchange}'")

    where = " AND ".join(conditions)

    # Base query: fundamentals + opportunities
    query = f"""
    WITH latest_fund AS (
        SELECT symbol, exchange, pe_ratio, dividend_yield, eps,
               revenue_ttm, debt_to_equity, shares_outstanding
        FROM fundamentals
        WHERE date = (SELECT MAX(date) FROM fundamentals)
          AND pe_ratio > 0
    ),
    latest_opp AS (
        SELECT symbol, composite_score, verdict, signals_fired, top_signals, sector
        FROM opportunities
        WHERE date = (SELECT MAX(date) FROM opportunities)
    ),
    latest_earnings AS (
        SELECT symbol,
               CAST(SUM(CASE WHEN beat_eps = true THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as beat_rate
        FROM earnings_history
        WHERE collection_date = (SELECT MAX(collection_date) FROM earnings_history)
        GROUP BY symbol
        HAVING COUNT(*) >= 4
    )
    SELECT f.symbol, f.exchange, f.pe_ratio, f.dividend_yield, f.eps,
           f.revenue_ttm, f.debt_to_equity,
           o.composite_score, o.verdict, o.signals_fired, o.top_signals, o.sector,
           e.beat_rate
    FROM latest_fund f
    LEFT JOIN latest_opp o ON f.symbol = o.symbol
    LEFT JOIN latest_earnings e ON f.symbol = e.symbol
    WHERE {where}
    """

    # Add post-join filters
    if min_signal_score is not None:
        query += f" AND o.composite_score >= {min_signal_score}"
    if sector is not None:
        query += f" AND o.sector = '{sector}'"
    if min_beat_rate is not None:
        query += f" AND e.beat_rate >= {min_beat_rate}"

    # Sort
    sort_map = {
        "composite_score": "o.composite_score DESC NULLS LAST",
        "pe_ratio": "f.pe_ratio ASC",
        "dividend_yield": "f.dividend_yield DESC",
        "beat_rate": "e.beat_rate DESC NULLS LAST",
    }
    order = sort_map.get(sort_by, "o.composite_score DESC NULLS LAST")
    query += f" ORDER BY {order} LIMIT {limit}"

    data = await query_athena(query)

    return {"results": data, "count": len(data), "filters_applied": conditions[1:]}


@router.get("/presets")
async def get_screener_presets():
    """Pre-built screener configurations."""
    return {
        "presets": [
            {
                "name": "Value + Insider Buying",
                "description": "Low PE stocks where insiders are buying",
                "params": {"max_pe": 15, "insider_buying": True, "min_signal_score": 50},
            },
            {
                "name": "Earnings Beaters",
                "description": "Stocks that consistently beat earnings estimates",
                "params": {"min_beat_rate": 0.75, "min_signal_score": 40},
            },
            {
                "name": "Undervalued Growth",
                "description": "Below-average PE with strong revenue growth",
                "params": {"max_pe": 20, "min_signal_score": 60},
            },
            {
                "name": "High Conviction BUY",
                "description": "Top scored opportunities with BUY verdict",
                "params": {"min_signal_score": 75},
            },
            {
                "name": "Dividend Income",
                "description": "High dividend yield with consistent earnings",
                "params": {"min_dividend_yield": 3.0, "min_beat_rate": 0.6},
            },
        ]
    }
