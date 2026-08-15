"""Stock endpoints — price data, fundamentals, insider, news, analyst, earnings."""

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Query

from api.services.athena_client import query_athena

router = APIRouter()


@router.get("/{symbol}")
async def get_stock_overview(symbol: str):
    """Full stock overview — latest price, fundamentals, signals."""
    symbol = symbol.upper()

    # Latest price
    price_data = await query_athena(f"""
        SELECT symbol, exchange, price, open, high, low, close, volume, timestamp, currency
        FROM stocks
        WHERE symbol = '{symbol}'
          AND date = (SELECT MAX(date) FROM stocks WHERE symbol = '{symbol}')
        ORDER BY timestamp DESC
        LIMIT 1
    """)

    # Latest fundamentals
    fundamentals = await query_athena(f"""
        SELECT *
        FROM fundamentals
        WHERE symbol = '{symbol}'
          AND date = (SELECT MAX(date) FROM fundamentals WHERE symbol = '{symbol}')
        LIMIT 1
    """)

    # Active signals for this stock
    signals = await query_athena(f"""
        SELECT signal_type, score, confidence, description
        FROM signals
        WHERE symbol = '{symbol}'
          AND date = (SELECT MAX(date) FROM signals WHERE symbol = '{symbol}')
        ORDER BY score DESC
    """)

    return {
        "symbol": symbol,
        "price": price_data[0] if price_data else None,
        "fundamentals": fundamentals[0] if fundamentals else None,
        "signals": signals,
    }


@router.get("/{symbol}/history")
async def get_price_history(
    symbol: str,
    days: int = Query(default=30, ge=1, le=365),
):
    """Price history for charting."""
    symbol = symbol.upper()
    start_date = (date.today() - timedelta(days=days)).isoformat()

    data = await query_athena(f"""
        SELECT date, timestamp, price, open, high, low, close, volume
        FROM stocks
        WHERE symbol = '{symbol}'
          AND date >= DATE('{start_date}')
        ORDER BY timestamp ASC
    """)

    return {"symbol": symbol, "period_days": days, "data": data}


@router.get("/{symbol}/fundamentals")
async def get_fundamentals(symbol: str):
    """Detailed fundamentals — PE, PB, margins, etc."""
    symbol = symbol.upper()

    data = await query_athena(f"""
        SELECT *
        FROM fundamentals
        WHERE symbol = '{symbol}'
          AND date = (SELECT MAX(date) FROM fundamentals WHERE symbol = '{symbol}')
        LIMIT 1
    """)

    return {"symbol": symbol, "fundamentals": data[0] if data else None}


@router.get("/{symbol}/insider")
async def get_insider_activity(symbol: str, days: int = Query(default=90)):
    """Insider transactions for a stock."""
    symbol = symbol.upper()
    start_date = (date.today() - timedelta(days=days)).isoformat()

    data = await query_athena(f"""
        SELECT transaction_date, insider_name, insider_title,
               transaction_type, shares, price_per_share, total_value
        FROM insider_transactions
        WHERE symbol = '{symbol}'
          AND date >= DATE('{start_date}')
        ORDER BY transaction_date DESC
    """)

    # Calculate net insider activity
    net_value = sum(
        r.get("total_value", 0) if r.get("transaction_type") in ("P", "Buy") else -r.get("total_value", 0)
        for r in data
    )

    return {
        "symbol": symbol,
        "period_days": days,
        "transactions": data,
        "net_value": net_value,
        "net_direction": "buying" if net_value > 0 else "selling",
    }


@router.get("/{symbol}/news")
async def get_stock_news(symbol: str, days: int = Query(default=7)):
    """Recent news mentioning this stock with sentiment."""
    symbol = symbol.upper()
    start_date = (date.today() - timedelta(days=days)).isoformat()

    data = await query_athena(f"""
        SELECT headline, source, url, published_at, sentiment_score
        FROM news
        WHERE contains(symbols_mentioned, '{symbol}')
          AND date >= DATE('{start_date}')
        ORDER BY published_at DESC
        LIMIT 20
    """)

    avg_sentiment = (
        sum(r.get("sentiment_score", 0) for r in data) / len(data) if data else 0
    )

    return {
        "symbol": symbol,
        "articles": data,
        "avg_sentiment": round(avg_sentiment, 3),
        "article_count": len(data),
    }


@router.get("/{symbol}/analyst")
async def get_analyst_data(symbol: str):
    """Analyst price targets and recommendations."""
    symbol = symbol.upper()

    targets = await query_athena(f"""
        SELECT current_price, target_low, target_mean, target_high,
               target_median, number_of_analysts, upside_pct
        FROM analyst_price_targets
        WHERE symbol = '{symbol}'
          AND date = (SELECT MAX(date) FROM analyst_price_targets WHERE symbol = '{symbol}')
        LIMIT 1
    """)

    recommendations = await query_athena(f"""
        SELECT period, strong_buy, buy, hold, sell, strong_sell, consensus
        FROM analyst_recommendations
        WHERE symbol = '{symbol}'
          AND date = (SELECT MAX(date) FROM analyst_recommendations WHERE symbol = '{symbol}')
        ORDER BY period
    """)

    return {
        "symbol": symbol,
        "price_targets": targets[0] if targets else None,
        "recommendations": recommendations,
    }


@router.get("/{symbol}/earnings")
async def get_earnings_history(symbol: str):
    """Earnings beat/miss history."""
    symbol = symbol.upper()

    history = await query_athena(f"""
        SELECT quarter_end, eps_estimate, eps_actual, eps_surprise_pct, beat_eps
        FROM earnings_history
        WHERE symbol = '{symbol}'
          AND collection_date = (SELECT MAX(collection_date) FROM earnings_history WHERE symbol = '{symbol}')
        ORDER BY quarter_end DESC
    """)

    # Calculate beat rate
    total = len(history)
    beats = sum(1 for r in history if r.get("beat_eps"))
    beat_rate = beats / total if total > 0 else 0

    # Next earnings date
    upcoming = await query_athena(f"""
        SELECT earnings_date, eps_estimate
        FROM earnings_dates
        WHERE symbol = '{symbol}'
          AND is_future = true
          AND collection_date = (SELECT MAX(collection_date) FROM earnings_dates WHERE symbol = '{symbol}')
        ORDER BY earnings_date ASC
        LIMIT 1
    """)

    return {
        "symbol": symbol,
        "history": history,
        "beat_rate": round(beat_rate, 2),
        "beats": beats,
        "total_quarters": total,
        "next_earnings": upcoming[0] if upcoming else None,
    }


@router.get("/{symbol}/profile")
async def get_company_profile(symbol: str):
    """Company profile — sector, industry, description, key metrics."""
    symbol = symbol.upper()

    data = await query_athena(f"""
        SELECT *
        FROM company_profiles
        WHERE symbol = '{symbol}'
          AND date = (SELECT MAX(date) FROM company_profiles WHERE symbol = '{symbol}')
        LIMIT 1
    """)

    return {"symbol": symbol, "profile": data[0] if data else None}
