"""Portfolio & Watchlist endpoints — user positions and tracking."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from api.services.athena_client import query_athena
from api.services.portfolio_store import (
    get_portfolio,
    add_position,
    remove_position,
    get_watchlist,
    add_to_watchlist,
    remove_from_watchlist,
)

router = APIRouter()


# ── Models ────────────────────────────────────────────────────────────────────


class PositionCreate(BaseModel):
    symbol: str
    shares: float
    avg_cost: float
    buy_date: str  # ISO date
    notes: Optional[str] = ""


class WatchlistItem(BaseModel):
    symbol: str
    target_price: Optional[float] = None
    notes: Optional[str] = ""


# ── Portfolio endpoints ───────────────────────────────────────────────────────


@router.get("")
async def get_user_portfolio(user_id: str = Query(default="default")):
    """
    Get portfolio with current P&L calculated from latest prices.
    """
    positions = get_portfolio(user_id)

    if not positions:
        return {"positions": [], "total_value": 0, "total_cost": 0, "total_pnl": 0}

    # Get current prices for all held symbols
    symbols = [p["symbol"] for p in positions]
    symbols_str = ",".join(f"'{s}'" for s in symbols)

    prices = await query_athena(f"""
        SELECT symbol, price
        FROM stocks
        WHERE date = (SELECT MAX(date) FROM stocks)
          AND symbol IN ({symbols_str})
    """)
    price_map = {r["symbol"]: r["price"] for r in prices}

    # Get signals for held stocks
    signals = await query_athena(f"""
        SELECT symbol, composite_score, verdict
        FROM opportunities
        WHERE date = (SELECT MAX(date) FROM opportunities)
          AND symbol IN ({symbols_str})
    """)
    signal_map = {r["symbol"]: r for r in signals}

    # Calculate P&L per position
    enriched = []
    total_value = 0
    total_cost = 0

    for pos in positions:
        current_price = price_map.get(pos["symbol"], 0)
        cost_basis = pos["shares"] * pos["avg_cost"]
        market_value = pos["shares"] * current_price
        pnl = market_value - cost_basis
        pnl_pct = (pnl / cost_basis * 100) if cost_basis > 0 else 0

        sig = signal_map.get(pos["symbol"], {})

        enriched.append(
            {
                **pos,
                "current_price": current_price,
                "market_value": round(market_value, 2),
                "cost_basis": round(cost_basis, 2),
                "pnl": round(pnl, 2),
                "pnl_pct": round(pnl_pct, 2),
                "signal_score": sig.get("composite_score"),
                "verdict": sig.get("verdict"),
            }
        )

        total_value += market_value
        total_cost += cost_basis

    return {
        "positions": enriched,
        "total_value": round(total_value, 2),
        "total_cost": round(total_cost, 2),
        "total_pnl": round(total_value - total_cost, 2),
        "total_pnl_pct": round(
            (total_value - total_cost) / total_cost * 100 if total_cost > 0 else 0, 2
        ),
    }


@router.post("/add")
async def add_portfolio_position(position: PositionCreate, user_id: str = Query(default="default")):
    """Add a new position to the portfolio."""
    add_position(
        user_id,
        symbol=position.symbol.upper(),
        shares=position.shares,
        avg_cost=position.avg_cost,
        buy_date=position.buy_date,
        notes=position.notes,
    )
    return {"status": "added", "symbol": position.symbol.upper()}


@router.delete("/{symbol}")
async def delete_portfolio_position(symbol: str, user_id: str = Query(default="default")):
    """Remove a position from the portfolio."""
    remove_position(user_id, symbol.upper())
    return {"status": "removed", "symbol": symbol.upper()}


# ── Watchlist endpoints ───────────────────────────────────────────────────────


@router.get("/watchlist")
async def get_user_watchlist(user_id: str = Query(default="default")):
    """Get watchlist with current prices and signals."""
    items = get_watchlist(user_id)

    if not items:
        return {"watchlist": []}

    symbols = [item["symbol"] for item in items]
    symbols_str = ",".join(f"'{s}'" for s in symbols)

    # Current prices
    prices = await query_athena(f"""
        SELECT symbol, price
        FROM stocks
        WHERE date = (SELECT MAX(date) FROM stocks)
          AND symbol IN ({symbols_str})
    """)
    price_map = {r["symbol"]: r["price"] for r in prices}

    # Signals
    signals = await query_athena(f"""
        SELECT symbol, composite_score, verdict, top_signals
        FROM opportunities
        WHERE date = (SELECT MAX(date) FROM opportunities)
          AND symbol IN ({symbols_str})
    """)
    signal_map = {r["symbol"]: r for r in signals}

    enriched = []
    for item in items:
        sig = signal_map.get(item["symbol"], {})
        enriched.append(
            {
                **item,
                "current_price": price_map.get(item["symbol"], 0),
                "signal_score": sig.get("composite_score"),
                "verdict": sig.get("verdict"),
                "top_signals": sig.get("top_signals"),
            }
        )

    return {"watchlist": enriched}


@router.post("/watchlist/add")
async def add_watchlist_item(item: WatchlistItem, user_id: str = Query(default="default")):
    """Add a stock to the watchlist."""
    add_to_watchlist(user_id, item.symbol.upper(), item.target_price, item.notes)
    return {"status": "added", "symbol": item.symbol.upper()}


@router.delete("/watchlist/{symbol}")
async def remove_watchlist_item(symbol: str, user_id: str = Query(default="default")):
    """Remove a stock from the watchlist."""
    remove_from_watchlist(user_id, symbol.upper())
    return {"status": "removed", "symbol": symbol.upper()}
