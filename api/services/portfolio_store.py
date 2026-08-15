"""
Portfolio & Watchlist storage — DynamoDB-backed persistence.

Table: market-data-portfolio
  PK: user_id
  SK: POSITION#{symbol} or WATCHLIST#{symbol}
"""

from __future__ import annotations

import os
from typing import Optional

import boto3
from boto3.dynamodb.conditions import Key

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
PORTFOLIO_TABLE = os.environ.get("PORTFOLIO_TABLE", "market-data-portfolio")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(PORTFOLIO_TABLE)


# ── Portfolio ─────────────────────────────────────────────────────────────────


def get_portfolio(user_id: str) -> list[dict]:
    """Get all positions for a user."""
    resp = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
        & Key("sk").begins_with("POSITION#"),
    )
    positions = []
    for item in resp.get("Items", []):
        positions.append(
            {
                "symbol": item["symbol"],
                "shares": float(item["shares"]),
                "avg_cost": float(item["avg_cost"]),
                "buy_date": item.get("buy_date", ""),
                "notes": item.get("notes", ""),
            }
        )
    return positions


def add_position(
    user_id: str,
    symbol: str,
    shares: float,
    avg_cost: float,
    buy_date: str,
    notes: str = "",
) -> None:
    """Add or update a position."""
    table.put_item(
        Item={
            "user_id": user_id,
            "sk": f"POSITION#{symbol}",
            "symbol": symbol,
            "shares": str(shares),
            "avg_cost": str(avg_cost),
            "buy_date": buy_date,
            "notes": notes,
        }
    )


def remove_position(user_id: str, symbol: str) -> None:
    """Remove a position."""
    table.delete_item(Key={"user_id": user_id, "sk": f"POSITION#{symbol}"})


# ── Watchlist ─────────────────────────────────────────────────────────────────


def get_watchlist(user_id: str) -> list[dict]:
    """Get all watchlist items for a user."""
    resp = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
        & Key("sk").begins_with("WATCHLIST#"),
    )
    items = []
    for item in resp.get("Items", []):
        items.append(
            {
                "symbol": item["symbol"],
                "target_price": float(item["target_price"]) if item.get("target_price") else None,
                "notes": item.get("notes", ""),
            }
        )
    return items


def add_to_watchlist(
    user_id: str, symbol: str, target_price: Optional[float] = None, notes: str = ""
) -> None:
    """Add a stock to the watchlist."""
    table.put_item(
        Item={
            "user_id": user_id,
            "sk": f"WATCHLIST#{symbol}",
            "symbol": symbol,
            "target_price": str(target_price) if target_price else "",
            "notes": notes,
        }
    )


def remove_from_watchlist(user_id: str, symbol: str) -> None:
    """Remove a stock from the watchlist."""
    table.delete_item(Key={"user_id": user_id, "sk": f"WATCHLIST#{symbol}"})
