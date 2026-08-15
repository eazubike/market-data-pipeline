"""
MarketPulse API — FastAPI backend serving stock analytics data.

Endpoints:
  /api/stocks/{symbol}          — Price history, latest quote
  /api/stocks/{symbol}/fundamentals — PE, EPS, margins, etc.
  /api/stocks/{symbol}/insider   — Insider transactions
  /api/stocks/{symbol}/news      — News with sentiment
  /api/stocks/{symbol}/analyst   — Price targets, recommendations
  /api/stocks/{symbol}/earnings  — Earnings history (beat/miss)
  /api/stocks/{symbol}/profile   — Company info, sector, description
  /api/signals                   — Today's signals (all types)
  /api/opportunities             — Top scored opportunities
  /api/screener                  — Filter stocks by criteria
  /api/portfolio                 — User portfolio CRUD
  /api/watchlist                 — User watchlist CRUD
  /api/dashboard                 — Aggregated dashboard data
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import stocks, signals, portfolio, screener, dashboard

app = FastAPI(
    title="MarketPulse API",
    description="AI-powered stock analytics and signal detection",
    version="1.0.0",
)

# CORS — allow React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(stocks.router, prefix="/api/stocks", tags=["Stocks"])
app.include_router(signals.router, prefix="/api/signals", tags=["Signals"])
app.include_router(portfolio.router, prefix="/api/portfolio", tags=["Portfolio"])
app.include_router(screener.router, prefix="/api/screener", tags=["Screener"])


@app.get("/api/health")
def health_check():
    return {"status": "ok", "service": "marketpulse-api"}
