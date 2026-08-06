# Market Data Pipeline — Roadmap

## Completed

- [x] Stock price collection (Lambda, every 30 min during market hours)
- [x] News collection with sentiment scoring (Lambda, every 4 hours)
- [x] Fundamentals collection separated to ECS Fargate (once daily 22:00 UTC)
- [x] Daily ticker list refresh from live sources (Lambda, 12:00 UTC Mon-Fri)
- [x] Per-batch S3 writes with compaction (no data loss on timeout)
- [x] All 11 exchanges: NASDAQ, NYSE, LSE, XETRA, Euronext (Paris/AMS/BRU/LIS), Borsa Italiana, TSE, HKEX
- [x] Corporate actions (splits, dividends)
- [x] Analyst data (price targets, recommendations, earnings dates)
- [x] Insider transactions
- [x] Financial statements (income, balance sheet, cash flow)
- [x] Glue Data Catalog for Athena queries
- [x] EventBridge scheduling for all jobs
- [x] Step Function orchestration with parallel exchange processing
- [x] Local test runner (run_local.py) with CSV output

## Next Up — Corporate News per Ticker (Watchlist)

- [ ] Configurable watchlist of tickers (stocks you own + any extras)
  - Stored in S3 config or DynamoDB — editable without redeploying
  - Default: your personal holdings + most active stocks
- [ ] Per-ticker corporate news collection via `yf.Ticker(symbol).news`
  - Headlines, publisher names, article URLs, publish dates
  - Sentiment scoring per article (VADER)
  - Symbols cross-referenced from article text
- [ ] Runs alongside existing news pipeline (every 4 hours or configurable)
- [ ] Output: `s3://bucket/news/source=corporate_ticker/date={}/data.parquet`
- [ ] Deduplication against existing news articles (by article_id hash)
- [ ] Most Active Stocks feed (Yahoo Finance screener: highest volume today)
  - Auto-add top movers to the per-ticker news fetch for that day

## Phase 2 — Technical Indicators (requires 2+ weeks of data)

- [ ] Technical indicators compute job (daily after market close)
  - RSI (14-day Relative Strength Index)
  - MACD (12/26/9 Moving Average Convergence Divergence)
  - SMA (20-day, 50-day, 200-day Simple Moving Averages)
  - EMA (12-day, 26-day Exponential Moving Averages)
  - Bollinger Bands (20-day, 2 std dev)
  - Stochastic Oscillator (14-day %K, %D)
  - ADX (Average Directional Index — trend strength)
  - ATR (Average True Range — volatility)
  - OBV (On-Balance Volume — volume confirmation)
  - VWAP (Volume Weighted Average Price)
- [ ] Support/Resistance level detection
  - Pivot points (daily/weekly)
  - Historical support/resistance from local min/max
  - Fibonacci retracement levels
- [ ] Momentum scoring (composite score per stock: bullish/neutral/bearish)
- [ ] Output: `s3://bucket/technicals/exchange={}/date={}/data.parquet`
- [ ] Glue table registration for Athena queries

## Phase 3 — Signals & Alerts

- [ ] Signal generation engine
  - Golden cross / death cross (50-day SMA crosses 200-day)
  - RSI overbought (>70) / oversold (<30) alerts
  - MACD crossover signals
  - Unusual volume spikes (>2x 30-day average)
  - Price breaking support/resistance
  - Bollinger Band squeeze (low volatility → breakout imminent)
- [ ] Alert delivery (SNS → email/SMS)
- [ ] Signal history stored in S3 for backtesting

## Phase 4 — Sentiment & News Correlation

- [ ] Correlate news sentiment with price movements
  - Match articles to tickers via symbols_mentioned field
  - Track sentiment score changes vs price changes over 1h/4h/1d windows
- [ ] Sector-level sentiment aggregation
- [ ] Earnings surprise impact analysis (actual EPS vs estimate → price move)
- [ ] Investment/M&A news impact tracking

## Phase 5 — Portfolio Analytics & Backtesting

- [ ] Hypothetical portfolio tracker
  - Input: list of holdings with entry price/date
  - Output: daily P&L, Sharpe ratio, max drawdown
- [ ] Backtesting framework
  - Test signal strategies against historical data
  - Win rate, risk/reward ratio, profit factor
- [ ] Sector rotation analysis
- [ ] Correlation matrix between stocks

## Phase 6 — ML & Prediction (long-term)

- [ ] Feature engineering from all collected data
  - Technical indicators + fundamentals + sentiment + insider activity
- [ ] Price direction classification (up/down/flat next day)
- [ ] Anomaly detection (unusual patterns in volume, price, insider activity)
- [ ] Model training pipeline (SageMaker or local)
- [ ] Prediction output stored alongside other data

## Infrastructure Improvements

- [ ] Data quality checks (Lambda that validates Parquet files daily)
- [ ] Cost monitoring dashboard (CloudWatch)
- [ ] Data retention policy (archive old data to Glacier after 1 year)
- [ ] Athena workgroup + saved queries for common analyses
- [ ] Grafana or QuickSight dashboard for live monitoring
- [ ] CI/CD pipeline for Lambda/ECS deployments
- [ ] Automated tests for ticker refresh + data collection

## Phase 7 — Alternative Asset Classes (yfinance)

- [ ] Cryptocurrencies — historical OHLCV for BTC-USD, ETH-USD, SOL-USD, etc.
  - Same 30-min collection schedule as stocks
  - Output: `s3://bucket/crypto/symbol={}/date={}/data.parquet`
- [ ] Forex — currency exchange rate history (EURUSD=X, GBPUSD=X, JPYUSD=X)
  - Daily snapshots
  - Output: `s3://bucket/forex/pair={}/date={}/data.parquet`
- [ ] Commodities Futures — Gold (GC=F), Oil (CL=F), Silver (SI=F), Corn (ZC=F)
  - Daily OHLCV
  - Output: `s3://bucket/commodities/symbol={}/date={}/data.parquet`
- [ ] Market Indices — S&P 500 (^GSPC), NASDAQ (^IXIC), VIX (^VIX), FTSE (^FTSE), DAX (^GDAXI)
  - Track alongside stock prices for correlation analysis
  - Output: `s3://bucket/indices/symbol={}/date={}/data.parquet`
- [ ] Bonds & Treasury Yields — 10-Year (^TNX), 2-Year (^IRX), 30-Year (^TYX)
  - Daily rates — critical for understanding rate environment vs equity valuations
  - Output: `s3://bucket/bonds/symbol={}/date={}/data.parquet`

## Phase 8 — Options & Derivatives Data

- [ ] Options chains per ticker (top 50–100 most liquid stocks)
  - All expiration dates (`ticker.options`)
  - Full call/put chains: strike, bid, ask, implied volatility, open interest, volume
  - Output: `s3://bucket/options/symbol={}/expiry={}/date={}/data.parquet`
- [ ] Implied volatility surface tracking (IV by strike + expiry over time)
- [ ] Put/Call ratio as market sentiment indicator
- [ ] Unusual options activity detection (large volume vs open interest)

## Phase 9 — ETFs, Mutual Funds & Institutional Ownership

- [ ] ETF/Mutual Fund data collection
  - Top holdings & allocation percentages (`ticker.fund_holding_info`)
  - Expense ratios, fund performance, AUM
  - Sector/geography allocation breakdown
  - Output: `s3://bucket/funds/symbol={}/date={}/data.parquet`
- [ ] Institutional holders per stock (`ticker.institutional_holders`)
  - Top funds, % ownership, shares held, value
  - Track changes over time (accumulation vs distribution)
  - Output: `s3://bucket/institutional_holders/exchange={}/date={}/data.parquet`
- [ ] Mutual fund holders (`ticker.mutualfund_holders`)
- [ ] Capital gains distributions for funds (`ticker.capital_gains`)

## Phase 10 — ESG, Company Profiles & Earnings Calendar

- [ ] ESG/Sustainability scores (`ticker.sustainability`)
  - Environmental, Social, Governance ratings
  - Controversy scores, peer comparison
  - Output: `s3://bucket/esg/exchange={}/date={}/data.parquet`
- [ ] Company profiles (`ticker.info`)
  - Business summary, sector, industry, full-time employees
  - Key executives (name, title, compensation)
  - Headquarters location, website, phone
  - Output: `s3://bucket/company_profiles/exchange={}/date={}/data.parquet`
- [ ] Earnings calendar & estimates (`ticker.calendar`, `ticker.earnings_dates`)
  - Upcoming earnings dates for all tracked stocks
  - EPS estimates vs actual (for surprise analysis)
  - Revenue estimates
  - Output: `s3://bucket/earnings_calendar/exchange={}/date={}/data.parquet`

