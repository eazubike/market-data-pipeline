# Market Data Pipeline — TODO (Pick up from here)

## Architecture Pattern (Industry Standard)
- EventBridge = cron (triggers on schedule)
- Step Functions = orchestrator (retries, monitoring, visibility)
- Lambda = quick jobs (<15 min): stock prices, news, ticker refresh
- ECS Fargate = long-running jobs (>15 min): fundamentals, insider, financials, analyst, corporate actions

## Immediate Fixes Needed

### 1. Move timing-out Lambdas to ECS Fargate
These Lambda functions timeout at 15 minutes because they make individual API calls per ticker (4,136+ tickers):
- [ ] **Financials** (income statement, balance sheet, cash flow) → ECS Fargate
- [ ] **Corporate Actions** (splits, dividends) → ECS Fargate
- [ ] **Analyst Data** (price targets, recommendations, earnings dates) → ECS Fargate

Pattern: same as `ecs/fundamentals/` and `ecs/insider_transactions/` — create container, Dockerfile, requirements.txt per job.

### 2. Wrap ALL Fargate tasks in Step Functions
Currently Fargate tasks are triggered directly by EventBridge (fire-and-forget). Wrap them in Step Functions for consistent monitoring:
- [ ] Create Step Function for **Fundamentals** (wraps ECS RunTask, waits for completion)
- [ ] Create Step Function for **Insider Transactions** (wraps ECS RunTask × 5 parallel)
- [ ] Create Step Function for **Financials** (new Fargate task, wrapped in SF)
- [ ] Create Step Function for **Corporate Actions** (new Fargate task, wrapped in SF)
- [ ] Create Step Function for **Analyst Data** (new Fargate task, wrapped in SF)

Each Step Function:
- Uses `ecs:RunTask` integration (Step Functions natively supports this)
- Waits for task completion (sync)
- Has 1-hour timeout
- EventBridge triggers the Step Function (not Fargate directly)

### 3. Deploy the Glue catalog fix
- [ ] Deploy the partition validation fix (skip partitions where value count != key count)
- Already coded in `lambdas/update_glue_catalog/handler.py`
- Deploy with: `cd infrastructure && cdk deploy MarketDataCompute --require-approval never`

### 4. Deploy the orchestration stack
- [ ] Deploy with: `cd infrastructure && cdk deploy MarketDataOrchestration --require-approval never`
- State machines are now `-v2` names (Standard type, not Express)
- Keep current timeouts on state machines

## Current State (What's Working)

| Component | Status | Runs on |
|---|---|---|
| Stock prices (OHLCV) | Working | Lambda (Step Function) |
| News (15+ sources) | Working | Lambda (Step Function) |
| Ticker refresh | Working | Lambda (EventBridge) |
| Fundamentals | Deployed but not yet triggered | ECS Fargate (EventBridge direct) |
| Insider transactions | Deployed but not yet triggered | ECS Fargate (EventBridge direct) |
| Financials | TIMING OUT | Lambda (needs → Fargate) |
| Corporate actions | TIMING OUT | Lambda (needs → Fargate) |
| Analyst data | TIMING OUT | Lambda (needs → Fargate) |

## State Machine Names (current)
- `market-data-pipeline-v2` — stock prices
- `market-data-news-pipeline-v2` — news
- `market-data-corporate-actions-v2` — corporate actions (timing out)
- `market-data-analyst-v2` — analyst data (timing out)
- `market-data-insider-transactions-v2` — insider (timing out, needs Fargate wrapper)
- `market-data-financials-v2` — financials (timing out)

## State Machine Inputs (for manual testing from console)
```json
// Stock prices + News:
{"source":"manual"}

// All others (corporate actions, analyst, insider, financials):
{"source":"manual","exchanges":["NASDAQ","NYSE","LSE","XETRA","EURONEXT","EURONEXT_AMS","EURONEXT_BRU","EURONEXT_LIS","BORSA_ITALIANA","TSE","HKEX"]}
```

## S3 Bucket
`s3://market-data-082121306678-us-east-1/`

## ECS Cluster
`market-data-fundamentals` (shared by all Fargate tasks)

## Schedules (EventBridge → Step Functions)
| Time (UTC) | Job |
|---|---|
| 12:00 Mon-Fri | Refresh ticker lists (Lambda) |
| 13:30 Mon-Fri | Stock prices (Lambda, parallel per exchange) |
| Every 30 min weekdays | News (Lambda) |
| Every 4 hours weekends | News (Lambda) |
| 22:00 Mon-Fri | Fundamentals (Fargate → wrap in SF) |
| 22:30 Mon-Fri | Insider transactions (Fargate → wrap in SF) |
| 07:00 Mon-Fri | Corporate actions (move to Fargate + SF) |
| 07:30 Mon-Fri | Analyst data (move to Fargate + SF) |
| Sunday 06:00 | Financials (move to Fargate + SF) |

## Lambda Concurrency
- Account limit: requested increase to 1000 (check if approved)
- Check with: `aws lambda get-account-settings --region us-east-1 --query "AccountLimit.ConcurrentExecutions"`

## Files Modified Today
- `infrastructure/stacks/orchestration_stack.py` — state machines now STANDARD type (-v2 names)
- `infrastructure/stacks/compute_stack.py` — single custom layer (no AWS SDK Pandas layer)
- `lambdas/update_glue_catalog/handler.py` — get_table fix + partition validation
- `lambdas/collect_stocks/handler.py` — per-batch write, compact, 30m interval, currency
- `lambdas/collect_news/handler.py` — added Reddit, Yahoo Finance news, extra RSS
- `lambdas/collect_financials/handler.py` — date[:10] fix
- `lambdas/collect_corporate_actions/handler.py` — date[:10] fix
- `lambdas/collect_analyst_data/handler.py` — date[:10] fix
- `lambdas/collect_insider_transactions/handler.py` — date[:10] fix + chunking
- `lambdas/refresh_tickers/handler.py` — new Lambda for daily ticker refresh
- `ecs/fundamentals/` — Fargate container for fundamentals
- `ecs/insider_transactions/` — Fargate container with SEC EDGAR + yfinance
- `run_local.py` — all local testing changes


## Bloomberg-Level Features — Build Plan

### Priority 1: New Data Sources (free, minimal effort)

- [ ] **Yield curve & bonds** — add ^TNX, ^IRX, ^TYX, ^FVX to stock collection (just more yfinance tickers)
- [ ] **Market indices** — add ^GSPC, ^VIX, ^IXIC, ^FTSE, ^GDAXI, ^N225 to stock collection
- [ ] **Commodities** — add GC=F, CL=F, SI=F, ZC=F to stock collection
- [ ] **Crypto** — add BTC-USD, ETH-USD, SOL-USD, ADA-USD to stock collection
- [ ] **Forex** — add EURUSD=X, GBPUSD=X, JPYUSD=X, AUDUSD=X to stock collection
- [ ] **Economic calendar (FRED API)** — new Lambda, pulls CPI, GDP, unemployment, fed funds rate (free API key)

### Priority 2: Compute on Existing Data (no new sources needed)

- [ ] **Technical indicators** — daily compute job on accumulated price data
  - RSI (14-day), MACD (12/26/9), SMA (20/50/200), EMA, Bollinger Bands
  - Stochastic Oscillator, ADX, ATR, OBV, VWAP
  - Support/resistance levels, Fibonacci retracement
- [ ] **Factor scoring** — per-stock score based on:
  - Momentum (price change 1m/3m/6m/12m)
  - Value (PE, PB, dividend yield vs sector median)
  - Quality (ROE, debt/equity, earnings stability)
  - Composite score: bullish / neutral / bearish
- [ ] **Earnings quality scores**
  - Altman Z-Score (bankruptcy risk from balance sheet)
  - Beneish M-Score (earnings manipulation probability)
  - Piotroski F-Score (financial strength)
- [ ] **Correlation matrices** — stock vs stock, stock vs index (needs 30+ days of data)
- [ ] **Beta calculation** — per stock vs S&P 500
- [ ] **Sharpe ratio / Sortino ratio** — per stock risk-adjusted return

### Priority 3: Alerts & Signals (built on existing data)

- [ ] **Insider cluster alerts** — 3+ insiders buying same stock within 30 days
- [ ] **Short squeeze candidates** — high short interest + rising price + volume spike
- [ ] **Volume surge detection** — current volume > 2x avg_volume_30d
- [ ] **Golden cross / death cross** — 50-day SMA crosses 200-day SMA
- [ ] **RSI extremes** — overbought (>70) / oversold (<30)
- [ ] **MACD crossover signals**
- [ ] **Earnings surprise alerts** — actual EPS vs estimate > 10% deviation
- [ ] **Price target breakthroughs** — stock exceeds analyst high target
- [ ] **Morning briefing email** — Lambda + SNS, summarizes overnight movers, pre-market gaps, earnings today

### Priority 4: Additional yfinance Data

- [ ] **Options chains** — per-ticker collection (strike, IV, open interest, bid/ask)
  - Put/call ratio as market sentiment indicator
  - Unusual options activity detection
- [ ] **ETF/Mutual Fund holdings** — top holdings, allocations, expense ratios
- [ ] **Institutional holders** — which funds own which stocks, track changes over time
- [ ] **ESG scores** — environmental, social, governance ratings
- [ ] **Company profiles** — sector, industry, employee count, executives

### Priority 5: Multi-factor Screener

- [ ] **Athena-based screener** — query across all data to find stocks matching multiple criteria
  - Example: PE < 15 AND revenue_growth > 20% AND insider_buying = true AND RSI < 30
- [ ] **Pre-built screens** — value stocks, growth stocks, dividend aristocrats, turnaround plays
- [ ] **Daily screener output** — S3 + email with top picks per strategy

### Priority 6: Portfolio & Risk

- [ ] **Portfolio tracker** — input holdings, calculate daily P&L, total return
- [ ] **Value at Risk (VaR)** — 95% confidence loss estimate
- [ ] **Monte Carlo simulation** — project portfolio outcomes over time
- [ ] **Efficient frontier** — optimal portfolio allocation given risk tolerance
- [ ] **Tax-loss harvesting signals** — identify positions with losses to realize

### Priority 7: Dashboard & Delivery

- [ ] **Interactive dashboard** — QuickSight or Grafana on top of Athena
  - Watchlist with latest prices, sentiment, insider activity
  - Sector heatmap
  - Portfolio P&L chart
- [ ] **API endpoint** — API Gateway + Lambda for external apps to query data
- [ ] **Mobile push notifications** — SNS for signal triggers
- [ ] **Custom watchlists** — configurable per user, stored in DynamoDB

### Priority 8: Alternative Data (harder, some paid)

- [ ] **Social sentiment velocity** — Reddit mention frequency per ticker (already have RSS)
- [ ] **Web traffic trends** — SimilarWeb/SEMrush data (paid)
- [ ] **Job postings analysis** — LinkedIn/Indeed scraping (complex)
- [ ] **Patent filings** — USPTO bulk data (free but complex parsing)
- [ ] **Supply chain mapping** — who supplies whom (paid databases)
- [ ] **Satellite/geolocation data** — parking lot counts, shipping (very expensive)
- [ ] **Credit ratings** — Moody's/S&P (paid, no free source)
