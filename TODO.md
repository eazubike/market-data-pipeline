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
