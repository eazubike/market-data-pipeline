# Market Data Pipeline

Serverless data lake pipeline that collects global stock prices, financial news, fundamentals, analyst data, corporate actions, insider transactions, and financial statements. Data lands in Amazon S3 as partitioned Parquet files, queryable instantly via Athena.

---

## Architecture

```
EventBridge Scheduler
  │
  ├─▶ market-data-pipeline-v2 (every 30 min)
  │     CheckMarketHours → Map(CollectStocks) → CompactStocks → EmitMetrics
  │
  ├─▶ market-data-news-pipeline-v2 (30 min weekdays / 4h weekends)
  │     CollectNews (RSS, Reddit, GDELT, Yahoo Finance, Marketaux)
  │
  ├─▶ market-data-corporate-actions-v2 (07:00 UTC Mon–Fri)
  │     Map(ECS Fargate per exchange) → splits + dividends
  │
  ├─▶ market-data-analyst-v2 (07:30 UTC Mon–Fri)
  │     Map(ECS Fargate per exchange) → price targets, recommendations, earnings
  │
  ├─▶ market-data-fundamentals-v2 (22:00 UTC Mon–Fri)
  │     Map(ECS Fargate per exchange) → PE, EPS, revenue, etc.
  │
  ├─▶ market-data-insider-fargate-v2 (22:30 UTC Mon–Fri)
  │     Map(ECS Fargate per exchange) → SEC EDGAR Form 4 + yfinance
  │
  ├─▶ market-data-financials-v2 (06:00 UTC Sunday)
  │     Map(ECS Fargate per exchange) → income, balance sheet, cash flow
  │
  └─▶ market-data-glue-sync-v2 (01:00, 07:00, 13:00, 19:00 UTC daily)
        UpdateGlueCatalog Lambda → register new S3 partitions with Glue
```

Additional scheduled jobs:
- **Refresh tickers** — Lambda at 12:00 UTC Mon–Fri (NASDAQ API, LSE XLS, JPX Excel, HKEX XLSX, companiesmarketcap.com)

---

## Exchanges Supported

| Exchange | Timezone | Currency | Ticker Source |
|----------|----------|----------|---------------|
| NASDAQ | America/New_York | USD | NASDAQ.com API (~4,136) |
| NYSE | America/New_York | USD | NASDAQ.com API (~2,712) |
| LSE | Europe/London | GBP | LSE SETS XLS (~999) |
| XETRA | Europe/Berlin | EUR | companiesmarketcap.com |
| Euronext Paris | Europe/Paris | EUR | companiesmarketcap.com |
| Euronext Amsterdam | Europe/Amsterdam | EUR | companiesmarketcap.com |
| Euronext Brussels | Europe/Brussels | EUR | companiesmarketcap.com |
| Euronext Lisbon | Europe/Lisbon | EUR | companiesmarketcap.com |
| Borsa Italiana | Europe/Rome | EUR | companiesmarketcap.com |
| TSE (Tokyo) | Asia/Tokyo | JPY | JPX official Excel (~3,716) |
| HKEX | Asia/Hong_Kong | HKD | HKEX official XLSX (~2,817) |

---

## S3 Data Layout

```
market-data-{account}-{region}/
├── stocks/date=YYYY-MM-DD/run_timestamp=.../exchange=NASDAQ/batch_0001.parquet
├── news/date=YYYY-MM-DD/run_timestamp=.../source=reuters/data.parquet
├── fundamentals/date=YYYY-MM-DD/exchange=NASDAQ/data.parquet
├── analyst/type=price_targets/date=YYYY-MM-DD/exchange=NASDAQ/data.parquet
├── analyst/type=recommendations/date=YYYY-MM-DD/exchange=NASDAQ/data.parquet
├── analyst/type=earnings_dates/date=YYYY-MM-DD/exchange=NASDAQ/data.parquet
├── corporate_actions/type=split/date=YYYY-MM-DD/exchange=NASDAQ/data.parquet
├── corporate_actions/type=dividend/date=YYYY-MM-DD/exchange=NASDAQ/data.parquet
├── insider_transactions/date=YYYY-MM-DD/exchange=NASDAQ/data.parquet
├── financials/date=YYYY-MM-DD/exchange=NASDAQ/data.parquet
└── config/
    ├── exchanges.json
    └── tickers/{EXCHANGE}.csv
```

---

## AWS Services Used

| Service | Purpose |
|---------|---------|
| S3 | Data lake (Parquet storage) |
| Lambda | Stock prices, news, market hours, metrics, ticker refresh, Glue sync |
| ECS Fargate | Long-running tasks: fundamentals, financials, analyst, corporate actions, insider |
| Step Functions | Orchestration with retries, error handling, parallel fan-out |
| EventBridge Scheduler | Cron schedules for all pipelines |
| Glue Data Catalog | Table metadata + partition registry for Athena |
| Athena | SQL queries over Parquet |
| DynamoDB | News article deduplication (TTL-based) |
| CloudWatch | Metrics, alarms, logs |
| SNS | Alert notifications |
| Secrets Manager | API keys (Marketaux) |

---

## Prerequisites

- AWS CLI configured (`aws configure`)
- Node.js 18+ (for CDK CLI)
- Python 3.12+
- CDK CLI: `npm install -g aws-cdk`

---

## Deployment

### 1. Create API key secrets

```bash
aws secretsmanager create-secret \
  --name market-data/marketaux-api-key \
  --secret-string '{"api_key": "YOUR_KEY_HERE"}'
```

### 2. Install CDK dependencies

```bash
cd infrastructure
pip install -r requirements.txt
```

### 3. Deploy all stacks

```bash
cd infrastructure
cdk bootstrap          # first time only
cdk deploy --all
```

Individual stacks:
```bash
cdk deploy MarketDataStorage
cdk deploy MarketDataCompute
cdk deploy MarketDataOrchestration
```

### 4. Update Lambda code only (no infra changes)

```bash
cd lambdas/update_glue_catalog
zip -r handler.zip .
aws lambda update-function-code \
  --function-name market-data-update-glue-catalog \
  --zip-file fileb://handler.zip
rm handler.zip
```

---

## Querying Data with Athena

Database: `market_data`

```sql
-- Latest prices for all NASDAQ stocks today
SELECT symbol, price, volume, timestamp
FROM stocks
WHERE exchange = 'NASDAQ' AND date = current_date
ORDER BY timestamp DESC;

-- Top 10 stocks by volume this week
SELECT symbol, exchange, SUM(volume) AS total_volume
FROM stocks
WHERE date >= date_add('day', -7, current_date)
GROUP BY symbol, exchange
ORDER BY total_volume DESC
LIMIT 10;

-- News mentioning a specific ticker
SELECT headline, source, published_at, sentiment_score
FROM news
WHERE contains(symbols_mentioned, 'AAPL')
  AND date >= date_add('day', -3, current_date)
ORDER BY published_at DESC;

-- Analyst consensus across exchanges
SELECT symbol, exchange, consensus, number_of_analysts, upside_pct
FROM analyst_price_targets
WHERE date = current_date AND number_of_analysts > 5
ORDER BY upside_pct DESC;

-- Recent insider buys > $100K
SELECT symbol, insider_name, insider_title, transaction_type, shares, value_usd
FROM insider_transactions
WHERE date >= date_add('day', -7, current_date)
  AND transaction_type = 'P'
  AND value_usd > 100000
ORDER BY value_usd DESC;
```

---

## Project Structure

```
market-data-pipeline/
├── infrastructure/              AWS CDK app (Python)
│   ├── app.py                   Entry point — 3 stacks
│   ├── cdk.json
│   ├── requirements.txt
│   └── stacks/
│       ├── storage_stack.py     S3, Glue catalog, DynamoDB
│       ├── compute_stack.py     Lambdas, ECS tasks, Layer
│       └── orchestration_stack.py  Step Functions + schedules
│
├── lambdas/
│   ├── shared/                  Shared utilities (in Lambda Layer)
│   │   ├── market_hours.py
│   │   ├── config_loader.py
│   │   └── parquet_writer.py
│   ├── check_market_hours/
│   ├── collect_stocks/
│   ├── compact_stocks/
│   ├── collect_news/
│   ├── update_glue_catalog/
│   ├── emit_metrics/
│   ├── refresh_tickers/
│   ├── collect_corporate_actions/
│   ├── collect_insider_transactions/
│   ├── collect_analyst_data/
│   └── collect_financials/
│
├── ecs/                         ECS Fargate containers
│   ├── fundamentals/
│   ├── financials/
│   ├── corporate_actions/
│   ├── analyst_data/
│   └── insider_transactions/
│
├── config/
│   ├── exchanges.json           Exchange hours, timezones, holidays
│   └── tickers/                 Per-exchange ticker CSVs (auto-populated)
│
├── layer/
│   └── requirements.txt         Lambda Layer: yfinance, feedparser, etc.
│
└── scripts/
    └── fetch_tickers.py         Manual ticker refresh script
```

---

## Data Sources

| Source | Data | Limit |
|--------|------|-------|
| Yahoo Finance (yfinance) | OHLCV, fundamentals, analyst, financials, insider | Unlimited (rate-limited) |
| Marketaux | Financial news + sentiment | 100 req/day free |
| RSS (Reuters, MarketWatch, CNBC, FT, SEC, etc.) | News headlines | Unlimited |
| GDELT 2.0 Doc API | Global news | Unlimited |
| Reddit RSS (r/stocks, r/wallstreetbets, etc.) | Social sentiment | Unlimited |
| SEC EDGAR | Form 4 insider filings (US) | Unlimited (10 req/s) |

---

## Schedule Summary

| Time (UTC) | Pipeline | Compute | Frequency |
|------------|----------|---------|-----------|
| 12:00 Mon–Fri | Refresh tickers | Lambda | Daily |
| Every 30 min | Stock prices | Lambda (Step Function) | 48x/day |
| Every 30 min weekdays / 4h weekends | News | Lambda (Step Function) | 48+6/day |
| 01:00, 07:00, 13:00, 19:00 | Glue partition sync | Lambda (Step Function) | 4x/day |
| 07:00 Mon–Fri | Corporate actions | ECS Fargate (Step Function) | Daily |
| 07:30 Mon–Fri | Analyst data | ECS Fargate (Step Function) | Daily |
| 22:00 Mon–Fri | Fundamentals | ECS Fargate (Step Function) | Daily |
| 22:30 Mon–Fri | Insider transactions | ECS Fargate (Step Function) | Daily |
| 06:00 Sunday | Financials | ECS Fargate (Step Function) | Weekly |

---

## Estimated Monthly Cost

~$3–10 for personal use on AWS Free Tier. Main costs:
- Lambda invocations (well within free tier)
- ECS Fargate (~$2–5/month for short daily tasks)
- S3 storage (grows ~1–2 GB/month, pennies)
- Glue catalog requests (~4,400/month, well within 1M free tier)

---

## Glue Partition Sync

Partitions are synced by a standalone `market-data-glue-sync-v2` state machine that runs 4x/day. It:
1. Fetches existing partitions from Glue (filtered to last 30 days)
2. Walks S3 prefixes for the same 30-day window
3. Diffs locally and batch-creates only missing partitions

This approach uses ~4,400 Glue API requests/month (0.4% of free tier) regardless of data volume growth.
