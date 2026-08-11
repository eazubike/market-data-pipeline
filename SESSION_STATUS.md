# Market Data Pipeline — Session Status & Migration Guide

**Date:** August 8, 2026
**Reason for pause:** AWS Free Tier credits exhausted on account 082121306678
**GitHub repo:** https://github.com/eazubike/market-data-pipeline (public)

---

## What Was Accomplished (This Session)

### 1. Fixed Glue Catalog EntityNotFoundException
- The `market-data-analyst-v2` Step Function was crashing because `update_glue_catalog` Lambda tried to register partitions for ALL tables (including `news` which didn't exist)
- Rewrote the Lambda to accept scoped table lists per pipeline

### 2. Standalone Glue Partition Sync (4x/day)
- **Removed** UpdateGlueCatalog steps from ALL 5 pipelines (stocks, news, analyst, corporate actions, financials)
- **Created** new `market-data-glue-sync-v2` Step Function that runs independently at 01:00, 07:00, 13:00, 19:00 UTC
- Uses `GetPartitions` (paginated bulk fetch) + local set diff — no more per-partition API calls
- Filters to last 30 days only (both S3 walk and Glue query)
- **Result:** ~4,400 Glue requests/month (was 4.5M before). Permanently capped.

### 3. Fixed Glue Free Tier Usage (was at 929K/1M)
- Root cause: `GetPartition` called once per leaf prefix, 48x/day
- Fix: Single `GetPartitions` call with `Expression="date >= 'YYYY-MM-DD'"` filter

### 4. Reduced Step Functions State Transitions
- Changed stocks schedule: every 30 min → **every 1 hour**
- Changed news weekday schedule: every 30 min → **every 1 hour**
- News weekend stays at every 4 hours
- **Result:** ~385 transitions/day instead of ~770

### 5. Market Hours Lookback (90 minutes)
- Before: only collected if market is open RIGHT NOW
- After: collects if market is open NOW or closed within last 90 min
- **Why:** With hourly frequency, ensures we always catch the close price

### 6. README + Git
- Comprehensive README.md created
- Pushed to GitHub: https://github.com/eazubike/market-data-pipeline
- `.gitignore` properly excludes `layer/python/`, `local_output/`, `tmp_wheels/`, `cdk.out/`

---

## Current Architecture (Final State)

```
EventBridge Scheduler
  ├── market-data-pipeline-v2         (hourly)     → stocks
  ├── market-data-news-pipeline-v2    (hourly weekday / 4h weekend) → news
  ├── market-data-corporate-actions-v2 (07:00 UTC Mon-Fri) → ECS
  ├── market-data-analyst-v2          (07:30 UTC Mon-Fri) → ECS
  ├── market-data-fundamentals-v2     (22:00 UTC Mon-Fri) → ECS
  ├── market-data-insider-fargate-v2  (22:30 UTC Mon-Fri) → ECS
  ├── market-data-financials-v2       (06:00 UTC Sunday)  → ECS
  └── market-data-glue-sync-v2        (01:00,07:00,13:00,19:00 UTC) → Lambda
```

---

## What Needs to Be Done on New Account

### Step 1: Prerequisites
```bash
npm install -g aws-cdk
aws configure   # with new account credentials
```

### Step 2: Create Secrets
```bash
aws secretsmanager create-secret \
  --name market-data/marketaux-api-key \
  --secret-string '{"api_key": "YOUR_MARKETAUX_KEY"}'
```

### Step 3: Install Lambda Layer
```bash
cd layer
pip install -r requirements.txt -t python/lib/python3.12/site-packages/
```

### Step 4: Deploy All Stacks
```bash
cd infrastructure
pip install -r requirements.txt
cdk bootstrap
cdk deploy --all
```

This deploys 3 stacks in order:
1. `MarketDataStorage` — S3 bucket, DynamoDB dedup table, Glue catalog
2. `MarketDataCompute` — All Lambdas, ECS tasks, Lambda Layer
3. `MarketDataOrchestration` — Step Functions, EventBridge schedules, alarms

### Step 5: Verify
```bash
# Check state machines exist
aws stepfunctions list-state-machines --query "stateMachines[?contains(name,'market-data')].[name]" --output table

# Manually trigger glue sync to test
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:NEW_ACCOUNT:stateMachine:market-data-glue-sync-v2 \
  --input '{"lookback_days": 30}'
```

---

## Still Pending (From Previous Sessions)

1. **Rename `date=` to `load_date=`** in S3 paths + add `balance_date` as physical column in Parquet
2. **Corporate actions ECS** — None check fix applied, needs verification
3. **Stock pipeline timeout** — verify during peak hours (all exchanges open)
4. **Financials/Analyst state machines** — verify they complete within 2-hour timeout
5. **News Parquet** — `use_compliant_nested_type=True` deployed, verify future files are correct

---

## Free Tier Limits to Watch

| Service | Free Tier | Current Usage Pattern | Monthly Estimate |
|---------|-----------|----------------------|------------------|
| Glue Catalog | 1,000,000 req | GetPartitions + BatchCreate | ~4,400 |
| Step Functions | 4,000 transitions | hourly stocks/news + daily ECS | ~11,500 (overage: ~$0.19/month) |
| Lambda | 1M invocations | all pipelines combined | ~5,000 |
| S3 | 5GB free tier | grows ~1-2 GB/month | within free tier initially |
| ECS Fargate | none free | ~5 min/day × 5 tasks | ~$2-5/month |

---

## Files Modified This Session

| File | Change |
|------|--------|
| `lambdas/update_glue_catalog/handler.py` | Complete rewrite: GetPartitions diff, 30-day filter, all 9 tables |
| `lambdas/check_market_hours/market_hours.py` | 90-min lookback window |
| `lambdas/shared/market_hours.py` | Same (shared copy) |
| `infrastructure/stacks/orchestration_stack.py` | Removed inline Glue steps, added glue-sync state machine, hourly schedules |
| `infrastructure/stacks/compute_stack.py` | Added `glue:GetPartitions` to IAM policy |
| `README.md` | Full rewrite with current architecture |
| `.gitignore` | Excludes layer/, local_output/, tmp_wheels/, test files |

---

## Git Status

- Repo: https://github.com/eazubike/market-data-pipeline
- Branch: `main`
- Latest commit: `feat: hourly frequency, 90min lookback for close prices, standalone glue sync 4x/day`
- All code is committed and pushed
