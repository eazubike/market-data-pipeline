# Progress log: Market Data Pipeline

Running log of what is built, how it runs, and what is next. Newest entries at the top of **Log**.
Anyone picking this up (human or AI agent): read **Current state** and **Rules** first, then add your own
dated entries to **Log** when you change something.

**Architecture page:** [architecture.html](architecture.html), published at https://claude.ai/artifact/97MBDvjQEaVuHfKq5RYtoJ
(kept current with every structural change; see `CLAUDE.md`). Older history before Oct 2026 is in git (the Aug 2026
`SESSION_STATUS.md` snapshot was deleted on 2026-10-26; recover with `git show 47643a3:SESSION_STATUS.md`).

## Current state (2026-10-26)

**Where it runs:** AWS account 082121306678, us-east-1. Everything is serverless or on-demand (Lambda, Fargate,
Step Functions, S3, Glue/Athena). The API and dashboard run on the owner's PC.

| Part | Where | Status |
|---|---|---|
| Daily price bars `stocks_daily` | Lambda `collect-stocks`, state machine `market-data-pipeline-v2`, every 2 h | Working. Each exchange collected once, >= 2 h after its local close. 1-year history from 2025-10-22. |
| News | Lambda `collect-news`, every 4 h | Working. 14 RSS feeds + SEC 8-K + MarketAux (~430 new articles per run). GDELT usually rate-limited. |
| Fundamentals | Fargate, Mon-Fri 22:00 UTC | Working, all 11 exchanges (~15,800 stocks) |
| Analyst data (targets, recs, earnings, profiles, holders) | Fargate, Mon-Fri 07:30 | Working, 100% of every exchange on 2026-10-23 rerun |
| Corporate actions (dividends, splits) | Fargate, Mon-Fri 07:00 | Working |
| Insider transactions | Fargate, Mon-Fri 22:30 | Working. US via SEC Form 4 (~300 symbols per 2 days); LSE/HKEX/TSE via Yahoo |
| Financials (statements) | Fargate, Sun 06:00 | Working. First complete run 2026-10-23 (~15M line items); Sun 2026-10-25 also succeeded |
| Signals + opportunities | Lambda `detect-signals`, Mon-Fri 23:00 | All 7 detectors firing since 2026-10-23 (16 opportunities that night, 9 BUY) |
| Ticker lists | Lambda `refresh-tickers`, Mon 12:00 | 11 exchanges, ~16,200 tickers |
| Athena | Glue db `market_data`, 16 tables | All tables have partitions; each stock run registers its own |
| API `api/` (FastAPI, port 8000) + dashboard `frontend/` (Next.js "MarketPulse", port 3000) | Owner's PC | Working; reads `stocks_daily` |

**How to run the API and dashboard** (two terminals):
```powershell
cd C:\Users\Echezona\Documents\ProgrammingLanguages\vibe-code\market-data-pipeline
python -m uvicorn api.main:app --port 8000
cd frontend; npm run dev        # then open http://localhost:3000
```

**Deploy:** from `infrastructure/`: `npx cdk deploy MarketDataCompute --exclusively --require-approval never`
(or MarketDataStorage / MarketDataOrchestration). Docker Desktop must be running for ECS image changes.

## Rules (agreed with the owner)
- Ticker lists should match what is tradable on **Trading 212**: keep common shares and warrants; drop SPAC units (U)
  and rights (R). Check a sample symbol in the Trading 212 app before adding or dropping a class of securities.
- Cost matters: serverless / pay-per-use only. Lambda stays inside the free tier in a clean month.
- Daily bars only (no intraday). Each exchange-day is marked collected only if every chunk succeeded.
- Never downgrade yfinance in the ECS images (pinned 1.6.0 / pandas 3.0.5 / pyarrow 25.0.1 / curl_cffi 0.16.0).
  Yahoo rate limits must be retried, never swallowed.
- Keep `docs/architecture.html` current and republish it with any structural change.
- Add a dated entry to this file's **Log** for every piece of work.

## Next
1. Backtest the 7 detectors on the 1-year daily history (forward returns at 5/20/60 days vs the market); drop what doesn't work.
2. Tighten `pre_earnings` (fires ~350-700 times a night and dominates opportunities) or require 3 stacked signals.
3. Sentiment: replace VADER with a finance word list (Loughran-McDonald) or FinBERT (needs a container Lambda).
4. Set `SEC_USER_AGENT` on the news Lambda (and insider task) to a real contact email; SEC asks for one.
5. Exchange holiday lists in `config/exchanges.json` only cover 2026; extend before 2027.
6. Optional: add primary-source news for non-US markets (LSE RNS, HKEXnews, TDnet, Euronext); FRED + VIX market-environment view.
7. Optional: subscribe an email to SNS topic `market-data-alerts` and add CloudWatch alarms on `ChunksFailed` and per-exchange
   coverage drops.
8. Optional: share one SEC fetch between the NASDAQ and NYSE insider tasks (each currently downloads the same feed, ~30 min).

## Gotchas
- Windows + Git Bash: prefix AWS CLI calls with `MSYS_NO_PATHCONV=1` (log group names like `/aws/lambda/...` get mangled).
- AWS CLI is v1 (no `--cli-binary-format`). In PowerShell, JSON input needs escaped quotes: `--input '{\"key\":\"value\"}'`.
- The scratch Python venv's long path breaks curl_cffi's DLL; map it to a short drive (`subst W: <path>`) before running.
- Don't `pip install` test packages into the global Python (it broke pyarrow/numpy once; reverted).
- ECS container logs are block-buffered: log timestamps are flush times, not when the line happened. Use task start/stop times.
- Cross-stack exports: deploy the importing stack first when removing something it imports (hit twice: task definitions, compact Lambda).
- The connection here is slow (~250 KB/s); ECS image builds take 20-40 min. Run deploys in the background.
- Athena CLI queries need `ResultConfiguration` `s3://market-data-082121306678-us-east-1/athena-results/` (primary workgroup has no output location).

## Log

### 2026-10-26: Claude Code (removed SESSION_STATUS.md)
- Deleted the Aug 2026 `SESSION_STATUS.md` at the owner's request: a frozen handover snapshot, outdated in places
  (intraday collection, old ticker lists). This file is now the only status doc. Still in git history (commit 47643a3).

### 2026-10-26: Claude Code (progress log started)
- Created this file and summarised the work since 2026-10-19 below. Added the "log every change" rule to `CLAUDE.md`.
- Checked weekend runs: all succeeded (28 stock checks, 28 news, Fri insider, Fri signals, Sun financials).
- Fri 2026-10-23 signals: all 7 detectors fired for the first time (analyst_breakout 50); 16 opportunities (9 BUY, 6 WATCH, 1 NEUTRAL).

### 2026-10-23: Claude Code (Yahoo throttling: root cause + reruns) - deployed 2026-10-23
- Analyst run "succeeded" with only 632 / 3,863 NASDAQ (similar NYSE/TSE). Yahoo throttled after ~640 tickers and every
  collector swallowed the rate-limit error, so the loop recorded empty data with failed=0.
- Root cause: on 2026-10-21 I pinned the ECS images to yfinance 0.2.61 to match the Lambda layer. The Aug 15 images that
  had worked ran yfinance 1.6.0, so the pin was a 17-month downgrade. Fix: pinned all 5 ECS images to yfinance 1.6.0,
  pandas 3.0.5, pyarrow 25.0.1, curl_cffi 0.16.0 (latest on 2026-08-15). Tested every job on these versions first.
- Also made analyst, financials, corporate actions and insider catch `YFRateLimitError`, wait 30/60/120/240/300 s and
  retry the ticker (corporate actions checked for '429', which yfinance's message doesn't contain).
- Reran analyst, corporate actions, financials on the new images: 0 rate-limit waits, 0 task failures. Analyst 100% of
  every exchange; financials first complete run (TSE 3,712, LSE 1,313, ~15M line items in 2 h 24 min).
- Daily prices checked: no throttling (Lambda makes 1 request per stock per day). Gaps are stocks that didn't trade
  (YFPricesMissingError, e.g. ~600 HK small caps a day) or unknown/delisted tickers (YFTzMissingError, ~200).
- Rules recorded in `CLAUDE.md`. Commits 4dd8b69, 1b7f3d0.

### 2026-10-22: Claude Code (architecture page) - published
- Built the architecture page (flow diagram, 24 h UTC timeline to scale, pipelines table, all Athena tables, how a day's
  prices are collected, design decisions). Saved as `docs/architecture.html`, published to the link above.
- Added `CLAUDE.md`: update and republish the page with any structural change; deployment lessons. Commit 57b8ccc.

### 2026-10-22: Claude Code (daily bars replace intraday; 1-year backfill) - deployed 2026-10-22
- Owner pointed out the 9x/day intraday snapshots were neither live nor a real intraday series. Switched to one daily bar
  per stock per exchange-day; same pipeline, chunks, batches and workers, `interval="1d"`.
- Schedule every 2 h, every day. `check-market-hours` collects an exchange when (in its own timezone) it was a trading day,
  it is >= 2 h after close, and the day isn't in `state/stocks_daily.json`. Missed days are fetched by the next run
  (start = day after last collected); finished days are never refetched. Marked only when all chunks succeed
  (failed chunk, time-out or 0 tickers = retry).
- New table `stocks_daily` (`stocks_daily/date=/exchange=/part-<offset>.parquet`, raw OHLC + adj_close). Old `stocks`
  table kept as an archive, nothing reads it.
- Backfill: same pipeline with `{"backfill_start": "2025-10-22"}`, ending at each exchange's last finished weekday.
  3.8M rows, 15,839 symbols, ~250 days each, in 6 minutes; 2,714 partitions registered.
- Detectors and API repointed to `stocks_daily`. Fixed sentiment divergence (its (MAX-MIN)/MIN change was always positive,
  so "good news, price fell" never fired). Volume surge now compares the latest daily volume to the prior 30-day average.
- Removed the unused `compact-stocks` Lambda. Commits 0f3fa29, 6f86d44.
- Local API + dashboard started and checked (AAPL latest + 30-day history from `stocks_daily`).

### 2026-10-22: Claude Code (news sources) - deployed 2026-10-22
- 5 configured sources had silently returned nothing (feedparser hid HTTP errors): Reuters (feed gone), BusinessWire and
  Seeking Alpha news.xml (empty), Google markets topic (404), Reddit (403, needs OAuth). Removed / disabled.
- SEC 8-K fixed (needed a contact User-Agent): 100 filings per run, 90% mapped to tickers via SEC's company_tickers.json.
- Added Google News last-day searches (markets, earnings), CNBC Markets, Nasdaq Markets. Feeds now fetched with requests
  and empty feeds logged. GDELT gets one retry. One run: 428 new articles (was ~420 per day). Commit fb87318.

### 2026-10-22: Claude Code (analyst count, penny-stock filter) - deployed 2026-10-22
- `number_of_analysts` was 0 on every row (wrong field); now from `.info["numberOfAnalystOpinions"]` (AAPL 39).
- Analyst-breakout detector ignores price < 1 and upside > 300% (stale targets gave up to +203,000%). Commit f34244f.

### 2026-10-21: Claude Code (full review of all pipelines + fixes) - deployed 2026-10-21
- Fundamentals: every task ignored `EXCHANGE` and ran all 11 exchanges (11 copies racing on the same S3 keys); only
  NASDAQ 36% and HKEX 13% survived. Fixed; now ~15,800 stocks.
- Glue catalog Lambda counted `type=price_targets` etc. as a partition value, so analyst, earnings, dividends, splits,
  opportunities never registered; financials (exchange=/date= order) wasn't listed. Fixed (match by name); backfill
  registered 4,154 partitions.
- New Glue tables company_profiles, earnings_history, institutional_holders (already written, read by detectors).
- Signals: insider query used a non-existent column. With these fixes, opportunities produced for the first time.
- Analyst/financials/fundamentals: all exchanges at once, 4 h limit (TSE/HKEX had been timing out; financials had timed
  out every Sunday since at least 13 Sep). Analyst task 1 GB (OOM on Oct 14).
- Corporate actions: one malformed ticker no longer crashes the exchange. Insider: SEC paging stopped at 200 of ~770
  filings when throttled; now paced and retried (1,149 transactions vs 204).
- ECS state machines reference task definitions by family name; EcsRunTask had imported the revisioned ARN, which blocked
  every ECS change (CloudFormation won't change an export in use).
- (Version pin to yfinance 0.2.61 made here was wrong; reversed on 2026-10-23.) Commits 1484d31, bbb1589.
- `emit-metrics` totals were always 0 (wrong event shape); fixed + new `ChunksFailed` metric. New `RegisterPartitions`
  step makes each run queryable at once.

### 2026-10-20: Claude Code (ticker lists) - deployed 2026-10-20
- LSE list was the FTSE 350 from a one-off July script; the weekly refresh had failed every week (xlrd missing; the SETS
  file stopped updating in 2020). LSE now from Yahoo's screener: 350 -> ~1,400.
- TSE 50 -> 3,752, Paris 338 -> 585, Milan 159 -> 364, Brussels 71 -> 105, Amsterdam 50 (corrupted `.AS.AS`) -> 105,
  Lisbon 20 -> 29, HKEX + 11 REITs. XETRA 251 -> 514 from Deutsche Börse's official file + companiesmarketcap.
- NASDAQ: dropped SPAC units and rights (not on Trading 212); warrants kept after the owner checked AACIW in the app.
- Refresh refuses any list that shrinks by more than half. Commits 7ac3fd7, b018876, e766f75.

### 2026-10-20: Claude Code (collect-stocks reliability) - deployed 2026-10-20
- Sharded runs were failing: Lambdas ran out of memory or hung in yfinance's thread wait loop (no timeout), and one failed
  chunk aborted the whole run. Last week's single-Lambda runs had been failing the same way (and timing out at 15 min x 3
  retries, which is why Lambda costs climbed to ~$12.69 by Oct 20).
- Replaced `yf.download(threads=True)` with our own 4-worker pool and a 60 s batch deadline; per-chunk catch in the Map;
  fixed the `$.Error` path; memory 768 MB; Yahoo symbol mapping (BRK/A -> BRK-A, TRTN^A -> TRTN-PA).
- Lambda cost estimate after the fix: ~$0 in a clean month (inside the free tier). Commit ecb621d.
