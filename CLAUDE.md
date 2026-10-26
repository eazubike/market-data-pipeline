# Market Data Pipeline — notes for Claude

## Progress log

`docs/PROGRESS.md` is the running log for this project. Read its **Current state**,
**Rules** and **Next** before starting. After every piece of work (fix, feature,
deploy, rerun, investigation), add a dated entry at the top of its **Log**: what was
done and why, what was verified, deploy status, commit hashes. Update **Current
state** and **Next** when they change. Commit it with the work.

## Keep the architecture doc current

`docs/architecture.html` is the architecture page for this project, published at
https://claude.ai/artifact/97MBDvjQEaVuHfKq5RYtoJ

Whenever a change affects how the system fits together, update
`docs/architecture.html` in the same piece of work and republish it to that same
URL (Artifact tool: publish with `url` set to the link above and `file_path`
`docs/architecture.html`). Do this for changes to:

- state machines, schedules or trigger times (`infrastructure/stacks/orchestration_stack.py`)
- Lambdas, Fargate tasks, memory/timeouts that the page states (`compute_stack.py`, `lambdas/`, `ecs/`)
- S3 prefixes, Glue tables or partitions (`storage_stack.py`, `update_glue_catalog`)
- data sources, ticker-list sources, signal detectors, or what the API/frontend read

Update the parts that changed (flow diagram, 24h timeline rows, pipelines table,
data table, header counts, "as of" date) and commit the HTML with the code change.
Bug fixes that don't change structure, schedules or data layout don't need it.

## Deploying

- CDK app in `infrastructure/` (`npx cdk deploy <Stack> --exclusively`), stacks:
  MarketDataStorage, MarketDataCompute, MarketDataOrchestration.
- Cross-stack exports can't change while imported. When a change removes or
  replaces something the other stack imports, deploy the importing stack first.
- ECS images build locally with Docker; library versions are pinned in each
  `ecs/*/requirements.txt` to yfinance 1.6.0 / pandas 3.0.5 / pyarrow 25.0.1 /
  curl_cffi 0.16.0 — the versions that ran without Yahoo throttling. Older
  yfinance (0.2.61) got rate-limited after ~640 tickers; never downgrade it.
  The Lambda layer (`layer/requirements.txt`, yfinance 0.2.61) is separate.
- Yahoo rate limits must be retried, never swallowed: catch YFRateLimitError
  explicitly, back off and retry the ticker (see ecs/analyst_data).
