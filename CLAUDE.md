# Market Data Pipeline — notes for Claude

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
  `ecs/*/requirements.txt` (match `layer/requirements.txt`).
