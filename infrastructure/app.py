#!/usr/bin/env python3
"""
Market Data Pipeline — CDK App entry point.

Deploy with:
    cdk deploy --all
"""
import os

import aws_cdk as cdk
from stacks.storage_stack import StorageStack
from stacks.compute_stack import ComputeStack
from stacks.orchestration_stack import OrchestrationStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account")
    or os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=app.node.try_get_context("region")
    or os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

storage = StorageStack(app, "MarketDataStorage", env=env)

compute = ComputeStack(
    app,
    "MarketDataCompute",
    data_bucket=storage.data_bucket,
    dedup_table=storage.dedup_table,
    glue_database_name=storage.glue_database_name,
    env=env,
)

OrchestrationStack(
    app,
    "MarketDataOrchestration",
    check_market_hours_fn=compute.check_market_hours_fn,
    collect_stocks_fn=compute.collect_stocks_fn,
    compact_stocks_fn=compute.compact_stocks_fn,
    collect_news_fn=compute.collect_news_fn,
    update_glue_catalog_fn=compute.update_glue_catalog_fn,
    emit_metrics_fn=compute.emit_metrics_fn,
    ecs_cluster=compute.ecs_cluster,
    financials_task_def=compute.financials_task_def,
    corporate_actions_task_def=compute.corporate_actions_task_def,
    analyst_task_def=compute.analyst_task_def,
    fundamentals_task_def=compute.fundamentals_task_def,
    insider_task_def=compute.insider_task_def,
    env=env,
)

app.synth()
