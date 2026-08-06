"""
Lambda: emit-metrics

Aggregates results from all parallel collection tasks and publishes
a summary to CloudWatch. Called at the end of every pipeline execution,
including on failure.

Input (success path):
    {
      "parallel_results": [
        [{"exchange": "NASDAQ", "tickers_ok": 3412, "tickers_failed": 8, ...}, ...],
        {"articles_new": 87, "articles_skipped_duplicate": 34}
      ],
      "run_timestamp": "...",
      "status": "SUCCESS"   (optional)
    }

Input (error path):
    { "error": "...", "cause": "...", "run_timestamp": "...", "status": "FAILED" }
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()
cw = boto3.client("cloudwatch")

NAMESPACE = "MarketData"


def _put(metric_name: str, value: float, unit: str, dimensions: list[dict]) -> None:
    cw.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[
            {
                "MetricName": metric_name,
                "Dimensions": dimensions,
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.now(timezone.utc),
            }
        ],
    )


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    start_ts = time.monotonic()

    status = event.get("status", "SUCCESS")
    run_timestamp = event.get("run_timestamp", datetime.now(timezone.utc).isoformat())

    global_dims = [{"Name": "Pipeline", "Value": "market-data"}]

    if status == "FAILED":
        logger.error(
            "Pipeline execution failed",
            extra={
                "error": event.get("error"),
                "cause": event.get("cause"),
                "run_timestamp": run_timestamp,
            },
        )
        _put("PipelineFailures", 1, "Count", global_dims)
        return {"status": "FAILED", "metrics_emitted": 1}

    parallel_results: list[Any] = event.get("parallel_results", [])

    # parallel_results[0] is a list of per-exchange stock results
    # parallel_results[1] is the news result
    stock_results: list[dict] = []
    news_result: dict = {}

    if len(parallel_results) >= 1:
        raw_stocks = parallel_results[0]
        if isinstance(raw_stocks, list):
            stock_results = raw_stocks
        elif isinstance(raw_stocks, dict):
            stock_results = [raw_stocks]

    if len(parallel_results) >= 2:
        raw_news = parallel_results[1]
        if isinstance(raw_news, dict):
            news_result = raw_news

    # ── Per-exchange stock metrics ────────────────────────────────────────────
    total_ok = 0
    total_failed = 0

    for result in stock_results:
        if not isinstance(result, dict):
            continue
        exchange = result.get("exchange", "UNKNOWN")
        ok = result.get("tickers_ok", 0)
        failed = result.get("tickers_failed", 0)
        total_ok += ok
        total_failed += failed

        exchange_dims = [{"Name": "Exchange", "Value": exchange}]
        _put("StocksCollected", ok, "Count", exchange_dims)
        _put("StocksFailed", failed, "Count", exchange_dims)

    _put("StocksCollected", total_ok, "Count", global_dims)
    _put("StocksFailed", total_failed, "Count", global_dims)

    # ── News metrics ──────────────────────────────────────────────────────────
    articles_new = news_result.get("articles_new", 0)
    _put("NewsArticlesCollected", articles_new, "Count", global_dims)

    # ── Duration ──────────────────────────────────────────────────────────────
    duration_ms = (time.monotonic() - start_ts) * 1000
    _put("ExecutionDurationMs", duration_ms, "Milliseconds", global_dims)
    _put("PipelineSuccesses", 1, "Count", global_dims)

    logger.info(
        "Metrics emitted",
        extra={
            "total_ok": total_ok,
            "total_failed": total_failed,
            "articles_new": articles_new,
            "exchanges": [r.get("exchange") for r in stock_results],
        },
    )

    return {
        "status": "SUCCESS",
        "total_stocks_collected": total_ok,
        "total_stocks_failed": total_failed,
        "articles_collected": articles_new,
    }
