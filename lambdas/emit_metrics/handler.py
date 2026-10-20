"""
Lambda: emit-metrics

Aggregates results from all parallel collection tasks and publishes
a summary to CloudWatch. Called at the end of every pipeline execution,
including on failure.

Input (success path):
    {
      "parallel_results": [
        {"stocks_results": [                       # one item per ticker chunk
          {"exchange": "NASDAQ", "stock_result": {"Payload": {"tickers_ok": 390, ...}}},
          {"exchange": "NYSE", "chunk_error": {"Error": "...", "Cause": "..."}},
          ...
        ]},
        {"articles_new": 87, "articles_skipped_duplicate": 34}   (optional)
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


def _chunk_items(raw_stocks: Any) -> list[dict]:
    """Map items from the stocks branch: its state dict, or a bare list of items."""
    if isinstance(raw_stocks, dict):
        raw_stocks = raw_stocks.get("stocks_results", [raw_stocks])
    return [item for item in raw_stocks or [] if isinstance(item, dict)]


def _chunk_result(item: dict) -> dict | None:
    """collect-stocks output for one Map item (unwrapping LambdaInvoke), or None if it failed."""
    if "tickers_ok" in item:
        return item
    payload = item.get("stock_result", {}).get("Payload")
    return payload if isinstance(payload, dict) else None


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

    # parallel_results[0] is the stocks branch (one Map item per ticker chunk)
    # parallel_results[1] is the news result, when present
    chunk_items = _chunk_items(parallel_results[0]) if parallel_results else []
    news_result: dict = {}

    if len(parallel_results) >= 2:
        raw_news = parallel_results[1]
        if isinstance(raw_news, dict):
            news_result = raw_news

    # ── Per-exchange stock metrics (summed across that exchange's chunks) ─────
    per_exchange: dict[str, dict[str, int]] = {}
    for item in chunk_items:
        result = _chunk_result(item)
        exchange = (result or item).get("exchange", "UNKNOWN")
        totals = per_exchange.setdefault(exchange, {"ok": 0, "failed": 0, "chunks_failed": 0})
        if result is None:
            totals["chunks_failed"] += 1
            continue
        totals["ok"] += result.get("tickers_ok", 0)
        totals["failed"] += result.get("tickers_failed", 0)

    for exchange, totals in per_exchange.items():
        exchange_dims = [{"Name": "Exchange", "Value": exchange}]
        _put("StocksCollected", totals["ok"], "Count", exchange_dims)
        _put("StocksFailed", totals["failed"], "Count", exchange_dims)
        _put("ChunksFailed", totals["chunks_failed"], "Count", exchange_dims)

    total_ok = sum(t["ok"] for t in per_exchange.values())
    total_failed = sum(t["failed"] for t in per_exchange.values())
    chunks_failed = sum(t["chunks_failed"] for t in per_exchange.values())
    _put("StocksCollected", total_ok, "Count", global_dims)
    _put("StocksFailed", total_failed, "Count", global_dims)
    _put("ChunksFailed", chunks_failed, "Count", global_dims)

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
            "chunks_failed": chunks_failed,
            "per_exchange": per_exchange,
        },
    )

    return {
        "status": "SUCCESS",
        "total_stocks_collected": total_ok,
        "total_stocks_failed": total_failed,
        "chunks_failed": chunks_failed,
        "articles_collected": articles_new,
    }
