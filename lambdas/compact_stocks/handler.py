"""
Lambda: compact-stocks

Runs after all exchange stock collection Lambdas finish.
Compacts per-batch Parquet files into a single data.parquet per partition.

Input (from Step Function — list of results from the Map):
    {
      "parallel_results": [[{"exchange": "NASDAQ", "stocks_s3_prefix": "stocks/..."}, ...]],
      ...
    }

Output:
    { "compacted": 5 }
"""

from __future__ import annotations

import io
import os
import sys

import boto3
import pandas as pd
import pyarrow as pa

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from parquet_writer import write_parquet

logger = Logger()

s3 = boto3.client("s3")
DATA_BUCKET = os.environ["DATA_BUCKET"]

STOCKS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("price", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("currency", pa.string()),
    ]
)


def _compact_prefix(s3_prefix: str) -> bool:
    """Read all batch_*.parquet files in a prefix, merge into data.parquet, delete batches."""
    if not s3_prefix:
        return False

    paginator = s3.get_paginator("list_objects_v2")
    batch_dfs: list[pd.DataFrame] = []
    batch_keys: list[str] = []

    for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "batch_" in key and key.endswith(".parquet"):
                resp = s3.get_object(Bucket=DATA_BUCKET, Key=key)
                batch_dfs.append(pd.read_parquet(io.BytesIO(resp["Body"].read())))
                batch_keys.append(key)

    if not batch_dfs:
        return False

    merged_df = pd.concat(batch_dfs, ignore_index=True)
    final_key = f"{s3_prefix}data.parquet"
    write_parquet(merged_df, final_key, schema=STOCKS_SCHEMA)
    logger.info(
        "Compacted",
        extra={"key": final_key, "rows": len(merged_df), "batches": len(batch_keys)},
    )

    # Delete batch files
    for key in batch_keys:
        s3.delete_object(Bucket=DATA_BUCKET, Key=key)

    return True


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    # The event comes from the Step Function — extract s3_prefixes from stock results
    # The parallel_results contains the Map output (list of per-exchange results)
    compacted = 0

    # Try to find stock results in various event shapes
    prefixes: list[str] = []

    # Shape 1: direct list of results from Map
    if isinstance(event, list):
        for item in event:
            if isinstance(item, dict) and item.get("stocks_s3_prefix"):
                prefixes.append(item["stocks_s3_prefix"])
    # Shape 2: nested in parallel_results
    elif "parallel_results" in event:
        for branch in event["parallel_results"]:
            if isinstance(branch, list):
                for item in branch:
                    if isinstance(item, dict) and item.get("stocks_s3_prefix"):
                        prefixes.append(item["stocks_s3_prefix"])
    # Shape 3: stocks_results directly
    elif "stocks_results" in event:
        for item in event["stocks_results"]:
            if isinstance(item, dict):
                # The stock_result is nested one more level from LambdaInvoke
                result = item.get("stock_result", {}).get("Payload", item)
                if result.get("stocks_s3_prefix"):
                    prefixes.append(result["stocks_s3_prefix"])

    logger.info("Starting compaction", extra={"prefixes_found": len(prefixes)})

    for prefix in prefixes:
        if _compact_prefix(prefix):
            compacted += 1

    return {"compacted": compacted}
