"""
Lambda: update-glue-catalog

Registers new S3 partitions with the Glue Data Catalog so Athena can
query them immediately without a full MSCK REPAIR TABLE scan.

Runs standalone on a schedule (4x/day). Scans S3 prefixes for the last
30 days, diffs against existing Glue partitions, and batch-creates any
missing ones.

Input (optional):
    { "lookback_days": 30 }

Output:
    { "partitions_added": 12 }
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone

import boto3

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()
glue = boto3.client("glue")
s3 = boto3.client("s3")

DATA_BUCKET = os.environ["DATA_BUCKET"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]

# All tables and their S3 layout
TABLE_REGISTRY = [
    {"name": "stocks", "prefix": "stocks/", "depth": 3},
    {"name": "fundamentals", "prefix": "fundamentals/", "depth": 2},
    {"name": "news", "prefix": "news/", "depth": 3},
    {
        "name": "analyst_price_targets",
        "prefix": "analyst/type=price_targets/",
        "depth": 2,
    },
    {
        "name": "analyst_recommendations",
        "prefix": "analyst/type=recommendations/",
        "depth": 2,
    },
    {"name": "earnings_dates", "prefix": "analyst/type=earnings_dates/", "depth": 2},
    {"name": "splits", "prefix": "corporate_actions/type=split/", "depth": 2},
    {"name": "dividends", "prefix": "corporate_actions/type=dividend/", "depth": 2},
    {"name": "insider_transactions", "prefix": "insider_transactions/", "depth": 2},
]


def _list_child_prefixes(prefix: str) -> list[str]:
    """Return immediate child prefixes under a given S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=DATA_BUCKET, Prefix=prefix, Delimiter="/")
    prefixes: list[str] = []
    for page in pages:
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])
    return prefixes


def _list_prefixes_at_depth(prefix: str, depth: int) -> list[str]:
    """Recursively list S3 prefixes down to the specified depth."""
    if depth == 0:
        return [prefix]
    children = _list_child_prefixes(prefix)
    results: list[str] = []
    for child in children:
        results.extend(_list_prefixes_at_depth(child, depth - 1))
    return results


def _partition_values_from_key(key: str) -> list[str]:
    """
    Extract ordered partition values from a Hive-style S3 key.
    e.g. 'stocks/date=2026-07-24/run_timestamp=.../exchange=NASDAQ/'
    → ['2026-07-24', '...', 'NASDAQ']
    """
    return re.findall(r"=([^/]+)", key)


def _get_existing_partitions(
    table_name: str, cutoff_date: str, partition_keys: list[str]
) -> set[tuple[str, ...]]:
    """
    Fetch registered partitions for a table, filtered to the last N days.
    Uses Glue's Expression filter on the 'date' partition key so we only
    pull back the relevant window — keeps API pages constant over time.
    """
    paginator = glue.get_paginator("get_partitions")
    existing: set[tuple[str, ...]] = set()

    # Build filter expression if table has a 'date' partition key
    paginate_kwargs: dict = {
        "DatabaseName": GLUE_DATABASE,
        "TableName": table_name,
    }
    if "date" in partition_keys:
        paginate_kwargs["Expression"] = f"date >= '{cutoff_date}'"

    try:
        for page in paginator.paginate(**paginate_kwargs):
            for p in page["Partitions"]:
                existing.add(tuple(p["Values"]))
    except glue.exceptions.EntityNotFoundException:
        logger.warning("Table not found in Glue catalog", extra={"table": table_name})
        return set()
    return existing


def _get_table_info(table_name: str) -> dict | None:
    """Get table metadata (storage descriptor + partition key count)."""
    try:
        resp = glue.get_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        return resp["Table"]
    except glue.exceptions.EntityNotFoundException:
        logger.warning("Table not found in Glue catalog", extra={"table": table_name})
        return None


def _filter_date_prefixes(prefixes: list[str], cutoff_date: str) -> list[str]:
    """
    Filter prefixes to only include those with date= >= cutoff_date.
    If a prefix doesn't contain a date= partition, keep it (let depth walk handle it).
    """
    filtered = []
    for p in prefixes:
        match = re.search(r"date=(\d{4}-\d{2}-\d{2})", p)
        if match:
            if match.group(1) >= cutoff_date:
                filtered.append(p)
        else:
            filtered.append(p)
    return filtered


def add_partitions_for_table(
    table_name: str, prefix: str, depth: int, cutoff_date: str
) -> int:
    """
    Scan S3 for leaf prefixes (scoped to last N days), diff against
    existing Glue partitions, and batch-create any missing ones.
    """
    table_info = _get_table_info(table_name)
    if table_info is None:
        return 0

    sd_template = table_info["StorageDescriptor"]
    partition_keys = [k["Name"] for k in table_info.get("PartitionKeys", [])]
    expected_keys = len(partition_keys)

    # Get existing partitions (filtered to last N days via Glue Expression)
    existing = _get_existing_partitions(table_name, cutoff_date, partition_keys)
    logger.info(
        "Existing partitions fetched",
        extra={"table": table_name, "count": len(existing)},
    )

    # Walk S3 but filter by date at the first date= level to limit scope
    # First level children
    first_level = _list_child_prefixes(prefix)
    first_level = _filter_date_prefixes(first_level, cutoff_date)

    # Continue walking remaining depth levels
    leaf_prefixes: list[str] = []
    remaining_depth = depth - 1
    for child in first_level:
        leaf_prefixes.extend(_list_prefixes_at_depth(child, remaining_depth))

    # Diff against existing partitions
    batch: list[dict] = []
    added = 0

    for leaf in leaf_prefixes:
        values = _partition_values_from_key(leaf)
        if not values:
            continue
        if len(values) != expected_keys:
            continue
        if tuple(values) in existing:
            continue

        sd = {**sd_template, "Location": f"s3://{DATA_BUCKET}/{leaf}"}
        batch.append({"Values": values, "StorageDescriptor": sd})

        if len(batch) == 100:
            glue.batch_create_partition(
                DatabaseName=GLUE_DATABASE,
                TableName=table_name,
                PartitionInputList=batch,
            )
            added += len(batch)
            batch = []

    if batch:
        glue.batch_create_partition(
            DatabaseName=GLUE_DATABASE,
            TableName=table_name,
            PartitionInputList=batch,
        )
        added += len(batch)

    return added


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    lookback_days = event.get("lookback_days", 30)
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime(
        "%Y-%m-%d"
    )

    logger.info("Starting partition sync", extra={"cutoff_date": cutoff_date})

    total_added = 0

    for table_cfg in TABLE_REGISTRY:
        table_name = table_cfg["name"]
        prefix = table_cfg["prefix"]
        depth = table_cfg["depth"]

        added = add_partitions_for_table(table_name, prefix, depth, cutoff_date)
        logger.info("Partitions added", extra={"table": table_name, "count": added})
        total_added += added

    return {"partitions_added": total_added, "cutoff_date": cutoff_date}
