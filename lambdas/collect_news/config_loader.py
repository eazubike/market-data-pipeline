"""
Shared utility — loads exchange config and ticker lists from S3.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any

import boto3

s3 = boto3.client("s3")
DATA_BUCKET = os.environ["DATA_BUCKET"]


@lru_cache(maxsize=1)
def load_exchanges() -> dict[str, Any]:
    """Return the full exchanges.json config from S3 (cached per Lambda warm start)."""
    resp = s3.get_object(Bucket=DATA_BUCKET, Key="config/exchanges.json")
    return json.loads(resp["Body"].read())


def load_tickers(exchange: str) -> list[str]:
    """Return list of ticker symbols for an exchange from config/tickers/{exchange}.csv."""
    key = f"config/tickers/{exchange}.csv"
    try:
        resp = s3.get_object(Bucket=DATA_BUCKET, Key=key)
        lines = resp["Body"].read().decode("utf-8").splitlines()
    except s3.exceptions.NoSuchKey:
        return []

    tickers: list[str] = []
    for line in lines[1:]:  # skip header
        parts = line.split(",")
        if parts:
            tickers.append(parts[0].strip())
    return [t for t in tickers if t]
