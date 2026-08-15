"""
Athena query service — executes SQL queries against the market_data Glue database
and returns results as list of dicts.
"""

from __future__ import annotations

import io
import os
import time

import boto3
import pandas as pd

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "market_data")
DATA_BUCKET = os.environ.get("DATA_BUCKET", "")
ATHENA_OUTPUT = os.environ.get(
    "ATHENA_OUTPUT", f"s3://{DATA_BUCKET}/athena-results/"
)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

athena = boto3.client("athena", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


async def query_athena(query: str) -> list[dict]:
    """
    Execute an Athena query and return results as a list of dicts.
    Async wrapper for synchronous boto3 calls.
    """
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": GLUE_DATABASE},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        execution_id = response["QueryExecutionId"]

        # Poll until complete (max ~5 minutes)
        for _ in range(60):
            status = athena.get_query_execution(QueryExecutionId=execution_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            elif state in ("FAILED", "CANCELLED"):
                return []
            time.sleep(5)
        else:
            return []

        # Read results from S3
        result_location = status["QueryExecution"]["ResultConfiguration"][
            "OutputLocation"
        ]
        bucket = result_location.split("/")[2]
        key = "/".join(result_location.split("/")[3:])

        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        # Convert to list of dicts, handle NaN
        records = df.where(df.notna(), None).to_dict("records")
        return records

    except Exception:
        return []
