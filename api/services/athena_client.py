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
DATA_BUCKET = os.environ.get("DATA_BUCKET", "market-data-082121306678-us-east-1")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", f"s3://{DATA_BUCKET}/athena-results/")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

athena = boto3.client("athena", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


async def query_athena(query: str) -> list[dict]:
    """
    Execute an Athena query and return results as a list of dicts.
    """
    print(f"[ATHENA] Database={GLUE_DATABASE} Output={ATHENA_OUTPUT}")
    print(f"[ATHENA] Query: {query[:200]}")

    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": GLUE_DATABASE},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        execution_id = response["QueryExecutionId"]
        print(f"[ATHENA] Execution ID: {execution_id}")

        # Poll until complete (max ~5 minutes)
        for _ in range(60):
            status = athena.get_query_execution(QueryExecutionId=execution_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                print(f"[ATHENA] Query SUCCEEDED")
                break
            elif state in ("FAILED", "CANCELLED"):
                reason = status["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown"
                )
                print(f"[ATHENA] Query FAILED: {reason}")
                return []
            time.sleep(3)
        else:
            print("[ATHENA] Query TIMED OUT")
            return []

        # Read results from S3
        result_location = status["QueryExecution"]["ResultConfiguration"][
            "OutputLocation"
        ]
        bucket = result_location.split("/")[2]
        key = "/".join(result_location.split("/")[3:])

        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        print(f"[ATHENA] Rows returned: {len(df)}")

        # Convert to list of dicts, handle NaN
        records = df.where(df.notna(), None).to_dict("records")
        return records

    except Exception as e:
        print(f"[ATHENA EXCEPTION] {type(e).__name__}: {e}")
        return []
