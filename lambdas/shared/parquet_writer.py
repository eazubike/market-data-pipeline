"""
Shared utility — writes a pandas DataFrame as Snappy-compressed Parquet to S3.
"""
from __future__ import annotations

import io
import os

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client("s3")
DATA_BUCKET = os.environ["DATA_BUCKET"]


def write_parquet(
    df: pd.DataFrame,
    s3_key: str,
    schema: pa.Schema | None = None,
    bucket: str | None = None,
) -> str:
    """
    Serialize df to Parquet (Snappy) and upload to S3.

    Returns the full s3:// URI of the written object.
    """
    target_bucket = bucket or DATA_BUCKET

    if df.empty:
        return ""

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", use_compliant_nested_type=True)
    buf.seek(0)

    s3.put_object(
        Bucket=target_bucket,
        Key=s3_key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )
    return f"s3://{target_bucket}/{s3_key}"
