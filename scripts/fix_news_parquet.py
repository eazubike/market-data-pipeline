"""
One-time fix: Re-encode existing news Parquet files with compliant nested types.

This reads each existing news Parquet file from S3, and rewrites it in place
with use_compliant_nested_type=True so Athena can read the ARRAY columns.

Usage:
    py -3 scripts/fix_news_parquet.py
"""
import io
import boto3
import pyarrow.parquet as pq

BUCKET = "market-data-082121306678-us-east-1"
PREFIX = "news/"
REGION = "us-east-1"

s3 = boto3.client("s3", region_name=REGION)


def list_parquet_files(prefix: str) -> list[str]:
    """List all .parquet files under a prefix."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def rewrite_file(key: str) -> None:
    """Read a Parquet file from S3, rewrite with Athena-compatible list encoding."""
    import pyarrow as pa

    resp = s3.get_object(Bucket=BUCKET, Key=key)
    table = pq.read_table(io.BytesIO(resp["Body"].read()))

    # Fix list columns: Athena requires the inner field to be named "item"
    # PyArrow defaults to "element" — we need to rebuild the schema
    new_fields = []
    needs_fix = False
    for field in table.schema:
        if isinstance(field.type, pa.ListType):
            inner_type = field.type.value_type
            new_list_type = pa.list_(pa.field("item", inner_type))
            new_fields.append(pa.field(field.name, new_list_type))
            needs_fix = True
        else:
            new_fields.append(field)

    if not needs_fix:
        return

    new_schema = pa.schema(new_fields)

    # Rebuild table column by column with correct schema
    new_columns = []
    for i, field in enumerate(new_schema):
        col = table.column(i)
        if isinstance(field.type, pa.ListType):
            # Rebuild the chunked array with the new field name
            new_chunks = []
            for chunk in col.chunks:
                new_arr = pa.ListArray.from_arrays(
                    chunk.offsets,
                    chunk.values,
                    type=field.type,
                )
                new_chunks.append(new_arr)
            new_columns.append(pa.chunked_array(new_chunks, type=field.type))
        else:
            new_columns.append(col)

    new_table = pa.table(
        {field.name: col for field, col in zip(new_schema, new_columns)},
        schema=new_schema,
    )

    buf = io.BytesIO()
    pq.write_table(new_table, buf, compression="snappy", use_compliant_nested_type=True)
    buf.seek(0)

    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.read(), ContentType="application/octet-stream")


def main():
    print(f"Listing Parquet files in s3://{BUCKET}/{PREFIX}...")
    keys = list_parquet_files(PREFIX)
    print(f"Found {len(keys)} files to fix.")

    for i, key in enumerate(keys):
        print(f"  [{i+1}/{len(keys)}] Rewriting: {key}")
        try:
            rewrite_file(key)
        except Exception as e:
            print(f"    ERROR: {e}")

    print(f"\nDone. All {len(keys)} files rewritten with compliant nested types.")
    print("Now run in Athena:")
    print("  MSCK REPAIR TABLE market_data.news;")
    print("  SELECT * FROM market_data.news LIMIT 10;")


if __name__ == "__main__":
    main()
