"""
migrate_s3_paths.py — One-time migration to reorder S3 partition paths.

Old structure:
  stocks/exchange=NASDAQ/date=2026-08-03/run_timestamp=.../data.parquet
  fundamentals/exchange=NASDAQ/date=2026-08-03/data.parquet
  insider_transactions/exchange=NASDAQ/date=2026-08-03/data.parquet
  news/source=cnbc/date=2026-08-03/run_timestamp=.../data.parquet
  corporate_actions/type=split/exchange=NASDAQ/date=2026-08-03/data.parquet
  analyst/type=price_targets/exchange=NASDAQ/date=2026-08-03/data.parquet
  financials/exchange=NASDAQ/date=2026-08-03/data.parquet

New structure:
  stocks/date=2026-08-03/run_timestamp=.../exchange=NASDAQ/data.parquet
  fundamentals/date=2026-08-03/exchange=NASDAQ/data.parquet
  insider_transactions/date=2026-08-03/exchange=NASDAQ/data.parquet
  news/date=2026-08-03/run_timestamp=.../source=cnbc/data.parquet
  corporate_actions/type=split/date=2026-08-03/exchange=NASDAQ/data.parquet
  analyst/type=price_targets/date=2026-08-03/exchange=NASDAQ/data.parquet
  financials/date=2026-08-03/exchange=NASDAQ/data.parquet

Usage:
    py -3 scripts/migrate_s3_paths.py
"""
import re
import boto3

BUCKET = "market-data-082121306678-us-east-1"
REGION = "us-east-1"

s3 = boto3.client("s3", region_name=REGION)


def list_all_keys(prefix: str) -> list[str]:
    """List all object keys under a prefix."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def migrate_stocks():
    """
    Old: stocks/exchange=X/date=D/run_timestamp=R/file.parquet
    New: stocks/date=D/run_timestamp=R/exchange=X/file.parquet
    """
    print("\n[stocks] Migrating...")
    keys = list_all_keys("stocks/exchange=")
    pattern = re.compile(r"stocks/exchange=([^/]+)/date=([^/]+)/run_timestamp=([^/]+)/(.+)")
    moved = 0
    for key in keys:
        m = pattern.match(key)
        if not m:
            continue
        exchange, date, run_ts, filename = m.groups()
        new_key = f"stocks/date={date}/run_timestamp={run_ts}/exchange={exchange}/{filename}"
        s3.copy_object(Bucket=BUCKET, CopySource=f"{BUCKET}/{key}", Key=new_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        moved += 1
    print(f"  Moved {moved} files")


def migrate_news():
    """
    Old: news/source=X/date=D/run_timestamp=R/file.parquet
    New: news/date=D/run_timestamp=R/source=X/file.parquet
    """
    print("\n[news] Migrating...")
    keys = list_all_keys("news/source=")
    pattern = re.compile(r"news/source=([^/]+)/date=([^/]+)/run_timestamp=([^/]+)/(.+)")
    moved = 0
    for key in keys:
        m = pattern.match(key)
        if not m:
            continue
        source, date, run_ts, filename = m.groups()
        new_key = f"news/date={date}/run_timestamp={run_ts}/source={source}/{filename}"
        s3.copy_object(Bucket=BUCKET, CopySource=f"{BUCKET}/{key}", Key=new_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        moved += 1
    print(f"  Moved {moved} files")


def migrate_fundamentals():
    """
    Old: fundamentals/exchange=X/date=D/file.parquet
    New: fundamentals/date=D/exchange=X/file.parquet
    """
    print("\n[fundamentals] Migrating...")
    keys = list_all_keys("fundamentals/exchange=")
    pattern = re.compile(r"fundamentals/exchange=([^/]+)/date=([^/]+)/(.+)")
    moved = 0
    for key in keys:
        m = pattern.match(key)
        if not m:
            continue
        exchange, date, filename = m.groups()
        new_key = f"fundamentals/date={date}/exchange={exchange}/{filename}"
        s3.copy_object(Bucket=BUCKET, CopySource=f"{BUCKET}/{key}", Key=new_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        moved += 1
    print(f"  Moved {moved} files")


def migrate_insider_transactions():
    """
    Old: insider_transactions/exchange=X/date=D/file.parquet
    New: insider_transactions/date=D/exchange=X/file.parquet
    """
    print("\n[insider_transactions] Migrating...")
    keys = list_all_keys("insider_transactions/exchange=")
    pattern = re.compile(r"insider_transactions/exchange=([^/]+)/date=([^/]+)/(.+)")
    moved = 0
    for key in keys:
        m = pattern.match(key)
        if not m:
            continue
        exchange, date, filename = m.groups()
        new_key = f"insider_transactions/date={date}/exchange={exchange}/{filename}"
        s3.copy_object(Bucket=BUCKET, CopySource=f"{BUCKET}/{key}", Key=new_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        moved += 1
    print(f"  Moved {moved} files")


def migrate_corporate_actions():
    """
    Old: corporate_actions/type=X/exchange=E/date=D/file.parquet
    New: corporate_actions/type=X/date=D/exchange=E/file.parquet
    """
    print("\n[corporate_actions] Migrating...")
    keys = list_all_keys("corporate_actions/")
    pattern = re.compile(r"corporate_actions/type=([^/]+)/exchange=([^/]+)/date=([^/]+)/(.+)")
    moved = 0
    for key in keys:
        m = pattern.match(key)
        if not m:
            continue
        action_type, exchange, date, filename = m.groups()
        new_key = f"corporate_actions/type={action_type}/date={date}/exchange={exchange}/{filename}"
        s3.copy_object(Bucket=BUCKET, CopySource=f"{BUCKET}/{key}", Key=new_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        moved += 1
    print(f"  Moved {moved} files")


def migrate_analyst():
    """
    Old: analyst/type=X/exchange=E/date=D/file.parquet
    New: analyst/type=X/date=D/exchange=E/file.parquet
    """
    print("\n[analyst] Migrating...")
    keys = list_all_keys("analyst/")
    pattern = re.compile(r"analyst/type=([^/]+)/exchange=([^/]+)/date=([^/]+)/(.+)")
    moved = 0
    for key in keys:
        m = pattern.match(key)
        if not m:
            continue
        data_type, exchange, date, filename = m.groups()
        new_key = f"analyst/type={data_type}/date={date}/exchange={exchange}/{filename}"
        s3.copy_object(Bucket=BUCKET, CopySource=f"{BUCKET}/{key}", Key=new_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        moved += 1
    print(f"  Moved {moved} files")


def migrate_financials():
    """
    Old: financials/exchange=X/date=D/file.parquet
    New: financials/date=D/exchange=X/file.parquet
    """
    print("\n[financials] Migrating...")
    keys = list_all_keys("financials/exchange=")
    pattern = re.compile(r"financials/exchange=([^/]+)/date=([^/]+)/(.+)")
    moved = 0
    for key in keys:
        m = pattern.match(key)
        if not m:
            continue
        exchange, date, filename = m.groups()
        new_key = f"financials/date={date}/exchange={exchange}/{filename}"
        s3.copy_object(Bucket=BUCKET, CopySource=f"{BUCKET}/{key}", Key=new_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        moved += 1
    print(f"  Moved {moved} files")


def main():
    print("=" * 60)
    print("  S3 Partition Migration")
    print(f"  Bucket: {BUCKET}")
    print("=" * 60)

    migrate_stocks()
    migrate_news()
    migrate_fundamentals()
    migrate_insider_transactions()
    migrate_corporate_actions()
    migrate_analyst()
    migrate_financials()

    print("\n" + "=" * 60)
    print("  Migration complete!")
    print("  Next: deploy code changes + recreate Athena tables")
    print("=" * 60)


if __name__ == "__main__":
    main()
