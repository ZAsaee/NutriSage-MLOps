"""Stream-ingest OpenFoodFacts JSONL → Parquet (year / country partitions)."""

from __future__ import annotations

import argparse
import gzip
import itertools
import json
import pathlib
import time
from typing import Any, Dict

import boto3
import pandas as pd
import awswrangler as wr
from tqdm import tqdm

from fe.schema import (
    KEEP_COLS,
    DTYPES,
    extract_columns,
    make_partition_values,
)

RAW_PREFIX = "raw/"
PROC_PREFIX = "processed/"


# 1 ───────── CLI ────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input",   required=True, help="local .jsonl.gz path")
    p.add_argument("--raw-bucket", required=True,
                   help="Bucket for raw/ immutable uploads")
    p.add_argument("--proc-bucket", required=True,
                   help="Bucket for processed/ Parquet dataset")
    p.add_argument("--profile", default=None, help="AWS profile (optional)")
    p.add_argument("--chunk-rows", type=int, default=50_000,
                   help="rows per chunk (RAM trade-off)")
    return p.parse_args()


# 2 ───────── AWS helpers ───────────────────────────────────────
def boto_session(profile: str | None) -> boto3.Session:
    return boto3.Session(profile_name=profile) if profile else boto3.Session()


def upload_raw(local: str | pathlib.Path, bucket: str, s3c: boto3.client) -> str:
    """Copy original file to raw/ and return the object key."""
    local = str(local)
    key = f"{RAW_PREFIX}{pathlib.Path(local).name}"
    print(f"→ Uploading raw file to s3://{bucket}/{key}")
    s3c.upload_file(local, bucket, key)
    return key


def write_parquet(df: pd.DataFrame, bucket: str, session: boto3.Session) -> None:
    wr.s3.to_parquet(
        df,
        path=f"s3://{bucket}/{PROC_PREFIX}",
        dataset=True,
        partition_cols=["year", "country"],
        mode="append",
        compression="snappy",
        boto3_session=session,
        # remove / rename the next line if your awswrangler version is old:
        # concurrent_partition_upload=True,
    )


# 3 ───────── Main streaming loop ───────────────────────────────
def stream_ingest(local_path: str, raw_bucket: str, proc_bucket: str,
                  session: boto3.Session, chunk_rows: int) -> None:
    s3c = session.client("s3")
    # upload_raw(local_path, raw_bucket, s3c)

    start, rows_written = time.time(), 0

    with gzip.open(local_path, "rt", encoding="utf-8") as fh, tqdm(unit="rows") as bar:
        while True:
            lines = list(itertools.islice(fh, chunk_rows))
            if not lines:
                break

            # flatten selected columns
            recs = (extract_columns(json.loads(l)) for l in lines)
            df = pd.DataFrame.from_records(recs)

            # add partition cols
            parts = df.apply(make_partition_values,
                             axis=1, result_type="expand")
            df["year"], df["country"] = parts["year"], parts["country"]

            # cast dtypes
            df = df.astype(DTYPES, errors="ignore")

            write_parquet(df, proc_bucket, session)
            rows_written += len(df)
            bar.update(len(df))

    mins = (time.time() - start) / 60
    print(f"✔ Ingested {rows_written:,} rows in {mins:.1f} min "
          f"({rows_written/(mins*60):,.0f} rows/s)")


# 4 ───────── Entry point ───────────────────────────────────────
if __name__ == "__main__":
    a = parse_args()
    sess = boto_session(a.profile)
    stream_ingest(a.input,
                  raw_bucket=a.raw_bucket,
                  proc_bucket=a.proc_bucket,
                  session=sess,
                  chunk_rows=a.chunk_rows)
