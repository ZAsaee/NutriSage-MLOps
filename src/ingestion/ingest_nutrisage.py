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

from fe import schema  # KEEP_COLS, DTYPES, extract_columns, make_partition_values

RAW_PREFIX = "raw/"
PROC_PREFIX = "processed/"


# ────────────────────────────── 1 · CLI ────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input",        required=True,
                   help="local .jsonl.gz file")
    p.add_argument("--raw-bucket",   required=True,
                   help="bucket for raw uploads")
    p.add_argument("--proc-bucket",  required=True,
                   help="bucket for processed dataset")
    p.add_argument("--profile",      default=None,
                   help="AWS named profile (optional)")
    p.add_argument("--chunk-rows",   type=int, default=50_000,
                   help="rows per chunk (RAM trade-off)")
    return p.parse_args()


# ─────────────────────────── 2 · AWS helpers ───────────────────────────────
def boto_session(profile: str | None) -> boto3.Session:
    return boto3.Session(profile_name=profile) if profile else boto3.Session()


def upload_raw(local: str | pathlib.Path, bucket: str, s3c: boto3.client) -> str:
    """Copy original file to raw/ and return the object key (idempotent)."""
    local = str(local)
    key = f"{RAW_PREFIX}{pathlib.Path(local).name}"
    print(f"→ Uploading raw file → s3://{bucket}/{key}")
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
    )


# ─────────────────────── 3 · Main streaming loop ───────────────────────────
def stream_ingest(local_path: str,
                  raw_bucket: str,
                  proc_bucket: str,
                  session: boto3.Session,
                  chunk_rows: int) -> None:

    s3c = session.client("s3")
    # Uncomment if you want to archive the raw file
    # upload_raw(local_path, raw_bucket, s3c)

    start, rows_written = time.time(), 0

    with gzip.open(local_path, "rt", encoding="utf-8") as fh, tqdm(unit="rows") as bar:
        while True:
            lines = list(itertools.islice(fh, chunk_rows))
            if not lines:
                break

            # ---------- flatten JSON → DataFrame ---------------------------------
            recs: list[Dict[str, Any]] = (
                schema.extract_columns(json.loads(l)) for l in lines)
            df = pd.DataFrame.from_records(recs)

            # ---------- add partition columns ------------------------------------
            parts = df.apply(schema.make_partition_values,
                             axis=1, result_type="expand")
            df["year"], df["country"] = parts["year"], parts["country"]

            # ---------- 1 · Float nutrient columns --------------------------------
            float_cols = [
                "energy-kcal_100g", "fat_100g", "saturated-fat_100g",
                "carbohydrates_100g", "sugars_100g", "fiber_100g",
                "proteins_100g", "sodium_100g", "fruits-vegetables-nuts_100g",
            ]
            df[float_cols] = df[float_cols].apply(
                lambda s: pd.to_numeric(s, errors="coerce").astype("float32")
            )

            # ---------- 2 · Integer count columns --------------------------------
            int_cols = [
                "additives_n",
                "ingredients_from_palm_oil_n",
                "ingredients_that_may_be_from_palm_oil_n",
            ]
            df[int_cols] = df[int_cols].apply(
                lambda s: pd.to_numeric(
                    s, errors="coerce").round().astype("Int64")
            )

            # ---------- 3 · Tag arrays → list[string] ----------------------------
            tag_cols = ["categories_tags", "brands_tags", "countries_tags"]

            def _to_str_list(x: Any) -> list[str]:
                if isinstance(x, list):
                    return [str(i) for i in x if pd.notna(i)]
                if pd.isna(x) or x in ("", None):
                    return []
                return [str(x)]

            for col in tag_cols:
                df[col] = df[col].apply(_to_str_list)

            # ---------- 4 · Misc strings & timestamp -----------------------------
            df["main_category"] = df["main_category"].astype(
                "string").replace("", pd.NA)
            df["serving_size"] = df["serving_size"].astype(
                "string").replace("", pd.NA)
            df["nutrition_grade_fr"] = df["nutrition_grade_fr"].astype(
                "string").replace("", pd.NA)
            df["created_t"] = pd.to_numeric(
                df["created_t"], errors="coerce").round().astype("Int64")

            # ---------- 5 · Final column order & cast ----------------------------
            # drop extras / keep order
            df = df[schema.KEEP_COLS + ["year", "country"]]
            # enforce final dtypes
            df = df.astype(schema.DTYPES, errors="ignore")

            # ---------- 6 · Write chunk ------------------------------------------
            write_parquet(df, proc_bucket, session)
            rows_written += len(df)
            bar.update(len(df))

    mins = (time.time() - start) / 60
    print(f"✔ Ingested {rows_written:,} rows in {mins:.1f} min "
          f"({rows_written/(mins*60):,.0f} rows/s)")


# ───────────────────────── 4 · Entry point ─────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    sess = boto_session(args.profile)
    stream_ingest(
        local_path=args.input,
        raw_bucket=args.raw_bucket,
        proc_bucket=args.proc_bucket,
        session=sess,
        chunk_rows=args.chunk_rows,
    )
