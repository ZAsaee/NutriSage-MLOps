"""
Validate the processed Parquet dataset in S3, showing row counts.

Works with aws-wrangler ≤2.x, 3.0–3.3 (detailed), 3.4+ (summary), and future.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict

import awswrangler as wr
import boto3
import pyarrow as pa
import pyarrow.dataset as ds

from fe.schema import KEEP_COLS, DTYPES

# ───────────────────────── CONFIG ─────────────────────────────────────────────
PROC_PREFIX = "processed"
PART_COLS = ["year", "country"]

# Explicit list-column types
LIST_TYPES: Dict[str, pa.DataType] = {
    "categories_tags": pa.list_(pa.string()),
    "brands_tags":     pa.list_(pa.string()),
    "countries_tags":  pa.list_(pa.string()),
}


# ─────────────────────── Read metadata ───────────────────────────────────────
def read_metadata(bucket: str, profile: str | None) -> Any:
    path = f"s3://{bucket}/{PROC_PREFIX}"
    sess = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return wr.s3.read_parquet_metadata(
        path, dataset=True, boto3_session=sess
    )


# ───────────────────── Summary mode ───────────────────────────────────────────
def is_summary(meta: Any) -> bool:
    return hasattr(meta, "columns_types") and hasattr(meta, "partitions_types")


def check_summary(meta: Any) -> None:
    cols: Dict[str, str] = meta.columns_types
    parts: Dict[str, str] = meta.partitions_types

    missing_cols = [c for c in KEEP_COLS if c not in cols]
    extra_cols = [c for c in cols if c not in KEEP_COLS]
    if missing_cols or extra_cols:
        raise ValueError(
            f"Summary: missing cols={missing_cols}, extras={extra_cols}")

    missing_parts = [p for p in PART_COLS if p not in parts]
    extra_parts = [p for p in parts if p not in PART_COLS]
    if missing_parts or extra_parts:
        raise ValueError(
            f"Summary: missing parts={missing_parts}, extras={extra_parts}")

    # Map pandas dtypes → summary string
    mapping = {"float32": "float", "Int64": "bigint", "string": "string"}
    errors = []
    for col, want in DTYPES.items():
        exp = mapping[want]
        act = cols[col].lower()
        if act != exp:
            errors.append(f"{col}: summary says {act}, expected {exp}")
    if errors:
        raise ValueError("Summary type errors:\n  " + "\n  ".join(errors))


def count_via_dataset(bucket: str) -> int:
    """Fast metadata-only row count via PyArrow dataset."""
    path = f"s3://{bucket}/{PROC_PREFIX}"
    dset = ds.dataset(path, format="parquet", partitioning="hive")
    return dset.count_rows()


# ───────────────────── Detailed mode ─────────────────────────────────────────
def _file_fragments(meta: Any):
    if isinstance(meta, pa.Table):
        yield meta
        return
    if isinstance(meta, dict):
        yield from meta.values()
        return
    fm = getattr(meta, "file_metadata", None)
    if isinstance(fm, dict):
        yield from fm.values()
        return
    raise RuntimeError(f"Cannot iterate fragments for {type(meta)}")


def check_detailed(meta: Any) -> int:
    total = 0
    first_schema: pa.Schema | None = None

    for frag in _file_fragments(meta):
        nr = getattr(frag, "num_rows", None)
        if isinstance(nr, int):
            total += nr
        if first_schema is None:
            schema = getattr(frag, "schema", None)
            if isinstance(schema, pa.Schema):
                first_schema = schema

    if total < 1:
        raise ValueError(f"Row-count too low: {total}")
    if first_schema is None:
        raise RuntimeError("No schema found in fragments")

    # 1) columns
    actual = first_schema.names
    expected = KEEP_COLS + PART_COLS
    missing = [c for c in expected if c not in actual]
    extras = [c for c in actual if c not in expected]
    if missing or extras:
        raise ValueError(f"Detailed: missing={missing}, extras={extras}")

    # 2) physical types
    basic = {"float32": pa.float32(), "Int64": pa.int64(),
             "string": pa.string()}
    expected_types: Dict[str, pa.DataType] = {
        col: basic[d] for col, d in DTYPES.items()
    }
    expected_types.update(LIST_TYPES)

    errs: list[str] = []
    for field in first_schema:
        want = expected_types.get(field.name)
        if want and not pa.types.is_same_type(field.type, want):
            errs.append(f"{field.name}: {field.type} ≠ {want}")
    if errs:
        raise ValueError("Detailed: type errors:\n  " + "\n  ".join(errs))

    return total


# ─────────────────────────────── CLI ─────────────────────────────────────────
def main(argv: list[str] | None = None):
    p = argparse.ArgumentParser(description="Validate Parquet ingest")
    p.add_argument("--bucket",  required=True, help="S3 bucket name")
    p.add_argument("--profile", help="AWS profile (optional)")
    args = p.parse_args(argv)

    try:
        meta = read_metadata(args.bucket, args.profile)

        if is_summary(meta):
            check_summary(meta)
            total = count_via_dataset(args.bucket)
            print(
                f"✔ Summary validation OK – {total:,} rows; cols & types match")
        else:
            total = check_detailed(meta)
            print(
                f"✔ Detailed validation OK – {total:,} rows; schema & types match")

    except Exception as exc:
        print(f"Validation failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
