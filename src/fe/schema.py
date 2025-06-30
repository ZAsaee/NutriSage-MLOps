"""
Nutrisage schema & helpers
_____________________________________________________________________________________
* Approved columns (currently 21). Update this list, not the number.
* KEEP_COLS - <= 20 approved columns(loaded from candidate-columns.yml)
* COLUMN_PATHS - JSON paths to reach each column in the raw object
* DTYPES - optional pandas dtypes for faster ingest
* normalize_country / make_partition_values - build year / country partitions
* extract_columns - flattens one raw JSON row into the selected columns
"""

from __future__ import annotations
import re
from datetime import datetime
from importlib.resources import files
from typing import Any, Dict, List
from pathlib import Path

import numpy as np
import yaml


KEEP_COLS: list[str] = [
    # numeric nutrients
    "energy_100g",
    "energy-kcal_100g",
    "fat_100g",
    "saturated-fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "fiber_100g",
    "proteins_100g",
    "sodium_100g",

    # text / lists / misc
    "product_name",
    "main_category",
    "brands_tags",
    "countries_tags",
    "serving_size",

    # timestamp
    "created_t",

    # target
    "nutrition_grade_fr",
]

# hive-style partition columns live in the path, not in KEEP_COLS
PART_COLS: list[str] = ["year", "country"]

# --------------------------------------------------------------------------- #
# Desired Pandas / PyArrow dtypes                                             #
# --------------------------------------------------------------------------- #
DTYPES: dict[str, str] = {
    # floats – keep as float32
    "energy_100g": "float32",
    "energy-kcal_100g": "float32",
    "fat_100g": "float32",
    "saturated-fat_100g": "float32",
    "carbohydrates_100g": "float32",
    "sugars_100g": "float32",
    "fiber_100g": "float32",
    "proteins_100g": "float32",
    "sodium_100g": "float32",

    # plain strings
    "main_category": "string",
    "serving_size": "string",
    "nutrition_grade_fr": "string",

    # epoch seconds
    "created_t": "Int64",
}


TARGET = "nutrition_grade_fr"
PREDICTORS = [c for c in KEEP_COLS if c != TARGET]

# Nutrient-style fields live under the "nutriments" sub-dict
NUTRIMENTS_KEY = [k for k in KEEP_COLS if k.endswith('_100g')]

# map canonical column name to list path inside JSON object
COLUMN_PATHS: Dict[str, List[str]] = {
    col: (["nutriments", col] if col in NUTRIMENTS_KEY else [col])
    for col in KEEP_COLS
}

# Partition helpers (year, country)
_LANG_PREFIX = re.compile(r"^[a-z]{2}[:_\-]?")  # en:, fr-, de_


def normalize_country(tag_field: Any) -> str | None:
    """Return slug like 'united-states' from countries_tags field"""
    if not tag_field:
        return None
    raw = tag_field[0] if isinstance(
        tag_field, list) else str(tag_field).split(',')[0]
    raw = _LANG_PREFIX.sub("", raw.strip().lower())
    slug = re.sub(r"\s+", "-", raw)
    slug = re.sub(r"[^a-z0-9\-]", "", slug)
    return slug or None


def make_partition_values(row: Dict[str, Any]) -> Dict[str, str]:
    """Compute partition values {'year':'2020', 'country':'canada'}"""
    try:
        ts = int(row['created_t'])
        year = datetime.utcfromtimestamp(ts).year
    except (KeyError, TypeError, ValueError):
        year = "unknown"

    country = normalize_country(row.get('countries_tags')) or "unknown"
    return {"year": str(year), "country": str(country)}


# Row flattener
def extract_columns(obj: Dict[str, Any],
                    paths: Dict[str, List[str]] = COLUMN_PATHS) -> Dict[str, Any]:
    """Flatten raw JSON object onto KEEP_COLS keys"""
    out: Dict[str, Any] = {}
    for col, path in paths.items():
        node: Any = obj
        for key in path:
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                node = None
                break
        out[col] = node
    return out


__all__ = [
    "KEEP_COLS",
    "TARGET",
    "PREDICTORS",
    "DTYPES",
    "normalize_country",
    "make_partition_values",
    "extract_columns",
]
