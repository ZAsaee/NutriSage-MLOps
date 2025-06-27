"""
Nutrisage schema & helpers
_____________________________________________________________________________________
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

# locate and read the yaml file listing selected features
if __package__:
    _yaml_path = files(__package__).joinpath("candidate-columns.yml")
elif __file__:
    _yaml_path = Path(__file__).with_name("candidate-columns.yml")

# get columns
KEEP_COLS: List[str] = yaml.safe_load(open(_yaml_path))["columns"]

TARGET = "nutrition_grade_fr"
PREDICTORS = [c for c in KEEP_COLS if c != TARGET]

# Nutrient-style fields live under the "nutrimets" sub-dict
NUTRIMENTS_KEY = [k for k in KEEP_COLS if k.endswith('_100g')]

# map canonical column name to list path inside JSON object
COLUMN_PATHS: Dict[str, List[str]] = {
    col: (["nutriments", col] if col in NUTRIMENTS_KEY else [col])
    for col in KEEP_COLS
}

# Optional Pandas dtypes (floats for nutrients)
DTYPES: Dict[str, Any] = {k: np.float32 for k in NUTRIMENTS_KEY}

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
