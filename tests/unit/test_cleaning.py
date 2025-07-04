import pandas as pd
import pytest
from data_prep.cleaning import clean


def test_cleaning_basic(monkeypatch):

    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda *a, **k: None)

    df = pd.DataFrame({
        'categories_tags': ['x', 'y'],
        'brands_tags': ['a', 'b'],
        'countries_tags': ['ca', 'us'],
        'serving_size': [10, 20],
        'created_t': [1, 2],
        'energy_100g': [50, 60],
        'fiber_100g': [3, 4],
        'sugar_100g': [50, 150],     # >100 → should drop row 1
        'protein_100g': [10, -5],    # <0   → should drop row 1
        'nutrition_grade_fr': ['a', '?']
    })

    cleaned = clean(df)

    # unwanted columns gone
    assert 'categories_tags' not in cleaned.columns
    # outlier rows removed
    assert cleaned.shape[0] == 1
    # target values valid
    assert set(cleaned['nutrition_grade_fr']) <= {'a', 'b', 'c', 'd', 'e'}
