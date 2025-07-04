import pandas as pd
import os

# set the flag for writing outliers
_WRITE_OUTLIERS = os.getenv("WRITE_OUTLIERS", "FALSE").lower() == "true"

# valid nutrient scores
VALID_GRADES = {'a', 'b', 'c', 'd', 'e'}

# drop non-required columns
DROP_COLS = ['categories_tags', 'brands_tags',
             'countries_tags', 'serving_size', 'created_t',
             'energy_100g', 'fiber_100g']


def clean(df: pd.DataFrame) -> pd.DataFrame:

    df_clean = df.drop(columns=DROP_COLS)

    cols = [col for col in df_clean.columns if col.endswith('_100g')]

    for col in cols:
        # record outliers
        mask = (df_clean[col] > 100) | (df_clean[col] < 0)
        outliers = df_clean[mask]

        # remove outliers
        df_clean = df_clean.loc[~mask]

    df_clean = df_clean.loc[df_clean['nutrition_grade_fr'].isin(VALID_GRADES
                                                                ), :]

    if _WRITE_OUTLIERS and not outliers.empty:
        df_clean.loc[mask].to_parquet(
            f"s3://nutrisage-athena-results-352364310453/logs/outliers/{pd.Timestamp.utcnow():%Y-%m-%d}.parquet")

    return df_clean.reset_index(drop=True)


# export "clean"
__all__ = ["clean"]
