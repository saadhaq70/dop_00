import os
import json
import pandas as pd
from typing import Tuple
from pathlib import Path

# ---------------------------------------------------------
# 1. MODULE IMPORTS
# ---------------------------------------------------------
from features import (
    add_time_features,
    add_lag_features,
    add_rolling_features,
    encode_categorical_features,
    build_target,
)

# ---------------------------------------------------------
# 2. PATH CONFIGURATION
# All paths are relative to this script's location, not CWD.
# ---------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

INPUT_PATH         = PROJECT_ROOT / "data" / "processed" / "idsp_disease_data.csv"
OUTPUT_DIR         = PROJECT_ROOT / "data" / "processed"
FINAL_DATASET_PATH = OUTPUT_DIR / "ml_ready_idsp.csv"
TRAIN_DATASET_PATH = OUTPUT_DIR / "train_idsp.csv"
TEST_DATASET_PATH  = OUTPUT_DIR / "test_idsp.csv"
FEATURE_LIST_PATH  = OUTPUT_DIR / "feature_columns.json"

# ---------------------------------------------------------
# 3. PIPELINE FUNCTIONS
# ---------------------------------------------------------

def load_and_clean_data(filepath: Path) -> pd.DataFrame:
    print(f"Loading data from {filepath}...")
    if not filepath.exists():
        raise FileNotFoundError(f"Missing input dataset at: {filepath}")

    df = pd.read_csv(filepath)
    df['date'] = pd.to_datetime(df['date'])

    # Enforce spatio-temporal sort and remove duplicates to prevent leakage
    initial_len = len(df)
    df = df.drop_duplicates(subset=['state', 'disease', 'date'])
    df = df.sort_values(by=['state', 'disease', 'date']).reset_index(drop=True)

    if len(df) < initial_len:
        print(f"  Removed {initial_len - len(df)} duplicate records.")

    print(f"  Loaded {len(df):,} records | "
          f"{df['state'].nunique()} states | {df['disease'].nunique()} diseases")
    return df


def apply_feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ['state', 'disease']

    print("Extracting time features...")
    df = add_time_features(df, date_col='date')

    print("Extracting lag features...")
    df = add_lag_features(df, group_cols=group_cols, target_col='total_cases')

    print("Extracting rolling features (leakage-safe)...")
    # Shift by 1 before rolling so the current week doesn't leak into its own
    # rolling statistics. Rolling is computed on the shifted series, not raw cases.
    df['cases_shifted_1'] = df.groupby(group_cols)['total_cases'].shift(1)
    df = add_rolling_features(df, group_cols=group_cols, target_col='cases_shifted_1')
    df = df.drop(columns=['cases_shifted_1'])

    # IMPORTANT: build_target MUST run before encode_categorical_features.
    # OHE replaces 'state' and 'disease' with dummy columns; the groupby
    # inside build_target requires the original string columns to exist.
    print("Building target variable (next week's case count)...")
    df = build_target(df, group_cols=group_cols, target_col='total_cases')

    print("Applying one-hot encoding...")
    df = encode_categorical_features(df, cat_cols=group_cols)

    # Drop raw source columns that are not model features:
    # total_cases  → already encoded in lags/rolling/target; keep as reference? No — drop.
    # total_deaths → useful signal; keep it.
    # date         → keep it for the temporal split; will be dropped from saved CSVs.
    cols_to_drop = [c for c in ['total_cases'] if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows that have NaN in any lag or rolling column.

    Notes:
    - build_target() already removed rows where target is NaN (last obs of each group).
    - Lag NaNs arise from the first N weeks of each (state, disease) group.
    - rolling_std_4 NaN arises from groups with < 2 observations (min_periods=2).
    - We do NOT check 'target' here again to avoid double-counting in the printout.
    """
    print("Handling missing values (dropping initialization gaps)...")
    lag_rolling_cols = [c for c in df.columns if 'lag' in c or 'rolling' in c]

    initial_len = len(df)
    df = df.dropna(subset=lag_rolling_cols).reset_index(drop=True)

    print(f"  Dropped {initial_len - len(df):,} rows lacking full lag/rolling history.")
    print(f"  Remaining: {len(df):,} ML-ready rows.")
    return df


def finalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder columns so 'target' is last, save feature list for inference,
    and return a version WITH 'date' still present (needed for the split).
    'date' is removed only when writing the final CSVs.
    """
    # Columns that are not features: date (index/split key) and target (label)
    non_feature_cols = {'date', 'target'}
    feature_cols = [c for c in df.columns if c not in non_feature_cols]

    # Final column order: date | features | target
    df = df[['date'] + feature_cols + ['target']]

    # Save feature schema (excluding date and target) for inference-time matching
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(FEATURE_LIST_PATH, 'w') as f:
        json.dump(feature_cols, f, indent=4)
    print(f"  Saved feature schema ({len(feature_cols)} features) → {FEATURE_LIST_PATH}")

    return df


def temporal_train_test_split(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Temporal split on the 'date' column. Uses strict chronological ordering
    so no future data leaks into training.

    'date' is dropped from both splits before saving — it is a bookkeeping
    column, not a model feature.
    """
    print("Executing temporal train/test split (cutoff: 2023-01-01)...")

    if 'date' not in df.columns:
        raise KeyError("'date' column missing — cannot perform temporal split.")

    cutoff_date = pd.Timestamp('2023-01-01')
    train_df = df[df['date'] < cutoff_date].drop(columns=['date']).reset_index(drop=True)
    test_df  = df[df['date'] >= cutoff_date].drop(columns=['date']).reset_index(drop=True)

    print(f"  Train: {len(train_df):,} rows  (< 2023-01-01)")
    print(f"  Test : {len(test_df):,} rows  (>= 2023-01-01)")
    return train_df, test_df


def main():
    print("=" * 50)
    print("  Sentinel STAI: ML Dataset Builder")
    print("=" * 50)

    df = load_and_clean_data(INPUT_PATH)
    df = apply_feature_engineering(df)
    df = handle_missing_values(df)
    df = finalize_schema(df)

    # Save ML-ready dataset (with date column for reference/debugging)
    df.to_csv(FINAL_DATASET_PATH, index=False)
    print(f"\n✅ ML-ready dataset → {FINAL_DATASET_PATH}")
    print(f"   Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # Split — date is dropped inside temporal_train_test_split
    train_df, test_df = temporal_train_test_split(df)
    train_df.to_csv(TRAIN_DATASET_PATH, index=False)
    test_df.to_csv(TEST_DATASET_PATH, index=False)
    print("✅ Train/Test splits exported.")


if __name__ == "__main__":
    main()