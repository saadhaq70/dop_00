import numpy as np
import pandas as pd


def add_time_features(df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
    """
    Extracts time-based features from the date column.

    - Cyclical sin/cos encoding for month and week_of_year avoids the
      discontinuity problem (model sees Dec→Jan as a small step, not a
      jump from 12→1).
    - Raw month and week_of_year integers are retained alongside cyclical
      features so tree-based models (XGBoost) can also use them directly.
    - is_monsoon is a pan-India indicator (June–September). Regional onset
      differs (Kerala: June; Northeast: April) but this is a useful baseline.
    - is_winter covers Nov–Feb when respiratory and vector-borne peaks differ.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    month        = df[date_col].dt.month
    week_of_year = df[date_col].dt.isocalendar().week.astype(int)
    year         = df[date_col].dt.year

    # ── Raw integer features (useful for tree models) ──────────────────────
    df['month']        = month
    df['week_of_year'] = week_of_year
    df['year']         = year

    # ── Cyclical encoding (useful for linear/neural models) ────────────────
    # Month: period = 12
    df['month_sin'] = np.sin(2 * np.pi * month / 12)
    df['month_cos'] = np.cos(2 * np.pi * month / 12)

    # Week of year: period = 52
    df['week_sin'] = np.sin(2 * np.pi * week_of_year / 52)
    df['week_cos'] = np.cos(2 * np.pi * week_of_year / 52)

    # ── Seasonal binary flags ──────────────────────────────────────────────
    df['is_monsoon'] = month.isin([6, 7, 8, 9]).astype(int)
    df['is_winter']  = month.isin([11, 12, 1, 2]).astype(int)

    return df


def add_lag_features(
    df: pd.DataFrame,
    group_cols: list,
    target_col: str = 'total_cases',
) -> pd.DataFrame:
    """
    Creates historical lag features grouped by state and disease.
    Lags: 1, 2, 4, 8 weeks — representing last week, fortnight, month, 2-month.
    NaN values from insufficient history are left intentionally; they will be
    dropped in the missing-value handling step of build_dataset.py.
    """
    df = df.copy()
    for lag in [1, 2, 4, 8]:
        df[f'lag_{lag}'] = df.groupby(group_cols)[target_col].shift(lag)
    return df


def add_rolling_features(
    df: pd.DataFrame,
    group_cols: list,
    target_col: str,           # no default — must be explicit to avoid leakage
) -> pd.DataFrame:
    """
    Creates rolling-window statistics grouped by state and disease.

    Rolling is applied to `target_col` which should already be shifted by 1
    before being passed here (see build_dataset.py) to prevent data leakage.

    min_periods behaviour:
      - mean, max: min_periods=1  (sensible with even 1 observation)
      - std:       min_periods=2  (std is undefined for a single point,
                                   so NaN is left in place rather than
                                   filling with a misleading 0)
    """
    df = df.copy()
    grouped = df.groupby(group_cols)[target_col]

    df['rolling_mean_4'] = grouped.transform(
        lambda x: x.rolling(window=4, min_periods=1).mean()
    )
    df['rolling_mean_8'] = grouped.transform(
        lambda x: x.rolling(window=8, min_periods=1).mean()
    )
    df['rolling_std_4'] = grouped.transform(
        lambda x: x.rolling(window=4, min_periods=2).std()
        # NaN for groups with < 2 observations; intentionally NOT filled with 0
    )
    df['rolling_max_4'] = grouped.transform(
        lambda x: x.rolling(window=4, min_periods=1).max()
    )

    return df


def encode_categorical_features(df: pd.DataFrame, cat_cols: list) -> pd.DataFrame:
    """
    Applies One-Hot Encoding to categorical columns.
    Boolean dummy columns are cast to int (0/1) for model compatibility.

    NOTE: Call this AFTER build_target() so that group_cols are still
    present when computing the target shift. See build_dataset.py.
    """
    df = pd.get_dummies(df, columns=cat_cols, drop_first=False)
    bool_cols = [col for col in df.columns if df[col].dtype == bool]
    df[bool_cols] = df[bool_cols].astype(int)
    return df


def build_target(
    df: pd.DataFrame,
    group_cols: list,
    target_col: str = 'total_cases',
) -> pd.DataFrame:
    """
    Creates the ML target variable: next week's case count per (state, disease).
    The last observation of each group has no future label and is dropped.

    Call this BEFORE encode_categorical_features so that group_cols
    (state, disease) still exist for the groupby operation.
    """
    df = df.copy()
    df['target'] = df.groupby(group_cols)[target_col].shift(-1)
    df = df.dropna(subset=['target']).reset_index(drop=True)
    return df