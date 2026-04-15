import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data" / "processed"

DATA_PATH = DATA_DIR / "ml_ready_idsp.csv"
FEATURE_JSON_PATH = DATA_DIR / "feature_columns.json"
# Fix: The dataset spans until late 2025. A cutoff of 2026 results in an empty test set.
# Use 2023-01-01 to match your temporal_train_test_split logic from build_dataset.py.
CUTOFF_DATE = '2023-01-01'

# ---------------------------------------------------------
# DATA PIPELINE
# ---------------------------------------------------------
def load_and_split_data():
    print(f"Loading ML-ready data from {DATA_PATH}...")
    df = pd.read_csv(DATA_PATH, parse_dates=['date'])
    
    with open(FEATURE_JSON_PATH, 'r') as f:
        feature_cols = json.load(f)
        
    train_df = df[df['date'] < CUTOFF_DATE].copy()
    test_df = df[df['date'] >= CUTOFF_DATE].copy()
    
    print(f"Train Set: {len(train_df):,} rows")
    print(f"Test Set: {len(test_df):,} rows")

    if len(test_df) == 0:
        raise ValueError(f"Test set is empty! Check CUTOFF_DATE. Data range: {df['date'].min().date()} to {df['date'].max().date()}")

    X_train = train_df[feature_cols]
    y_train = train_df['target']
    X_test = test_df[feature_cols]
    y_test = test_df['target']
    
    test_meta = test_df[['date', 'target']].copy()
    
    return X_train, y_train, X_test, y_test, test_meta

# ---------------------------------------------------------
# TRAINING & EVALUATION
# ---------------------------------------------------------
if __name__ == "__main__":
    X_train, y_train, X_test, y_test, test_meta = load_and_split_data()
    
    print("\n--- Training XGBoost ---")
    xgb = XGBRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
    xgb.fit(X_train, y_train)
    
    print("\n--- Evaluating Predictions ---")
    preds = xgb.predict(X_test)
    preds = np.maximum(0, preds)  # Prevent negative predictions
    
    print(f"RMSE: {np.sqrt(mean_squared_error(y_test, preds)):.2f}")
    print(f"MAE:  {mean_absolute_error(y_test, preds):.2f}")
    
    test_meta['xgb_pred'] = preds
    daily_actual = test_meta.groupby('date')['target'].sum()
    daily_xgb = test_meta.groupby('date')['xgb_pred'].sum()
    
    plt.figure(figsize=(12, 5))
    plt.plot(daily_actual.index, daily_actual.values, label='Actual Cases', color='black', linewidth=2)
    plt.plot(daily_xgb.index, daily_xgb.values, label='XGBoost Prediction', color='blue', linestyle='--')
    plt.title('Disease Predictions: XGBoost vs Actual (2023-2025)')
    plt.xlabel('Date')
    plt.ylabel('Aggregated Cases')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(SCRIPT_DIR / 'xgboost_results.png', dpi=150)
    print(f"Plot saved to {SCRIPT_DIR / 'xgboost_results.png'}")
    plt.show()