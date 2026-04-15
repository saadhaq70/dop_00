import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.metrics import mean_squared_error, mean_absolute_error

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data" / "processed"

DATA_PATH = DATA_DIR / "ml_ready_idsp.csv"
FEATURE_JSON_PATH = DATA_DIR / "feature_columns.json"
# Fix: Using '2026-01-01' results in an empty test set since the dataset ends late 2025.
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

    # For neural networks, we need strictly scaled features
    X_train_raw = train_df[feature_cols].values
    y_train_raw = train_df['target'].values
    X_test_raw = test_df[feature_cols].values
    y_test_raw = test_df['target'].values
    
    # Scale inputs
    X_mean = X_train_raw.mean(axis=0)
    X_std = X_train_raw.std(axis=0) + 1e-8
    
    X_train_scaled = (X_train_raw - X_mean) / X_std
    X_test_scaled = (X_test_raw - X_mean) / X_std
    
    test_meta = test_df[['date', 'target']].copy()
    
    return X_train_scaled, y_train_raw, X_test_scaled, y_test_raw, test_meta

# ---------------------------------------------------------
# PYTORCH LSTM MODEL
# ---------------------------------------------------------
class OutbreakLSTM(nn.Module):
    def __init__(self, input_dim):
        super(OutbreakLSTM, self).__init__()
        # Treating our engineered tabular features as a sequence of length 1 
        # (since historical lags are already embedded in each row).
        self.lstm1 = nn.LSTM(input_size=input_dim, hidden_size=64, batch_first=True)
        self.dropout = nn.Dropout(0.2)
        self.lstm2 = nn.LSTM(input_size=64, hidden_size=32, batch_first=True)
        self.fc1 = nn.Linear(32, 16)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(16, 1)

    def forward(self, x):
        # x shape: (batch_size, seq_len=1, features)
        x, _ = self.lstm1(x)
        x = self.dropout(x)
        x, _ = self.lstm2(x)
        
        # Take the output of the last timestep
        x = x[:, -1, :]
        x = self.relu(self.fc1(x))
        x = self.fc2(x)
        return x

# ---------------------------------------------------------
# TRAINING & EVALUATION
# ---------------------------------------------------------
if __name__ == "__main__":
    X_train, y_train, X_test, y_test, test_meta = load_and_split_data()
    
    # Reshape for LSTM: (Samples, Timesteps=1, Features)
    X_train_3d = torch.tensor(X_train).unsqueeze(1).float()
    y_train_t = torch.tensor(y_train).unsqueeze(1).float()
    
    X_test_3d = torch.tensor(X_test).unsqueeze(1).float()
    
    train_dataset = TensorDataset(X_train_3d, y_train_t)
    train_loader = DataLoader(train_dataset, batch_size=128, shuffle=True)
    
    print("\n--- Training LSTM (PyTorch) ---")
    model = OutbreakLSTM(input_dim=X_train.shape[1])
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    
    epochs = 20
    model.train()
    for epoch in range(epochs):
        epoch_loss = 0
        for batch_X, batch_y in train_loader:
            optimizer.zero_grad()
            outputs = model(batch_X)
            loss = criterion(outputs, batch_y)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item() * batch_X.size(0)
        
        print(f"Epoch {epoch+1}/{epochs} - Loss: {epoch_loss/len(train_dataset):.4f}")
        
    print("\n--- Evaluating Predictions ---")
    model.eval()
    with torch.no_grad():
        preds = model(X_test_3d).squeeze().numpy()
        
    preds = np.maximum(0, preds)  # Prevent negative predictions
    
    print(f"RMSE: {np.sqrt(mean_squared_error(y_test, preds)):.2f}")
    print(f"MAE:  {mean_absolute_error(y_test, preds):.2f}")
    
    test_meta['lstm_pred'] = preds
    daily_actual = test_meta.groupby('date')['target'].sum()
    daily_lstm = test_meta.groupby('date')['lstm_pred'].sum()
    
    plt.figure(figsize=(12, 5))
    plt.plot(daily_actual.index, daily_actual.values, label='Actual Cases', color='black', linewidth=2)
    plt.plot(daily_lstm.index, daily_lstm.values, label='LSTM Prediction', color='red', linestyle='-.')
    plt.title('Disease Predictions: LSTM vs Actual (2023-2025)')
    plt.xlabel('Date')
    plt.ylabel('Aggregated Cases')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(SCRIPT_DIR / 'lstm_results.png', dpi=150)
    print(f"Plot saved to {SCRIPT_DIR / 'lstm_results.png'}")
    plt.show()