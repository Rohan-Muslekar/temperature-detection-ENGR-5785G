import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(SCRIPT_DIR, "..", "data", "oshawa_weather_2024.csv")
MODEL_PATH = os.path.join(SCRIPT_DIR, "model.pkl")

FEATURE_COLUMNS = {
    "Temp (°C)": "temp_c",
    "Dew Point Temp (°C)": "dew_point_c",
    "Rel Hum (%)": "rel_hum_pct",
    "Wind Spd (km/h)": "wind_spd_kmh",
    "Stn Press (kPa)": "stn_press_kpa",
}


def load_and_prepare_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["Date/Time (LST)"])
    df = df.rename(columns=FEATURE_COLUMNS)
    df = df[["Date/Time (LST)"] + list(FEATURE_COLUMNS.values())].copy()
    df = df.dropna(subset=["temp_c"])
    df = df.sort_values("Date/Time (LST)").reset_index(drop=True)
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df["temp_lag_1"] = df["temp_c"].shift(1)
    df["temp_lag_2"] = df["temp_c"].shift(2)
    df["temp_lag_3"] = df["temp_c"].shift(3)
    df["target_temp"] = df["temp_c"].shift(-1)
    df = df.dropna().reset_index(drop=True)
    return df


def train_model(df: pd.DataFrame) -> tuple:
    feature_cols = [
        "temp_c", "dew_point_c", "rel_hum_pct",
        "wind_spd_kmh", "stn_press_kpa",
        "temp_lag_1", "temp_lag_2", "temp_lag_3",
    ]
    X = df[feature_cols]
    y = df["target_temp"]

    split_idx = int(len(df) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    return model, mae, rmse, r2


def main():
    print("Loading data...")
    df = load_and_prepare_data(DATA_PATH)
    print(f"Loaded {len(df)} rows")

    print("Engineering features...")
    df = engineer_features(df)
    print(f"Training on {len(df)} samples (after dropping NaN)")

    print("Training Random Forest...")
    model, mae, rmse, r2 = train_model(df)

    print(f"\n--- Model Performance (Test Set) ---")
    print(f"MAE:  {mae:.2f} °C")
    print(f"RMSE: {rmse:.2f} °C")
    print(f"R²:   {r2:.4f}")

    joblib.dump(model, MODEL_PATH)
    print(f"\nModel saved to {MODEL_PATH}")


if __name__ == "__main__":
    main()
