import numpy as np
import pandas as pd


def add_cyclical_features(df: pd.DataFrame) -> pd.DataFrame:
    if "month" in df.columns:
        df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
        df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    if "day" in df.columns:
        df["day_sin"] = np.sin(2 * np.pi * df["day"] / 365)
        df["day_cos"] = np.cos(2 * np.pi * df["day"] / 365)
    if "hour" in df.columns:
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    if "minute" in df.columns:
        df["minute_sin"] = np.sin(2 * np.pi * df["minute"] / 60)
        df["minute_cos"] = np.cos(2 * np.pi * df["minute"] / 60)
    return df


def add_lag_features(df: pd.DataFrame, col: str, lags: list) -> pd.DataFrame:
    for lag in lags:
        df[f"{col}_lag_{lag}"] = df[col].shift(lag)
    return df


def add_rolling_features(df: pd.DataFrame, col: str, windows: list) -> pd.DataFrame:
    for w in windows:
        df[f"{col}_roll_mean_{w}"] = df[col].rolling(w, min_periods=1).mean()
        df[f"{col}_roll_std_{w}"] = df[col].rolling(w, min_periods=1).std().fillna(0)
    return df


def add_diff_features(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df[f"{col}_diff_1"] = df[col].diff(1)
    df[f"{col}_diff_7"] = df[col].diff(7)
    return df


def build_largo_plazo_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["day_of_year"] = df["date"].dt.dayofyear
    df = add_cyclical_features(df)
    df = add_lag_features(df, "tmax", [1, 2, 3, 7, 14, 30])
    df = add_lag_features(df, "tmin", [1, 7, 30])
    df = add_lag_features(df, "precip", [1, 3, 7])
    df = add_rolling_features(df, "tmax", [7, 14, 30])
    df = add_rolling_features(df, "tmin", [7, 30])
    df = add_rolling_features(df, "precip", [7, 30])
    df = add_diff_features(df, "tmax")
    df["tmax_tmin_diff"] = df["tmax"] - df["tmin"]
    df["precip_accum_7"] = df["precip"].rolling(7, min_periods=1).sum()
    df["precip_accum_30"] = df["precip"].rolling(30, min_periods=1).sum()
    df = df.dropna()
    return df


def build_corto_plazo_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("ts").copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df["hour"] = df["ts"].dt.hour
    df["minute"] = df["ts"].dt.minute
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["minute_sin"] = np.sin(2 * np.pi * df["minute"] / 60)
    df["minute_cos"] = np.cos(2 * np.pi * df["minute"] / 60)
    for col in ["temperatura", "humedad", "presion"]:
        if col in df.columns:
            df = add_lag_features(df, col, [1, 3, 5, 10, 15])
            df = add_rolling_features(df, col, [5, 15, 30])
            df[f"{col}_diff_1"] = df[col].diff(1)
            df[f"{col}_rate"] = df[col].diff(1).rolling(5, min_periods=1).mean()
    if "temperatura" in df.columns and "humedad" in df.columns:
        df["temp_hum_ratio"] = df["temperatura"] / (df["humedad"] + 1)
    if "temperatura" in df.columns and "presion" in df.columns:
        df["temp_pres_ratio"] = df["temperatura"] / (df["presion"] + 1)
    df = df.dropna()
    return df


def get_feature_columns_largo_plazo() -> list:
    return [
        "month_sin", "month_cos", "day_sin", "day_cos", "day_of_year",
        "tmax_lag_1", "tmax_lag_2", "tmax_lag_3", "tmax_lag_7", "tmax_lag_14", "tmax_lag_30",
        "tmin_lag_1", "tmin_lag_7", "tmin_lag_30",
        "precip_lag_1", "precip_lag_3", "precip_lag_7",
        "tmax_roll_mean_7", "tmax_roll_std_7", "tmax_roll_mean_14", "tmax_roll_mean_30",
        "tmin_roll_mean_7", "tmin_roll_mean_30",
        "precip_roll_mean_7", "precip_roll_mean_30",
        "tmax_diff_1", "tmax_diff_7",
        "tmax_tmin_diff", "precip_accum_7", "precip_accum_30",
    ]


def get_feature_columns_corto_plazo() -> list:
    cols = []
    for var in ["temperatura", "humedad", "presion"]:
        for lag in [1, 3, 5, 10, 15]:
            cols.append(f"{var}_lag_{lag}")
        for w in [5, 15, 30]:
            cols.append(f"{var}_roll_mean_{w}")
            cols.append(f"{var}_roll_std_{w}")
        cols.append(f"{var}_diff_1")
        cols.append(f"{var}_rate")
    cols.extend(["hour_sin", "hour_cos", "minute_sin", "minute_cos",
                  "temp_hum_ratio", "temp_pres_ratio"])
    return cols
