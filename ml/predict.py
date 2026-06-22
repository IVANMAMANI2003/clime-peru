import os
import glob
import numpy as np
import pandas as pd
import joblib

MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")
WEATHER_DATA = os.path.join(os.path.dirname(__file__), "..", "artifacts", "weather_data")
PARQUET_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "artifacts", "parquet_output")

STATION_DIR_MAP = {
    "PUNO": "department=PUNO/province=PUNO/district=PUNO",
    "AZANGARO": "department=PUNO/province=AZANGARO/district=AZANGARO",
    "LAMPA": "department=PUNO/province=LAMPA/district=LAMPA",
    "CAPACHICA": "department=PUNO/province=PUNO/district=CAPACHICA",
}

sys_path = os.path.dirname(__file__)
import sys
sys.path.insert(0, sys_path)
from features import (
    build_largo_plazo_features, get_feature_columns_largo_plazo,
    build_corto_plazo_features, get_feature_columns_corto_plazo,
)


def load_model(model_name: str) -> dict:
    path = os.path.join(MODELS_DIR, model_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Modelo no encontrado: {path}")
    return joblib.load(path)


def list_models() -> list:
    if not os.path.exists(MODELS_DIR):
        return []
    models = []
    for f in sorted(os.listdir(MODELS_DIR)):
        if f.endswith(".pkl"):
            name = f.replace(".pkl", "")
            mtype = "largo_plazo" if "largo" in name else "corto_plazo"
            station = name.split("_", 2)[-1] if "grupo" not in name else name.replace("corto_plazo_", "")
            grupo = None
            if mtype == "corto_plazo" and "grupo" in name:
                grupo = name.replace("corto_plazo_", "")
            models.append({"file": f, "name": name, "type": mtype, "station": station, "grupo": grupo})
    return models


def predict_largo_plazo(station_name: str, days_ahead: int = 7) -> pd.DataFrame:
    safe_name = station_name.lower().replace(" ", "_")
    data = load_model(f"largo_plazo_{safe_name}.pkl")
    model = data["model"]
    feature_cols = data["features"]

    key = station_name.upper().strip()
    subdir = STATION_DIR_MAP.get(key)
    if not subdir:
        return pd.DataFrame()
    dir_path = os.path.join(WEATHER_DATA, subdir)
    files = glob.glob(os.path.join(dir_path, "year=*", "*.parquet"))
    frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if "station_name" in df.columns:
                df = df[df["station_name"].str.upper() == key]
            if not df.empty:
                frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()

    hist = pd.concat(frames, ignore_index=True)
    hist["date"] = pd.to_datetime(hist["date"])
    hist = hist.sort_values("date").drop_duplicates(subset=["date"])
    for col in ["tmax", "tmin", "precip"]:
        if col in hist.columns:
            hist[col] = pd.to_numeric(hist[col], errors="coerce")

    predictions = []
    current = hist.copy()
    for i in range(days_ahead):
        feat_df = build_largo_plazo_features(current)
        if feat_df.empty:
            break
        last_row = feat_df.iloc[[-1]]
        available = [c for c in feature_cols if c in last_row.columns]
        if len(available) < len(feature_cols) * 0.5:
            break
        pred_tmax = model.predict(last_row[feature_cols])[0]
        next_date = current["date"].max() + pd.Timedelta(days=1)
        pred_row = {
            "date": next_date,
            "predicted_tmax": round(pred_tmax, 1),
        }
        predictions.append(pred_row)
        new_row = pd.DataFrame([{
            "date": next_date, "station_name": station_name,
            "month": next_date.month, "day": next_date.day,
            "tmax": pred_tmax, "tmin": pred_tmax - 5, "precip": 0,
        }])
        current = pd.concat([current, new_row], ignore_index=True)

    recent = hist.tail(30)[["date", "tmax"]].copy()
    recent["type"] = "historical"
    recent = recent.rename(columns={"tmax": "value"})

    if predictions:
        pred_df = pd.DataFrame(predictions)
        pred_df["type"] = "predicted"
        pred_df = pred_df.rename(columns={"predicted_tmax": "value"})
        result = pd.concat([
            recent[["date", "value", "type"]],
            pred_df[["date", "value", "type"]],
        ], ignore_index=True)
    else:
        result = recent[["date", "value", "type"]]

    return result


def predict_corto_plazo(grupo: str, minutes_ahead: int = 5) -> pd.DataFrame:
    data = load_model(f"corto_plazo_{grupo}.pkl")
    model = data["model"]
    feature_cols = data["features"]

    base = os.path.join(PARQUET_OUTPUT, grupo)
    files = sorted(glob.glob(os.path.join(base, "*.parquet")))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files[-20:]:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    for col in ["temperatura", "humedad", "presion"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "ts" not in df.columns and "created_at" in df.columns:
        df["ts"] = df["created_at"]

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts", "temperatura"]).sort_values("ts").drop_duplicates(subset=["ts"])

    predictions = []
    current = df.copy()
    for i in range(minutes_ahead):
        feat_df = build_corto_plazo_features(current)
        if feat_df.empty:
            break
        last_row = feat_df.iloc[[-1]]
        available = [c for c in feature_cols if c in last_row.columns]
        if len(available) < len(feature_cols) * 0.5:
            break
        pred_temp = model.predict(last_row[feature_cols])[0]
        next_ts = current["ts"].max() + pd.Timedelta(minutes=1)
        last_hum = current["humedad"].iloc[-1] if "humedad" in current.columns else 50
        last_pres = current["presion"].iloc[-1] if "presion" in current.columns else 650
        new_row = pd.DataFrame([{
            "ts": next_ts, "temperatura": pred_temp,
            "humedad": last_hum, "presion": last_pres,
        }])
        predictions.append({"ts": next_ts, "value": round(pred_temp, 2), "type": "predicted"})
        current = pd.concat([current, new_row], ignore_index=True)

    recent = df.tail(30)[["ts", "temperatura"]].copy()
    recent["type"] = "actual"
    recent = recent.rename(columns={"temperatura": "value", "ts": "ts"})

    if predictions:
        pred_df = pd.DataFrame(predictions)
        result = pd.concat([
            recent[["ts", "value", "type"]],
            pred_df[["ts", "value", "type"]],
        ], ignore_index=True)
    else:
        result = recent[["ts", "value", "type"]]

    return result


import json


def get_model_info(model_name: str) -> dict:
    metrics_path = os.path.join(MODELS_DIR, model_name.replace(".pkl", "_metrics.json"))
    if os.path.exists(metrics_path):
        with open(metrics_path) as f:
            return json.load(f)
    return {"error": "Metrics not found"}
