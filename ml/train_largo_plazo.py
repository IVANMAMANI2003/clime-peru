import os
import sys
import glob
import json
import numpy as np
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

sys.path.insert(0, os.path.dirname(__file__))
from features import build_largo_plazo_features, get_feature_columns_largo_plazo

WEATHER_DATA = os.path.join(os.path.dirname(__file__), "..", "artifacts", "weather_data")
MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")

STATION_DIR_MAP = {
    "PUNO": "department=PUNO/province=PUNO/district=PUNO",
    "AZANGARO": "department=PUNO/province=AZANGARO/district=AZANGARO",
    "LAMPA": "department=PUNO/province=LAMPA/district=LAMPA",
    "CAPACHICA": "department=PUNO/province=PUNO/district=CAPACHICA",
}


def load_station_data(station_name: str) -> pd.DataFrame:
    key = station_name.upper().strip()
    if key in STATION_DIR_MAP:
        subdir = os.path.join(WEATHER_DATA, STATION_DIR_MAP[key])
        files = glob.glob(os.path.join(subdir, "year=*", "*.parquet"))
    else:
        files = []
        for dir_key, dir_path in STATION_DIR_MAP.items():
            candidate = os.path.join(WEATHER_DATA, dir_path)
            if os.path.exists(candidate):
                files.extend(glob.glob(os.path.join(candidate, "year=*", "*.parquet")))

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
    return pd.concat(frames, ignore_index=True)


def train_model(station_name: str):
    print(f"\n{'='*60}")
    print(f"  ENTRENANDO MODELO LARGO PLAZO: {station_name}")
    print(f"{'='*60}")

    df = load_station_data(station_name)
    if df.empty:
        print(f"[ERROR] No hay datos para {station_name}")
        return None

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset=["date"])
    print(f"  Datos cargados: {len(df)} registros ({df['date'].min().date()} - {df['date'].max().date()})")

    for col in ["tmax", "tmin", "precip"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    initial_len = len(df)
    df = build_largo_plazo_features(df)
    print(f"  Feature engineering: {initial_len} -> {len(df)} registros")

    feature_cols = get_feature_columns_largo_plazo()
    feature_cols = [c for c in feature_cols if c in df.columns]
    X = df[feature_cols]
    y = df["tmax"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    print(f"  Train: {len(X_train)} | Test: {len(X_test)}")

    model = XGBRegressor(
        n_estimators=500, max_depth=6, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        random_state=42, verbosity=0,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    print(f"\n  RESULTADOS:")
    print(f"  MAE:  {mae:.2f} °C")
    print(f"  RMSE: {rmse:.2f} °C")
    print(f"  R2:   {r2:.4f}")

    importance = dict(zip(feature_cols, model.feature_importances_))
    top5 = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\n  Top 5 features:")
    for name, imp in top5:
        print(f"    {name}: {imp:.4f}")

    os.makedirs(MODELS_DIR, exist_ok=True)
    safe_name = station_name.lower().replace(" ", "_")
    model_path = os.path.join(MODELS_DIR, f"largo_plazo_{safe_name}.pkl")
    joblib.dump({"model": model, "features": feature_cols, "station": station_name}, model_path)
    print(f"\n  Modelo guardado: {model_path}")

    metrics = {
        "station": station_name, "type": "largo_plazo",
        "mae": round(float(mae), 3), "rmse": round(float(rmse), 3), "r2": round(float(r2), 4),
        "train_size": len(X_train), "test_size": len(X_test),
        "date_range": f"{df['date'].min().date()} - {df['date'].max().date()}",
        "top_features": [{"name": n, "importance": round(float(i), 4)} for n, i in top5],
    }
    metrics_path = os.path.join(MODELS_DIR, f"largo_plazo_{safe_name}_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    return metrics


if __name__ == "__main__":
    stations = list(STATION_DIR_MAP.keys())

    if len(sys.argv) > 1:
        target = sys.argv[1].upper()
        stations = [s for s in stations if target in s.upper()]

    all_metrics = []
    for station in stations:
        m = train_model(station)
        if m:
            all_metrics.append(m)

    if all_metrics:
        print(f"\n{'='*60}")
        print("  RESUMEN LARGO PLAZO")
        print(f"{'='*60}")
        for m in all_metrics:
            print(f"  {m['station']}: MAE={m['mae']}°C, RMSE={m['rmse']}°C, R2={m['r2']}")
