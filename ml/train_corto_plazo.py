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
from features import build_corto_plazo_features, get_feature_columns_corto_plazo

PARQUET_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "artifacts", "parquet_output")
MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")


STATION_MAP = {
    "grupo_2": {"name": "LAMPA", "table": "grupo_2"},
    "grupo_3": {"name": "PUNO", "table": "grupo_3"},
    "grupo_4": {"name": "AZANGARO", "table": "grupo_4"},
}


def load_streaming_data(grupo: str) -> pd.DataFrame:
    base = os.path.join(PARQUET_OUTPUT, grupo)
    if not os.path.exists(base):
        print(f"[ERROR] Directorio no encontrado: {base}")
        return pd.DataFrame()
    files = glob.glob(os.path.join(base, "*.parquet"))
    if not files:
        print(f"[ERROR] No hay parquet en {base}")
        return pd.DataFrame()
    print(f"  Leyendo {len(files)} archivos parquet...")
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def train_model(grupo: str):
    info = STATION_MAP.get(grupo, {"name": grupo, "table": grupo})
    station_name = info["name"]

    print(f"\n{'='*60}")
    print(f"  ENTRENANDO MODELO CORTO PLAZO: {grupo} ({station_name})")
    print(f"{'='*60}")

    df = load_streaming_data(grupo)
    if df.empty:
        print(f"[ERROR] No hay datos para {grupo}")
        return None

    for col in ["temperatura", "humedad", "presion"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "ts" not in df.columns:
        if "created_at" in df.columns:
            df["ts"] = df["created_at"]
        else:
            print("[ERROR] No hay columna ts ni created_at")
            return pd.DataFrame()

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts", "temperatura"]).sort_values("ts").drop_duplicates(subset=["ts"])
    print(f"  Datos cargados: {len(df)} registros ({df['ts'].min()} - {df['ts'].max()})")

    df = build_corto_plazo_features(df)
    print(f"  Despues de feature engineering: {len(df)} registros")

    feature_cols = get_feature_columns_corto_plazo()
    feature_cols = [c for c in feature_cols if c in df.columns]

    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        print(f"  [WARN] Features faltantes: {missing}")
        feature_cols = [c for c in feature_cols if c in df.columns]

    if len(feature_cols) < 5:
        print(f"[ERROR] Muy pocas features disponibles: {len(feature_cols)}")
        return None

    X = df[feature_cols]
    y = df["temperatura"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    print(f"  Train: {len(X_train)} | Test: {len(X_test)}")

    model = XGBRegressor(
        n_estimators=300, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        random_state=42, verbosity=0,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    print(f"\n  RESULTADOS:")
    print(f"  MAE:  {mae:.3f} °C")
    print(f"  RMSE: {rmse:.3f} °C")
    print(f"  R²:   {r2:.4f}")

    importance = dict(zip(feature_cols, model.feature_importances_))
    top5 = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\n  Top 5 features:")
    for name, imp in top5:
        print(f"    {name}: {imp:.4f}")

    os.makedirs(MODELS_DIR, exist_ok=True)
    model_path = os.path.join(MODELS_DIR, f"corto_plazo_{grupo}.pkl")
    joblib.dump({"model": model, "features": feature_cols, "station": info["name"], "grupo": grupo}, model_path)
    print(f"\n  Modelo guardado: {model_path}")

    metrics = {
        "station": station_name, "grupo": grupo, "type": "corto_plazo",
        "mae": round(mae, 3), "rmse": round(rmse, 3), "r2": round(r2, 4),
        "train_size": len(X_train), "test_size": len(X_test),
        "date_range": f"{df['ts'].min()} - {df['ts'].max()}",
        "top_features": [{"name": n, "importance": round(float(i), 4)} for n, i in top5],
    }
    metrics_path = os.path.join(MODELS_DIR, f"corto_plazo_{grupo}_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    return metrics


if __name__ == "__main__":
    if len(sys.argv) > 1:
        groups = [g for g in sys.argv[1:] if g in STATION_MAP]
    else:
        groups = list(STATION_MAP.keys())

    all_metrics = []
    for grupo in groups:
        m = train_model(grupo)
        if m:
            all_metrics.append(m)

    if all_metrics:
        print(f"\n{'='*60}")
        print("  RESUMEN CORTO PLAZO")
        print(f"{'='*60}")
        for m in all_metrics:
            print(f"  {m['station']} ({m['grupo']}): MAE={m['mae']}°C, RMSE={m['rmse']}°C, R²={m['r2']}")
