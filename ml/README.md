# ML — Machine Learning Pipeline

Pipeline de Machine Learning para predicción de temperatura en dos horizontes: largo plazo (tmax diario con datos históricos SENAMHI) y corto plazo (temperatura a 5-15 min con datos streaming de sensores IoT).

## Estructura

| Archivo | Propósito |
|---|---|
| `features.py` | Ingeniería de características para ambos horizontes |
| `train_largo_plazo.py` | Entrena XGBoost con Parquet SENAMHI |
| `train_corto_plazo.py` | Entrena XGBoost con Parquet streaming |
| `predict.py` | Inferencia unificada + listado de modelos |
| `models/` | Modelos .pkl persistidos + métricas JSON |

## Componentes

### `features.py`

- `build_largo_plazo_features(df)` — lags (1,3,5,7 días), rolling means (3,7,14,30 días), rolling std, diferencia vs 30d, tendencia, codificación cíclica de mes/día, estación del año.
- `build_corto_plazo_features(df)` — lags (1,3), rolling mean (5), ratio presión/temperatura, hora cíclica.

### `train_largo_plazo.py`

Entrena 4 modelos (PUNO, AZANGARO, LAMPA, CAPACHICA) con datos SENAMHI. Filtra estaciones con >1000 registros. XGBoost con 500 árboles, `learning_rate=0.05`, `max_depth=7`.

```bash
docker exec clime-jupyter python -m ml.train_largo_plazo
```

### `train_corto_plazo.py`

Entrena 3 modelos (grupo_2, grupo_3, grupo_4) con datos streaming (últimas 10,000 obs por grupo). XGBoost con 300 árboles, `learning_rate=0.05`, `max_depth=5`.

```bash
docker exec clime-jupyter python -m ml.train_corto_plazo
```

### `predict.py`

- `predict_largo_plazo(station, days_ahead=7)` — predicción secuencial auto-regresiva desde el último registro histórico.
- `predict_corto_plazo(grupo, minutes_ahead=5)` — predicción hacia adelante desde el último dato streaming.
- `list_models()` — lista modelos disponibles con tipo, estación y grupo.
- `get_model_info(name)` — métricas guardadas (MAE, RMSE, R²).

### `models/`

Modelos persistidos en formato joblib (`.pkl`) + métricas JSON. Montado como volumen Docker (`../ml:/app/ml`) para persistencia entre recreaciones del contenedor.

## Modelos Entrenados

| Horizonte | Estación | MAE | R² |
|---|---|---|---|
| Largo | PUNO | 0.08°C | 0.994 |
| Largo | AZANGARO | 0.09°C | 0.995 |
| Largo | LAMPA | 0.07°C | 0.997 |
| Largo | CAPACHICA | 0.08°C | 0.994 |
| Corto | grupo_2 (LAMPA) | 0.044°C | 0.992 |
| Corto | grupo_3 (PUNO) | 6.355°C | -1.410 (sensor defectuoso) |
| Corto | grupo_4 (AZANGARO) | 0.053°C | 0.993 |

## Notas

- Los modelos de largo plazo predicen desde el último registro histórico (~2012), no desde la fecha actual. Para predecir 2026, usar datos streaming como punto de partida o re-entrenar con datos actualizados.
- El sensor grupo_3 tiene presión anómala (186 hPa vs ~646 hPa esperado) y temperatura constante ~-0.06°C, lo que impide entrenar un modelo de corto plazo confiable para PUNO.
- `predict_largo_plazo()` usa `STATION_DIR_MAP` en vez de escanear todos los Parquet para reducir tiempo de predicción de ~minutos a ~1.5s.
