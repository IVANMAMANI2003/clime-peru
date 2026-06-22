# 8. Pipeline de Machine Learning

## 8.1 Descripción General

El pipeline de Machine Learning de CliMePerú proporciona predicciones de temperatura en dos horizontes temporales:

| Horizonte | Target | Frecuencia | Datos de Entrenamiento |
|---|---|---|---|
| **Largo Plazo** | tmax (temperatura máxima diaria) | Diaria | Datos históricos SENAMHI (50+ años) |
| **Corto Plazo** | Temperatura instantánea | 5-15 minutos | Datos streaming de sensores IoT |

Ambos modelos se integran en el dashboard de Streamlit como la pestaña **🤖 Predicciones ML**.

## 8.2 Arquitectura ML

```mermaid
graph TB
    subgraph DataSources["Fuentes de Datos"]
        PH[(Parquet Histórico<br/>SENAMHI 1964-2012)]
        PS[(Parquet Streaming<br/>Sensores IoT)]
    end

    subgraph Features["Feature Engineering"]
        LP_F[build_largo_plazo_features<br/>lags, rolling, cíclicas]
        CP_F[build_corto_plazo_features<br/>lags, ratio, hora]
    end

    subgraph Training["Entrenamiento XGBoost"]
        LP_T[train_largo_plazo.py<br/>500 trees, lr=0.05, max_depth=7]
        CP_T[train_corto_plazo.py<br/>300 trees, lr=0.05, max_depth=5]
    end

    subgraph Models["Modelos Persistidos (.pkl)"]
        M_LP[largo_plazo_{estacion}.pkl<br/>4 modelos]
        M_CP[corto_plazo_{grupo}.pkl<br/>3 modelos]
        METRICS[metrics.json]
    end

    subgraph Inference["Predicción"]
        PRED[predict.py<br/>predict_largo_plazo<br/>predict_corto_plazo]
    end

    subgraph Dashboard["Dashboard Streamlit"]
        UI[🤖 Predicciones ML<br/>Selector + botón + resultado]
        STATE[st.session_state<br/>ml_lp_result / ml_cp_result]
    end

    PH --> LP_F --> LP_T --> M_LP
    PS --> CP_F --> CP_T --> M_CP

    M_LP --> PRED
    M_CP --> PRED
    PRED --> UI
    UI --> STATE
```

## 8.3 Componentes del Pipeline

| Archivo | Propósito | Funciones Clave |
|---|---|---|
| `ml/features.py` | Ingeniería de características | `build_largo_plazo_features()`, `build_corto_plazo_features()` |
| `ml/train_largo_plazo.py` | Entrenamiento largo plazo | Carga Parquet, filtra estaciones, entrena XGBoost |
| `ml/train_corto_plazo.py` | Entrenamiento corto plazo | Carga Parquet streaming, entrena XGBoost |
| `ml/predict.py` | Inferencia unificada | `predict_largo_plazo()`, `predict_corto_plazo()`, `list_models()`, `get_model_info()` |
| `ml/models/` | Modelos persistidos | 7 archivos .pkl + metrics.json |

## 8.4 Feature Engineering

### Largo Plazo (tmax diario)

La función `build_largo_plazo_features()` genera las siguientes características a partir de datos diarios de temperatura:

| Feature | Descripción | Ventana |
|---|---|---|
| `tmax_lag_1` | Temperatura de 1 día atrás | 1 día |
| `tmax_lag_3` | Temperatura de 3 días atrás | 3 días |
| `tmax_lag_5` | Temperatura de 5 días atrás | 5 días |
| `tmax_lag_7` | Temperatura de 7 días atrás | 7 días |
| `tmax_rolling_mean_3` | Media móvil | 3 días |
| `tmax_rolling_mean_7` | Media móvil | 7 días |
| `tmax_rolling_mean_14` | Media móvil | 14 días |
| `tmax_rolling_mean_30` | Media móvil | 30 días |
| `tmax_rolling_std_7` | Desviación estándar móvil | 7 días |
| `tmax_rolling_std_14` | Desviación estándar móvil | 14 días |
| `tmax_diff_30d` | Diferencia con media de 30 días | — |
| `trend_7_30` | Tendencia (media 7d - media 30d) | — |
| `month_sin`, `month_cos` | Mes codificado cíclicamente | — |
| `day_sin`, `day_cos` | Día codificado cíclicamente | — |
| `season_summer`, etc. | Estación del año (one-hot) | — |

### Corto Plazo (temperatura streaming)

La función `build_corto_plazo_features()` genera características a partir de mediciones cada ~5 minutos:

| Feature | Descripción |
|---|---|
| `temp_lag_1` | Última temperatura registrada |
| `temp_lag_3` | Temperatura 3 pasos atrás |
| `temp_rolling_mean_5` | Media móvil de 5 observaciones |
| `presion_temp_ratio` | Presión / Temperatura (indicador atmosférico) |
| `hour_sin`, `hour_cos` | Hora del día codificada cíclicamente |

## 8.5 Entrenamiento

### Largo Plazo

El script `train_largo_plazo.py`:

1. **Lectura**: Carga Parquet histórico con `spark.read.parquet("artifacts/weather_data")`.
2. **Filtrado**: Selecciona estaciones con >1000 registros de tmax.
3. **Preprocesamiento**: Ordena por fecha, elimina nulos.
4. **Features**: Aplica `build_largo_plazo_features()` sobre secuencias ordenadas.
5. **Train/Test**: 80% entrenamiento, 20% prueba (últimos 20% cronológicos).
6. **Modelo**: XGBoost Regressor con `n_estimators=500`, `learning_rate=0.05`, `max_depth=7`.
7. **Evaluación**: MAE, RMSE, R² sobre conjunto de prueba.
8. **Persistencia**: Guarda modelo + métricas en `ml/models/`.

### Corto Plazo

El script `train_corto_plazo.py`:

1. **Lectura**: Carga Parquet streaming de `artifacts/parquet_output/{grupo}/`.
2. **Muestreo**: Selecciona últimas 10,000 observaciones de cada grupo.
3. **Preprocesamiento**: Ordena por `created_at`, elimina temperatura nula.
4. **Features**: Aplica `build_corto_plazo_features()`.
5. **Train/Test**: 80% / 20%.
6. **Modelo**: XGBoost Regressor con `n_estimators=300`, `learning_rate=0.05`, `max_depth=5`.
7. **Evaluación**: MAE, RMSE, R².

## 8.6 Integración con Dashboard

La función `render_ml_predictions()` en `dashboard/app.py`:

```python
def render_ml_predictions():
    st.subheader("🤖 Predicciones de Temperatura")
    
    col1, col2 = st.columns(2)
    with col1:
        horizonte = st.selectbox("Horizonte", ["Largo Plazo", "Corto Plazo"])
    
    modelos = list_models(horizonte.lower().replace(" ", "_"))
    estacion = st.selectbox("Estación", modelos)
    
    if horizonte == "Largo Plazo":
        fecha = st.date_input("Fecha de predicción")
    
    if st.button("Predecir"):
        if horizonte == "Largo Plazo":
            result = predict_largo_plazo(estacion, fecha)
            st.session_state.ml_lp_result = result
        else:
            result = predict_corto_plazo(estacion)
            st.session_state.ml_cp_result = result
    
    # Mostrar resultado desde session_state (persiste entre reruns)
    if st.session_state.get("ml_lp_result"):
        mostrar_resultado(st.session_state.ml_lp_result)
```

## 8.7 Optimización de Rendimiento

### Problema Original

`predict_largo_plazo()` escaneaba **todos los 2982 archivos Parquet** usando `glob("**/*.parquet")`, tomando **varios minutos** por predicción.

### Solución

Se implementó `STATION_DIR_MAP` mapeando cada estación al subdirectorio específico:

```python
STATION_DIR_MAP = {
    "PUNO": "department=PUNO/province=PUNO/district=PUNO",
    "AZANGARO": "department=PUNO/province=AZANGARO/district=AZANGARO",
    "LAMPA": "department=PUNO/province=LAMPA/district=LAMPA",
    "CAPACHICA": "department=PUNO/province=PUNO/district=CAPACHICA",
}
```

La función ahora construye la ruta directa `artifacts/weather_data/{dept}/{prov}/{dist}/`, reduciendo el tiempo de predicción de **~minutos a ~1.5 segundos**.

### Persistencia en Session State

Las predicciones se almacenan en `st.session_state` para persistir entre reruns automáticos del dashboard (`streamlit-autorefresh` cada 2s):

```python
st.session_state.ml_lp_result = result    # Largo plazo
st.session_state.ml_cp_result = result    # Corto plazo
```

## 8.8 Comandos de Re-entrenamiento

```bash
# Largo plazo — entrenar modelos con datos SENAMHI
docker exec clime-jupyter python -m ml.train_largo_plazo

# Corto plazo — entrenar modelos con datos streaming
docker exec clime-jupyter python -m ml.train_corto_plazo
```

Los modelos se guardan en `ml/models/`, montado como volumen Docker (`../ml:/app/ml`) para persistencia entre recreaciones del contenedor.
