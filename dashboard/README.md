# Dashboard — Interfaz de Visualización Streamlit

Dashboard web interactivo construido con Streamlit y Plotly que integra datos históricos (Parquet), datos streaming (Kafka) y métricas del stack (Prometheus) para el monitoreo climático.

## Acceso

```
URL: http://localhost:8501
Consumer group Kafka: dashboard-consumer
Auto-refresh: cada 2 segundos (pestaña Tiempo Real)
```

## Pestañas

### 📊 Datos Históricos

Carga datos del Parquet histórico (`artifacts/weather_data/`) y ofrece:

**Filtros:**
- Departamento, Provincia, Estación (cascading selects)
- Rango de fechas
- Granularidad: Diaria / Mensual / Anual
- Variables: Precipitación, Temp. Máxima, Temp. Mínima

**Visualizaciones:**
- Serie temporal (líneas, área o ambos)
- Box plots por variable
- Mapa de estaciones (Plotly Scattermapbox)
- Tabla de datos descargable (CSV)

### ⏱️ Tiempo Real

Consume de Kafka mediante consumer group `dashboard-consumer` para los topics `clima-grupo_2`, `clima-grupo_3`, `clima-grupo_4`:

**Métricas en tarjetas:**
| Indicador | Descripción |
|-----------|-------------|
| 🌡️ Temperatura | Valor actual + máx/mín del buffer |
| 💧 Humedad | Valor actual + promedio |
| 🏭 IAQ | Índice calidad aire + eCO₂ + VOC |
| 📊 Presión | Valor actual + altitud |

**Gráficos:**
- Gauge de temperatura con zonas de color
- Evolución multi-variable (ejes Y múltiples)
- Panel de streaming últimos 60 segundos
- Estadísticas de ventana (min, max, mean, std)

**Filtros disponibles:**
- Ventana de tiempo: 5min / 15min / 1h / 6h / Todo el día
- Variables activas: Temperatura, Humedad, IAQ, Presión
- Tipo de gráfico: Líneas / Área / Ambos
- Promedio móvil (toggle)

### 🤖 Predicciones ML

Tercera pestaña del dashboard que integra modelos XGBoost para predicción de temperatura:

**Horizontes:**
- **Largo Plazo**: Predice tmax (temperatura máxima) diaria usando datos históricos SENAMHI (50+ años). Features: lags (1,3,5,7 días), rolling means (3,7,14,30 días), codificación temporal cíclica.
- **Corto Plazo**: Predice temperatura a 5-15 minutos usando datos streaming de sensores IoT. Features: lags (1,3), rolling mean (5), ratio presión/temperatura.

**Interfaz:**
- Selector de horizonte (Largo Plazo / Corto Plazo)
- Selector de estación (dinámico según horizonte)
- Date input para fecha de predicción (largo plazo)
- Botón "Predecir"
- Tarjeta de resultado con temperatura predicha, MAE, RMSE y R²

**Persistencia:** Resultados guardados en `st.session_state.ml_lp_result` / `st.session_state.ml_cp_result` para mantener visibles tras auto-refresh.

**Modelos disponibles** (cargados desde `ml/models/`):

| Horizonte | Estación | MAE | R² |
|-----------|----------|-----|----|
| Largo | PUNO | 0.08°C | 0.994 |
| Largo | AZANGARO | 0.09°C | 0.995 |
| Largo | LAMPA | 0.07°C | 0.997 |
| Largo | CAPACHICA | 0.08°C | 0.994 |
| Corto | grupo_2 (LAMPA) | 0.044°C | 0.992 |
| Corto | grupo_4 (AZANGARO) | 0.053°C | 0.993 |

### 📡 Métricas del Stack

Indicadores de salud del pipeline Kafka:

| Métrica | Fuente |
|---------|--------|
| Offset por tópico | Prometheus (kafka_topic_partition_current_offset) |
| Brokers disponibles | Prometheus (kafka_brokers) |
| Consumer Lag | Prometheus (kafka_consumergroup_lag) |
| Conexión Kafka Exporter | Prometheus (up) |

## Temas

Soporte para modo **Oscuro** y **Claro** vía toggle en la barra lateral:

- CSS dinámico con variables de color
- Plantillas Plotly específicas por tema
- Persistencia en `st.session_state.theme`

## Fuentes de Datos

| Fuente | Método | Frecuencia |
|--------|--------|------------|
| Parquet histórico | `pd.read_parquet()` | Una vez al cargar la página |
| Kafka (streaming) | Consumer group `dashboard-consumer` | Streaming continuo |
| Prometheus (métricas) | REST API (`fetch_kafka_metrics`) | Cache 15s |

## Streaming Buffer

```python
StreamingBuffer(maxlen=100)  # cola thread-safe (deque)
```

Almacena hasta 100 lecturas recientes para cálculos de ventana y gráficos. Normaliza timestamps: strings ISO → objetos `datetime` UTC naive.

## Arquitectura del Sidebar

```
⚙️ Panel de Control
├── 🌙/☀️ Modo Oscuro/Claro (toggle)
├── 📡 Estación activa (selector desde sensor_catalog.json)
└── 📡 Streaming
    ├── ⏸️/▶️ Pausar/Reanudar
    └── 🔴 EN VIVO / 🟡 PAUSADO
```

## Ejecución

```bash
# Via docker-compose
cd docker && docker compose up -d dashboard

# Local
cd dashboard && streamlit run app.py --server.address 0.0.0.0 --server.port 8501
```

## Conexiones

| Componente | Conexión |
|-----------|----------|
| → `config/config.yaml` | Parámetros de dashboard, paths |
| → `artifacts/weather_data/` | Lectura de Parquet histórico |
| → `artifacts/stations_metadata.parquet` | Metadatos para mapa |
| → `artifacts/sensor_catalog.json` | Catálogo de estaciones activas |
| → Kafka (`clima-grupo_*`) | Consumer group `dashboard-consumer` |
| → Prometheus (http://prometheus:9090) | Consulta métricas Kafka vía API |
