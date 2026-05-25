# Dashboard — Interfaz de Visualización Streamlit

Dashboard web interactivo construido con Streamlit y Plotly que integra datos históricos (Parquet) y en tiempo real (Supabase WebSocket + Kafka) para el monitoreo climático.

## Acceso

```
URL: http://localhost:8501
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

Lecturas de sensores vía WebSocket desde Supabase:

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

### 📡 Métricas del Stack

Indicadores de salud del pipeline Kafka:

| Métrica | Fuente |
|---------|--------|
| Offset `clima-puno` | Prometheus (kafka_topic_partition_current_offset) |
| Offset `clima-anomalias` | Prometheus |
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
| Supabase (inicial) | REST API (`fetch_all_supabase_readings`) | 500 registros al iniciar |
| Supabase (streaming) | WebSocket Realtime + cola asíncrona | Eventos INSERT en tiempo real |
| Prometheus (métricas) | REST API (`fetch_kafka_metrics`) | Cache 15s |

## Streaming Buffer

```python
StreamingBuffer(maxlen=100)  # cola thread-safe (deque)
```

Almacena hasta 100 lecturas recientes para cálculos de ventana y gráficos.

## Arquitectura del Sidebar

```
⚙️ Panel de Control
├── 🌙/☀️ Modo Oscuro/Claro (toggle)
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
| → `streaming/supabase_kafka_bridge.py` | Misma tabla Supabase (grupo_3_air_quality) |
| → Prometheus (http://prometheus:9090) | Consulta métricas Kafka vía API |
| → `streaming/spark_streaming_processor.py` | Consume anomalías vía Kafka metrics |
