# 4. Formatos de Almacenamiento

## 4.1 Parquet

**Parquet** es el formato columnar principal de CliMePerú, utilizado tanto para datos históricos como para datos streaming.

### Ventajas del Formato Columnar

| Característica | Beneficio |
|---|---|
| Compresión por columnas | Ratio de compresión 3-5x vs CSV |
| Podado de columnas | Lee solo las columnas consultadas |
| Estadísticas en metadatos | Min/max por row group para filtrado eficiente |
| Esquema autodescriptivo | Los archivos contienen el schema |
| Compatibilidad Spark nativa | Sin dependencias externas |

### Configuración

```yaml
spark:
  parquet_compression: "snappy"
```

### Uso en CliMePerú

#### Parquet Histórico (`artifacts/weather_data/`)

- 1,073,151 registros, 60 estaciones.
- Particionado por `department/province/district/year`.
- Compresión Snappy, ~14 MB en 2989 archivos.
- Columnas: `station_name, month, day, precip, tmax, tmin, date`.

#### Parquet Streaming (`artifacts/parquet_output/{grupo}/`)

- ~374,000 registros totales (grupo_2: 136K, grupo_3: 141K, grupo_4: 97K).
- Escritura continua vía Spark Structured Streaming.
- Columnas: `sensor_id, temperatura, humedad, presion, iaq, eco2, voc, ts, department, province, district, anomalyScore, isAnomaly, ...`
- Particionado por `year/month/day/hour`.

## 4.2 PostgreSQL

Base de datos relacional para almacenamiento persistente de datos streaming.

### Esquema

```sql
CREATE TABLE IF NOT EXISTS sensor_data_{estacion} (
    id BIGINT PRIMARY KEY,
    sensor_id VARCHAR(50),
    estacion VARCHAR(50),
    department VARCHAR(50),
    province VARCHAR(50),
    district VARCHAR(50),
    temperatura FLOAT,
    humedad FLOAT,
    presion FLOAT,
    altura FLOAT,
    iaq FLOAT,
    eco2 FLOAT,
    voc FLOAT,
    calidad_aire VARCHAR(50),
    ts VARCHAR(50),
    created_at VARCHAR(50),
    processed_at TIMESTAMP
);
```

### Deduplicación

```sql
INSERT INTO sensor_data_grupo_2 (...) VALUES (...)
ON CONFLICT (id) DO NOTHING;
```

### Tablas Actuales

| Tabla | Registros | Grupo |
|---|---|---|
| `sensor_data_grupo_2` | ~136,000 | grupo_2 (LAMPA) |
| `sensor_data_grupo_3` | ~141,000 | grupo_3 (PUNO) |
| `sensor_data_grupo_4` | ~97,000 | grupo_4 (AZANGARO) |
| **Total** | **~374,000** | |

## 4.3 Formato de Mensajes Kafka

Los mensajes en Kafka se serializan como **JSON** codificado en UTF-8.

### Evento Sensor

```json
{
  "sensor_id": "grupo_2",
  "estacion": "grupo_2",
  "department": "PUNO",
  "province": "LAMPA",
  "district": "LAMPA",
  "temperatura": 18.5,
  "humedad": 65.2,
  "presion": 1013.25,
  "altura": 3820.0,
  "iaq": 42.0,
  "eco2": 450.0,
  "voc": 0.15,
  "calidad_aire": "Bueno",
  "ts": "2026-05-25T12:00:00-05:00",
  "created_at": "2026-05-25T12:00:00-05:00",
  "id": 132255
}
```

### Evento Anomalía

Todos los campos del evento sensor más:

```json
{
  "isAnomaly": true,
  "anomalyScore": 2.5,
  "anomalyType": "alta",
  "promedioHistorico": 18.5,
  "desviacionEstandar": 2.1,
  "processedAt": "2026-05-25T12:00:05"
}
```

### Normalización de Timestamps

Spark aplica una normalización doble a los timestamps:

```
1. 2026-05-25T12:00:00-05:00   (timestamp original con timezone)
   → 2026-05-25T12:00:00       (sin timezone)

2. 2026-05-25T12:00:00.123456  (timestamp con microsegundos)
   → 2026-05-25T12:00:00       (sin microsegundos)
```

## 4.4 Modelos ML (joblib)

Los modelos de Machine Learning se persisten en formato **joblib** (`.pkl`).

### Ubicación

```
ml/models/
├── largo_plazo_puno.pkl
├── largo_plazo_azangaro.pkl
├── largo_plazo_lampa.pkl
├── largo_plazo_capachica.pkl
├── corto_plazo_grupo_2.pkl
├── corto_plazo_grupo_3.pkl
├── corto_plazo_grupo_4.pkl
└── metrics.json
```

### Persistencia

Los modelos se guardan en `ml/models/`, directorio montado como volumen Docker (`../ml:/app/ml`) para persistencia entre recreaciones del contenedor:

```yaml
# docker-compose.yml
volumes:
  - ../ml:/app/ml
```

## 4.5 Formatos de Archivos de Configuración

| Archivo | Formato | Propósito |
|---|---|---|
| `config/config.yaml` | YAML | Configuración centralizada del sistema |
| `artifacts/sensor_catalog.json` | JSON | Mapeo tabla → ubicación geográfica |
| `artifacts/sensor_config.json` | JSON | Estación activa para dashboard |
| `docker/prometheus/prometheus.yml` | YAML | Configuración de scraping Prometheus |
| `docker/prometheus/rules/alert.rules.yml` | YAML | Reglas de alerta Prometheus |

## 4.6 Comparativa de Formatos

| Formato | Uso | Tamaño | Velocidad Lectura | Velocidad Escritura |
|---|---|---|---|---|
| **Parquet** | Almacenamiento analítico | ~14 MB (1M registros) | Excelente (columnar) | Buena |
| **PostgreSQL** | Datos transaccionales streaming | ~50 MB (374K registros) | Buena (indexado) | Buena (batch 100) |
| **JSON** | Mensajería Kafka | ~1 KB/mensaje | N/A (transporte) | N/A (serialización) |
| **joblib** | Modelos ML | ~1-5 MB/modelo | Instantánea | Instantánea |
| **YAML** | Configuración | ~5 KB | N/A | N/A |
