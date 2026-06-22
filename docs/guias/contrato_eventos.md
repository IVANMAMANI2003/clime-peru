# Contrato de Eventos

## Evento Sensor

Mensaje publicado por los bridges Kafka al tópico `clima-{grupo}`.

### Esquema

```json
{
  "sensor_id": "string",
  "estacion": "string",
  "department": "string",
  "province": "string",
  "district": "string",
  "temperatura": "float",
  "humedad": "float",
  "presion": "float",
  "altura": "float|null",
  "iaq": "float",
  "eco2": "float",
  "voc": "float",
  "calidad_aire": "string",
  "ts": "string (ISO 8601)",
  "created_at": "string (ISO 8601)",
  "id": "int"
}
```

### Campos Detallados

| Campo | Tipo | Rango/Válido | Descripción |
|---|---|---|---|
| `sensor_id` | string | `"grupo_2"`, `"grupo_3"`, `"grupo_4"` | ID único del sensor |
| `estacion` | string | Mismo que sensor_id | Nombre de estación en catálogo |
| `department` | string | `"PUNO"` | Departamento (del catálogo) |
| `province` | string | `"LAMPA"`, `"PUNO"`, `"AZANGARO"` | Provincia (del catálogo) |
| `district` | string | `"LAMPA"`, `"PUNO"`, `"AZANGARO"` | Distrito (del catálogo) |
| `temperatura` | float | -20 a 40 | Temperatura en °C |
| `humedad` | float | 0 a 100 | Humedad relativa % |
| `presion` | float | 300 a 1100 | Presión atmosférica hPa |
| `altura` | float/null | 0 a 5000 | Altitud msnm |
| `iaq` | float | 0 a 500 | Índice de calidad del aire |
| `eco2` | float | 400 a 5000 | CO₂ equivalente ppm |
| `voc` | float | 0 a 1000 | Compuestos orgánicos volátiles ppb |
| `calidad_aire` | string | Bueno, Moderado, Insalubre, Peligroso | Clasificación ICA |
| `ts` | string | ISO 8601 | Timestamp del sensor |
| `created_at` | string | ISO 8601 con offset `-05:00` | Timestamp normalizado por bridge |
| `id` | int | > 0 | ID único en Supabase |

### Ejemplo

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

## Evento Anomalía

Mensaje publicado por Spark al tópico `clima-{grupo}-anomalias`. Contiene todos los campos del evento sensor más los siguientes:

### Campos Adicionales

| Campo | Tipo | Descripción |
|---|---|---|
| `isAnomaly` | boolean | `true` si la lectura es anómala |
| `anomalyScore` | float | Desviación en sigmas (`\|z-score\|`) |
| `anomalyType` | string | `"alta"`, `"baja"`, `"normal"` |
| `promedioHistorico` | float | Promedio histórico de temperatura |
| `desviacionEstandar` | float | Desviación estándar histórica |
| `processedAt` | string | Timestamp de procesamiento |

### Ejemplo

```json
{
  "sensor_id": "grupo_2",
  "temperatura": 32.5,
  "isAnomaly": true,
  "anomalyScore": 2.5,
  "anomalyType": "alta",
  "promedioHistorico": 18.5,
  "desviacionEstandar": 2.1,
  "processedAt": "2026-05-25T12:00:05"
}
```

### Regla de Detección

```
anomalyScore = |temperatura - promedioHistorico| / desviacionEstandar

isAnomaly = anomalyScore > sigma (sigma = 2.0)

anomalyType:
  - "alta":  anomalyScore > sigma AND temperatura > promedio
  - "baja":  anomalyScore > sigma AND temperatura < promedio
  - "normal": otherwise
```

### Topics de Anomalías

Solo los registros con `isAnomaly == True` se publican al tópico de anomalías, gracias al filtro en `write_to_kafka()`:

```python
def write_to_kafka(batch_df, batch_id):
    anomalies = batch_df.filter(col("isAnomaly") == True)
    if anomalies.count() > 0:
        anomalies.selectExpr("to_json(struct(*)) AS value") \
            .write.format("kafka") \
            .option("topic", anomaly_topic) \
            .save()
```

## Normalización de Timestamps

Spark aplica una normalización en dos pasos:

```python
# Paso 1: Eliminar timezone
regexp_replace(col("ts"), r"[+-]\d{2}:\d{2}$", "")
# "2026-05-25T12:00:00-05:00" → "2026-05-25T12:00:00"

# Paso 2: Eliminar microsegundos
regexp_replace(col("ts"), r"\.\d+", "")
# "2026-05-25T12:00:00.123456" → "2026-05-25T12:00:00"
```

Resultado final: formato uniforme `yyyy-MM-ddTHH:mm:ss` sin timezone ni fracciones de segundo.

## Esquemas

### Spark Schema (Evento Sensor)

```python
from pyspark.sql.types import *

sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("estacion", StringType(), True),
    StructField("department", StringType(), True),
    StructField("province", StringType(), True),
    StructField("district", StringType(), True),
    StructField("temperatura", DoubleType(), True),
    StructField("humedad", DoubleType(), True),
    StructField("presion", DoubleType(), True),
    StructField("altura", DoubleType(), True),
    StructField("iaq", DoubleType(), True),
    StructField("eco2", DoubleType(), True),
    StructField("voc", DoubleType(), True),
    StructField("calidad_aire", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("created_at", StringType(), True),
])
```

### Tabla PostgreSQL

```sql
CREATE TABLE sensor_data_{estacion} (
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

## Tópicos Kafka

| Tópico | Tipo | Producido por | Consumido por |
|---|---|---|---|
| `clima-grupo_2` | Datos crudos | bridge-grupo2 | spark-grupo2, dashboard |
| `clima-grupo_3` | Datos crudos | bridge-grupo3 | spark-grupo3, dashboard |
| `clima-grupo_4` | Datos crudos | bridge-grupo4 | spark-grupo4, dashboard |
| `clima-grupo_2-anomalias` | Anomalías | spark-grupo2 | dashboard, Kafka Exporter |
| `clima-grupo_3-anomalias` | Anomalías | spark-grupo3 | dashboard, Kafka Exporter |
| `clima-grupo_4-anomalias` | Anomalías | spark-grupo4 | dashboard, Kafka Exporter |
