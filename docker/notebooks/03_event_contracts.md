# Contratos de Eventos - ClimePeru

## 1. Evento de Sensor (topic: `clima-puno`)

```json
{
  "sensor_id": "SENAMHI-PUNO-001",
  "temperatura": 22.5,
  "humedad": 65.0,
  "timestamp": 1710000000000,
  "ubicacion": {
    "lat": -15.84,
    "lon": -70.02
  }
}
```

| Campo | Tipo | Obligatorio | Descripcion |
|-------|------|-------------|-------------|
| sensor_id | String | Si | Identificador unico del sensor |
| temperatura | Float | Si | Temperatura en grados Celsius |
| humedad | Float | No | Humedad relativa en porcentaje |
| timestamp | Long | Si | Unix epoch en milisegundos |
| ubicacion.lat | Float | No | Latitud de la estacion |
| ubicacion.lon | Float | No | Longitud de la estacion |

### Schema PySpark

```python
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

schema_sensor = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperatura", FloatType(), True),
    StructField("humedad", FloatType(), True),
    StructField("timestamp", LongType(), True),
    StructField("ubicacion", StructType([
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True)
    ]), True)
])
```

## 2. Evento de Anomalia (topic: `clima-anomalias`)

```json
{
  "sensor_id": "SENAMHI-PUNO-001",
  "temperatura": -5.0,
  "timestamp": 1710000000000,
  "promedio_historico": 12.0,
  "desviacion_estandar": 2.5,
  "limite_inferior": 7.0,
  "limite_superior": 17.0,
  "es_anomalia": "SI",
  "diferencia_promedio": -17.0,
  "mensaje": "Alerta: Temperatura -5.0°C fuera del rango historico 7.0 a 17.0°C",
  "processed_at": "2025-01-01T00:00:00Z"
}
```

| Campo | Tipo | Obligatorio | Descripcion |
|-------|------|-------------|-------------|
| sensor_id | String | Si | Identificador del sensor |
| temperatura | Float | Si | Temperatura medida |
| timestamp | Long | Si | Unix epoch del evento |
| promedio_historico | Float | Si | Promedio historico de temperatura |
| desviacion_estandar | Float | Si | Desviacion estandar historica |
| limite_inferior | Float | Si | Limite inferior (promedio - sigma*threshold) |
| limite_superior | Float | Si | Limite superior (promedio + sigma*threshold) |
| es_anomalia | String | Si | "SI" o "NO" |
| diferencia_promedio | Float | Si | Diferencia respecto al promedio |
| mensaje | String | No | Mensaje descriptivo |
| processed_at | Timestamp | Si | Momento de procesamiento en Spark |

## 3. Producir eventos de prueba

### Python (desde Jupyter o script)

```python
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_id = "SENAMHI-PUNO-001"

while True:
    data = {
        "sensor_id": sensor_id,
        "temperatura": round(random.uniform(5.0, 35.0), 1),
        "humedad": round(random.uniform(30.0, 100.0), 1),
        "timestamp": int(time.time() * 1000),
        "ubicacion": {"lat": -15.84, "lon": -70.02}
    }
    producer.send("clima-puno", value=data)
    print("Enviado:", data)
    time.sleep(2)
```

### Kafka CLI

```bash
# Desde el contenedor Kafka
kafka-console-producer.sh \
  --broker-list kafka:9092 \
  --topic clima-puno

# Enviar JSON linea por linea:
{"sensor_id":"SENAMHI-PUNO-001","temperatura":22.5,"humedad":65,"timestamp":1710000000000,"ubicacion":{"lat":-15.84,"lon":-70.02}}
```

## 4. Consumir eventos

### Kafka CLI

```bash
kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic clima-puno \
  --from-beginning

# Consumir topic de anomalias
kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic clima-anomalias \
  --from-beginning
```

## 5. Metricas de Observabilidad

| Metrica | Donde se mide | Descripcion |
|---------|---------------|-------------|
| `latencyMs` | Spark (notebook) | Diferencia entre timestamp del evento y procesamiento |
| `numInputRows` | Spark lastProgress | Eventos recibidos por micro-batch |
| `inputRowsPerSecond` | Spark lastProgress | Tasa de entrada de eventos |
| `processedRowsPerSecond` | Spark lastProgress | Tasa de procesamiento |
| `kafka_consumergroup_lag` | Prometheus / Grafana | Eventos pendientes por consumir |
| `kafka_brokers` | Prometheus / Grafana | Cantidad de brokers disponibles |
| `up{job="kafka-exporter"}` | Prometheus / Grafana | Estado del Kafka Exporter |

## 6. Alertas

| Alerta | Expresion PromQL | Umbral |
|--------|------------------|--------|
| Exporter Down | `up{job="kafka-exporter"} == 0` | 1 minuto |
| High Lag | `kafka_consumergroup_lag > 100` | 2 minutos |
| Latencia Alta | `latencyMs > 1000` (Spark) | Evaluacion manual |
| Particiones No Replicadas | `kafka_topic_partition_under_replicated_partitions > 0` | 1 minuto |
