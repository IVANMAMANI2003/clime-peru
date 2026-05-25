# Streaming — Procesamiento en Tiempo Real

Pipeline de streaming que conecta Supabase, Kafka y Spark para detección de anomalías climáticas en tiempo real.

## Arquitectura del Pipeline Streaming

```
Supabase ──WebSocket/REST──▶ supabase_kafka_bridge.py ──▶ Kafka (clima-puno)
                                                               │
                                                    spark_streaming_processor.py
                                                               │
                                               ┌───────────────┴───────────────┐
                                               ▼                               ▼
                                    Kafka (clima-anomalias)          Parquet (output/)
                                    Solo es_anomalia == "SI"
                                               │
                                               ▼
                                    Kafka Exporter ──▶ Prometheus ──▶ Grafana
                                               │
                                               ▼
                                    Dashboard (Streamlit)
```

## Componentes

### `supabase_kafka_bridge.py` — Puente Supabase → Kafka

Convierte lecturas de sensores desde Supabase en mensajes Kafka.

| Aspecto | Detalle |
|---------|---------|
| **Tabla origen** | `grupo_3_air_quality` (Supabase) |
| **Tópico destino** | `clima-puno` (Kafka) |
| **Carga inicial** | 500 registros vía REST al iniciar |
| **Realtime** | WebSocket `postgres_changes` (INSERT) |
| **Polling respaldo** | Cada 30 segundos (`id > last_id`) |
| **Serializer** | JSON (`json.dumps` → UTF-8) |
| **Acks Kafka** | `all` (escritura completa en líder) |

**Mensaje típico**:
```json
{
  "sensor_id": "estacion_001",
  "temperatura": 18.5,
  "humedad": 65.2,
  "presion": 1013.25,
  "altura": 3820.0,
  "iaq": 42.0,
  "eco2": 450.0,
  "voc": 0.15,
  "calidad_aire": "Bueno",
  "created_at": "2026-05-25T12:00:00Z",
  "id": 132255
}
```

### `spark_streaming_processor.py` — Spark Structured Streaming

Procesa el stream de Kafka, detecta anomalías y escribe resultados.

| Parámetro | Valor |
|-----------|-------|
| **Trigger** | 5 segundos |
| **Watermark** | 30 segundos |
| **Ventana** | 1 minuto (tumbling) |
| **Output mode** | append |
| **Checkpoint** | `/app/artifacts/checkpoints` |

**Pipeline de transformación**:

1. **Parseo**: `from_json` con esquema tipado (sensor_id, temperatura, humedad, timestamp, ubicación)
2. **Observabilidad**: `isValid`, `processedAt` (epoch millis), `latencyMs`
3. **Detección de anomalías**:
   - Calcula `limite_inferior` y `limite_superior` usando promedio histórico y sigma (2.0)
   - Marca `es_anomalia = "SI"` si temperatura fuera del rango
   - Columnas: `promedio_historico`, `desviacion_estandar`, `diferencia_promedio`, `mensaje`

**Salidas**:

| Salida | Destino | Filtro |
|--------|---------|--------|
| Consola | stdout | Todos los registros |
| Kafka | `clima-anomalias` | Solo `es_anomalia == "SI"` |
| Parquet | `/app/artifacts/parquet_output/` | Todos los registros con campos de observabilidad |

### `kafka_consumer.py` — Consumidor/Productor Kafka

Clases auxiliares:

| Clase | Función |
|-------|---------|
| `ClimateKafkaConsumer` | Consume `clima-puno`, valida lecturas, detecta anomalías (hardcoded), fire callbacks |
| `ClimateProducer` | Envía lecturas de sensores (JSON) a Kafka. Incluye `simulate_sensor_data()` para pruebas |

### `supabase_consumer.py` — Consumidores Supabase

| Clase | Función |
|-------|---------|
| `SupabaseConsumer` | Polling REST a Supabase para nuevas lecturas, validación y detección de anomalías |
| `MockSupabaseConsumer` | Genera datos sintéticos aleatorios para pruebas sin conexión a Supabase |

## Tópicos Kafka

| Tópico | Cómo se llena | Cómo se consume |
|--------|--------------|-----------------|
| `clima-puno` | `supabase_kafka_bridge.py` publica datos de sensores | `spark_streaming_processor.py` lee el stream |
| `clima-anomalias` | `spark_streaming_processor.py` escribe anomalías detectadas | Dashboard, Kafka UI, Kafka Exporter |

## Gestión de Tópicos

```bash
# Crear tópicos
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --topic clima-puno --partitions 1 --replication-factor 1

# Describir tópicos
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh \
  --describe --topic clima-puno --bootstrap-server kafka:9092

# Ver offsets
docker exec clime-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:9092 --topic clima-puno

# Consumir mensajes (CLI)
docker exec clime-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic clima-puno --from-beginning --max-messages 5
```

## Contrato de Evento

### Evento Sensor (`clima-puno`)

| Campo | Tipo | Descripción | Ejemplo |
|-------|------|-------------|---------|
| `sensor_id` | string | ID del sensor/estación | `"estacion_001"` |
| `temperatura` | float | Temperatura en °C | `18.5` |
| `humedad` | float | Humedad relativa % | `65.2` |
| `presion` | float | Presión atmosférica hPa | `1013.25` |
| `altura` | float/null | Altitud msnm | `3820.0` |
| `iaq` | float | Índice calidad aire | `42.0` |
| `eco2` | float | CO₂ eq ppm | `450.0` |
| `voc` | float | COV ppb | `0.15` |
| `calidad_aire` | string | Clasificación ICA | `"Bueno"` |
| `created_at` | string | Timestamp ISO 8601 | `"2026-05-25T12:00:00Z"` |

### Evento Anomalía (`clima-anomalias`)

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `sensor_id` | string | ID del sensor |
| `temperatura` | float | Temperatura reportada |
| `promedio_historico` | float | Promedio histórico para la estación |
| `desviacion_estandar` | float | Desviación estándar histórica |
| `limite_inferior` | float | Límite inferior del rango normal |
| `limite_superior` | float | Límite superior del rango normal |
| `diferencia_promedio` | float | Diferencia con el promedio |
| `es_anomalia` | string | `"SI"` o `"NO"` |
| `mensaje` | string | Descripción de la alerta |

## Métricas Expuestas

El Spark Streaming Processor logea automáticamente por cada micro-batch:

- `batchId` — ID del batch
- `numInputRows` — Filas de entrada
- `inputRowsPerSecond` — Tasa de llegada
- `processedRowsPerSecond` — Tasa de procesamiento
- `avgOffsetsBehindLatest` — Lag promedio
- `maxOffsetsBehindLatest` — Lag máximo

Estas métricas son visibles en los logs del contenedor:

```bash
docker logs clime-spark-streaming
```

## Ejecución

### Modo consola (debug)

```bash
docker exec clime-spark-streaming python -m streaming.spark_streaming_processor --mode console
```

### Modo pipeline completo (Kafka + anomalías)

```bash
docker exec clime-spark-streaming python -m streaming.spark_streaming_processor --mode kafka
```

### Modo servicio (vía docker-compose)

```bash
cd docker && docker compose up -d spark-streaming
```

## Conexiones

| Componente | Conexión |
|-----------|----------|
| → `config/config.yaml` | Parámetros Kafka, sensor, streaming |
| → `batch/etl_senamhi.py` | Parquet histórico para promedios por estación |
| → `dashboard/app.py` | Kafka metrics vía Prometheus API |
| → `docker/prometheus/prometheus.yml` | Kafka Exporter scrapea métricas |
