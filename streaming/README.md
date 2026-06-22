# Streaming — Procesamiento en Tiempo Real

Pipeline de streaming que conecta Supabase, Kafka, Spark y PostgreSQL para detección de anomalías climáticas en tiempo real. Implementa 3 bridges (1 por tabla Supabase) y 3 procesadores Spark (1 por estación).

## Arquitectura del Pipeline Streaming

```
Supabase                   Kafka                       Spark                       Salidas
─────────                  ─────                       ─────                       ──────
tabla_g2 ──bridge-g2──▶ clima-grupo_2 ──spark-g2──▶  ┌────────────────────┐   clima-grupo_2-anomalias
tabla_g3 ──bridge-g3──▶ clima-grupo_3 ──spark-g3──▶  │ Parseo             │──▶ Parquet streaming
tabla_g4 ──bridge-g4──▶ clima-grupo_4 ──spark-g4──▶  │ Normalizar ts      │──▶ PostgreSQL (sensor_data_*)
                                                      │ Detectar anomalías │
                                                      │ 3 sinks paralelos  │
                                                      └────────────────────┘
```

## Componentes

### `supabase_kafka_bridge.py` — Puente Supabase → Kafka (3 instancias)

Convierte lecturas de sensores desde Supabase en mensajes Kafka, con checkpoint persistente.

| Aspecto | Detalle |
|---------|---------|
| **Tabla origen** | Configurable via `--table` |
| **Tópico destino** | Configurable via `--topic` |
| **Carga inicial** | Todos los registros vía REST API (paginación 1000) |
| **Checkpoint** | Guarda `last_id` en `/app/artifacts/bridge_checkpoints/{tabla}.json` |
| **Reinicio** | Si existe checkpoint, carga solo `id > last_id` (evita reprocesar todo) |
| **Realtime** | WebSocket `postgres_changes` (INSERT) |
| **Polling respaldo** | Cada 30s consulta `id > last_id` |
| **Catálogo** | Lee `sensor_catalog.json` para obtener `department`, `province`, `district` |
| **Serializer** | JSON (`json.dumps` → UTF-8) |
| **Acks Kafka** | `all` (escritura completa en líder) |

**Mensaje típico**:
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

### `spark_streaming_processor.py` — Spark Structured Streaming (3 instancias)

Procesa el stream de Kafka, normaliza timestamps, detecta anomalías y escribe a 3 sinks.

| Parámetro | Valor |
|-----------|-------|
| **Trigger** | 5 segundos (micro-batch) |
| **Watermark** | 30 segundos |
| **Ventana** | 1 minuto (tumbling) |
| **Output mode** | append |
| **Starting offsets** | `earliest` (procesa desde el inicio del tópico) |
| **Checkpoint** | `/app/artifacts/checkpoints/{kafka,parquet}/{estacion}` |

**Pipeline de transformación**:

1. **Parseo**: `from_json` con esquema tipado (sensor_id, temperatura, humedad, ts, department, province, district...)
2. **Normalización ts**: `regexp_replace` doble — quita timezone (`[+-]\d{2}:\d{2}$`) y microsegundos (`\.\d+`) → formato `yyyy-MM-ddTHH:mm:ss`
3. **Stats históricos**: Carga Parquet batch, filtra por ubicación del catálogo, calcula `avg(tmax)` y `stddev(tmax)`
4. **Detección de anomalías**: `(temperatura - promedio) / stddev` → `|score| > sigma` → `isAnomaly`
5. **Campos de observabilidad**: `promedioHistorico`, `desviacionEstandar`, `thresholdSigma`, `processedAt`

**Salidas**:

| Salida | Destino | Contenido |
|--------|---------|-----------|
| Kafka | `clima-{estacion}-anomalias` | anomalyScore, isAnomaly, anomalyType (alta/baja/normal) + todos los campos |
| Parquet | `/app/artifacts/parquet_output/{estacion}/` | Todos los registros con campos de observabilidad. **Usado por ML corto plazo** (`ml/train_corto_plazo.py`) |
| PostgreSQL | `sensor_data_{estacion}` | id, sensor_id, estacion, department, province, district, temperatura, humedad, presion, altura, iaq, eco2, voc, calidad_aire, ts, created_at, processed_at |

### `kafka_consumer.py` — Consumidor/Productor Kafka

Clases auxiliares:

| Clase | Función |
|-------|---------|
| `ClimateKafkaConsumer` | Consume tópicos Kafka, valida lecturas, detecta anomalías (hardcoded), fire callbacks |
| `ClimateProducer` | Envía lecturas de sensores (JSON) a Kafka. Incluye `simulate_sensor_data()` para pruebas |

### `supabase_consumer.py` — Consumidores Supabase

| Clase | Función |
|-------|---------|
| `SupabaseConsumer` | Polling REST a Supabase para nuevas lecturas, validación y detección de anomalías |
| `MockSupabaseConsumer` | Genera datos sintéticos aleatorios para pruebas sin conexión a Supabase |

## Tópicos Kafka

| Tópico | Cómo se llena | Cómo se consume |
|--------|--------------|-----------------|
| `clima-grupo_2` | `bridge-grupo2` publica datos de Supabase tabla grupo_2_air_quality | `spark-grupo2` lee el stream |
| `clima-grupo_3` | `bridge-grupo3` publica datos de Supabase tabla grupo_3_air_quality | `spark-grupo3` lee el stream |
| `clima-grupo_4` | `bridge-grupo4` publica datos de Supabase tabla grupo4_air_quality | `spark-grupo4` lee el stream |
| `clima-grupo_2-anomalias` | `spark-grupo2` escribe anomalías detectadas | Dashboard, Kafka UI, Kafka Exporter |
| `clima-grupo_3-anomalias` | `spark-grupo3` escribe anomalías detectadas | Dashboard, Kafka UI, Kafka Exporter |
| `clima-grupo_4-anomalias` | `spark-grupo4` escribe anomalías detectadas | Dashboard, Kafka UI, Kafka Exporter |

## Gestión de Tópicos

```bash
# Listar tópicos
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Ver offsets
docker exec clime-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic clima-grupo_2

# Consumir mensajes (CLI)
docker exec clime-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic clima-grupo_2 --from-beginning --max-messages 5

# Consumir anomalías
docker exec clime-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic clima-grupo_2-anomalias --from-beginning --max-messages 5
```

## Contrato de Evento

### Evento Sensor (`clima-grupo_2`, `clima-grupo_3`, `clima-grupo_4`)

| Campo | Tipo | Descripción | Ejemplo |
|-------|------|-------------|---------|
| `sensor_id` | string | ID del sensor/estación | `"grupo_2"` |
| `estacion` | string | Nombre de estación en catálogo | `"grupo_2"` |
| `department` | string | Departamento (del catálogo) | `"PUNO"` |
| `province` | string | Provincia (del catálogo) | `"LAMPA"` |
| `district` | string | Distrito (del catálogo) | `"LAMPA"` |
| `temperatura` | float | Temperatura en °C | `18.5` |
| `humedad` | float | Humedad relativa % | `65.2` |
| `presion` | float | Presión atmosférica hPa | `1013.25` |
| `altura` | float/null | Altitud msnm | `3820.0` |
| `iaq` | float | Índice calidad aire | `42.0` |
| `eco2` | float | CO₂ eq ppm | `450.0` |
| `voc` | float | COV ppb | `0.15` |
| `calidad_aire` | string | Clasificación ICA | `"Bueno"` |
| `ts` | string | Timestamp normalizado (tz+µs eliminados por Spark) | `"2026-05-25T12:00:00"` |
| `created_at` | string | Timestamp ISO 8601 original | `"2026-05-25T12:00:00-05:00"` |
| `id` | int | ID único en Supabase | `132255` |

### Evento Anomalía (`clima-{estacion}-anomalias`)

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id` | int | ID del registro original |
| `sensor_id` | string | ID del sensor |
| `estacion` | string | Nombre de estación |
| `department` | string | Departamento |
| `province` | string | Provincia |
| `district` | string | Distrito |
| `temperatura` | float | Temperatura reportada |
| `humedad` | float | Humedad relativa |
| `presion` | float | Presión atmosférica |
| `altura` | float | Altitud |
| `iaq` | float | Índice calidad aire |
| `eco2` | float | CO₂ eq |
| `voc` | float | COV |
| `calidad_aire` | string | Clasificación ICA |
| `ts` | string | Timestamp normalizado |
| `created_at` | string | Timestamp original |
| `isAnomaly` | boolean | `true` si es anomalía |
| `anomalyScore` | float | Desviación en sigmas |
| `anomalyType` | string | `"alta"`, `"baja"`, `"normal"` |
| `promedioHistorico` | float | Promedio histórico de temperatura |
| `desviacionEstandar` | float | Desviación estándar histórica |
| `processedAt` | timestamp | Cuándo se procesó |

## Tablas PostgreSQL

Tablas creadas automáticamente por Spark via `CREATE TABLE IF NOT EXISTS`:

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

Inserción con `ON CONFLICT (id) DO NOTHING` para evitar duplicados en reprocesos.

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
docker logs clime-spark-grupo2
```

## Ejecución

### Modo servicio (vía docker-compose)

```bash
cd docker && docker compose up -d
```

### Modo individual

```bash
# Bridge específico
docker compose -f docker/docker-compose.yml up -d --no-deps bridge-grupo2

# Spark específico
docker compose -f docker/docker-compose.yml up -d --no-deps spark-grupo2
```

## Conexiones

| Componente | Conexión |
|-----------|----------|
| → `config/config.yaml` | Parámetros Kafka, sensor, streaming, database |
| → `batch/etl_senamhi.py` | Parquet histórico para promedios por estación (usa `tmax`) |
| → `dashboard/app.py` | Kafka metrics vía Prometheus API |
| → `docker/prometheus/prometheus.yml` | Kafka Exporter scrapea métricas |
| → `artifacts/sensor_catalog.json` | Mapeo tabla → ubicación geográfica |

## Limpieza Completa

```bash
# Detener servicios
docker compose stop bridge-grupo2 bridge-grupo3 bridge-grupo4 spark-grupo2 spark-grupo3 spark-grupo4 dashboard

# Eliminar topics Kafka
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_2
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_3
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_4
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_2-anomalias
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_3-anomalias
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_4-anomalias

# Eliminar artifacts
rm -rf artifacts/checkpoints artifacts/parquet_output artifacts/bridge_checkpoints

# Drop tablas PostgreSQL
docker exec clime-postgres psql -U clime -d climedb -c "DROP TABLE IF EXISTS sensor_data_grupo_2, sensor_data_grupo_3, sensor_data_grupo_4"

# Levantar todo fresco
docker compose up -d
```


 # Ver estado del contenedor PostgreSQL
docker ps | grep postgres

# Entrar al contenedor
docker exec -it clime-postgres bash

# Ver logs de PostgreSQL
docker logs clime-postgres

# Reiniciar PostgreSQL
docker restart clime-postgres

# Conectar a la base de datos
psql -U clime -d climedb
\dt
SELECT * FROM sensor_data_grupo_2 LIMIT 5;
SELECT COUNT(*) FROM sensor_data_grupo_2;