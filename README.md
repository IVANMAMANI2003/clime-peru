# CimaPerú — Sistema de Monitoreo Climático Inteligente

Sistema Big Data híbrido que integra datos climáticos históricos del SENAMHI con lecturas en tiempo real de sensores IoT, utilizando **Apache Kafka** como backbone de mensajería, **Apache Spark Structured Streaming** para detección de anomalías, y **PostgreSQL** como almacenamiento persistente, con observabilidad completa del pipeline.

## Arquitectura General

```
                          ┌─────────────────────────────────────────────────────────────────────┐
                          │                         CIMAPERÚ STACK                               │
                          │                                                                     │
 ┌──────────────┐         │  ┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌──────────┐  │
 │   SENAMHI    │─────────│─▶│  SPARK   │────▶│ PARQUET  │    │  DASHBOARD   │    │POSTGRES │  │
 │  (.txt .dat) │         │  │ETL BATCH │    │HISTÓRICO │    │  STREAMLIT   │    │ CLIMEDB │  │
 └──────────────┘         │  └──────────┘    └─────┬────┘    │  :8501       │    │ :15432  │  │
                          │                        │         └──────▲───────┘    └────▲────┘  │
 ┌──────────────┐         │  ┌──────────┐    ┌─────▼─────┐         │              │          │
 │  SUPABASE    │         │  │  KAFKA   │    │   SPARK   │─────────┘──────────────┘          │
 │  tabla_1 ────│─Bridge──│─▶│clima-g2  │───▶│ STREAMING │ Anomalías + Data                  │
 │  tabla_2 ────│─Bridge──│─▶│clima-g3  │───▶│ (3 inst.) │──────▶ .anomalias                  │
 │  tabla_3 ────│─Bridge──│─▶│clima-g4  │───▶│ c/u       │──────▶ Parquet streaming           │
 └──────────────┘  (3)    │  └─────┬──────┘    └───────────┘                                   │
                          │        │                                                           │
                          │  ┌─────▼──────┐   ┌─────────────────────────────────────┐         │
                          │  │ KAFKA UI   │   │      OBSERVABILIDAD                  │         │
                          │  │ :18085     │   │ Kafka Exporter ─▶ Prometheus ─▶ Graf│         │
                          │  └────────────┘   │ (19308)           (19090)    (13000) │         │
                          │                   │ Alertas: Lag > 100, Brokers, Exporter│         │
                          │                   └─────────────────────────────────────┘         │
                          └─────────────────────────────────────────────────────────────────────┘
```

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Stack Tecnológico](#stack-tecnológico)
- [Requisitos Previos](#requisitos-previos)
- [Inicio Rápido](#inicio-rápido-docker)
- [Servicios y Puertos](#servicios-y-puertos)
- [Pipeline de Datos](#pipeline-de-datos)
- [Observabilidad](#observabilidad)
- [Parámetros de Configuración](#parámetros-de-configuración)
- [Troubleshooting](#troubleshooting)

## Arquitectura

El sistema sigue una **arquitectura Kappa**: un único pipeline de streaming procesa tanto datos históricos (reprocesados) como en tiempo real.

| Capa | Componente | Tecnología | Propósito |
|------|-----------|-----------|-----------|
| **Fuentes** | SENAMHI | Archivos .txt | 60 estaciones meteorológicas históricas (1940-2015) |
| | Supabase | PostgreSQL + WebSocket | Múltiples tablas (1 por sensor IoT) |
| **Catálogo** | sensor_catalog.json | Mapeo tabla → estación → ubicación | {department, province, district} |
| **Ingesta** | Kafka | apache/kafka:4.2.0 (KRaft) | 1 tópico por estación (auto-create) |
| **Procesamiento Batch** | Spark ETL | PySpark 4.1.2 | Convierte .txt → Parquet particionado |
| **Procesamiento Streaming** | Spark Structured Streaming | PySpark + Kafka connector | Parseo, detección de anomalías, PostgreSQL sink |
| **Almacenamiento** | Parquet | Snappy compression | ~14 MB, 1,073,151 registros, Hive-partitioned |
| | PostgreSQL 15 | psycopg2 + JDBC | Datos streaming con ON CONFLICT DO NOTHING |
| **Observabilidad** | Prometheus + Grafana | Exporters + Dashboards | Métricas de Kafka, lag, brokers, alertas |
| **Visualización** | Streamlit + Plotly | Dashboard interactivo | Histórico + Tiempo Real + Métricas del Stack |

### Flujo de Datos

```
1. SENAMHI (.txt) ──[ETL Batch]──▶ Parquet Histórico (artifacts/weather_data)
2. Supabase (tabla_N) ──[Bridge N]──▶ Kafka (clima-{ESTACION})
3. Kafka (clima-{ESTACION}) ──[Spark Streaming N]──▶ Kafka (clima-{ESTACION}-anomalias)
                                                      ──▶ Parquet streaming (artifacts/parquet_output/)
                                                      ──▶ PostgreSQL (sensor_data_{ESTACION})
4. Spark filtra parquet por ubicación de sensor_catalog.json y calcula avg/stddev reales
5. Dashboard lee sensor_catalog.json, selecciona estación, muestra datos + anomalías
```

## Estructura del Proyecto

```
clime-peru/
│
├── batch/                               # Procesamiento Batch
│   ├── etl_senamhi.py                   #   ETL PySpark (60 archivos → Parquet)
│   ├── etl_pandas.py                    #   ETL alternativo sin Spark
│   └── utils.py                         #   Utilidades: parsing, dataclasses
│
├── streaming/                           # Procesamiento en Tiempo Real
│   ├── kafka_consumer.py                #   Consumidor/Productor Kafka + simulador
│   ├── supabase_consumer.py             #   Consumidor Supabase + mock
│   ├── supabase_kafka_bridge.py         #   Puente Supabase → Kafka con checkpoint persistente
│   └── spark_streaming_processor.py     #   Spark Structured Streaming (3 sinks: Kafka, Parquet, PG)
│
├── dashboard/                           # Interfaz Web (Streamlit)
│   ├── app.py                           #   Aplicación principal
│   └── utils/
│       └── __init__.py                  #   Temas, charts, Supabase, métricas
│
├── config/                              # Configuración Centralizada
│   ├── __init__.py                      #   ConfigManager + dataclasses (incl. DatabaseConfig)
│   ├── config.yaml                      #   YAML con todos los parámetros
│   └── logger.py                        #   Logging coloreado + archivos rotativos
│
├── docker/                              # Infraestructura Docker
│   ├── docker-compose.yml               #   Stack completo (14 servicios)
│   ├── docker-compose.dev.yml           #   Dev: solo Kafka + UI
│   ├── Dockerfile.jupyter               #   Jupyter Lab + PySpark
│   ├── Dockerfile.dashboard             #   Streamlit dashboard
│   ├── Dockerfile.streaming             #   Bridge + Spark processor
│   ├── Dockerfile.etl                   #   ETL batch container
│   ├── notebooks/                       #   Notebooks Jupyter
│   ├── prometheus/                      #   prometheus.yml + alert.rules.yml
│   └── grafana/                         #   Provisioning + dashboards
│
├── data/                                # Datos
│   ├── raw/                             #   60 archivos .txt SENAMHI
│   └── processed/
│
├── artifacts/                           # Salidas
│   ├── weather_data/                    #   Parquet histórico (particionado)
│   ├── stations_metadata.parquet/       #   Metadatos de estaciones
│   ├── bridge_checkpoints/              #   Checkpoints por tabla (último id publicado)
│   ├── checkpoints/                     #   Checkpoints de streaming Spark
│   ├── parquet_output/                  #   Salida streaming a Parquet
│   ├── sensor_catalog.json              #   Mapeo tabla Supabase → estación → ubicación
│   └── sensor_config.json               #   Estación activa (dashboard)
│
├── docs/                                # Documentación (generador .docx)
│
├── tests/                               # Tests unitarios
│   ├── test_etl.py                      #   Pruebas de parsing ETL
│   └── test_utils.py                    #   Pruebas de validación/anomalías
│
└── logs/                                # Logs de aplicación
```

## Stack Tecnológico

| Componente | Versión | Imagen Docker | Propósito |
|-----------|---------|--------------|-----------|
| Apache Kafka | 4.2.0 | `apache/kafka:4.2.0` | Broker de mensajería (KRaft, sin ZK) |
| Kafka Exporter | 1.9.0 | `danielqsj/kafka-exporter:v1.9.0` | Exporta métricas Kafka a Prometheus |
| Apache Spark | 4.1.2 (PySpark) | `python:3.10-slim` + JARs | ETL batch y Structured Streaming |
| Prometheus | latest | `prom/prometheus:latest` | Recolección de métricas (scrape 15s) |
| Grafana | latest | `grafana/grafana:latest` | Dashboards de observabilidad |
| Kafka UI | latest | `provectuslabs/kafka-ui` | Gestión visual de tópicos |
| Streamlit | latest | `python:3.11-slim` | Dashboard interactivo |
| PostgreSQL | 15 | `postgres:15` | Almacenamiento persistente datos streaming |
| Supabase | SaaS | - | Base de datos + Realtime WebSocket |
| Parquet | - | - | Formato columnar comprimido (snappy) |

## Requisitos Previos

- **Docker Desktop** con WSL2 backend (Windows) o Docker Engine (Linux)
- **Docker Compose** (incluido con Docker Desktop)
- **Python 3.10+** (solo para desarrollo local sin Docker)

## Inicio Rápido (Docker)

### 1. Clonar y entrar al proyecto

```bash
git clone <repo-url> clime-peru
cd clime-peru
```

### 2. Levantar todo el stack

```bash
cd docker
docker compose up -d
```

Esto inicia los 14 servicios. La primera vez descarga imágenes y construye contenedores (~5-10 min).

### 3. Verificar que todo esté corriendo

```bash
docker ps
```

Deberías ver 14 contenedores con estado `Up`.

### 4. Ejecutar ETL batch (cargar datos históricos)

```bash
docker exec clime-jupyter python -m batch.etl_senamhi
```

Procesa 60 archivos .txt → ~1M registros → Parquet particionado.

### 5. Verificar el dashboard

Abrir http://localhost:8501 — pestaña **📊 Datos Históricos** con filtros por departamento.

### 6. Verificar datos en PostgreSQL

```bash
docker exec clime-postgres psql -U clime -d climedb -c "SELECT COUNT(*) FROM sensor_data_grupo_2"
```

## Servicios y Puertos

| Servicio | Contenedor | Puerto Interno | Puerto Externo | URL | Credenciales |
|----------|-----------|---------------|---------------|-----|--------------|
| **Dashboard** | `clime-dashboard` | 8501 | 8501 | http://localhost:8501 | — |
| **Jupyter Lab** | `clime-jupyter` | 8888 | 8888 | http://localhost:8888 | sin token |
| **Spark UI** | `clime-jupyter` | 4040 | 4040 | http://localhost:4040 | — |
| **Kafka Broker** | `clime-kafka` | 9092 / 19092 | 19092 | localhost:19092 | — |
| **Kafka UI** | `clime-kafka-ui` | 8080 | 18085 | http://localhost:18085 | — |
| **Kafka Exporter** | `clime-kafka-exporter` | 9308 | 19308 | http://localhost:19308/metrics | — |
| **Prometheus** | `clime-prometheus` | 9090 | 19090 | http://localhost:19090 | — |
| **Grafana** | `clime-grafana` | 3000 | 13000 | http://localhost:13000 | `admin` / `admin` |
| **PostgreSQL** | `clime-postgres` | 5432 | 15432 | localhost:15432 | `clime` / `clime123` |
| **Bridge grupo_2** | `clime-bridge-grupo2` | — | — | — | — |
| **Bridge grupo_3** | `clime-bridge-grupo3` | — | — | — | — |
| **Bridge grupo_4** | `clime-bridge-grupo4` | — | — | — | — |
| **Spark grupo_2** | `clime-spark-grupo2` | — | — | — | — |
| **Spark grupo_3** | `clime-spark-grupo3` | — | — | — | — |
| **Spark grupo_4** | `clime-spark-grupo4` | — | — | — | — |

### Tópicos Kafka (auto-creados)

Cada estación tiene su propio par de tópicos (datos crudos + anomalías):

| Tópico | Propósito |
|--------|-----------|
| `clima-grupo_2` | Datos crudos del sensor PUNO/LAMPA/LAMPA |
| `clima-grupo_2-anomalias` | Anomalías detectadas por Spark para grupo_2 |
| `clima-grupo_3` | Datos crudos del sensor PUNO/PUNO/PUNO |
| `clima-grupo_3-anomalias` | Anomalías detectadas por Spark para grupo_3 |
| `clima-grupo_4` | Datos crudos del sensor PUNO/AZANGARO/AZANGARO |
| `clima-grupo_4-anomalias` | Anomalías detectadas por Spark para grupo_4 |

Los tópicos de datos los crean los **bridges** al publicar. Los de anomalías los crean los **sparks** al escribir el primer microbatch (`auto.create.topics.enable=true`).

### Tablas PostgreSQL

| Tabla | Estación | Columnas |
|-------|----------|----------|
| `sensor_data_grupo_2` | grupo_2 (PUNO/LAMPA/LAMPA) | 17: id, sensor_id, estacion, department, province, district, temperatura, humedad, presion, altura, iaq, eco2, voc, calidad_aire, ts, created_at, processed_at |
| `sensor_data_grupo_3` | grupo_3 (PUNO/PUNO/PUNO) | misma estructura |
| `sensor_data_grupo_4` | grupo_4 (PUNO/AZANGARO/AZANGARO) | misma estructura |

### sensor_catalog.json

Archivo que mapea cada tabla de Supabase a su estación y ubicación geográfica real:

```json
{
  "grupo_3_air_quality": {
    "estacion": "grupo_3",
    "department": "PUNO",
    "province": "PUNO",
    "district": "PUNO"
  },
  "grupo_2_air_quality": {
    "estacion": "grupo_2",
    "department": "PUNO",
    "province": "LAMPA",
    "district": "LAMPA"
  },
  "grupo4_air_quality": {
    "estacion": "grupo_4",
    "department": "PUNO",
    "province": "AZANGARO",
    "district": "AZANGARO"
  }
}
```

El bridge lo usa para setear `department`, `province`, `district` en cada mensaje Kafka.
Spark lo usa para filtrar el parquet histórico y calcular `avg(tmax)` y `stddev(tmax)` reales de esa ubicación.

## Pipeline de Datos

### ETL Batch (SENAMHI → Parquet)

**Componente**: `batch/etl_senamhi.py`

| Etapa | Descripción |
|-------|-------------|
| **Extract** | Lee 60 archivos .txt con formato `ESTACION-DEPARTAMENTO-PROVINCIA-DISTRITO.txt` |
| **Transform** | Parsea líneas (6 columnas: YYYY MM DD precip tmax tmin), filtra rangos inválidos |
| **Load** | Escribe Parquet particionado por `department/province/district/year`, compresión snappy |

**Resultado**: 1,073,151 registros, 60 estaciones, 11 departamentos, 14 MB.

### Puente Supabase → Kafka (3 instancias)

**Componente**: `streaming/supabase_kafka_bridge.py`

| Parámetro CLI | Ejemplo | Descripción |
|--------------|---------|-------------|
| `--table` | `grupo_3_air_quality` | Tabla en Supabase |
| `--topic` | `clima-grupo_3` | Tópico Kafka de salida |
| `--time-format` | `string` o `timestamptz` | Formato de `created_at` |

| Mecanismo | Descripción |
|-----------|-------------|
| **Carga inicial** | Todos los registros históricos vía REST API (paginación 1000, desde id > checkpoint) |
| **Checkpoint persistente** | Guarda `last_id` en `/app/artifacts/bridge_checkpoints/{tabla}.json` entre reinicios |
| **Realtime** | WebSocket `postgres_changes` para INSERT en la tabla configurada |
| **Polling** | Cada 30s consulta `id > last_id` como respaldo |
| **Catálogo** | Lee `sensor_catalog.json` para obtener `department`, `province`, `district` según `--table` |
| **Normalización** | Convierte timestamps a ISO 8601 con offset `-05:00` antes de publicar |

Se ejecutan 3 bridges en simultáneo (uno por tabla Supabase):

```bash
bridge-grupo2: --table grupo_2_air_quality --topic clima-grupo_2 --time-format string
bridge-grupo3: --table grupo_3_air_quality --topic clima-grupo_3 --time-format string
bridge-grupo4: --table grupo4_air_quality  --topic clima-grupo_4 --time-format string
```

### Spark Streaming (Kafka → Anomalías + Parquet + PostgreSQL, 3 instancias)

**Componente**: `streaming/spark_streaming_processor.py`

| Parámetro CLI | Ejemplo | Descripción |
|--------------|---------|-------------|
| `--input-topic` | `clima-grupo_2` | Tópico Kafka de entrada |
| `--anomaly-topic` | `clima-grupo_2-anomalias` | Tópico de anomalías |

```
Kafka (clima-{ESTACION})
  ──▶ Parseo JSON (incluye sensor_id, temperatura, humedad, ts, department...)
  ──▶ Normalización de ts (elimina timezone y microsegundos → yyyy-MM-ddTHH:mm:ss)
  ──▶ Cálculo de stats históricos (filtra parquet por ubicación del catálogo)
  ──▶ Detección de anomalías (avg ± sigma × std)
  ──▶ Kafka (clima-{ESTACION}-anomalias) [anomalyScore + isAnomaly + anomalyType]
  ──▶ Parquet streaming (artifacts/parquet_output/{estacion}/)
  ──▶ PostgreSQL (sensor_data_{estacion}) [ON CONFLICT (id) DO NOTHING]
```

**Parámetros de procesamiento**:

| Parámetro | Valor |
|-----------|-------|
| Trigger | 5 segundos (micro-batch) |
| Watermark | 30 segundos |
| Ventana | 1 minuto (tumbling) |
| Output mode | append |
| Sigma anomalía | 2.0 (configurable) |
| Stats históricos | `avg(tmax)` y `stddev(tmax)` del parquet según ubicación real |
| Starting offsets | `earliest` (procesa todo desde 0 en primera ejecución) |

### Dashboard Streamlit

**Componentes**: `dashboard/app.py` + `dashboard/utils/__init__.py`

| Pestaña | Funcionalidad |
|---------|--------------|
| 📊 **Datos Históricos** | Filtros por departamento/provincia/estación, rango de fechas, granularidad diaria/mensual/anual. Gráficos de series temporales, box plots, mapa de estaciones, tabla descargable. |
| ⏱️ **Tiempo Real** | Consume de Kafka vía consumer group `dashboard-consumer`. Métricas de temperatura, humedad, IAQ (eCO₂/VOC), presión. Panel de streaming 60s. Gráficos multi-eje. |
| 📡 **Métricas del Stack** | Offsets de Kafka, lag, brokers, estado de conexión vía Prometheus. |

## Observabilidad

### Métricas en Prometheus

Endpoint: http://localhost:19090

| Consulta PromQL | Descripción |
|----------------|-------------|
| `kafka_brokers` | Número de brokers activos |
| `kafka_topic_partition_current_offset{topic="clima-grupo_2"}` | Offset por tópico |
| `kafka_consumergroup_lag` | Lag por consumer group |
| `up{job="kafka-exporter"}` | Estado del Kafka Exporter |

### Dashboards en Grafana

Endpoint: http://localhost:13000 (admin/admin)

- **Kafka Overview - ClimePeru**: Paneles de brokers, offsets, lag, estado del exporter.

### Alertas

| Alerta | Expresión | For | Descripción |
|--------|-----------|-----|-------------|
| `KafkaExporterDown` | `up{job="kafka-exporter"} == 0` | 1m | Exporter no responde |
| `HighKafkaLag` | `kafka_consumergroup_lag > 100` | 2m | Lag excede 100 mensajes |
| `UnderReplicatedPartitions` | `kafka_topic_partition_under_replicated_partitions > 0` | 1m | Particiones sub-replicadas |
| `KafkaBrokerDown` | `kafka_brokers < 1` | 1m | Sin brokers disponibles |

### Logs

Logs estructurados con formato coloreado, archivos rotativos diarios en `/app/logs/`.

## Parámetros de Configuración

Archivo: `config/config.yaml`

| Sección | Parámetros Clave |
|---------|-----------------|
| **spark** | master: `local[*]`, memory: `2g`, adaptive: true |
| **kafka** | bootstrap: `kafka:9092` |
| **streaming** | trigger: `5 seconds`, watermark: `30 seconds`, checkpoint: `/app/artifacts/checkpoints` |
| **sensor** | sigma: `2.0`, temp range: `-20` a `40` °C |
| **database** | url: `jdbc:postgresql://postgres:5432/climedb`, user: `clime`, table_prefix: `sensor_data_`, batch_size: 100 |

## Troubleshooting

### Limpieza completa de streaming

Para reiniciar todo el pipeline streaming desde 0:

```bash
# 1. Detener servicios streaming
docker compose -f docker/docker-compose.yml stop bridge-grupo2 bridge-grupo3 bridge-grupo4 spark-grupo2 spark-grupo3 spark-grupo4 dashboard

# 2. Eliminar topics Kafka
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_2
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_3
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_4
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_2-anomalias
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_3-anomalias
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic clima-grupo_4-anomalias

# 3. Eliminar artifacts locales
rm -rf artifacts/checkpoints artifacts/parquet_output artifacts/bridge_checkpoints

# 4. Drop tablas PostgreSQL
docker exec clime-postgres psql -U clime -d climedb -c "DROP TABLE IF EXISTS sensor_data_grupo_2, sensor_data_grupo_3, sensor_data_grupo_4"

# 5. Levantar todo de nuevo
docker compose -f docker/docker-compose.yml up -d
```

### Error: Container name conflict
```bash
docker rm -f clime-jupyter clime-kafka clime-dashboard clime-spark-grupo2 clime-spark-grupo3 clime-spark-grupo4 clime-bridge-grupo2 clime-bridge-grupo3 clime-bridge-grupo4
cd docker && docker compose up -d
```

### Error: Kafka connection refused
```bash
docker compose -f docker/docker-compose.yml restart kafka
```

### Error: Bridge no conecta a Supabase
```bash
docker logs clime-bridge-grupo2
docker restart clime-bridge-grupo2
```

### Ver estado de todos los servicios
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
