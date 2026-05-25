# CimaPerú — Sistema de Monitoreo Climático Inteligente

Sistema Big Data híbrido que integra datos climáticos históricos del SENAMHI con lecturas en tiempo real de sensores IoT, utilizando **Apache Kafka** como backbone de mensajería y **Apache Spark Structured Streaming** para detección de anomalías, con observabilidad completa del pipeline.

## Arquitectura General

```
                         ┌─────────────────────────────────────────────────────┐
                         │                   CIMAPERÚ STACK                     │
                         │                                                     │
┌──────────────┐         │  ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│   SENAMHI    │─────────│─▶│  SPARK   │────▶│ PARQUET  │    │  DASHBOARD   │  │
│  (.txt .dat) │         │  │ETL BATCH │    │HISTÓRICO │    │  STREAMLIT   │  │
└──────────────┘         │  └──────────┘    └─────┬────┘    │  :8501       │  │
                         │                        │         └──────▲───────┘  │
┌──────────────┐         │  ┌──────────┐    ┌─────▼─────┐         │         │
│   SUPABASE   │─────────│─▶│  KAFKA   │────▶│   SPARK   │─────────┘         │
│  (Sensores)  │ WebSocket│  │Bridge:9092│  │  STREAMING│ Anomalías          │
└──────────────┘ Poll 30s│  └──────────┘    └───────────┘                    │
                         │        │                                            │
                         │  ┌─────▼──────┐                                   │
                         │  │ KAFKA UI   │  :18085                            │
                         │  └────────────┘                                   │
                         │                                                     │
                         │  ┌──────────────────────────────────────────┐      │
                         │  │        OBSERVABILIDAD                    │      │
                         │  │  Kafka Exporter ─▶ Prometheus ─▶ Grafana│      │
                         │  │  (19308)           (19090)      (13000)  │      │
                         │  │  Alertas: Lag > 100, Brokers, Exporter  │      │
                         │  └──────────────────────────────────────────┘      │
                         └─────────────────────────────────────────────────────┘
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
- [Licencia](#licencia)

## Arquitectura

El sistema sigue una **arquitectura Kappa**: un único pipeline de streaming procesa tanto datos históricos (reprocesados) como en tiempo real.

| Capa | Componente | Tecnología | Propósito |
|------|-----------|-----------|-----------|
| **Fuentes** | SENAMHI | Archivos .txt | 60 estaciones meteorológicas históricas (1940-2015) |
| | Supabase | PostgreSQL + WebSocket | Lecturas de sensores IoT en tiempo real |
| **Ingesta** | Kafka | apache/kafka:4.2.0 (KRaft) | Mensajería distribuida, 2 tópicos |
| **Procesamiento Batch** | Spark ETL | PySpark 4.1.2 | Convierte .txt → Parquet particionado |
| **Procesamiento Streaming** | Spark Structured Streaming | PySpark + Kafka connector | Parseo, detección de anomalías, ventanas |
| **Almacenamiento** | Parquet | Snappy compression | ~14 MB, 1,073,151 registros, Hive-partitioned |
| **Observabilidad** | Prometheus + Grafana | Exporters + Dashboards | Métricas de Kafka, lag, brokers, alertas |
| **Visualización** | Streamlit + Plotly | Dashboard interactivo | Histórico + Tiempo Real + Métricas del Stack |

### Flujo de Datos

```
1. SENAMHI (.txt) ──[ETL Batch]──▶ Parquet Histórico (artifacts/weather_data)
2. Supabase (DB) ──[Bridge/WebSocket]──▶ Kafka (clima-puno)
3. Kafka (clima-puno) ──[Spark Streaming]──▶ Kafka (clima-anomalias) [solo anomalías]
4. Kafka (clima-puno) ──[Kafka Exporter]──▶ Prometheus ──▶ Grafana
5. Parquet + Supabase ──[Dashboard]──▶ Streamlit (localhost:8501)
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
│   ├── supabase_kafka_bridge.py         #   Puente Supabase → Kafka (WebSocket + REST)
│   └── spark_streaming_processor.py     #   Spark Structured Streaming (anomalías)
│
├── dashboard/                           # Interfaz Web (Streamlit)
│   ├── app.py                           #   Aplicación principal (723 líneas)
│   └── utils/
│       └── __init__.py                  #   Temas, charts, Supabase, métricas
│
├── config/                              # Configuración Centralizada
│   ├── __init__.py                      #   ConfigManager + dataclasses
│   ├── config.yaml                      #   YAML con todos los parámetros
│   └── logger.py                        #   Logging coloreado + archivos rotativos
│
├── docker/                              # Infraestructura Docker
│   ├── docker-compose.yml               #   Stack completo (9 servicios)
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
│   ├── checkpoints/                     #   Checkpoints de streaming
│   └── parquet_output/                  #   Salida streaming a Parquet
│
├── docs/                                # Documentación
│   ├── package.json                     #   Generador .docx (npm)
│   ├── generate-docx.js                 #   Script principal
│   ├── climeperu_doc_p1.js              #   Parte 1: portada, intro, kafka
│   ├── climeperu_doc_p2.js              #   Parte 2: spark, métricas, grafana
│   └── ClimePeru_Unidad2_Documentacion_Completa.docx
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
| Apache Spark | 4.1.2 (PySpark) | `python:3.10-slim` + JAR | ETL batch y Structured Streaming |
| Prometheus | latest | `prom/prometheus:latest` | Recolección de métricas (scrape 15s) |
| Grafana | latest | `grafana/grafana:latest` | Dashboards de observabilidad |
| Kafka UI | latest | `provectuslabs/kafka-ui` | Gestión visual de tópicos |
| Streamlit | latest | `python:3.11-slim` | Dashboard interactivo |
| Supabase | SaaS | - | Base de datos + Realtime WebSocket |
| Parquet | - | - | Formato columnar comprimido (snappy) |

## Requisitos Previos

- **Docker Desktop** con WSL2 backend (Windows) o Docker Engine (Linux)
- **Docker Compose** (incluido con Docker Desktop)
- **Python 3.10+** (solo para desarrollo local sin Docker)
- **Git** (opcional)

## Inicio Rápido (Docker)

### 1. Clonar y entrar al proyecto

```bash
git clone <repo-url> clime-peru
cd clime-peru
```

### 2. Levantar todo el stack

```bash
cd docker
docker compose up -d --build
```

Esto inicia los 9 servicios. La primera vez descarga imágenes y construye contenedores (~5-10 min).

### 3. Verificar que todo esté corriendo

```bash
docker ps
```

Deberías ver 9 contenedores con estado `Up`.

### 4. Crear tópicos Kafka

```bash
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --topic clima-puno --partitions 1 --replication-factor 1

docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --topic clima-anomalias --partitions 1 --replication-factor 1
```

### 5. Ejecutar ETL batch (cargar datos históricos)

```bash
docker exec clime-jupyter python -m batch.etl_senamhi
```

Procesa 60 archivos .txt → ~1M registros → Parquet particionado.

### 6. Verificar el dashboard

Abrir http://localhost:8501 — pestaña **📊 Datos Históricos** con filtros por departamento.

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
| **Supabase Bridge** | `clime-supabase-bridge` | — | — | — | — |
| **Spark Streaming** | `clime-spark-streaming` | — | — | — | — |

### Tópicos Kafka

| Tópico | Particiones | Replicación | Propósito |
|--------|------------|-------------|-----------|
| `clima-puno` | 1 | 1 | Datos crudos de sensores desde Supabase |
| `clima-anomalias` | 1 | 1 | Solo registros clasificados como anomalías |

### Ver offsets actuales

```bash
# Offset de clima-puno
docker exec clime-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:9092 --topic clima-puno

# Offset de clima-anomalias
docker exec clime-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:9092 --topic clima-anomalias
```

## Pipeline de Datos

### ETL Batch (SENAMHI → Parquet)

**Componente**: `batch/etl_senamhi.py`

| Etapa | Descripción |
|-------|-------------|
| **Extract** | Lee 60 archivos .txt con formato `ESTACION-DEPARTAMENTO-PROVINCIA-DISTRITO.txt` |
| **Transform** | Parsea líneas (6 columnas: YYYY MM DD precip tmax tmin), filtra rangos inválidos |
| **Load** | Escribe Parquet particionado por `department/province/district/year`, compresión snappy |

**Resultado**: 1,073,151 registros, 60 estaciones, 11 departamentos, 14 MB.

### Puente Supabase → Kafka

**Componente**: `streaming/supabase_kafka_bridge.py`

| Mecanismo | Descripción |
|-----------|-------------|
| **Carga inicial** | 500 registros más recientes vía REST API al iniciar |
| **Realtime** | WebSocket `postgres_changes` para INSERT en `grupo_3_air_quality` |
| **Polling** | Cada 30s consulta `id > last_id` como respaldo |

### Spark Streaming (Kafka → Anomalías)

**Componente**: `streaming/spark_streaming_processor.py`

```
Kafka (clima-puno) ──▶ Parseo JSON ──▶ Detección de anomalías ──▶ Kafka (clima-anomalias)
                                         │                        └── Solo es_anomalia == "SI"
                                         └── Parquet (checkpoint)
```

**Parámetros de procesamiento**:

| Parámetro | Valor |
|-----------|-------|
| Trigger | 5 segundos (micro-batch) |
| Watermark | 30 segundos |
| Ventana | 1 minuto (tumbling) |
| Output mode | append |
| Sigma anomalía | 2.0 (configurable) |

### Dashboard Streamlit

**Componentes**: `dashboard/app.py` + `dashboard/utils/__init__.py`

| Pestaña | Funcionalidad |
|---------|--------------|
| 📊 **Datos Históricos** | Filtros por departamento/provincia/estación, rango de fechas, granularidad diaria/mensual/anual. Gráficos de series temporales, box plots, mapa de estaciones, tabla descargable. |
| ⏱️ **Tiempo Real** | Actualización vía WebSocket cada 2s. Métricas de temperatura, humedad, IAQ (eCO₂/VOC), presión. Panel de streaming 60s. Gráficos multi-eje. |
| 📡 **Métricas del Stack** | Offsets de Kafka, lag, brokers, estado de conexión vía Prometheus. |

## Observabilidad

### Métricas en Prometheus

Endpoint: http://localhost:19090

| Consulta PromQL | Descripción |
|----------------|-------------|
| `kafka_brokers` | Número de brokers activos |
| `kafka_topic_partition_current_offset{topic="clima-puno"}` | Offset de clima-puno |
| `kafka_topic_partition_current_offset{topic="clima-anomalias"}` | Offset de clima-anomalias |
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
| **kafka** | bootstrap: `kafka:9092`, topics: `clima-puno`/`clima-anomalias` |
| **supabase** | table: `grupo_3_air_quality`, poll: 30s |
| **streaming** | trigger: `5 seconds`, watermark: implícito en código, checkpoint: `/app/artifacts/checkpoints` |
| **sensor** | sigma: `2.0`, temp range: `-20` a `40` °C |

## Troubleshooting

### Error: Container name conflict

```bash
docker rm -f clime-jupyter clime-kafka clime-dashboard clime-spark-streaming clime-supabase-bridge
cd docker && docker compose up -d
```

### Error: Kafka connection refused

```bash
docker compose -f docker/docker-compose.yml restart kafka
```

### Error: Spark no encuentra datos (offset changed)

Cuando se recrean tópicos Kafka, reiniciar Spark:

```bash
docker restart clime-spark-streaming
```

### Error: Dashboard no encuentra datos históricos

```bash
# Verificar que el Parquet existe
docker exec clime-dashboard ls /app/artifacts/weather_data

# Re-ejecutar ETL si es necesario
docker exec clime-jupyter python -m batch.etl_senamhi
```

### Error: Bridge no conecta a Supabase

```bash
docker logs clime-supabase-bridge
docker restart clime-supabase-bridge
```

### Ver estado de todos los servicios

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## Licencia

MIT License

## Autores

- **Ivan Yomar Mamani Merma** — Universidad Peruana Unión (UPeU)
- **Docente**: Abel Angel Sullon Macalupu
- Curso: Big Data — Unidad 2: Pipeline Streaming
