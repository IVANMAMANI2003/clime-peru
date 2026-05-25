# Docker — Infraestructura del Stack

Contiene toda la configuración de contenedores, Dockerfiles y archivos de provisioning para ejecutar el stack completo de CimaPerú.

## Stack Completo (9 Servicios)

```
docker compose up -d   # Levanta todo
docker compose down    # Detiene todo
docker compose logs -f # Logs en vivo
```

## Servicios

| Servicio | Contenedor | Imagen | Puerto | Depende de |
|----------|-----------|--------|--------|-----------|
| **kafka** | `clime-kafka` | `apache/kafka:4.2.0` | 19092 | — |
| **kafka-exporter** | `clime-kafka-exporter` | `danielqsj/kafka-exporter:v1.9.0` | 19308 | kafka |
| **prometheus** | `clime-prometheus` | `prom/prometheus:latest` | 19090 | kafka-exporter |
| **grafana** | `clime-grafana` | `grafana/grafana:latest` | 13000 | prometheus |
| **kafka-ui** | `clime-kafka-ui` | `provectuslabs/kafka-ui:latest` | 18085 | kafka |
| **jupyter** | `clime-jupyter` | `Dockerfile.jupyter` (build local) | 8888, 4040 | — |
| **supabase-bridge** | `clime-supabase-bridge` | `Dockerfile.streaming` (build local) | — | kafka |
| **spark-streaming** | `clime-spark-streaming` | `Dockerfile.streaming` (build local) | — | kafka |
| **dashboard** | `clime-dashboard` | `Dockerfile.dashboard` (build local) | 8501 | — |

## Dockerfiles

### `Dockerfile.jupyter`
- Base: `python:3.10-slim-bookworm`
- Java 17 (JRE) + PySpark 4.1.2 + Jupyter Lab
- Puertos: 8888 (Lab), 4040 (Spark UI)
- Volúmenes: `data/`, `artifacts/`, `notebooks/`

### `Dockerfile.streaming`
- Base: `python:3.10-slim-bookworm`
- Java 17 (JRE) + PySpark + kafka-python + supabase
- Usado tanto para `supabase-bridge` como para `spark-streaming`
- Comando override vía `docker-compose.yml`

### `Dockerfile.dashboard`
- Base: `python:3.11-slim`
- Streamlit + plotly + supabase + pyarrow
- Puerto: 8501
- CMD: `streamlit run app.py`

### `Dockerfile.etl`
- Base: `python:3.10-slim-bookworm`
- PySpark + pandas + pyarrow
- CMD: `python -m batch.etl_senamhi`

## Archivos de Configuración

### Prometheus (`prometheus/prometheus.yml`)

4 jobs de recolección:

| Job | Target | Puerto |
|-----|--------|--------|
| `prometheus` | localhost:9090 | Métricas internas |
| `kafka-exporter` | kafka-exporter:9308 | Métricas Kafka |
| `kafka-ui` | kafka-ui:8080 | Actuator metrics |
| `jupyter` | jupyter:8888 | Spark metrics |

### Alertas (`prometheus/rules/alert.rules.yml`)

4 reglas: KafkaExporterDown, HighKafkaLag, UnderReplicatedPartitions, KafkaBrokerDown.

### Grafana (`grafana/provisioning/`)

- **Datasource**: Prometheus auto-configurado
- **Dashboard**: Kafka Overview pre-instalado

## Volúmenes

| Volumen | Host → Contenedor | Servicios |
|---------|------------------|-----------|
| `../artifacts` | `/app/artifacts` | jupyter, dashboard |
| `../data` | `/app/data` | jupyter, dashboard |
| `grafana_data` (named) | `/var/lib/grafana` | grafana |

## Redes

Todos los servicios comparten la red `clime-net` (driver bridge). Kafka tiene alias de red `kafka` para resolución interna.

## Dev Stack

Para desarrollo rápido sin el stack completo:

```bash
cd docker
docker compose -f docker-compose.dev.yml up -d
```

Esto levanta solo Kafka + Kafka UI para pruebas de productores/consumidores.
