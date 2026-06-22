# Costos y Estrategia de Escalado

## Recursos Actuales (Local Docker)

### Consumo de Recursos

| Recurso | Estimado | Detalle |
|---|---|---|
| **CPU** | 4-6 cores | Spark local[*] (2 c/u), Kafka, bridges |
| **RAM** | 8-12 GB | Spark 2g × 4 (3 streaming + 1 ETL), Kafka 2g, PostgreSQL 1g |
| **Disco** | ~5 GB | Parquet: 14 MB, Checkpoints: ~100 MB, Modelos: ~10 MB, Logs: variable |
| **Contenedores** | 15 | Ver sección Puertos y Servicios |

### Límites del Stack Local

| Componente | Limitación | Riesgo |
|---|---|---|
| Spark | `local[*]` — sin distribución | Sin tolerancia a fallos, un solo JVM |
| Kafka | 1 broker, 1 partición/topic | Sin HA, throughput limitado |
| PostgreSQL | Single instance | Punto único de fallo |
| Dashboard | 1 instancia Streamlit | Sin balanceo de carga |
| Bridges | 1 por tabla | Punto único de fallo por bridge |

## Estimación de Costos Cloud

### Opción 1: AWS MSK + EMR + RDS (Recomendada)

| Servicio | AWS | Instancia | Costo Estimado/mes |
|---|---|---|---|
| **Kafka** | MSK Serverless | 1 broker `kafka.m5.large` | ~$150 |
| **Spark** | EMR on EC2 | 1 nodo `m5.xlarge` (4 vCPU, 16 GB) | ~$200 |
| **PostgreSQL** | RDS | `db.t3.medium` (2 vCPU, 4 GB) | ~$80 |
| **Dashboard** | EC2 | `t3.small` (2 vCPU, 2 GB) | ~$25 |
| **Prometheus** | EC2 (incluido) | Mismo que dashboard | — |
| **Grafana** | EC2 (incluido) | Mismo que dashboard | — |
| **Almacenamiento** | S3 | 50 GB (Parquet + modelos + logs) | ~$5 |
| **Red** | Data transfer | 100 GB/mes | ~$10 |
| **Total** | | | **~$470/mes** |

### Opción 2: On-Premise (Servidor Dedicado)

| Componente | Especificación | Costo Estimado (unico) |
|---|---|---|
| Servidor | 8 cores, 32 GB RAM, 500 GB SSD | ~$1,500 |
| Docker Host | Linux + Docker Compose | — |
| Electricidad + Internet | | ~$50/mes |
| Mantenimiento | | ~$100/mes |
| **Total primer año** | | **~$3,300** |

### Opción 3: GCP (Alternativa)

| Servicio | GCP | Instancia | Costo/mes |
|---|---|---|---|
| Kafka | Pub/Sub Lite | 3 topics | ~$100 |
| Spark | Dataproc | 1 nodo n1-standard-4 | ~$150 |
| PostgreSQL | Cloud SQL | db-custom-2-4096 | ~$70 |
| Dashboard | Compute Engine | e2-small | ~$20 |
| **Total** | | | **~$340/mes** |

## Estrategia de Escalado

### Escalado Horizontal (Recomendado)

```mermaid
graph TB
    subgraph Actual["Estado Actual (Local)"]
        A1[1 broker Kafka]
        A2[1 Spark local[*]]
        A3[1 PostgreSQL]
    end

    subgraph Objetivo["Escalado Horizontal"]
        B1[3 brokers Kafka<br/>replicación factor 2]
        B2[Cluster Spark Standalone<br/>1 master + 3 workers]
        B3[PostgreSQL<br/>Primary + Read Replica]
        B4[Dashboard<br/>2+ instancias con LB]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3
```

### Plan de Escalado por Fases

#### Fase 1: Alta Disponibilidad (+$200/mes)

| Acción | Mejora | Costo Adicional |
|---|---|---|
| 3 brokers Kafka con replicación factor 2 | Tolerancia a fallos de broker | +$100/mes |
| Spark Standalone con 2 workers | Distribución de carga | +$100/mes |
| PostgreSQL con failover automático | HA en base de datos | +$50/mes |

#### Fase 2: Rendimiento (+$400/mes)

| Acción | Mejora | Costo Adicional |
|---|---|---|
| 5 particiones por tópico Kafka | Paralelismo de consumo | +$50/mes |
| Spark con 4 workers | Throughput 2x | +$200/mes |
| Dashboard auto-scalable | Múltiples usuarios | +$100/mes |
| Caché Redis para dashboard | Menor latencia | +$50/mes |

#### Fase 3: Producción Completa (+$800/mes)

| Acción | Mejora |
|---|---|
| Multi-región (us-east-1 + us-west-2) | DR (Disaster Recovery) |
| Kafka MirrorMaker 2 | Réplica cross-región |
| Spark Structured Streaming con exactly-once | Garantía de procesamiento |
| Pipeline CI/CD para ML modelos | Despliegue automatizado |
| Alertmanager con Slack/Email/PagerDuty | Notificaciones en tiempo real |

## Costos de ML Pipeline

### Entrenamiento

| Modelo | Tiempo | Costo EC2 (m5.xlarge) |
|---|---|---|
| Largo Plazo (4 estaciones) | ~5 min | ~$0.02 |
| Corto Plazo (3 grupos) | ~2 min | ~$0.01 |
| Re-entrenamiento semanal | ~7 min/semana | ~$0.12/mes |

### Inferencia

| Volumen | Predicciones/día | Costo |
|---|---|---|
| Bajo (< 100 consultas/día) | Dashboard local | Incluido |
| Medio (1,000 consultas/día) | Dashboard + API | ~$5/mes |
| Alto (10,000+ consultas/día) | API serverless (Lambda) | ~$20/mes |

## Recomendaciones de Optimización de Costos

1. **Spot Instances** para Spark EMR: ahorro del 60-70%.
2. **Auto-scaling** de Spark: agregar workers solo durante picos de datos.
3. **Lifecycle S3**: mover Parquet antiguo a Glacier después de 90 días.
4. **Grafana Cloud** gratis: hasta 10,000 series de métricas.
5. **Streamlit Sharing** gratis: para dashboard público.
