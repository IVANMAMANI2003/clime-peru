# CliMePerú — Sistema de Monitoreo Climático Inteligente

**CliMePerú** (Clima + Monitoreo + Perú) es un sistema Big Data híbrido que integra datos climáticos históricos del SENAMHI con lecturas en tiempo real de sensores IoT, utilizando **Apache Kafka** como backbone de mensajería, **Apache Spark Structured Streaming** para detección de anomalías, **XGBoost** para predicción de temperatura y **PostgreSQL** como almacenamiento persistente, con observabilidad completa del pipeline vía Prometheus y Grafana.

---

## Arquitectura General

```mermaid
graph TB
    subgraph Fuentes
        SENAMHI[SENAMHI .txt]
        SUPABASE[Supabase 3 tablas]
    end

    subgraph Ingesta
        BRIDGE1[bridge-grupo2]
        BRIDGE2[bridge-grupo3]
        BRIDGE3[bridge-grupo4]
    end

    subgraph Kafka["Apache Kafka"]
        K_G2[clima-grupo_2]
        K_G3[clima-grupo_3]
        K_G4[clima-grupo_4]
        KA_G2[clima-grupo_2-anomalias]
        KA_G3[clima-grupo_3-anomalias]
        KA_G4[clima-grupo_4-anomalias]
    end

    subgraph Spark["Spark Processing"]
        SPARK_ETL[Spark ETL Batch]
        SPARK_S2[Spark Streaming grupo_2]
        SPARK_S3[Spark Streaming grupo_3]
        SPARK_S4[Spark Streaming grupo_4]
    end

    subgraph Storage["Almacenamiento"]
        PARQUET_H[(Parquet Histórico<br/>1,073,151 registros)]
        PARQUET_S[(Parquet Streaming<br/>~374K registros)]
        POSTGRES[(PostgreSQL<br/>3 tablas)]
        MODELS[(Modelos ML<br/>7 .pkl)]
    end

    subgraph ML["Machine Learning XGBoost"]
        LP[Largo Plazo<br/>tmax diario]
        CP[Corto Plazo<br/>5-15 min]
    end

    subgraph Observabilidad
        PROM[Prometheus]
        GRAFANA[Grafana]
        KAFKA_UI[Kafka UI]
        EXP[Kafka Exporter]
        VALEXP[Value Exporter]
    end

    subgraph Dashboard["Dashboard"]
        HIST[📊 Históricos]
        RT[⏱️ Tiempo Real]
        ML_PRED[🤖 Predicciones ML]
        STACK[📡 Métricas Stack]
    end

    SENAMHI --> SPARK_ETL
    SPARK_ETL --> PARQUET_H

    SUPABASE --> BRIDGE1 --> K_G2
    SUPABASE --> BRIDGE2 --> K_G3
    SUPABASE --> BRIDGE3 --> K_G4

    K_G2 --> SPARK_S2
    K_G3 --> SPARK_S3
    K_G4 --> SPARK_S4

    PARQUET_H --> SPARK_S2
    PARQUET_H --> SPARK_S3
    PARQUET_H --> SPARK_S4

    SPARK_S2 --> KA_G2
    SPARK_S3 --> KA_G3
    SPARK_S4 --> KA_G4

    SPARK_S2 --> PARQUET_S
    SPARK_S3 --> PARQUET_S
    SPARK_S4 --> PARQUET_S

    SPARK_S2 --> POSTGRES
    SPARK_S3 --> POSTGRES
    SPARK_S4 --> POSTGRES

    PARQUET_H --> LP --> MODELS
    PARQUET_S --> CP --> MODELS

    MODELS --> ML_PRED

    K_G2 --> RT
    K_G3 --> RT
    K_G4 --> RT

    KA_G2 --> EXP
    KA_G3 --> EXP
    KA_G4 --> EXP

    EXP --> PROM --> GRAFANA
    VALEXP --> PROM
    KAFKA_UI --> PROM

    PROM --> STACK
```

## Stack Tecnológico

| Componente | Versión | Propósito |
|---|---|---|
| Apache Kafka | 4.2.0 | Broker de mensajería (KRaft, sin ZooKeeper) |
| Apache Spark | 4.1.2 (PySpark) | ETL batch y Structured Streaming |
| Prometheus | latest | Recolección de métricas |
| Grafana | latest | Dashboards de observabilidad |
| Kafka UI | latest | Gestión visual de tópicos |
| Streamlit | 1.58.0 | Dashboard interactivo |
| PostgreSQL | 15 | Almacenamiento persistente |
| XGBoost | latest | Modelos de regresión para predicción |
| Supabase | SaaS | Base de datos + Realtime WebSocket |
| Parquet | Snappy | Formato columnar comprimido |

## Estructura del Proyecto

```
clime-peru/
├── batch/                # ETL batch SENAMHI → Parquet
├── streaming/            # Bridges Supabase→Kafka + Spark Streaming
├── dashboard/            # Dashboard Streamlit (3 pestañas + ML)
├── ml/                   # Machine Learning (features, train, predict)
├── config/               # Configuración centralizada (YAML + dataclasses)
├── docker/               # Docker Compose + Dockerfiles + Prometheus/Grafana
├── data/                 # Datos SENAMHI crudos (.txt)
├── artifacts/            # Parquet, checkpoints, modelos
├── docs/                 # Documentación (MkDocs)
├── scripts/              # Scripts auxiliares
└── tests/                # Tests unitarios
```

## Enlaces Rápidos

- **Dashboard**: [http://localhost:8501](http://localhost:8501)
- **Kafka UI**: [http://localhost:18085](http://localhost:18085)
- **Prometheus**: [http://localhost:19090](http://localhost:19090)
- **Grafana**: [http://localhost:13000](http://localhost:13000) (admin/admin)
- **PostgreSQL**: `localhost:15432` (clime/climedb)
- **Repositorio**: [https://github.com/anomalyco/clime-peru](https://github.com/anomalyco/clime-peru)
