# Guía de Inicio Rápido

## Requisitos Previos

- **Docker Desktop** con WSL2 backend (Windows) o Docker Engine (Linux/Mac)
- **Docker Compose** (incluido con Docker Desktop)
- **Git** para clonar el repositorio
- **Python 3.10+** (solo para desarrollo local sin Docker)

## Paso 1: Clonar el Repositorio

```bash
git clone <repo-url> clime-peru
cd clime-peru
```

## Paso 2: Levantar el Stack Completo

```bash
cd docker
docker compose up -d
```

Esto inicia **15 servicios** (la primera vez descarga imágenes y construye contenedores, ~5-10 min).

### Verificar Contenedores

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Deberías ver los 15 contenedores en estado `Up`:

```
NAMES                    STATUS         PORTS
clime-kafka              Up             0.0.0.0:19092->9092/tcp
clime-kafka-ui           Up             0.0.0.0:18085->8080/tcp
clime-kafka-exporter     Up             0.0.0.0:19308->9308/tcp
clime-kafka-value-exporter Up           0.0.0.0:8000->8000/tcp
clime-postgres           Up             0.0.0.0:15432->5432/tcp
clime-prometheus         Up             0.0.0.0:19090->9090/tcp
clime-grafana            Up             0.0.0.0:13000->3000/tcp
clime-jupyter            Up             0.0.0.0:8888->8888/tcp, 0.0.0.0:4040->4040/tcp
clime-dashboard          Up             0.0.0.0:8501->8501/tcp
clime-bridge-grupo2      Up
clime-bridge-grupo3      Up
clime-bridge-grupo4      Up
clime-spark-grupo2       Up
clime-spark-grupo3       Up
clime-spark-grupo4       Up
```

## Paso 3: Cargar Datos Históricos (ETL Batch)

```bash
docker exec clime-jupyter python -m batch.etl_senamhi
```

Procesa 60 archivos `.txt` → ~1M registros → Parquet particionado (~14 MB). Tiempo estimado: 2-5 minutos.

## Paso 4: Verificar el Pipeline

### Dashboard

Abrir [http://localhost:8501](http://localhost:8501)

- Pestaña **📊 Datos Históricos**: explora los datos SENAMHI.
- Pestaña **⏱️ Tiempo Real**: datos streaming desde Kafka.
- Pestaña **🤖 Predicciones ML**: predicciones con XGBoost.

### PostgreSQL

```bash
docker exec clime-postgres psql -U clime -d climedb -c "SELECT COUNT(*) FROM sensor_data_grupo_2"
```

### Kafka UI

Abrir [http://localhost:18085](http://localhost:18085) para ver tópicos, offsets y mensajes.

## Paso 5: Entrenar Modelos ML (Opcional)

Si deseas re-entrenar los modelos con los datos actuales:

```bash
# Largo plazo
docker exec clime-jupyter python -m ml.train_largo_plazo

# Corto plazo
docker exec clime-jupyter python -m ml.train_corto_plazo
```

## Paso 6: Acceder a la Observabilidad

| Herramienta | URL | Credenciales |
|---|---|---|
| Grafana | [http://localhost:13000](http://localhost:13000) | admin / admin |
| Prometheus | [http://localhost:19090](http://localhost:19090) | — |
| Kafka UI | [http://localhost:18085](http://localhost:18085) | — |

## Resumen de Acceso por Servicio

| Servicio | URL / Conexión |
|---|---|
| Dashboard | `http://localhost:8501` |
| JupyterLab | `http://localhost:8888/lab?token=sintoken` |
| Spark UI | `http://localhost:4040` |
| PostgreSQL | `localhost:15432` (user=clime, db=climedb) |
| Kafka | `localhost:19092` |
| Prometheus | `http://localhost:19090` |
| Grafana | `http://localhost:13000` (admin/admin) |
| Kafka UI | `http://localhost:18085` |

## Comandos Útiles

```bash
# Ver logs de un servicio específico
docker logs clime-bridge-grupo2
docker logs clime-spark-grupo2
docker logs clime-dashboard

# Detener el stack
docker compose down

# Detener y eliminar volúmenes (borra PostgreSQL y Grafana data)
docker compose down -v

# Reiniciar un servicio
docker compose restart dashboard
```
