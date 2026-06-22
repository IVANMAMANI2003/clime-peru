# Troubleshooting

## Limpieza Completa del Pipeline Streaming

Para reiniciar todo el pipeline streaming desde cero (tópicos Kafka, checkpoints y tablas PostgreSQL):

```powershell
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

## Error: Container Name Conflict

```powershell
docker rm -f clime-jupyter clime-kafka clime-dashboard clime-spark-grupo2 clime-spark-grupo3 clime-spark-grupo4 clime-bridge-grupo2 clime-bridge-grupo3 clime-bridge-grupo4 clime-postgres clime-prometheus clime-grafana clime-kafka-ui clime-kafka-exporter clime-kafka-value-exporter

cd docker
docker compose up -d
```

## Error: Kafka Connection Refused

```powershell
# Verificar si Kafka está corriendo
docker ps | grep clime-kafka

# Ver logs
docker logs clime-kafka

# Reiniciar Kafka
docker compose -f docker/docker-compose.yml restart kafka

# Esperar 10 segundos y verificar
timeout /t 10
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Error: Bridge No Conecta a Supabase

```powershell
# Ver logs del bridge
docker logs clime-bridge-grupo2

# Verificar variables de entorno
docker exec clime-bridge-grupo2 env | grep SUPABASE

# Reiniciar bridge
docker restart clime-bridge-grupo2
```

## Error: Spark No Procesa Datos

```powershell
# Verificar que Kafka tenga datos
docker exec clime-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic clima-grupo_2

# Ver logs de Spark
docker logs clime-spark-grupo2

# Verificar checkpoint
ls artifacts/checkpoints/kafka/grupo_2/

# Para reiniciar Spark desde cero (después de limpiar topics)
rm -rf artifacts/checkpoints/kafka/grupo_2
docker restart clime-spark-grupo2
```

## Error: PostgreSQL Conexión Rechazada

```powershell
# Verificar contenedor
docker ps | grep clime-postgres

# Ver logs
docker logs clime-postgres

# Verificar conectividad desde Spark
docker exec clime-spark-grupo2 python -c "
import psycopg2
conn = psycopg2.connect(host='postgres', port=5432, user='clime', password='clime123', dbname='climedb')
print('Conexión exitosa')
"

# Reiniciar PostgreSQL
docker restart clime-postgres
```

## Error: Dashboard No Muestra Datos en Tiempo Real

```powershell
# Verificar que el dashboard pueda conectar a Kafka
docker logs clime-dashboard | grep kafka

# Verificar consumer group
docker exec clime-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group dashboard-consumer --describe

# Verificar que los topics tengan datos
docker exec clime-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic clima-grupo_2
```

## Error: Modelos ML No Cargados

```powershell
# Verificar que existan los archivos .pkl
docker exec clime-dashboard ls -la /app/ml/models/

# Ver logs del dashboard (sección ML)
docker logs clime-dashboard | grep -i "ml\|model\|predict"

# Re-entrenar modelos si es necesario
docker exec clime-jupyter python -m ml.train_largo_plazo
docker exec clime-jupyter python -m ml.train_corto_plazo
```

## Error: Prometheus Target Down

```powershell
# Ver targets en Prometheus
curl http://localhost:19090/api/v1/targets | python -m json.tool

# Verificar que los exporters estén activos
curl http://localhost:19308/metrics | head -5
curl http://localhost:8000/metrics | head -5
```

## Verificación Rápida del Estado

```powershell
# Todos los contenedores
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Topics Kafka
docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Datos en PostgreSQL
docker exec clime-postgres psql -U clime -d climedb -c "
SELECT 'grupo_2' as tabla, COUNT(*) as registros FROM sensor_data_grupo_2
UNION ALL
SELECT 'grupo_3', COUNT(*) FROM sensor_data_grupo_3
UNION ALL
SELECT 'grupo_4', COUNT(*) FROM sensor_data_grupo_4
UNION ALL
SELECT 'TOTAL', COUNT(*) FROM (
  SELECT id FROM sensor_data_grupo_2
  UNION ALL
  SELECT id FROM sensor_data_grupo_3
  UNION ALL
  SELECT id FROM sensor_data_grupo_4
) t
"

# Modelos ML
docker exec clime-dashboard ls -la /app/ml/models/
```

## Comandos de Diagnóstico Rápido

```powershell
# Estado de todos los servicios
function Get-ServiceStatus {
    Write-Host "=== CONTENEDORES ===" -ForegroundColor Cyan
    docker ps --format "table {{.Names}}\t{{.Status}}"
    
    Write-Host "`n=== TOPICS KAFKA ===" -ForegroundColor Cyan
    docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
    
    Write-Host "`n=== POSTGRESQL ===" -ForegroundColor Cyan
    docker exec clime-postgres psql -U clime -d climedb -c "\dt"
    
    Write-Host "`n=== MODELOS ML ===" -ForegroundColor Cyan
    docker exec clime-dashboard ls /app/ml/models/
}
```
