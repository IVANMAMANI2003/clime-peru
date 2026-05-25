# Config — Configuración Centralizada

Sistema de configuración unificada mediante dataclasses tipadas con carga desde YAML y sobrescritura por variables de entorno.

## Arquitectura

```
config/config.yaml ──▶ ConfigManager (singleton) ──▶ AppConfig (dataclasses)
                              │
                    Env vars (override)
```

## Archivos

### `config.yaml`

Archivo YAML maestro con todas las secciones:

```yaml
application:         # Nombre, versión, entorno, nivel de log
spark:               # app_name, master, memory, cores, adaptive, parquet_compression
paths:               # base, input, output, metadata, logs
kafka:               # bootstrap_servers, topics, consumer_group, timeout
supabase:            # url, api_key, table, poll_interval, realtime_enabled
etl:                 # input_pattern, partition_columns, date_format, null_values
sensor:              # min/max_temperature, anomaly_threshold_sigma
dashboard:           # host, port, title, page_icon, layout
streaming:           # trigger_interval, output_mode, checkpoint_location, output_parquet
api:                 # cors_origins, timeout_seconds, max_retries
```

### `__init__.py`

Implementa `ConfigManager` (singleton) que:

1. Lee `config.yaml` desde el mismo directorio
2. Parsea en dataclasses tipadas: `SparkConfig`, `PathsConfig`, `KafkaConfig`, `SupabaseConfig`, `ETLConfig`, `SensorConfig`, `DashboardConfig`, `StreamingConfig`
3. Aplica sobrescrituras de variables de entorno
4. Expone `get_config()` → `AppConfig`

### `logger.py`

Sistema de logging estructurado con:

- `ColoredFormatter`: Colores ANSI por nivel (DEBUG=cyan, INFO=green, WARNING=yellow, ERROR=red)
- `setup_logger()`: Configura handlers de consola y archivo
- `get_logger()`: Cache de loggers por nombre
- `LogContext`: Context manager para logging con contexto
- `create_daily_logger()`: Archivos rotativos por día

## Variables de Entorno

| Variable | Sección | Campo |
|----------|---------|-------|
| `SUPABASE_URL` | supabase | url |
| `SUPABASE_API_KEY` | supabase | api_key |
| `KAFKA_BOOTSTRAP_SERVERS` | kafka | bootstrap_servers |
| `KAFKA_TOPIC` | kafka | topic |

## Uso en Código

```python
from config import get_config

config = get_config()
print(config.kafka.topic)           # "clima-puno"
print(config.streaming.trigger_interval)  # "5 seconds"
print(config.sensor.anomaly_threshold_sigma)  # 2.0
```

## Valores por Defecto vs YAML

Todos los dataclasses tienen valores por defecto. Si una clave no existe en `config.yaml`, se usa el default de la dataclass. Esto permite tener configuraciones mínimas.
