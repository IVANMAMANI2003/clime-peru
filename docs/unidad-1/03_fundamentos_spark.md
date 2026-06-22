# 3. Fundamentos Apache Spark

## 3.1 Introducción

Apache Spark es el motor de procesamiento unificado utilizado en CliMePerú tanto para ETL batch como para procesamiento streaming. Se ejecuta en modo `local[*]` dentro de contenedores Docker con PySpark 4.1.2 y Java 17.

## 3.2 Configuración de Spark

La configuración se centraliza en `config/config.yaml` y se aplica a través de `SparkSession.Builder`:

```yaml
spark:
  app_name: "ClimePeru"
  master: "local[*]"
  memory: "2g"
  cores: 2
  adaptive: true
  parquet_compression: "snappy"
```

### En el Código

```python
from config import get_config

config = get_config()
spark = SparkSession.builder \
    .appName(config.spark.app_name) \
    .master(config.spark.master) \
    .config("spark.sql.adaptive.enabled", str(config.spark.adaptive).lower()) \
    .config("spark.driver.memory", config.spark.memory) \
    .config("spark.sql.parquet.compression.codec", config.spark.parquet_compression) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:4.1.2") \
    .getOrCreate()
```

## 3.3 SparkSession

`SparkSession` es la entrada unificada a todas las funcionalidades de Spark en PySpark.

### Creación

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MiAplicacion") \
    .master("local[*]") \
    .getOrCreate()
```

### Lectura de Datos

```python
# Parquet
df = spark.read.parquet("artifacts/weather_data/")

# CSV
df = spark.read.option("header", "true").csv("data/input.csv")

# Kafka (streaming)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "clima-grupo_2") \
    .load()
```

### Escritura de Datos

```python
# Parquet
df.write.mode("overwrite").parquet("output/")

# Kafka
df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "clima-grupo_2-anomalias") \
    .start()
```

## 3.4 DataFrame API

El DataFrame API es la abstracción principal para trabajar con datos estructurados.

### Esquemas Tipados

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperatura", DoubleType(), True),
    StructField("humedad", DoubleType(), True),
    StructField("presion", DoubleType(), True),
    StructField("iaq", DoubleType(), True),
    StructField("eco2", DoubleType(), True),
    StructField("voc", DoubleType(), True),
    StructField("calidad_aire", StringType(), True),
    StructField("department", StringType(), True),
    StructField("province", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("created_at", StringType(), True),
])
```

### Transformaciones Comunes

```python
from pyspark.sql.functions import col, avg, stddev, when, abs as spark_abs

# Selección
df.select("sensor_id", "temperatura")

# Filtro
df.filter(col("temperatura").isNotNull())
df.filter(col("department") == "PUNO")

# Agregación
df.groupBy("department").agg(
    avg("tmax").alias("avg_tmax"),
    stddev("tmax").alias("std_tmax")
)
```

## 3.5 Catálogo de Sensores

El archivo `artifacts/sensor_catalog.json` mapea cada tabla de Supabase a su ubicación geográfica real, permitiendo que Spark filtre el Parquet histórico y calcule estadísticos específicos por ubicación:

```json
{
  "grupo_3_air_quality": {
    "estacion": "grupo_3",
    "department": "PUNO",
    "province": "PUNO",
    "district": "PUNO"
  }
}
```

## 3.6 Uso en CliMePerú

### ETL Batch (`batch/etl_senamhi.py`)

- `SparkContext.wholeTextFiles()` para leer múltiples archivos pequeños.
- `DataFrame.write.partitionBy()` para escribir Parquet particionado.
- Transformaciones con expresiones regulares para parsear nombres de archivos.

### Spark Streaming (`streaming/spark_streaming_processor.py`)

- `readStream.format("kafka")` para consumir desde Kafka.
- `from_json` con esquema tipado para parsear mensajes JSON.
- `writeStream.format("kafka")` para publicar anomalías.
- `foreachBatch` para escritura PostgreSQL con upsert.
- Watermark y ventanas para agregaciones temporales.

## 3.7 Optimizaciones

### Spark SQL Adaptive (AQE)

Habilitado vía `spark.sql.adaptive.enabled=true` para:

- Coalescing dinámico de particiones.
- Optimización de joins basada en estadísticas.
- Ajuste dinámico de tamaños de shuffle.

### Particionamiento

El Parquet histórico está particionado por `department/province/district/year` para:

- **Podado de particiones**: Spark lee solo los directorios relevantes al filtrar por ubicación.
- **Consultas eficientes**: Los estadísticos históricos se calculan en segundos aún con 1M+ registros.

### Broadcast Join

Para el catálogo de sensores (relativamente pequeño):

```python
from pyspark.sql.functions import broadcast

df_streaming.join(broadcast(df_catalogo), "estacion")
```

## 3.8 Referencia Rápida de Funciones

| Función | Propósito | Ejemplo |
|---|---|---|
| `from_json` | Parsear JSON string → columnas | `from_json(col("value"), schema)` |
| `to_json` | Columnas → JSON string | `to_json(struct(*))` |
| `regexp_replace` | Reemplazo regex en strings | `regexp_replace(col("ts"), "\.\d+", "")` |
| `col` | Referencia a columna | `col("temperatura")` |
| `when/otherwise` | When condicional | `when(col("x") > 0, "positivo").otherwise("negativo")` |
| `window` | Ventana temporal | `window(col("ts"), "1 minute")` |
| `avg/stddev` | Agregaciones | `groupBy("estacion").agg(avg("temperatura"))` |

## 3.9 Logging y Depuración

Los pipelines Spark en CliMePerú loguean automáticamente:

- **Batch ID** y número de filas por micro-batch.
- **Tasa de procesamiento** (filas/segundo).
- **Lag de offsets** (promedio y máximo).
- **Tiempo de procesamiento** por etapa.

```bash
docker logs clime-spark-grupo2 | grep "batchId"
```
