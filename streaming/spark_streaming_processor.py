import sys
import os
from pathlib import Path
from typing import Optional
import json
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, when, lit, concat_ws,
    avg as spark_avg, stddev as spark_stddev,
    current_timestamp, window, unix_millis, count as spark_count,
    regexp_replace, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    LongType, IntegerType, TimestampType
)

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger

logger = get_logger("spark_streaming")

CATALOG_PATH = "/app/artifacts/sensor_catalog.json"


def load_catalog() -> dict:
    try:
        with open(CATALOG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("sensor_catalog.json no encontrado en %s", CATALOG_PATH)
        return {}


class SparkStreamingProcessor:

    def __init__(self, input_topic: str = None, anomaly_topic: str = None):
        self.config = get_config()
        self.spark: Optional[SparkSession] = None
        self.historical_df: Optional[DataFrame] = None
        self.promedio_historico: float = 12.0
        self.desviacion_estandar: float = 2.5

        self.input_topic = input_topic or self.config.kafka.topic
        self.anomaly_topic = anomaly_topic or f"{self.input_topic}-anomalias"

        self.catalog = load_catalog()
        self.estacion = None
        self._resolve_station_from_topic()

    def _resolve_station_from_topic(self):
        for table_name, info in self.catalog.items():
            expected_topic = f"clima-{info['estacion']}"
            if expected_topic == self.input_topic:
                self.estacion = info["estacion"]
                self.department = info.get("department", "")
                self.province = info.get("province", "")
                self.district = info.get("district", "")
                logger.info("Resolved topic '%s' -> estacion '%s' (%s/%s/%s)",
                            self.input_topic, self.estacion, self.department, self.province, self.district)
                return
        self.estacion = self.input_topic.replace("clima-", "")
        self.department = ""
        self.province = ""
        self.district = ""
        logger.info("Topic '%s' no encontrado en catalogo, usando estacion inferida '%s'", self.input_topic, self.estacion)

    def create_spark_session(self) -> SparkSession:
        spark_version = "3.5.1"
        scala_version = "2.12"
        kafka_connector = (
            f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
        )

        builder = SparkSession.builder \
            .appName(f"ClimePeru-{self.estacion}") \
            .master(self.config.spark.master) \
            .config("spark.driver.memory", self.config.spark.memory) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.kafka.kafka.timeout", "30000") \
            .config("spark.jars.packages", ",".join([
                kafka_connector,
                "org.postgresql:postgresql:42.7.1",
            ])) \
            .config("spark.jars.repositories", "https://repo1.maven.org/maven2") \
            .config("spark.sql.streaming.schemaInference", "true")

        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session creada para '%s' (topic=%s)", self.estacion, self.input_topic)
        return self.spark

    def get_sensor_schema(self) -> StructType:
        return StructType([
            StructField("sensor_id", StringType(), True),
            StructField("estacion", StringType(), True),
            StructField("temperatura", FloatType(), True),
            StructField("humedad", FloatType(), True),
            StructField("presion", FloatType(), True),
            StructField("altura", FloatType(), True),
            StructField("iaq", FloatType(), True),
            StructField("eco2", FloatType(), True),
            StructField("voc", FloatType(), True),
            StructField("calidad_aire", StringType(), True),
            StructField("ts", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("id", LongType(), True),
            StructField("department", StringType(), True),
            StructField("province", StringType(), True),
            StructField("district", StringType(), True),
        ])

    def load_historical_data(self) -> DataFrame:
        path = self.config.paths.output
        try:
            df = self.spark.read.parquet(path)
            logger.info("Datos historicos cargados: %d registros desde %s", df.count(), path)
            return df
        except Exception as e:
            logger.warning("No se pudieron cargar datos historicos desde %s: %s", path, e)
            return None

    def calculate_stats_from_historical(self):
        if self.historical_df is None:
            logger.warning("Sin datos historicos, usando valores por defecto")
            return
        try:
            temp_col = "temperatura"
            if temp_col not in self.historical_df.columns:
                temp_col = "tmax"
            if temp_col not in self.historical_df.columns:
                raise ValueError(f"No se encontro columna de temperatura en: {self.historical_df.columns}")
            stats = self.historical_df.agg(
                spark_avg(temp_col).alias("promedio"),
                spark_stddev(temp_col).alias("desviacion")
            ).collect()[0]
            self.promedio_historico = stats["promedio"] or 12.0
            self.desviacion_estandar = stats["desviacion"] or 2.5
            logger.info("Stats historicos (col=%s): avg=%.2f°C std=%.2f°C",
                        temp_col, self.promedio_historico, self.desviacion_estandar)
        except Exception as e:
            logger.warning("Error calculando stats historicos: %s", e)

    def create_kafka_stream(self) -> DataFrame:
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

    def parse_sensor_data(self, stream_df: DataFrame) -> DataFrame:
        schema = self.get_sensor_schema()
        parsed_df = stream_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafkaTimestamp"),
            from_json(col("value").cast("string"), schema).alias("sensor")
        ).select("topic", "partition", "offset", "kafkaTimestamp", "sensor.*")
        return parsed_df

    def add_observability_fields(self, parsed_df: DataFrame) -> DataFrame:
        threshold_sigma = self.config.sensor.anomaly_threshold_sigma
        df = parsed_df \
            .withColumn("timestamp", col("kafkaTimestamp")) \
            .withColumn("temperatura", col("temperatura").cast("float")) \
            .withColumn("humedad", col("humedad").cast("float")) \
            .withColumn("presion", col("presion").cast("float")) \
            .withColumn("altura", col("altura").cast("float")) \
            .withColumn("iaq", col("iaq").cast("float")) \
            .withColumn("eco2", col("eco2").cast("float")) \
            .withColumn("voc", col("voc").cast("float")) \
            .withColumn("ts_normalized",
                        regexp_replace(
                            regexp_replace(col("ts"), "[+-]\\d{2}:\\d{2}$", ""),
                            "\\.\\d+", ""
                        )) \
            .withColumn("ts", col("ts_normalized")) \
            .drop("ts_normalized") \
            .withColumn("promedioHistorico", lit(self.promedio_historico)) \
            .withColumn("desviacionEstandar", lit(self.desviacion_estandar)) \
            .withColumn("thresholdSigma", lit(threshold_sigma)) \
            .withColumn("processedAt", current_timestamp())
        return df

    def detect_anomalies(self, obs_df: DataFrame) -> DataFrame:
        threshold_sigma = self.config.sensor.anomaly_threshold_sigma
        stddev_val = self.desviacion_estandar if self.desviacion_estandar > 0 else 1.0
        anomaly_df = obs_df \
            .withColumn("anomalyScore",
                        when(col("temperatura").isNotNull(),
                             (col("temperatura") - lit(self.promedio_historico)) / lit(stddev_val)
                             ).otherwise(lit(0.0))) \
            .withColumn("isAnomaly",
                        when(col("anomalyScore").isNotNull() &
                             (spark_abs(col("anomalyScore")) > lit(threshold_sigma)), lit(True))
                        .otherwise(lit(False))) \
            .withColumn("anomalyType",
                        when(col("isAnomaly") & (col("temperatura") > lit(self.promedio_historico)),
                             lit("alta")).when(col("isAnomaly"),
                             lit("baja")).otherwise(lit("normal")))
        return anomaly_df

    def write_to_kafka(self, anomaly_df: DataFrame):
        kafka_df = anomaly_df \
            .filter(col("isAnomaly") == True) \
            .select(
            to_json(struct(
                col("id"), col("sensor_id"), col("estacion"),
                col("department"), col("province"), col("district"),
                col("temperatura"), col("humedad"), col("presion"),
                col("altura"), col("iaq"), col("eco2"), col("voc"),
                col("calidad_aire"), col("ts"), col("created_at"),
                col("isAnomaly"), col("anomalyScore"), col("anomalyType"),
                col("promedioHistorico"), col("desviacionEstandar"),
                col("processedAt")
            )).alias("value")
        )
        query = kafka_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
            .option("topic", self.anomaly_topic) \
            .option("checkpointLocation",
                    f"{self.config.streaming.checkpoint_location}/kafka/{self.estacion}") \
            .outputMode("append") \
            .trigger(processingTime=self.config.streaming.trigger_interval) \
            .start()
        return query

    def write_to_parquet(self, df_observable: DataFrame, output_path: str):
        query = df_observable.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation",
                    f"{self.config.streaming.checkpoint_location}/parquet/{self.estacion}") \
            .outputMode("append") \
            .trigger(processingTime=self.config.streaming.trigger_interval) \
            .start()
        return query

    def write_to_postgres(self, df_observable: DataFrame):
        db = self.config.database
        table_name = f"{db.table_prefix}{self.estacion}"

        def write_batch(df, batch_id):
            import psycopg2
            conn = psycopg2.connect(
                host=db.url.replace("jdbc:postgresql://", "").split(":")[0],
                port=db.url.replace("jdbc:postgresql://", "").split(":")[1].split("/")[0],
                dbname=db.url.split("/")[-1],
                user=db.user,
                password=db.password,
            )
            cur = conn.cursor()
            try:
                from psycopg2.extras import execute_values
                rows_df = df.select(
                    "id", "sensor_id", "estacion",
                    "department", "province", "district",
                    "temperatura", "humedad", "presion", "altura",
                    "iaq", "eco2", "voc", "calidad_aire",
                    "ts", "created_at"
                ).collect()
                if not rows_df:
                    return
                from datetime import datetime
                processed_at = datetime.utcnow()
                rows = [tuple(r) + (processed_at,) for r in rows_df]
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
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
                    )
                """)
                conn.commit()
                execute_values(
                    cur,
                    f"""INSERT INTO {table_name} (
                        id, sensor_id, estacion,
                        department, province, district,
                        temperatura, humedad, presion, altura,
                        iaq, eco2, voc, calidad_aire,
                        ts, created_at, processed_at
                    ) VALUES %s ON CONFLICT (id) DO NOTHING""",
                    rows,
                    page_size=db.batch_size,
                )
                conn.commit()
                logger.info("Batch %d: %d registros escritos a PostgreSQL '%s'",
                            batch_id, len(rows), table_name)
            except Exception as e:
                logger.error("Error escribiendo a PostgreSQL batch %d: %s", batch_id, e)
                logger.exception(e)
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

        query = df_observable \
            .writeStream \
            .queryName(f"clima_postgres_{self.estacion}") \
            .foreachBatch(write_batch) \
            .outputMode("append") \
            .trigger(processingTime=self.config.streaming.trigger_interval) \
            .start()
        return query

    def query_with_windowing(self, df_observable: DataFrame):
        watermark = self.config.streaming.watermark
        window_dur = self.config.streaming.window_duration
        df_windowing = df_observable \
            .withColumn("eventTime",
                        (col("timestamp") / 1000).cast("timestamp")) \
            .withWatermark("eventTime", watermark) \
            .groupBy(window(col("eventTime"), window_dur)) \
            .agg(
                spark_count("*").alias("numEventos"),
                spark_avg("temperatura").alias("temperaturaPromedio"),
                spark_avg("humedad").alias("humedadPromedio"),
            )
        return df_windowing

    def log_throughput_metrics(self, query, topic: str):
        progress = query.lastProgress
        if progress is None:
            return
        source = progress["sources"][0]
        metrics = source.get("metrics", {})
        log_data = {
            "service": "spark-streaming",
            "component": "consumer",
            "estacion": self.estacion,
            "topic": topic,
            "batchId": progress["batchId"],
            "numInputRows": progress["numInputRows"],
            "inputRowsPerSecond": progress["inputRowsPerSecond"],
            "processedRowsPerSecond": progress["processedRowsPerSecond"],
            "avgOffsetsBehindLatest": metrics.get("avgOffsetsBehindLatest"),
            "maxOffsetsBehindLatest": metrics.get("maxOffsetsBehindLatest"),
        }
        logger.info("Metrics [%s]: %s", self.estacion, json.dumps(log_data))

    def run_full_pipeline(self):
        logger.info("=" * 60)
        logger.info("SPARK STREAMING - %s", self.estacion)
        logger.info("  Topic input:  %s", self.input_topic)
        logger.info("  Topic anomalias: %s", self.anomaly_topic)
        logger.info("  Ubicacion:    %s / %s / %s", self.department, self.province, self.district)
        logger.info("=" * 60)

        try:
            self.create_spark_session()

            self.historical_df = self.load_historical_data()
            self.calculate_stats_from_historical()

            stream_df = self.create_kafka_stream()
            parsed_df = self.parse_sensor_data(stream_df)
            obs_df = self.add_observability_fields(parsed_df)
            anomaly_df = self.detect_anomalies(obs_df)

            query_kafka = self.write_to_kafka(anomaly_df)
            query_parquet = self.write_to_parquet(
                obs_df,
                f"{self.config.streaming.checkpoint_location}/../parquet_output/{self.estacion}"
            )
            query_postgres = self.write_to_postgres(obs_df)

            logger.info("Pipeline iniciado para '%s'. Ctrl+C para detener.", self.estacion)
            query_kafka.awaitTermination()
        except Exception as e:
            logger.error("Error en pipeline '%s': %s", self.estacion, e)
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Spark Streaming Processor")
    parser.add_argument("--input-topic", help="Topico Kafka de entrada (default: config)")
    parser.add_argument("--anomaly-topic", help="Topico Kafka de anomalias (default: {input-topic}-anomalias)")
    parser.add_argument("--mode", choices=["console", "kafka"], default="kafka",
                        help="Modo de ejecucion")
    args = parser.parse_args()

    processor = SparkStreamingProcessor(
        input_topic=args.input_topic,
        anomaly_topic=args.anomaly_topic,
    )
    processor.run_full_pipeline()


if __name__ == "__main__":
    main()
