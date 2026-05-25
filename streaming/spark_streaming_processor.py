import sys
import os
from pathlib import Path
from typing import Optional
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, when, lit, concat_ws,
    avg as spark_avg, stddev as spark_stddev,
    current_timestamp, window, unix_millis, count as spark_count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    LongType, IntegerType, TimestampType
)

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger

logger = get_logger("spark_streaming")


class SparkStreamingProcessor:

    def __init__(self):
        self.config = get_config()
        self.spark: Optional[SparkSession] = None
        self.historical_df: Optional[DataFrame] = None

    def create_spark_session(self) -> SparkSession:
        logger.info("Inicializando Spark Session para Streaming...")

        spark_version = "3.5.1"
        scala_version = "2.12"
        kafka_connector = (
            f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
        )

        builder = SparkSession.builder \
            .appName("ClimePeruStreaming") \
            .master(self.config.spark.master) \
            .config("spark.driver.memory", self.config.spark.memory) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.kafka.kafka.timeout", "30000") \
            .config("spark.jars.packages", kafka_connector) \
            .config("spark.jars.repositories", "https://repo1.maven.org/maven2") \
            .config("spark.sql.streaming.schemaInference", "true")

        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark version: {self.spark.version}")
        logger.info(f"Conector Kafka: {kafka_connector}")

        return self.spark

    def get_sensor_schema(self) -> StructType:
        return StructType([
            StructField("sensor_id", StringType(), True),
            StructField("temperatura", FloatType(), True),
            StructField("humedad", FloatType(), True),
            StructField("timestamp", LongType(), True),
            StructField("ubicacion", StructType([
                StructField("lat", FloatType(), True),
                StructField("lon", FloatType(), True)
            ]), True)
        ])

    def get_anomaly_output_schema(self) -> StructType:
        return StructType([
            StructField("sensor_id", StringType(), True),
            StructField("temperatura", FloatType(), True),
            StructField("timestamp", LongType(), True),
            StructField("promedio_historico", FloatType(), True),
            StructField("desviacion_estandar", FloatType(), True),
            StructField("limite_inferior", FloatType(), True),
            StructField("limite_superior", FloatType(), True),
            StructField("es_anomalia", StringType(), True),
            StructField("diferencia_promedio", FloatType(), True),
            StructField("mensaje", StringType(), True),
            StructField("processed_at", TimestampType(), True)
        ])

    def load_historical_data(self) -> DataFrame:
        parquet_path = self.config.paths.output
        logger.info(f"Cargando datos historicos desde: {parquet_path}")
        try:
            df = self.spark.read.parquet(parquet_path)
            logger.info(f"Datos historicos cargados: {df.count()} registros")
            return df
        except Exception as e:
            logger.warning(f"No se pudieron cargar datos historicos: {e}")
            return None

    def calculate_historical_stats(self) -> DataFrame:
        if self.historical_df is None:
            logger.warning("No hay datos historicos disponibles")
            return None
        stats = self.historical_df \
            .filter(col("tmin").isNotNull()) \
            .groupBy("station_name", "month") \
            .agg(
                spark_avg("tmin").alias("avg_tmin"),
                spark_stddev("tmin").alias("std_tmin")
            ) \
            .fillna({"std_tmin": 2.0})
        logger.info(f"Estadisticas calculadas: {stats.count()} estaciones-meses")
        return stats

    def create_kafka_stream(self) -> DataFrame:
        logger.info(f"Creando stream desde Kafka: {self.config.kafka.topic}")
        stream_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
            .option("subscribe", self.config.kafka.topic) \
            .option("startingOffsets", self.config.kafka.auto_offset_reset) \
            .option("failOnDataLoss", "false") \
            .load()
        logger.info("Stream de Kafka creado")
        return stream_df

    def parse_sensor_data(self, stream_df: DataFrame) -> DataFrame:
        schema = self.get_sensor_schema()

        df_value = stream_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafkaTimestamp"),
            col("value").cast("string").alias("value")
        )

        parsed_df = df_value.select(
            "topic", "partition", "offset", "kafkaTimestamp",
            from_json(col("value"), schema).alias("sensor")
        ).select("topic", "partition", "offset", "kafkaTimestamp", "sensor.*")

        return parsed_df

    def add_observability_fields(self, parsed_df: DataFrame) -> DataFrame:
        df_obs = parsed_df \
            .withColumn("isValid",
                        col("sensor_id").isNotNull() & col("temperatura").isNotNull()) \
            .withColumn("processedAt", unix_millis(current_timestamp())) \
            .withColumn("latencyMs", col("processedAt") - col("timestamp"))
        return df_obs

    def detect_anomalies(self, parsed_df: DataFrame) -> DataFrame:
        sigma_threshold = self.config.sensor.anomaly_threshold_sigma

        anomaly_df = parsed_df \
            .withColumn("promedio_historico", lit(12.0)) \
            .withColumn("desviacion_estandar", lit(2.5)) \
            .withColumn(
                "limite_inferior",
                col("promedio_historico") - (lit(sigma_threshold) * col("desviacion_estandar"))
            ) \
            .withColumn(
                "limite_superior",
                col("promedio_historico") + (lit(sigma_threshold) * col("desviacion_estandar"))
            ) \
            .withColumn(
                "diferencia_promedio",
                col("temperatura") - col("promedio_historico")
            ) \
            .withColumn(
                "es_anomalia",
                when(
                    (col("temperatura") < col("limite_inferior")) |
                    (col("temperatura") > col("limite_superior")),
                    "SI"
                ).otherwise("NO")
            ) \
            .withColumn(
                "mensaje",
                when(
                    col("es_anomalia") == "SI",
                concat_ws(" ",
                          lit("Alerta: Temperatura"),
                          col("temperatura").cast("string"),
                          lit("°C fuera del rango historico"),
                          col("limite_inferior").cast("string"),
                          lit("a"),
                          col("limite_superior").cast("string"),
                          lit("°C"))
                ).otherwise(
                    concat_ws(" ",
                              lit("Temperatura"),
                              col("temperatura").cast("string"),
                              lit("°C dentro del rango historico"))
                )
            ) \
            .withColumn("processed_at", current_timestamp())

        return anomaly_df

    def write_to_kafka(self, anomaly_df: DataFrame):
        output_topic = self.config.kafka.topic_anomalias

        query = anomaly_df \
            .filter(col("es_anomalia") == "SI") \
            .select(
                col("sensor_id").alias("key"),
                to_json(struct(
                    col("sensor_id"),
                    col("temperatura"),
                    col("timestamp"),
                    col("promedio_historico"),
                    col("desviacion_estandar"),
                    col("limite_inferior"),
                    col("limite_superior"),
                    col("es_anomalia"),
                    col("diferencia_promedio"),
                    col("mensaje"),
                    col("processed_at")
                )).alias("value")
            ) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
            .option("topic", output_topic) \
            .option("checkpointLocation",
                    f"{self.config.streaming.checkpoint_location}/anomaly") \
            .outputMode(self.config.streaming.output_mode) \
            .trigger(processingTime=self.config.streaming.trigger_interval) \
            .start()

        logger.info(f"Stream de anomalias escribira a: {output_topic}")
        return query

    def write_to_console(self, anomaly_df: DataFrame):
        query = anomaly_df \
            .writeStream \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "20") \
            .trigger(processingTime=self.config.streaming.trigger_interval) \
            .start()
        return query

    def write_to_parquet(self, df_observable: DataFrame, output_path: str):
        query = df_observable \
            .writeStream \
            .queryName("clima_parquet") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation",
                    f"{output_path}/../checkpoint/streaming") \
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
                spark_avg("latencyMs").alias("latenciaPromedio")
            )
        return df_windowing

    def log_throughput_metrics(self, query, topic: str):
        progress = query.lastProgress
        if progress is None:
            logger.info("No hay micro-batch procesado aun.")
            return
        source = progress["sources"][0]
        metrics = source.get("metrics", {})
        log_data = {
            "service": "spark-streaming",
            "component": "consumer",
            "topic": topic,
            "batchId": progress["batchId"],
            "numInputRows": progress["numInputRows"],
            "inputRowsPerSecond": progress["inputRowsPerSecond"],
            "processedRowsPerSecond": progress["processedRowsPerSecond"],
            "avgOffsetsBehindLatest": metrics.get("avgOffsetsBehindLatest"),
            "maxOffsetsBehindLatest": metrics.get("maxOffsetsBehindLatest"),
            "status": "idle" if progress["numInputRows"] == 0 else "processed"
        }
        logger.info(f"Throughput metrics: {json.dumps(log_data)}")

    def run_console_mode(self):
        logger.info("=" * 60)
        logger.info("INICIANDO SPARK STREAMING - MODO CONSOLA")
        logger.info("=" * 60)

        try:
            self.create_spark_session()
            stream_df = self.create_kafka_stream()
            parsed_df = self.parse_sensor_data(stream_df)
            obs_df = self.add_observability_fields(parsed_df)
            anomaly_df = self.detect_anomalies(obs_df)
            query = self.write_to_console(anomaly_df)
            logger.info("Stream iniciado. Presiona Ctrl+C para detener.")
            query.awaitTermination()
        except Exception as e:
            logger.error(f"Error en streaming: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

    def run_full_pipeline(self):
        logger.info("=" * 60)
        logger.info("INICIANDO SPARK STREAMING - PIPELINE COMPLETO")
        logger.info("=" * 60)

        try:
            self.create_spark_session()

            self.historical_df = self.load_historical_data()
            stream_df = self.create_kafka_stream()
            parsed_df = self.parse_sensor_data(stream_df)
            obs_df = self.add_observability_fields(parsed_df)
            anomaly_df = self.detect_anomalies(obs_df)

            query_kafka = self.write_to_kafka(anomaly_df)
            query_parquet = self.write_to_parquet(
                obs_df,
                f"{self.config.streaming.checkpoint_location}/../parquet_output"
            )

            logger.info("Pipeline iniciado. Ctrl+C para detener.")
            query_kafka.awaitTermination()
        except Exception as e:
            logger.error(f"Error en pipeline: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Spark Streaming Processor")
    parser.add_argument(
        "--mode",
        choices=["console", "kafka"],
        default="console",
        help="Modo de ejecucion"
    )
    parser.add_argument(
        "--kafka-output",
        action="store_true",
        help="Habilitar escritura a Kafka"
    )
    parser.add_argument(
        "--observable",
        action="store_true",
        help="Incluir campos de observabilidad"
    )

    args = parser.parse_args()
    processor = SparkStreamingProcessor()

    if args.mode == "console" and not args.kafka_output:
        processor.run_console_mode()
    else:
        processor.run_full_pipeline()


if __name__ == "__main__":
    main()
