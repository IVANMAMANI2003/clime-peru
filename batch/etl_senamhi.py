"""
ETL Batch para procesar datos históricos de SENAMHI.
Convierte archivos .txt a formato Parquet optimizado.

Usage:
    python -m batch.etl_senamhi
"""

import os
import sys
import shutil
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, concat_ws, to_date, year, month, day as day_col,
    when, lit, coalesce, regexp_replace
)
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger


logger = get_logger("etl_senamhi")


def extract_metadata(filepath: str) -> tuple:
    base = os.path.basename(filepath)
    name_without_ext = os.path.splitext(base)[0]
    parts = name_without_ext.split('-')
    if len(parts) >= 4:
        district = parts[-1]
        province = parts[-2]
        department = parts[-3]
        station_name = '-'.join(parts[:-3])
        return (station_name, department, province, district)
    raise ValueError(f"Formato de archivo inesperado: {base}")


def parse_line(line: str) -> Optional[Row]:
    parts = line.strip().split()
    if len(parts) != 6:
        return None
    try:
        return Row(
            year=int(parts[0]), month=int(parts[1]), day=int(parts[2]),
            precip=None if parts[3] in ['-99.9', 'NA'] else float(parts[3]),
            tmax=None if parts[4] in ['-99.9', 'NA'] else float(parts[4]),
            tmin=None if parts[5] in ['-99.9', 'NA'] else float(parts[5]),
        )
    except (ValueError, IndexError):
        return None


def process_file(content: str, filepath: str) -> list:
    try:
        station_name, department, province, district = extract_metadata(filepath)
    except ValueError as e:
        logger.warning(f"Skipping {filepath}: {e}")
        return []
    rows = []
    for line in content.split('\n'):
        if not line.strip() or line.startswith('#'):
            continue
        parsed = parse_line(line)
        if parsed:
            rows.append(Row(
                station_name=station_name, department=department,
                province=province, district=district,
                year=parsed.year, month=parsed.month, day=parsed.day,
                precip=parsed.precip, tmax=parsed.tmax, tmin=parsed.tmin
            ))
    return rows


SCHEMA = StructType([
    StructField("station_name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("province", StringType(), False),
    StructField("district", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("precip", FloatType(), True),
    StructField("tmax", FloatType(), True),
    StructField("tmin", FloatType(), True),
])


class SenamhiETL:
    """
    Pipeline ETL para datos SENAMHI.
    """
    
    def __init__(self):
        self.config = get_config()
        self.spark: Optional[SparkSession] = None
        
    def create_spark_session(self) -> SparkSession:
        """Crea y configura la sesión de Spark."""
        
        logger.info("Inicializando sesión de Spark...")
        
        builder = SparkSession.builder \
            .appName(self.config.spark.app_name) \
            .master(self.config.spark.master) \
            .config("spark.driver.memory", self.config.spark.memory) \
            .config("spark.sql.adaptive.enabled", str(self.config.spark.adaptive_enabled).lower()) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", str(self.config.spark.coalesce_partitions_enabled).lower()) \
            .config("spark.sql.parquet.compression.codec", self.config.spark.parquet_compression)
        
        self.spark = builder.getOrCreate()
        
        logger.info(f"Spark versión: {self.spark.version}")
        logger.info(f"Master: {self.spark.sparkContext.master}")
        
        return self.spark
    
    def extract(self) -> DataFrame:
        """Extrae datos de los archivos fuente."""
        input_path = self.config.paths.input
        logger.info(f"Leyendo archivos desde: {input_path}")
        
        file_rdd = self.spark.sparkContext.wholeTextFiles(f"{input_path}/*.txt")
        file_count = file_rdd.count()
        logger.info(f"Archivos encontrados: {file_count}")
        
        df_files = file_rdd.toDF(["filepath", "content"])
        
        all_rows = df_files.rdd.flatMap(
            lambda row: process_file(row.content, row.filepath)
        )
        
        df = self.spark.createDataFrame(all_rows, SCHEMA)
        
        logger.info(f"Registros extraídos: {df.count()}")
        
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Transforma los datos: limpia y estructura."""
        logger.info("Transformando datos...")
        
        df_clean = df.filter(
            (col("month").between(1, 12)) & 
            (col("day").between(1, 31)) &
            (col("year").between(1900, 2030))
        )
        
        df_clean = df_clean.withColumn(
            "date",
            to_date(concat_ws("-", col("year"), col("month"), col("day")), "yyyy-M-d")
        )
        
        df_clean = df_clean.withColumn(
            "year", col("year").cast("integer")
        ).withColumn(
            "month", col("month").cast("integer")
        ).withColumn(
            "day", col("day").cast("integer")
        )
        
        record_count = df_clean.count()
        logger.info(f"Registros después de limpieza: {record_count}")
        
        return df_clean
    
    def load(self, df: DataFrame) -> None:
        """Carga los datos al destino en formato Parquet."""
        output_path = self.config.paths.output
        
        logger.info(f"Guardando datos en: {output_path}")
        
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
            logger.info(f"Directorio existente eliminado: {output_path}")
        
        partition_cols = self.config.etl.partition_columns
        
        df.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .option("compression", self.config.spark.parquet_compression) \
            .parquet(output_path)
        
        logger.info(f"Datos guardados exitosamente en {output_path}")
        
        self._save_metadata(df)
    
    def _save_metadata(self, df: DataFrame) -> None:
        """Guarda metadatos de estaciones únicas."""
        metadata_path = self.config.paths.metadata
        
        df_stations = df.select(
            "station_name", 
            "department", 
            "province", 
            "district"
        ).distinct()
        
        df_stations.write.mode("overwrite").parquet(metadata_path)
        
        station_count = df_stations.count()
        logger.info(f"Estaciones únicas guardadas: {station_count}")
    
    def show_sample(self, df: DataFrame, n: int = 10) -> None:
        """Muestra una muestra de los datos."""
        logger.info(f"Muestra de {n} registros:")
        df.select(
            "station_name", "department", "year", "month", "day",
            "precip", "tmax", "tmin"
        ).show(n, truncate=False)
    
    def show_output_structure(self) -> None:
        """Muestra la estructura de salida."""
        output_path = self.config.paths.output
        
        logger.info("Estructura de salida:")
        for root, dirs, files in os.walk(output_path):
            level = root.replace(output_path, '').count(os.sep)
            indent = ' ' * 2 * level
            logger.info(f"{indent}{os.path.basename(root)}/")
            if level < 2:
                subindent = ' ' * 2 * (level + 1)
                for file in files[:3]:
                    logger.info(f"{subindent}{file}")
                if len(files) > 3:
                    logger.info(f"{subindent}... y {len(files) - 3} más")
    
    def run(self) -> None:
        """Ejecuta el pipeline ETL completo."""
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE ETL - SENAMHI")
        logger.info("=" * 60)
        
        try:
            self.create_spark_session()
            
            df_raw = self.extract()
            
            df_transformed = self.transform(df_raw)
            
            self.load(df_transformed)
            
            self.show_sample(df_transformed)
            
            self.show_output_structure()
            
            logger.info("=" * 60)
            logger.info("PIPELINE ETL COMPLETADO EXITOSAMENTE")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error en pipeline ETL: {e}")
            raise
            
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Sesión de Spark finalizada")


def main():
    """Punto de entrada principal."""
    etl = SenamhiETL()
    etl.run()


if __name__ == "__main__":
    main()