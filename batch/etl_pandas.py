"""
ETL Alternativo usando Pandas (sin Spark).
Para Windows o cuando Spark no está disponible.

Usage:
    python -m batch.etl_pandas
"""

import os
import sys
from pathlib import Path
import glob
from typing import Optional
from datetime import datetime

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger


logger = get_logger("etl_pandas")


def extract_metadata_from_filename(filepath: str) -> tuple:
    """Extrae metadatos del nombre del archivo."""
    base = os.path.basename(filepath)
    name_without_ext = os.path.splitext(base)[0]
    parts = name_without_ext.split('-')
    
    if len(parts) >= 4:
        district = parts[-1]
        province = parts[-2]
        department = parts[-3]
        station_name = '-'.join(parts[:-3])
        return (station_name, department, province, district)
    else:
        raise ValueError(f"Formato de archivo inesperado: {base}")


def parse_line(line: str) -> Optional[dict]:
    """Parsea una línea de datos SENAMHI."""
    parts = line.strip().split()
    
    if len(parts) != 6:
        return None
    
    try:
        return {
            'year': int(parts[0]),
            'month': int(parts[1]),
            'day': int(parts[2]),
            'precip': None if parts[3] in ['-99.9', '-99.9', 'NA'] else float(parts[3]),
            'tmax': None if parts[4] in ['-99.9', '-99.9', 'NA'] else float(parts[4]),
            'tmin': None if parts[5] in ['-99.9', '-99.9', 'NA'] else float(parts[5]),
        }
    except (ValueError, IndexError):
        return None


def process_file(filepath: str) -> pd.DataFrame:
    """Procesa un archivo y retorna un DataFrame."""
    try:
        station_name, department, province, district = extract_metadata_from_filename(filepath)
    except ValueError as e:
        logger.warning(f"Skipping {filepath}: {e}")
        return pd.DataFrame()
    
    rows = []
    
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if not line.strip() or line.startswith('#'):
                continue
            
            parsed = parse_line(line)
            if parsed:
                parsed['station_name'] = station_name
                parsed['department'] = department
                parsed['province'] = province
                parsed['district'] = district
                rows.append(parsed)
    
    if rows:
        df = pd.DataFrame(rows)
        return df
    return pd.DataFrame()


def run_etl():
    """Ejecuta el pipeline ETL completo."""
    logger.info("=" * 60)
    logger.info("INICIANDO PIPELINE ETL - PANDAS")
    logger.info("=" * 60)
    
    config = get_config()
    input_path = config.paths.input
    output_path = config.paths.output
    
    logger.info(f"Input: {input_path}")
    logger.info(f"Output: {output_path}")
    
    if not os.path.exists(input_path):
        logger.error(f"Directorio de entrada no existe: {input_path}")
        return
    
    txt_files = glob.glob(os.path.join(input_path, "*.txt"))
    
    if not txt_files:
        logger.warning(f"No se encontraron archivos .txt en {input_path}")
        logger.info("Copia tus archivos .txt de SENAMHI a esa carpeta")
        return
    
    logger.info(f"Archivos encontrados: {len(txt_files)}")
    
    all_dfs = []
    
    for i, filepath in enumerate(txt_files):
        if (i + 1) % 10 == 0:
            logger.info(f"Procesando archivo {i + 1}/{len(txt_files)}")
        
        df = process_file(filepath)
        if not df.empty:
            all_dfs.append(df)
    
    if not all_dfs:
        logger.error("No se procesaron datos")
        return
    
    logger.info("Concatenando DataFrames...")
    df = pd.concat(all_dfs, ignore_index=True)
    
    logger.info(f"Total registros: {len(df)}")
    
    df = df[
        (df['month'] >= 1) & (df['month'] <= 12) &
        (df['day'] >= 1) & (df['day'] <= 31) &
        (df['year'] >= 1900) & (df['year'] <= 2030)
    ]
    
    logger.info(f"Registros después de limpieza: {len(df)}")
    
    df['date'] = pd.to_datetime(
        df['year'].astype(str) + '-' + 
        df['month'].astype(str).str.zfill(2) + '-' + 
        df['day'].astype(str).str.zfill(2),
        format='%Y-%m-%d',
        errors='coerce'
    )
    
    df = df.dropna(subset=['date'])
    
    os.makedirs(output_path, exist_ok=True)
    
    output_file = os.path.join(output_path, "weather_data.parquet")
    df.to_parquet(output_file, index=False, compression='snappy')
    
    logger.info(f"Datos guardados en: {output_file}")
    
    metadata_path = config.paths.metadata
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    
    df_stations = df[['station_name', 'department', 'province', 'district']].drop_duplicates()
    df_stations.to_parquet(metadata_path, index=False, compression='snappy')
    
    logger.info(f"Metadatos guardados en: {metadata_path}")
    
    logger.info("=" * 60)
    logger.info("ESTADISTICAS FINALES")
    logger.info("=" * 60)
    logger.info(f"Total registros: {len(df):,}")
    logger.info(f"Estaciones unicas: {df['station_name'].nunique()}")
    logger.info(f"Departamentos: {df['department'].nunique()}")
    logger.info(f"Rango de fechas: {df['date'].min()} a {df['date'].max()}")
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETADO")
    logger.info("=" * 60)


def main():
    """Punto de entrada."""
    run_etl()


if __name__ == "__main__":
    main()