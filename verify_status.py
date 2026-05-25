"""
Script de verificación del sistema CimaPerú.
Permite verificar el estado del batch y streaming.

Usage:
    python verify_status.py --batch    # Verificar ETL
    python verify_status.py --supabase # Verificar Supabase
    python verify_status.py --all      # Verificar todo
"""

import os
import sys
from pathlib import Path
import argparse
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from config import get_config
from config.logger import get_logger


logger = get_logger("verify")


def check_batch_status():
    """Verifica el estado del procesamiento batch."""
    print("\n" + "="*60)
    print("[BATCH] VERIFICACION - PROCESAMIENTO ETL")
    print("="*60)
    
    config = get_config()
    parquet_path = config.paths.output
    metadata_path = config.paths.metadata
    
    print(f"\nRutas configuradas:")
    print(f"   - Parquet: {parquet_path}")
    print(f"   - Metadata: {metadata_path}")
    
    if not os.path.exists(parquet_path):
        print(f"\n[ERROR] El directorio Parquet NO existe")
        print(f"   Ejecuta: python -m batch.etl_senamhi")
        return False
    
    print(f"\n[OK] Directorio Parquet existe")
    
    import pyarrow.parquet as pq
    
    try:
        parquet_file = pq.ParquetFile(parquet_path)
        total_rows = sum(pq.read_metadata(parquet_path).num_rows for _ in [1])
        
        print(f"\nEstadisticas:")
        
        import pandas as pd
        df = pd.read_parquet(parquet_path)
        total = len(df)
        
        print(f"   - Total registros: {total:,}")
        print(f"   - Estaciones unicas: {df['station_name'].nunique()}")
        print(f"   - Departamentos: {df['department'].nunique()}")
        print(f"   - Rango de anos: {df['year'].min()} - {df['year'].max()}")
        print(f"   - Variables: precip, tmax, tmin")
        
        print(f"\nPrimera particion encontrada:")
        print(df.head(3)[['station_name', 'department', 'date', 'tmax', 'tmin']].to_string())
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Al leer Parquet: {e}")
        return False


def check_supabase_connection():
    """Verifica la conexión a Supabase."""
    print("\n" + "="*60)
    print("[SUPABASE] VERIFICACION")
    print("="*60)
    
    config = get_config()
    
    supabase_url = config.supabase.url
    table = config.supabase.table
    
    print(f"\nConfiguracion:")
    print(f"   - URL: {supabase_url}")
    print(f"   - Tabla: {table}")
    
    if "placeholder" in supabase_url.lower():
        print(f"\n[ATENCION] URL de Supabase no configurada")
        print(f"   Edita config/config.yaml y configura:")
        print(f"   - supabase.url")
        print(f"   - supabase.api_key")
        return False
    
    import requests
    
    headers = {
        "apikey": config.supabase.api_key,
        "Authorization": f"Bearer {config.supabase.api_key}"
    }
    
    url = f"{supabase_url}/rest/v1/{table}"
    params = {"select": "count", "limit": 1}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            url_count = f"{supabase_url}/rest/v1/{table}?select=count"
            count_response = requests.get(url_count, headers=headers, timeout=10)
            
            if count_response.status_code == 200:
                data = count_response.json()
                if data and 'count' in data[0]:
                    total = data[0]['count']
                    print(f"\n[OK] Conexion exitosa")
                    print(f"   - Total registros en tabla: {total:,}")
                else:
                    print(f"\n[OK] Conexion exitosa (no se pudo obtener count)")
            else:
                print(f"\n[OK] Conexion exitosa (error al contar)")
            
            last_url = f"{supabase_url}/rest/v1/{table}?select=*&order=created_at.desc&limit=1"
            last_response = requests.get(last_url, headers=headers, timeout=10)
            
            if last_response.status_code == 200:
                last_data = last_response.json()
                if last_data:
                    last = last_data[0]
                    print(f"\nUltima lectura:")
                    print(f"   - Estacion: {last.get('estacion', 'N/A')}")
                    print(f"   - Temperatura: {last.get('temperatura', 'N/A')} C")
                    print(f"   - Humedad: {last.get('humedad', 'N/A')}%")
                    print(f"   - Created at: {last.get('created_at', 'N/A')}")
            
            return True
            
        elif response.status_code == 401:
            print(f"\n[ERROR] API Key invalida")
            return False
        else:
            print(f"\n[ERROR] Estado {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"\n[ERROR] No se puede conectar a Supabase")
        print(f"   Verifica la URL y tu conexion a internet")
        return False
    except Exception as e:
        print(f"\n[ERROR] {e}")
        return False


def check_kafka_status():
    """Verifica el estado de Kafka."""
    print("\n" + "="*60)
    print("[KAFKA] VERIFICACION")
    print("="*60)
    
    config = get_config()
    
    print(f"\nConfiguracion:")
    print(f"   - Broker: {config.kafka.bootstrap_servers}")
    print(f"   - Topico: {config.kafka.topic}")
    print(f"   - Grupo: {config.kafka.consumer_group}")
    
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            api_version_auto_timeout_ms=5000
        )
        
        topics = consumer.topics()
        
        print(f"\n[OK] Conexion a Kafka exitosa")
        print(f"   - Topicos disponibles: {list(topics.keys())}")
        
        if config.kafka.topic in topics:
            print(f"   - [OK] Topico '{config.kafka.topic}' existe")
        else:
            print(f"   - [WARN] Topico '{config.kafka.topic}' NO existe")
            print(f"   - Crea con: docker exec clime-kafka kafka-topics.sh --create --topic {config.kafka.topic} --bootstrap-server localhost:19092")
        
        consumer.close()
        return True
        
    except KafkaError as e:
        print(f"\n[ERROR] No se puede conectar a Kafka")
        print(f"   - Verifica que Kafka este corriendo")
        print(f"   - Ejecuta: docker compose -f docker/docker-compose.dev.yml up -d")
        return False


def check_dashboard_data():
    """Verifica que el dashboard pueda cargar datos."""
    print("\n" + "="*60)
    print("[DASHBOARD] VERIFICACION")
    print("="*60)
    
    config = get_config()
    
    try:
        import pandas as pd
        
        df = pd.read_parquet(config.paths.output)
        
        print(f"\n[OK] Datos disponibles para el dashboard:")
        print(f"   - {len(df):,} registros")
        print(f"   - {df['station_name'].nunique()} estaciones")
        
        print(f"\nPara iniciar el dashboard:")
        print(f"   cd dashboard")
        print(f"   streamlit run app.py")
        
        return True
        
    except FileNotFoundError:
        print(f"\n[ERROR] No hay datos procesados")
        print(f"   Ejecuta primero el ETL")
        return False
    except Exception as e:
        print(f"\n[ERROR] {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Verificar estado del sistema")
    parser.add_argument("--batch", action="store_true", help="Verificar batch")
    parser.add_argument("--supabase", action="store_true", help="Verificar Supabase")
    parser.add_argument("--kafka", action="store_true", help="Verificar Kafka")
    parser.add_argument("--dashboard", action="store_true", help="Verificar dashboard")
    parser.add_argument("--all", action="store_true", help="Verificar todo")
    
    args = parser.parse_args()
    
    if not any([args.batch, args.supabase, args.kafka, args.dashboard, args.all]):
        parser.print_help()
        return
    
    results = {}
    
    if args.batch or args.all:
        results['batch'] = check_batch_status()
    
    if args.supabase or args.all:
        results['supabase'] = check_supabase_connection()
    
    if args.kafka or args.all:
        results['kafka'] = check_kafka_status()
    
    if args.dashboard or args.all:
        results['dashboard'] = check_dashboard_data()
    
    print("\n" + "="*60)
    print("RESUMEN")
    print("="*60)
    
    for key, status in results.items():
        emoji = "[OK]" if status else "[ERROR]"
        print(f"   {emoji} {key.upper()}")
    
    all_ok = all(results.values())
    print(f"\n{'[OK] SISTEMA LISTO' if all_ok else '[ERROR] REVISAR ERRORES'}")


if __name__ == "__main__":
    main()