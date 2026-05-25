"""
Consumidor de datos de Supabase.
Pollhea la tabla de lecturas del sensor y procesa los datos.

Usage:
    python -m streaming.supabase_consumer
"""

import sys
import time
import threading
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime
from collections import deque

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger
from batch.utils import (
    validate_sensor_reading,
    calculate_anomaly,
    AnomalyResult
)


logger = get_logger("supabase_consumer")


class SupabaseConsumer:
    """
    Consumidor de datos desde Supabase.
    """
    
    def __init__(self):
        self.config = get_config()
        self.base_url = self.config.supabase.url
        self.api_key = self.config.supabase.api_key
        self.table = self.config.supabase.table
        self.poll_interval = self.config.supabase.poll_interval_seconds
        
        self.headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        self.running = False
        self.last_timestamp = 0
        self.reading_callbacks = []
        self.anomaly_callbacks = []
        
        self._recent_readings = deque(maxlen=100)
    
    def register_reading_callback(self, callback) -> None:
        """Registra callback para nuevas lecturas."""
        self.reading_callbacks.append(callback)
    
    def register_anomaly_callback(self, callback) -> None:
        """Registra callback para anomalías."""
        self.anomaly_callbacks.append(callback)
    
    def fetch_readings(self, since_timestamp: int = 0) -> List[Dict[str, Any]]:
        """
        Obtiene lecturas desde la última marca de tiempo.
        
        Args:
            since_timestamp: Timestamp desde el cual obtener datos
            
        Returns:
            Lista de lecturas
        """
        url = f"{self.base_url}/rest/v1/{self.table}"
        
        order_col = self.config.supabase.get("order_column", "created_at")
        
        params = {
            "select": "*",
            "order": f"{order_col}.desc",
            "limit": 50
        }
        
        if since_timestamp > 0:
            params[order_col] = f"gt.{since_timestamp}"
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Error al obtener lecturas: {e}")
            return []
    
    def fetch_latest(self, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Obtiene las últimas lecturas.
        
        Args:
            limit: Número de registros a obtener
            
        Returns:
            Lista de lecturas
        """
        url = f"{self.base_url}/rest/v1/{self.table}"
        
        params = {
            "select": "*",
            "order": "timestamp.desc",
            "limit": limit
        }
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Error al obtener últimas lecturas: {e}")
            return []
    
    def process_reading(self, data: Dict[str, Any]) -> Optional[AnomalyResult]:
        """Procesa una lectura y detecta anomalías."""
        try:
            sensor_id = data.get("estacion", "sensor_puno_01")
            temperatura = data.get("temperatura")
            
            created_at = data.get("created_at")
            if isinstance(created_at, str):
                try:
                    timestamp = int(datetime.fromisoformat(created_at.replace('Z', '+00:00')).timestamp())
                except:
                    timestamp = int(datetime.now().timestamp())
            else:
                timestamp = int(datetime.now().timestamp())
            
            if temperatura is None:
                logger.warning("Lectura sin temperatura")
                return None
            
            if not validate_sensor_reading(
                temperatura,
                self.config.sensor.min_temperature,
                self.config.sensor.max_temperature
            ):
                logger.warning(f"Temperatura fuera de rango: {temperatura}")
                return None
            
            logger.info(f"Lectura Supabase - Estación: {sensor_id}, Temp: {temperatura}°C")
            
            for callback in self.reading_callbacks:
                try:
                    callback(data)
                except Exception as e:
                    logger.error(f"Error en callback: {e}")
            
            promedio_historico = 12.0
            desviacion_estandar = 2.5
            
            result = calculate_anomaly(
                temperatura_actual=temperatura,
                promedio_historico=promedio_historico,
                desviacion_estandar=desviacion_estandar,
                sigma_threshold=self.config.sensor.anomaly_threshold_sigma
            )
            
            result.sensor_id = sensor_id
            result.timestamp = timestamp
            
            self._recent_readings.append(result)
            
            if result.es_anomalia:
                logger.warning(f"⚠️ ANOMALÍA: {result.mensaje}")
                
                for callback in self.anomaly_callbacks:
                    try:
                        callback(result)
                    except Exception as e:
                        logger.error(f"Error en callback de anomalía: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error al procesar lectura: {e}")
            return None
    
    def start_polling(self) -> None:
        """Inicia el polling de datos."""
        logger.info(f"Iniciando polling (intervalo: {self.poll_interval}s)")
        self.running = True
        
        while self.running:
            try:
                readings = self.fetch_readings(since_timestamp=self.last_timestamp)
                
                if readings:
                    for reading in readings:
                        timestamp = reading.get("timestamp", 0)
                        if timestamp > self.last_timestamp:
                            self.last_timestamp = timestamp
                        
                        self.process_reading(reading)
                    
                    logger.info(f"Procesadas {len(readings)} lecturas")
                else:
                    logger.debug("Sin nuevas lecturas")
                    
            except Exception as e:
                logger.error(f"Error en polling: {e}")
            
            time.sleep(self.poll_interval)
    
    def start_realtime(self) -> None:
        """
        Inicia el modo de suscripción realtime (si está configurado).
        Requiere SUPABASE_REALTIME_ENABLED=true
        """
        if not self.config.supabase.realtime_enabled:
            logger.warning("Realtime no habilitado, usando polling")
            self.start_polling()
            return
        
        try:
            from supabase import create_client, SupabaseClient
            
            logger.info("Iniciando cliente realtime de Supabase...")
            
            client: SupabaseClient = create_client(
                self.config.supabase.url,
                self.config.supabase.api_key
            )
            
            channel = client.channel("sensor-readings")
            
            channel.on(
                "postgres_changes",
                {
                    "event": "INSERT",
                    "schema": "public",
                    "table": self.table
                },
                lambda payload: self.process_reading(payload.get("new", {}))
            ).subscribe()
            
            logger.info("Suscrito a cambios en tiempo real")
            
            while self.running:
                time.sleep(1)
                
        except ImportError:
            logger.warning("Librería supabase-py no instalada, usando polling")
            self.start_polling()
        except Exception as e:
            logger.error(f"Error en realtime: {e}")
            self.start_polling()
    
    def stop(self) -> None:
        """Detiene el consumidor."""
        logger.info("Deteniendo consumidor Supabase...")
        self.running = False
    
    def get_recent_readings(self, limit: int = 10) -> List[AnomalyResult]:
        """Obtiene las lecturas recientes procesadas."""
        return list(self._recent_readings)[-limit:]


class MockSupabaseConsumer:
    """
    Consumidor de Supabase mock para pruebas.
    Simula datos cuando Supabase no está disponible.
    """
    
    def __init__(self):
        self.config = get_config()
        self.poll_interval = self.config.supabase.poll_interval_seconds
        self.running = False
        self.reading_callbacks = []
        self.anomaly_callbacks = []
        
        self._recent_readings = deque(maxlen=100)
    
    def register_reading_callback(self, callback) -> None:
        self.reading_callbacks.append(callback)
    
    def register_anomaly_callback(self, callback) -> None:
        self.anomaly_callbacks.append(callback)
    
    def _generate_mock_reading(self) -> Dict[str, Any]:
        """Genera una lectura simulada."""
        import random
        
        month = datetime.now().month
        base_temps = {
            1: 8, 2: 9, 3: 11, 4: 13, 5: 15,
            6: 17, 7: 18, 8: 17, 9: 16, 10: 14,
            11: 11, 12: 9
        }
        
        base_temp = base_temps.get(month, 12)
        temperatura = round(base_temp + random.uniform(-3, 5), 1)
        
        return {
            "sensor_id": "sensor_puno_01",
            "temperatura": temperatura,
            "humedad": round(random.uniform(50, 80), 1),
            "timestamp": int(datetime.now().timestamp()),
            "latitud": -15.5,
            "longitud": -70.0
        }
    
    def start_polling(self) -> None:
        """Inicia la simulación de datos."""
        import random
        
        logger.info("Iniciando consumidor Mock Supabase")
        self.running = True
        
        while self.running:
            try:
                reading = self._generate_mock_reading()
                temperatura = reading["temperatura"]
                
                logger.info(f"Lectura mock - Temp: {temperatura}°C")
                
                for callback in self.reading_callbacks:
                    try:
                        callback(reading)
                    except Exception as e:
                        logger.error(f"Error en callback: {e}")
                
                promedio = 12.0
                desviacion = 2.5
                
                result = calculate_anomaly(
                    temperatura_actual=temperatura,
                    promedio_historico=promedio,
                    desviacion_estandar=desviacion,
                    sigma_threshold=self.config.sensor.anomaly_threshold_sigma
                )
                
                result.sensor_id = reading["sensor_id"]
                result.timestamp = reading["timestamp"]
                
                self._recent_readings.append(result)
                
                if result.es_anomalia:
                    logger.warning(f"⚠️ ANOMALÍA: {result.mensaje}")
                    
                    for callback in self.anomaly_callbacks:
                        try:
                            callback(result)
                        except Exception as e:
                            logger.error(f"Error en callback anomalía: {e}")
                
            except Exception as e:
                logger.error(f"Error en mock: {e}")
            
            time.sleep(self.poll_interval)
    
    def stop(self) -> None:
        self.running = False
    
    def get_recent_readings(self, limit: int = 10) -> list:
        return list(self._recent_readings)[-limit:]


def main():
    """Punto de entrada principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Consumidor Supabase para clima")
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Usar consumidor mock para pruebas"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Intervalo de polling en segundos"
    )
    
    args = parser.parse_args()
    
    if args.mock:
        consumer = MockSupabaseConsumer()
        consumer.poll_interval = args.poll_interval
    else:
        consumer = SupabaseConsumer()
        consumer.poll_interval = args.poll_interval
    
    try:
        consumer.start_polling()
    except KeyboardInterrupt:
        logger.info("Detenido por usuario")
    finally:
        consumer.stop()


if __name__ == "__main__":
    main()