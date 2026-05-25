"""
Consumidor Kafka para datos del sensor de clima.
Escucha el tópico de datos en tiempo real y procesa las lecturas.

Usage:
    python -m streaming.kafka_consumer
"""

import sys
import json
import signal
import threading
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger
from batch.utils import (
    validate_sensor_reading, 
    calculate_anomaly,
    deserialize_from_kafka,
    AnomalyResult
)


logger = get_logger("kafka_consumer")


class ClimateKafkaConsumer:
    """
    Consumidor de eventos Kafka para datos climáticos.
    """
    
    def __init__(self):
        self.config = get_config()
        self.consumer: Optional[KafkaConsumer] = None
        self.running = False
        self.anomaly_callbacks = []
        self.reading_callbacks = []
        
    def connect(self) -> None:
        """Establece conexión con el broker Kafka."""
        logger.info(f"Conectando a Kafka: {self.config.kafka.bootstrap_servers}")
        
        try:
            self.consumer = KafkaConsumer(
                self.config.kafka.topic,
                bootstrap_servers=self.config.kafka.bootstrap_servers,
                auto_offset_reset=self.config.kafka.auto_offset_reset,
                enable_auto_commit=self.config.kafka.enable_auto_commit,
                group_id=self.config.kafka.consumer_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                session_timeout_ms=self.config.kafka.session_timeout_ms,
                max_poll_records=self.config.kafka.max_poll_records,
            )
            
            logger.info(f"Suscrito al tópico: {self.config.kafka.topic}")
            logger.info(f"Consumer group: {self.config.kafka.consumer_group}")
            
        except KafkaError as e:
            logger.error(f"Error al conectar a Kafka: {e}")
            raise
    
    def register_anomaly_callback(self, callback) -> None:
        """Registra un callback para notificaciones de anomalías."""
        self.anomaly_callbacks.append(callback)
    
    def register_reading_callback(self, callback) -> None:
        """Registra un callback para notificaciones de lecturas."""
        self.reading_callbacks.append(callback)
    
    def validate_reading(self, data: Dict[str, Any]) -> bool:
        """Valida que la lectura del sensor sea correcta."""
        required_fields = ["sensor_id", "temperatura", "timestamp"]
        
        for field in required_fields:
            if field not in data:
                logger.warning(f"Campo requerido ausente: {field}")
                return False
        
        temp = data.get("temperatura")
        if not validate_sensor_reading(
            temp,
            self.config.sensor.min_temperature,
            self.config.sensor.max_temperature
        ):
            logger.warning(f"Temperatura fuera de rango: {temp}")
            return False
        
        return True
    
    def process_reading(self, data: Dict[str, Any]) -> Optional[AnomalyResult]:
        """
        Procesa una lectura del sensor y detecta anomalías.
        En producción, esto consultaría el parquet histórico.
        """
        if not self.validate_reading(data):
            return None
        
        sensor_id = data.get("sensor_id", "unknown")
        temperatura = data.get("temperatura")
        timestamp = data.get("timestamp", 0)
        
        logger.info(f"Lectura recibida - Sensor: {sensor_id}, Temp: {temperatura}°C")
        
        for callback in self.reading_callbacks:
            try:
                callback(data)
            except Exception as e:
                logger.error(f"Error en callback de lectura: {e}")
        
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
        
        if result.es_anomalia:
            logger.warning(f"⚠️ ANOMALÍA DETECTADA: {result.mensaje}")
            
            for callback in self.anomaly_callbacks:
                try:
                    callback(result)
                except Exception as e:
                    logger.error(f"Error en callback de anomalía: {e}")
        
        return result
    
    def consume(self) -> None:
        """Inicia el consumo de mensajes."""
        logger.info("Iniciando consumo de mensajes...")
        self.running = True
        
        try:
            while self.running:
                try:
                    records = self.consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, messages in records.items():
                        for message in messages:
                            try:
                                data = message.value
                                logger.debug(f"Mensaje recibido: {data}")
                                
                                self.process_reading(data)
                                
                            except json.JSONDecodeError as e:
                                logger.error(f"Error al decodificar JSON: {e}")
                            except Exception as e:
                                logger.error(f"Error al procesar mensaje: {e}")
                                
                except Exception as e:
                    logger.error(f"Error en poll: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Interrumpido por usuario")
        finally:
            self.stop()
    
    def stop(self) -> None:
        """Detiene el consumidor."""
        logger.info("Deteniendo consumidor...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Conexión cerrada")


class ClimateProducer:
    """
    Productor Kafka para simular/enviar datos de sensores.
    """
    
    def __init__(self):
        self.config = get_config()
        self.producer = None
    
    def connect(self) -> None:
        """Conecta al broker Kafka."""
        from kafka import KafkaProducer
        
        logger.info(f"Conectando productor a: {self.config.kafka.bootstrap_servers}")
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.kafka.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )
    
    def send_reading(
        self,
        sensor_id: str,
        temperatura: float,
        humedad: Optional[float] = None,
        latitud: float = -15.5,
        longitud: float = -70.0
    ) -> None:
        """Envía una lectura al tópico de Kafka."""
        if not self.producer:
            self.connect()
        
        message = {
            "sensor_id": sensor_id,
            "temperatura": temperatura,
            "humedad": humedad,
            "timestamp": int(datetime.now().timestamp()),
            "ubicacion": {
                "lat": latitud,
                "lon": longitud
            }
        }
        
        future = self.producer.send(
            self.config.kafka.topic,
            key=sensor_id,
            value=message
        )
        
        record_metadata = future.get(timeout=10)
        logger.info(
            f"Mensaje enviado - Topic: {record_metadata.topic}, "
            f"Partition: {record_metadata.partition}, "
            f"Offset: {record_metadata.offset}"
        )
    
    def close(self) -> None:
        """Cierra el productor."""
        if self.producer:
            self.producer.close()


def simulate_sensor_data(interval: int = 5) -> None:
    """
    Simula datos del sensor y los envía a Kafka.
    Útil para pruebas.
    """
    import random
    import time
    
    producer = ClimateProducer()
    producer.connect()
    
    base_temps = {
        1: 8, 2: 9, 3: 11, 4: 13, 5: 15,
        6: 17, 7: 18, 8: 17, 9: 16, 10: 14,
        11: 11, 12: 9
    }
    
    month = datetime.now().month
    base_temp = base_temps.get(month, 12)
    
    logger.info(f"Iniciando simulación (temp base: {base_temp}°C)")
    
    try:
        while True:
            temperatura = round(base_temp + random.uniform(-3, 5), 1)
            
            producer.send_reading(
                sensor_id="sensor_puno_01",
                temperatura=temperatura,
                humedad=round(random.uniform(50, 80), 1),
                latitud=-15.5,
                longitud=-70.0
            )
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        logger.info("Simulación detenida")
    finally:
        producer.close()


def main():
    """Punto de entrada principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Consumidor Kafka para clima")
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Ejecutar modo simulación"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Intervalo de simulación en segundos"
    )
    
    args = parser.parse_args()
    
    if args.simulate:
        simulate_sensor_data(args.interval)
    else:
        consumer = ClimateKafkaConsumer()
        consumer.connect()
        consumer.consume()


if __name__ == "__main__":
    main()