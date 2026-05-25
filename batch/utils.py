"""
Utilidades comunes para el sistema CimaPerú.
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class ClimateRecord:
    """Representa un registro climático."""
    station_name: str
    department: str
    province: str
    district: str
    year: int
    month: int
    day: int
    precip: Optional[float] = None
    tmax: Optional[float] = None
    tmin: Optional[float] = None


@dataclass
class SensorReading:
    """Representa una lectura del sensor."""
    sensor_id: str
    temperatura: float
    humedad: Optional[float] = None
    timestamp: int = 0
    latitud: float = 0.0
    longitud: float = 0.0


@dataclass
class AnomalyResult:
    """Resultado del análisis de anomalía."""
    sensor_id: str
    temperatura: float
    timestamp: int
    promedio_historico: float
    desviacion_estandar: float
    limite_inferior: float
    limite_superior: float
    es_anomalia: bool
    diferencia_promedio: float
    mensaje: str


def extract_metadata_from_filename(filepath: str) -> tuple:
    """
    Extrae metadatos de una estación desde el nombre del archivo.
    
    Formato esperado: ESTACION-DEPARTAMENTO-PROVINCIA-DISTRITO.txt
    
    Args:
        filepath: Ruta al archivo
        
    Returns:
        Tupla (station_name, department, province, district)
    """
    import os
    
    base = os.path.basename(filepath)
    name_without_ext = base.replace('.txt', '')
    parts = name_without_ext.split('-')
    
    if len(parts) >= 4:
        district = parts[-1]
        province = parts[-2]
        department = parts[-3]
        station_name = '-'.join(parts[:-3])
        return (station_name, department, province, district)
    else:
        raise ValueError(f"Formato inesperado: {base}")


def parse_senamhi_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Parsea una línea de datos SENAMHI.
    
    Formato: Año Mes Día Precip Tmax Tmin
    
    Args:
        line: Línea de texto con datos
        
    Returns:
        Diccionario con los valores parseados o None si es inválido
    """
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


def validate_sensor_reading(
    temperatura: float,
    min_temp: float = -20.0,
    max_temp: float = 40.0
) -> bool:
    """
    Valida que una lectura de temperatura esté en rango válido.
    
    Args:
        temperatura: Valor de temperatura
        min_temp: Temperatura mínima válida
        max_temp: Temperatura máxima válida
        
    Returns:
        True si la lectura es válida
    """
    return min_temp <= temperatura <= max_temp


def calculate_anomaly(
    temperatura_actual: float,
    promedio_historico: float,
    desviacion_estandar: float,
    sigma_threshold: float = 2.0
) -> AnomalyResult:
    """
    Calcula si una lectura es anómala basándose en el historial.
    
    Args:
        temperatura_actual: Lectura actual del sensor
        promedio_historico: Promedio histórico del mismo período
        desviacion_estandar: Desviación estándar histórica
        sigma_threshold: Número de desviaciones estándar para considerar anómalo
        
    Returns:
        AnomalyResult con el análisis completo
    """
    limite_inferior = promedio_historico - (sigma_threshold * desviacion_estandar)
    limite_superior = promedio_historico + (sigma_threshold * desviacion_estandar)
    
    es_anomalia = not (limite_inferior <= temperatura_actual <= limite_superior)
    diferencia = temperatura_actual - promedio_historico
    
    if es_anomalia:
        if temperatura_actual > promedio_historico:
            mensaje = f"Temperatura {temperatura_actual}°C es {abs(diferencia):.1f}°C superior al promedio histórico"
        else:
            mensaje = f"Temperatura {temperatura_actual}°C es {abs(diferencia):.1f}°C inferior al promedio histórico"
    else:
        mensaje = f"Temperatura dentro del rango histórico ({limite_inferior:.1f}°C a {limite_superior:.1f}°C)"
    
    return AnomalyResult(
        sensor_id="",
        temperatura=temperatura_actual,
        timestamp=0,
        promedio_historico=promedio_historico,
        desviacion_estandar=desviacion_estandar,
        limite_inferior=limite_inferior,
        limite_superior=limite_superior,
        es_anomalia=es_anomalia,
        diferencia_promedio=diferencia,
        mensaje=mensaje
    )


def serialize_for_kafka(obj: Any) -> str:
    """Serializa un objeto a JSON para enviar a Kafka."""
    if hasattr(obj, '__dict__'):
        return json.dumps(obj.__dict__, default=str)
    return json.dumps(obj, default=str)


def deserialize_from_kafka(data: bytes) -> Dict[str, Any]:
    """Deserializa un mensaje de Kafka desde JSON."""
    return json.loads(data.decode('utf-8'))


def format_timestamp(timestamp: int, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Formatea un timestamp Unix a formato legible."""
    return datetime.fromtimestamp(timestamp).strftime(fmt)


def get_month_name(month: int, locale: str = "es") -> str:
    """Retorna el nombre del mes."""
    months_es = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]
    months_en = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    
    months = months_es if locale == "es" else months_en
    return months[month - 1] if 1 <= month <= 12 else "Mes inválido"


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """Divide una lista en chunks de tamaño específico."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]