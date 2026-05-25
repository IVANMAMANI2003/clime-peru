"""
Módulo de logging para CimaPerú.
Proporciona configuración de logging estandarizada.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


class ColoredFormatter(logging.Formatter):
    """Formateador con colores para la consola."""
    
    COLORS = {
        'DEBUG': '\033[36m',
        'INFO': '\033[32m',
        'WARNING': '\033[33m',
        'ERROR': '\033[31m',
        'CRITICAL': '\033[35m',
    }
    RESET = '\033[0m'
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logger(
    name: str,
    level: str = "INFO",
    log_file: Optional[str] = None,
    console: bool = True
) -> logging.Logger:
    """
    Configura un logger con salida a consola y opcionalmente a archivo.
    
    Args:
        name: Nombre del logger
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Ruta opcional para archivo de log
        console: Si True, muestra salida en consola
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    logger.handlers.clear()
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        console_handler.setFormatter(ColoredFormatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        ))
        logger.addHandler(console_handler)
    
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Obtiene un logger configurado. Si ya existe, lo retorna.
    
    Args:
        name: Nombre del logger
        level: Nivel de logging
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        return setup_logger(name, level)
    
    return logger


class LogContext:
    """Context manager para logs con información adicional."""
    
    def __init__(self, logger: logging.Logger, context: str):
        self.logger = logger
        self.context = context
    
    def __enter__(self):
        self.logger.info(f"▶ {self.context}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.logger.error(f"✖ {self.context}: {exc_val}")
        else:
            self.logger.info(f"✓ {self.context} completado")


def create_daily_logger(name: str, log_dir: str) -> logging.Logger:
    """
    Crea un logger con archivo rotativo diario.
    
    Args:
        name: Nombre del logger
        log_dir: Directorio para archivos de log
        
    Returns:
        Logger configurado
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    log_file = log_path / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
    
    return setup_logger(name, log_file=str(log_file))