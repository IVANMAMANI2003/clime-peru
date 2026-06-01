"""
Módulo de configuración centralizada para CimaPerú.
Carga configuraciones desde config.yaml y variables de entorno.
"""

import os
import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class SparkConfig:
    app_name: str = "ClimePeruETL"
    master: str = "local[*]"
    memory: str = "2g"
    cores: str = "2"
    adaptive_enabled: bool = True
    coalesce_partitions_enabled: bool = True
    parquet_compression: str = "snappy"


@dataclass
class PathsConfig:
    base: str = ""
    input: str = ""
    output: str = ""
    metadata: str = ""
    logs: str = ""


@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:19092"
    internal_bootstrap: str = "clime-kafka:9092"
    topic: str = "clima-puno"
    topic_anomalias: str = "clima-anomalias"
    consumer_group: str = "clime-peru-consumer-group"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    session_timeout_ms: int = 30000
    max_poll_records: int = 500


@dataclass
class SupabaseConfig:
    url: str = ""
    api_key: str = ""
    table: str = "lecturas_sensor"
    poll_interval_seconds: int = 5
    realtime_enabled: bool = False


@dataclass
class ETLConfig:
    input_pattern: str = "*.txt"
    partition_columns: tuple = ("department", "province", "district", "year")
    date_format: str = "yyyy-M-d"
    null_values: tuple = (-99.9, "-99.9", "NA", "null")


@dataclass
class SensorConfig:
    min_temperature: float = -20.0
    max_temperature: float = 40.0
    anomaly_threshold_sigma: float = 2.0


@dataclass
class DashboardConfig:
    host: str = "0.0.0.0"
    port: int = 8501
    title: str = "CimaPerú"
    page_icon: str = "🌤️"
    layout: str = "wide"


@dataclass
class DatabaseConfig:
    url: str = ""
    driver: str = "org.postgresql.Driver"
    user: str = ""
    password: str = ""
    table_prefix: str = "sensor_data_"
    batch_size: int = 100


@dataclass
class StreamingConfig:
    trigger_interval: str = "5 seconds"
    watermark: str = "30 seconds"
    window_duration: str = "1 minute"
    window_slide: str = "1 minute"
    output_mode: str = "append"
    checkpoint_location: str = ""


@dataclass
class AppConfig:
    spark: SparkConfig = field(default_factory=SparkConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    supabase: SupabaseConfig = field(default_factory=SupabaseConfig)
    etl: ETLConfig = field(default_factory=ETLConfig)
    sensor: SensorConfig = field(default_factory=SensorConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)


class ConfigManager:
    """
    Gestor centralizado de configuración.
    Soporta carga desde YAML y variables de entorno.
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Optional[AppConfig] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._load_config()
    
    def _load_config(self) -> None:
        """Carga la configuración desde el archivo YAML."""
        config_path = Path(__file__).parent / "config.yaml"
        
        if not config_path.exists():
            raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
        
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        
        self._config = self._parse_config(data)
        self._apply_env_overrides()
    
    def _parse_config(self, data: Dict[str, Any]) -> AppConfig:
        """Convierte el diccionario en objetos dataclass."""
        
        spark_data = data.get("spark", {})
        spark_config = SparkConfig(
            app_name=spark_data.get("app_name", "ClimePeruETL"),
            master=spark_data.get("master", "local[*]"),
            memory=spark_data.get("memory", "2g"),
            cores=spark_data.get("cores", "2"),
            adaptive_enabled=spark_data.get("adaptive_enabled", True),
            coalesce_partitions_enabled=spark_data.get("coalesce_partitions_enabled", True),
            parquet_compression=spark_data.get("parquet_compression", "snappy"),
        )
        
        paths_data = data.get("paths", {})
        paths_config = PathsConfig(
            base=paths_data.get("base", ""),
            input=paths_data.get("input", ""),
            output=paths_data.get("output", ""),
            metadata=paths_data.get("metadata", ""),
            logs=paths_data.get("logs", ""),
        )
        
        kafka_data = data.get("kafka", {})
        kafka_config = KafkaConfig(
            bootstrap_servers=kafka_data.get("bootstrap_servers", "localhost:19092"),
            internal_bootstrap=kafka_data.get("internal_bootstrap", "clime-kafka:9092"),
            topic=kafka_data.get("topic", "clima-puno"),
            topic_anomalias=kafka_data.get("topic_anomalias", "clima-anomalias"),
            consumer_group=kafka_data.get("consumer_group", "clime-peru-consumer-group"),
            auto_offset_reset=kafka_data.get("auto_offset_reset", "earliest"),
            enable_auto_commit=kafka_data.get("enable_auto_commit", True),
            session_timeout_ms=kafka_data.get("session_timeout_ms", 30000),
            max_poll_records=kafka_data.get("max_poll_records", 500),
        )
        
        supabase_data = data.get("supabase", {})
        supabase_config = SupabaseConfig(
            url=supabase_data.get("url", ""),
            api_key=supabase_data.get("api_key", ""),
            table=supabase_data.get("table", "lecturas_sensor"),
            poll_interval_seconds=supabase_data.get("poll_interval_seconds", 5),
            realtime_enabled=supabase_data.get("realtime_enabled", False),
        )
        
        etl_data = data.get("etl", {})
        etl_config = ETLConfig(
            input_pattern=etl_data.get("input_pattern", "*.txt"),
            partition_columns=tuple(etl_data.get("partition_columns", ["department", "province", "district", "year"])),
            date_format=etl_data.get("date_format", "yyyy-M-d"),
            null_values=tuple(etl_data.get("null_values", [-99.9, "-99.9", "NA", "null"])),
        )

        sensor_data = data.get("sensor", {})
        sensor_config = SensorConfig(
            min_temperature=sensor_data.get("min_temperature", -20.0),
            max_temperature=sensor_data.get("max_temperature", 40.0),
            anomaly_threshold_sigma=sensor_data.get("anomaly_threshold_sigma", 2.0),
        )
        
        dashboard_data = data.get("dashboard", {})
        dashboard_config = DashboardConfig(
            host=dashboard_data.get("host", "0.0.0.0"),
            port=dashboard_data.get("port", 8501),
            title=dashboard_data.get("title", "CimaPerú"),
            page_icon=dashboard_data.get("page_icon", "🌤️"),
            layout=dashboard_data.get("layout", "wide"),
        )
        
        database_data = data.get("database", {})
        database_config = DatabaseConfig(
            url=os.getenv("POSTGRES_URL", database_data.get("url", "")),
            driver=database_data.get("driver", "org.postgresql.Driver"),
            user=os.getenv("POSTGRES_USER", database_data.get("user", "")),
            password=os.getenv("POSTGRES_PASSWORD", database_data.get("password", "")),
            table_prefix=database_data.get("table_prefix", "sensor_data_"),
            batch_size=database_data.get("batch_size", 100),
        )

        streaming_data = data.get("streaming", {})
        streaming_config = StreamingConfig(
            trigger_interval=streaming_data.get("trigger_interval", "5 seconds"),
            watermark=streaming_data.get("watermark", "30 seconds"),
            window_duration=streaming_data.get("window_duration", "1 minute"),
            window_slide=streaming_data.get("window_slide", "1 minute"),
            output_mode=streaming_data.get("output_mode", "append"),
            checkpoint_location=streaming_data.get("checkpoint_location", ""),
        )
        
        return AppConfig(
            spark=spark_config,
            paths=paths_config,
            kafka=kafka_config,
            supabase=supabase_config,
            etl=etl_config,
            sensor=sensor_config,
            database=database_config,
            dashboard=dashboard_config,
            streaming=streaming_config,
        )
    
    def _apply_env_overrides(self) -> None:
        """Aplica sobrescrituras desde variables de entorno."""
        
        env_mappings = {
            "SUPABASE_URL": (lambda v: setattr(self._config.supabase, "url", v)),
            "SUPABASE_API_KEY": (lambda v: setattr(self._config.supabase, "api_key", v)),
            "KAFKA_BOOTSTRAP_SERVERS": (lambda v: setattr(self._config.kafka, "bootstrap_servers", v)),
            "KAFKA_TOPIC": (lambda v: setattr(self._config.kafka, "topic", v)),
        }
        
        for env_var, setter in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                setter(value)
    
    @property
    def config(self) -> AppConfig:
        """Retorna la configuración."""
        return self._config


def get_config() -> AppConfig:
    """Obtiene la instancia singleton de configuración."""
    return ConfigManager().config


def reload_config() -> AppConfig:
    """Recarga la configuración desde el archivo."""
    ConfigManager._instance = None
    ConfigManager._config = None
    return ConfigManager().config