import os
import json
import time
import logging
import threading
from datetime import datetime, timezone
from prometheus_client import start_http_server, Gauge, Counter, Info
from kafka import KafkaConsumer, TopicPartition

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("kafka-value-exporter")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8000"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))

DATA_TOPICS = ["clima-grupo_2", "clima-grupo_3", "clima-grupo_4"]
ANOMALY_TOPICS = [f"{t}-anomalias" for t in DATA_TOPICS]

station_labels = ["station", "department", "province", "district"]

sensor_temp = Gauge("sensor_temperature_celsius", "Temperatura en °C", station_labels)
sensor_humidity = Gauge("sensor_humidity_percent", "Humedad relativa en %", station_labels)
sensor_pressure = Gauge("sensor_pressure_hpa", "Presión atmosférica en hPa", station_labels)
sensor_altitude = Gauge("sensor_altitude_m", "Altitud en msnm", station_labels)
sensor_iaq = Gauge("sensor_iaq_index", "Índice de calidad del aire (IAQ)", station_labels)
sensor_eco2 = Gauge("sensor_eco2_ppm", "CO₂ equivalente en ppm", station_labels)
sensor_voc = Gauge("sensor_voc_ppb", "Compuestos orgánicos volátiles en ppb", station_labels)
sensor_air_quality = Info("sensor_air_quality", "Clasificación de calidad del aire", station_labels)

anomaly_is_anomaly = Gauge("anomaly_detected", "1 si es anomalía, 0 si no", station_labels)
anomaly_score = Gauge("anomaly_z_score", "Z-score de la anomalía", station_labels)
anomaly_type = Info("anomaly_type", "Tipo de anomalía (normal/alerta/critica)", station_labels)
anomaly_historical_avg = Gauge("anomaly_historical_avg", "Promedio histórico de temperatura", station_labels)
anomaly_historical_std = Gauge("anomaly_historical_std", "Desviación estándar histórica", station_labels)

offset_current = Gauge("kafka_topic_offset", "Offset actual del topic", ["topic"])
offset_lag = Gauge("kafka_consumer_lag", "Lag del dashboard-consumer", ["topic"])
last_updated = Gauge("exporter_last_update_timestamp", "Timestamp Unix de última actualización", ["topic"])

station_map = {
    "clima-grupo_2": {"station": "grupo_2", "department": "PUNO", "province": "LAMPA", "district": "LAMPA"},
    "clima-grupo_3": {"station": "grupo_3", "department": "PUNO", "province": "PUNO", "district": "PUNO"},
    "clima-grupo_4": {"station": "grupo_4", "department": "PUNO", "province": "AZANGARO", "district": "AZANGARO"},
}

def get_last_message(consumer, topic):
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    latest_offset = consumer.position(tp)
    offset_current.labels(topic=topic).set(latest_offset)
    if latest_offset == 0:
        return None, 0
    consumer.seek(tp, latest_offset - 1)
    msg = consumer.poll(timeout_ms=5000, max_records=1)
    for tp_records in msg.values():
        for record in tp_records:
            return record, latest_offset
    return None, latest_offset

def get_consumer_lag(topic):
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id="dashboard-consumer",
            enable_auto_commit=False,
            client_id=f"lag-checker-{topic}",
        )
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        end_offset = consumer.end_offsets([tp])[tp]
        consumer.close()
        return None
    except Exception as e:
        logger.warning("Error getting lag for %s: %s", topic, e)
        return None

def update_sensor_metrics(topic, data):
    labels = station_map.get(topic)
    if not labels:
        return
    lbl = [labels["station"], labels["department"], labels["province"], labels["district"]]
    sensor_temp.labels(*lbl).set(float(data.get("temperatura", 0)))
    sensor_humidity.labels(*lbl).set(float(data.get("humedad", 0)))
    sensor_pressure.labels(*lbl).set(float(data.get("presion", 0)))
    sensor_altitude.labels(*lbl).set(float(data.get("altura", 0)))
    sensor_iaq.labels(*lbl).set(float(data.get("iaq", 0)))
    sensor_eco2.labels(*lbl).set(float(data.get("eco2", 0)))
    sensor_voc.labels(*lbl).set(float(data.get("voc", 0)))
    aq = data.get("calidad_aire", "Desconocido")
    sensor_air_quality.labels(*lbl).info({"calidad_aire": aq})
    last_updated.labels(topic=topic).set(time.time())

def update_anomaly_metrics(topic, data):
    labels = station_map.get(topic.replace("-anomalias", ""))
    if not labels:
        return
    lbl = [labels["station"], labels["department"], labels["province"], labels["district"]]
    is_anom = 1 if data.get("isAnomaly", False) in (True, "true", "True") else 0
    anomaly_is_anomaly.labels(*lbl).set(is_anom)
    anomaly_score.labels(*lbl).set(float(data.get("anomalyScore", 0)))
    atype = data.get("anomalyType", "normal")
    anomaly_type.labels(*lbl).info({"anomalyType": atype})
    anomaly_historical_avg.labels(*lbl).set(float(data.get("promedioHistorico", 0)))
    anomaly_historical_std.labels(*lbl).set(float(data.get("desviacionEstandar", 0)))
    last_updated.labels(topic=topic).set(time.time())

def poll_loop():
    logger.info("Iniciando Kafka Value Exporter en puerto %d", EXPORTER_PORT)
    logger.info("Servers: %s", BOOTSTRAP_SERVERS)
    logger.info("Topics: %s", DATA_TOPICS + ANOMALY_TOPICS)
    while True:
        try:
            consumer_raw = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                client_id="value-exporter-raw",
            )
            consumer_anom = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                client_id="value-exporter-anom",
            )
            for topic in DATA_TOPICS:
                msg, offset = get_last_message(consumer_raw, topic)
                if msg and msg.value:
                    update_sensor_metrics(topic, msg.value)
                    logger.debug("Actualizado %s: offset=%d temp=%.2f", topic, offset, msg.value.get("temperatura"))
                else:
                    logger.debug("Topic %s: sin datos", topic)
            for topic in ANOMALY_TOPICS:
                msg, offset = get_last_message(consumer_anom, topic)
                if msg and msg.value:
                    update_anomaly_metrics(topic, msg.value)
            consumer_raw.close()
            consumer_anom.close()
        except Exception as e:
            logger.error("Error en ciclo de polling: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    start_http_server(EXPORTER_PORT)
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    logger.info("Exporter corriendo en :%d", EXPORTER_PORT)
    t.join()
