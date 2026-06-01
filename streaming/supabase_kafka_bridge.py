import os
import sys
import json
import asyncio
import threading
import time
import argparse
from pathlib import Path
from typing import Optional

import requests
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger

logger = get_logger("supabase_kafka_bridge")

SUPABASE_URL = "https://mangrxgusewzgtewoayx.supabase.co"


def _normalize_ts(raw_ts: str) -> str:
    ts = raw_ts.replace("Z", "+00:00").strip()
    if len(ts) >= 3 and ts[-3] in ('+', '-') and ts[-2:].isdigit():
        if ts[-5] != ':':
            ts += ':00'
    return ts
SUPABASE_KEY = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
                "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1hbmdyeGd1c2V3emd0ZXdvYXl4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU3NzA5MzUsImV4cCI6MjA5MTM0NjkzNX0."
                "ZkKvxC0M2WCp5JABUTfCNgh6rcTWKDYYP9S2qymmM48")
CATALOG_PATH = "/app/artifacts/sensor_catalog.json"
CHECKPOINT_DIR = "/app/artifacts/bridge_checkpoints"
INITIAL_PAGE_LIMIT = 1000
MAX_INITIAL_RECORDS = 200000


def load_catalog() -> dict:
    try:
        with open(CATALOG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("sensor_catalog.json no encontrado en %s", CATALOG_PATH)
        return {}


class SupabaseKafkaBridge:

    def __init__(self, supabase_table: str, kafka_topic: str, time_format: str = "string"):
        self.config = get_config()
        self.supabase_table = supabase_table
        self.kafka_topic = kafka_topic
        self.time_format = time_format
        self.producer: Optional[KafkaProducer] = None
        self.running = False

        catalog = load_catalog()
        entry = catalog.get(supabase_table, {})
        self.estacion = entry.get("estacion", supabase_table)
        self.department = entry.get("department", "")
        self.province = entry.get("province", "")
        self.district = entry.get("district", "")

        self._kafka_servers = (
            self.config.kafka.internal_bootstrap or
            self.config.kafka.bootstrap_servers
        )

        self._checkpoint_path = f"{CHECKPOINT_DIR}/{self.supabase_table}.json"

    def _read_checkpoint(self) -> int:
        try:
            with open(self._checkpoint_path, "r") as f:
                data = json.load(f)
            last_id = data.get("last_id", 0)
            logger.info("Checkpoint encontrado para '%s': last_id=%d", self.supabase_table, last_id)
            return last_id
        except (FileNotFoundError, json.JSONDecodeError):
            logger.info("No hay checkpoint para '%s' — primera ejecucion", self.supabase_table)
            return 0

    def _write_checkpoint(self, last_id: int):
        import os
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        with open(self._checkpoint_path, "w") as f:
            json.dump({"last_id": last_id, "table": self.supabase_table, "topic": self.kafka_topic}, f)
        logger.info("Checkpoint guardado para '%s': last_id=%d", self.supabase_table, last_id)

    def _connect_kafka(self):
        logger.info("Conectando productor Kafka a %s", self._kafka_servers)
        self.producer = KafkaProducer(
            bootstrap_servers=self._kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks="all",
            retries=5,
        )
        logger.info("Productor Kafka conectado a topic '%s'", self.kafka_topic)

    def _parse_ts(self, raw: str):
        return _normalize_ts(raw)

    def _record_to_message(self, row: dict) -> dict:
        def safe_float(val, default=0.0):
            if val is None:
                return default
            try:
                return float(val)
            except (TypeError, ValueError):
                return default

        return {
            "sensor_id": self.estacion,
            "estacion": self.estacion,
            "department": self.department,
            "province": self.province,
            "district": self.district,
            "temperatura": safe_float(row.get("temperatura")),
            "humedad": safe_float(row.get("humedad")),
            "presion": safe_float(row.get("presion")),
            "altura": safe_float(row.get("altura"), None),
            "iaq": safe_float(row.get("iaq"), 50),
            "eco2": safe_float(row.get("eco2"), 400),
            "voc": safe_float(row.get("voc")),
            "calidad_aire": row.get("calidad_aire", ""),
            "ts": self._parse_ts(row.get("created_at", "")),
            "created_at": row.get("created_at", ""),
            "id": row.get("id"),
        }

    def _publish(self, message: dict):
        if not self.producer:
            return
        try:
            key = str(message.get("id", message.get("created_at", "")))
            future = self.producer.send(self.kafka_topic, key=key, value=message)
            future.get(timeout=10)
        except Exception as e:
            logger.error("Error publicando en Kafka: %s", e)

    def _load_initial_data(self, start_from: int = 0) -> int:
        logger.info("Cargando datos historicos desde Supabase tabla '%s' (desde id > %d)...", self.supabase_table, start_from)
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        url = f"{SUPABASE_URL}/rest/v1/{self.supabase_table}"

        max_id = start_from
        total_published = 0
        offset = 0
        try:
            while offset < MAX_INITIAL_RECORDS:
                params_page = {
                    "select": "*", "order": "id.asc",
                    "limit": INITIAL_PAGE_LIMIT, "offset": offset
                }
                if start_from > 0:
                    params_page["id"] = f"gt.{start_from}"
                resp_page = requests.get(url, headers=headers, params=params_page, timeout=30)
                resp_page.raise_for_status()
                records = resp_page.json()
                if not records:
                    break
                for record in records:
                    rid = record.get("id", 0)
                    if rid > max_id:
                        max_id = rid
                    msg = self._record_to_message(record)
                    self._publish(msg)
                total_published += len(records)
                logger.info("Lote %d: %d registros publicados (total: %d, ultimo id=%d) a topic '%s'",
                            offset // INITIAL_PAGE_LIMIT + 1, len(records), total_published, max_id, self.kafka_topic)
                offset += INITIAL_PAGE_LIMIT
                if start_from > 0 and len(records) < INITIAL_PAGE_LIMIT:
                    break

            logger.info("Carga inicial completada: %d registros a topic '%s' (ultimo id=%d)",
                        total_published, self.kafka_topic, max_id)
        except Exception as e:
            logger.error("Error en carga inicial para tabla '%s': %s", self.supabase_table, e)
        return max_id

    def _poll_supabase(self, last_id: int) -> int:
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        url = f"{SUPABASE_URL}/rest/v1/{self.supabase_table}"
        params = {"id": f"gt.{last_id}", "select": "*", "order": "id.asc", "limit": 50}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            records = resp.json()
            if not records:
                return last_id
            max_id = last_id
            for record in records:
                rid = record.get("id", 0)
                msg = self._record_to_message(record)
                self._publish(msg)
                if rid > max_id:
                    max_id = rid
            logger.info("Polling: %d nuevos registros (IDs %d..%d) a topic '%s'",
                        len(records), last_id, max_id, self.kafka_topic)
            return max_id
        except Exception as e:
            logger.error("Error en polling de '%s': %s", self.supabase_table, e)
            return last_id

    async def _subscribe_realtime(self):
        from supabase import create_async_client

        logger.info("Conectando a Supabase Realtime para tabla '%s'...", self.supabase_table)
        try:
            client = await create_async_client(SUPABASE_URL, SUPABASE_KEY)
        except Exception as e:
            logger.error("Error creando cliente Supabase: %s", e)
            return

        def on_insert(payload):
            try:
                row = payload.get("new", {})
                if not row:
                    return
                msg = self._record_to_message(row)
                self._publish(msg)
                logger.info("Realtime: nuevo registro estacion=%s id=%s", self.estacion, row.get("id"))
            except Exception as e:
                logger.error("Error en callback Realtime: %s", e, exc_info=True)

        channel = client.channel(f"bridge-{self.supabase_table}")
        channel.on_postgres_changes(
            event="INSERT",
            schema="public",
            table=self.supabase_table,
            callback=on_insert,
        )
        await channel.subscribe()
        logger.info("Suscrito a cambios Realtime en %s", self.supabase_table)

        last_id = self._read_checkpoint()
        if last_id > 0:
            logger.info("Checkpoint encontrado, publicando solo datos nuevos desde id=%d", last_id)
            last_id = self._load_initial_data(start_from=last_id)
        else:
            logger.info("Sin checkpoint — publicando todo el historico")
            last_id = self._load_initial_data(start_from=0)
        self._write_checkpoint(last_id)

        while self.running:
            await asyncio.sleep(30)
            new_last_id = self._poll_supabase(last_id)
            if new_last_id > last_id:
                self._write_checkpoint(new_last_id)
                last_id = new_last_id

    def _run_async_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._subscribe_realtime())
        except Exception as e:
            logger.error("Error en loop Realtime: %s", e)
        finally:
            loop.close()

    def start(self):
        logger.info("=" * 60)
        logger.info("PUENTE SUPABASE -> KAFKA")
        logger.info("  Tabla:   %s", self.supabase_table)
        logger.info("  Estacion: %s", self.estacion)
        logger.info("  Topic:    %s", self.kafka_topic)
        logger.info("  Time:     %s", self.time_format)
        logger.info("=" * 60)

        self.running = True
        self._connect_kafka()

        thread = threading.Thread(target=self._run_async_loop, daemon=True)
        thread.start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        logger.info("Deteniendo bridge %s...", self.estacion)
        self.running = False
        if self.producer:
            self.producer.close()


def main():
    parser = argparse.ArgumentParser(description="Supabase → Kafka Bridge")
    parser.add_argument("--table", required=True, help="Nombre de tabla en Supabase")
    parser.add_argument("--topic", required=True, help="Topico Kafka de salida")
    parser.add_argument("--time-format", choices=["string", "timestamptz"], default="string",
                        help="Formato de created_at en Supabase")
    args = parser.parse_args()

    bridge = SupabaseKafkaBridge(
        supabase_table=args.table,
        kafka_topic=args.topic,
        time_format=args.time_format,
    )
    try:
        bridge.start()
    except KeyboardInterrupt:
        bridge.stop()


if __name__ == "__main__":
    main()
