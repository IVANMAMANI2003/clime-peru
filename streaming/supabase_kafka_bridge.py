import sys
import json
import asyncio
import threading
import time
from pathlib import Path
from typing import Optional

import requests
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger

logger = get_logger("supabase_kafka_bridge")

SUPABASE_URL = "https://mangrxgusewzgtewoayx.supabase.co"
SUPABASE_KEY = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
                "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1hbmdyeGd1c2V3emd0ZXdvYXl4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU3NzA5MzUsImV4cCI6MjA5MTM0NjkzNX0."
                "ZkKvxC0M2WCp5JABUTfCNgh6rcTWKDYYP9S2qymmM48")
SUPABASE_TABLE = "grupo_3_air_quality"
KAFKA_TOPIC = "clima-puno"
INITIAL_PAGE_LIMIT = 1000
MAX_INITIAL_RECORDS = 50000


class SupabaseKafkaBridge:

    def __init__(self):
        self.config = get_config()
        self.producer: Optional[KafkaProducer] = None
        self.running = False

        self._kafka_servers = (
            self.config.kafka.internal_bootstrap or
            self.config.kafka.bootstrap_servers
        )

    def _connect_kafka(self):
        logger.info("Conectando productor Kafka a %s", self._kafka_servers)
        self.producer = KafkaProducer(
            bootstrap_servers=self._kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks="all",
            retries=5,
        )
        logger.info("Productor Kafka conectado")

    def _record_to_message(self, row: dict) -> dict:
        return {
            "sensor_id": row.get("estacion", "supabase"),
            "temperatura": float(row.get("temperatura", 0)),
            "humedad": float(row.get("humedad", 0)),
            "presion": float(row.get("presion", 0)),
            "altura": float(row["altura"]) if row.get("altura") else None,
            "iaq": float(row.get("iaq", 50)),
            "eco2": float(row.get("eco2", 400)),
            "voc": float(row.get("voc", 0)),
            "calidad_aire": row.get("calidad_aire", ""),
            "created_at": row.get("created_at", ""),
            "id": row.get("id"),
        }

    def _publish(self, message: dict):
        if not self.producer:
            return
        try:
            key = str(message.get("id", message.get("created_at", "")))
            future = self.producer.send(KAFKA_TOPIC, key=key, value=message)
            future.get(timeout=10)
        except Exception as e:
            logger.error("Error publicando en Kafka: %s", e)

    def _load_initial_data(self) -> int:
        logger.info("Cargando TODOS los datos históricos desde Supabase...")
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
        params = {"select": "id,created_at", "order": "created_at.desc", "limit": 1}
        max_id = 0
        total_published = 0
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data:
                max_id = data[0].get("id", 0)
                logger.info("ID máximo en Supabase: %d", max_id)

            offset = 0
            while offset < MAX_INITIAL_RECORDS:
                params_page = {
                    "select": "*", "order": "created_at.desc",
                    "limit": INITIAL_PAGE_LIMIT, "offset": offset
                }
                resp_page = requests.get(url, headers=headers, params=params_page, timeout=30)
                resp_page.raise_for_status()
                records = resp_page.json()
                if not records:
                    break
                for record in reversed(records):
                    msg = self._record_to_message(record)
                    self._publish(msg)
                total_published += len(records)
                logger.info("Lote %d: publicados %d registros (total acumulado: %d)",
                            offset // INITIAL_PAGE_LIMIT + 1, len(records), total_published)
                offset += INITIAL_PAGE_LIMIT

            logger.info("Carga inicial completada: %d registros publicados en Kafka", total_published)
        except Exception as e:
            logger.error("Error en carga inicial: %s", e)
        return max_id

    def _poll_supabase(self, last_id: int) -> int:
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
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
            logger.info("Polling: publicados %d nuevos registros (IDs %d..%d)",
                        len(records), last_id, max_id)
            return max_id
        except Exception as e:
            logger.error("Error en polling Supabase: %s", e)
            return last_id

    async def _subscribe_realtime(self):
        from supabase import create_async_client

        logger.info("Conectando a Supabase Realtime...")
        try:
            client = await create_async_client(SUPABASE_URL, SUPABASE_KEY)
        except Exception as e:
            logger.error("Error creando cliente Supabase: %s", e)
            return

        def on_insert(payload):
            try:
                row = payload.get("new", {})
                if not row:
                    logger.debug("Payload sin 'new': %s", payload)
                    return
                msg = self._record_to_message(row)
                self._publish(msg)
                logger.info("Realtime: nuevo registro id=%s", row.get("id"))
            except Exception as e:
                logger.error("Error en callback Realtime: %s", e, exc_info=True)

        channel = client.channel("kafka-bridge")
        channel.on_postgres_changes(
            event="INSERT",
            schema="public",
            table=SUPABASE_TABLE,
            callback=on_insert,
        )
        await channel.subscribe()
        logger.info("Suscrito a cambios Realtime en %s", SUPABASE_TABLE)

        last_id = self._load_initial_data()
        while self.running:
            await asyncio.sleep(30)
            last_id = self._poll_supabase(last_id)

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
        logger.info("INICIANDO PUENTE SUPABASE -> KAFKA")
        logger.info("=" * 60)

        self.running = True
        self._connect_kafka()

        thread = threading.Thread(target=self._run_async_loop, daemon=True)
        thread.start()
        logger.info("Listener Realtime iniciado en thread background")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        logger.info("Deteniendo puente...")
        self.running = False
        if self.producer:
            self.producer.close()
            logger.info("Productor Kafka cerrado")


def main():
    bridge = SupabaseKafkaBridge()
    try:
        bridge.start()
    except KeyboardInterrupt:
        bridge.stop()


if __name__ == "__main__":
    main()
