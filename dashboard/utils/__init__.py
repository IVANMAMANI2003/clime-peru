import asyncio
import json
import os
import queue
import random
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

PARQUET_CACHE_KEY = "clime_peru_parquet_data"
METADATA_CACHE_KEY = "clime_peru_metadata"

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://mangrxgusewzgtewoayx.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1hbmdyeGd1c2V3emd0ZXdvYXl4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU3NzA5MzUsImV4cCI6MjA5MTM0NjkzNX0.ZkKvxC0M2WCp5JABUTfCNgh6rcTWKDYYP9S2qymmM48")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "grupo_3_air_quality")

PERU_TZ = timezone(timedelta(hours=-5))

def peru_now() -> datetime:
    return datetime.now(PERU_TZ)

def utc_to_peru(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PERU_TZ)

def _normalize_ts(raw_ts: str) -> str:
    """Normalize timestamp for fromisoformat: Z->+00:00, -05->-05:00."""
    ts = raw_ts.replace("Z", "+00:00").strip()
    if len(ts) >= 3 and ts[-3] in ('+', '-') and ts[-2:].isdigit():
        if ts[-5] != ':':
            ts += ':00'
    return ts


# ÔöÇÔöÇ Supabase Realtime (WebSocket) ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
def _record_from_payload(payload: dict) -> Optional[dict]:
    """Convierte un payload de Realtime en un registro estandarizado."""
    try:
        data = payload if isinstance(payload, dict) and "new" in payload else {}
        row = data.get("new", payload if not isinstance(payload, dict) else {})
        if not row:
            return None
        raw_ts = str(row.get("created_at", ""))
        if raw_ts:
            try:
                dt = datetime.fromisoformat(_normalize_ts(raw_ts))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=PERU_TZ)
                dt_utc = dt.astimezone(timezone.utc)
                ts_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
                ts_dt = dt_utc
            except (ValueError, TypeError):
                dt_utc = peru_now().astimezone(timezone.utc)
                ts_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
                ts_dt = dt_utc
        else:
            dt_utc = peru_now().astimezone(timezone.utc)
            ts_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
            ts_dt = dt_utc
        return {
            "sensor_id": row.get("estacion", "supabase"),
            "temperatura": float(row.get("temperatura", 0)),
            "humedad": float(row.get("humedad", 0)),
            "presion": float(row.get("presion", 0)),
            "altura": float(row.get("altura", 0)) if row.get("altura") else None,
            "iaq": float(row.get("iaq", 50)),
            "eco2": float(row.get("eco2", 400)),
            "voc": float(row.get("voc", 0)),
            "calidad_aire": str(row.get("calidad_aire", "")),
            "timestamp": int(ts_dt.timestamp()),
            "latencyMs": 0,
            "created_at": ts_str,
            "ts": ts_dt,
        }
    except Exception:
        return None


def init_supabase_realtime() -> Optional['queue.Queue']:
    """Subscribe to INSERT on ALL catalog tables."""
    try:
        from supabase import create_async_client
    except ImportError:
        st.warning("⚠️ 'supabase' no instalado. Usando polling REST como fallback.")
        return queue.Queue()

    data_queue: queue.Queue = queue.Queue()

    async def _subscribe():
        supabase = await create_async_client(SUPABASE_URL, SUPABASE_KEY)

        def on_insert(payload):
            record = _record_from_payload(payload)
            if record:
                data_queue.put(record)
                try:
                    st.rerun()
                except Exception:
                    pass

        catalog = load_sensor_catalog()
        for i, table_name in enumerate(catalog.keys()):
            channel = supabase.channel(f"realtime-stream-{i}")
            channel.on_postgres_changes(
                event="INSERT",
                schema="public",
                table=table_name,
                callback=on_insert,
            )
            await channel.subscribe()

        while True:
            await asyncio.sleep(3600)

    def _run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_subscribe())
        except Exception as e:
            print(f"[Realtime] Loop error: {e}")

    thread = threading.Thread(target=_run_loop, daemon=True)
    thread.start()
    print("[Realtime] WebSocket iniciado en background thread")
    return data_queue
def drain_realtime_queue(data_queue: 'queue.Queue') -> List[dict]:
    """Extrae todos los registros acumulados en la cola Realtime."""
    records = []
    while True:
        try:
            item = data_queue.get_nowait()
            if item is None:
                continue
            records.append(item)
        except queue.Empty:
            break
    return records


# ── Kafka Consumer ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def init_kafka_consumer() -> "queue.Queue":
    """Crea un consumer Kafka background que lee de todos los topics del catálogo."""
    try:
        from kafka import KafkaConsumer as KConsumer
    except ImportError:
        st.warning("⚠️ 'kafka-python' no instalado. Usando polling REST como fallback.")
        return queue.Queue()

    data_queue: queue.Queue = queue.Queue()
    catalog = load_sensor_catalog()

    topics = []
    for table_name, info in catalog.items():
        estacion = info.get("estacion", table_name)
        topics.append(f"clima-{estacion}")

    if not topics:
        print("[Kafka] Catálogo vacío, no hay topics que consumir")
        return data_queue

    consumer = None
    max_retries = 15
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KConsumer(
                *topics,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="dashboard-consumer",
                request_timeout_ms=5000,
                max_poll_interval_ms=300000,
            )
            print(f"[Kafka] Conectado a {KAFKA_BOOTSTRAP_SERVERS} en intento {attempt}")
            break
        except Exception as e:
            wait = min(2 ** attempt, 30)
            print(f"[Kafka] Intento {attempt}/{max_retries} falló: {e}. Reintentando en {wait}s...")
            time.sleep(wait)

    if consumer is None:
        print("[Kafka] No se pudo conectar tras {max_retries} intentos. El dashboard funcionará sin streaming en vivo.")
        return data_queue

    def _poll():
        while True:
            try:
                msgs = consumer.poll(timeout_ms=1000, max_records=100)
                for records in msgs.values():
                    for record in records:
                        data_queue.put(record.value)
            except Exception as e:
                print(f"[Kafka] Error en poll: {e}")
            time.sleep(0.5)

    thread = threading.Thread(target=_poll, daemon=True)
    thread.start()
    print(f"[Kafka] Consumer iniciado en background: topics={topics}")
    return data_queue


def drain_kafka_queue(data_queue: "queue.Queue") -> List[dict]:
    """Extrae todos los registros acumulados en la cola Kafka."""
    records = []
    while True:
        try:
            item = data_queue.get_nowait()
            if item is None:
                continue
            records.append(item)
        except queue.Empty:
            break
    return records

# ÔöÇÔöÇ Themes ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
THEMES = {
    "dark": {
        "bg_primary": "#0E1117",
        "bg_secondary": "#1A1C23",
        "bg_card": "#1E2028",
        "border": "#2A2D3A",
        "text_primary": "#F8F9FA",
        "text_secondary": "#9CA3AF",
        "accent_cyan": "#00D2C4",
        "accent_blue": "#38BDF8",
        "accent_orange": "#F59E0B",
        "accent_red": "#EF4444",
        "accent_green": "#10B981",
        "success": "#10B981",
        "warning": "#F59E0B",
        "error": "#EF4444",
        "plotly_template": "plotly_dark",
        "map_style": "carto-darkmatter",
    },
    "light": {
        "bg_primary": "#F8FAFC",
        "bg_secondary": "#FFFFFF",
        "bg_card": "#FFFFFF",
        "border": "#E2E8F0",
        "text_primary": "#0F172A",
        "text_secondary": "#64748B",
        "accent_cyan": "#0284C7",
        "accent_blue": "#0EA5E9",
        "accent_orange": "#D97706",
        "accent_red": "#DC2626",
        "accent_green": "#059669",
        "success": "#059669",
        "warning": "#D97706",
        "error": "#DC2626",
        "plotly_template": "plotly_white",
        "map_style": "carto-positron",
    },
}


def get_theme() -> dict:
    """Returns theme colors. Backgrounds are transparent (CSS handles them via data-theme)."""
    try:
        base = st.get_option("theme.base")
    except Exception:
        base = None
    if base == "light":
        t = dict(THEMES["light"])
    elif base == "dark":
        t = dict(THEMES["dark"])
    else:
        t = dict(THEMES["dark"])
    t["bg_primary"] = "rgba(0,0,0,0)"
    t["bg_card"] = "rgba(0,0,0,0)"
    t["plotly_template"] = "plotly_white"
    return t


# ÔöÇÔöÇ Kafka Metrics ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
KAFKA_METRICS_CACHE_SEC = 15


@st.cache_data(ttl=KAFKA_METRICS_CACHE_SEC, show_spinner=False)
def fetch_kafka_metrics() -> Dict[str, Any]:
    """Obtiene metricas clave de Kafka via API de Prometheus."""
    import requests as _requests
    metrics: Dict[str, Any] = {
        "clima_puno_offset": 0, "clima_anomalias_offset": 0,
        "brokers": 0, "consumer_lag": 0, "under_replicated": 0,
        "exporter_up": 0, "connected": False,
    }
    queries = {
        "clima_puno_offset": 'kafka_topic_partition_current_offset{topic="clima-puno"}',
        "clima_anomalias_offset": 'kafka_topic_partition_current_offset{topic="clima-anomalias"}',
        "brokers": "kafka_brokers",
        "consumer_lag": "kafka_consumergroup_lag",
        "exporter_up": 'up{job="kafka-exporter"}',
    }
    for key, query in queries.items():
        try:
            resp = _requests.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": query},
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("data", {}).get("result", [])
            if results:
                val = float(results[0]["value"][1])
                metrics[key] = val
        except Exception:
            pass
    if metrics["clima_puno_offset"] > 0 or metrics["brokers"] > 0:
        metrics["connected"] = True
    return metrics


def render_kafka_metrics_card(metrics: Dict[str, Any]):
    """Renderiza una fila de tarjetas con m├®tricas de Kafka."""
    t = get_theme()
    connected = metrics.get("connected", False)
    status_color = t["accent_green"] if connected else t["accent_red"]
    status_text = "CONECTADO" if connected else "SIN CONEXI├ôN"
    st.markdown(f"""
    <div style="background:{t['bg_card']};border:1px solid {t['border']};
                border-radius:10px;padding:14px;margin-bottom:12px;">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;">
            <span style="font-size:1.1rem;font-weight:600;color:{t['text_primary']};">­De Kafka</span>
            <span style="background:{status_color}22;color:{status_color};
                        padding:2px 10px;border-radius:12px;font-size:0.7rem;
                        font-weight:600;border:1px solid {status_color}44;">
                {status_text}
            </span>
        </div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;">
            <div style="text-align:center;">
                <div style="color:{t['text_secondary']};font-size:0.65rem;text-transform:uppercase;">Tópico clima-puno</div>
                <div style="font-size:1.3rem;font-weight:700;color:{t['accent_cyan']};">{int(metrics.get('clima_puno_offset', 0))}</div>
                <div style="color:{t['text_secondary']};font-size:0.6rem;">mensajes</div>
            </div>
            <div style="text-align:center;">
                <div style="color:{t['text_secondary']};font-size:0.65rem;text-transform:uppercase;">Tópicos anomalos</div>
                <div style="font-size:1.3rem;font-weight:700;color:{t['accent_orange']};">{int(metrics.get('clima_anomalias_offset', 0))}</div>
                <div style="color:{t['text_secondary']};font-size:0.6rem;">mensajes</div>
            </div>
            <div style="text-align:center;">
                <div style="color:{t['text_secondary']};font-size:0.65rem;text-transform:uppercase;">Brokers</div>
                <div style="font-size:1.3rem;font-weight:700;color:{t['accent_green']};">{int(metrics.get('brokers', 0))}</div>
                <div style="color:{t['text_secondary']};font-size:0.6rem;">activos</div>
            </div>
            <div style="text-align:center;">
                <div style="color:{t['text_secondary']};font-size:0.65rem;text-transform:uppercase;">Consumer Lag</div>
                <div style="font-size:1.3rem;font-weight:700;color:{'#EF4444' if metrics.get('consumer_lag', 0) > 100 else t['accent_green']};">{int(metrics.get('consumer_lag', 0))}</div>
                <div style="color:{t['text_secondary']};font-size:0.6rem;">pendientes</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


PERU_COORDS: Dict[str, Dict[str, float]] = {
    "AMAZONAS": {"lat": -5.5, "lon": -78.0},
    "ANCASH": {"lat": -9.5, "lon": -77.5},
    "APURIMAC": {"lat": -14.0, "lon": -73.0},
    "AREQUIPA": {"lat": -16.4, "lon": -71.5},
    "AYACUCHO": {"lat": -13.2, "lon": -74.2},
    "CAJAMARCA": {"lat": -7.2, "lon": -78.5},
    "CALLAO": {"lat": -12.0, "lon": -77.1},
    "CUSCO": {"lat": -13.5, "lon": -72.0},
    "HUANCAVELICA": {"lat": -12.8, "lon": -75.0},
    "HUANUCO": {"lat": -9.9, "lon": -76.2},
    "ICA": {"lat": -14.1, "lon": -75.7},
    "JUNIN": {"lat": -11.2, "lon": -75.5},
    "LA_LIBERTAD": {"lat": -8.0, "lon": -79.0},
    "LAMBAYEQUE": {"lat": -6.7, "lon": -79.9},
    "LIMA": {"lat": -12.0, "lon": -77.0},
    "LORETO": {"lat": -4.0, "lon": -74.0},
    "MADRE_DE_DIOS": {"lat": -12.0, "lon": -70.5},
    "MOQUEGUA": {"lat": -17.2, "lon": -70.9},
    "PASCO": {"lat": -10.5, "lon": -75.5},
    "PIURA": {"lat": -5.2, "lon": -80.6},
    "PUNO": {"lat": -15.8, "lon": -70.0},
    "SAN_MARTIN": {"lat": -6.5, "lon": -76.5},
    "TACNA": {"lat": -18.0, "lon": -70.2},
    "TUMBES": {"lat": -3.6, "lon": -80.5},
    "UCAYALI": {"lat": -8.5, "lon": -74.5},
    "AMAMZONAS": {"lat": -5.5, "lon": -78.0},
}


# ÔöÇÔöÇ Streaming Buffer ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
class StreamingBuffer:
    """Buffer circular thread-safe para datos en streaming (sin l├¡mite)."""

    def __init__(self, maxlen: int = 0):
        self._data = deque()  # sin l├¡mite
        self._last_ts: Optional[datetime] = None
        self._supabase_anchor: Optional[dict] = None

    def add(self, reading: dict) -> None:
        if "ts" in reading and isinstance(reading["ts"], str):
            raw = _normalize_ts(reading["ts"])
            try:
                dt = datetime.fromisoformat(raw)
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                reading["ts"] = dt
            except (ValueError, TypeError):
                pass
        self._data.append(reading)
        if "ts" in reading:
            self._last_ts = reading["ts"]

    def add_all(self, readings: List[dict]) -> None:
        for r in readings:
            self.add(r)

    def to_df(self) -> pd.DataFrame:
        if not self._data:
            return pd.DataFrame()
        df = pd.DataFrame(list(self._data))
        if "ts" in df.columns:
            if df["ts"].dtype == "object":
                df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
            df = df.sort_values("ts").reset_index(drop=True)
        return df

    @property
    def latest(self) -> Optional[dict]:
        return self._data[-1] if self._data else None

    @property
    def count(self) -> int:
        return len(self._data)

    def clear(self) -> None:
        self._data.clear()


def sensor_data_generator(
    buffer: StreamingBuffer,
    base_temp: float = 12.0,
    temp_range: float = 5.0,
    base_hum: float = 65.0,
    hum_range: float = 15.0,
) -> Generator[dict, None, None]:
    """Generador infinito de datos de sensor simulados."""
    month = datetime.now().month
    seasonal = {1: -2, 2: -1, 3: 1, 4: 3, 5: 4, 6: 5, 7: 5, 8: 4, 9: 3, 10: 2, 11: 0, 12: -1}
    offset = seasonal.get(month, 0)

    while True:
        temp = round(base_temp + offset + random.uniform(-temp_range / 2, temp_range / 2), 1)
        hum = round(base_hum + random.uniform(-hum_range / 2, hum_range / 2), 1)
        now = datetime.now(timezone.utc)
        reading = {
            "sensor_id": "sensor_puno_01",
            "temperatura": temp,
            "humedad": hum,
            "presion": round(random.uniform(640, 660), 1),
            "iaq": random.randint(20, 80),
            "eco2": random.randint(400, 800),
            "voc": round(random.uniform(0.1, 1.5), 2),
            "calidad_aire": random.choice(["EXCELENTE", "BUENA", "MODERADA"]),
            "timestamp": int(now.timestamp()),
            "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "ts": now,
        }
        yield reading


def get_temp_class(temp: float) -> str:
    if temp < 5:
        return "cold"
    if temp < 15:
        return "moderate"
    if temp < 25:
        return "warm"
    return "hot"


def get_aq_class(calidad: str) -> str:
    m = {"EXCELENTE": "excellent", "BUENA": "good", "MODERADA": "moderate-aq", "MALA": "bad"}
    return m.get(calidad.upper() if isinstance(calidad, str) else "", "moderate-aq")


def render_metric_card(label: str, value: str, unit: str = "", css_class: str = "", sub: str = ""):
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    st.markdown(f"""
    <div class="metric-card {css_class}">
        <div class="label">{label}</div>
        <div class="value">{value}</div>
        <div class="unit">{unit}</div>
        {sub_html}
    </div>
    """, unsafe_allow_html=True)


def _hex_to_rgba(hex_color: str, alpha: float = 0.2) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def build_gauge_chart(value: float, title: str = "Temperatura", min_val: float = -5,
                       max_val: float = 35, threshold: float = 25,
                       sub_title: str = "") -> go.Figure:
    t = get_theme()
    color = t["accent_green"]
    if value > threshold:
        color = t["accent_red"]
    elif value > threshold * 0.8:
        color = t["accent_orange"]
    elif value < 5:
        color = t["accent_blue"]

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        delta={"reference": 15, "increasing": {"color": t["accent_red"]},
               "decreasing": {"color": t["accent_blue"]}},
        number={"font": {"color": t["text_primary"], "size": 40},
                "suffix": "┬░C"},
        gauge={
            "axis": {"range": [min_val, max_val], "tickcolor": t["text_secondary"],
                     "tickfont": {"color": t["text_secondary"]}},
            "bar": {"color": color, "thickness": 0.5},
            "bgcolor": t["bg_card"],
            "borderwidth": 1,
            "bordercolor": t["border"],
            "steps": [
                {"range": [min_val, 5], "color": _hex_to_rgba(t["accent_blue"], 0.2)},
                {"range": [5, 15], "color": _hex_to_rgba(t["accent_green"], 0.2)},
                {"range": [15, 25], "color": _hex_to_rgba(t["accent_orange"], 0.2)},
                {"range": [25, max_val], "color": _hex_to_rgba(t["accent_red"], 0.2)},
            ],
            "threshold": {
                "line": {"color": t["accent_red"], "width": 3},
                "thickness": 0.75,
                "value": threshold,
            },
        },
    ))
    fig.update_layout(
        title={"text": f"{title}<br><span style='font-size:11px;color:{t['text_secondary']}'>{sub_title}</span>" if sub_title else title,
               "font": {"color": t["text_secondary"], "size": 14}},
        paper_bgcolor=t["bg_primary"],
        font={"color": t["text_primary"]},
        height=260,
        margin=dict(l=30, r=30, t=50, b=20),
    )
    return fig


# ÔöÇÔöÇ Supabase ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# ── Supabase ─────────────────────────────────────────────────────────────────────────────────────────
def _read_supabase_table(table_name: str, limit: int = 500) -> List[Dict[str, Any]]:
    """Fetch readings from a single Supabase table."""
    readings = []
    try:
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        url = f"{SUPABASE_URL}/rest/v1/{table_name}?order=created_at.desc&limit={limit}"
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        for row in data:
            raw_ts = str(row.get("created_at", ""))
            if raw_ts:
                try:
                    dt = datetime.fromisoformat(_normalize_ts(raw_ts))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=PERU_TZ)
                    dt_utc = dt.astimezone(timezone.utc)
                    ts_int = int(dt_utc.timestamp())
                    ts_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
                    ts_dt = dt_utc
                except Exception:
                    ts_int = 0
                    ts_str = str(raw_ts)
                    ts_dt = peru_now().astimezone(timezone.utc)
            else:
                ts_int = 0
                ts_str = ""
                ts_dt = peru_now().astimezone(timezone.utc)
            readings.append({
                "sensor_id": row.get("estacion", "supabase"),
                "temperatura": float(row.get("temperatura", 0)),
                "humedad": float(row.get("humedad", 0)),
                "presion": float(row.get("presion", 0)),
                "altura": float(row.get("altura", 0)) if row.get("altura") else None,
                "iaq": float(row.get("iaq", 50)),
                "eco2": float(row.get("eco2", 400)),
                "voc": float(row.get("voc", 0)),
                "calidad_aire": str(row.get("calidad_aire", "")),
                "timestamp": ts_int,
                "created_at": ts_str,
                "ts": ts_dt,
            })
    except Exception as e:
                print(f"[Supabase] Error fetching {table_name}: {e}")
    return readings


def fetch_all_supabase_readings(limit: int = 500) -> List[Dict[str, Any]]:
    """Fetch readings from ALL catalog tables."""
    catalog = load_sensor_catalog()
    all_readings = []
    for table_name in catalog:
        per_table = max(50, limit // max(len(catalog), 1))
        all_readings.extend(_read_supabase_table(table_name, per_table))
    return all_readings
def supabase_to_df(readings: List[Dict]) -> pd.DataFrame:
    if not readings:
        return pd.DataFrame()
    df = pd.DataFrame(readings)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.sort_values("ts")
    return df


# ÔöÇÔöÇ Map ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
def build_station_map(stations_df: pd.DataFrame) -> go.Figure:
    t = get_theme()
    if stations_df.empty:
        return go.Figure()
    depts = stations_df["department"].str.upper().str.strip()
    stations_df = stations_df.copy()
    stations_df["lat"] = depts.map(lambda d: PERU_COORDS.get(d, {}).get("lat"))
    stations_df["lon"] = depts.map(lambda d: PERU_COORDS.get(d, {}).get("lon"))
    stations_df = stations_df.dropna(subset=["lat", "lon"])

    fig = go.Figure()
    fig.add_trace(go.Scattermap(
        lat=stations_df["lat"],
        lon=stations_df["lon"],
        mode="markers+text",
        marker=dict(size=11, color=t["accent_cyan"], opacity=0.85),
        text=stations_df["station_name"].str.replace("_", " ").str.title(),
        textposition="top center",
        textfont=dict(color=t["text_secondary"], size=9),
        hovertext=stations_df.apply(
            lambda r: f"<b>{r['station_name'].replace('_',' ').title()}</b><br>"
                      f"{r.get('department','').title()}<br>"
                      f"{r.get('province','').replace('_',' ').title()}",
            axis=1,
        ),
        hoverinfo="text",
    ))
    fig.update_layout(
        map=dict(
            style=t["map_style"],
            center=dict(lat=-9.5, lon=-76.0),
            zoom=4.2,
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        height=520,
        paper_bgcolor=t["bg_primary"],
        font=dict(color=t["text_primary"]),
    )
    return fig


# ÔöÇÔöÇ Historical helpers ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
@st.cache_data(ttl=3600, show_spinner="Cargando datos...")
def load_climate_data(parquet_path: str) -> pd.DataFrame:
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet no encontrado: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=3600, show_spinner="Cargando metadatos...")
def load_stations_metadata(metadata_path: str) -> pd.DataFrame:
    if not os.path.exists(metadata_path):
        return pd.DataFrame()
    return pd.read_parquet(metadata_path)


def get_filter_options(df: pd.DataFrame) -> Dict[str, List]:
    return {
        "departments": sorted(df["department"].dropna().unique().tolist()),
        "variables": ["precip", "tmax", "tmin"],
        "provinces": sorted(df["province"].dropna().unique().tolist()),
    }


def filter_data(df: pd.DataFrame, department: Optional[str] = None,
                station: Optional[str] = None, start_date: Optional[str] = None,
                end_date: Optional[str] = None, variable: str = "tmax",
                province: Optional[str] = None) -> pd.DataFrame:
    result = df.copy()
    if department and department != "Todos":
        result = result[result["department"] == department]
    if province and province != "Todas":
        result = result[result["province"] == province]
    if station and station != "Todas":
        result = result[result["station_name"] == station]
    if start_date:
        result = result[result["date"] >= pd.to_datetime(start_date)]
    if end_date:
        result = result[result["date"] <= pd.to_datetime(end_date)]
    return result


def get_variable_label(variable: str) -> str:
    return {"precip": "Precipitación (mm)", "tmax": "Temperatura Maíxima (C°)",
            "tmin": "Temperatura Minima (C°)"}.get(variable, variable)


def calculate_statistics(df: pd.DataFrame, variable: str) -> Dict[str, float]:
    if variable not in df.columns:
        return {}
    col_data = df[variable].dropna()
    if len(col_data) == 0:
        return {"media": 0, "mediana": 0, "max": 0, "min": 0, "count": 0}
    return {"media": round(col_data.mean(), 2), "mediana": round(col_data.median(), 2),
            "max": round(col_data.max(), 2), "min": round(col_data.min(), 2),
            "count": len(col_data), "nulos": df[variable].isna().sum()}


def prepare_time_series(df: pd.DataFrame, variable: str, freq: str = "D") -> pd.DataFrame:
    result = df.set_index("date")[[variable]].copy()
    result = result.dropna()
    result = result.resample(freq).mean()
    return result.reset_index()


def prepare_download_data(df: pd.DataFrame, variable: str) -> pd.DataFrame:
    download_df = df[["date", "station_name", "department", variable]].copy()
    download_df["date"] = download_df["date"].dt.strftime("%Y-%m-%d")
    return download_df


@st.cache_data
def convert_df_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


# ── Sensor Config ────────────────────────────────────────────────────────
SENSOR_CONFIG_PATH = os.getenv("SENSOR_CONFIG_PATH",
                               str(Path(__file__).parent.parent.parent / "artifacts" / "sensor_config.json"))
CATALOG_PATH = os.getenv("CATALOG_PATH",
                         str(Path(__file__).parent.parent.parent / "artifacts" / "sensor_catalog.json"))


def load_sensor_config() -> dict:
    default = {"department": "PUNO", "province": "PUNO", "district": "PUNO", "station_name": "PUNO"}
    try:
        with open(SENSOR_CONFIG_PATH, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_sensor_config(config: dict):
    Path(SENSOR_CONFIG_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(SENSOR_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


def load_sensor_catalog() -> dict:
    try:
        with open(CATALOG_PATH, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def get_catalog_stations() -> list:
    catalog = load_sensor_catalog()
    stations = []
    for table_name, info in catalog.items():
        estacion = info.get("estacion", table_name)
        dept = info.get("department", "")
        prov = info.get("province", "")
        dist = info.get("district", "")
        stations.append({
            "table": table_name,
            "estacion": estacion,
            "department": dept,
            "province": prov,
            "district": dist,
            "topic": f"clima-{estacion}",
        })
    return sorted(stations, key=lambda s: s["estacion"])
