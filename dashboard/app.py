import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config
from config.logger import get_logger
from dashboard.utils import (
    load_climate_data, load_stations_metadata, get_filter_options,
    filter_data, get_variable_label, calculate_statistics,
    prepare_time_series, prepare_download_data, convert_df_to_csv,
    fetch_all_supabase_readings, supabase_to_df, build_station_map,
    render_metric_card, get_temp_class, get_aq_class, build_gauge_chart,
    StreamingBuffer, get_theme, get_theme_css,
    init_supabase_realtime, drain_realtime_queue,
    fetch_kafka_metrics, render_kafka_metrics_card,
    PERU_COORDS, SUPABASE_TABLE,
)

logger = get_logger("dashboard")


# ── Streaming Helpers ───────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _get_realtime_queue():
    """Crea y cachea la suscripción Realtime (persiste entre reruns)."""
    return init_supabase_realtime()


def init_streaming_buffer() -> StreamingBuffer:
    if "stream_buffer" not in st.session_state:
        st.session_state.stream_buffer = StreamingBuffer(maxlen=100)
    return st.session_state.stream_buffer


def init_session_state():
    if "theme" not in st.session_state:
        st.session_state.theme = "dark"
    if "streaming_paused" not in st.session_state:
        st.session_state.streaming_paused = False
    if "supabase_initialized" not in st.session_state:
        st.session_state.supabase_initialized = False
    init_streaming_buffer()
    _get_realtime_queue()  # inicia la suscripción WebSocket


# ── Dashboard ───────────────────────────────────────────────────────────
class ClimateDashboard:

    def __init__(self):
        self.config = get_config()
        init_session_state()
        self.buffer = init_streaming_buffer()
        self._data_loaded = False
        self._df: pd.DataFrame = pd.DataFrame()

    # ── Page Setup ──────────────────────────────────────────────────────
    def setup_page(self) -> None:
        st.set_page_config(
            page_title="CimaPerú - Monitoreo Climático",
            page_icon="🌤️", layout="wide",
            initial_sidebar_state="expanded",
        )
        st.markdown(get_theme_css(), unsafe_allow_html=True)
        t = get_theme()
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
            <h1 style="margin:0;background:linear-gradient(135deg,{t['accent_cyan']},{t['accent_blue']});
                       -webkit-background-clip:text;-webkit-text-fill-color:transparent;">
                🌤️ CimaPerú
            </h1>
            <span style="color:{t['text_secondary']};font-size:0.85rem;padding-top:8px;">
                Monitoreo Climático Inteligente
            </span>
        </div>
        """, unsafe_allow_html=True)

    # ── Data ────────────────────────────────────────────────────────────
    def load_data(self) -> tuple:
        try:
            self._df = load_climate_data(self.config.paths.output)
            self._data_loaded = True
            return self._df, True
        except FileNotFoundError:
            st.error("Datos históricos no encontrados.")
            return pd.DataFrame(), False

    # ── Sidebar (solo global: tema + streaming) ─────────────────────────
    def render_sidebar(self) -> None:
        with st.sidebar:
            st.markdown('<div class="sidebar-header">⚙️ Panel de Control</div>',
                        unsafe_allow_html=True)

            # ── Theme Toggle ──
            current_theme = st.session_state.theme
            new_theme = st.toggle(
                "🌙  Modo Oscuro" if current_theme == "dark" else "☀️  Modo Claro",
                value=(current_theme == "dark"),
                key="theme_toggle",
            )
            desired = "dark" if new_theme else "light"
            if desired != current_theme:
                st.session_state.theme = desired
                st.rerun()

            st.divider()

            # ── Streaming Controls ──
            st.markdown("#### 📡 Streaming")
            paused = st.session_state.streaming_paused
            if st.button(
                "⏸️  Pausar" if not paused else "▶️  Reanudar",
                type="secondary" if not paused else "primary",
                use_container_width=True,
            ):
                st.session_state.streaming_paused = not paused
                st.rerun()

            status_class = "paused" if paused else "live"
            status_text = "PAUSADO" if paused else "EN VIVO"
            dot_color = "#F59E0B" if paused else "#10B981"
            st.markdown(
                f'<div class="stream-status {status_class}">'
                f'<span style="width:8px;height:8px;border-radius:50%;background:{dot_color};display:inline-block;"></span>'
                f'{status_text}</div>',
                unsafe_allow_html=True,
            )

            st.divider()
            st.markdown(f"""
            <div style="color:{get_theme()['text_secondary']};font-size:0.75rem;padding:8px;text-align:center;">
                🌤️ CimaPerú v1.0<br>
                Datos: Supabase · {SUPABASE_TABLE}
            </div>
            """, unsafe_allow_html=True)

    # ── Historical Tab ──────────────────────────────────────────────────
    def render_historical(self, df: pd.DataFrame) -> None:
        st.markdown("## 📊 Análisis Histórico")
        t = get_theme()

        with st.expander("🔍 Filtros Históricos", expanded=True):
            options = get_filter_options(df) if not df.empty else {"departments": [], "variables": ["tmax"]}

            col1, col2, col3 = st.columns(3)
            with col1:
                department = st.selectbox("📍 Departamento", ["Todos"] + options["departments"],
                                          key="hist_dept")
            with col2:
                provinces = []
                if department and department != "Todos":
                    provinces = sorted(
                        df[df["department"] == department]["province"].dropna().unique().tolist()
                    )
                province = st.selectbox("🏛️ Provincia", ["Todas"] + provinces, key="hist_prov")
            with col3:
                stations = []
                if province and province != "Todas":
                    stations = sorted(
                        df[(df["province"] == province)]["station_name"].dropna().unique().tolist()
                    )
                elif department and department != "Todos":
                    stations = sorted(
                        df[df["department"] == department]["station_name"].dropna().unique().tolist()
                    )
                station = st.selectbox("📡 Estación", ["Todas"] + stations, key="hist_station")

            min_date = df["date"].min().date() if not df.empty else datetime(1900, 1, 1).date()
            max_date = df["date"].max().date() if not df.empty else datetime.now().date()

            col_d1, col_d2, col_g, col_v = st.columns([1.5, 1.5, 1, 2])
            with col_d1:
                start_date = st.date_input("📅 Fecha inicio", min_date,
                                           min_value=min_date, max_value=max_date,
                                           key="hist_start")
            with col_d2:
                end_date = st.date_input("📅 Fecha fin", max_date,
                                         min_value=min_date, max_value=max_date,
                                         key="hist_end")
            with col_g:
                granularity = st.selectbox("⏳ Granularidad", ["Diaria", "Mensual", "Anual"],
                                           index=1, key="hist_gran")
            with col_v:
                st.markdown("#### 🌡️ Variables")
                vc1, vc2, vc3 = st.columns(3)
                with vc1:
                    show_precip = st.checkbox("Precipitación", value=True, key="hist_precip")
                with vc2:
                    show_tmax = st.checkbox("Temp. Máxima", value=True, key="hist_tmax")
                with vc3:
                    show_tmin = st.checkbox("Temp. Mínima", value=False, key="hist_tmin")

            variables = []
            if show_precip:
                variables.append("precip")
            if show_tmax:
                variables.append("tmax")
            if show_tmin:
                variables.append("tmin")
            if not variables:
                variables = ["tmax"]

        filters = {
            "department": department, "province": province, "station": station,
            "start_date": start_date, "end_date": end_date,
            "granularity": granularity, "variables": variables,
        }

        filtered_df = filter_data(
            df, department=filters["department"], province=filters["province"],
            station=filters["station"], start_date=filters["start_date"],
            end_date=filters["end_date"],
            variable=filters["variables"][0] if filters["variables"] else "tmax",
        )

        if filtered_df.empty:
            st.warning("No hay datos para los filtros seleccionados.")
            return

        freq = {"Diaria": "D", "Mensual": "ME", "Anual": "YE"}.get(filters["granularity"], "ME")
        t = get_theme()

        tab_h, tab_m, tab_d = st.tabs(["📈 Gráficos", "🗺️ Mapa", "📋 Datos"])

        with tab_h:
            stats_row = st.columns(len(filters["variables"]))
            for i, var in enumerate(filters["variables"]):
                stats = calculate_statistics(filtered_df, var)
                with stats_row[i % len(stats_row)]:
                    st.metric(f"{get_variable_label(var)} — Media",
                              f"{stats.get('media', 0):.1f}",
                              delta=f"Máx: {stats.get('max', 0):.1f}")

            st.divider()

            fig = go.Figure()
            for var in filters["variables"]:
                ts_data = prepare_time_series(filtered_df, var, freq=freq)
                if not ts_data.empty:
                    fig.add_trace(go.Scatter(
                        x=ts_data["date"], y=ts_data[var],
                        mode="lines+markers", name=get_variable_label(var),
                        line=dict(width=2.5),
                    ))

            if fig.data:
                fig.update_layout(
                    title=dict(text="Evolución de Variables Climáticas",
                               font=dict(color=t["text_primary"])),
                    xaxis=dict(title="Fecha", gridcolor=t["border"], color=t["text_secondary"]),
                    yaxis=dict(title="Valor", gridcolor=t["border"], color=t["text_secondary"]),
                    paper_bgcolor=t["bg_primary"], plot_bgcolor=t["bg_primary"],
                    legend=dict(font=dict(color=t["text_secondary"]), orientation="h", y=1.12),
                    hovermode="x unified", height=400,
                    template=t["plotly_template"],
                )
                st.plotly_chart(fig, use_container_width=True)

            st.divider()
            col_a, col_b = st.columns(2)
            with col_a:
                fig_box = go.Figure()
                for var in filters["variables"]:
                    fig_box.add_trace(go.Box(
                        y=filtered_df[var].dropna(), name=get_variable_label(var),
                        marker_color=t["accent_cyan"],
                    ))
                fig_box.update_layout(
                    title="Distribución por Variable", height=350,
                    paper_bgcolor=t["bg_primary"], plot_bgcolor=t["bg_primary"],
                    font=dict(color=t["text_primary"]), template=t["plotly_template"],
                )
                st.plotly_chart(fig_box, use_container_width=True)

        with tab_m:
            stations_md = self._load_stations()
            if not stations_md.empty:
                st.plotly_chart(build_station_map(stations_md), use_container_width=True)
            else:
                st.info("Metadatos de estaciones no disponibles.")

        with tab_d:
            display_cols = ["date", "station_name", "department"]
            for var in filters["variables"]:
                if var in filtered_df.columns:
                    display_cols.append(var)
            st.dataframe(filtered_df[display_cols].head(200), use_container_width=True, height=400)
            csv_df = filtered_df[display_cols].copy()
            if "date" in csv_df.columns:
                csv_df["date"] = csv_df["date"].dt.strftime("%Y-%m-%d")
            st.download_button(
                label="📥 Descargar CSV",
                data=convert_df_to_csv(csv_df),
                file_name=f"clima_{filters['department'] or 'todas'}.csv",
                mime="text/csv",
            )

    def _load_stations(self):
        md_path = getattr(self.config.paths, "metadata", "")
        if not md_path:
            md_path = os.path.join(os.path.dirname(self.config.paths.output),
                                   "stations_metadata.parquet")
        return load_stations_metadata(md_path)

    # ── Real-Time Tab ───────────────────────────────────────────────────
    def _window_seconds(self, label: str) -> int:
        return {"Últimos 5 min": 300, "Últimos 15 min": 900,
                "Última 1 hora": 3600, "Últimas 6 horas": 21600,
                "Todo el día": 86400}.get(label, 3600)

    def _filter_window(self, df: pd.DataFrame, window_sec: int) -> pd.DataFrame:
        if df.empty or "ts" not in df.columns:
            return df
        cutoff = datetime.utcnow() - timedelta(seconds=window_sec)
        return df[df["ts"] >= cutoff].copy()

    def render_realtime(self) -> None:
        t = get_theme()
        paused = st.session_state.streaming_paused

        st.markdown("## ⏱️ Monitoreo en Tiempo Real")

        # ── Filtros ──
        with st.expander("🔍 Filtros Tiempo Real", expanded=False):
            cols = st.columns([1.2, 1.5, 1, 1])
            with cols[0]:
                rt_window = st.selectbox("🕐 Ventana", index=2,
                    options=["Últimos 5 min", "Últimos 15 min",
                             "Última 1 hora", "Últimas 6 horas", "Todo el día"],
                    key="rt_window")
            with cols[1]:
                rt_vars = st.multiselect("🌡️ Variables",
                    options=["Temperatura", "Humedad", "IAQ", "Presión"],
                    default=["Temperatura", "Humedad", "IAQ"], key="rt_vars")
            with cols[2]:
                rt_chart_type = st.selectbox("📈 Tipo gráfico",
                    options=["Líneas", "Área", "Ambos"], key="rt_chart_type")
            with cols[3]:
                st.markdown("#### &nbsp;")
                rt_avg = st.checkbox("📊 Promedio móvil (3 pts)", value=False, key="rt_avg")

        # ── Status bar ──
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:16px;padding:8px 16px;
                    background:{t['bg_card']};border:1px solid {t['border']};
                    border-radius:10px;margin-bottom:12px;">
            <div class="stream-status {"paused" if paused else "live"}">
                <span style="width:10px;height:10px;border-radius:50%;
                    background:{"#F59E0B" if paused else "#10B981"};display:inline-block;"></span>
                {"PAUSADO" if paused else "EN VIVO"}
            </div>
            <span style="color:{t['text_secondary']};font-size:0.85rem;">
                {self.buffer.count}/100 puntos · {st.session_state.get("rt_window","Última 1 hora")}
            </span>
            <span style="color:{t['text_secondary']};font-size:0.85rem;margin-left:auto;">
                {datetime.utcnow().strftime("%H:%M:%S")} UTC
            </span>
        </div>
        """, unsafe_allow_html=True)

        # ── Auto-refresh + ingesta ──
        st_autorefresh(interval=2000, key="rt_stream")
        self._ingest_stream_data()

        # ── Data ──
        buf_df = self.buffer.to_df()
        latest = self.buffer.latest

        if buf_df.empty:
            st.info("⏳ Cargando datos de sensores...")
            return

        window_sec = self._window_seconds(rt_window)
        filtered = self._filter_window(buf_df, window_sec)
        if filtered.empty:
            st.info(f"⏳ Acumulando datos para ventana de {rt_window}...")
            filtered = buf_df.tail(10)

        var_map = {"Temperatura": "temperatura", "Humedad": "humedad",
                   "IAQ": "iaq", "Presión": "presion"}
        active_vars = [var_map[v] for v in rt_vars if v in var_map]
        if not active_vars:
            active_vars = ["temperatura", "humedad"]

        # ── Metrics ──
        self._render_metric_row(latest, buf_df)
        st.divider()

        # ── Gauge + Chart (1:2.5 ratio) ──
        gauge_col, chart_col = st.columns([1, 2.5])

        with gauge_col:
            if latest:
                fig_gauge = build_gauge_chart(
                    latest.get("temperatura", 0),
                    "Temperatura Actual",
                    sub_title=f"IAQ: {latest.get('iaq', 0):.0f} · {latest.get('calidad_aire', '')}"
                )
                st.plotly_chart(fig_gauge, use_container_width=True)

                # Stats compactas
                st.markdown(f"""
                <div style="background:{t['bg_card']};border:1px solid {t['border']};
                            border-radius:10px;padding:12px;text-align:center;">
                    <div style="color:{t['text_secondary']};font-size:0.7rem;text-transform:uppercase;letter-spacing:1px;">
                        Estadísticas de ventana
                    </div>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px;">
                        <div><span style="color:{t['text_secondary']};font-size:0.65rem;">MÍN</span><br>
                             <span style="font-size:1.1rem;font-weight:600;">{filtered['temperatura'].min():.1f}°C</span></div>
                        <div><span style="color:{t['text_secondary']};font-size:0.65rem;">MÁX</span><br>
                             <span style="font-size:1.1rem;font-weight:600;">{filtered['temperatura'].max():.1f}°C</span></div>
                        <div><span style="color:{t['text_secondary']};font-size:0.65rem;">MEDIA</span><br>
                             <span style="font-size:1.1rem;font-weight:600;">{filtered['temperatura'].mean():.1f}°C</span></div>
                        <div><span style="color:{t['text_secondary']};font-size:0.65rem;">DESV.EST</span><br>
                             <span style="font-size:1.1rem;font-weight:600;">{filtered['temperatura'].std():.1f}°C</span></div>
                    </div>
                    <div style="margin-top:8px;color:{t['text_secondary']};font-size:0.7rem;">
                        {len(filtered)} lecturas en ventana
                    </div>
                </div>
                """, unsafe_allow_html=True)

        with chart_col:
            fig_line = self._build_streaming_chart(
                filtered, active_vars,
                chart_type=rt_chart_type,
                show_avg=rt_avg,
                title=f"Evolución — {rt_window}"
            )
            st.plotly_chart(fig_line, use_container_width=True)

        st.divider()

        # ── Streaming Panel ──
        st.markdown(f"### 🎯 Panel de Streaming (Últimos 60s)")
        self._render_streaming_panel(filtered, active_vars)

        # ── Kafka Metrics ──
        st.divider()
        st.markdown("### 📡 Métricas del Stack")
        kafka_metrics = fetch_kafka_metrics()
        render_kafka_metrics_card(kafka_metrics)

        # ── Data Table ──
        st.divider()
        st.markdown("### 📋 Últimas Lecturas")
        self._render_data_table(filtered, active_vars)

    # ── Ingest ──────────────────────────────────────────────────────────
    def _ingest_stream_data(self):
        if st.session_state.streaming_paused:
            return

        # 1. Vaciar cola Realtime (WebSocket) — datos llegan al instante
        rt_queue = _get_realtime_queue()
        new_records = drain_realtime_queue(rt_queue)
        for r in new_records:
            self.buffer.add(r)

        # 2. Carga inicial vía REST (una sola vez)
        if not st.session_state.supabase_initialized:
            readings = fetch_all_supabase_readings(limit=500)
            for r in readings:
                self.buffer.add(r)
            st.session_state.supabase_initialized = True

    # ── Render Helpers ──────────────────────────────────────────────────
    def _render_metric_row(self, latest: Optional[dict], buf_df: pd.DataFrame):
        if not latest:
            return
        temp_class = get_temp_class(latest.get("temperatura", 0))
        aq_class = get_aq_class(latest.get("calidad_aire", ""))

        cols = st.columns(4)
        with cols[0]:
            render_metric_card(
                "Temperatura", f"{latest.get('temperatura', 0):.1f}", "°C",
                temp_class,
                f"Máx: {buf_df['temperatura'].max():.1f}°C · Mín: {buf_df['temperatura'].min():.1f}°C",
            )
        with cols[1]:
            render_metric_card(
                "Humedad", f"{latest.get('humedad', 0):.1f}", "%",
                "moderate",
                f"Prom: {buf_df['humedad'].mean():.1f}%",
            )
        with cols[2]:
            render_metric_card(
                "IAQ", f"{latest.get('iaq', 0):.0f}",
                latest.get("calidad_aire", "").capitalize(),
                aq_class,
                f"eCO₂: {latest.get('eco2', 0):.0f} ppm · VOC: {latest.get('voc', 0):.2f} ppb",
            )
        with cols[3]:
            render_metric_card(
                "Presión", f"{latest.get('presion', 0):.1f}", "hPa",
                "moderate",
                f"Altura: {latest.get('altura', 'N/A')} m" if latest.get('altura') else "",
            )

    def _build_streaming_chart(self, df: pd.DataFrame, active_vars: List[str],
                                chart_type: str = "Líneas", show_avg: bool = False,
                                title: str = "Evolución") -> go.Figure:
        t = get_theme()
        if df.empty:
            return go.Figure()

        var_config = {
            "temperatura": {"color": t["accent_red"], "yaxis": "y", "label": "Temperatura (°C)"},
            "humedad": {"color": t["accent_blue"], "yaxis": "y2", "label": "Humedad (%)"},
            "iaq": {"color": t["accent_green"], "yaxis": "y3", "label": "IAQ"},
            "presion": {"color": t["accent_orange"], "yaxis": "y4", "label": "Presión (hPa)"},
        }

        fig = go.Figure()
        fill = "tozeroy" if chart_type in ("Área", "Ambos") else None
        show_line = chart_type in ("Líneas", "Ambos")

        for var in active_vars:
            if var not in df.columns or var not in var_config:
                continue
            cfg = var_config[var]
            if show_line:
                fig.add_trace(go.Scatter(
                    x=df["ts"], y=df[var],
                    mode="lines+markers",
                    name=cfg["label"],
                    line=dict(color=cfg["color"], width=2),
                    marker=dict(size=4, color=cfg["color"]),
                    yaxis=cfg["yaxis"],
                    fill=fill if fill else None,
                ))
            if show_avg and len(df) >= 3:
                avg = df[var].rolling(3, min_periods=1).mean()
                fig.add_trace(go.Scatter(
                    x=df["ts"], y=avg,
                    mode="lines",
                    name=f"{cfg['label']} (media móvil)",
                    line=dict(color=cfg["color"], width=1.5, dash="dot"),
                    yaxis=cfg["yaxis"],
                    opacity=0.6,
                ))

        yaxis_layout = {
            "yaxis": dict(title="Temperatura (°C)", gridcolor=t["border"],
                       color=t["accent_red"], side="left"),
            "yaxis2": dict(title="Humedad (%)", gridcolor=t["border"],
                       color=t["accent_blue"], side="right",
                       overlaying="y", anchor="x"),
            "yaxis3": dict(title="IAQ", gridcolor=t["border"],
                       color=t["accent_green"], side="left",
                       overlaying="y", anchor="free",
                       position=0.05, showgrid=False),
            "yaxis4": dict(title="Presión (hPa)", gridcolor=t["border"],
                       color=t["accent_orange"], side="right",
                       overlaying="y", anchor="free",
                       position=0.95, showgrid=False),
        }

        layout = {
            "title": dict(text=title, font=dict(color=t["text_primary"])),
            "xaxis": dict(title="Tiempo", gridcolor=t["border"],
                          color=t["text_secondary"], tickformat="%H:%M"),
            "paper_bgcolor": t["bg_primary"],
            "plot_bgcolor": t["bg_primary"],
            "legend": dict(font=dict(color=t["text_secondary"]),
                           orientation="h", y=1.12, bgcolor=t["bg_card"]),
            "hovermode": "x unified",
            "height": 380,
            "template": t["plotly_template"],
            "margin": dict(l=10, r=60, t=50, b=30),
        }

        used_yaxes = set()
        for var in active_vars:
            if var in var_config:
                used_yaxes.add(var_config[var]["yaxis"])
        yaxis_to_layout = {"y": "yaxis", "y2": "yaxis2", "y3": "yaxis3", "y4": "yaxis4"}
        for yax in used_yaxes:
            layout_key = yaxis_to_layout.get(yax)
            if layout_key and layout_key in yaxis_layout:
                layout[layout_key] = yaxis_layout[layout_key]

        fig.update_layout(**layout)
        return fig

    def _render_streaming_panel(self, df: pd.DataFrame, active_vars: List[str]):
        t = get_theme()
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=60)
        stream = df[df["ts"] >= cutoff] if "ts" in df.columns and not df.empty else df

        if len(stream) < 3:
            remaining = max(0, 3 - len(stream))
            st.info(f"⏳ Acumulando datos para panel streaming... faltan {remaining} lecturas (mín. 3)")
            return

        fig = go.Figure()

        pair_colors = {"temperatura": t["accent_red"], "humedad": t["accent_blue"],
                       "iaq": t["accent_green"], "presion": t["accent_orange"]}
        pair_symbols = {"temperatura": "circle", "humedad": "square",
                        "iaq": "diamond", "presion": "x"}

        for i, var in enumerate(active_vars[:4]):
            if var not in stream.columns:
                continue
            yaxis = "y" if i == 0 else f"y{i+1}"
            side = "left" if i % 2 == 0 else "right"
            fig.add_trace(go.Scatter(
                x=stream["ts"], y=stream[var],
                mode="lines+markers",
                name=var.replace("_", " ").title(),
                line=dict(color=pair_colors.get(var, t["accent_cyan"]), width=2.5),
                marker=dict(size=6, color=pair_colors.get(var, t["accent_cyan"]),
                            symbol=pair_symbols.get(var, "circle")),
                yaxis=yaxis,
            ))

        duration = max((now - stream["ts"].min()).total_seconds(), 1)
        readings_per_sec = len(stream) / duration

        yaxes = {}
        for i, var in enumerate(active_vars[:4]):
            if var not in stream.columns:
                continue
            yax = f"yaxis{i+1}" if i > 0 else "yaxis"
            yaxes[yax] = dict(
                title=var.replace("_", " ").title(),
                gridcolor=t["border"], color=pair_colors.get(var, t["text_secondary"]),
                side="left" if i % 2 == 0 else "right",
                overlaying="y" if i > 0 else None,
                anchor="x" if i > 0 else None,
            )

        fig.update_layout(
            xaxis=dict(title="Tiempo", gridcolor=t["border"], color=t["text_secondary"],
                       tickformat="%H:%M:%S"),
            paper_bgcolor=t["bg_primary"], plot_bgcolor=t["bg_primary"],
            legend=dict(font=dict(color=t["text_secondary"]), orientation="h", y=1.15),
            hovermode="x unified", height=300,
            template=t["plotly_template"],
            margin=dict(l=10, r=50, t=10, b=30),
            **yaxes,
        )
        st.plotly_chart(fig, use_container_width=True)

        col_l, col_r = st.columns(2)
        with col_l:
            st.metric("Frecuencia de muestreo", f"{readings_per_sec:.2f} lecturas/s")
        with col_r:
            st.metric("Última actualización",
                      stream["ts"].max().strftime("%H:%M:%S"))

    def _render_data_table(self, df: pd.DataFrame, active_vars: List[str]):
        t = get_theme()
        col_map = {"ts": "Fecha/Hora", "temperatura": "Temp (°C)",
                   "humedad": "Humedad (%)", "iaq": "IAQ", "presion": "Presión (hPa)",
                   "calidad_aire": "Calidad"}
        show_cols = ["ts"] + [v for v in active_vars if v in df.columns]
        if "calidad_aire" in df.columns:
            show_cols.append("calidad_aire")

        display = df[show_cols].tail(10).copy()
        if "ts" in display.columns:
            display["ts"] = display["ts"].dt.strftime("%Y-%m-%d %H:%M")
        display.columns = [col_map.get(c, c.title()) for c in display.columns]

        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            column_config={c: st.column_config.NumberColumn(c, format="%.1f")
                          for c in display.columns if c not in ("Fecha/Hora", "Calidad")},
        )

    # ── About ───────────────────────────────────────────────────────────
    def render_about(self) -> None:
        t = get_theme()
        st.divider()
        st.markdown(f"""
        <div style="text-align:center;color:{t['text_secondary']};padding:12px;font-size:0.8rem;">
            <b>CimaPerú</b> · Monitoreo Climático Inteligente ·
            Datos: SENAMHI + Supabase · Streaming: Kafka + Spark ·
            Visualización: Streamlit + Plotly · {len(PERU_COORDS)} departamentos monitoreados
        </div>
        """, unsafe_allow_html=True)

    # ── Main ────────────────────────────────────────────────────────────
    def run(self) -> None:
        self.setup_page()
        df, data_loaded = self.load_data()
        self.render_sidebar()

        tab1, tab2 = st.tabs(["📊 Datos Históricos", "⏱️ Tiempo Real"])

        with tab1:
            if data_loaded and not self._df.empty:
                self.render_historical(self._df)
            else:
                st.warning("Ejecuta el ETL batch para ver datos históricos.")

        with tab2:
            self.render_realtime()

        self.render_about()


def main():
    ClimateDashboard().run()


if __name__ == "__main__":
    main()
