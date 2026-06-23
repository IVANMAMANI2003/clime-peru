const pptxgen = require("pptxgenjs");
const fs = require("node:fs");
const path = require("node:path");

const C = {
  dark:     "0F172A",
  teal:     "0EA5E9",
  tealD:    "0284C7",
  emerald:  "10B981",
  emeraldD:"059669",
  amber:    "F59E0B",
  amberD:   "D97706",
  rose:     "F43F5E",
  violet:   "8B5CF6",
  slate:    "64748B",
  slateL:   "94A3B8",
  white:    "FFFFFF",
  bg:       "F0F9FF",
  bgCard:   "F8FAFC",
};

const W = 10;
const H = 7.5;

async function generate() {
  const pptx = new pptxgen();
  pptx.defineLayout({ name: "WIDE", width: W, height: H });
  pptx.layout = "WIDE";

  // ── Slide 1: PORTADA IMPACTANTE ──
  {
    const s = pptx.addSlide();
    // Gradient background
    s.background = { color: C.dark };
    s.addShape(pptx.ShapeType.rect, {
      x: 0, y: 0, w: W, h: H,
      fill: { type: "solid", color: C.dark },
    });
    // Decorative accent bars
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: 0.12, h: H, fill: { color: C.teal } });
    s.addShape(pptx.ShapeType.rect, { x: 0.12, y: 0, w: 0.06, h: H, fill: { color: C.emerald } });
    // Top right accent
    s.addShape(pptx.ShapeType.rect, { x: 6, y: 0, w: 4, h: 0.08, fill: { color: C.amber } });
    s.addShape(pptx.ShapeType.rect, { x: 8, y: 0, w: 2, h: 0.08, fill: { color: C.rose } });

    // Main title
    s.addText("CliMePerú", {
      x: 0.8, y: 1.2, w: 6, h: 1,
      fontSize: 52, bold: true, color: C.white, fontFace: "Calibri",
    });
    s.addText("Monitoreo Climático Inteligente", {
      x: 0.8, y: 2.1, w: 6, h: 0.5,
      fontSize: 22, color: C.teal, fontFace: "Calibri", italic: false,
    });

    // Value proposition boxes
    const vBoxes = [
      { n: "1M+", l: "Registros históricos", c: C.teal },
      { n: "374K", l: "Lecturas streaming", c: C.emerald },
      { n: "7", l: "Modelos ML entrenados", c: C.amber },
      { n: "15", l: "Servicios Docker", c: C.rose },
    ];
    vBoxes.forEach((b, i) => {
      const bx = 0.8 + i * 2.2;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: 3.0, w: 1.9, h: 1.6,
        fill: { color: "1E293B" },
        line: { color: b.c, width: 2 },
        rectRadius: 0.12,
      });
      s.addText(b.n, {
        x: bx, y: 3.1, w: 1.9, h: 0.7,
        fontSize: 32, bold: true, color: b.c, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(b.l, {
        x: bx, y: 3.8, w: 1.9, h: 0.6,
        fontSize: 11, color: C.slateL, fontFace: "Calibri", align: "center", valign: "top",
      });
    });

    // Bottom tagline
    s.addText("Big Data · Streaming · Machine Learning · Observabilidad", {
      x: 0.8, y: 5.0, w: 8.5, h: 0.4,
      fontSize: 13, color: C.slateL, fontFace: "Calibri",
    });
    s.addText("Apache Kafka + Spark + XGBoost + Streamlit + Prometheus + Grafana", {
      x: 0.8, y: 5.4, w: 8.5, h: 0.4,
      fontSize: 11, color: C.slate, fontFace: "Calibri",
    });
    s.addText("UPeU — Curso de Big Data 2026", {
      x: 0.8, y: 6.2, w: 8.5, h: 0.3,
      fontSize: 11, color: C.slate, fontFace: "Calibri",
    });
  }

  // ── Slide 2: ¿POR QUÉ CliMePerú? ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.bg };

    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.08, fill: { color: C.teal } });
    s.addText("¿Por qué CliMePerú?", {
      x: 0.6, y: 0.3, w: 8.8, h: 0.6,
      fontSize: 30, bold: true, color: C.dark, fontFace: "Calibri",
    });
    s.addText("El clima en el altiplano peruano es extremo y cambiante. Necesitamos datos en tiempo real para tomar decisiones.", {
      x: 0.6, y: 0.9, w: 8.8, h: 0.5,
      fontSize: 14, color: C.slate, fontFace: "Calibri",
    });

    const problems = [
      { i: "🌡️", t: "Monitoreo en Tiempo Real", d: "Temperatura, humedad, presión y calidad del aire cada 5 minutos desde sensores IoT en Puno.", c: C.teal },
      { i: "⚠️", t: "Detección de Anomalías", d: "Spark detecta desviaciones climáticas en segundos usando z-score sobre promedios históricos de 50 años.", c: C.rose },
      { i: "🤖", t: "Predicción con IA", d: "XGBoost predice temperatura con MAE < 0.1°C. Modelos entrenados con datos SENAMHI + streaming.", c: C.amber },
      { i: "📊", t: "Observabilidad Total", d: "Prometheus + Grafana monitorean cada servicio. 10 alertas automáticas para fallos y anomalías.", c: C.emerald },
    ];
    problems.forEach((p, i) => {
      const col = i % 2;
      const row = Math.floor(i / 2);
      const bx = 0.6 + col * 4.6;
      const by = 1.7 + row * 2.3;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: by, w: 4.3, h: 2.0,
        fill: { color: C.white },
        line: { color: p.c, width: 2 },
        rectRadius: 0.12,
        shadow: { type: "outer", blur: 8, offset: 2, color: "CBD5E1", opacity: 0.4 },
      });
      s.addText(`${p.i}  ${p.t}`, {
        x: bx + 0.2, y: by + 0.15, w: 3.9, h: 0.5,
        fontSize: 16, bold: true, color: p.c, fontFace: "Calibri",
      });
      s.addText(p.d, {
        x: bx + 0.2, y: by + 0.7, w: 3.9, h: 1.1,
        fontSize: 12, color: C.dark, fontFace: "Calibri",
      });
    });
  }

  // ── Slide 3: CÓMO FUNCIONA (público general) ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.white };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.08, fill: { color: C.emerald } });
    s.addText("¿Cómo funciona?", {
      x: 0.6, y: 0.3, w: 8.8, h: 0.6,
      fontSize: 30, bold: true, color: C.dark, fontFace: "Calibri",
    });

    const steps = [
      { n: "01", t: "Capturamos", d: "Datos del SENAMHI (1964-2012) + sensores IoT en tiempo real desde Supabase", c: C.teal },
      { n: "02", t: "Procesamos", d: "Kafka transporta, Spark analiza y detecta anomalías al instante", c: C.emerald },
      { n: "03", t: "Aprendemos", d: "XGBoost entrena modelos que predicen temperatura con precisión milimétrica", c: C.amber },
      { n: "04", t: "Visualizamos", d: "Dashboard interactivo + alertas en Grafana para tomar decisiones", c: C.rose },
    ];
    steps.forEach((st, i) => {
      const by = 1.3 + i * 1.4;
      // Number circle
      s.addShape(pptx.ShapeType.ellipse, {
        x: 0.6, y: by + 0.05, w: 0.7, h: 0.7,
        fill: { color: st.c },
      });
      s.addText(st.n, {
        x: 0.6, y: by + 0.05, w: 0.7, h: 0.7,
        fontSize: 18, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle",
      });
      // Connector line
      if (i < 3) {
        s.addShape(pptx.ShapeType.rect, {
          x: 0.92, y: by + 0.75, w: 0.06, h: 0.65,
          fill: { color: st.c },
        });
      }
      // Title + description
      s.addText(st.t, {
        x: 1.6, y: by, w: 3, h: 0.4,
        fontSize: 18, bold: true, color: C.dark, fontFace: "Calibri",
      });
      s.addText(st.d, {
        x: 1.6, y: by + 0.4, w: 7.4, h: 0.4,
        fontSize: 13, color: C.slate, fontFace: "Calibri",
      });
    });
  }

  // ── Slide 4: ARQUITECTURA TÉCNICA ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.dark };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.06, fill: { color: C.teal } });
    s.addText("Arquitectura del Sistema", {
      x: 0.6, y: 0.25, w: 8.8, h: 0.5,
      fontSize: 26, bold: true, color: C.white, fontFace: "Calibri",
    });

    const archLines = [
      { t: "SENAMHI (.txt) ──[Spark ETL]──▶ Parquet Histórico", c: C.teal },
      { t: "Parquet ──[stats avg/stddev]──▶ 3 Spark Streaming", c: C.slateL },
      { t: "", c: C.white },
      { t: "Supabase (3 tablas IoT) ──[Bridges ×3 c/checkpoint]──▶ Kafka (clima-grupo_2/3/4)", c: C.teal },
      { t: "", c: C.white },
      { t: "Kafka ──[Spark Structured Streaming ×3]──▶ 3 Sinks:", c: C.teal },
      { t: "   ├── Kafka (*-anomalias)  solo anomalías", c: C.rose },
      { t: "   ├── PostgreSQL (sensor_data_*)  foreachBatch", c: C.emerald },
      { t: "   └── Parquet Streaming", c: C.amber },
      { t: "", c: C.white },
      { t: "Parquet Histórico + Streaming ──[XGBoost]──▶ Modelos .pkl (7)", c: C.violet },
      { t: "", c: C.white },
      { t: "Modelos + Kafka + Parquet ──[Dashboard Streamlit]──▶ 4 Pestañas", c: C.teal },
      { t: "Kafka ──[Exporters]──▶ Prometheus ──[Grafana]──▶ Dashboards + 10 Alertas", c: C.amber },
      { t: "", c: C.white },
      { t: "Docker Compose ──[15 servicios, red clime-net, volúmenes]──▶ Stack Completo", c: C.slateL },
    ];

    let y = 0.95;
    archLines.forEach(l => {
      if (l.t === "") { y += 0.06; return; }
      s.addText(l.t, {
        x: 0.6, y, w: 8.8, h: 0.28,
        fontSize: 11.5, bold: l.c !== C.slateL && l.c !== C.rose && l.c !== C.emerald && l.c !== C.amber && l.c !== C.violet,
        color: l.c, fontFace: "Consolas", valign: "top",
      });
      y += 0.24;
    });

    s.addText("💡 Usa este texto para generar un diagrama profesional con DALL-E, Midjourney o similar", {
      x: 0.6, y: 6.6, w: 8.8, h: 0.35,
      fontSize: 10, color: C.slate, fontFace: "Calibri", italic: true,
    });
  }

  // ── Slide 5: STACK TECNOLÓGICO ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.bg };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.08, fill: { color: C.violet } });
    s.addText("Stack Tecnológico", {
      x: 0.6, y: 0.3, w: 8.8, h: 0.6,
      fontSize: 30, bold: true, color: C.dark, fontFace: "Calibri",
    });

    const techs = [
      ["Apache Kafka 4.2.0", "KRaft", "Broker de mensajería", C.teal],
      ["Spark Structured Streaming", "4.1.2", "Procesamiento en tiempo real", C.amber],
      ["XGBoost + scikit-learn", "latest", "Modelos de regresión ML", C.rose],
      ["Streamlit + Plotly", "1.58.0", "Dashboard interactivo", C.emerald],
      ["Prometheus + Grafana", "latest", "Métricas + Dashboards", C.violet],
      ["PostgreSQL 15", "psycopg2 + JDBC", "Almacenamiento persistente", C.tealD],
    ];
    techs.forEach((t, i) => {
      const col = i % 3;
      const row = Math.floor(i / 3);
      const bx = 0.4 + col * 3.2;
      const by = 1.3 + row * 2.6;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: by, w: 3, h: 2.2,
        fill: { color: C.white },
        line: { color: t[3], width: 2.5 },
        rectRadius: 0.12,
        shadow: { type: "outer", blur: 6, offset: 2, color: "CBD5E1", opacity: 0.3 },
      });
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: by, w: 3, h: 0.55,
        fill: { color: t[3] },
        rectRadius: 0.0,
      });
      s.addText(t[0], {
        x: bx, y: by + 0.05, w: 3, h: 0.45,
        fontSize: 15, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(t[1], {
        x: bx + 0.15, y: by + 0.7, w: 2.7, h: 0.3,
        fontSize: 11, color: C.slate, fontFace: "Calibri",
      });
      s.addText(t[2], {
        x: bx + 0.15, y: by + 1.1, w: 2.7, h: 0.8,
        fontSize: 12, color: C.dark, fontFace: "Calibri",
      });
    });
  }

  // ── Slide 6: ML — VISIÓN GENERAL ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.dark };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.06, fill: { color: C.amber } });
    s.addText("Machine Learning", {
      x: 0.6, y: 0.25, w: 8.8, h: 0.5,
      fontSize: 28, bold: true, color: C.white, fontFace: "Calibri",
    });
    s.addText("Dos horizontes de predicción con XGBoost", {
      x: 0.6, y: 0.75, w: 8.8, h: 0.35,
      fontSize: 15, color: C.slateL, fontFace: "Calibri",
    });

    // Left: Largo Plazo
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.4, y: 1.4, w: 4.5, h: 5.3,
      fill: { color: "1E293B" },
      line: { color: C.teal, width: 2 },
      rectRadius: 0.15,
    });
    s.addText("📅 Largo Plazo", {
      x: 0.7, y: 1.55, w: 4, h: 0.5,
      fontSize: 20, bold: true, color: C.teal, fontFace: "Calibri",
    });
    const lpItems = [
      ["🎯 Target", "tmax (temperatura máxima diaria)"],
      ["📊 Datos", "SENAMHI 1964-2012, 4 estaciones"],
      ["🔧 Features", "Lags (1,3,5,7d), rolling means (3-30d), std, mes/día cíclico, estación"],
      ["🤖 Modelo", "XGBoost, 500 árboles, lr=0.05, depth=7"],
      ["📈 Resultado", "R² > 0.99 · MAE < 0.1°C"],
    ];
    lpItems.forEach((item, i) => {
      const iy = 2.2 + i * 0.75;
      s.addText(item[0], {
        x: 0.7, y: iy, w: 4, h: 0.25,
        fontSize: 10, bold: true, color: C.teal, fontFace: "Calibri",
      });
      s.addText(item[1], {
        x: 0.7, y: iy + 0.25, w: 4, h: 0.35,
        fontSize: 12, color: C.white, fontFace: "Calibri",
      });
    });

    // Right: Corto Plazo
    s.addShape(pptx.ShapeType.roundRect, {
      x: 5.1, y: 1.4, w: 4.5, h: 5.3,
      fill: { color: "1E293B" },
      line: { color: C.amber, width: 2 },
      rectRadius: 0.15,
    });
    s.addText("⚡ Corto Plazo", {
      x: 5.4, y: 1.55, w: 4, h: 0.5,
      fontSize: 20, bold: true, color: C.amber, fontFace: "Calibri",
    });
    const cpItems = [
      ["🎯 Target", "Temperatura instantánea"],
      ["📊 Datos", "Streaming IoT 2026, 3 grupos (~374K regs)"],
      ["🔧 Features", "Lags (1,3), rolling mean (5), ratio presión/temp, hora cíclica"],
      ["🤖 Modelo", "XGBoost, 300 árboles, lr=0.05, depth=5"],
      ["📈 Resultado", "R² > 0.99 · MAE < 0.05°C (sensores OK)"],
    ];
    cpItems.forEach((item, i) => {
      const iy = 2.2 + i * 0.75;
      s.addText(item[0], {
        x: 5.4, y: iy, w: 4, h: 0.25,
        fontSize: 10, bold: true, color: C.amber, fontFace: "Calibri",
      });
      s.addText(item[1], {
        x: 5.4, y: iy + 0.25, w: 4, h: 0.35,
        fontSize: 12, color: C.white, fontFace: "Calibri",
      });
    });
  }

  // ── Slide 7: FEATURE ENGINEERING ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.bg };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.08, fill: { color: C.emerald } });
    s.addText("Feature Engineering", {
      x: 0.6, y: 0.3, w: 8.8, h: 0.6,
      fontSize: 28, bold: true, color: C.dark, fontFace: "Calibri",
    });
    s.addText("Convertimos tiempo en predictores numéricos. XGBoost no entiende fechas, entiende números.", {
      x: 0.6, y: 0.85, w: 8.8, h: 0.35,
      fontSize: 13, color: C.slate, fontFace: "Calibri",
    });

    // Largo plazo table
    s.addText("Largo Plazo — 16 features por día:", {
      x: 0.6, y: 1.4, w: 4.2, h: 0.35,
      fontSize: 14, bold: true, color: C.teal, fontFace: "Calibri",
    });
    const lpH = { fontSize: 10, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle" };
    const lpC = { fontSize: 10, color: C.dark, fontFace: "Calibri", valign: "middle" };
    const lpRows = [
      [{ text: "Feature", options: { ...lpH, align: "left" } }, { text: "Ventana", options: lpH }, { text: "Qué captura", options: lpH }],
      [{ text: "tmax_lag_1", options: { ...lpC, bold: true } }, { text: "1 día", options: lpC }, { text: "Valor de ayer", options: lpC }],
      [{ text: "tmax_lag_7", options: { ...lpC, bold: true } }, { text: "7 días", options: lpC }, { text: "Mismo día semana pasada", options: lpC }],
      [{ text: "rolling_mean_7", options: { ...lpC, bold: true } }, { text: "7 días", options: lpC }, { text: "Promedio semanal", options: lpC }],
      [{ text: "rolling_mean_30", options: { ...lpC, bold: true } }, { text: "30 días", options: lpC }, { text: "Tendencia mensual", options: lpC }],
      [{ text: "month_sin/cos", options: { ...lpC, bold: true } }, { text: "12 meses", options: lpC }, { text: "Mes como círculo", options: lpC }],
    ];
    s.addTable(lpRows, {
      x: 0.6, y: 1.8, w: 4.2, colW: [1.8, 1.0, 1.4],
      rowH: [0.3, 0.25, 0.25, 0.25, 0.25, 0.25],
      border: { type: "solid", color: C.teal, pt: 0.5 },
      autoPage: false,
    });

    // Corto plazo table
    s.addText("Corto Plazo — 7 features por medición:", {
      x: 5.2, y: 1.4, w: 4.2, h: 0.35,
      fontSize: 14, bold: true, color: C.amber, fontFace: "Calibri",
    });
    const cpRows = [
      [{ text: "Feature", options: { ...lpH, align: "left" } }, { text: "Rango", options: lpH }, { text: "Qué captura", options: lpH }],
      [{ text: "temp_lag_1", options: { ...lpC, bold: true } }, { text: "1 paso", options: lpC }, { text: "Última medición", options: lpC }],
      [{ text: "temp_rolling_mean_5", options: { ...lpC, bold: true } }, { text: "5 pasos", options: lpC }, { text: "Tendencia inmediata", options: lpC }],
      [{ text: "presion_temp_ratio", options: { ...lpC, bold: true } }, { text: "1 paso", options: lpC }, { text: "Condición atmosférica", options: lpC }],
      [{ text: "hour_sin/cos", options: { ...lpC, bold: true } }, { text: "24h", options: lpC }, { text: "Hora del día en círculo", options: lpC }],
    ];
    s.addTable(cpRows, {
      x: 5.2, y: 1.8, w: 4.2, colW: [1.8, 1.0, 1.4],
      rowH: [0.3, 0.25, 0.25, 0.25, 0.25],
      border: { type: "solid", color: C.amber, pt: 0.5 },
      autoPage: false,
    });

    // Key insight
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 3.9, w: 8.8, h: 0.8,
      fill: { color: C.dark },
      rectRadius: 0.1,
    });
    s.addText("💡 Para evitar el salto dic-ene, usamos seno+coseno: mes 12 (dic) y mes 1 (ene) quedan vecinos en el círculo. Lo mismo con hora 23 y 0.", {
      x: 0.8, y: 4.0, w: 8.4, h: 0.6,
      fontSize: 12, color: C.white, fontFace: "Calibri", valign: "middle",
    });

    // Why XGBoost mini section
    s.addText("¿Por qué XGBoost en vez de ARIMA, Prophet o LSTM?", {
      x: 0.6, y: 5.0, w: 8.8, h: 0.35,
      fontSize: 14, bold: true, color: C.dark, fontFace: "Calibri",
    });
    const reasons = [
      ["XGBoost", "Acepta features externas (presión, hora, lags) — ARIMA no"],
      ["XGBoost", "Captura no linealidades con árboles — Prophet es lineal"],
      ["XGBoost", "Entrena en CPU en segundos — LSTM requiere GPU"],
      ["XGBoost", "Misma lógica para largo y corto plazo — simple y robusto"],
    ];
    reasons.forEach((r, i) => {
      const ry = 5.4 + i * 0.3;
      s.addText("✅", { x: 0.6, y: ry, w: 0.3, h: 0.25, fontSize: 10, fontFace: "Calibri" });
      s.addText(`${r[0]} — ${r[1]}`, {
        x: 1.0, y: ry, w: 8.2, h: 0.25,
        fontSize: 11, color: C.dark, fontFace: "Calibri", valign: "top",
      });
    });
  }

  // ── Slide 8: RESULTADOS ML ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.dark };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.06, fill: { color: C.emerald } });
    s.addText("Resultados — Modelos ML", {
      x: 0.6, y: 0.25, w: 8.8, h: 0.5,
      fontSize: 28, bold: true, color: C.white, fontFace: "Calibri",
    });

    // Largo Plazo table
    s.addText("📅 Largo Plazo (tmax diario)", {
      x: 0.6, y: 0.9, w: 8.8, h: 0.35,
      fontSize: 16, bold: true, color: C.teal, fontFace: "Calibri",
    });
    const hdr = { fontSize: 11, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle" };
    const cel = { fontSize: 13, color: C.dark, fontFace: "Calibri", align: "center", valign: "middle" };
    const celB = { ...cel, bold: true };
    const lpTable = [
      [
        { text: "Estación", options: { ...hdr, align: "left" } },
        { text: "MAE", options: hdr },
        { text: "RMSE", options: hdr },
        { text: "R²", options: hdr },
        { text: "Precisión", options: hdr },
      ],
      [
        { text: "PUNO", options: { ...celB, align: "left", color: C.teal } },
        { text: "0.08°C", options: cel },
        { text: "0.20°C", options: cel },
        { text: "0.994", options: { ...celB, color: C.emerald } },
        { text: "99.4%", options: cel },
      ],
      [
        { text: "AZANGARO", options: { ...celB, align: "left", color: C.teal } },
        { text: "0.09°C", options: cel },
        { text: "0.20°C", options: cel },
        { text: "0.995", options: { ...celB, color: C.emerald } },
        { text: "99.5%", options: cel },
      ],
      [
        { text: "LAMPA", options: { ...celB, align: "left", color: C.teal } },
        { text: "0.07°C", options: cel },
        { text: "0.16°C", options: cel },
        { text: "0.997", options: { ...celB, color: C.emerald } },
        { text: "99.7%", options: cel },
      ],
      [
        { text: "CAPACHICA", options: { ...celB, align: "left", color: C.teal } },
        { text: "0.08°C", options: cel },
        { text: "0.18°C", options: cel },
        { text: "0.994", options: { ...celB, color: C.emerald } },
        { text: "99.4%", options: cel },
      ],
    ];
    s.addTable(lpTable, {
      x: 0.6, y: 1.3, w: 8.8, colW: [2, 1.7, 1.7, 1.7, 1.7],
      rowH: [0.38, 0.38, 0.38, 0.38, 0.38],
      border: { type: "solid", color: C.teal, pt: 1 },
      autoPage: false,
    });

    // Corto Plazo table
    s.addText("⚡ Corto Plazo (temperatura streaming)", {
      x: 0.6, y: 3.5, w: 8.8, h: 0.35,
      fontSize: 16, bold: true, color: C.amber, fontFace: "Calibri",
    });
    const cpTable = [
      [
        { text: "Grupo / Estación", options: { ...hdr, align: "left" } },
        { text: "MAE", options: hdr },
        { text: "R²", options: hdr },
        { text: "Estado", options: hdr },
      ],
      [
        { text: "grupo_2 / LAMPA", options: { ...celB, align: "left", color: C.amber } },
        { text: "0.044°C", options: { ...cel, color: C.emerald } },
        { text: "0.992", options: { ...celB, color: C.emerald } },
        { text: "✅ OK", options: { ...cel, color: C.emerald } },
      ],
      [
        { text: "grupo_3 / PUNO", options: { ...celB, align: "left", color: C.amber } },
        { text: "6.355°C", options: { ...cel, color: C.rose } },
        { text: "-1.41", options: { ...celB, color: C.rose } },
        { text: "❌ Sensor defectuoso", options: { ...cel, color: C.rose } },
      ],
      [
        { text: "grupo_4 / AZANGARO", options: { ...celB, align: "left", color: C.amber } },
        { text: "0.053°C", options: { ...cel, color: C.emerald } },
        { text: "0.993", options: { ...celB, color: C.emerald } },
        { text: "✅ OK", options: { ...cel, color: C.emerald } },
      ],
    ];
    s.addTable(cpTable, {
      x: 0.6, y: 3.9, w: 8.8, colW: [3, 2.2, 1.8, 1.8],
      rowH: [0.38, 0.38, 0.38, 0.38],
      border: { type: "solid", color: C.amber, pt: 1 },
      autoPage: false,
    });

    // Note
    s.addText("R² > 0.99 en todos los modelos con datos de calidad. El sensor grupo_3 (PUNO) tiene presión anómala (186 hPa vs 646 esperado) y temperatura constante -0.06°C.", {
      x: 0.6, y: 5.9, w: 8.8, h: 0.5,
      fontSize: 11, color: C.slateL, fontFace: "Calibri", italic: true,
    });
  }

  // ── Slide 9: DASHBOARD ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.bg };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.08, fill: { color: C.rose } });
    s.addText("Dashboard Streamlit", {
      x: 0.6, y: 0.3, w: 8.8, h: 0.6,
      fontSize: 28, bold: true, color: C.dark, fontFace: "Calibri",
    });
    s.addText("4 pestañas para monitorear, analizar y predecir el clima en tiempo real", {
      x: 0.6, y: 0.85, w: 8.8, h: 0.35,
      fontSize: 13, color: C.slate, fontFace: "Calibri",
    });

    const tabs = [
      { i: "📊", t: "Datos Históricos", d: "Explora 60 estaciones SENAMHI. Filtros por departamento, provincia, fechas. Gráficos de líneas, box plots y mapa interactivo.", c: C.teal },
      { i: "⏱️", t: "Tiempo Real", d: "Streaming desde Kafka cada 2s. Temperatura, humedad, IAQ, presión. Gauges, evolución multi-variable y panel de 60s.", c: C.emerald },
      { i: "🤖", t: "Predicciones ML", d: "Selecciona estación y horizonte. XGBoost predice temperatura. Tarjetas con MAE, RMSE, R². Gráfica histórico + predicción.", c: C.amber },
      { i: "📡", t: "Métricas del Stack", d: "Offsets Kafka, lag, brokers activos, estado de exporters vía Prometheus API. Salud del pipeline en tiempo real.", c: C.violet },
    ];

    tabs.forEach((tab, i) => {
      const by = 1.4 + i * 1.35;
      s.addShape(pptx.ShapeType.roundRect, {
        x: 0.6, y: by, w: 8.8, h: 1.15,
        fill: { color: C.white },
        line: { color: tab.c, width: 2 },
        rectRadius: 0.1,
        shadow: { type: "outer", blur: 6, offset: 2, color: "CBD5E1", opacity: 0.3 },
      });
      s.addText(tab.i, {
        x: 0.8, y: by + 0.1, w: 0.6, h: 0.5,
        fontSize: 28, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(tab.t, {
        x: 1.5, y: by + 0.1, w: 7.5, h: 0.4,
        fontSize: 17, bold: true, color: tab.c, fontFace: "Calibri", valign: "middle",
      });
      s.addText(tab.d, {
        x: 1.5, y: by + 0.5, w: 7.5, h: 0.55,
        fontSize: 12, color: C.dark, fontFace: "Calibri", valign: "top",
      });
    });
  }

  // ── Slide 10: OBSERVABILIDAD ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.dark };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.06, fill: { color: C.rose } });
    s.addText("Observabilidad Total", {
      x: 0.6, y: 0.25, w: 8.8, h: 0.5,
      fontSize: 28, bold: true, color: C.white, fontFace: "Calibri",
    });

    const obsItems = [
      { n: "2", l: "Dashboards Grafana", d: "Kafka Overview + Sensores Variables", c: C.teal },
      { n: "10", l: "Alertas Automáticas", d: "4 infraestructura + 6 sensores", c: C.rose },
      { n: "15s", l: "Scraping Prometheus", d: "Métricas cada 15 segundos", c: C.emerald },
      { n: "100+", l: "Métricas Exposed", d: "Offset, lag, temp, humedad, IAQ, etc.", c: C.amber },
    ];
    obsItems.forEach((item, i) => {
      const bx = 0.4 + i * 2.4;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: 1.0, w: 2.2, h: 1.8,
        fill: { color: "1E293B" },
        line: { color: item.c, width: 2 },
        rectRadius: 0.12,
      });
      s.addText(item.n, {
        x: bx, y: 1.1, w: 2.2, h: 0.6,
        fontSize: 36, bold: true, color: item.c, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(item.l, {
        x: bx + 0.1, y: 1.7, w: 2, h: 0.3,
        fontSize: 11, bold: true, color: C.white, fontFace: "Calibri", align: "center",
      });
      s.addText(item.d, {
        x: bx + 0.1, y: 2.0, w: 2, h: 0.5,
        fontSize: 10, color: C.slateL, fontFace: "Calibri", align: "center",
      });
    });

    // Alerts table
    s.addText("Reglas de Alerta", {
      x: 0.6, y: 3.1, w: 8.8, h: 0.35,
      fontSize: 16, bold: true, color: C.white, fontFace: "Calibri",
    });
    const ah = { fontSize: 10, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle" };
    const ac = { fontSize: 10.5, color: C.white, fontFace: "Calibri", valign: "middle" };
    const alertRows = [
      [
        { text: "Alerta", options: { ...ah, align: "left" } },
        { text: "Condición", options: ah },
        { text: "Severidad", options: ah },
      ],
      [
        { text: "KafkaExporterDown", options: { ...ac, bold: true, color: C.rose } },
        { text: "up{job='kafka-exporter'} == 0", options: ac },
        { text: "🔴 Critical", options: { ...ac, color: C.rose } },
      ],
      [
        { text: "HighKafkaLag", options: { ...ac, bold: true, color: C.amber } },
        { text: "kafka_consumergroup_lag > 100", options: ac },
        { text: "🟡 Warning", options: { ...ac, color: C.amber } },
      ],
      [
        { text: "TemperatureAnomaly", options: { ...ac, bold: true, color: C.teal } },
        { text: "sensor_anomalia_count > 0", options: ac },
        { text: "🟠 Info", options: { ...ac, color: C.teal } },
      ],
      [
        { text: "BridgeDown", options: { ...ac, bold: true, color: C.rose } },
        { text: "sensor_temperatura_celsius absent 5m", options: ac },
        { text: "🔴 Critical", options: { ...ac, color: C.rose } },
      ],
    ];
    s.addTable(alertRows, {
      x: 0.6, y: 3.5, w: 8.8, colW: [3, 4.3, 1.5],
      rowH: [0.3, 0.3, 0.3, 0.3, 0.3],
      border: { type: "solid", color: C.slate, pt: 0.5 },
      autoPage: false,
    });

    // Pipeline diagram
    s.addText("Pipeline de Métricas:", {
      x: 0.6, y: 5.3, w: 3, h: 0.3,
      fontSize: 12, bold: true, color: C.white, fontFace: "Calibri",
    });
    s.addText("Kafka ──[Exporters]──▶ Prometheus ──[scrape 15s]──▶ Grafana ──[Dashboards]──▶ Usuario", {
      x: 0.6, y: 5.7, w: 8.8, h: 0.3,
      fontSize: 12, color: C.teal, fontFace: "Consolas",
    });
  }

  // ── Slide 11: LIMITACIONES Y PRÓXIMOS PASOS ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.bg };
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.08, fill: { color: C.amber } });
    s.addText("Próximos Pasos", {
      x: 0.6, y: 0.3, w: 8.8, h: 0.6,
      fontSize: 28, bold: true, color: C.dark, fontFace: "Calibri",
    });

    // Current
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.4, y: 1.2, w: 4.5, h: 5.2,
      fill: { color: C.white },
      line: { color: C.rose, width: 2 },
      rectRadius: 0.12,
    });
    s.addText("📌 Hoy", {
      x: 0.7, y: 1.35, w: 4, h: 0.4,
      fontSize: 18, bold: true, color: C.rose, fontFace: "Calibri",
    });
    const currentItems = [
      "⚠️ 4 estaciones SENAMHI con datos suficientes",
      "⚠️ Sensor grupo_3 defectuoso (PUNO)",
      "⚠️ Predicción larga arranca desde ~2012",
      "⚠️ Sin re-entrenamiento automático",
      "⚠️ Solo XGBoost (sin ensemble)",
      "⚠️ Sin features climáticos externos",
      "⚠️ Sin alertas por email/WhatsApp",
      "✓ 15 servicios funcionando 24/7",
      "✓ R² > 0.99 en 6 de 7 modelos",
      "✓ Dashboard con 4 pestañas operativas",
    ];
    currentItems.forEach((item, i) => {
      const ci = item.startsWith("✓") ? C.emerald : C.rose;
      s.addText(item, {
        x: 0.7, y: 1.9 + i * 0.42, w: 4, h: 0.35,
        fontSize: 11, color: ci, fontFace: "Calibri", valign: "top",
      });
    });

    // Next
    s.addShape(pptx.ShapeType.roundRect, {
      x: 5.1, y: 1.2, w: 4.5, h: 5.2,
      fill: { color: C.dark },
      line: { color: C.emerald, width: 2 },
      rectRadius: 0.12,
    });
    s.addText("🚀 Siguiente", {
      x: 5.4, y: 1.35, w: 4, h: 0.4,
      fontSize: 18, bold: true, color: C.emerald, fontFace: "Calibri",
    });
    const nextItems = [
      "➕ Incorporar más estaciones SENAMHI",
      "🔧 Reemplazar sensor grupo_3",
      "🔄 Usar sensor IoT como punto de partida 2026",
      "⏰ Re-entrenamiento automático (cron)",
      "🧠 Ensemble: XGBoost + RF + LSTM",
      "🌤️ Features externos: viento, radiación",
      "📲 Alertas por email/WhatsApp/Slack",
      "📈 Spark MLlib para escalar",
      "📉 Intervalos de confianza en predicciones",
      "🔁 CI/CD para deploy de modelos",
    ];
    nextItems.forEach((item, i) => {
      s.addText(item, {
        x: 5.4, y: 1.9 + i * 0.42, w: 4, h: 0.35,
        fontSize: 11, color: C.white, fontFace: "Calibri", valign: "top",
      });
    });
  }

  // ── Slide 12: CIERRE ──
  {
    const s = pptx.addSlide();
    s.background = { color: C.dark };
    // Diagonal accent
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.06, fill: { color: C.teal } });
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0.06, w: W, h: 0.04, fill: { color: C.emerald } });
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0.1, w: W, h: 0.02, fill: { color: C.amber } });

    s.addText("CliMePerú", {
      x: 0, y: 1.8, w: W, h: 1,
      fontSize: 48, bold: true, color: C.white, fontFace: "Calibri", align: "center",
    });
    s.addText("Monitoreo Climático Inteligente para el Altiplano Peruano", {
      x: 0, y: 2.8, w: W, h: 0.5,
      fontSize: 18, color: C.teal, fontFace: "Calibri", align: "center",
    });
    s.addText("Big Data · Streaming · Machine Learning · Observabilidad", {
      x: 0, y: 3.4, w: W, h: 0.4,
      fontSize: 13, color: C.slateL, fontFace: "Calibri", align: "center",
    });

    s.addShape(pptx.ShapeType.roundRect, {
      x: 2, y: 4.2, w: 6, h: 1.2,
      fill: { color: "1E293B" },
      line: { color: C.teal, width: 1.5 },
      rectRadius: 0.1,
    });
    s.addText("https://github.com/IVANMAMANI2003/clime-peru", {
      x: 2, y: 4.3, w: 6, h: 0.5,
      fontSize: 14, color: C.teal, fontFace: "Calibri", align: "center", valign: "middle",
    });
    s.addText("UPeU — Curso de Big Data 2026", {
      x: 2, y: 4.8, w: 6, h: 0.4,
      fontSize: 12, color: C.slate, fontFace: "Calibri", align: "center", valign: "middle",
    });

    s.addText("¿Preguntas?", {
      x: 0, y: 5.8, w: W, h: 0.6,
      fontSize: 24, bold: true, color: C.amber, fontFace: "Calibri", align: "center",
    });
  }

  // ── Save ──
  const outputPath = path.join(__dirname, "CliMePeru_Presentacion_v2.pptx");
  await pptx.writeFile({ fileName: outputPath });
  console.log(`✓ Presentación generada: ${outputPath}`);
  console.log(`  Tamaño: ${(fs.statSync(outputPath).size / 1024).toFixed(1)} KB`);
}

generate().catch(err => {
  console.error("Error:", err.message);
  console.error(err.stack);
  process.exit(1);
});
