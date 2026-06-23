const pptxgen = require("pptxgenjs");
const fs = require("node:fs");
const path = require("node:path");

const C = {
  navy:     "0B1121",
  navyL:    "1A2744",
  teal:     "06B6D4",
  tealD:    "0891B2",
  emerald:  "34D399",
  emeraldD:"059669",
  amber:    "FBBF24",
  amberD:   "D97706",
  rose:     "FB7185",
  violet:   "A78BFA",
  slate:    "64748B",
  slateL:   "94A3B8",
  white:    "FFFFFF",
  offWhite: "F1F5F9",
  gradient1:"0F172A",
  gradient2:"1E3A5F",
};

const W = 10;
const H = 7.5;

async function generate() {
  const pptx = new pptxgen();
  pptx.defineLayout({ name: "WIDE", width: W, height: H });
  pptx.layout = "WIDE";

  function headerBar(slide) {
    slide.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.04, fill: { color: C.teal } });
    slide.addShape(pptx.ShapeType.rect, { x: W-2.5, y: 0, w: 2.5, h: 0.04, fill: { color: C.emerald } });
  }

  function sectionSlide(slide, num, title, subtitle) {
    slide.background = { color: C.navy };
    headerBar(slide);
    slide.addText(num, {
      x: 0.6, y: 0.4, w: 1, h: 0.5,
      fontSize: 14, bold: true, color: C.teal, fontFace: "Calibri",
    });
    slide.addText(title, {
      x: 1.3, y: 0.4, w: 8, h: 0.5,
      fontSize: 24, bold: true, color: C.white, fontFace: "Calibri",
    });
    if (subtitle) {
      slide.addText(subtitle, {
        x: 0.6, y: 0.95, w: 8.8, h: 0.35,
        fontSize: 13, color: C.slateL, fontFace: "Calibri",
      });
    }
  }

  function kpiCard(slide, x, y, w, h, number, label, accent) {
    slide.addShape(pptx.ShapeType.roundRect, {
      x, y, w, h,
      fill: { color: "0F1B33" },
      line: { color: accent, width: 1.5 },
      rectRadius: 0.1,
      shadow: { type: "outer", blur: 10, offset: 3, color: "000000", opacity: 0.3 },
    });
    slide.addText(number, {
      x, y: y + h*0.08, w, h: h*0.52,
      fontSize: 30, bold: true, color: accent, fontFace: "Calibri", align: "center", valign: "middle",
    });
    slide.addText(label, {
      x, y: y + h*0.58, w, h: h*0.35,
      fontSize: 10, color: C.slateL, fontFace: "Calibri", align: "center", valign: "top",
    });
  }

  function bulletLine(slide, x, y, w, h, icon, text, accent) {
    slide.addShape(pptx.ShapeType.ellipse, {
      x, y: y+0.04, w: 0.28, h: 0.28,
      fill: { color: accent },
    });
    slide.addText(icon, {
      x, y: y+0.04, w: 0.28, h: 0.28,
      fontSize: 10, fontFace: "Calibri", align: "center", valign: "middle",
    });
    slide.addText(text, {
      x: x+0.4, y, w, h,
      fontSize: 11, color: C.offWhite, fontFace: "Calibri", valign: "middle",
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 1 — PORTADA DE VENTAS (Espectacular)
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    // Deep gradient bg
    s.background = { color: C.navy };
    s.addShape(pptx.ShapeType.rect, {
      x: 0, y: 0, w: W, h: H,
      fill: { type: "solid", color: C.navy },
    });
    // Top tech accent bar
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W*0.6, h: 0.05, fill: { color: C.teal } });
    s.addShape(pptx.ShapeType.rect, { x: W*0.6, y: 0, w: W*0.4, h: 0.05, fill: { color: C.emerald } });
    // Left vertical accent
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: 0.07, h: H, fill: { color: C.teal } });
    s.addShape(pptx.ShapeType.rect, { x: 0.07, y: 0, w: 0.04, h: H, fill: { color: C.emerald, transparency: 50 } });

    // Top-right micro-badge: live indicator
    s.addShape(pptx.ShapeType.roundRect, {
      x: 7.2, y: 0.3, w: 2.5, h: 0.5,
      fill: { color: "0F1B33" },
      line: { color: C.emerald, width: 1.5 },
      rectRadius: 0.25,
    });
    s.addText("🔴  3 ESTACIONES · EN VIVO", {
      x: 7.2, y: 0.3, w: 2.5, h: 0.5,
      fontSize: 10, bold: true, color: C.emerald, fontFace: "Calibri", align: "center", valign: "middle",
    });

    // Main headline
    s.addText("CliMePerú", {
      x: 0.7, y: 0.9, w: 7, h: 1.0,
      fontSize: 52, bold: true, color: C.white, fontFace: "Calibri",
    });
    s.addText("El clima del altiplano, anticipado.", {
      x: 0.7, y: 1.85, w: 7, h: 0.5,
      fontSize: 22, color: C.teal, fontFace: "Calibri", italic: false,
    });

    // Value paragraph
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.7, y: 2.6, w: 5.8, h: 1.3,
      fill: { color: "0F1B33" },
      line: { color: C.navyL, width: 1 },
      rectRadius: 0.1,
    });
    s.addText([
      { text: "Una poderosa infraestructura Big Data ", options: { bold: true, color: C.white } },
      { text: "que conecta sensores IoT en campos de cultivo con un cerebro de ", options: { color: C.slateL } },
      { text: "Inteligencia Artificial (XGBoost)", options: { bold: true, color: C.amber } },
      { text: " entrenado con ", options: { color: C.slateL } },
      { text: "48 años de datos SENAMHI", options: { bold: true, color: C.white } },
      { text: ". Predice heladas y variaciones térmicas extremas a ", options: { color: C.slateL } },
      { text: "14 días con 99.4% de precisión.", options: { bold: true, color: C.emerald } },
      { text: " Datos en tiempo real cada 2 segundos.", options: { color: C.slateL } },
    ], {
      x: 0.85, y: 2.7, w: 5.5, h: 1.1,
      fontSize: 12, fontFace: "Calibri", valign: "middle",
    });

    // Right side: Big KPI card
    s.addShape(pptx.ShapeType.roundRect, {
      x: 6.8, y: 2.6, w: 2.8, h: 2.0,
      fill: { color: "0F1B33" },
      line: { color: C.emerald, width: 2.5 },
      rectRadius: 0.15,
      shadow: { type: "outer", blur: 15, offset: 4, color: C.emerald, opacity: 0.15 },
    });
    s.addText("99.4%", {
      x: 6.8, y: 2.7, w: 2.8, h: 0.8,
      fontSize: 42, bold: true, color: C.emerald, fontFace: "Calibri", align: "center", valign: "middle",
    });
    s.addText("R²  Precisión", {
      x: 6.8, y: 3.4, w: 2.8, h: 0.4,
      fontSize: 14, bold: true, color: C.white, fontFace: "Calibri", align: "center",
    });
    s.addText("Modelo XGBoost · MAE 0.08°C", {
      x: 6.8, y: 3.8, w: 2.8, h: 0.3,
      fontSize: 10, color: C.slateL, fontFace: "Calibri", align: "center",
    });
    s.addText("Helada detectada ⏱ 48 hrs", {
      x: 6.8, y: 4.1, w: 2.8, h: 0.3,
      fontSize: 10, bold: true, color: C.amber, fontFace: "Calibri", align: "center",
    });

    // 4-Step flow
    const steps = [
      { n: "01", t: "Captura IoT", d: "Datos en tiempo real (T°, Humedad, Presión, IAQ) transmitidos cada 2 segundos desde sensores físicos en campo.", c: C.teal },
      { n: "02", t: "IA Predictiva", d: "XGBoost optimizado para el altiplano. Predice heladas con margen de error de solo 0.08°C. Entrenado con 48 años de histórico.", c: C.amber },
      { n: "03", t: "Dashboard Smart", d: "Visualización interactiva adaptada a productores. Streaming Kafka en vivo + predicciones ML + alertas en tiempo real.", c: C.emerald },
      { n: "04", t: "Alerta Temprana", d: "Sistema de notificaciones ante heladas inminentes. Detecta patrones de riesgo con hasta 48 horas de anticipación.", c: C.rose },
    ];
    steps.forEach((st, i) => {
      const bx = 0.5 + i * 2.35;
      const by = 4.3;
      // Card bg
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: by, w: 2.2, h: 2.6,
        fill: { color: "0F1B33" },
        line: { color: st.c, width: 1.5 },
        rectRadius: 0.1,
        shadow: { type: "outer", blur: 8, offset: 2, color: "000000", opacity: 0.3 },
      });
      // Number + title bar
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: by, w: 2.2, h: 0.5,
        fill: { color: st.c },
        rectRadius: 0.0,
      });
      s.addText(`${st.n}  ${st.t}`, {
        x: bx, y: by, w: 2.2, h: 0.5,
        fontSize: 12, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle",
      });
      // Description
      s.addText(st.d, {
        x: bx + 0.1, y: by + 0.65, w: 2.0, h: 1.8,
        fontSize: 10, color: C.slateL, fontFace: "Calibri", valign: "top",
      });
    });

    // Bottom bar
    s.addShape(pptx.ShapeType.rect, {
      x: 0, y: 7.1, w: W, h: 0.4,
      fill: { color: "071021" },
    });
    s.addText("Apache Kafka · Spark Streaming · XGBoost · Streamlit · Prometheus · Grafana · Docker · PostgreSQL", {
      x: 0.5, y: 7.1, w: 9, h: 0.4,
      fontSize: 9, color: C.slate, fontFace: "Calibri", align: "center", valign: "middle",
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 2 — EL PROBLEMA / NUESTRA SOLUCIÓN
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    s.background = { color: C.navy };
    headerBar(s);
    s.addText("01", {
      x: 0.6, y: 0.35, w: 1, h: 0.45,
      fontSize: 13, bold: true, color: C.teal, fontFace: "Calibri",
    });
    s.addText("El problema real del altiplano", {
      x: 1.3, y: 0.35, w: 8, h: 0.45,
      fontSize: 24, bold: true, color: C.white, fontFace: "Calibri",
    });

    // Problem text
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 1.1, w: 8.8, h: 1.0,
      fill: { color: "0F1B33" },
      line: { color: C.rose, width: 1.5 },
      rectRadius: 0.1,
    });
    s.addText("En Puno, una helada no anunciada puede destruir una cosecha entera en una noche. Los productores dependen de pronósticos genéricos que no reflejan las microcondiciones del altiplano. Hasta ahora.", {
      x: 0.8, y: 1.15, w: 8.4, h: 0.9,
      fontSize: 14, color: C.offWhite, fontFace: "Calibri", valign: "middle",
    });

    // Solution: 3 pillars
    const pillars = [
      { i: "📡", t: "Monitoreo Hiperlocal", d: "Sensores IoT instalados en los propios campos de cultivo miden temperatura, humedad, presión y calidad del aire en tiempo real, transmitiendo datos cada 2 segundos vía Supabase + Kafka.", c: C.teal },
      { i: "🧠", t: "IA Entrenada con 48 Años de Historia", d: "XGBoost conoce el clima de Puno desde 1964. Cada helada, cada lluvia, cada variación estacional está codificada en sus 500 árboles de decisión. Predice con solo 0.08°C de error.", c: C.amber },
      { i: "🖥️", t: "Visualización que Cualquiera Puede Usar", d: "Dashboard interactivo con 4 pestañas: históricos, tiempo real, predicciones ML y métricas del sistema. Diseñado tanto para el técnico como para el productor.", c: C.emerald },
    ];
    pillars.forEach((p, i) => {
      const py = 2.4 + i * 1.5;
      s.addShape(pptx.ShapeType.roundRect, {
        x: 0.6, y: py, w: 8.8, h: 1.3,
        fill: { color: "0F1B33" },
        line: { color: p.c, width: 1.5 },
        rectRadius: 0.1,
        shadow: { type: "outer", blur: 6, offset: 2, color: "000000", opacity: 0.2 },
      });
      s.addText(p.i, {
        x: 0.8, y: py + 0.1, w: 0.5, h: 0.5,
        fontSize: 28, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(p.t, {
        x: 1.4, y: py + 0.1, w: 7.6, h: 0.4,
        fontSize: 17, bold: true, color: p.c, fontFace: "Calibri", valign: "middle",
      });
      s.addText(p.d, {
        x: 1.4, y: py + 0.55, w: 7.6, h: 0.65,
        fontSize: 12, color: C.slateL, fontFace: "Calibri", valign: "top",
      });
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 3 — ARQUITECTURA BIG DATA
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "02", "Arquitectura Big Data", "Pipeline completo: del sensor físico al dashboard inteligente");

    // Flow: horizontal pipeline with icons
    const stages = [
      { t: "Sensores IoT\nen Campo", d: "T°, Humedad\nPresión, IAQ\nC/2 segundos", c: C.teal },
      { t: "Bridges\nKafka", d: "Checkpoint\n3 consumers\nSupabase→Kafka", c: C.emerald },
      { t: "Kafka\nBroker", d: "Clima-grupo_2/3/4\nKRaft mode\nAlta tasa", c: C.amber },
      { t: "Spark\nStreaming", d: "3 pipelines\nZ-score\nAnomalías", c: C.rose },
      { t: "ML\nXGBoost", d: "7 modelos .pkl\nLargo + Corto\nR² > 0.99", c: C.violet },
      { t: "Dashboard\nStreamlit", d: "4 pestañas\nKafka consumer\nAuto-refresh 2s", c: C.teal },
    ];

    stages.forEach((st, i) => {
      const bx = 0.2 + i * 1.63;
      // Stage box
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: 1.5, w: 1.5, h: 1.6,
        fill: { color: "0F1B33" },
        line: { color: st.c, width: 2 },
        rectRadius: 0.1,
        shadow: { type: "outer", blur: 6, offset: 2, color: "000000", opacity: 0.3 },
      });
      s.addText(st.t, {
        x: bx, y: 1.55, w: 1.5, h: 0.7,
        fontSize: 11, bold: true, color: st.c, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(st.d, {
        x: bx + 0.05, y: 2.2, w: 1.4, h: 0.8,
        fontSize: 8.5, color: C.slateL, fontFace: "Calibri", align: "center", valign: "top",
      });
      // Arrow
      if (i < stages.length - 1) {
        s.addText("▶", {
          x: bx + 1.5, y: 2.1, w: 0.2, h: 0.3,
          fontSize: 10, color: C.slate, fontFace: "Calibri", align: "center",
        });
      }
    });

    // Sinks section
    s.addText("3 Sinks de Procesamiento", {
      x: 0.6, y: 3.4, w: 4, h: 0.3,
      fontSize: 13, bold: true, color: C.white, fontFace: "Calibri",
    });

    const sinks = [
      { t: "Kafka — Tópicos de Anomalías", d: "Solo registros con isAnomaly=True se publican a clima-{estacion}-anomalias. Alertas en tiempo real.", c: C.rose },
      { t: "PostgreSQL 15", d: "Datos persistentes con INSERT ON CONFLICT DO NOTHING. ~374K registros almacenados.", c: C.emerald },
      { t: "Parquet Columnar", d: "Almacenamiento optimizado para analítica. 2982 archivos históricos + streaming continuo.", c: C.teal },
    ];
    sinks.forEach((sn, i) => {
      const sx = 0.3 + i * 3.2;
      s.addShape(pptx.ShapeType.roundRect, {
        x: sx, y: 3.8, w: 3, h: 0.85,
        fill: { color: "0F1B33" },
        line: { color: sn.c, width: 1.5 },
        rectRadius: 0.08,
      });
      s.addText(sn.t, {
        x: sx + 0.1, y: 3.82, w: 2.8, h: 0.35,
        fontSize: 11, bold: true, color: sn.c, fontFace: "Calibri",
      });
      s.addText(sn.d, {
        x: sx + 0.1, y: 4.15, w: 2.8, h: 0.4,
        fontSize: 9.5, color: C.slateL, fontFace: "Calibri", valign: "top",
      });
    });

    // Observability
    s.addText("Observabilidad", {
      x: 0.6, y: 4.9, w: 4, h: 0.3,
      fontSize: 13, bold: true, color: C.white, fontFace: "Calibri",
    });
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 5.2, w: 8.8, h: 0.6,
      fill: { color: "0F1B33" },
      line: { color: C.amber, width: 1.5 },
      rectRadius: 0.08,
    });
    s.addText("Kafka ──[2 Exporters]──▶ Prometheus ──[scrape 15s]──▶ Grafana ──[2 Dashboards + 10 Alertas]──▶ Notificaciones", {
      x: 0.8, y: 5.25, w: 8.4, h: 0.5,
      fontSize: 11, color: C.amber, fontFace: "Consolas", valign: "middle", align: "center",
    });

    // Infrastructure tag
    s.addText("15 servicios Docker · red clime-net · volúmenes persistentes · GitHub Actions · 24/7 operación", {
      x: 0.6, y: 6.1, w: 8.8, h: 0.3,
      fontSize: 10, color: C.slate, fontFace: "Calibri", italic: true, align: "center",
    });

    // Bottom KPIs
    const bKpis = [
      { n: "3", l: "Estaciones IoT en Vivo", c: C.teal },
      { n: "374K", l: "Registros Streaming", c: C.emerald },
      { n: "2982", l: "Archivos Parquet Históricos", c: C.amber },
      { n: "1M+", l: "Datos SENAMHI 1964-2012", c: C.rose },
    ];
    bKpis.forEach((k, i) => {
      const bx = 0.3 + i * 2.45;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: 6.5, w: 2.25, h: 0.7,
        fill: { color: "0F1B33" },
        line: { color: k.c, width: 1.5 },
        rectRadius: 0.08,
      });
      s.addText(k.n, {
        x: bx, y: 6.5, w: 2.25, h: 0.7,
        fontSize: 18, bold: true, color: k.c, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(k.l, {
        x: bx + 0.8, y: 6.5, w: 1.4, h: 0.7,
        fontSize: 9, color: C.slateL, fontFace: "Calibri", valign: "middle",
      });
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 4 — RESULTADOS ML: LARGO PLAZO
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "03", "Resultados — Predicción Climática a 14 Días", "XGBoost entrenado con 48 años de datos SENAMHI (1964-2012)");

    // Big result
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 1.4, w: 4.2, h: 2.0,
      fill: { color: "0F1B33" },
      line: { color: C.emerald, width: 2 },
      rectRadius: 0.12,
      shadow: { type: "outer", blur: 12, offset: 3, color: C.emerald, opacity: 0.1 },
    });
    s.addText("R² > 0.994", {
      x: 0.6, y: 1.45, w: 4.2, h: 0.8,
      fontSize: 38, bold: true, color: C.emerald, fontFace: "Calibri", align: "center", valign: "middle",
    });
    s.addText("en TODAS las estaciones", {
      x: 0.6, y: 2.15, w: 4.2, h: 0.35,
      fontSize: 14, bold: true, color: C.white, fontFace: "Calibri", align: "center",
    });
    s.addText("Error promedio: 0.08°C · Máximo: 0.09°C · Mínimo: 0.07°C", {
      x: 0.6, y: 2.55, w: 4.2, h: 0.35,
      fontSize: 11, color: C.slateL, fontFace: "Calibri", align: "center",
    });
    s.addText("4 estaciones · 500 árboles · depth=7 · lr=0.05", {
      x: 0.6, y: 2.85, w: 4.2, h: 0.3,
      fontSize: 10, color: C.slate, fontFace: "Calibri", align: "center",
    });

    // Table by station
    s.addText("Precisión por Estación", {
      x: 5.2, y: 1.4, w: 4.5, h: 0.35,
      fontSize: 14, bold: true, color: C.white, fontFace: "Calibri",
    });
    const hdr = { fontSize: 10, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle" };
    const cel = { fontSize: 11.5, color: C.offWhite, fontFace: "Calibri", align: "center", valign: "middle" };
    const celG = { fontSize: 11.5, color: C.emerald, fontFace: "Calibri", align: "center", valign: "middle", bold: true };
    const lpRows = [
      [
        { text: "Estación", options: { ...hdr, align: "left" } },
        { text: "MAE", options: hdr },
        { text: "RMSE", options: hdr },
        { text: "R²", options: hdr },
        { text: "Exactitud", options: hdr },
      ],
      [{ text: "PUNO", options: { ...cel, align: "left", bold: true } }, { text: "0.08°C", options: cel }, { text: "0.20°C", options: cel }, { text: "0.994", options: celG }, { text: "99.4%", options: cel }],
      [{ text: "AZÁNGARO", options: { ...cel, align: "left", bold: true } }, { text: "0.09°C", options: cel }, { text: "0.20°C", options: cel }, { text: "0.995", options: celG }, { text: "99.5%", options: cel }],
      [{ text: "LAMPA", options: { ...cel, align: "left", bold: true } }, { text: "0.07°C", options: cel }, { text: "0.16°C", options: cel }, { text: "0.997", options: celG }, { text: "99.7%", options: cel }],
      [{ text: "CAPACHICA", options: { ...cel, align: "left", bold: true } }, { text: "0.08°C", options: cel }, { text: "0.18°C", options: cel }, { text: "0.994", options: celG }, { text: "99.4%", options: cel }],
    ];
    s.addTable(lpRows, {
      x: 5.2, y: 1.85, w: 4.5, colW: [1.2, 0.85, 0.85, 0.8, 0.8],
      rowH: [0.3, 0.28, 0.28, 0.28, 0.28],
      border: { type: "solid", color: C.teal, pt: 0.5 },
      autoPage: false,
    });

    // Feature importance
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 3.7, w: 8.8, h: 0.8,
      fill: { color: "0F1B33" },
      line: { color: C.amber, width: 1.5 },
      rectRadius: 0.1,
    });
    s.addText("🔑  Features más influyentes:", {
      x: 0.8, y: 3.72, w: 2.5, h: 0.3,
      fontSize: 12, bold: true, color: C.amber, fontFace: "Calibri",
    });
    const feats = [
      "tmax_lag_1 (31%) — La temperatura de ayer es el mejor predictor",
      "tmax_lag_7 (18%) — El mismo día de la semana pasada importa",
      "rolling_mean_7 (12%) — La tendencia semanal define el clima",
      "month_sin/cos (10%) — La estacionalidad del altiplano",
    ];
    feats.forEach((f, i) => {
      s.addText(`• ${f}`, {
        x: 0.8, y: 4.0 + i * 0.2, w: 8.4, h: 0.2,
        fontSize: 9.5, color: C.slateL, fontFace: "Calibri",
      });
    });

    // Bottom KPI row
    const kpis = [
      { n: "4", l: "Estaciones entrenadas", c: C.teal },
      { n: "50 años", l: "Datos históricos", c: C.emerald },
      { n: "14 días", l: "Horizonte predicción", c: C.amber },
      { n: "< 1.5s", l: "Inferencia por estación", c: C.rose },
    ];
    kpis.forEach((k, i) => {
      const bx = 0.3 + i * 2.45;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: 5.7, w: 2.25, h: 1.1,
        fill: { color: "0F1B33" },
        line: { color: k.c, width: 1.5 },
        rectRadius: 0.1,
      });
      s.addText(k.n, {
        x: bx, y: 5.75, w: 2.25, h: 0.5,
        fontSize: 22, bold: true, color: k.c, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(k.l, {
        x: bx, y: 6.25, w: 2.25, h: 0.4,
        fontSize: 10, color: C.slateL, fontFace: "Calibri", align: "center", valign: "top",
      });
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 5 — RESULTADOS: CORTO PLAZO
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "04", "Resultados — Predicción en Tiempo Real", "XGBoost para datos streaming IoT con actualización cada 2 segundos");

    // Dual card: OK vs Defective
    // Left: good results
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 1.4, w: 4.2, h: 2.8,
      fill: { color: "0F1B33" },
      line: { color: C.emerald, width: 2 },
      rectRadius: 0.12,
      shadow: { type: "outer", blur: 10, offset: 3, color: C.emerald, opacity: 0.08 },
    });
    s.addText("✅  Rendimiento Óptimo", {
      x: 0.8, y: 1.5, w: 3.8, h: 0.4,
      fontSize: 15, bold: true, color: C.emerald, fontFace: "Calibri",
    });
    s.addText("R² > 0.99", {
      x: 0.8, y: 1.95, w: 3.8, h: 0.7,
      fontSize: 34, bold: true, color: C.emerald, fontFace: "Calibri", align: "center",
    });
    s.addText("MAE < 0.05°C · Sensores operando correctamente", {
      x: 0.8, y: 2.6, w: 3.8, h: 0.3,
      fontSize: 11, color: C.slateL, fontFace: "Calibri", align: "center",
    });
    s.addText("Grupo 2 — Estación LAMPA: 0.044°C MAE, R² 0.992\nGrupo 4 — Estación AZÁNGARO: 0.053°C MAE, R² 0.993", {
      x: 0.8, y: 3.0, w: 3.8, h: 0.7,
      fontSize: 11, color: C.offWhite, fontFace: "Calibri", valign: "top",
    });

    // Right: defective sensor (value add)
    s.addShape(pptx.ShapeType.roundRect, {
      x: 5.2, y: 1.4, w: 4.2, h: 2.8,
      fill: { color: "0F1B33" },
      line: { color: C.rose, width: 2 },
      rectRadius: 0.12,
    });
    s.addText("⚠️  Detección de Sensor Defectuoso", {
      x: 5.4, y: 1.5, w: 3.8, h: 0.4,
      fontSize: 15, bold: true, color: C.rose, fontFace: "Calibri",
    });
    s.addText("R² = -1.41", {
      x: 5.4, y: 1.95, w: 3.8, h: 0.7,
      fontSize: 34, bold: true, color: C.rose, fontFace: "Calibri", align: "center",
    });
    s.addText("Sensor identificado como defectuoso por el pipeline", {
      x: 5.4, y: 2.6, w: 3.8, h: 0.3,
      fontSize: 11, color: C.slateL, fontFace: "Calibri", align: "center",
    });
    s.addText("Grupo 3 — PUNO: Presión anómala (186 hPa vs 646)\nTemperatura constante -0.06°C. El modelo ML\n detectó automáticamente datos no confiables.", {
      x: 5.4, y: 3.0, w: 3.8, h: 0.7,
      fontSize: 11, color: C.offWhite, fontFace: "Calibri", valign: "top",
    });

    // Data volume section
    s.addText("Volumen de Datos Streaming Procesados", {
      x: 0.6, y: 4.5, w: 8.8, h: 0.35,
      fontSize: 14, bold: true, color: C.white, fontFace: "Calibri",
    });

    const vols = [
      { g: "grupo_2 / LAMPA", r: "136,425 registros", pct: 1.0, c: C.teal },
      { g: "grupo_3 / PUNO", r: "141,289 registros", pct: 1.0, c: C.rose },
      { g: "grupo_4 / AZÁNGARO", r: "96,938 registros", pct: 0.69, c: C.emerald },
    ];
    vols.forEach((v, i) => {
      const vy = 4.95 + i * 0.45;
      s.addText(v.g, {
        x: 0.6, y: vy, w: 2.8, h: 0.35,
        fontSize: 11, bold: true, color: v.c, fontFace: "Calibri", valign: "middle",
      });
      s.addText(v.r, {
        x: 3.6, y: vy, w: 1.5, h: 0.35,
        fontSize: 11, color: C.slateL, fontFace: "Calibri", valign: "middle",
      });
      // Bar
      s.addShape(pptx.ShapeType.roundRect, {
        x: 5.3, y: vy + 0.05, w: v.pct * 3.5, h: 0.22,
        fill: { color: v.c },
        rectRadius: 0.04,
      });
    });

    s.addText("Total: ~374,652 registros · ~152 MB en Parquet · 3 tópicos Kafka activos", {
      x: 0.6, y: 6.5, w: 8.8, h: 0.3,
      fontSize: 10, color: C.slate, fontFace: "Calibri", italic: true, align: "center",
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 6 — DASHBOARD STREAMLIT
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "05", "Dashboard Inteligente", "4 herramientas en una sola interfaz");

    const tabs = [
      { i: "📊", t: "Datos Históricos", d: "Explora 60+ estaciones SENAMHI. Filtros dinámicos por departamento, provincia y fechas. Gráficos de líneas, box plots, mapa interactivo con clustering de estaciones. Datos cargados directamente desde Parquet.", c: C.teal },
      { i: "⏱️", t: "Tiempo Real", d: "Streaming en vivo desde Kafka cada 2 segundos. Gauges animados de temperatura, humedad, IAQ y presión. Panel de últimas 60 lecturas con detección de eventos anómalos.", c: C.emerald },
      { i: "🤖", t: "Predicciones ML", d: "Selecciona estación y horizonte. Predicción a 14 días (largo plazo) o 15 minutos (corto plazo). Tarjetas con MAE, RMSE y R². Gráfica histórico + predicción auto-regresiva.", c: C.amber },
      { i: "📡", t: "Métricas del Stack", d: "Monitoreo en vivo de la salud del pipeline Big Data: offsets Kafka, consumer lag, brokers activos, estado de exporters vía Prometheus API.", c: C.violet },
    ];
    tabs.forEach((tab, i) => {
      const by = 1.3 + i * 1.35;
      s.addShape(pptx.ShapeType.roundRect, {
        x: 0.6, y: by, w: 8.8, h: 1.15,
        fill: { color: "0F1B33" },
        line: { color: tab.c, width: 2 },
        rectRadius: 0.1,
        shadow: { type: "outer", blur: 6, offset: 2, color: "000000", opacity: 0.2 },
      });
      s.addText(tab.i, {
        x: 0.8, y: by + 0.1, w: 0.6, h: 0.5,
        fontSize: 26, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(tab.t, {
        x: 1.5, y: by + 0.1, w: 7.5, h: 0.35,
        fontSize: 16, bold: true, color: tab.c, fontFace: "Calibri", valign: "middle",
      });
      s.addText(tab.d, {
        x: 1.5, y: by + 0.45, w: 7.5, h: 0.6,
        fontSize: 11, color: C.slateL, fontFace: "Calibri", valign: "top",
      });
    });

    s.addText("⏱ Auto-refresh 2 segundos · 🌙 Tema oscuro/claro nativo · 📱 Responsivo", {
      x: 0.6, y: 6.8, w: 8.8, h: 0.3,
      fontSize: 10, color: C.slate, fontFace: "Calibri", italic: true, align: "center",
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 7 — OBSERVABILIDAD
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "06", "Observabilidad Total", "Prometheus + Grafana: monitoreo 24/7 de cada componente");

    const obs = [
      { n: "10", l: "Alertas Automáticas", sl: "4 infraestructura + 6 sensores", c: C.rose },
      { n: "2", l: "Dashboards Grafana", sl: "Kafka Overview + Variables Sensor", c: C.teal },
      { n: "15s", l: "Scraping", sl: "Ciclo de recolección Prometheus", c: C.emerald },
      { n: "100+", l: "Métricas Expuestas", sl: "Offset, lag, T°, humedad, IAQ", c: C.amber },
    ];
    obs.forEach((k, i) => {
      kpiCard(s, 0.3 + i*2.45, 1.2, 2.25, 1.4, k.n, k.l, k.c);
    });

    // Alert rules
    s.addText("Reglas de Alerta", {
      x: 0.6, y: 2.9, w: 4, h: 0.3,
      fontSize: 14, bold: true, color: C.white, fontFace: "Calibri",
    });
    const ah = { fontSize: 9.5, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle" };
    const ac = { fontSize: 10, color: C.white, fontFace: "Calibri", valign: "middle" };
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
        { text: "sensor absent for 5 minutes", options: ac },
        { text: "🔴 Critical", options: { ...ac, color: C.rose } },
      ],
      [
        { text: "SparkPipelineDown", options: { ...ac, bold: true, color: C.rose } },
        { text: "spark_streaming_records == 0", options: ac },
        { text: "🔴 Critical", options: { ...ac, color: C.rose } },
      ],
    ];
    s.addTable(alertRows, {
      x: 0.6, y: 3.25, w: 8.8, colW: [3, 4.3, 1.5],
      rowH: [0.25, 0.27, 0.27, 0.27, 0.27, 0.27],
      border: { type: "solid", color: C.slate, pt: 0.5 },
      autoPage: false,
    });

    // Pipeline
    s.addText("Pipeline de Métricas:", {
      x: 0.6, y: 5.3, w: 3, h: 0.3,
      fontSize: 12, bold: true, color: C.white, fontFace: "Calibri",
    });
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 5.65, w: 8.8, h: 0.5,
      fill: { color: "0F1B33" },
      line: { color: C.amber, width: 1.5 },
      rectRadius: 0.08,
    });
    s.addText("Kafka ──[2 Exporters]──▶ Prometheus ──[scrape 15s]──▶ Grafana ──[Dashboard + Alertas]──▶ Notificaciones", {
      x: 0.8, y: 5.7, w: 8.4, h: 0.4,
      fontSize: 11, color: C.amber, fontFace: "Consolas", valign: "middle", align: "center",
    });

    // Dashboards
    const dbs = [
      { n: "📊  Kafka Overview", d: "Mensajes/s, bytes, particiones, consumidores líderes, lag por grupo" },
      { n: "🌡️  Sensores — Variables por Estación", d: "Temperatura, humedad, IAQ, presión + alertas + estado de conexión" },
    ];
    dbs.forEach((db, i) => {
      s.addShape(pptx.ShapeType.roundRect, {
        x: 0.6, y: 6.3 + i*0.4, w: 8.8, h: 0.35,
        fill: { color: "0F1B33" },
        line: { color: C.teal, width: 1 },
        rectRadius: 0.06,
      });
      s.addText(db.n, {
        x: 0.8, y: 6.3 + i*0.4, w: 2.8, h: 0.35,
        fontSize: 11, bold: true, color: C.teal, fontFace: "Calibri", valign: "middle",
      });
      s.addText(db.d, {
        x: 3.7, y: 6.3 + i*0.4, w: 5.5, h: 0.35,
        fontSize: 10, color: C.slateL, fontFace: "Calibri", valign: "middle",
      });
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 8 — STACK TECNOLÓGICO
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "07", "Stack Tecnológico", "Herramientas enterprise para un pipeline de clase mundial");

    const techs = [
      ["Apache Kafka 4.2.0", "KRaft", "Broker de mensajería · 6 tópicos · streaming real-time", C.teal],
      ["Spark Streaming", "Python", "3 pipelines · trigger 5s · z-score anomalías", C.amber],
      ["XGBoost", "scikit-learn", "7 modelos .pkl · R² > 0.99 · largo + corto plazo", C.rose],
      ["Streamlit + Plotly", "Python", "Dashboard 4 tabs · Kafka consumer · auto-refresh 2s", C.emerald],
      ["Prometheus + Grafana", "Go / TS", "Scraping 15s · 2 dashboards · 10 alertas automáticas", C.violet],
      ["PostgreSQL 15", "JDBC", "374K registros · INSERT ON CONFLICT · alta disponibilidad", C.tealD],
      ["Docker Compose", "15 servicios", "red clime-net · volúmenes · orquestación completa", C.amberD],
      ["GitHub Actions", "CI/CD", "Deploy automático · MkDocs · GitHub Pages", C.slateL],
    ];
    techs.forEach((t, i) => {
      const col = i % 4;
      const row = Math.floor(i / 4);
      const bx = 0.3 + col * 2.4;
      const by = 1.3 + row * 2.8;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: by, w: 2.2, h: 2.4,
        fill: { color: "0F1B33" },
        line: { color: t[3], width: 2 },
        rectRadius: 0.1,
        shadow: { type: "outer", blur: 6, offset: 2, color: "000000", opacity: 0.3 },
      });
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: by, w: 2.2, h: 0.5,
        fill: { color: t[3] },
        rectRadius: 0.0,
      });
      s.addText(t[0], {
        x: bx, y: by + 0.02, w: 2.2, h: 0.45,
        fontSize: 11, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(t[1], {
        x: bx + 0.1, y: by + 0.6, w: 2, h: 0.25,
        fontSize: 10, color: C.slateL, fontFace: "Calibri",
      });
      s.addText(t[2], {
        x: bx + 0.1, y: by + 0.9, w: 2, h: 1.3,
        fontSize: 10, color: C.offWhite, fontFace: "Calibri",
      });
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 9 — FEATURE ENGINEERING (CÓMO FUNCIONA)
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "08", "¿Cómo funciona la IA?", "Convertimos 50 años de clima en un modelo matemático que entiende el altiplano");

    // Intuitive explanation
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 1.3, w: 8.8, h: 1.0,
      fill: { color: "0F1B33" },
      line: { color: C.teal, width: 1.5 },
      rectRadius: 0.1,
    });
    s.addText("💡  XGBoost funciona como un meteorólogo experto que ha estudiado 50 años de datos: aprende que si ayer hizo calor y es verano, probablemente mañana también hará calor. Pero además captura patrones complejos que un humano no vería.", {
      x: 0.8, y: 1.35, w: 8.4, h: 0.9,
      fontSize: 13, color: C.offWhite, fontFace: "Calibri", valign: "middle",
    });

    // Largo plazo
    s.addText("📅  Para Predicción a 14 Días (Largo Plazo)", {
      x: 0.6, y: 2.5, w: 4.5, h: 0.3,
      fontSize: 13, bold: true, color: C.teal, fontFace: "Calibri",
    });
    const lpFeats = [
      ["tmax_lag_1 / _3 / _5 / _7", "¿Cuánto fue la temperatura máxima ayer y los últimos 7 días?"],
      ["rolling_mean_3 / _7 / _14 / _30", "¿Cuál es la tendencia de los últimos 3, 7, 14 y 30 días?"],
      ["rolling_std_7 / _14", "¿Qué tan variable está siendo el clima?"],
      ["month_sin + month_cos", "¿Es verano, otoño, invierno o primavera? (codificado como círculo)"],
    ];
    lpFeats.forEach((f, i) => {
      const fy = 2.9 + i * 0.4;
      s.addText(f[0], {
        x: 0.6, y: fy, w: 4.2, h: 0.3,
        fontSize: 10, bold: true, color: C.teal, fontFace: "Consolas",
      });
      s.addText(f[1], {
        x: 5, y: fy, w: 4.5, h: 0.3,
        fontSize: 10, color: C.slateL, fontFace: "Calibri",
      });
    });

    // Corto plazo
    s.addText("⚡  Para Predicción a 15 Minutos (Corto Plazo)", {
      x: 0.6, y: 4.7, w: 4.5, h: 0.3,
      fontSize: 13, bold: true, color: C.amber, fontFace: "Calibri",
    });
    const cpFeats = [
      ["temp_lag_1 / _3", "¿Cuánto fue la temperatura en las últimas 1 y 3 mediciones?"],
      ["temp_rolling_mean_5", "¿Cuál es la tendencia de los últimos 5 minutos?"],
      ["presion_temp_ratio", "Relación presión/temperatura: indicador atmosférico clave"],
      ["hour_sin + hour_cos", "¿Es de día o de noche? La hora codificada como círculo"],
    ];
    cpFeats.forEach((f, i) => {
      const fy = 5.1 + i * 0.4;
      s.addText(f[0], {
        x: 0.6, y: fy, w: 4.2, h: 0.3,
        fontSize: 10, bold: true, color: C.amber, fontFace: "Consolas",
      });
      s.addText(f[1], {
        x: 5, y: fy, w: 4.5, h: 0.3,
        fontSize: 10, color: C.slateL, fontFace: "Calibri",
      });
    });

    s.addText("➕  5 features adicionales de estacionalidad (día_sin/cos, season_onehot)", {
      x: 0.6, y: 6.7, w: 8.8, h: 0.3,
      fontSize: 10, color: C.slate, fontFace: "Calibri", italic: true, align: "center",
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 10 — ROADMAP
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "09", "Roadmap de Evolución", "De la prueba de concepto a la producción regional");

    const phases = [
      { p: "Fase 1", t: "Consolidación", c: C.teal, items: [
        "✅  Reemplazar sensor defectuoso grupo_3 PUNO",
        "✅  Incorporar más estaciones SENAMHI activas",
        "✅  Usar datos IoT como seed para predicción 2026",
      ]},
      { p: "Fase 2", t: "Automatización", c: C.emerald, items: [
        "✅  Re-entrenamiento automático programado (cron)",
        "✅  CI/CD para deploy de modelos ML",
        "✅  Alertas predictivas multi-canal",
      ]},
      { p: "Fase 3", t: "Escalamiento", c: C.amber, items: [
        "✅  Ensemble: XGBoost + Random Forest + LSTM",
        "✅  Spark MLlib para 100+ estaciones",
        "✅  Features climáticos externos (viento, radiación)",
      ]},
      { p: "Fase 4", t: "Producción", c: C.rose, items: [
        "✅  Intervalos de confianza en predicciones",
        "✅  Despliegue cloud (AWS/GCP)",
        "✅  App móvil con notificaciones push",
      ]},
    ];

    phases.forEach((ph, i) => {
      const px = 0.2 + i * 2.45;
      s.addShape(pptx.ShapeType.roundRect, {
        x: px, y: 1.3, w: 2.35, h: 0.55,
        fill: { color: ph.c },
        rectRadius: 0.08,
      });
      s.addText(`${ph.p}: ${ph.t}`, {
        x: px, y: 1.3, w: 2.35, h: 0.55,
        fontSize: 12, bold: true, color: C.white, fontFace: "Calibri", align: "center", valign: "middle",
      });
      ph.items.forEach((item, j) => {
        const iy = 2.0 + j * 0.6;
        s.addShape(pptx.ShapeType.roundRect, {
          x: px, y: iy, w: 2.35, h: 0.52,
          fill: { color: "0F1B33" },
          line: { color: ph.c, width: 1 },
          rectRadius: 0.06,
        });
        s.addText(item, {
          x: px + 0.08, y: iy, w: 2.19, h: 0.52,
          fontSize: 9.5, color: C.slateL, fontFace: "Calibri", valign: "middle",
        });
      });
    });

    // Vision
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.6, y: 4.8, w: 8.8, h: 1.5,
      fill: { color: "0F1B33" },
      line: { color: C.amber, width: 2 },
      rectRadius: 0.12,
    });
    s.addText("🎯  Visión 2026-2027", {
      x: 0.8, y: 4.85, w: 8.4, h: 0.4,
      fontSize: 16, bold: true, color: C.amber, fontFace: "Calibri",
    });
    s.addText("Un sistema integral de monitoreo y predicción climática para toda la región Puno, con modelos auto-actualizados que aprenden de cada nueva medición. Alertas predictivas en tiempo real que protegen cultivos, ganado y comunidades. Democratizar el acceso a inteligencia climática de alta precisión para agricultores, gobiernos locales e investigadores.", {
      x: 0.8, y: 5.25, w: 8.4, h: 0.9,
      fontSize: 12, color: C.slateL, fontFace: "Calibri",
    });
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 11 — MÉTRICAS CLAVE
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    sectionSlide(s, "10", "Métricas Clave del Sistema", "Indicadores de rendimiento y salud del pipeline");

    // Section: Pipeline
    s.addText("Pipeline de Datos", {
      x: 0.6, y: 1.3, w: 8.8, h: 0.3,
      fontSize: 13, bold: true, color: C.teal, fontFace: "Calibri",
    });
    const p1 = [
      { n: "~374K", l: "Registros Streaming", sl: "3 grupos IoT", c: C.teal },
      { n: "2982", l: "Archivos Parquet", sl: "Históricos SENAMHI", c: C.emerald },
      { n: "6", l: "Tópicos Kafka", sl: "3 datos + 3 anomalías", c: C.amber },
      { n: "1M+", l: "Registros Históricos", sl: "1964-2012", c: C.rose },
    ];
    p1.forEach((k, i) => kpiCard(s, 0.3 + i*2.45, 1.65, 2.25, 1.3, k.n, k.l, k.c));

    // Section: ML
    s.addText("Machine Learning", {
      x: 0.6, y: 3.25, w: 8.8, h: 0.3,
      fontSize: 13, bold: true, color: C.amber, fontFace: "Calibri",
    });
    const p2 = [
      { n: "7", l: "Modelos Entrenados", sl: "4 largo + 3 corto plazo", c: C.amber },
      { n: "R²>0.99", l: "Precisión Promedio", sl: "6 de 7 modelos", c: C.emerald },
      { n: "<0.1°C", l: "Error Promedio (MAE)", sl: "Largo plazo", c: C.teal },
      { n: "<1.5s", l: "Inferencia", sl: "Por estación", c: C.rose },
    ];
    p2.forEach((k, i) => kpiCard(s, 0.3 + i*2.45, 3.6, 2.25, 1.3, k.n, k.l, k.c));

    // Section: Infrastructure
    s.addText("Infraestructura", {
      x: 0.6, y: 5.2, w: 8.8, h: 0.3,
      fontSize: 13, bold: true, color: C.violet, fontFace: "Calibri",
    });
    const p3 = [
      { n: "15", l: "Servicios Docker", sl: "Orquestación completa", c: C.violet },
      { n: "10", l: "Alertas Activas", sl: "Prometheus rule files", c: C.rose },
      { n: "100%", l: "Uptime Esperado", sl: "Reinicio automático", c: C.emerald },
      { n: "CI/CD", l: "GitHub Actions", sl: "Pages + deploy", c: C.teal },
    ];
    p3.forEach((k, i) => kpiCard(s, 0.3 + i*2.45, 5.55, 2.25, 1.2, k.n, k.l, k.c));
  }

  // ════════════════════════════════════════════════════════
  // SLIDE 12 — CIERRE
  // ════════════════════════════════════════════════════════
  {
    const s = pptx.addSlide();
    s.background = { color: C.navy };
    // Top bars
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: W, h: 0.05, fill: { color: C.teal } });
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0.05, w: W*0.6, h: 0.03, fill: { color: C.emerald } });
    s.addShape(pptx.ShapeType.rect, { x: 0, y: 0.08, w: W*0.3, h: 0.02, fill: { color: C.amber } });

    // Main text
    s.addText("CliMePerú", {
      x: 0, y: 1.2, w: W, h: 0.9,
      fontSize: 48, bold: true, color: C.white, fontFace: "Calibri", align: "center",
    });
    s.addText("Monitoreo Climático Inteligente para la Región Puno", {
      x: 0, y: 2.1, w: W, h: 0.45,
      fontSize: 18, color: C.teal, fontFace: "Calibri", align: "center",
    });

    // Achievements
    const achievements = [
      "✅ Pipeline Big Data end-to-end: desde el sensor IoT hasta el dashboard en 2 segundos",
      "✅  7 modelos ML con R² > 0.99: predicción con precisión de décimas de grado",
      "✅  Detección de anomalías y sensores defectuosos en tiempo real vía Spark Streaming",
      "✅  Observabilidad total: 10 alertas · 2 dashboards Grafana · 15 servicios monitoreados",
    ];
    s.addShape(pptx.ShapeType.roundRect, {
      x: 1.2, y: 2.9, w: 7.6, h: 1.8,
      fill: { color: "0F1B33" },
      line: { color: C.teal, width: 1.5 },
      rectRadius: 0.12,
    });
    achievements.forEach((a, i) => {
      s.addText(a, {
        x: 1.4, y: 3.0 + i*0.4, w: 7.2, h: 0.35,
        fontSize: 12, color: C.slateL, fontFace: "Calibri", valign: "middle",
      });
    });

    // CTA box
    s.addShape(pptx.ShapeType.roundRect, {
      x: 2.5, y: 5.1, w: 5, h: 0.7,
      fill: { color: "0F1B33" },
      line: { color: C.amber, width: 1.5 },
      rectRadius: 0.1,
      shadow: { type: "outer", blur: 12, offset: 3, color: C.amber, opacity: 0.1 },
    });
    s.addText("🔗  github.com/IVANMAMANI2003/clime-peru", {
      x: 2.5, y: 5.1, w: 5, h: 0.4,
      fontSize: 13, color: C.amber, fontFace: "Calibri", align: "center", valign: "middle",
    });
    s.addText("UPeU — Curso de Big Data 2026", {
      x: 2.5, y: 5.5, w: 5, h: 0.25,
      fontSize: 11, color: C.slate, fontFace: "Calibri", align: "center",
    });

    s.addText("¿Preguntas?", {
      x: 0, y: 6.3, w: W, h: 0.5,
      fontSize: 24, bold: true, color: C.emerald, fontFace: "Calibri", align: "center",
    });
  }

  // ── Save ──
  const outputPath = path.join(__dirname, "CliMePeru_Presentacion_Comercial.pptx");
  await pptx.writeFile({ fileName: outputPath });
  console.log(`✓ Presentación ejecutiva generada: ${outputPath}`);
  console.log(`  Tamaño: ${(fs.statSync(outputPath).size / 1024).toFixed(1)} KB`);
  console.log(`  Slides: 12`);
}

generate().catch(err => {
  console.error("Error:", err.message);
  console.error(err.stack);
  process.exit(1);
});
