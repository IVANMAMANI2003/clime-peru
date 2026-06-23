const pptxgen = require("pptxgenjs");
const fs = require("node:fs");
const path = require("node:path");

const COLORS = {
  primary: "0D7377",    // teal oscuro
  secondary: "14919B",  // teal claro
  accent: "F59E0B",     // amber
  dark: "1E293B",       // slate 800
  light: "F8FAFC",      // slate 50
  gray: "64748B",       // slate 500
  white: "FFFFFF",
  green: "10B981",
  red: "EF4444",
  orange: "F97316",
  cyan: "06B6D4",
};

const W = 10; // slide width in inches
const H = 7.5;

async function generate() {
  const pptx = new pptxgen();
  pptx.defineLayout({ name: "WIDE", width: W, height: H });
  pptx.layout = "WIDE";

  // ── Helper: title + subtitle bar ──
  function slideHeader(slide, title, subtitle) {
    slide.background = { color: COLORS.white };
    slide.addShape(pptx.ShapeType.rect, {
      x: 0, y: 0, w: W, h: 1.2,
      fill: { color: COLORS.primary },
    });
    slide.addText(title, {
      x: 0.5, y: 0.15, w: 9, h: 0.6,
      fontSize: 28, bold: true, color: COLORS.white, fontFace: "Calibri",
    });
    slide.addText(subtitle || "", {
      x: 0.5, y: 0.65, w: 9, h: 0.4,
      fontSize: 14, color: COLORS.white, fontFace: "Calibri", italic: true,
    });
  }

  // ── Helper: bullet text ──
  function addBullets(slide, items, opts = {}) {
    const x = opts.x || 0.5;
    let y = opts.y || 1.5;
    const lineH = opts.lineH || 0.35;
    for (const item of items) {
      slide.addText(`• ${item}`, {
        x, y, w: opts.w || 9, h: lineH,
        fontSize: opts.fontSize || 13,
        color: opts.color || COLORS.dark,
        fontFace: "Calibri", valign: "top",
        bold: opts.bold || false,
      });
      y += lineH;
    }
  }

  // ── Slide 1: Portada ──
  {
    const s = pptx.addSlide();
    s.background = { color: COLORS.primary };
    s.addShape(pptx.ShapeType.rect, {
      x: 0, y: 2.5, w: W, h: 2.5,
      fill: { color: COLORS.secondary, transparency: 30 },
    });
    s.addText("CliMePerú", {
      x: 0.5, y: 2.7, w: 9, h: 1,
      fontSize: 44, bold: true, color: COLORS.white, fontFace: "Calibri",
      align: "center",
    });
    s.addText("Sistema de Monitoreo Climático Inteligente", {
      x: 0.5, y: 3.6, w: 9, h: 0.6,
      fontSize: 20, color: COLORS.white, fontFace: "Calibri",
      align: "center", italic: true,
    });
    s.addText("Big Data · Streaming · Machine Learning", {
      x: 0.5, y: 4.3, w: 9, h: 0.5,
      fontSize: 14, color: COLORS.white, fontFace: "Calibri",
      align: "center",
    });
    s.addText("UPeU — Curso de Big Data", {
      x: 0.5, y: 5.5, w: 9, h: 0.4,
      fontSize: 12, color: COLORS.white, fontFace: "Calibri",
      align: "center",
    });
  }

  // ── Slide 2: ¿Qué es CliMePerú? ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "¿Qué es CliMePerú?", "Sistema Big Data para monitoreo climático en Puno, Perú");
    s.addText([
      { text: "CliMePerú ", options: { bold: true, fontSize: 18, color: COLORS.primary } },
      { text: "integra datos climáticos históricos del SENAMHI con lecturas en tiempo real de sensores IoT, utilizando herramientas modernas de Big Data.", options: { fontSize: 16, color: COLORS.dark } },
    ], { x: 0.5, y: 1.5, w: 9, h: 0.8, fontFace: "Calibri" });

    const boxes = [
      { t: "📡 Monitorear", d: "Temperatura, humedad, presión y calidad del aire en tiempo real", c: COLORS.cyan },
      { t: "⚠️ Detectar", d: "Anomalías climáticas vía Spark Streaming con z-score", c: COLORS.orange },
      { t: "🤖 Predecir", d: "Temperatura con XGBoost entrenado con datos históricos y streaming", c: COLORS.accent },
      { t: "📊 Visualizar", d: "Dashboard interactivo + Grafana + alertas Prometheus", c: COLORS.secondary },
    ];
    boxes.forEach((b, i) => {
      const bx = 0.5 + i * 2.3;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: 2.6, w: 2.1, h: 2.5,
        fill: { color: COLORS.white },
        line: { color: b.c, width: 2 },
        rectRadius: 0.15,
        shadow: { type: "outer", blur: 6, offset: 2, color: "CCCCCC", opacity: 0.3 },
      });
      s.addText(b.t, {
        x: bx, y: 2.7, w: 2.1, h: 0.6,
        fontSize: 14, bold: true, color: b.c, fontFace: "Calibri", align: "center",
      });
      s.addText(b.d, {
        x: bx + 0.1, y: 3.3, w: 1.9, h: 1.6,
        fontSize: 11, color: COLORS.gray, fontFace: "Calibri", align: "center", valign: "top",
      });
    });

    s.addText("Stack: Apache Kafka · Spark · XGBoost · Streamlit · Prometheus · Grafana · PostgreSQL", {
      x: 0.5, y: 5.5, w: 9, h: 0.5,
      fontSize: 11, color: COLORS.gray, fontFace: "Calibri", align: "center", italic: true,
    });
  }

  // ── Slide 3: Arquitectura (texto para generar imagen en otra IA) ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "Arquitectura del Sistema", "Usa este diagrama para generar una imagen profesional con otra IA");

    const lines = [
      { t: "ARQUITECTURA — CliMePerú", b: true, s: 15, c: COLORS.primary },
      { t: "", b: false, s: 6, c: COLORS.white },
      { t: "SENAMHI (.txt) ──[Spark ETL Batch]──▶ Parquet Histórico", b: false, s: 12, c: COLORS.dark },
      { t: "Parquet Histórico ──[stats avg/stddev vía tmax]──▶ 3 Spark Streaming", b: false, s: 12, c: COLORS.gray },
      { t: "", b: false, s: 6, c: COLORS.white },
      { t: "Supabase (3 tablas IoT) ──[Bridges Kafka ×3 c/checkpoint]──▶ Kafka", b: false, s: 12, c: COLORS.dark },
      { t: "                                            clima-grupo_2/3/4", b: false, s: 10, c: COLORS.gray },
      { t: "", b: false, s: 6, c: COLORS.white },
      { t: "Kafka ──[Spark Structured Streaming ×3, trigger 5s]──▶ 3 sinks:", b: false, s: 12, c: COLORS.dark },
      { t: "   ├──▶ Kafka (clima-grupo_2/3/4-anomalias) solo isAnomaly=True", b: false, s: 11, c: COLORS.gray },
      { t: "   ├──▶ PostgreSQL (sensor_data_grupo_2/3/4) foreachBatch", b: false, s: 11, c: COLORS.gray },
      { t: "   └──▶ Parquet Streaming", b: false, s: 11, c: COLORS.gray },
      { t: "", b: false, s: 6, c: COLORS.white },
      { t: "Parquet Histórico + Streaming ──[ML XGBoost]──▶ Modelos .pkl (7)", b: false, s: 12, c: COLORS.accent },
      { t: "", b: false, s: 6, c: COLORS.white },
      { t: "Modelos .pkl ──[predict]──▶ Dashboard Streamlit (🤖 Predicciones ML)", b: false, s: 12, c: COLORS.dark },
      { t: "Kafka ──[consumer dashboard-consumer]──▶ Dashboard (⏱️ Tiempo Real)", b: false, s: 12, c: COLORS.gray },
      { t: "Parquet Histórico ──[pd.read_parquet]──▶ Dashboard (📊 Históricos)", b: false, s: 12, c: COLORS.gray },
      { t: "", b: false, s: 6, c: COLORS.white },
      { t: "Kafka ──[Kafka Exporter]──▶ Prometheus ──[Grafana]──▶ Kafka Overview", b: false, s: 12, c: COLORS.dark },
      { t: "Kafka ──[Value Exporter]──▶ Prometheus ──[Grafana]──▶ Sensores Variables", b: false, s: 12, c: COLORS.gray },
      { t: "Prometheus ──[alert.rules.yml]──▶ 10 Alertas", b: false, s: 12, c: COLORS.gray },
      { t: "", b: false, s: 6, c: COLORS.white },
      { t: "Docker Compose ──[15 servicios, red clime-net, volúmenes]──▶ Stack Completo", b: false, s: 12, c: COLORS.dark },
    ];

    let y = 1.45;
    for (const l of lines) {
      if (l.t === "") { y += 0.04; continue; }
      s.addText(l.t, {
        x: 0.5, y, w: 9, h: 0.28,
        fontSize: l.s, bold: l.b, color: l.c, fontFace: "Consolas", valign: "top",
      });
      y += 0.22;
    }

    s.addText("💡 Sugerencia: Copia este texto a una IA generadora de imágenes (DALL-E, Midjourney) y pídele un diagrama de arquitectura profesional.", {
      x: 0.5, y: 6.3, w: 9, h: 0.5,
      fontSize: 10, color: COLORS.primary, fontFace: "Calibri", italic: true,
    });
  }

  // ── Slide 4: Stack Tecnológico ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "Stack Tecnológico", "15 servicios Docker orquestados");

    const techs = [
      ["Apache Kafka 4.2.0", "Broker de mensajería (KRaft)", COLORS.secondary],
      ["Spark Structured Streaming", "Procesamiento en tiempo real", COLORS.orange],
      ["XGBoost + scikit-learn", "Modelos de regresión ML", COLORS.accent],
      ["Streamlit + Plotly", "Dashboard interactivo", COLORS.red],
      ["Prometheus + Grafana", "Métricas y observabilidad", COLORS.green],
      ["PostgreSQL 15", "Almacenamiento persistente", COLORS.cyan],
    ];

    techs.forEach((t, i) => {
      const col = i % 3;
      const row = Math.floor(i / 3);
      const bx = 0.5 + col * 3.1;
      const by = 1.5 + row * 2.2;
      s.addShape(pptx.ShapeType.roundRect, {
        x: bx, y: by, w: 2.9, h: 1.8,
        fill: { color: COLORS.light },
        line: { color: t[2], width: 2 },
        rectRadius: 0.1,
      });
      s.addText(t[0], {
        x: bx + 0.15, y: by + 0.15, w: 2.6, h: 0.5,
        fontSize: 14, bold: true, color: t[2], fontFace: "Calibri",
      });
      s.addText(t[1], {
        x: bx + 0.15, y: by + 0.7, w: 2.6, h: 0.8,
        fontSize: 12, color: COLORS.gray, fontFace: "Calibri",
      });
    });

    s.addText("+ Kafka UI · Kafka Exporter · Value Exporter · Jupyter · Supabase", {
      x: 0.5, y: 6.3, w: 9, h: 0.4,
      fontSize: 11, color: COLORS.gray, fontFace: "Calibri", align: "center",
    });
  }

  // ── Slide 5: Machine Learning — Visión General ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "Machine Learning: Visión General", "Pipeline de predicción con XGBoost");

    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.3, y: 1.5, w: 4.5, h: 2.5,
      fill: { color: COLORS.light },
      line: { color: COLORS.primary, width: 2 },
      rectRadius: 0.15,
    });
    s.addText("📅 Largo Plazo (Diario)", {
      x: 0.5, y: 1.6, w: 4.1, h: 0.5,
      fontSize: 18, bold: true, color: COLORS.primary, fontFace: "Calibri",
    });
    addBullets(s, [
      "Target: tmax (temperatura máxima diaria)",
      "Datos: SENAMHI histórico (1964-2012)",
      "Estaciones: PUNO, AZANGARO, LAMPA, CAPACHICA",
      "Features: lags, rolling means, codificación cíclica",
      "Modelo: XGBoost (500 árboles, lr=0.05)",
    ], { x: 0.5, y: 2.2, w: 4.1, fontSize: 11, lineH: 0.3 });

    s.addShape(pptx.ShapeType.roundRect, {
      x: 5.2, y: 1.5, w: 4.5, h: 2.5,
      fill: { color: COLORS.light },
      line: { color: COLORS.accent, width: 2 },
      rectRadius: 0.15,
    });
    s.addText("⚡ Corto Plazo (5-15 min)", {
      x: 5.4, y: 1.6, w: 4.1, h: 0.5,
      fontSize: 18, bold: true, color: COLORS.accent, fontFace: "Calibri",
    });
    addBullets(s, [
      "Target: Temperatura instantánea",
      "Datos: Streaming sensores IoT (~374K registros)",
      "Grupos: grupo_2, grupo_3, grupo_4",
      "Features: lags rápidos, ratio presión/temp, hora",
      "Modelo: XGBoost (300 árboles, lr=0.05)",
    ], { x: 5.4, y: 2.2, w: 4.1, fontSize: 11, lineH: 0.3 });

    s.addText("Ambos modelos se integran en el Dashboard Streamlit como la pestaña 🤖 Predicciones ML", {
      x: 0.5, y: 4.3, w: 9, h: 0.5,
      fontSize: 13, color: COLORS.dark, fontFace: "Calibri", align: "center",
    });
  }

  // ── Slide 6: Feature Engineering ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "Feature Engineering", "Convertimos tiempo en predictores numéricos");

    s.addText("Largo Plazo — features generados por día:", {
      x: 0.5, y: 1.5, w: 9, h: 0.4,
      fontSize: 15, bold: true, color: COLORS.primary, fontFace: "Calibri",
    });

    const featLP = [
      ["tmax_lag_1 / _3 / _5 / _7", "Temperatura de 1, 3, 5 y 7 días atrás"],
      ["tmax_rolling_mean_3 / _7 / _14 / _30", "Promedio móvil de 3, 7, 14 y 30 días"],
      ["tmax_rolling_std_7 / _14", "Desviación estándar móvil (variabilidad)"],
      ["tmax_diff_30d", "¿Cuánto se desvía hoy del promedio del mes?"],
      ["trend_7_30", "Tendencia: ¿está subiendo o bajando?"],
      ["month_sin / month_cos", "Mes como círculo (evita salto dic-ene)"],
      ["day_sin / day_cos", "Día del año codificado"],
      ["season *", "Estación (one-hot: verano, otoño, inv, prim)"],
    ];

    featLP.forEach((f, i) => {
      s.addText(f[0], {
        x: 0.5, y: 2.0 + i * 0.35, w: 4.5, h: 0.3,
        fontSize: 11, bold: true, color: COLORS.dark, fontFace: "Consolas",
      });
      s.addText(f[1], {
        x: 5.2, y: 2.0 + i * 0.35, w: 4.5, h: 0.3,
        fontSize: 11, color: COLORS.gray, fontFace: "Calibri",
      });
    });

    s.addText("Corto Plazo — features por medición (~5 min):", {
      x: 0.5, y: 5.0, w: 9, h: 0.4,
      fontSize: 15, bold: true, color: COLORS.accent, fontFace: "Calibri",
    });

    const featCP = [
      ["temp_lag_1 / _3", "Últimas 1 y 3 mediciones de temperatura"],
      ["temp_rolling_mean_5", "Media móvil de 5 lecturas"],
      ["presion_temp_ratio", "Presión / Temperatura (indicador atmosférico)"],
      ["hour_sin / hour_cos", "Hora del día en círculo"],
    ];

    featCP.forEach((f, i) => {
      s.addText(f[0], {
        x: 0.5, y: 5.5 + i * 0.35, w: 4.5, h: 0.3,
        fontSize: 11, bold: true, color: COLORS.dark, fontFace: "Consolas",
      });
      s.addText(f[1], {
        x: 5.2, y: 5.5 + i * 0.35, w: 4.5, h: 0.3,
        fontSize: 11, color: COLORS.gray, fontFace: "Calibri",
      });
    });
  }

  // ── Slide 7: ¿Por qué XGBoost y no Series de Tiempo? ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "¿Por qué XGBoost y no Series de Tiempo?", "Regresión supervisada con lags vs ARIMA/Prophet/LSTM");

    const leftX = 0.3;
    const rightX = 5.1;
    const boxW = 4.6;

    // Left: Series de Tiempo
    s.addShape(pptx.ShapeType.roundRect, {
      x: leftX, y: 1.5, w: boxW, h: 4.5,
      fill: { color: "FFF5F5" },
      line: { color: COLORS.red, width: 2 },
      rectRadius: 0.12,
    });
    s.addText("❌ Series de Tiempo Clásicas", {
      x: leftX + 0.15, y: 1.6, w: boxW - 0.3, h: 0.45,
      fontSize: 16, bold: true, color: COLORS.red, fontFace: "Calibri",
    });
    s.addText("ARIMA, Prophet, LSTM", {
      x: leftX + 0.15, y: 2.05, w: boxW - 0.3, h: 0.3,
      fontSize: 12, color: COLORS.gray, fontFace: "Calibri", italic: true,
    });

    const tsProblems = [
      "Modelan solo la variable target en el tiempo",
      "No aceptan features externas fácilmente",
      "Requieren estacionariedad (diferenciación)",
      "ARIMA: orden p,d,q complicado de elegir",
      "LSTM: requiere muchos datos y GPU",
      "Difícil integrar múltiples estaciones",
      "Prophet: bueno para tendencia, malo para corto plazo",
    ];
    addBullets(s, tsProblems, { x: leftX + 0.15, y: 2.5, w: boxW - 0.3, fontSize: 11, lineH: 0.35, color: COLORS.dark });

    // Right: XGBoost
    s.addShape(pptx.ShapeType.roundRect, {
      x: rightX, y: 1.5, w: boxW, h: 4.5,
      fill: { color: "F0FDF4" },
      line: { color: COLORS.green, width: 2 },
      rectRadius: 0.12,
    });
    s.addText("✅ XGBoost con Features Temporales", {
      x: rightX + 0.15, y: 1.6, w: boxW - 0.3, h: 0.45,
      fontSize: 16, bold: true, color: COLORS.green, fontFace: "Calibri",
    });
    s.addText("Regresión supervisada + lags", {
      x: rightX + 0.15, y: 2.05, w: boxW - 0.3, h: 0.3,
      fontSize: 12, color: COLORS.gray, fontFace: "Calibri", italic: true,
    });

    const xgbBenefits = [
      "Acepta cualquier feature numérico (lags, rolling, cíclicas)",
      "Captura no linealidades con los árboles",
      "No necesita estacionariedad",
      "Funciona con datos de distinta frecuencia",
      "Entrena en segundos en CPU",
      "Misma lógica para largo y corto plazo",
      "Fácil de persistir y recargar (.pkl)",
    ];
    addBullets(s, xgbBenefits, { x: rightX + 0.15, y: 2.5, w: boxW - 0.3, fontSize: 11, lineH: 0.35, color: COLORS.dark });

    // Bottom insight
    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.3, y: 6.2, w: 9.4, h: 0.7,
      fill: { color: COLORS.primary },
      rectRadius: 0.1,
    });
    s.addText("💡 Convertimos el tiempo en features (lags, rolling, cíclicas) y usamos regresión clásica. Más simple, más rápido, igual o mejor precisión.", {
      x: 0.5, y: 6.25, w: 9, h: 0.6,
      fontSize: 13, color: COLORS.white, fontFace: "Calibri", align: "center", valign: "middle",
    });
  }

  // ── Slide 9: Cómo Funciona XGBoost ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "¿Cómo Funciona XGBoost?", "Gradient Boosting: árboles que corrigen árboles");

    s.addText([
      { text: "XGBoost ", options: { bold: true, fontSize: 16, color: COLORS.primary } },
      { text: "construye cientos de árboles de decisión pequeños, donde cada nuevo árbol se enfoca en corregir los errores del anterior.", options: { fontSize: 14, color: COLORS.dark } },
    ], { x: 0.5, y: 1.5, w: 9, h: 0.7, fontFace: "Calibri" });

    const steps = [
      ["Árbol 1", "Si month > 6, tmax ≈ 18°C, si no ≈ 22°C", "Error promedio: 2.5°C", COLORS.secondary],
      ["Árbol 2", "Donde árbol 1 falló: si lag_7 > 20°C, ajustar +1.5°C", "Error: 1.2°C", COLORS.primary],
      ["Árbol 3", "Donde aún falla: si rolling_std > 3, ajustar -0.8°C", "Error: 0.7°C", COLORS.cyan],
      ["... 500 árboles", "Cada árbol contribuye 0.05 × su corrección", "Error final: 0.08°C", COLORS.accent],
    ];

    steps.forEach((st, i) => {
      const by = 2.5 + i * 0.85;
      s.addShape(pptx.ShapeType.roundRect, {
        x: 0.5, y: by, w: 9, h: 0.7,
        fill: { color: COLORS.light },
        line: { color: st[3], width: 1.5 },
        rectRadius: 0.08,
      });
      s.addText(st[0], {
        x: 0.7, y: by + 0.05, w: 2, h: 0.6,
        fontSize: 12, bold: true, color: st[3], fontFace: "Calibri",
        valign: "middle",
      });
      s.addText(st[1], {
        x: 2.8, y: by + 0.05, w: 5, h: 0.6,
        fontSize: 11, color: COLORS.dark, fontFace: "Calibri",
        valign: "middle",
      });
      s.addText(st[2], {
        x: 7.8, y: by + 0.05, w: 1.5, h: 0.6,
        fontSize: 10, color: COLORS.gray, fontFace: "Calibri",
        valign: "middle", align: "right",
      });
    });

    s.addText("🔑 Clave: learning_rate=0.05 → cada árbol aporta poco, pero 500 árboles juntos predicen con MAE < 0.1°C", {
      x: 0.5, y: 6.2, w: 9, h: 0.5,
      fontSize: 13, bold: true, color: COLORS.primary, fontFace: "Calibri", align: "center",
    });
  }

  // ── Slide 10: Predicción Auto-Regresiva ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "Predicción Auto-Regresiva", "El modelo usa sus propias predicciones como entrada");

    s.addText("Largo Plazo — paso a paso:", {
      x: 0.5, y: 1.5, w: 9, h: 0.4,
      fontSize: 16, bold: true, color: COLORS.primary, fontFace: "Calibri",
    });

    const steps = [
      ["Paso 1", "Cargar datos históricos de la estación (últimos 30 días)"],
      ["Paso 2", "Generar features con build_largo_plazo_features()"],
      ["Paso 3", "Predecir día N+1 con model.predict()"],
      ["Paso 4", "Agregar predicción como si fuera un dato real"],
      ["Paso 5", "Repetir pasos 2-4 para N+2, N+3... (hasta 14 días)"],
    ];

    steps.forEach((st, i) => {
      const by = 2.1 + i * 0.6;
      s.addShape(pptx.ShapeType.roundRect, {
        x: 0.5, y: by, w: 1.5, h: 0.45,
        fill: { color: COLORS.primary },
        rectRadius: 0.08,
      });
      s.addText(st[0], {
        x: 0.5, y: by, w: 1.5, h: 0.45,
        fontSize: 11, bold: true, color: COLORS.white, fontFace: "Calibri", align: "center", valign: "middle",
      });
      s.addText(st[1], {
        x: 2.2, y: by, w: 7.3, h: 0.45,
        fontSize: 12, color: COLORS.dark, fontFace: "Calibri", valign: "middle",
      });
    });

    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.5, y: 5.3, w: 9, h: 1.2,
      fill: { color: "FFF3E0" },
      line: { color: COLORS.accent, width: 1.5 },
      rectRadius: 0.1,
    });
    s.addText("⚠️ Limitación actual", {
      x: 0.7, y: 5.35, w: 8.6, h: 0.3,
      fontSize: 13, bold: true, color: COLORS.orange, fontFace: "Calibri",
    });
    s.addText("Los datos SENAMHI llegan hasta ~2012. La predicción arranca desde esa fecha, no desde 2026. Para predecir 2026, necesitamos datos recientes de partida (sensores IoT) o re-entrenar con datos actualizados.", {
      x: 0.7, y: 5.65, w: 8.6, h: 0.7,
      fontSize: 11, color: COLORS.dark, fontFace: "Calibri",
    });
  }

  // ── Slide 11: Resultados ML ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "Resultados — Modelos ML", "Métricas de entrenamiento y rendimiento");

    s.addText("Largo Plazo (tmax diario, SENAMHI 1964-2012):", {
      x: 0.5, y: 1.5, w: 9, h: 0.4,
      fontSize: 15, bold: true, color: COLORS.primary, fontFace: "Calibri",
    });

    const headerOpts = { fontSize: 11, bold: true, color: COLORS.white, fontFace: "Calibri", align: "center", valign: "middle" };
    const cellOpts = { fontSize: 12, color: COLORS.dark, fontFace: "Calibri", align: "center", valign: "middle" };

    const rowsLP = [
      [
        { text: "Estación", options: headerOpts },
        { text: "MAE (°C)", options: headerOpts },
        { text: "RMSE (°C)", options: headerOpts },
        { text: "R²", options: headerOpts },
      ],
      ["PUNO", "0.08", "0.20", "0.994"].map(t => ({ text: t, options: cellOpts })),
      ["AZANGARO", "0.09", "0.20", "0.995"].map(t => ({ text: t, options: cellOpts })),
      ["LAMPA", "0.07", "0.16", "0.997"].map(t => ({ text: t, options: cellOpts })),
      ["CAPACHICA", "0.08", "0.18", "0.994"].map(t => ({ text: t, options: cellOpts })),
    ];

    s.addTable(rowsLP, {
      x: 0.5, y: 2.0, w: 9, colW: [2.25, 2.25, 2.25, 2.25],
      rowH: [0.4, 0.35, 0.35, 0.35, 0.35],
      border: { type: "solid", color: COLORS.primary, pt: 1 },
      autoPage: false,
    });

    s.addText("Corto Plazo (temperatura streaming, sensores IoT 2026):", {
      x: 0.5, y: 4.1, w: 9, h: 0.4,
      fontSize: 15, bold: true, color: COLORS.accent, fontFace: "Calibri",
    });

    const rowsCP = [
      [
        { text: "Grupo / Estación", options: headerOpts },
        { text: "MAE (°C)", options: headerOpts },
        { text: "R²", options: headerOpts },
        { text: "Estado", options: headerOpts },
      ],
      ["grupo_2 / LAMPA", "0.044", "0.992", "✅"].map(t => ({ text: t, options: cellOpts })),
      ["grupo_3 / PUNO", "6.355", "-1.41", "❌"].map(t => ({ text: t, options: { ...cellOpts, color: COLORS.red } })),
      ["grupo_4 / AZANGARO", "0.053", "0.993", "✅"].map(t => ({ text: t, options: cellOpts })),
    ];

    s.addTable(rowsCP, {
      x: 0.5, y: 4.6, w: 9, colW: [3, 2, 2, 2],
      rowH: [0.4, 0.35, 0.35, 0.35],
      border: { type: "solid", color: COLORS.accent, pt: 1 },
      autoPage: false,
    });

    s.addText("R² > 0.99 en todos los modelos con datos de calidad. El sensor grupo_3 (PUNO) está defectuoso (presión 186 hPa, temp constante -0.06°C).", {
      x: 0.5, y: 6.2, w: 9, h: 0.5,
      fontSize: 11, color: COLORS.gray, fontFace: "Calibri", italic: true, align: "center",
    });
  }

  // ── Slide 12: Dashboard ML ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "Dashboard — Predicciones ML", "Interfaz interactiva en Streamlit");

    s.addText("La pestaña 🤖 Predicciones ML del dashboard ofrece:", {
      x: 0.5, y: 1.5, w: 9, h: 0.4,
      fontSize: 15, color: COLORS.dark, fontFace: "Calibri",
    });

    const features = [
      ["📅 Largo Plazo", "Selecciona estación y días a predecir (1-14). Muestra gráfica histórico + predicción + métricas del modelo."],
      ["⚡ Corto Plazo", "Selecciona grupo y minutos a predecir (1-15). Muestra últimos datos reales + predicción."],
      ["💾 Persistencia", "Los resultados se guardan en st.session_state para no perderse con el auto-refresh (2s)."],
      ["📊 Métricas", "Tarjetas con MAE, RMSE y R² del modelo seleccionado, cargadas desde metrics.json."],
    ];

    features.forEach((f, i) => {
      const by = 2.1 + i * 1.0;
      s.addShape(pptx.ShapeType.roundRect, {
        x: 0.5, y: by, w: 9, h: 0.8,
        fill: { color: i % 2 === 0 ? COLORS.light : COLORS.white },
        line: { color: COLORS.primary, width: i === 0 ? 2 : 0.5 },
        rectRadius: 0.08,
      });
      s.addText(f[0], {
        x: 0.7, y: by + 0.05, w: 2.5, h: 0.7,
        fontSize: 14, bold: true, color: COLORS.primary, fontFace: "Calibri", valign: "middle",
      });
      s.addText(f[1], {
        x: 3.3, y: by + 0.05, w: 6.1, h: 0.7,
        fontSize: 11, color: COLORS.dark, fontFace: "Calibri", valign: "middle",
      });
    });

    s.addText("Integración directa con el dashboard existente — sin servicios adicionales.", {
      x: 0.5, y: 6.4, w: 9, h: 0.4,
      fontSize: 11, color: COLORS.gray, fontFace: "Calibri", align: "center", italic: true,
    });
  }

  // ── Slide 13: Limitaciones y Mejoras ──
  {
    const s = pptx.addSlide();
    slideHeader(s, "Limitaciones y Mejoras Futuras", "Estado actual y roadmap");

    s.addText("Limitaciones actuales:", {
      x: 0.5, y: 1.5, w: 4.3, h: 0.4,
      fontSize: 15, bold: true, color: COLORS.red, fontFace: "Calibri",
    });

    const limits = [
      "Solo 4 estaciones SENAMHI con >1000 registros",
      "Sensor grupo_3 defectuoso (sin ML corto plazo)",
      "Predicción arranca desde ~2012 (no 2026)",
      "Sin re-entrenamiento automático programado",
      "Solo XGBoost (sin ensemble con RF o LSTM)",
    ];
    addBullets(s, limits, { x: 0.5, y: 2.0, w: 4.3, fontSize: 11, lineH: 0.35, color: COLORS.dark });

    s.addText("Mejoras propuestas:", {
      x: 5.2, y: 1.5, w: 4.3, h: 0.4,
      fontSize: 15, bold: true, color: COLORS.green, fontFace: "Calibri",
    });

    const improves = [
      "Usar sensor IoT como punto de partida para 2026",
      "Re-entrenar con datos SENAMHI actualizados",
      "Cron job o botón en dashboard para re-train",
      "Ensemble: XGBoost + Random Forest + LSTM",
      "Spark MLlib para escalar a cientos de estaciones",
      "Intervalos de confianza por cuantiles",
    ];
    addBullets(s, improves, { x: 5.2, y: 2.0, w: 4.3, fontSize: 11, lineH: 0.35, color: COLORS.dark });

    s.addShape(pptx.ShapeType.roundRect, {
      x: 0.5, y: 5.0, w: 9, h: 1.5,
      fill: { color: COLORS.primary },
      rectRadius: 0.15,
    });
    s.addText("📈 Visión: Un sistema de monitoreo y predicción climática completo para toda la región Puno, con modelos actualizados automáticamente y alertas predictivas basadas en ML.", {
      x: 0.8, y: 5.2, w: 8.4, h: 1.1,
      fontSize: 14, color: COLORS.white, fontFace: "Calibri", align: "center", valign: "middle",
      italic: true,
    });
  }

  // ── Slide 14: Cierre ──
  {
    const s = pptx.addSlide();
    s.background = { color: COLORS.primary };
    s.addText("¡Gracias!", {
      x: 0, y: 2, w: W, h: 1,
      fontSize: 44, bold: true, color: COLORS.white, fontFace: "Calibri", align: "center",
    });
    s.addText("CliMePerú — Monitoreo Climático Inteligente", {
      x: 0, y: 3, w: W, h: 0.6,
      fontSize: 20, color: COLORS.white, fontFace: "Calibri", align: "center",
    });
    s.addText("https://github.com/IVANMAMANI2003/clime-peru", {
      x: 0, y: 4, w: W, h: 0.5,
      fontSize: 14, color: COLORS.white, fontFace: "Calibri", align: "center",
      italic: true,
    });
    s.addText("UPeU — Curso de Big Data 2026", {
      x: 0, y: 5, w: W, h: 0.4,
      fontSize: 12, color: COLORS.white, fontFace: "Calibri", align: "center",
    });
  }

  // ── Save ──
  const outputPath = path.join(__dirname, "CliMePeru_Presentacion.pptx");
  await pptx.writeFile({ fileName: outputPath });
  console.log(`✓ Presentación generada: ${outputPath}`);
  console.log(`  Tamaño: ${(fs.statSync(outputPath).size / 1024).toFixed(1)} KB`);
}

generate().catch(err => {
  console.error("Error:", err.message);
  console.error(err.stack);
  process.exit(1);
});
