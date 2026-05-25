const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
  ShadingType, VerticalAlign, PageNumber, PageBreak, LevelFormat,
  TableOfContents, UnderlineType
} = require('docx');

const C = {
  primary: "1B3A6B", accent: "F97316", secondary: "2E75B6",
  dark: "1E293B", muted: "64748B", light: "F1F5F9", white: "FFFFFF",
  border: "CBD5E1", success: "16A34A", warning: "D97706", danger: "DC2626",
};

function hr() {
  return new Paragraph({
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: C.secondary, space: 1 } },
    spacing: { after: 200 }, children: []
  });
}

function pageBreak() {
  return new Paragraph({ children: [new PageBreak()] });
}

function h1(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_1, spacing: { before: 400, after: 200 },
    children: [new TextRun({ text, color: C.primary, size: 36, bold: true })] });
}

function h2(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_2, spacing: { before: 300, after: 160 },
    children: [new TextRun({ text, color: C.secondary, size: 28, bold: true })] });
}

function h3(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_3, spacing: { before: 200, after: 120 },
    children: [new TextRun({ text, color: C.dark, size: 24, bold: true })] });
}

function h4(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_4, spacing: { before: 120, after: 80 },
    children: [new TextRun({ text, color: C.muted, size: 22, bold: true })] });
}

function p(text, opts = {}) {
  return new Paragraph({
    alignment: opts.center ? AlignmentType.CENTER : AlignmentType.JUSTIFIED,
    spacing: { after: 120, line: 276 },
    children: [new TextRun({ text, size: opts.size || 22, color: opts.color || C.dark, bold: opts.bold || false, italics: opts.italic || false })]
  });
}

function pMixed(runs, opts = {}) {
  return new Paragraph({
    alignment: opts.center ? AlignmentType.CENTER : AlignmentType.JUSTIFIED,
    spacing: { after: 120, line: 276 },
    children: runs.map(r => new TextRun({ size: 22, color: C.dark, ...r }))
  });
}

function note(text) {
  return new Paragraph({
    spacing: { before: 80, after: 80 }, indent: { left: 720 },
    border: { left: { style: BorderStyle.SINGLE, size: 12, color: C.accent } },
    children: [new TextRun({ text, size: 20, color: C.muted, italics: true })]
  });
}

function code(text) {
  return new Paragraph({
    spacing: { before: 60, after: 60 }, indent: { left: 360 },
    border: { left: { style: BorderStyle.SINGLE, size: 6, color: C.border } },
    shading: { fill: "F8FAFC", type: ShadingType.CLEAR },
    children: [new TextRun({ text, size: 18, color: C.dark, font: "Consolas" })]
  });
}

function spacer(n = 1) {
  return Array.from({ length: n }, () => new Paragraph({ children: [new TextRun("")], spacing: { after: 80 } }));
}

function bullet(text, level = 0) {
  return new Paragraph({
    numbering: { reference: "bullets", level },
    spacing: { after: 60 },
    children: [new TextRun({ text, size: 22, color: C.dark })]
  });
}

function num(text, level = 0) {
  return new Paragraph({
    numbering: { reference: "numbers", level },
    spacing: { after: 60 },
    children: [new TextRun({ text, size: 22, color: C.dark })]
  });
}

const bdr = { style: BorderStyle.SINGLE, size: 1, color: C.border };
const borders = { top: bdr, bottom: bdr, left: bdr, right: bdr };

function cell(text, opts = {}) {
  return new TableCell({
    borders, verticalAlign: VerticalAlign.CENTER,
    width: opts.width ? { size: opts.width, type: WidthType.DXA } : undefined,
    shading: opts.shade ? { fill: opts.shade, type: ShadingType.CLEAR } : undefined,
    margins: { top: 60, bottom: 60, left: 100, right: 100 },
    children: [new Paragraph({
      alignment: opts.center ? AlignmentType.CENTER : AlignmentType.LEFT,
      children: [new TextRun({ text, size: opts.size || 20, color: opts.color || C.dark, bold: opts.bold || false })]
    })]
  });
}

function headerRow(cols, widths) {
  return new TableRow({
    tableHeader: true,
    children: cols.map((c, i) => cell(c, { width: widths[i], shade: C.primary, color: C.white, bold: true, center: true }))
  });
}

function dataRow(cols, widths, shade) {
  return new TableRow({
    children: cols.map((c, i) => cell(c, { width: widths[i], shade }))
  });
}

function simpleTable(headers, rows, widths) {
  const totalW = widths.reduce((a, b) => a + b, 0);
  return new Table({
    width: { size: totalW, type: WidthType.DXA },
    columnWidths: widths,
    rows: [
      headerRow(headers, widths),
      ...rows.map((r, i) => dataRow(r, widths, i % 2 === 0 ? C.white : C.light))
    ]
  });
}

function coverPage() {
  return [
    ...spacer(6),
    p("UNIVERSIDAD PERUANA UNIÓN", { center: true, bold: true, size: 32, color: C.primary }),
    p("Facultad de Ingeniería y Arquitectura", { center: true, size: 26, color: C.secondary }),
    p("Escuela Profesional de Ingeniería de Sistemas", { center: true, size: 24, color: C.muted }),
    ...spacer(4),
    hr(),
    p("CimaPerú", { center: true, bold: true, size: 72, color: C.accent }),
    p("Sistema de Monitoreo Climático Inteligente", { center: true, size: 28, color: C.secondary, italic: true }),
    hr(),
    ...spacer(3),
    p("ENTREGABLE UNIDAD 2", { center: true, bold: true, size: 36, color: C.primary }),
    p("Pipeline Streaming en Spark para BI/ML a Escala y en Tiempo Real", { center: true, size: 24, color: C.muted }),
    ...spacer(4),
    simpleTable(
      ["Campo", "Detalle"],
      [
        ["Curso", "Big Data"],
        ["Unidad", "2"],
        ["Estudiante / Equipo", "Ivan Yomar Mamani Merma"],
        ["Fecha", "25/05/2026"],
        ["Docente", "Abel Angel Sullon Macalupu"],
        ["Institución", "Universidad Peruana Unión (UPeU)"],
        ["Sede", "Juliaca, Puno, Perú"],
        ["Producto", "Pipeline streaming Kafka + Spark con métricas, observabilidad y documentación operativa"],
      ],
      [3200, 6000]
    ),
    pageBreak(),
  ];
}

function chapter1() {
  return [
    h1("1. RESUMEN EJECUTIVO"),
    hr(),
    p("El presente documento describe la implementación de un pipeline de streaming en tiempo real para el monitoreo climático inteligente, denominado CimaPerú. El sistema integra datos de estaciones meteorológicas del SENAMHI (Servicio Nacional de Meteorología e Hidrología del Perú) procesados por un pipeline ETL batch, junto con lecturas en tiempo real provenientes de sensores de calidad de aire alojados en Supabase, para construir un flujo continuo de detección de anomalías climáticas."),
    p("El pipeline sigue una arquitectura Kappa basada en Apache Kafka como sistema de ingesta y mensajería distribuida, y Apache Spark Structured Streaming como motor de procesamiento en tiempo real. Los datos históricos de 60 estaciones meteorológicas (más de 1 millón de registros) son almacenados en formato Parquet particionado para consultas analíticas eficientes. Los datos en tiempo real fluyen desde Supabase hacia Kafka mediante un puente dedicado, y Spark procesa el stream detectando anomalías basadas en promedios históricos y desviaciones estándar."),
    p("El sistema incluye métricas de operación exportadas a Prometheus, visualización en Grafana, un dashboard interactivo en Streamlit, y una suite completa de observabilidad con alertas configuradas para latencia, lag de consumidor y disponibilidad de servicios. Se presentan métricas de rendimiento medidas bajo diferentes configuraciones de trigger y watermark, así como una estimación de costos y escalabilidad."),
    p("Resultado: Pipeline streaming funcional con 9 servicios contenedorizados, 1,073,151 registros históricos procesados, ingesta en tiempo real desde Supabase, detección de anomalías con umbrales configurables y visualización unificada en dashboard interactivo y Grafana."),
  ];
}

function chapter2() {
  return [
    h1("2. ARQUITECTURA DEL PIPELINE (KAPPA)"),
    hr(),
    h2("2.1 Visión General de la Arquitectura"),
    p("CimaPerú implementa una arquitectura Kappa, donde un único pipeline de streaming procesa tanto datos históricos (reprocesados) como datos en tiempo real. A diferencia de la arquitectura Lambda (que requiere dos rutas paralelas: batch y streaming), Kappa simplifica el mantenimiento al usar un solo motor de procesamiento."),
    p("La arquitectura se compone de las siguientes capas:"),
    bullet("Fuente de datos: Archivos históricos SENAMHI (.txt) y lecturas de sensores en tiempo real desde Supabase (tabla grupo_3_air_quality)."),
    bullet("Ingesta y mensajería: Apache Kafka 4.2.0 como backbone de mensajería distribuida, con dos tópicos: clima-puno (datos crudos de sensores) y clima-anomalias (resultados de detección)."),
    bullet("Procesamiento streaming: Apache Spark Structured Reading 4.1.2 con modo micro-batch, ventanas de 1 minuto y watermark de 30 segundos."),
    bullet("Almacenamiento: Datos históricos en Parquet particionado por departamento, provincia, distrito y año. Checkpoints en almacenamiento local."),
    bullet("Observabilidad: Prometheus para recolección de métricas, Grafana para dashboards, Kafka UI para gestión de tópicos."),
    bullet("Visualización: Dashboard Streamlit con análisis histórico y monitoreo en tiempo real."),

    h2("2.2 Diagrama de Arquitectura"),
    p("El flujo de datos sigue la siguiente secuencia:", { bold: true }),
    code("SENAMHI (.txt) ──[ETL Batch]──> Parquet Histórico ──[Spark Streaming]──> Kafka (clima-puno)"),
    code("Supabase (DB) ──[Bridge/WebSocket]──> Kafka (clima-puno) ──[Spark Streaming]──> Kafka (clima-anomalias)"),
    code("Kafka (clima-puno) ──[Kafka Exporter]──> Prometheus ──[Grafana]──> Dashboard"),
    code("Kafka (clima-anomalias) ──[Spark Filter]──> Dashboard ─── Dashboard Streamlit (8501)"),
    ...spacer(1),
    p("Componentes del stack:", { bold: true }),
    bullet("Kafka 4.2.0 (KRaft mode, sin Zookeeper) — 1 nodo, 2 tópicos"),
    bullet("Kafka Exporter v1.9.0 — Exporta métricas de offsets, brokers y lag"),
    bullet("Prometheus — Scrapea cada 15s, 4 jobs de recolección"),
    bullet("Grafana — Dashboard pre-configurado Kafka Overview, credenciales admin/admin"),
    bullet("Kafka UI — Interfaz web para gestión de tópicos en puerto 18085"),
    bullet("Supabase Bridge — Python asyncio, WebSocket Realtime + polling REST cada 30s"),
    bullet("Spark Streaming Processor — PySpark Structured Streaming con Kafka connector"),
    bullet("Dashboard Streamlit — Visualización con auto-refresh cada 2s"),
    bullet("Jupyter Lab — Notebooks de análisis en puerto 8888"),

    h2("2.3 Supuestos Técnicos"),
    bullet("Ejecución en entorno Docker single-node (WSL2 backend en Windows)."),
    bullet("Spark opera en modo local[*] con 2 GB de memoria driver."),
    bullet("Kafka opera en modo KRaft con 1 partición por tópico y factor de replicación 1."),
    bullet("Los datos históricos se asumen correctos y no requieren limpieza adicional."),
    bullet("La conexión a Supabase es vía Internet; se asume disponibilidad de red."),
    bullet("Los sensores generan lecturas cada ~1 minuto en horario continuo."),
  ];
}

function chapter3() {
  return [
    h1("3. INGESTA EN TIEMPO REAL CON KAFKA"),
    hr(),

    h2("3.1 Configuración de Kafka"),
    p("Se utiliza Apache Kafka 4.2.0 en modo KRaft (sin Zookeeper), ejecutado como contenedor Docker. La configuración permite auto-creación de tópicos y retención de mensajes por 168 horas (7 días). El broker está expuesto internamente en kafka:9092 y externamente en localhost:19092."),

    h3("3.1.1 Tópicos Kafka"),
    simpleTable(
      ["Nombre del Tópico", "Particiones", "Replicación", "Propósito"],
      [
        ["clima-puno", "1", "1", "Recibe lecturas de sensores desde Supabase Bridge y productores externos"],
        ["clima-anomalias", "1", "1", "Recibe solo registros clasificados como anomalías (filtro es_anomalia == 'SI')"],
      ],
      [2500, 1800, 1800, 5000]
    ),

    h3("3.1.2 Comandos de Creación"),
    code("docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh \\"),
    code("  --bootstrap-server kafka:9092 \\"),
    code("  --create --topic clima-puno --partitions 1 --replication-factor 1"),
    ...spacer(1),

    h2("3.2 Productor: Puente Supabase → Kafka"),
    p("El servicio supabase-bridge (clime-supabase-bridge) actúa como productor Kafka, leyendo datos desde la tabla grupo_3_air_quality de Supabase. Utiliza dos mecanismos de ingesta:"),

    h3("3.2.1 Mecanismo de Ingesta"),
    bullet("Carga inicial: Al iniciar, obtiene los 500 registros más recientes vía REST API y los publica en orden cronológico al tópico clima-puno."),
    bullet("Suscripción Realtime: Utiliza el cliente asíncrono de Supabase (WebSocket) para recibir eventos INSERT en tiempo real."),
    bullet("Polling de respaldo: Cada 30 segundos consulta registros nuevos (id > last_id) como mecanismo de tolerancia a fallos."),

    h3("3.2.2 Configuración del Productor"),
    code("KafkaProducer(bootstrap_servers='clime-kafka:9092', acks='all', retries=5,"),
    code("  value_serializer=lambda v: json.dumps(v).encode('utf-8'))"),

    h2("3.3 Consumidor: Spark Structured Streaming"),
    p("El Spark Streaming Processor se suscribe al tópico clima-puno como consumidor, usando la integración nativa spark-sql-kafka-0-10. Configuración:"),
    code("df = spark.readStream.format('kafka')"),
    code("  .option('kafka.bootstrap.servers', 'kafka:9092')"),
    code("  .option('subscribe', 'clima-puno')"),
    code("  .option('startingOffsets', 'earliest')"),
    code("  .option('failOnDataLoss', 'false')"),

    h2("3.4 Contrato de Evento"),
    p("Cada mensaje en el tópico sigue la siguiente estructura JSON:"),
    simpleTable(
      ["Campo", "Tipo", "Descripción", "Ejemplo"],
      [
        ["sensor_id", "string", "Identificador único del sensor/estación", '"estacion_001"'],
        ["temperatura", "float", "Temperatura ambiente en °C", "18.5"],
        ["humedad", "float", "Humedad relativa en %", "65.2"],
        ["presion", "float", "Presión atmosférica en hPa", "1013.25"],
        ["altura", "float/null", "Altitud del sensor en msnm", "3820.0"],
        ["iaq", "float", "Índice de calidad del aire (0-500)", "42.0"],
        ["eco2", "float", "CO₂ equivalente en ppm", "450.0"],
        ["voc", "float", "Compuestos orgánicos volátiles en ppb", "0.15"],
        ["calidad_aire", "string", "Clasificación de calidad del aire", '"Bueno"'],
        ["created_at", "string", "Timestamp ISO 8601 del evento", '"2026-05-25T12:00:00Z"'],
        ["id", "int", "ID autoincremental en Supabase", "132255"],
      ],
      [2200, 1500, 3500, 2200]
    ),
    ...spacer(1),
    p("Ejemplo de mensaje completo:", { bold: true }),
    code('{"sensor_id":"estacion_001","temperatura":18.5,"humedad":65.2,"presion":1013.25,'),
    code(' "altura":3820.0,"iaq":42.0,"eco2":450.0,"voc":0.15,"calidad_aire":"Bueno",'),
    code(' "created_at":"2026-05-25T12:00:00Z","id":132255}'),

    h2("3.5 Esquema de Particionado"),
    p("Actualmente cada tópico utiliza 1 partición, suficiente para el volumen de datos actual (~1 msg/minuto). Para escalar a múltiples sensores, se propone:"),
    bullet("Aumentar a N particiones (N = número de consumidores en el grupo)."),
    bullet("Usar sensor_id como clave de partición para garantizar orden por sensor."),
    bullet("Distribuir las particiones entre múltiples brokers en un cluster de 3+ nodos."),
    pageBreak(),
  ];
}

module.exports = { coverPage, chapter1, chapter2, chapter3, h1, h2, h3, p, pageBreak, hr, spacer, bullet, note, simpleTable, code, C, pMixed, num, cell, headerRow, dataRow };
