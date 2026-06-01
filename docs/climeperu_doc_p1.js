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
    p("El sistema incluye métricas de operación exportadas a Prometheus, visualización en Grafana, un dashboard interactivo en Streamlit, almacenamiento persistente en PostgreSQL, y una suite completa de observabilidad con alertas configuradas para latencia, lag de consumidor y disponibilidad de servicios. Se presentan métricas de rendimiento medidas bajo diferentes configuraciones de trigger y watermark, así como una estimación de costos y escalabilidad."),
    p("Resultado: Pipeline streaming funcional con 14 servicios contenedorizados, 3 estaciones monitoreadas (grupo_2, grupo_3, grupo_4), más de 366,000 registros streaming almacenados en PostgreSQL, 1,073,151 registros históricos procesados en Parquet, ingesta en tiempo real desde Supabase con checkpoint recovery, detección de anomalías con umbrales configurables y visualización unificada en dashboard interactivo y Grafana."),
  ];
}

function chapter2() {
  return [
    h1("2. ARQUITECTURA DEL PIPELINE (KAPPA)"),
    hr(),
    h2("2.1 Visión General de la Arquitectura"),
    p("CimaPerú implementa una arquitectura Kappa, donde un único pipeline de streaming procesa tanto datos históricos (reprocesados) como datos en tiempo real. A diferencia de la arquitectura Lambda (que requiere dos rutas paralelas: batch y streaming), Kappa simplifica el mantenimiento al usar un solo motor de procesamiento."),
    p("La arquitectura se compone de las siguientes capas:"),
    bullet("Fuente de datos: Archivos históricos SENAMHI (.txt) y lecturas de sensores en tiempo real desde Supabase (3 tablas: grupo_2_air_quality, grupo_3_air_quality, grupo4_air_quality)."),
    bullet("Ingesta y mensajería: Apache Kafka 4.2.0 como backbone de mensajería distribuida, con 6 tópicos: clima-grupo_2/3/4 (datos crudos) y clima-grupo_2/3/4-anomalias (resultados de detección)."),
    bullet("Procesamiento streaming: 3 instancias de Apache Spark Structured Streaming 4.1.2 con modo micro-batch, trigger de 5 segundos y checkpoint recovery."),
    bullet("Almacenamiento: Datos históricos en Parquet particionado. Datos streaming en PostgreSQL (tablas sensor_data_grupo_2/3/4). Checkpoints en almacenamiento local con bind-mount."),
    bullet("Observabilidad: Prometheus para recolección de métricas, Grafana para dashboards, Kafka UI para gestión de tópicos."),
    bullet("Visualización: Dashboard Streamlit con análisis histórico y monitoreo en tiempo real con filtro por estación."),

    h2("2.2 Diagrama de Arquitectura"),
    p("El flujo de datos sigue la siguiente secuencia:", { bold: true }),
    code("SENAMHI (.txt) ──[ETL Batch]──> Parquet Histórico ──[Spark Stats]──> Kafka (clima-grupo_2/3/4)"),
    code("Supabase (3 tablas) ──[3 Bridges c/checkpoint]──> Kafka (clima-grupo_2/3/4) ──[3 Spark Streaming]──> Kafka (*-anomalias)"),
    code("Kafka ──[Spark Streaming]──> PostgreSQL (sensor_data_grupo_2/3/4) + Parquet Streaming"),
    code("Kafka ──[Kafka Exporter]──> Prometheus ──[Grafana]──> Kafka Overview Dashboard"),
code("Kafka ──[Value Exporter]──> Prometheus ──[Grafana]──> Sensor Variables Dashboard"),
    code("Dashboard Streamlit (8501) ──[consumer dashboard-consumer]──> Kafka (3 topics)"),
    ...spacer(1),
    p("Componentes del stack:", { bold: true }),
    bullet("Kafka 4.2.0 (KRaft mode, sin Zookeeper) — 1 nodo, 6 tópicos"),
    bullet("Kafka Exporter v1.9.0 — Exporta métricas de offsets, brokers y lag"),
    bullet("Prometheus — Scrapea cada 15s, jobs de recolección"),
    bullet("Grafana — Dashboard pre-configurado Kafka Overview, credenciales admin/admin"),
    bullet("Kafka UI — Interfaz web para gestión de tópicos en puerto 18085"),
    bullet("3 Supabase Bridges — Python con checkpoint persistente, polling incremental (id > last_id), catálogo sensor_catalog.json para inyectar ubicación (department/province/district)"),
    bullet("3 Spark Streaming Processors — PySpark Structured Streaming, uno por estación, con detección de anomalías y escritura a PostgreSQL"),
    bullet("PostgreSQL 15 — Almacenamiento persistente de datos streaming con INSERT ON CONFLICT DO NOTHING, tablas sensor_data_grupo_2/3/4"),
    bullet("Dashboard Streamlit — Consumer group 'dashboard-consumer', auto-refresh cada 2s, filtro por estación desde sensor_catalog.json"),
    bullet("Kafka Value Exporter — Exporter Prometheus que lee el último mensaje de cada topic Kafka y expone todas las variables del sensor como métricas gauge (temperatura, humedad, IAQ, eCO₂, VOC, anomalías, etc.) con labels station/department/province/district"),
    bullet("Jupyter Lab — Notebooks de análisis en puerto 8888"),

    h2("2.3 Supuestos Técnicos"),
    bullet("Ejecución en entorno Docker single-node (WSL2 backend en Windows)."),
    bullet("3 instancias Spark (grupo_2/3/4) en modo local[*] con 2 GB de memoria driver cada una."),
    bullet("Kafka opera en modo KRaft con 1 partición por tópico y factor de replicación 1."),
    bullet("Los datos históricos se asumen correctos y no requieren limpieza adicional."),
    bullet("La conexión a Supabase es vía Internet; se asume disponibilidad de red."),
    bullet("Los sensores generan lecturas cada ~1 minuto en horario continuo."),
    bullet("3 bridges independientes leen de Supabase (1 por tabla/sensor) y publican a topics Kafka dedicados."),
    bullet("PostgreSQL almacena datos streaming con columna processed_at generada en Python (datetime.utcnow())."),
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
        ["clima-grupo_2", "1", "1", "Recibe lecturas del sensor grupo_2 (PUNO/LAMPA/LAMPA) desde Supabase Bridge"],
        ["clima-grupo_3", "1", "1", "Recibe lecturas del sensor grupo_3 (PUNO/PUNO/PUNO) desde Supabase Bridge"],
        ["clima-grupo_4", "1", "1", "Recibe lecturas del sensor grupo_4 (PUNO/AZANGARO/AZANGARO) desde Supabase Bridge"],
        ["clima-grupo_2-anomalias", "1", "1", "Recibe anomalías detectadas por Spark para grupo_2"],
        ["clima-grupo_3-anomalias", "1", "1", "Recibe anomalías detectadas por Spark para grupo_3"],
        ["clima-grupo_4-anomalias", "1", "1", "Recibe anomalías detectadas por Spark para grupo_4"],
      ],
      [3200, 1500, 1500, 5000]
    ),

    h3("3.1.2 Comandos de Creación"),
    code("docker exec clime-kafka /opt/kafka/bin/kafka-topics.sh \\"),
    code("  --bootstrap-server kafka:9092 \\"),
    code("  --create --topic clima-puno --partitions 1 --replication-factor 1"),
    ...spacer(1),

    h2("3.2 Productores: Puentes Supabase → Kafka (3 instancias)"),
    p("Se implementan 3 bridges independientes (clime-bridge-grupo2, clime-bridge-grupo3, clime-bridge-grupo4), cada uno leyendo desde su tabla correspondiente en Supabase. El mapeo tabla-estación se define en artifacts/sensor_catalog.json:"),

    simpleTable(
      ["Servicio Bridge", "Tabla Supabase", "Topic Kafka", "Ubicación (Dept/Prov/Dist)"],
      [
        ["clime-bridge-grupo2", "grupo_2_air_quality", "clima-grupo_2", "PUNO/LAMPA/LAMPA"],
        ["clime-bridge-grupo3", "grupo_3_air_quality", "clima-grupo_3", "PUNO/PUNO/PUNO"],
        ["clime-bridge-grupo4", "grupo4_air_quality", "clima-grupo_4", "PUNO/AZANGARO/AZANGARO"],
      ],
      [2800, 2500, 2500, 3200]
    ),
    ...spacer(1),

    h3("3.2.1 Mecanismo de Ingesta"),
    bullet("Carga inicial: Al iniciar, obtiene todos los registros vía REST API paginada (lotes de 1000) y los publica en orden cronológico al tópico clima-grupo_X."),
    bullet("Polling incremental: Consulta registros nuevos (id > last_id) mediante polling periódico. El último id publicado se persiste en artifacts/bridge_checkpoints/{tabla}.json."),
    bullet("Checkpoint recovery: Al reiniciar, el bridge lee el checkpoint y reanuda desde last_id + 1, evitando duplicados."),

    h3("3.2.2 Inyección de Ubicación"),
    p("Cada bridge consulta el catálogo sensor_catalog.json para obtener department, province y district de su estación, e inyecta estos campos en cada mensaje Kafka antes de publicarlo."),
    code('{"last_id": 136028, "table": "grupo_2_air_quality", "topic": "clima-grupo_2"}'),

    h3("3.2.3 Configuración del Productor"),
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
    p("Cada mensaje en el tópico sigue la siguiente estructura JSON. Existen dos variantes:"),

    h3("3.4.1 Mensaje Raw (Bridge → Topic crudo)"),
    p("Mensaje publicado por el bridge al topic clima-grupo_X, con ubicación inyectada desde el catálogo:"),
    simpleTable(
      ["Campo", "Tipo", "Descripción", "Ejemplo"],
      [
        ["sensor_id", "string", "Identificador del sensor/estación", '"grupo_2"'],
        ["estacion", "string", "Nombre de estación (del catálogo)", '"grupo_2"'],
        ["department", "string", "Departamento (inyectado por bridge)", '"PUNO"'],
        ["province", "string", "Provincia (inyectado por bridge)", '"LAMPA"'],
        ["district", "string", "Distrito (inyectado por bridge)", '"LAMPA"'],
        ["temperatura", "float", "Temperatura ambiente en °C", "20.82"],
        ["humedad", "float", "Humedad relativa en %", "69.24"],
        ["presion", "float", "Presión atmosférica en hPa", "645.31"],
        ["altura", "float", "Altitud del sensor en msnm", "3647.39"],
        ["iaq", "float", "Índice de calidad del aire (0-500)", "0.0"],
        ["eco2", "float", "CO₂ equivalente en ppm", "400.0"],
        ["voc", "float", "Compuestos orgánicos volátiles en ppb", "0.0"],
        ["calidad_aire", "string", "Clasificación de calidad del aire", '"Bueno"'],
        ["ts", "string", "Timestamp normalizado (sin timezone)", '"2026-04-19T22:38:06"'],
        ["created_at", "string", "Timestamp ISO 8601 original", '"2026-04-19T22:38:06.053392"'],
        ["id", "int", "ID autoincremental en Supabase", "1"],
      ],
      [2200, 1500, 3500, 3800]
    ),
    ...spacer(1),

    h3("3.4.2 Mensaje con Anomalía (Spark → Topic anomalías)"),
    p("Mensaje enriquecido por Spark con campos de detección de anomalías, publicado al topic clima-grupo_X-anomalias:"),
    simpleTable(
      ["Campo", "Tipo", "Descripción", "Ejemplo"],
      [
        ["isAnomaly", "bool", "Indica si el registro es anómalo", "false"],
        ["anomalyScore", "float", "Z-score: (temp - promedio) / desviación", "-0.049"],
        ["anomalyType", "string", "Tipo: normal, crítica, alerta", '"normal"'],
        ["promedioHistorico", "float", "Promedio histórico de temperatura", "21.12"],
        ["desviacionEstandar", "float", "Desviación estándar histórica", "6.01"],
        ["processedAt", "string", "Timestamp de procesamiento Spark (ISO 8601)", '"2026-05-26T16:15:01.561Z"'],
      ],
      [2200, 1500, 3500, 3800]
    ),

    h2("3.5 Esquema de Particionado"),
    p("Actualmente cada tópico utiliza 1 partición, suficiente para el volumen de datos actual (1-2 mensajes/minuto por estación). Los 3 bridges publican en paralelo a sus topics dedicados. Para escalar a más estaciones, se propone:"),
    bullet("Aumentar a N particiones (N = número de consumidores en el grupo)."),
    bullet("Usar estacion como clave de partición para garantizar orden por estación."),
    bullet("Distribuir las particiones entre múltiples brokers en un cluster de 3+ nodos."),
    bullet("Cada estación tiene su propio topic, eliminando la necesidad de filtrado por clave de partición."),
    pageBreak(),
  ];
}

module.exports = { coverPage, chapter1, chapter2, chapter3, h1, h2, h3, p, pageBreak, hr, spacer, bullet, note, simpleTable, code, C, pMixed, num, cell, headerRow, dataRow };
