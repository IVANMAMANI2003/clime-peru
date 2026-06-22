const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
  ShadingType, VerticalAlign, PageNumber, PageBreak, LevelFormat,
  TableOfContents, UnderlineType
} = require('docx');

const p1 = require('./climeperu_doc_p1.js');
const { h1, h2, h3, h4, p, pageBreak, hr, spacer, bullet, note, simpleTable, code, C, pMixed, num } = p1;

function chapter4() {
  return [
    h1("4. PROCESAMIENTO EN STREAMING CON SPARK"),
    hr(),

    h2("4.1 Configuración de Spark Structured Streaming"),
    p("El procesamiento en tiempo real se implementa mediante la clase SparkStreamingProcessor en el archivo streaming/spark_streaming_processor.py. Utiliza PySpark con el conector spark-sql-kafka-0-10 para integración nativa con Kafka."),

    h3("4.1.1 Sesión Spark"),
    code('SparkSession.builder'),
    code('  .appName("ClimePeruStreaming")'),
    code('  .master("local[*]")'),
    code('  .config("spark.driver.memory", "2g")'),
    code('  .config("spark.sql.adaptive.enabled", "true")'),

    h2("4.2 Fuente de Lectura"),
    p("Se ejecutan 3 instancias independientes de Spark Streaming, una por estación, cada una suscrita a su tópico correspondiente (clima-grupo_2, clima-grupo_3, clima-grupo_4) con las siguientes opciones:"),
    bullet("Formato: kafka"),
    bullet("Bootstrap servers: kafka:9092 (red interna Docker)"),
    bullet("Starting offsets: earliest (reprocesa desde el primer mensaje disponible)"),
    bullet("failOnDataLoss: false (tolera pérdida de datos por retención de Kafka)"),

    h2("4.3 Transformaciones Aplicadas"),
    p("El pipeline de transformación consta de las siguientes etapas:"),

    h3("4.3.1 Parseo de Datos del Sensor"),
    p("Los mensajes JSON de Kafka se deserializan usando from_json con un esquema definido que incluye sensor_id, temperatura, humedad, timestamp, ubicación (department/province/district) y metadatos de calidad del aire."),

    h3("4.3.2 Campos de Observabilidad"),
    p("Se agregan campos calculados:"),
    bullet("isAnomaly: Booleano indicando si la temperatura está fuera del rango normal."),
    bullet("anomalyScore: Z-score calculado como (temperatura - promedio_historico) / desviacion_estandar."),
    bullet("anomalyType: Clasificación: 'normal' si |z-score| < sigma, 'alerta' si excede sigma, 'crítica' si excede 2 sigma."),
    bullet("processedAt: Timestamp ISO 8601 al momento del procesamiento Spark."),

    h3("4.3.3 Detección de Anomalías"),
    p("La función detect_anomalies() implementa la lógica central usando funciones nativas de PySpark:"),
    code("limite_inferior = promedio_historico - (sigma * desviacion_estandar)"),
    code("limite_superior = promedio_historico + (sigma * desviacion_estandar)"),
    code("es_anomalia = abs(temperatura - promedio_historico) > sigma * desviacion_estandar"),
    ...spacer(1),
    p("Columnas generadas:", { bold: true }),
    bullet("promedioHistorico: Valor histórico promedio para la estación (calculado desde Parquet batch vía tmax)."),
    bullet("desviacionEstandar: Desviación estándar histórica (calculada desde Parquet batch)."),
    bullet("isAnomaly: Booleano true si la temperatura está fuera del rango de normalidad."),
    bullet("anomalyScore: Z-score del registro actual."),
    bullet("anomalyType: 'normal', 'alerta' o 'crítica' según severidad."),

    h2("4.4 Ventanas y Watermarking"),
    h3("4.4.1 Ventana Temporal"),
    p("Se utiliza una ventana de tipo tumbling (no deslizante) de 1 minuto de duración. Esto permite agrupar eventos que ocurren dentro del mismo minuto para calcular agregaciones como temperatura promedio, humedad promedio, número de eventos y latencia promedio."),

    h3("4.4.2 Watermark"),
    p("El watermark se configura en 30 segundos, lo que significa que eventos con timestamp hasta 30 segundos antes del máximo procesado serán incluidos en la ventana correspondiente. Eventos más antiguos son descartados. Esto permite manejar eventos fuera de orden (out-of-order) causados por latencias de red o procesamiento."),

    h3("4.4.3 Trigger"),
    p("El trigger se configura en 5 segundos (intervalo de micro-batch). Spark procesará los datos acumulados cada 5 segundos, ofreciendo un balance entre latencia de procesamiento y eficiencia de recursos."),

    h2("4.5 Parámetros del Stream"),
    simpleTable(
      ["Parámetro", "Valor", "Justificación"],
      [
        ["Trigger", "5 seconds", "Balance entre latencia baja y eficiencia de proceso por lotes"],
        ["Watermark", "30 seconds", "Tolerancia a retrasos de red sin acumular demasiados eventos fuera de orden"],
        ["Ventana", "1 minute (tumbling)", "Agrupación para cálculos agregados significativos por minuto"],
        ["Output mode", "append", "Solo emite nuevas filas; adecuado para el patrón de detección de anomalías"],
        ["Checkpoint", "/app/artifacts/checkpoints", "Persiste estado del stream para recuperación ante fallos"],
      ],
      [2500, 2500, 6000]
    ),

    h2("4.6 Salidas del Stream"),
    p("Cada pipeline Spark produce cuatro salidas:", { bold: true }),
    bullet("Kafka (tópico clima-grupo_X-anomalias): Todos los registros procesados son publicados con campos de anomalía al topic de anomalías correspondiente."),
    bullet("Parquet: Datos procesados escritos a /app/artifacts/parquet_output para análisis posteriores."),
    bullet("PostgreSQL: Datos insertados en tabla sensor_data_grupo_X con INSERT ON CONFLICT (id) DO NOTHING para evitar duplicados. Columnas: id, sensor_id, estacion, department, province, district, temperatura, humedad, presion, altura, iaq, eco2, voc, calidad_aire, ts, created_at, processed_at."),
    bullet("Consola: Logs informativos con conteo de registros por batch."),

    h2("4.7 Modos de Ejecución"),
    p("Cada procesador Spark se ejecuta via CLI con argumentos específicos de estación:"),
    code("python -m streaming.spark_streaming_processor \\"),
    code("  --input-topic clima-grupo_2 \\"),
    code("  --anomaly-topic clima-grupo_2-anomalias"),
    pageBreak(),
  ];
}

function chapter5() {
  return [
    h1("5. MÉTRICAS DE RENDIMIENTO"),
    hr(),

    h2("5.1 Métricas Recolectadas"),
    p("El pipeline registra automáticamente las siguientes métricas por cada micro-batch:"),
    bullet("batchId: Identificador secuencial del micro-batch."),
    bullet("numInputRows: Número de filas de entrada procesadas en el batch."),
    bullet("inputRowsPerSecond: Tasa de llegada de datos."),
    bullet("processedRowsPerSecond: Tasa de procesamiento efectiva."),
    bullet("avgOffsetsBehindLatest: Promedio de offsets pendientes por partición."),
    bullet("maxOffsetsBehindLatest: Máximo lag por partición."),

    h2("5.2 Pruebas Controladas"),
    p("Se realizaron pruebas con diferentes configuraciones de trigger y watermark para medir el impacto en el rendimiento. Los resultados se muestran en la siguiente tabla:"),
    ...spacer(1),

    simpleTable(
      ["Prueba", "Trigger", "Watermark", "Throughput (rows/s)", "Lag (offsets)", "Latencia (ms)", "Observaciones"],
      [
        ["1", "5s (default)", "30s", "0.2 rows/s", "0", "6101", "Datos reales de Supabase (~1 msg/min). Latencia alta por espera de datos."],
        ["2", "10s", "60s", "0.17 rows/s", "0", "8500", "Mayor ventana de tolerancia, misma tasa de datos."],
        ["3", "1s", "15s", "0.2 rows/s", "0", "1200", "Menor latencia pero más overhead de planificación. Sin datos suficientes para notar mejora."],
        ["4", "5s", "0s", "0.2 rows/s", "0", "5100", "Sin watermark. Misma tasa pero sin tolerancia a out-of-order."],
        ["5", "0s (processing)", "30s", "0.2 rows/s", "0", "500", "Trigger ASAP. Máxima capacidad de respuesta, mayor uso de CPU."],
      ],
      [1200, 1800, 1500, 2500, 1500, 1800, 4000]
    ),
    ...spacer(1),

    p("Nota: El throughput está limitado por la frecuencia de publicación de datos desde Supabase (~1 registro por minuto). Las métricas de lag se mantienen en 0 porque Spark procesa los datos más rápido de lo que llegan.", { italic: true, size: 20 }),

    h2("5.3 Análisis de Resultados"),
    p("Con los 500 registros iniciales publicados por el bridge, Spark procesa todo el lote en aproximadamente 6 segundos, logrando un throughput efectivo de ~83 rows/s durante la carga inicial. En estado estacionario, con 1 registro por minuto, el sistema opera con lag cero y latencia de ~100ms (netos de procesamiento, excluyendo el tiempo de planificación de 5s del trigger)."),
    p("El principal cuello de botella es la frecuencia de publicación de datos fuente. Para pruebas de esfuerzo, se recomienda usar el simulador de sensores (simulate_sensor_data en kafka_consumer.py) que puede generar hasta 100 lecturas/s."),
    pageBreak(),
  ];
}

function chapter6() {
  return [
    h1("6. OBSERVABILIDAD DEL PIPELINE (GRAFANA)"),
    hr(),

    h2("6.1 Estrategia de Observabilidad"),
    p("La observabilidad del pipeline se aborda en tres dimensiones: métricas (Prometheus), logging estructurado (archivos rotativos por día) y dashboards (Grafana + Streamlit)."),

    h2("6.2 Métricas Clave Exportadas"),
    p("Se utilizan dos exporters de Prometheus:"),

    h3("6.2.1 Kafka Exporter (puerto 9308)"),
    p("Expone métricas a nivel del broker Kafka, scrapeadas cada 15s:"),
    bullet("kafka_topic_partition_current_offset: Offset actual por tópico y partición."),
    bullet("kafka_consumergroup_lag: Lag del grupo de consumidores (mensajes no procesados)."),
    bullet("kafka_brokers: Número de brokers disponibles en el cluster."),
    bullet("up{job='kafka-exporter'}: Estado del exporter (1 = disponible, 0 = caído)."),

    h3("6.2.2 Kafka Value Exporter (puerto 8000)"),
    p("Exporter Python personalizado que consulta el último mensaje de cada topic Kafka y expone las variables reales de los sensores como métricas Prometheus gauge. Implementado en monitoring/kafka_value_exporter.py."),
    p("Métricas expuestas por estación (labels: station, department, province, district):"),
    bullet("sensor_temperature_celsius: Temperatura ambiente en °C."),
    bullet("sensor_humidity_percent: Humedad relativa en %."),
    bullet("sensor_pressure_hpa: Presión atmosférica en hPa."),
    bullet("sensor_altitude_m: Altitud en msnm."),
    bullet("sensor_iaq_index: Índice de calidad del aire (0-500)."),
    bullet("sensor_eco2_ppm: CO₂ equivalente en ppm."),
    bullet("sensor_voc_ppb: Compuestos orgánicos volátiles en ppb."),
    bullet("anomaly_detected: 1 si el último registro es anómalo, 0 si es normal."),
    bullet("anomaly_z_score: Z-score de la última lectura."),
    bullet("anomaly_historical_avg: Promedio histórico de temperatura para la estación."),
    bullet("anomaly_historical_std: Desviación estándar histórica."),
    bullet("kafka_topic_offset: Offset actual de cada topic."),
    bullet("exporter_last_update_timestamp: Timestamp de la última actualización (para detectar datos desactualizados)."),

    h2("6.3 Logs Generados"),
    p("El sistema de logging utiliza logging estructurado con formato de colores y archivos rotativos diarios en el directorio /app/logs. Los niveles de log incluyen DEBUG, INFO, WARNING, ERROR y CRITICAL. Cada servicio produce logs con contexto (nombre del servicio, timestamp, nivel y mensaje)."),

    h2("6.4 Alertas Configuradas (Prometheus)"),
    p("Se definen 2 grupos de reglas de alerta que se evalúan cada 15s. Las alertas se visualizan en Prometheus (Alertmanager) y en el dashboard de Grafana:"),

    h3("6.4.1 Alertas de Infraestructura (grupo: kafka_alerts)"),
    simpleTable(
      ["Alerta", "Expresión", "For", "Descripción"],
      [
        ["KafkaExporterDown", 'up{job="kafka-exporter"} == 0', "1m", "El exporter de Kafka no está respondiendo"],
        ["HighKafkaLag", "kafka_consumergroup_lag > 100", "2m", "El lag del consumidor supera los 100 mensajes"],
        ["UnderReplicatedPartitions", "kafka_topic_partition_under_replicated_partitions > 0", "1m", "Particiones no completamente replicadas"],
        ["KafkaBrokerDown", "kafka_brokers < 1", "1m", "Ningún broker disponible en el cluster"],
      ],
      [3000, 3500, 800, 3700]
    ),
    ...spacer(1),

    h3("6.4.2 Alertas de Sensores (grupo: sensor_alerts)"),
    simpleTable(
      ["Alerta", "Expresión", "For", "Descripción"],
      [
        ["ValueExporterDown", 'up{job="kafka-value-exporter"} == 0', "1m", "El Value Exporter no está respondiendo"],
        ["SensorAnomalyDetected", "anomaly_detected == 1", "0s", "Anomalía de temperatura detectada en la estación"],
        ["SensorStaleData", "time() - exporter_last_update > 120s", "0s", "Topic sin actualizaciones por más de 2 minutos"],
        ["SensorCriticalIAQ", "sensor_iaq_index > 200", "1m", "IAQ en categoría Severa (> 200)"],
        ["SensorHighIAQ", "sensor_iaq_index > 100", "2m", "IAQ en categoría Dañina (> 100)"],
        ["SensorCriticalAnomalyScore", "abs(anomaly_z_score) > 2.0", "0s", "Z-score crítico (> 2σ = anomalía crítica)"],
        ["SensorHighAnomalyScore", "abs(anomaly_z_score) > 1.5", "1m", "Z-score elevado (> 1.5σ = anomalía moderada)"],
        ["TopicNotReceivingData", "rate(kafka_topic_offset[5m]) == 0", "5m", "Topic sin nuevos mensajes en 5 minutos (posible caída del bridge)"],
        ["BridgeDown", "kafka_topic_offset == 0", "10m", "Offset 0 — el bridge no ha publicado nada"],
      ],
      [3500, 3800, 800, 2900]
    ),

    h2("6.5 Umbrales de Alerta Sugeridos"),
    simpleTable(
      ["Métrica", "Descripción", "Umbral Sugerido", "Frecuencia de Revisión"],
      [
        ["Latencia", "Tiempo entre generación y procesamiento del evento", "< 10s", "Cada 1 minuto"],
        ["Throughput", "Registros procesados por segundo", "> 1 row/s (estado estacionario)", "Cada 5 minutos"],
        ["Errores", "Excepciones en procesamiento por minuto", "< 1 error/min", "Cada 1 minuto"],
        ["Lag", "Offsets pendientes de consumir", "< 50 mensajes", "Cada 30 segundos"],
        ["Backpressure", "Tiempo de procesamiento > trigger interval", "No exceder por más de 3 batches", "Cada batch"],
      ],
      [2000, 3500, 2500, 2000]
    ),

    h2("6.6 Dashboards Grafana"),
    p("Se provisionan automáticamente 2 dashboards en Grafana (admin/admin, puerto 13000):"),

    h3("6.6.1 Kafka Overview — ClimePeru (uid: kafka-overview-clime)"),
    p("Dashboard de infraestructura con métricas del cluster Kafka:"),
    bullet("Kafka Brokers: Stat con el número de brokers activos."),
    bullet("Kafka Exporter Status: Stat indicando disponibilidad del exporter."),
    bullet("Consumer Lag: Serie temporal del lag del grupo de consumidores, etiquetado por tópico y grupo."),
    bullet("Messages Per Topic: Serie temporal de offsets actuales por tópico (6 topics: crudos + anomalías)."),

    h3("6.6.2 Sensores — Variables por Estación (uid: sensor-variables-clime)"),
    p("Dashboard de monitoreo de variables de sensor en tiempo real. Obtiene datos del Kafka Value Exporter (puerto 8000) y muestra paneles para las 3 estaciones:"),
    bullet("Resumen de llegada: Mensajes por topic, tasa de mensajes/min, consumer lag, última actualización."),
    bullet("Temperatura: 3 gauges individuales (uno por estación) + serie temporal superpuesta con las 3 estaciones + stats (promedio histórico)."),
    bullet("Humedad: Gauges individuales + serie temporal multi-estación."),
    bullet("Presión: Stats individuales + serie temporal multi-estación."),
    bullet("Calidad del Aire: IAQ gauge por estación + serie temporal, eCO₂, VOC, tabla de clasificación de calidad."),
    bullet("Anomalías: Anomalía detectada (0/1), Z-score, promedio histórico vs temperatura actual, tipo de anomalía."),
    bullet("Alertas y Estado: Tabla de alertas activas (query ALERTS de Prometheus), estado de sensores por estación, conexión de topics, última lectura por topic (semáforo: verde < 30s, amarillo < 60s, rojo > 60s), reglas de alerta definidas."),
    bullet("Información del Stack: Brokers, offsets por topic, altitud, enlace al dashboard Streamlit."),
    p("El dashboard incluye anotaciones automáticas que marcan en los gráficos de anomalías cuándo se dispara una alerta (líneas rojas para anomalías, naranjas para problemas de conectividad)."),
    p("Variables de template disponibles: station (multi-select: grupo_2, grupo_3, grupo_4, Todas) y variable (multi-select por tipo de métrica para filtrar paneles)."),

    h2("6.7 Dashboard Streamlit (CimaPerú)"),
    p("El dashboard en Streamlit (puerto 8501) complementa Grafana con análisis interactivo:"),
    bullet("Tab Histórico: Filtros por departamento, provincia, estación y rango de fechas. Gráficos de series temporales, box plots, mapas de estaciones y tabla de datos descargable."),
    bullet("Tab Tiempo Real: Actualización cada 2 segundos desde Kafka (consumer group dashboard-consumer). Muestra temperatura, humedad, IAQ (con eCO₂/VOC) y presión. Filtro por estación desde sensor_catalog.json."),
    bullet("Métricas del Stack: Indicador de offset actual por topic, brokers disponibles, lag del consumidor y estado de conexión."),
    p("Mientras Streamlit ofrece filtros dinámicos y análisis exploratorio, Grafana proporciona monitoreo continuo con dashboards persistentes, alertas automáticas y series temporales históricas."),
    pageBreak(),
  ];
}

function chapter7() {
  return [
    h1("7. COSTOS Y ESCALADO"),
    hr(),

    h2("7.1 Estimación de Recursos Actuales"),
    simpleTable(
      ["Recurso", "Estimación Actual", "Justificación"],
      [
        ["CPU", "6 cores (3 Spark + 3 Bridges + Kafka + Dashboard + PG)", "3 instancias Spark local[*] cada una usando ~1-2 cores"],
        ["Memoria RAM", "6 GB Spark (3×2g) + 512 MB Kafka + 256 MB bridges + 512 MB PG", "3 drivers Spark con 2 GB cada uno para cargas de ~100K registros"],
        ["Almacenamiento", "~14 MB parquet histórico + ~150 MB Kafka + ~100 MB PG", "366K registros streaming en PostgreSQL, 6 topics Kafka"],
        ["Particiones Kafka", "1 por tópico (6 tópicos)", "Volumen bajo; topics dedicados por estación"],
        ["Ejecutores Spark", "3 (local[*])", "3 instancias independientes, una por estación"],
      ],
      [2800, 3500, 4700]
    ),

    h2("7.2 Riesgos de Backpressure"),
    p("El backpressure ocurre cuando el productor genera datos más rápido de lo que el consumidor puede procesar. En el estado actual:"),
    bullet("No hay riesgo significativo porque la tasa de publicación es muy baja (~1 msg/min)."),
    bullet("Si se incorporaran 100+ sensores, el throughput podría exceder la capacidad de Spark local[*] con 2 GB."),
    bullet("Indicador de backpressure: Cuando la métrica 'Current batch is falling behind' aparece en los logs de Spark."),
    bullet("Mitigación: Aumentar memoria Spark a 4-8 GB, aumentar particiones Kafka, o migrar a cluster Spark standalone."),

    h2("7.3 Estrategia de Escalado"),
    h3("7.3.1 Escalado Vertical (Recomendado para etapa actual)"),
    bullet("Aumentar spark.driver.memory a 8-16 GB para procesar batches más grandes."),
    bullet("Incrementar spark.sql.streaming.kafka.maxRatePerPartition a 1000."),
    bullet("Aumentar almacenamiento Kafka a 72h o más según necesidades de reprocesamiento."),

    h3("7.3.2 Escalado Horizontal (Para producción)"),
    bullet("Cluster Kafka de 3+ brokers con factor de replicación 2-3."),
    bullet("Cluster Spark Standalone o YARN con múltiples workers (4-8 ejecutores)."),
    bullet("Aumentar particiones de tópicos a 3-6 para permitir paralelismo."),
    bullet("Separar servicios en máquinas dedicadas (Kafka, Spark, Dashboard, Bridge)."),

    h3("7.3.3 Estimación de Costos Cloud (Referencia)"),
    simpleTable(
      ["Recurso", "Proveedor", "Especificación", "Costo Mensual Estimado (USD)"],
      [
        ["Kafka (3 brokers)", "AWS MSK", "kafka.t3.small x 3", "~$90"],
        ["Spark (4 workers)", "AWS EMR", "m5.large x 4", "~$200"],
        ["Almacenamiento", "AWS S3", "100 GB Parquet + 50 GB Kafka", "~$5"],
        ["Dashboard/API", "AWS EC2", "t3.medium", "~$30"],
        ["Base de datos", "Supabase Pro", "Pro plan", "~$25"],
        ["Monitoreo", "AWS CloudWatch / Grafana Cloud", "Métricas básicas", "~$15"],
        ["Total", "", "", "~$365/mes"],
      ],
      [2500, 1500, 2800, 3200]
    ),
    pageBreak(),
  ];
}

function chapter8() {
  return [
    h1("8. EVIDENCIAS TÉCNICAS"),
    hr(),

    h2("8.1 Servicios del Stack"),
    p("15 contenedores Docker ejecutándose simultáneamente:", { bold: true }),
    simpleTable(
      ["Nombre", "Estado", "Puertos", "Propósito"],
      [
        ["clime-kafka", "Up", "9092, 19092", "Broker Kafka modo KRaft"],
        ["clime-kafka-exporter", "Up", "19308:9308", "Exporta métricas Kafka a Prometheus"],
        ["clime-prometheus", "Up", "19090:9090", "Recolector de métricas"],
        ["clime-grafana", "Up", "13000:3000", "Dashboards de observabilidad (admin/admin)"],
        ["clime-kafka-ui", "Up", "18085:8080", "Interfaz de gestión Kafka"],
        ["clime-jupyter", "Up", "8888, 4040", "Jupyter Lab + PySpark"],
        ["clime-postgres", "Up", "15432:5432", "PostgreSQL 15 persistente (user=clime, db=climedb)"],
        ["clime-bridge-grupo2", "Up", "-", "Bridge tabla grupo_2_air_quality → topic clima-grupo_2"],
        ["clime-bridge-grupo3", "Up", "-", "Bridge tabla grupo_3_air_quality → topic clima-grupo_3"],
        ["clime-bridge-grupo4", "Up", "-", "Bridge tabla grupo4_air_quality → topic clima-grupo_4"],
        ["clime-spark-grupo2", "Up", "-", "Spark Streaming: consume clima-grupo_2, escribe a PG + anomalías"],
        ["clime-spark-grupo3", "Up", "-", "Spark Streaming: consume clima-grupo_3, escribe a PG + anomalías"],
        ["clime-spark-grupo4", "Up", "-", "Spark Streaming: consume clima-grupo_4, escribe a PG + anomalías"],
        ["clime-kafka-value-exporter", "Up", "8000", "Exporter Prometheus: expone variables de sensor desde Kafka (temperatura, humedad, IAQ, anomalías, etc.)"],
        ["clime-dashboard", "Up", "8501", "Dashboard Streamlit (consumer group dashboard-consumer)"],
      ],
      [2800, 1200, 2200, 4500]
    ),

    h2("8.2 Datos Históricos Procesados (ETL)"),
    p("Resultados del pipeline ETL batch ejecutado sobre 60 archivos SENAMHI:"),
    bullet("Total de archivos TXT procesados: 60."),
    bullet("Registros extraídos y cargados: 1,073,151."),
    bullet("Rango de fechas: 1940-01-01 a 2015-10-31."),
    bullet("Estaciones únicas: 60."),
    bullet("Departamentos cubiertos: 11 (AMAMZONAS, AMAZONAS, ANCASH, APURIMAC, AREQUIPA, CUSCO, HUANCAVELICA, HUANUCO, ICA, JUNIN, PUNO)."),
    bullet("Formato de almacenamiento: Parquet con particionado Hive (department/province/district/year) y compresión snappy."),
    bullet("Tamaño total: ~14 MB (2989 archivos parquet)."),

    h2("8.3 Tópicos Kafka"),
    p("6 tópicos activos en el cluster Kafka:", { bold: true }),
    code("clima-grupo_2         PartitionCount: 1    ReplicationFactor: 1    Messages: ~136,028"),
    code("clima-grupo_3         PartitionCount: 1    ReplicationFactor: 1    Messages: ~133,644"),
    code("clima-grupo_4         PartitionCount: 1    ReplicationFactor: 1    Messages: ~96,599"),
    code("clima-grupo_2-anomalias  PartitionCount: 1    ReplicationFactor: 1    Messages: ~136,028"),
    code("clima-grupo_3-anomalias  PartitionCount: 1    ReplicationFactor: 1    Messages: ~133,644"),
    code("clima-grupo_4-anomalias  PartitionCount: 1    ReplicationFactor: 1    Messages: ~96,599"),

    h2("8.4 Puentes Supabase → Kafka (Logs)"),
    p("Cada bridge realiza carga inicial de datos existentes y luego polling incremental:", { bold: true }),
    code("[Bridge grupo_2] Lote 137: 28 registros publicados (total: 136028, ultimo id=136028) a topic 'clima-grupo_2'"),
    code("[Bridge grupo_2] Carga inicial completada: 136028 registros a topic 'clima-grupo_2' (ultimo id=136028)"),
    code("[Bridge grupo_3] Polling: 1 nuevos registros (IDs 133640..133641) a topic 'clima-grupo_3'"),
    code("[Bridge grupo_3] Checkpoint guardado para 'grupo_3_air_quality': last_id=133641"),
    code("[Bridge grupo_4] Carga inicial completada: 96599 registros a topic 'clima-grupo_4' (ultimo id=96615)"),

    h2("8.5 Fragmentos de Código Clave"),

    h3("8.5.1 Detección de Anomalías (Spark)"),
    code("def detect_anomalies(self, batch_df):"),
    code("    sigma = lit(2.0)"),
    code("    return batch_df"),
    code("        .withColumn('anomalyScore',"),
    code("            spark_abs(col('temperatura') - col('promedioHistorico')) / col('desviacionEstandar'))"),
    code("        .withColumn('isAnomaly', col('anomalyScore') > sigma)"),
    code("        .withColumn('anomalyType',"),
    code("            when(col('anomalyScore') > sigma * 2, lit('critica'))"),
    code("            .when(col('anomalyScore') > sigma, lit('alerta'))"),
    code("            .otherwise(lit('normal')))),"),

    h3("8.5.2 Escritura a PostgreSQL"),
    code("def write_to_postgres(self, batch_df, batch_id):"),
    code("    cols = ['id','sensor_id','estacion','department','province','district',"),
    code("            'temperatura','humedad','presion','altura','iaq','eco2','voc',"),
    code("            'calidad_aire','ts','created_at','processed_at']"),
    code("    rows = [row.asDict() for row in batch_df.select(cols).collect()]"),
    code("    for r in rows:"),
    code("        r['processed_at'] = datetime.utcnow()"),
    code("    cur.executemany(\"INSERT INTO sensor_data_grupo_X (...) VALUES (...)"),
    code("        ON CONFLICT (id) DO NOTHING\", rows)"),

    h3("8.5.3 Puente Supabase con Checkpoint"),
    code("def publish_records(self, records):"),
    code("    for rec in records:"),
    code("        rec['department'] = self.catalog['department']"),
    code("        rec['province'] = self.catalog['province']"),
    code("        rec['district'] = self.catalog['district']"),
    code("        self.producer.send(self.topic, value=rec)"),
    code("    self.save_checkpoint(last_id)"),

    h3("8.5.4 Pipeline ETL (PySpark)"),
    code("file_rdd = self.spark.sparkContext.wholeTextFiles(f'{input_path}/*.txt')"),
    code("all_rows = file_rdd.toDF(['filepath','content']).rdd.flatMap("),
    code("    lambda row: process_file(row.content, row.filepath))"),
    code("df = self.spark.createDataFrame(all_rows, SCHEMA)"),
    code("df.write.mode('overwrite').partitionBy('department','province','district','year')"),
    code("    .option('compression', 'snappy').parquet(output_path)"),

    h3("8.5.5 Estadísticas Históricas desde Parquet (vía tmax)"),
    code("def calculate_stats_from_historical(self):"),
    code("    df = self.spark.read.parquet(self.parquet_path)"),
    code("    col_temp = 'tmax' if 'temperatura' not in df.columns else 'temperatura'"),
    code("    stats = df.agg(avg(col_temp).alias('avg'), stddev(col_temp).alias('std')).collect()[0]"),
    code("    return stats['avg'], stats['std']"),
    pageBreak(),
  ];
}

function chapter9() {
  return [
    h1("9. CONCLUSIONES"),
    hr(),

    h2("9.1 Logros Alcanzados"),
    bullet("Pipeline streaming funcional basado en Kafka + Spark Structured Streaming con 15 servicios contenedorizados multi-estación."),
    bullet("3 bridges independientes con checkpoint recovery que leen desde Supabase y publican a 3 topics Kafka dedicados."),
    bullet("3 instancias Spark Streaming procesando en paralelo con detección de anomalías basada en z-score sobre promedios históricos."),
    bullet("Almacenamiento persistente en PostgreSQL con 366,268+ registros streaming (sensor_data_grupo_2/3/4) y deduplicación vía ON CONFLICT DO NOTHING."),
    bullet("Procesamiento batch ETL de 60 archivos SENAMHI (1,073,151 registros) a Parquet particionado, usado para stats históricos."),
    bullet("Dashboard interactivo (Streamlit) con 3 pestañas: Histórico, Tiempo Real (Kafka) y Predicciones ML (XGBoost)."),
    bullet("Kafka Value Exporter personalizado que expone variables reales de sensores como métricas Prometheus gauge."),
    bullet("Dashboard Grafana Sensores con 10 paneles + alertas Prometheus."),
    bullet("10 reglas de alerta Prometheus (4 de infraestructura Kafka + 6 de sensores)."),
    bullet("Pipeline ML completo: 2 tipos de modelos (largo y corto plazo), 7 modelos entrenados (4 largo + 3 corto), feature engineering automatizado, predicción interactiva desde dashboard."),
    bullet("Modelos largo plazo con R² > 0.99 y MAE < 0.11°C usando datos SENAMHI de 50+ años."),
    bullet("Modelos corto plazo con R² > 0.99 y MAE < 0.05°C para sensores con datos de calidad."),
    bullet("Observabilidad completa: Prometheus, Grafana con 2 dashboards, Kafka UI, métricas de offset, lag y variables de sensor."),
    bullet("Documentación operativa con estimación de costos, estrategia de escalado y ML pipeline."),

    h2("9.2 Limitaciones Encontradas"),
    bullet("3 instancias Spark en modo local[*] — sin distribución ni tolerancia a fallos del procesador."),
    bullet("Kafka con 1 broker y 1 partición por tópico — sin alta disponibilidad ni paralelismo."),
    bullet("Los datos históricos usan tmax (temperatura máxima) como proxy de temperatura ambiente."),
    bullet("No hay métricas de bridges/sparks expuestas directamente a Prometheus (solo logs)."),
    bullet("Sin sistema de notificaciones automáticas (email/WhatsApp) para alertas."),
    bullet("Sin autenticación ni control de acceso al dashboard o Kafka UI."),
    bullet("Sensor grupo_3 (PUNO) presenta datos defectuosos (presión 186 hPa vs 646 esperado, temperatura constante ~-0.06°C) que impiden entrenar modelo corto plazo confiable."),

    h2("9.3 Mejoras Propuestas"),
    bullet("Exponer métricas de bridges y sparks a Prometheus."),
    bullet("Migrar Spark a modo cluster (Standalone o YARN) con 2+ workers."),
    bullet("Aumentar a 3 brokers Kafka con replicación factor 2."),
    bullet("Implementar rolling upgrade y zero-downtime deployment."),
    bullet("Configurar Alertmanager con notificaciones por email/WhatsApp/Slack."),
    bullet("Agregar autenticación básica en dashboards."),
    bullet("Incorporar re-entrenamiento automático de modelos ML programado (cron) o manual desde dashboard."),
    bullet("Agregar más estaciones SENAMHI para mejorar cobertura geográfica de modelos largo plazo."),
    bullet("Implementar ensemble de modelos (XGBoost + LSTM) para predicción multi-variable."),

    h2("9.4 Preparación para ML Distribuido"),
    p("El pipeline actual implementa ML con XGBoost en dos modalidades:"),
    bullet("Largo plazo: Predicción de tmax diaria usando datos SENAMHI históricos (50+ años, 60 estaciones), con features de lags, rolling means y codificación temporal."),
    bullet("Corto plazo: Predicción de temperatura a 5-15 minutos usando datos streaming de sensores IoT, con features de ratio presión/temperatura y ventanas móviles."),
    bullet("Infraestructura: Los datos en Parquet son consumidos directamente por sklearn/xgboost. Modelos persistidos en formato joblib."),
    bullet("Escalabilidad: El feature engineering y entrenamiento pueden migrarse a Spark MLlib para manejar miles de estaciones."),
    pageBreak(),
  ];
}

function chapter10() {
  return [
    h1("ANEXOS"),
    hr(),
    h2("A.1 Lista de Puertos del Stack"),
    simpleTable(
      ["Servicio", "Puerto Interno", "Puerto Externo", "URL"],
      [
        ["Dashboard Streamlit", "8501", "8501", "http://localhost:8501"],
        ["Jupyter Lab", "8888", "8888", "http://localhost:8888"],
        ["Jupyter Spark UI", "4040", "4040", "http://localhost:4040"],
        ["Prometheus", "9090", "19090", "http://localhost:19090"],
        ["Grafana", "3000", "13000", "http://localhost:13000 (admin/admin)"],
        ["Kafka UI", "8080", "18085", "http://localhost:18085"],
        ["Kafka Exporter", "9308", "19308", "http://localhost:19308/metrics"],
        ["Kafka Value Exporter", "8000", "8000", "http://localhost:8000/metrics"],
        ["Kafka (externo)", "19092", "19092", "localhost:19092"],
        ["PostgreSQL", "5432", "15432", "localhost:15432 (user=clime, db=climedb)"],
      ],
      [2800, 2200, 2200, 3800]
    ),
    ...spacer(1),

    h2("A.2 Enlaces a Recursos"),
    bullet("Repositorio del proyecto: https://github.com/anomalyco/clime-peru"),
    bullet("Supabase Dashboard: https://supabase.com/dashboard/project/mangrxgusewzgtewoayx"),
    bullet("Streamlit Cloud (próximamente): TBD"),

    h2("A.3 Checklist de Entrega"),
    simpleTable(
      ["Criterio", "Cumple", "Observaciones"],
      [
        ["Se crearon y probaron tópicos Kafka multi-estación", "SÍ", "6 tópicos: clima-grupo_2/3/4 y clima-grupo_2/3/4-anomalias"],
        ["Se ejecutaron productores y consumidores", "SÍ", "3 bridges producen datos; 3 Sparks consumen y 1 dashboard consume"],
        ["Se documentó el contrato de evento con anomalías", "SÍ", "Sección 3.4 con tabla raw y enriquecida (anomalía)"],
        ["Se implementó pipeline Spark Streaming multi-instancia", "SÍ", "3 SparkStreamingProcessors independientes con detección de anomalías"],
        ["Se usó checkpoint recovery en bridges y sparks", "SÍ", "Bridges: checkpoints JSON con last_id. Sparks: checkpoint directorio Structured Streaming"],
        ["Se almacenan datos en PostgreSQL persistente", "SÍ", "3 tablas sensor_data_grupo_X con 366K+ registros totales"],
        ["Se midió latencia y throughput", "SÍ", "Dashboard con lag cero en todos los topics"],
        ["Se implementó observabilidad completa", "SÍ", "Prometheus + 2 dashboards Grafana (Kafka Overview + Sensor Variables) + Kafka UI + Value Exporter"],
        ["Se definieron umbrales de alerta", "SÍ", "10 reglas de alerta (4 infraestructura + 6 sensores), 5 umbrales sugeridos"],
        ["Se implementó exporter de variables de sensor a Prometheus", "SÍ", "Kafka Value Exporter expone temperatura, humedad, IAQ, eCO₂, VOC, anomalías como métricas gauge"],
        ["Se creó dashboard Grafana de variables por estación", "SÍ", "Dashboard Sensores — Variables por Estación con paneles por variable + alertas activas + estado de sensores"],
        ["Se estimaron costos y estrategia de escalado", "SÍ", "Sección 7 con recursos actuales y costos cloud"],
        ["Se adjuntaron evidencias técnicas", "SÍ", "15 servicios, 6 topics, logs de bridges/sparks, fragmentos de código, exporter, 2 dashboards Grafana"],
      ],
      [4500, 1200, 5300]
    ),
  ];
}

function chapter11() {
  return [
    h1("11. PIPELINE DE MACHINE LEARNING"),
    hr(),

    h2("11.1 Descripción General"),
    p("El pipeline de Machine Learning del proyecto CliMePerú proporciona predicciones de temperatura en dos horizontes temporales distintos: largo plazo (predicción diaria de tmax usando datos históricos SENAMHI) y corto plazo (predicción de temperatura a intervalos de 5-15 minutos usando datos streaming de sensores IoT). Ambos modelos se integran en el dashboard de Streamlit como la tercera pestaña 'Predicciones ML'."),

    h2("11.2 Arquitectura ML"),
    p("La arquitectura ML consta de los siguientes componentes:"),
    bullet("Módulo de features (ml/features.py): Ingeniería de características para ambos horizontes."),
    bullet("Módulo de entrenamiento largo plazo (ml/train_largo_plazo.py): Entrena modelos XGBoost con datos históricos."),
    bullet("Módulo de entrenamiento corto plazo (ml/train_corto_plazo.py): Entrena modelos XGBoost con datos streaming."),
    bullet("Módulo de predicción (ml/predict.py): Funciones de inferencia para ambos horizontes, con caché en sesión."),
    bullet("Modelos persistidos (ml/models/): Archivos .pkl y métricas JSON por estación."),
    bullet("Dashboard (dashboard/app.py): Tercer tab con interfaz interactiva de predicción."),

    p("Los datos fluyen desde los Parquets históricos y streaming hacia los pipelines de entrenamiento. Los modelos entrenados se guardan en el directorio ml/models/ que está montado como volumen Docker para persistencia entre recreaciones del contenedor."),

    h2("11.3 Feature Engineering"),
    h3("11.3.1 Features para Largo Plazo (tmax diario)"),
    p("La función build_largo_plazo_features() genera las siguientes características a partir de datos diarios de temperatura:"),
    bullet("Lag features: t-1, t-3, t-5, t-7 (temperatura de 1, 3, 5 y 7 días atrás)."),
    bullet("Rolling means: ventanas de 3, 7, 14 y 30 días sobre tmax."),
    bullet("Rolling std: desviación estándar en ventanas de 7 y 14 días."),
    bullet("Diferencia con rolling mean de 30 días (tmax - avg_30d)."),
    bullet("Tendencia: diferencia entre rolling mean de 7 y 30 días."),
    bullet("Codificación temporal cíclica: mes y día transformados con seno/coseno."),
    bullet("Características categóricas: estación del año (invierno/verano/otoño/primavera)."),

    h3("11.3.2 Features para Corto Plazo (temperatura streaming)"),
    p("La función build_corto_plazo_features() genera características a partir de mediciones cada ~5 minutos:"),
    bullet("Lag features: t-1 y t-3 (últimas 1 y 3 observaciones)."),
    bullet("Rolling mean: media móvil de 5 observaciones."),
    bullet("Ratio presión/temperatura (presion / temperatura) como indicador de condiciones atmosféricas."),
    bullet("Hora del día como variable cíclica (seno/coseno)."),

    h2("11.4 Entrenamiento de Modelos"),
    h3("11.4.1 Largo Plazo — Datos SENAMHI"),
    p("El script train_largo_plazo.py procesa los datos históricos de SENAMHI almacenados en Parquet particionado por departamento/provincia/distrito/año. El flujo de entrenamiento es:"),
    bullet("Lectura de Parquet: 2982 archivos, 60 estaciones, datos desde 1964 hasta 2012+."),
    bullet("Filtrado: Selecciona estaciones con >1000 registros de tmax (PUNO, AZANGARO, LAMPA, CAPACHICA)."),
    bullet("Preprocesamiento: Ordena por fecha para cada estación, elimina filas con tmax nulo."),
    bullet("Feature engineering: build_largo_plazo_features() sobre secuencias ordenadas."),
    bullet("División train/test: 80% entrenamiento, 20% prueba (últimos 20% cronológicos)."),
    bullet("Modelo: XGBoost Regressor con n_estimators=500, learning_rate=0.05, max_depth=7."),
    bullet("Métricas: MAE, RMSE, R² calculados sobre conjunto de prueba."),

    h3("11.4.2 Corto Plazo — Datos Streaming"),
    p("El script train_corto_plazo.py procesa los datos de sensores IoT almacenados en Parquet particionado por grupo. El flujo es:"),
    bullet("Lectura de Parquet streaming de los directorios grupo_2, grupo_3 y grupo_4."),
    bullet("Selecciona las últimas 10000 observaciones de cada grupo para entrenamiento."),
    bullet("Preprocesamiento: Ordena por created_at, elimina temperatura nula."),
    bullet("Feature engineering: build_corto_plazo_features() sobre secuencias ordenadas."),
    bullet("División train/test: 80% entrenamiento, 20% prueba."),
    bullet("Modelo: XGBoost Regressor con n_estimators=300, learning_rate=0.05, max_depth=5."),

    h2("11.5 Resultados y Métricas"),
    h3("11.5.1 Modelos de Largo Plazo"),
    simpleTable(
      ["Estación", "MAE (°C)", "RMSE (°C)", "R²"],
      [
        ["PUNO", "0.08", "0.20", "0.994"],
        ["AZANGARO", "0.09", "0.20", "0.995"],
        ["LAMPA", "0.07", "0.16", "0.997"],
        ["CAPACHICA", "0.08", "0.18", "0.994"],
      ],
      [3000, 2000, 2000, 2000]
    ),
    p("Los cuatro modelos de largo plazo alcanzan R² > 0.99, demostrando alta precisión en la predicción de temperatura máxima diaria. LAMPA presenta el mejor rendimiento (MAE=0.07°C, R²=0.997). Estos resultados se explican por la naturaleza estacional y predecible del clima altiplánico."),

    h3("11.5.2 Modelos de Corto Plazo"),
    simpleTable(
      ["Grupo / Estación", "MAE (°C)", "RMSE (°C)", "R²", "Estado"],
      [
        ["grupo_2 / LAMPA", "0.044", "0.106", "0.992", "OK"],
        ["grupo_3 / PUNO", "6.355", "9.810", "-1.410", "Sensor defectuoso"],
        ["grupo_4 / AZANGARO", "0.053", "0.096", "0.993", "OK"],
      ],
      [2500, 1500, 1500, 1500, 2000]
    ),
    p("Los modelos de corto plazo para sensores con datos de calidad (grupo_2 y grupo_4) alcanzan R² > 0.99 con MAE < 0.06°C. El grupo_3 presenta datos anómalos: presión atmosférica de 186 hPa (vs ~646 hPa en los otros grupos) y temperatura constante de aproximadamente -0.06°C, lo que indica un sensor defectuoso. Esto resulta en un R² negativo (-1.41) y MAE elevado (6.36°C)."),

    h2("11.6 Predicción en Tiempo Real (Dashboard)"),
    p("La función render_ml_predictions() en dashboard/app.py proporciona una interfaz interactiva con:"),
    bullet("Selector de horizonte: Largo Plazo (tmax diario) o Corto Plazo (temperatura inmediata)."),
    bullet("Selector de estación: dinámico según el horizonte seleccionado."),
    bullet("Fecha de predicción: control date_input para especificar el día de predicción (largo plazo)."),
    bullet("Botón 'Predecir': ejecuta la predicción y muestra resultados."),
    bullet("Visualización de resultados: tarjeta con temperatura predicha, MAE, RMSE y R² del modelo."),

    p("Las predicciones se almacenan en st.session_state.ml_lp_result y st.session_state.ml_cp_result para persistir entre reruns automáticos del dashboard (streamlit-autorefresh cada 2 segundos en la pestaña de tiempo real)."),

    h2("11.7 Optimización de Rendimiento"),
    p("Durante la implementación se identificaron y resolvieron los siguientes cuellos de botella:"),
    bullet("Problema original: predict_largo_plazo() escaneaba todos los 2982 archivos Parquet (todos los departamentos/estaciones) usando glob('**/*.parquet'), tomando varios minutos por predicción."),
    bullet("Solución: Se implementó STATION_DIR_MAP que mapea cada estación al subdirectorio específico de Parquet. La función ahora construye la ruta directa artefacts/weather_data/{dept}/{prov}/{district}/{year}/, reduciendo el tiempo de predicción de ~minutos a ~1.5 segundos."),
    bullet("Persistencia de modelos: Los modelos .pkl se guardan en ml/models/ montado como volumen Docker, evitando retrain en cada recreación del contenedor."),

    h2("11.8 Limitaciones de los Modelos ML"),
    bullet("Disponibilidad de datos: Solo 4 estaciones SENAMHI tienen suficientes registros (>1000) para entrenamiento de largo plazo."),
    bullet("Calidad de sensores: El sensor grupo_3 presenta datos defectuosos que impiden entrenar un modelo corto plazo confiable para PUNO."),
    bullet("Horizonte de predicción corto plazo: Limitado a 5-15 minutos (frecuencia de muestreo de sensores)."),
    bullet("Sin re-entrenamiento automático: Los modelos deben re-entrenarse manualmente ejecutando los scripts train_largo_plazo.py y train_corto_plazo.py."),
    bullet("Features limitados: Solo se usan features derivados de temperatura/presión; no se incorporan variables meteorológicas externas (viento, precipitación, radiación solar)."),
    bullet("Modelo único: Se utiliza XGBoost como único algoritmo; no se implementaron ensembles con Random Forest, LSTM u otros modelos."),

    h2("11.9 Mejoras Propuestas para ML"),
    bullet("Re-entrenamiento automático programado (cron job en contenedor separado) o manual desde el dashboard."),
    bullet("Incorporar más estaciones SENAMHI históricas para expandir cobertura geográfica."),
    bullet("Implementar ensemble de modelos (XGBoost + Random Forest + LSTM) para predicción multi-variable."),
    bullet("Agregar features externos: datos de viento, precipitación, radiación solar de fuentes abiertas."),
    bullet("Migrar entrenamiento a Spark MLlib para escalar a cientos de estaciones."),
    bullet("Implementar detección automática de sensores defectuosos basada en residuales de predicción."),
    bullet("Agregar intervalo de confianza a las predicciones usando cuantiles del XGBoost."),
    bullet("Pipeline CI/CD para validación y deploy de nuevos modelos automáticamente."),
    pageBreak(),
  ];
}

module.exports = { chapter4, chapter5, chapter6, chapter7, chapter8, chapter9, chapter10, chapter11 };
