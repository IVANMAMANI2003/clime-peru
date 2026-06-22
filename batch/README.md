# Batch — ETL de Datos Históricos SENAMHI

Procesamiento batch para convertir archivos climáticos del SENAMHI (formato .txt) a Parquet optimizado para consultas analíticas.

## Flujo

```
data/raw/*.txt ──▶ PySpark ETL ──▶ artifacts/weather_data/ (Parquet particionado)
                                    artifacts/stations_metadata.parquet
```

## Componentes

### `etl_senamhi.py` — ETL con PySpark (recomendado)

Pipeline de 3 etapas:

| Etapa | Descripción |
|-------|-------------|
| **Extract** | `sparkContext.wholeTextFiles()` lee todos los .txt. Cada archivo debe nombrarse como `ESTACION-DEPARTAMENTO-PROVINCIA-DISTRITO.txt`. Cada línea tiene 6 columnas separadas por espacio: `YYYY MM DD precipitacion tmax tmin`. |
| **Transform** | Filtra registros inválidos (mes 1-12, día 1-31, año 1900-2030). Crea columna `date` con `to_date(concat_ws("-", year, month, day))`. |
| **Load** | Escribe a Parquet con `snappy` compression, particionado por `department/province/district/year`. Guarda metadatos de estaciones únicas. |

### `etl_pandas.py` — ETL alternativo (sin Spark)

Misma lógica usando solo pandas. Útil en Windows o cuando no hay Spark disponible. Escribe un solo archivo `weather_data.parquet` (no particionado).

## Formato de Archivo de Entrada

```
# Comentario opcional
1965 01 01 -99.9 15.2 4.6
1965 01 02 -99.9 15.3 5.0
1965 01 03 0.0 14.8 4.8
```

**Columnas**: Año, Mes, Día, Precipitación (mm), Temperatura Máxima (°C), Temperatura Mínima (°C).

**Valores nulos**: `-99.9`, `NA`.

## Salida Parquet

```
artifacts/weather_data/
├── department=PUNO/
│   └── province=PUNO/
│       └── district=PUNO/
│           ├── year=1964/
│           │   └── part-00001-xxx.snappy.parquet
│           └── year=1965/
│               └── part-00001-xxx.snappy.parquet
├── department=ICA/
│   └── ...
└── _SUCCESS
```

## Ejecución

### Con Docker (recomendado)

```bash
docker exec clime-jupyter python -m batch.etl_senamhi
```

### Local (sin Docker)

```bash
python -m batch.etl_pandas
```

## Datos Actuales

- **60 archivos** procesados
- **1,073,151 registros**
- **60 estaciones** únicas
- **11 departamentos**: AMAMZONAS, AMAZONAS, ANCASH, APURIMAC, AREQUIPA, CUSCO, HUANCAVELICA, HUANUCO, ICA, JUNIN, PUNO
- **Período**: 1940-01-01 a 2015-10-31
- **Tamaño**: ~14 MB (2989 archivos parquet comprimidos snappy)

## Uso en Streaming

El Spark Streaming Processor (`streaming/spark_streaming_processor.py`) usa este Parquet para calcular `avg(tmax)` y `stddev(tmax)` históricos por ubicación geográfica (department/province/district). Estos estadísticos se usan como baseline para detectar anomalías en tiempo real.

Nota: usa `tmax` (temperatura máxima) como proxy de `temperatura` ya que los datos streaming contienen temperatura ambiente en lugar de máximas/mínimas.

### Uso en ML

El Parquet histórico generado por este ETL es consumido por `ml/train_largo_plazo.py` para entrenar modelos XGBoost de predicción de tmax diario. Los scripts ML filtran estaciones con >1000 registros (PUNO, AZANGARO, LAMPA, CAPACHICA) y utilizan las columnas `tmax` y `date` para feature engineering con lags y rolling means.

## Conexiones

| Componente | Conexión |
|-----------|----------|
| `streaming/spark_streaming_processor.py` | Lee el Parquet histórico para calcular promedios y desviaciones por estación (columna `tmax`) |
| `dashboard/app.py` | Lee `artifacts/weather_data` y `artifacts/stations_metadata.parquet` para visualización |
| `ml/train_largo_plazo.py` | Lee el Parquet histórico para entrenar modelos XGBoost de predicción de tmax |
| `ml/predict.py` | Lee el Parquet histórico para inferencia de largo plazo (usa STATION_DIR_MAP) |
