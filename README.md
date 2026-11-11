# Deteccion temprana de perdida de paridad en stablecoins

Proyecto del curso **Big Data y Analitica de Datos** (UPAO, NRC 9770). Se procesan velas (klines) y operaciones agregadas (aggTrades) de Binance sobre un cluster Hadoop/YARN/Spark para detectar de forma temprana la perdida de paridad (depeg) de stablecoins en tres eventos: **Terra/UST (mayo 2022)**, **FTX (noviembre 2022)** y **SVB (marzo 2023)**.

## 1. Requisitos

| Componente | Version |
|---|---|
| Java | 17 |
| Hadoop (HDFS + YARN) | 3.3.6 |
| Spark | 3.5.1 |
| Python | 3.11 |

Librerias de Python (se recomienda un entorno virtual; los notebooks se ejecutan con `nbconvert`):
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt nbconvert
```

## 2. Cluster

- Master: VM Debian (2 vCPU, 3.8 GiB RAM) con NameNode, ResourceManager y DataNode.
- Worker: nodo de Andres Pagan (24 GB de RAM), registrado en YARN con 20 GB y 20 vcores.
- Los nodos se unen por Tailscale.
- Spark sobre YARN con 2 ejecutores de 4 GB y 2 nucleos cada uno (`--master yarn --deploy-mode client --num-executors 2 --executor-memory 4g --executor-cores 2`) y `spark.yarn.archive` apuntando a las librerias de Spark en HDFS (evita subirlas en cada ejecucion).
- Spark en modo local para los jobs pequenos: `--master 'local[*]' --driver-memory 2g`.
- Zona horaria de sesion de Spark: UTC. Particiones de shuffle: 8.

## 3. Estructura de carpetas

```
scripts/        descarga y carga de datos (download_klines.py, download_aggtrades.py, upload_aggtrades.sh)
src/            jobs de Spark (quality.py, quality_opt.py, benchmark_join.py, ingest_aggtrades.py, aggtrades_hourly.py,
                capa_consumo.py, replicate_aggtrades.py, bench_aggtrades.py)
notebooks/      01_ingesta, 02_calidad_datos, 03_eda, 04_features, 05_modelo_baseline, 05_modelo_definitivo (historico),
                06_linea_base, 07_validacion_fuga, 06_benchmarks
benchmarks/     resultados de benchmarks (resultados/pedido5, escalabilidad, aggtrades, escalabilidad_aggtrades, consumo)
./              scripts de benchmark (run_pedido5.sh, bench_scale.sh, bench_local.sh, bench_opt.sh, bench_join.sh,
                run_escalabilidad_aggtrades.sh)
aportes/        aportes individuales (aportes/adriel: verificacion de calidad de klines_clean)
informe/        figuras del informe
architecture/   figura de arquitectura (system_architecture.png; copia en informe/fig_arquitectura.png)
data/sample/    muestras reales (100 filas por simbolo)
data/consumo/   CSV de la capa de consumo para el tablero (tabla de hechos y 4 dimensiones, mas la consulta de prueba)
data/           klines_clean y features.parquet usados por los notebooks
```

## 4. Arquitectura del sistema
![Arquitectura del sistema](./architecture/system_architecture.png)

Linea continua = implementado (ruta por lotes, capa de consumo y tablero). Linea punteada = planificado (ruta de velocidad). La zona `/datalake/benchmark/` esta aparte y es solo para medir rendimiento.

## 5. Orden de ejecucion

### 5.1 Descargar klines
```bash
python3 scripts/download_klines.py
# opciones: --solo-proxy  --solo-proxy-svb  --solo-proxy-btc  --solo-proxy-btc-svb  --solo-marzo
```
Los .zip se descargan a `zona_cruda/` y se extraen con `unzip`.

### 5.2 Cargar klines a HDFS
```bash
hdfs dfs -mkdir -p /datalake/raw/binance/klines
hdfs dfs -put -f zona_cruda/klines/* /datalake/raw/binance/klines/
```

### 5.3 Calidad y limpieza de klines
```bash
spark-submit --master yarn --deploy-mode client src/quality.py \
  --input hdfs:///datalake/raw/binance/klines \
  --output hdfs:///datalake/processed/binance/klines_clean
```
`src/quality_opt.py` es la version optimizada (ver encabezado del script).

### 5.4 Benchmarks de join y escalabilidad
```bash
spark-submit src/benchmark_join.py
bash run_pedido5.sh
bash bench_scale.sh <E>
bash bench_local.sh <orig|opt> <10|100|150|200>
```
Resultados en `benchmarks/resultados/`.

### 5.5 aggTrades de las ventanas de eventos
```bash
python3 scripts/download_aggtrades.py      # descarga, verifica SHA-256 y extrae
bash scripts/upload_aggtrades.sh           # sube a HDFS
spark-submit --master 'local[*]' --driver-memory 2g src/ingest_aggtrades.py
spark-submit --master 'local[*]' --driver-memory 2g src/aggtrades_hourly.py
```

### 5.6 Notebooks (orden de lectura)
`notebooks/01_ingesta` -> `02_calidad_datos` -> `03_eda` -> `04_features` -> `05_modelo_baseline` -> `05_modelo_definitivo` (historico) -> `06_linea_base` -> `07_validacion_fuga`. `06_benchmarks` documenta los benchmarks de join.
Leen `data/klines_clean/` y `data/features.parquet`.

`05_modelo_definitivo` queda como **historico**: sus cifras se retiraron por la fuga de la brecha de arbitraje (`spread_btc_usdc_usdt` deriva de la etiqueta). La evaluacion vigente esta en `07_validacion_fuga`.

### 5.7 Linea base y validacion temporal
Ejecutar desde la carpeta `notebooks/` (las rutas son relativas a ella):
```bash
cd notebooks
jupyter nbconvert --to notebook --execute 06_linea_base.ipynb --output-dir /tmp/repro
jupyter nbconvert --to notebook --execute 07_validacion_fuga.ipynb --output-dir /tmp/repro
cd ..
```
Los resultados quedan en los notebooks ejecutados de `/tmp/repro/`.

### 5.8 Prueba de escalabilidad sobre aggTrades
```bash
spark-submit src/replicate_aggtrades.py --factor <2|3|5> --output hdfs:///datalake/benchmark/aggtrades_<N>x   # replicas de aggtrades_clean
bash run_escalabilidad_aggtrades.sh <local|cluster> <1|2|3|5> <repeticion 1..3>
```
Las replicas son datos sinteticos y solo existen en `/datalake/benchmark/`. Resultados en `benchmarks/resultados/escalabilidad_aggtrades/`.

### 5.9 Capa de consumo (modelo dimensional)
```bash
spark-submit --master 'local[*]' src/capa_consumo.py \
  --features data/features.parquet \
  --salida hdfs:///datalake/processed/binance/consumo \
  --klines hdfs:///datalake/processed/binance/klines_clean \
  --csv-dir data/consumo
```
Escribe en modo `overwrite`: para repetir la prueba sin tocar la zona procesada, usar otra ruta en `--salida` (por ejemplo `hdfs:///tmp/repro_consumo`). Imprime las lineas `QC ...` de validacion y la consulta de prueba.

### 5.10 Verificacion de calidad (sin cluster)
```bash
python aportes/adriel/verificar_calidad.py
```
Revisa `data/klines_clean/` con pandas (esquema, nulos, duplicados, OHLC, huecos, particiones, proxy) y deja resultados en `aportes/adriel/resultados/`. Hallazgos en `aportes/adriel/reporte_verificacion.md`.

### 5.11 Informe
Las figuras del informe estan en `informe/`. `fig_matrices_confusion.png` y `fig_matrices_confusion_definitivo.png` corresponden a cifras **retiradas**; la figura vigente es `fig_matrices_validacion_temporal.png`.

## 6. Rutas en HDFS

```
/datalake/raw/binance/klines/<SIMBOLO>/
/datalake/raw/binance/aggtrades/<SIMBOLO>/*.zip   y   .../csv/*.csv
/datalake/processed/binance/klines_clean
/datalake/processed/binance/aggtrades_clean       (particionado simbolo/anio/mes)
/datalake/processed/binance/aggtrades_hourly
/datalake/processed/binance/aggtrades_klines_join
/datalake/processed/binance/consumo/              (fact_paridad_hora, dim_tiempo, dim_fuente, dim_evento, dim_severidad)
/datalake/benchmark/                              (replicas sinteticas de aggTrades, solo para medir rendimiento)
```

## 7. Resultados

- aggTrades: 14 archivos diarios, 7,965,158 registros, 0 violaciones de calidad, resumen horario de 322 horas y cruce 322/322 con klines.
- Hueco real: USDCUSDT 2023-03-11 00:00-13:00 UTC sin operaciones.
- Linea base (`06_linea_base`): la desviacion de 1 % se alcanza 18 h despues de la primera alerta en Terra, 24 h en FTX y 17 h en SVB (como maximo).
- Validacion temporal (`07_validacion_fuga`): F1 macro promedio entre eventos: persistencia 0.943, random forest + SMOTE 0.533, regresion logistica 0.301, clase mayoritaria 0.161. Sistema adoptado: alerta por umbrales.
- Escalabilidad aggTrades: la maquina sola falla en 2x; el cluster llega hasta 5x en ~75 s.
- Capa de consumo: 17,172 filas en `fact_paridad_hora`, 0 duplicados, 0 huerfanos (15,412 normal, 1,587 alerta, 173 estres).
- Logs y tablas en `benchmarks/resultados/`.

## 8. Datos de muestra

`data/sample/` contiene las primeras 100 filas reales de klines y aggTrades. Los datos completos no se versionan.

## 9. Fuente y licencia de los datos

Datos publicos de Binance (https://data.binance.vision), publicados bajo licencia **MIT**.

## 10. Integrantes

- John Marlon Chavez Vargas - ingenieria de datos
- Andres Pagan - analisis y modelado
- Juan Carlos Vilca Jimenez - coordinacion
- Adriel Zumaeta Calderon - calidad y documentacion
