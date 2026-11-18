# src/ — Jobs de Spark

Aquí están los trabajos de PySpark del pipeline por lotes y de los benchmarks. Todos se lanzan con `spark-submit` desde la raíz del repositorio (ver la sección 5 del [README principal](../README.md)). Las rutas por defecto apuntan a HDFS (`hdfs:///datalake/...`).

## Pipeline principal (zona raw → zona processed)

| Script | Qué hace | Entrada | Salida |
|---|---|---|---|
| `quality.py` | Limpieza de klines: quita duplicados `(simbolo, open_time)`, descarta velas con OHLC incoherente y reporta los huecos de tiempo (no los rellena). | `raw/binance/klines` (CSV) | `processed/binance/klines_clean` (Parquet `simbolo/anio/mes`) |
| `ingest_aggtrades.py` | Ingesta y limpieza de aggTrades con esquema explícito. Reglas: duplicado, nulo, precio ≤ 0, cantidad ≤ 0 y fuera del día del archivo. | `raw/binance/aggtrades/*/csv` | `processed/binance/aggtrades_clean` |
| `aggtrades_hourly.py` | Resumen horario de aggTrades, unión con klines_clean y validación cruzada de volumen y número de operaciones. | `aggtrades_clean`, `klines_clean` | `aggtrades_hourly`, `aggtrades_klines_join` |
| `capa_consumo.py` | Modelo en estrella para el tablero: `fact_paridad_hora` más 4 dimensiones (tiempo, fuente, evento y severidad). Valida llaves y nulos, y exporta CSV. | `data/features.parquet` (+ `klines_clean` para contrastar) | `processed/binance/consumo/` y `data/consumo/*.csv` |

## Benchmarks (solo escriben en `hdfs:///datalake/benchmark/`)

| Script | Pedido | Qué mide |
|---|---|---|
| `benchmark_join.py` | 2 | Sort-merge join frente a broadcast join (6 corridas por variante; la primera es de calentamiento). |
| `benchmark_join_verif.py` | 2 | Complemento: `--modo ui` (captura de la Spark UI) y `--modo verif` (comprueba que ambas salidas sean iguales). |
| `quality_opt.py` | 5 | Versión optimizada de `quality.py` (persistencia, menos `count()` y ventana por mes opcional). Mismo resultado. |
| `join_broadcast_solo.py` | 5 | Una sola corrida del join con broadcast, para medir la escalabilidad. |
| `replicate_raw.py` | 5 | Réplicas sintéticas de klines raw (N copias desplazadas en el tiempo). |
| `replicate_aggtrades.py` | 9 | Réplicas sintéticas de aggtrades_clean (ids desplazados en k·10¹²). |
| `bench_aggtrades.py` | 9 | Cadena completa de aggTrades (calidad, resumen horario y unión). Es el mismo código en `local[*]` y en YARN. |

Los scripts `.sh` de la raíz (`bench_*.sh`, `run_*.sh`) lanzan estos benchmarks y guardan los logs en [`benchmarks/resultados/`](../benchmarks/resultados/README.md).

## Convenciones

- Todo el tiempo se maneja en **UTC** (`spark.sql.session.timeZone=UTC` en los scripts nuevos).
- Las líneas de validación se imprimen con prefijo `QC` para poder filtrarlas con `grep`.
- Las réplicas sintéticas solo existen en la zona `benchmark` y nunca alimentan el análisis ni el modelo.
- `features.py` está vacío: las variables se construyen en `notebooks/04_features.ipynb`.
