# data/ — Datos locales para los notebooks

Copias locales (pequeñas) de los datos del clúster, para poder correr los notebooks sin HDFS. Los datos crudos completos **no se versionan**: se descargan con los scripts de `scripts/`.

| Ruta | Qué es | De dónde sale |
|---|---|---|
| `klines_clean/` | Klines horarios limpios de los 5 símbolos, en Parquet particionado `simbolo=/anio=/mes=`. | Copia de `hdfs:///datalake/processed/binance/klines_clean` (salida de `src/quality.py`). |
| `features.parquet` | Serie horaria de paridad con las 5 variables y la etiqueta `severidad` (17 172 horas). | `notebooks/04_features.ipynb` |
| `consumo/` | CSV del modelo en estrella para el tablero. Ver [`consumo/README.md`](consumo/README.md). | `src/capa_consumo.py` |
| `sample/` | Primeras 100 filas reales de klines y aggTrades. Ver [`sample/README.md`](sample/README.md). | Archivos originales de Binance |
| `missing.log` | Archivos que no existían en la fuente al descargar. | `scripts/download_klines.py` |

## Columnas clave

- `open_time` / `close_time`: milisegundos desde 1970, en UTC. `open_time_ts` es la versión timestamp.
- `fuente_paridad`: `USDCUSDT` (dato real) o `BUSDUSDT_proxy` (sustituto en el hueco de oct-2022 a mar-2023; ver `docs/decisiones_abiertas.md`).
- `severidad`: `normal`, `alerta` (desviación ≥ 0.0011) o `estres` (≥ 0.0034).

> Las particiones `anio`/`mes` de `klines_clean` se calcularon con la zona horaria de la sesión de Spark (UTC-5), así que las primeras horas de un mes en UTC pueden quedar en el mes anterior. Para filtrar por fecha, usar `open_time`.
