# data/consumo/ — Capa de consumo (modelo en estrella)

CSV que exporta `src/capa_consumo.py` y que usa el tablero (`tablero/`). Están en UTF-8 con BOM para que Excel muestre bien las tildes. La misma información está en Parquet en `hdfs:///datalake/processed/binance/consumo/`.

| Archivo | Filas | Contenido |
|---|---|---|
| `fact_paridad_hora.csv` | 17 172 | **Tabla de hechos**: una fila por hora con dato. Llaves: `id_tiempo`, `id_fuente`, `id_evento` e `id_severidad`. Medidas: `close_paridad`, `desviacion_paridad`, `volumen`, `volumen_relativo` y `volatilidad_ventana`. Banderas: `en_alerta_o_estres` y `supera_1pct`. |
| `dim_tiempo.csv` | 17 520 | Calendario horario completo (`id_tiempo` = `AAAAMMDDHH`). `tiene_dato = 0` marca las horas sin dato (huecos). |
| `dim_fuente.csv` | 2 | 1 = USDCUSDT (real), 2 = BUSDUSDT_proxy. |
| `dim_evento.csv` | 4 | 0 = fuera de evento, 1 = Terra, 2 = FTX, 3 = SVB (con fechas y mecanismo). |
| `dim_severidad.csv` | 3 | normal, alerta y estrés, con umbrales y acción sugerida. |
| `consulta_prueba_horas_por_evento_fuente.csv` | 6 | Resultado de la consulta de prueba en Spark SQL: horas en alerta y estrés por evento y fuente. |

`spread_btc_usdc_usdt` no se incluye a propósito: deriva de la etiqueta (ver `notebooks/07_validacion_fuga.ipynb`).

Para regenerar estos archivos, ver la sección 5.9 del [README principal](../../README.md).
