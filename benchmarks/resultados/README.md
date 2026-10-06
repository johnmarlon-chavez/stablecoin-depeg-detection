# Salidas crudas de los benchmarks

## Pedido 2 — Broadcast join frente a sort-merge join (sección 3.6)

Zonas raw y processed solo leídas; todo se escribió en `hdfs:///datalake/benchmark/`.

- `pedido2_explain_output.txt`: planes físicos (`explain`) de la unión con sort-merge join (autoBroadcastJoinThreshold=-1, AQE desactivado) y con broadcast join explícito.
- `pedido2_run.log`: salida completa de `src/benchmark_join.py` (6 corridas por variante, la primera de calentamiento se descarta).
- `pedido2_ui.log`: corrida de `src/benchmark_join_verif.py --modo ui`, usada para capturar el Shuffle Read/Write y los stages en la Spark UI.
- `pedido2_verif.log`: corrida de `src/benchmark_join_verif.py --modo verif` (conteos y `exceptAll` entre ambas variantes: 48,271 filas, 0 diferencias).

Las cifras y su análisis están en el informe del Pedido 2.

## Pedido 5 — Prueba de escalabilidad (sección 3.7)

Las réplicas se generaron con `src/replicate_raw.py` en `hdfs:///datalake/benchmark/` (`raw_10x`, `raw_100x`); las zonas raw y processed solo se leyeron. Las corridas se lanzaron con `bench_scale.sh` (pipeline original), `bench_opt.sh` (limpieza optimizada, `src/quality_opt.py`) y `bench_join.sh` (unión con broadcast, `src/join_broadcast_solo.py`). Los logs están en `pedido5/`.

Los tiempos de la tabla son los que contiene cada log: en `q_*` y `j_*`, el `time` del proceso completo (incluye el arranque de Spark); en `qopt_*`, el `TOTAL` medido dentro del script. No son comparables entre sí columna a columna, y las cifras oficiales son las del informe del Pedido 5.

| Log | Qué es | Tiempo en el log |
|---|---|---|
| `q_1x_r2.log` | `quality.py` original, 1x | 2 min 30.6 s (`time`) |
| `q_10x_r2.log` | `quality.py` original, 10x | 8 min 38.8 s (`time`) |
| `j_1x_r2.log` | unión con broadcast, 1x | 1 min 16.9 s (`time`) |
| `j_10x_r2.log` | unión con broadcast, 10x | 2 min 24.3 s (`time`) |
| `qopt_1x_simbolo.log` | `quality_opt.py`, 1x, ventana por símbolo | 190.0 s |
| `qopt_10x_simbolo.log` | `quality_opt.py`, 10x, ventana por símbolo | 351.3 s |
| `qopt_10x_mes.log` | `quality_opt.py`, 10x, ventana por mes | 387.0 s |
| `qopt_10x_simbolo_sp16.log` | 10x, símbolo, `shuffle.partitions`=16 | 233.3 s |
| `qopt_10x_simbolo_sp16_rw16.log` | 10x, símbolo, `shuffle.partitions`=16, `--rep-escritura` 16 | 256.2 s |
| `qopt_100x_mes_sp64_rw0.log` | 100x, ventana por mes, `shuffle.partitions`=64, `--rep-escritura` 0 | 2005.0 s |
| `jopt_100x.log` | unión con broadcast, 100x (segundo intento, completo) | no está en el log (ver informe) |
| `jopt_100x_fail1_oom.log` | primer intento de la unión 100x: el proceso se terminó por falta de memoria en el master; el log acaba sin cierre limpio | — |
| `pedido5_log_completo.txt` | intento fallido del 2-oct: los ejecutores no pudieron conectarse al worker (`Conexión rehusada`) | — |
| `pedido5_log_completo_v2.txt` | intento fallido del 2-oct: la unión 100x falló por `BlockMissingException` (bloques de `clean_100x` no disponibles en HDFS) | — |

Los tres últimos logs (intentos fallidos) no se usan para las cifras del informe; se conservan como evidencia de cómo se llegó a la configuración final.
