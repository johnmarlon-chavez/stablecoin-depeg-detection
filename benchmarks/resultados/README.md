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

## Pedido de Escalabilidad (3.7): corridas en una sola maquina (`--master local[*]`)

Master debian-bigdata: 2 vCPU, 3,915 MB de RAM, 974 MiB de swap. `spark-submit --master local[*] --driver-memory 3g`, `spark.network.timeout=600s`. Script: `bench_local.sh` (muestrea memoria cada 5 s en los `*_mem.csv`). Un solo DataNode vivo; salidas solo en `hdfs:///datalake/benchmark/`. Cada escala se midio una sola vez. Los logs grandes de 100x van comprimidos (`.gz`).

| Corrida | Filas de la replica | Resultado | Tiempo real (s) | RSS max JVM (MB) | RAM max usada (MB) | Swap max (MB) | Log |
|---|---|---|---|---|---|---|---|
| quality.py 10x | 519,190 | OK | 235.639 | 1,574 | 3,283 | 107 | escalabilidad/local_orig_10x_quality.log |
| join 10x | 519,190 | OK | 80.233 | 1,020 | 2,723 | 129 | escalabilidad/local_orig_10x_join.log |
| quality_opt.py 100x | 5,191,900 | OK | 934.318 | 2,577 | 3,858 | 974 | escalabilidad/local_opt_100x_quality.log.gz |
| join 100x | 5,191,900 | OK (5,390,242 filas) | 472.870 | 1,447 | 2,955 | 617 | escalabilidad/local_opt_100x_join.log.gz |
| quality_opt.py 150x | 7,787,850 | SIGKILL, codigo 137 (OOM killer) | 342.504 | 3,002 | 3,910 | 974 | escalabilidad/local_opt_150x_quality.log |
| quality_opt.py 200x | 10,383,800 | SIGKILL, codigo 137 (OOM killer) | 247.042 | 2,904 | 3,896 | 974 | escalabilidad/local_opt_200x_quality.log |

Evidencia del kernel (`dmesg`): proceso 47328 (200x, anon-rss 3,042,552 kB) y proceso 48806 (150x, anon-rss 3,104,580 kB). Los conteos de 100x local coinciden con el cluster: 5,191,900 (limpieza) y 5,390,242 (join). Replicas: `replicate_150x_local.log`, `replicate_200x_local.log`.
