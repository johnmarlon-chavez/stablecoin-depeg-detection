# Salidas crudas de los benchmarks

## Pedido 2 — Broadcast join frente a sort-merge join (sección 3.6)

Zonas raw y processed solo leídas; todo se escribió en `hdfs:///datalake/benchmark/`.

- `pedido2_explain_output.txt`: planes físicos (`explain`) de la unión con sort-merge join (autoBroadcastJoinThreshold=-1, AQE desactivado) y con broadcast join explícito.
- `pedido2_run.log`: salida completa de `src/benchmark_join.py` (6 corridas por variante, la primera de calentamiento se descarta).
- `pedido2_ui.log`: corrida de `src/benchmark_join_verif.py --modo ui`, usada para capturar el Shuffle Read/Write y los stages en la Spark UI.
- `pedido2_verif.log`: corrida de `src/benchmark_join_verif.py --modo verif` (conteos y `exceptAll` entre ambas variantes: 48,271 filas, 0 diferencias).

Las cifras y su análisis están en el informe del Pedido 2.
