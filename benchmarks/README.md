# benchmarks/ — Evidencia de rendimiento y escalabilidad

Esta carpeta guarda las **salidas crudas** (logs, CSV de memoria y resúmenes) de las pruebas de rendimiento hechas en el clúster y en la máquina sola. Aquí no hay código: los scripts que generaron estos archivos están en `src/` y en los `.sh` de la raíz.

| Carpeta | Qué contiene | Generado por |
|---|---|---|
| `resultados/pedido2_*` | Broadcast frente a sort-merge join: planes físicos, tiempos y verificación. | `src/benchmark_join.py`, `src/benchmark_join_verif.py` |
| `resultados/pedido5/` | Escalabilidad del pipeline de klines en YARN (1x, 10x y 100x), original y optimizado. | `run_pedido5.sh`, `bench_scale.sh`, `bench_opt.sh`, `bench_join.sh` |
| `resultados/escalabilidad/` | Las mismas pruebas en una sola máquina (`local[*]`) hasta 200x, con la memoria muestreada. | `bench_local.sh` |
| `resultados/aggtrades/` | Descarga, ingesta, resumen horario y validación de aggTrades (Pedido 6). | `scripts/download_aggtrades.py`, `src/ingest_aggtrades.py`, `src/aggtrades_hourly.py` |
| `resultados/escalabilidad_aggtrades/` | aggTrades: local frente a clúster, de 1x a 5x y 3 repeticiones (Pedido 9). | `run_escalabilidad_aggtrades.sh` + `src/bench_aggtrades.py` |
| `resultados/consumo/` | Validación de la capa de consumo (líneas `QC`). | `src/capa_consumo.py` |

El detalle de cada log y la tabla de tiempos están en [`resultados/README.md`](resultados/README.md). El notebook `notebooks/06_benchmarks.ipynb` lee estos archivos y los resume en tablas y gráficos.

**Regla:** estos archivos son evidencia. No se editan a mano. Si una cifra no coincide con el informe, vale la del informe y la diferencia se reporta.
