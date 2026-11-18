# architecture/ — Arquitectura del sistema

`system_architecture.png` es el diagrama de la arquitectura: ingesta desde data.binance.vision, zonas raw y processed en HDFS, procesamiento con Spark sobre YARN, capa de consumo y tablero.

- **Línea continua** = implementado (ruta por lotes, capa de consumo y tablero).
- **Línea punteada** = planificado (ruta de velocidad).
- La zona `/datalake/benchmark/` va aparte y solo sirve para medir el rendimiento.

Hay una copia en `informe/fig_arquitectura.png`. Si se actualiza el diagrama, hay que actualizar las dos.
