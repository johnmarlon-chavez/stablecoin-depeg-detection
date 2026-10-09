# Reporte de verificación de calidad y reproducibilidad

**Responsable:** Adriel Zumaeta Calderón (Calidad y documentación)
**Fecha:** 9 de octubre de 2026
**Objeto revisado:** `data/klines_clean/` (zona procesada, salida de `src/quality.py`) y `README.md`
**Herramienta:** `aportes/adriel/verificar_calidad.py` (pandas, independiente del job de Spark)

## 1. Cómo reproducir

Desde la raíz del repositorio:

```bash
pip install -r requirements.txt
python aportes/adriel/verificar_calidad.py
```

Las salidas quedan en `aportes/adriel/resultados/`:

| Archivo | Contenido |
|---|---|
| `chequeos.json` | Resultado de cada regla (OK / REVISAR y número de filas que fallan) |
| `cobertura_por_simbolo.csv` | Filas, rango de fechas, precio mínimo/máximo y % de completitud por símbolo |
| `huecos_horarios.csv` | Cada tramo de horas faltantes por símbolo (desde, hasta, horas) |
| `fig_cobertura_simbolos.png` | Línea de tiempo de cobertura por símbolo con los tres eventos |
| `salida_consola.txt` | Salida completa de la última ejecución |

## 2. Resultado de los chequeos (51 919 filas)

| Estado | Fallas | Chequeo |
|---|---:|---|
| OK | 0 | Esquema: columnas y tipos |
| OK | 0 | Nulos en columnas obligatorias |
| OK | 0 | Duplicados por (simbolo, open_time) |
| OK | 0 | Coherencia OHLC (precios > 0, low ≤ open/close ≤ high) |
| OK | 0 | Volúmenes y trades no negativos |
| OK | 0 | taker_buy_base ≤ volume |
| REVISAR | 3 | close_time = open_time + 1 h − 1 ms (vela horaria completa) |
| OK | 0 | open_time alineado a la hora exacta |
| OK | 0 | open_time_ts en UTC (coincide con open_time) |
| REVISAR | 350 | Partición anio/mes coincide con el timestamp |
| OK | 0 | fuente_paridad correcta por símbolo (decisión B.1) |
| REVISAR | 1 | Stablecoins dentro de [0.80, 1.20] |

Las reglas que aplica `src/quality.py` (duplicados, rango OHLC) se cumplen en la salida. Los tres puntos marcados como REVISAR son controles nuevos que el job de Spark no hace; se detallan abajo.

## 3. Cobertura por símbolo

| Símbolo | Filas | Inicio (UTC) | Fin (UTC) | Completitud |
|---|---:|---|---|---:|
| BTCUSDT | 17 519 | 2022-01-01 00:00 | 2023-12-31 23:00 | 99,99 % |
| BTCUSDC | 13 580 | 2022-01-01 00:00 | 2023-12-31 23:00 | 77,51 % |
| USDCUSDT | 13 524 | 2022-01-01 00:00 | 2023-12-31 23:00 | 77,19 % |
| BUSDUSDT (proxy) | 3 648 | 2022-10-01 00:00 | 2023-03-10 23:00 | 94,41 % |
| BTCBUSD (proxy) | 3 648 | 2022-10-01 00:00 | 2023-03-10 23:00 | 94,41 % |

![Cobertura por símbolo](resultados/fig_cobertura_simbolos.png)

## 4. Hallazgos

### H1. Horas sin ninguna serie de paridad (USDCUSDT ni proxy BUSDUSDT)

Combinando USDCUSDT con el proxy BUSDUSDT, quedan 348 horas sin dato de paridad:

| Desde (UTC) | Hasta (UTC) | Horas | Comentario |
|---|---|---:|---|
| 2022-09-26 03:00 | 2022-09-30 23:00 | 117 | USDCUSDT termina el 26-sep; el proxy empieza el 1-oct |
| 2023-03-01 00:00 | 2023-03-09 23:00 | 216 | El proxy solo trae hasta feb-2023 y el 10-mar; falta 1–9 de marzo |
| 2023-03-11 00:00 | 2023-03-11 13:00 | 14 | Ni el proxy (termina el 10-mar) ni USDCUSDT (vuelve el 11-mar a las 14:00) |
| 2023-03-24 13:00 | 2023-03-24 13:00 | 1 | Hueco común a todos los pares (ver H2) |

El tramo del **11 de marzo de 2023, de 00:00 a 13:00 UTC**, es el más delicado: corresponde a las primeras horas del depeg de USDC tras el colapso de SVB. El mínimo que registra la serie limpia es 0,882 a las 14:00, la primera hora disponible, así que el punto más bajo del evento puede haber quedado fuera de los datos. Esto responde en parte al "pendiente de verificar" de la decisión B.1 en `docs/decisiones_abiertas.md`: en la zona procesada, USDCUSDT no tiene filas entre el 26-sep-2022 y el 11-mar-2023 a las 14:00.

**Recomendación:** declarar estas horas en la sección 3.4 del informe y en la limitación del evento SVB. Revisar si la ventana del 1 al 9 de marzo se puede completar con el archivo mensual `BUSDUSDT-1h-2023-03` o con los diarios de esas fechas, ya que hoy el proxy solo se cargó hasta febrero más el día 10.

### H2. Vela truncada y hora faltante el 24 de marzo de 2023

La vela de las 12:00 UTC del 24-mar-2023 cierra a las 12:39 (no a las 12:59:59.999) en BTCUSDT, BTCUSDC y USDCUSDT, y la hora 13:00 falta en los tres. El patrón, que se repite igual en todos los pares, coincide con la suspensión temporal del trading spot que tuvo Binance ese día. No es un error del pipeline, sino un hueco de la fuente.

**Recomendación:** dejarlo documentado como hueco de origen; no afecta a las ventanas de los tres eventos.

### H3. Partición `anio`/`mes` desplazada cinco horas

En 350 filas (5 por mes y símbolo) la partición no coincide con la fecha UTC: las velas de 00:00 a 04:00 UTC del día 1 de cada mes quedan guardadas en la partición del mes anterior. Por ejemplo, `2022-11-01 00:00 UTC` está en `mes=10`. El desfase de 5 horas es el de UTC−5 (hora de Perú): `src/quality.py` calcula `F.year("open_time_ts")` y `F.month("open_time_ts")` con la zona horaria de la sesión de Spark, que en esa corrida no estaba fijada en UTC.

Los valores de `open_time` y `open_time_ts` sí son correctos (chequeo OK), así que **ningún dato está mal**. Solo podría afectar a una consulta que filtre por partición (`WHERE mes = 11`), que perdería las primeras 5 horas del mes.

**Recomendación:** fijar `spark.sql.session.timeZone=UTC` dentro de `src/quality.py` (el README ya indica UTC como zona de sesión) y regenerar `klines_clean`.

### H4. Pico aislado de USDCUSDT durante Terra

La vela `2022-05-11 20:00 UTC` de USDCUSDT tiene `high = 3,99` con `open = 1,0029` y `close = 1,0051`. Es una mecha aislada en plena crisis de Terra/UST, no un cambio de nivel: la apertura y el cierre de esa hora siguen pegados a 1. Pasa la regla OHLC porque es internamente coherente.

**Recomendación:** no eliminarla (es dato real de la fuente), pero no usar `high`/`low` sin acotar en features. Hoy `src/features.py` trabaja con `close`, por lo que el modelo no se ve afectado.

## 5. Revisión del README (reproducibilidad)

| # | Problema | Estado |
|---|---|---|
| R1 | La sección 4.4 indica `bash benchmarks/run_pedido5.sh`, `benchmarks/bench_scale.sh` y `benchmarks/bench_local.sh`, pero esos scripts están en la raíz del repositorio; `benchmarks/` solo contiene `resultados/`. Los comandos fallan tal como están escritos. | Corregido |
| R2 | La estructura de carpetas describe `benchmarks/` como "scripts y resultados" y no menciona los scripts de la raíz. | Corregido |
| R3 | La lista de notebooks omite `01_ingesta.ipynb` y `06_benchmarks.ipynb`. | Corregido |
| R4 | El rol de Adriel Zumaeta figuraba como "integrante del equipo" en lugar de "calidad y documentación" (sección 0.1 del informe). | Corregido |
| R5 | No había un paso para verificar la zona procesada sin el clúster. | Se agrega la referencia a este script |
