# aportes/ — Aportes individuales del equipo

Cada integrante tiene una subcarpeta con el trabajo propio de su rol, separado del pipeline principal.

## `adriel/` — Calidad y documentación (Adriel Zumaeta)

| Archivo | Qué es |
|---|---|
| `verificar_calidad.py` | Verificación **independiente** de `data/klines_clean/` con pandas (no reutiliza `src/quality.py`): esquema, nulos, duplicados, OHLC, `close_time`, alineación horaria, particiones, `fuente_paridad` del proxy y rango de precios de las stablecoins. |
| `resultados/` | Salidas del script: `chequeos.json`, `cobertura_por_simbolo.csv`, `huecos_horarios.csv`, `fig_cobertura_simbolos.png` y la salida de consola. |
| `reporte_verificacion.md` | Hallazgos de la verificación. |
| `seccion_3_4.md` | Propuesta de actualización de la sección 3.4 del informe (control de calidad). |

Se ejecuta sin clúster, desde la raíz del repositorio:

```bash
python aportes/adriel/verificar_calidad.py
```

## `juancarlos/` — Coordinación (Juan Carlos Vilca)

| Archivo | Qué es |
|---|---|
| `requisitos.md` | Documento de coordinación: equipo, roles y requisitos del proyecto. |
