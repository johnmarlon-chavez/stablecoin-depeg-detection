# docs/ — Decisiones del equipo

Documentación de las decisiones de diseño que afectan a los datos y al análisis.

| Archivo | Qué contiene |
|---|---|
| `decisiones_abiertas.md` | Registro de decisiones. La principal es la **B.1**: USDCUSDT y BTCUSDC no tienen datos en Binance entre el 27-sep-2022 y el 10/11-mar-2023 (Binance convirtió USDC a BUSD), así que en ese tramo se usan **BUSDUSDT** y **BTCBUSD** como sustitutos (proxy). Por eso existe la columna `fuente_paridad` en todos los datos. |

La citan, entre otros, `src/quality.py`, `notebooks/02_calidad_datos`, `notebooks/03_eda` y `aportes/adriel/verificar_calidad.py`. `scripts/download_klines.py` se refiere a ella como "Anexo B.1 de la guía operativa".
