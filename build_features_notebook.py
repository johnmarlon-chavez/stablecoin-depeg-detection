import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []

cells.append(nbf.v4.new_markdown_cell(
"""# Fase 5.1 — Feature Engineering

Construye las 5 variables acordadas con el equipo (ver `Respuesta_Marlon_Fase5.docx`,
sección 1.1) a partir del Parquet limpio de la Fase 3:

| Feature | Fuente | Propósito |
|---|---|---|
| `desviacion_paridad` | `close` del par de referencia (`USDCUSDT` o `BUSDUSDT_proxy` según `fuente_paridad`) | `ABS(close - 1.0)`, variable base de severidad |
| `severidad` | `desviacion_paridad` clasificada | Etiqueta de 3 niveles: normal / alerta / estrés |
| `volumen_relativo` | `volume` del par objetivo sobre su media móvil | Detecta picos de actividad |
| `spread_btc_usdc_usdt` | BTCUSDC vs BTCUSDT en el mismo instante | Señal de arbitraje / estrés de liquidez cruzada |
| `volatilidad_ventana` | desviación estándar móvil de los retornos (BTCUSDT, contexto de mercado) | Contexto de mercado, no solo el par objetivo |

Los umbrales de `severidad` se fijan con los **percentiles reales** de la distribución
histórica de `desviacion_paridad` (no números arbitrarios), como pide la sección 2.1
de la respuesta de Vilca."""
))

cells.append(nbf.v4.new_code_cell(
"""import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

pd.set_option("display.max_columns", None)
plt.rcParams["figure.figsize"] = (12, 4.5)

RUTA_DATOS = Path("../data/klines_clean")
RUTA_SALIDA = Path("../data/features.parquet")
"""
))

cells.append(nbf.v4.new_markdown_cell("## 1. Carga de datos"))

cells.append(nbf.v4.new_code_cell(
"""df = pd.read_parquet(RUTA_DATOS)
df = df.sort_values(["simbolo", "open_time_ts"]).reset_index(drop=True)

print(f"Total de filas: {len(df):,}")
print(f"Símbolos: {sorted(df['simbolo'].unique())}")

assert "fuente_paridad" in df.columns, "Falta la columna fuente_paridad"
print("\\nValores de fuente_paridad:", df["fuente_paridad"].unique())
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 2. Serie de paridad combinada (USDCUSDT + proxy BUSDUSDT)

Se concatenan las filas de `USDCUSDT` y `BUSDUSDT`. Como el proxy solo existe
en la ventana del hueco (donde USDCUSDT no tiene dato), la unión da una serie
continua de "precio de paridad" sin traslape, tal como ya se usó en el EDA
(Fase 4)."""
))

cells.append(nbf.v4.new_code_cell(
"""serie_paridad = df[df["simbolo"].isin(["USDCUSDT", "BUSDUSDT"])].copy()
serie_paridad = serie_paridad.sort_values("open_time_ts").reset_index(drop=True)

duplicados = serie_paridad["open_time_ts"].duplicated().sum()
print(f"Timestamps duplicados en la serie de paridad: {duplicados}")
assert duplicados == 0, "Hay traslape entre USDCUSDT y el proxy BUSDUSDT — revisar"

serie_paridad[["open_time_ts", "simbolo", "fuente_paridad", "close", "volume"]].head()
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 3. Feature 1 — `desviacion_paridad`

`desviacion_paridad = |close - 1.0|`, calculada sobre la serie de paridad combinada."""
))

cells.append(nbf.v4.new_code_cell(
"""serie_paridad["desviacion_paridad"] = (serie_paridad["close"] - 1.0).abs()

serie_paridad[["open_time_ts", "fuente_paridad", "close", "desviacion_paridad"]].describe()
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 4. Umbrales de severidad (percentiles reales)

Según la sección 2.1: percentil 90 para el corte normal→alerta, y percentil 99
(o el mínimo observado durante los 3 eventos ancla) para el corte alerta→estrés.
Se documentan aquí los valores exactos usados, con el histograma como evidencia."""
))

cells.append(nbf.v4.new_code_cell(
"""EVENTOS = {
    "Terra/UST (may-2022)": ("2022-05-07", "2022-05-17"),
    "FTX (nov-2022)": ("2022-11-05", "2022-11-15"),
    "SVB/USDC (mar-2023)": ("2023-03-08", "2023-03-18"),
}

p90 = serie_paridad["desviacion_paridad"].quantile(0.90)
p99 = serie_paridad["desviacion_paridad"].quantile(0.99)

desviacion_minima_por_evento = {}
for nombre, (inicio, fin) in EVENTOS.items():
    ventana = serie_paridad[
        (serie_paridad["open_time_ts"] >= inicio) & (serie_paridad["open_time_ts"] <= fin)
    ]
    desviacion_minima_por_evento[nombre] = ventana["desviacion_paridad"].max()

print(f"Percentil 90 de desviacion_paridad (histórico completo): {p90:.5f}")
print(f"Percentil 99 de desviacion_paridad (histórico completo): {p99:.5f}")
print("\\nPico de desviación (máximo) alcanzado en cada evento ancla:")
for nombre, valor in desviacion_minima_por_evento.items():
    print(f"  {nombre}: {valor:.5f}")

UMBRAL_ALERTA = p90
UMBRAL_ESTRES = min(p99, min(desviacion_minima_por_evento.values()))

print(f"\\nUmbral normal->alerta (p90):  {UMBRAL_ALERTA:.5f}")
print(f"Umbral alerta->estrés (min(p99, pico mínimo de evento)): {UMBRAL_ESTRES:.5f}")
"""
))

cells.append(nbf.v4.new_code_cell(
"""fig, ax = plt.subplots()
ax.hist(serie_paridad["desviacion_paridad"], bins=200, color="steelblue")
ax.axvline(UMBRAL_ALERTA, color="orange", linestyle="--", label=f"Umbral alerta (p90={UMBRAL_ALERTA:.4f})")
ax.axvline(UMBRAL_ESTRES, color="red", linestyle="--", label=f"Umbral estrés ({UMBRAL_ESTRES:.4f})")
ax.set_yscale("log")
ax.set_xlabel("desviacion_paridad")
ax.set_ylabel("Frecuencia (escala log)")
ax.set_title("Distribución histórica de desviacion_paridad y umbrales elegidos")
ax.legend()
plt.tight_layout()
plt.savefig("../informe/fig_umbrales_severidad.png", dpi=150)
plt.show()
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 5. Feature 2 — `severidad`

Clasificación de 3 niveles usando los umbrales definidos arriba."""
))

cells.append(nbf.v4.new_code_cell(
"""def clasificar_severidad(desviacion):
    if desviacion >= UMBRAL_ESTRES:
        return "estres"
    elif desviacion >= UMBRAL_ALERTA:
        return "alerta"
    return "normal"

serie_paridad["severidad"] = serie_paridad["desviacion_paridad"].apply(clasificar_severidad)

conteo = serie_paridad["severidad"].value_counts()
print(conteo)
print(f"\\nProporción:\\n{(conteo / len(serie_paridad) * 100).round(2)}")
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 6. Feature 3 — `volumen_relativo`

`volume` del par de paridad sobre su media móvil de 24 horas (ventana de 1 día,
ya que los datos son horarios)."""
))

cells.append(nbf.v4.new_code_cell(
"""VENTANA_VOLUMEN = 24  # horas

serie_paridad["volumen_media_movil"] = (
    serie_paridad["volume"].rolling(window=VENTANA_VOLUMEN, min_periods=1).mean()
)
serie_paridad["volumen_relativo"] = serie_paridad["volume"] / serie_paridad["volumen_media_movil"]

serie_paridad[["open_time_ts", "volume", "volumen_media_movil", "volumen_relativo"]].describe()
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 7. Feature 4 — `spread_btc_usdc_usdt`

Diferencia relativa entre el precio de BTC cotizado en USDC y en USDT, en el
mismo instante. Señal de arbitraje / estrés de liquidez cruzada: si USDC pierde
confianza, el precio de BTCUSDC se desvía del de BTCUSDT.

**Corrección aplicada**: BTCUSDC tiene el mismo hueco de datos que USDCUSDT
(confirmado en la Fase 3 — conteo bajo de filas en 2023-03). Sin proxy, el
spread quedaría `NaN` en todo el evento FTX y el `dropna()` de la Fase 5.3
vaciaría por completo esa combinación de validación cruzada. Se usa **BTCBUSD**
como proxy de BTCUSDC en la misma ventana (oct-2022 a feb-2023 y 10-mar-2023),
igual que BUSDUSDT es proxy de USDCUSDT. Se agrega `fuente_btc_usdc` para
trazar dónde se usó el proxy."""
))

cells.append(nbf.v4.new_code_cell(
"""serie_btc_usdc = df[df["simbolo"].isin(["BTCUSDC", "BTCBUSD"])].copy()
serie_btc_usdc = serie_btc_usdc.sort_values("open_time_ts").reset_index(drop=True)
serie_btc_usdc = serie_btc_usdc.rename(columns={
    "close": "close_btcusdc", "fuente_paridad": "fuente_btc_usdc"
})[["open_time_ts", "fuente_btc_usdc", "close_btcusdc"]]

duplicados_btc = serie_btc_usdc["open_time_ts"].duplicated().sum()
print(f"Timestamps duplicados en la serie BTC-USDC combinada: {duplicados_btc}")
assert duplicados_btc == 0, "Hay traslape entre BTCUSDC y el proxy BTCBUSD — revisar"

btc_usdt = df[df["simbolo"] == "BTCUSDT"][["open_time_ts", "close"]].rename(columns={"close": "close_btcusdt"})

spread_df = pd.merge(serie_btc_usdc, btc_usdt, on="open_time_ts", how="inner")
spread_df["spread_btc_usdc_usdt"] = (
    (spread_df["close_btcusdc"] - spread_df["close_btcusdt"]) / spread_df["close_btcusdt"]
)

print(f"\\nFilas con spread calculado: {len(spread_df):,}")
print(f"Filas por fuente_btc_usdc:\\n{spread_df['fuente_btc_usdc'].value_counts()}")
spread_df[["open_time_ts", "fuente_btc_usdc", "close_btcusdc", "close_btcusdt", "spread_btc_usdc_usdt"]].describe()
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 8. Feature 5 — `volatilidad_ventana`

Desviación estándar móvil (24h) de los retornos porcentuales de BTCUSDT, como
contexto general del mercado (no solo el par objetivo), tal como pide la
sección 1.1."""
))

cells.append(nbf.v4.new_code_cell(
"""VENTANA_VOLATILIDAD = 24  # horas

btc_mercado = df[df["simbolo"] == "BTCUSDT"][["open_time_ts", "close"]].sort_values("open_time_ts").copy()
btc_mercado["retorno"] = btc_mercado["close"].pct_change()
btc_mercado["volatilidad_ventana"] = (
    btc_mercado["retorno"].rolling(window=VENTANA_VOLATILIDAD, min_periods=2).std()
)

btc_mercado[["open_time_ts", "close", "retorno", "volatilidad_ventana"]].describe()
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 9. Ensamblado del dataset de features

Se combinan las 5 features en un solo DataFrame, indexado por `open_time_ts`,
conservando `fuente_paridad` y `fuente_btc_usdc` para poder distinguir después,
en la Fase 5.3, entre desempeño sobre dato real y desempeño sobre el proxy
(sección 2.2)."""
))

cells.append(nbf.v4.new_code_cell(
"""features = serie_paridad[[
    "open_time_ts", "fuente_paridad", "close", "volume",
    "desviacion_paridad", "severidad", "volumen_relativo",
]].copy()

features = features.merge(
    spread_df[["open_time_ts", "fuente_btc_usdc", "spread_btc_usdc_usdt"]], on="open_time_ts", how="left"
)
features = features.merge(
    btc_mercado[["open_time_ts", "volatilidad_ventana"]], on="open_time_ts", how="left"
)

features = features.sort_values("open_time_ts").reset_index(drop=True)

print(f"Dataset de features: {len(features):,} filas, {features.shape[1]} columnas")
print(f"Nulos por columna:\\n{features.isna().sum()}")
features.head()
"""
))

cells.append(nbf.v4.new_code_cell(
"""features.to_parquet(RUTA_SALIDA, index=False)
print(f"Guardado en: {RUTA_SALIDA.resolve()}")
"""
))

cells.append(nbf.v4.new_markdown_cell(
"""## 10. Resumen para el informe

- Umbrales de severidad: normal→alerta = p90 de la distribución histórica
  (ver celda de la sección 4); alerta→estrés = mínimo entre el p99 histórico
  y el pico más bajo de los tres eventos ancla (para no excluir el evento más leve).
- Figura `informe/fig_umbrales_severidad.png` con el histograma y los umbrales marcados.
- Feature `spread_btc_usdc_usdt` ahora completa en las 3 ventanas de evento gracias
  al proxy BTCBUSD (columna `fuente_btc_usdc` trazando su origen).
- Dataset de features guardado en `data/features.parquet`.
- Siguiente paso: `05_modelo_baseline.ipynb` — clasificador de 3 niveles con las
  3 combinaciones de validación cruzada (Terra+SVB→FTX, Terra+FTX→SVB, FTX+SVB→Terra)."""
))

nb["cells"] = cells

with open("notebooks/04_features.ipynb", "w") as f:
    nbf.write(nb, f)

print("Notebook generado en notebooks/04_features.ipynb")
