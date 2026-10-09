"""
Verificación independiente de calidad sobre data/klines_clean (zona procesada).

Rol: Calidad y documentación (Adriel Zumaeta).

No reutiliza src/quality.py: vuelve a comprobar con pandas, sobre la salida
ya limpia, que las reglas de la sección 3.4 del informe se cumplan, y agrega
controles que el job de Spark no hace (partición vs. timestamp, close_time,
fuente_paridad del proxy, rango de precios de las stablecoins).

Uso (desde la raíz del repositorio):
    python aportes/adriel/verificar_calidad.py
    python aportes/adriel/verificar_calidad.py --input data/klines_clean --salida aportes/adriel/resultados
"""

import argparse
import json
from pathlib import Path

import pandas as pd

HORA_MS = 60 * 60 * 1000

COLUMNAS_ESPERADAS = {
    "open_time": "int64", "open": "float64", "high": "float64",
    "low": "float64", "close": "float64", "volume": "float64",
    "close_time": "int64", "quote_volume": "float64", "trades": "int64",
    "taker_buy_base": "float64", "taker_buy_quote": "float64",
    "fuente_paridad": "object", "open_time_ts": "datetime64[ns]",
}

# fuente_paridad que debe llevar cada símbolo (decisión B.1 en docs/decisiones_abiertas.md)
FUENTE_ESPERADA = {
    "USDCUSDT": "USDCUSDT",
    "BTCUSDC": "BTCUSDC",
    "BTCUSDT": "BTCUSDT",
    "BUSDUSDT": "BUSDUSDT_proxy",
    "BTCBUSD": "BTCBUSD_proxy",
}

STABLECOINS = ["USDCUSDT", "BUSDUSDT"]
# Banda amplia: el mínimo histórico de USDC en SVB fue ~0.87, así que fuera
# de [0.80, 1.20] se considera dato sospechoso, no un depeg.
RANGO_STABLE = (0.80, 1.20)


def cargar(ruta: Path) -> pd.DataFrame:
    df = pd.read_parquet(ruta)
    for col in ("simbolo", "anio", "mes"):
        df[col] = df[col].astype(str)
    return df.sort_values(["simbolo", "open_time"]).reset_index(drop=True)


def chequeo(nombre, fallas, detalle=""):
    return {"chequeo": nombre, "fallas": int(fallas),
            "estado": "OK" if fallas == 0 else "REVISAR", "detalle": detalle}


def verificar(df: pd.DataFrame):
    resultados = []

    faltantes = [c for c in COLUMNAS_ESPERADAS if c not in df.columns]
    tipos_mal = [c for c, t in COLUMNAS_ESPERADAS.items()
                 if c in df.columns and str(df[c].dtype) != t]
    resultados.append(chequeo("Esquema: columnas y tipos",
                              len(faltantes) + len(tipos_mal),
                              f"faltantes={faltantes} tipos_distintos={tipos_mal}"))

    nulos = df[list(COLUMNAS_ESPERADAS)].isna().sum()
    resultados.append(chequeo("Nulos en columnas obligatorias", nulos.sum(),
                              str(nulos[nulos > 0].to_dict())))

    dup = df.duplicated(["simbolo", "open_time"]).sum()
    resultados.append(chequeo("Duplicados por (simbolo, open_time)", dup))

    ohlc_mal = ~(
        (df[["open", "high", "low", "close"]] > 0).all(axis=1)
        & (df["high"] >= df[["open", "close", "low"]].max(axis=1))
        & (df["low"] <= df[["open", "close"]].min(axis=1))
    )
    resultados.append(chequeo("Coherencia OHLC (precios > 0, low <= open/close <= high)",
                              ohlc_mal.sum()))

    neg = (df[["volume", "quote_volume", "trades",
               "taker_buy_base", "taker_buy_quote"]] < 0).any(axis=1).sum()
    resultados.append(chequeo("Volúmenes y trades no negativos", neg))

    taker_mal = (df["taker_buy_base"] > df["volume"] * (1 + 1e-9)).sum()
    resultados.append(chequeo("taker_buy_base <= volume", taker_mal))

    cierre_mal = (df["close_time"] - df["open_time"] != HORA_MS - 1).sum()
    resultados.append(chequeo("close_time = open_time + 1h - 1ms (vela horaria)", cierre_mal))

    alineado_mal = (df["open_time"] % HORA_MS != 0).sum()
    resultados.append(chequeo("open_time alineado a la hora exacta", alineado_mal))

    ts_utc = pd.to_datetime(df["open_time"], unit="ms")
    ts_mal = (ts_utc != df["open_time_ts"]).sum()
    resultados.append(chequeo("open_time_ts en UTC (coincide con open_time)", ts_mal))

    part_mal = ((ts_utc.dt.year.astype(str) != df["anio"])
                | (ts_utc.dt.month.astype(str) != df["mes"])).sum()
    resultados.append(chequeo("Partición anio/mes coincide con el timestamp", part_mal))

    fuente_mal = (df["fuente_paridad"] != df["simbolo"].map(FUENTE_ESPERADA)).sum()
    resultados.append(chequeo("fuente_paridad correcta por símbolo (decisión B.1)", fuente_mal))

    st = df[df["simbolo"].isin(STABLECOINS)]
    fuera = ((st["low"] < RANGO_STABLE[0]) | (st["high"] > RANGO_STABLE[1])).sum()
    resultados.append(chequeo(f"Stablecoins dentro de [{RANGO_STABLE[0]}, {RANGO_STABLE[1]}]",
                              fuera))

    return resultados


def huecos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["gap_ms"] = df.groupby("simbolo")["open_time"].diff()
    h = df[df["gap_ms"].notna() & (df["gap_ms"] != HORA_MS)].copy()
    h["desde"] = pd.to_datetime(h["open_time"] - h["gap_ms"] + HORA_MS, unit="ms")
    h["hasta"] = pd.to_datetime(h["open_time"] - HORA_MS, unit="ms")
    h["horas_faltantes"] = (h["gap_ms"] // HORA_MS - 1).astype(int)
    return h[["simbolo", "desde", "hasta", "horas_faltantes"]].reset_index(drop=True)


def cobertura(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("simbolo").agg(
        filas=("open_time", "size"),
        inicio=("open_time_ts", "min"),
        fin=("open_time_ts", "max"),
        precio_min=("low", "min"),
        precio_max=("high", "max"),
    )
    g["horas_esperadas"] = ((g["fin"] - g["inicio"]) / pd.Timedelta(hours=1)).astype(int) + 1
    g["completitud_%"] = (100 * g["filas"] / g["horas_esperadas"]).round(2)
    return g.reset_index()


def figura_cobertura(df: pd.DataFrame, ruta: Path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    orden = ["USDCUSDT", "BUSDUSDT", "BTCUSDC", "BTCBUSD", "BTCUSDT"]
    fig, ax = plt.subplots(figsize=(10, 3.2))
    for i, sim in enumerate(orden):
        ts = df.loc[df["simbolo"] == sim, "open_time_ts"].sort_values()
        tramo = (ts.diff() != pd.Timedelta(hours=1)).cumsum()
        for _, t in ts.groupby(tramo):
            ax.barh(i, t.iloc[-1] - t.iloc[0] + pd.Timedelta(hours=1),
                    left=t.iloc[0], height=0.6, color="#3b6ea5")
    for nombre, fecha in [("Terra", "2022-05-09"), ("FTX", "2022-11-08"), ("SVB", "2023-03-10")]:
        ax.axvline(pd.Timestamp(fecha), color="#c0392b", lw=1, ls="--")
        ax.text(pd.Timestamp(fecha), -0.45, " " + nombre, color="#c0392b", fontsize=8, va="bottom")
    ax.set_yticks(range(len(orden)), orden)
    ax.invert_yaxis()
    ax.set_title("Cobertura horaria por símbolo en data/klines_clean (UTC)")
    fig.tight_layout()
    fig.savefig(ruta, dpi=150)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/klines_clean")
    p.add_argument("--salida", default="aportes/adriel/resultados")
    args = p.parse_args()

    df = cargar(Path(args.input))
    salida = Path(args.salida)
    salida.mkdir(parents=True, exist_ok=True)

    res = verificar(df)
    cob = cobertura(df)
    hue = huecos(df)

    print(f"Filas leídas: {len(df)}\n")
    print(pd.DataFrame(res)[["estado", "fallas", "chequeo"]].to_string(index=False))
    print("\nCobertura por símbolo:")
    print(cob.to_string(index=False))
    print(f"\nHuecos horarios: {len(hue)} tramos, {hue['horas_faltantes'].sum()} horas")
    if len(hue):
        print(hue.groupby("simbolo")["horas_faltantes"].agg(["count", "sum", "max"]).to_string())

    cob.to_csv(salida / "cobertura_por_simbolo.csv", index=False)
    hue.to_csv(salida / "huecos_horarios.csv", index=False)
    with open(salida / "chequeos.json", "w", encoding="utf-8") as f:
        json.dump({"filas": len(df), "chequeos": res}, f, ensure_ascii=False, indent=2)
    figura_cobertura(df, salida / "fig_cobertura_simbolos.png")
    print(f"\nResultados guardados en {salida}/")


if __name__ == "__main__":
    main()
