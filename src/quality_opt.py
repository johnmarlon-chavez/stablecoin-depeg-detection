"""
Fase 3 — Calidad de datos y limpieza (Spark) — VERSION OPTIMIZADA (experimento Pedido 5)

Mismas reglas y misma salida que src/quality.py, pero:

  1. Persiste (MEMORY_AND_DISK) los DataFrames intermedios que se reutilizan,
     en vez de releer los CSV y repetir los shuffles en cada count()/show()/write.
  2. Reemplaza los ~9 count() separados por 2 agregaciones (una sobre el
     DataFrame deduplicado y otra sobre el DataFrame con gaps).
  3. Opcional (--ventana mes): la ventana lag() se particiona por
     (simbolo, año-mes) en vez de solo por simbolo. Con 4 simbolos, la ventana
     original manda todos los datos de un simbolo a UNA sola tarea; por mes hay
     muchas tareas. Los huecos en la frontera entre meses se calculan aparte
     con una tabla diminuta (min/max open_time por simbolo y mes), asi el
     resultado es identico.
  4. Termina con codigo distinto de 0 si algo falla o si no existe _SUCCESS.

Uso:
    spark-submit ... src/quality_opt.py --input <raw> --output <salida> \
        [--ventana simbolo|mes]
"""

import argparse
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, StringType
)

ESQUEMA_KLINES = StructType([
    StructField("open_time", LongType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("close_time", LongType(), False),
    StructField("quote_volume", DoubleType(), False),
    StructField("trades", LongType(), False),
    StructField("taker_buy_base", DoubleType(), False),
    StructField("taker_buy_quote", DoubleType(), False),
    StructField("ignore", StringType(), True),
])

SIMBOLOS = {
    "USDCUSDT": "USDCUSDT",
    "BTCUSDC": "BTCUSDC",
    "BTCUSDT": "BTCUSDT",
    "BUSDUSDT": "BUSDUSDT_proxy",
    "BTCBUSD": "BTCBUSD_proxy",
}

INTERVALO_ESPERADO_MS = 60 * 60 * 1000  # 1 hora


def leer_simbolo(spark, base_path, simbolo, fuente_paridad):
    rutas = [f"{base_path}/{simbolo}/*.csv"]
    if simbolo in ("BUSDUSDT", "BTCBUSD"):
        rutas.append(f"{base_path}/{simbolo}/diario/*.csv")
    df = spark.read.schema(ESQUEMA_KLINES).csv(rutas)
    return df.withColumn("simbolo", F.lit(simbolo)) \
             .withColumn("fuente_paridad", F.lit(fuente_paridad))


def convertir_tipos(df):
    return df \
        .withColumn("open_time_ts", (F.col("open_time") / 1000).cast("timestamp")) \
        .withColumn("close_time_ts", (F.col("close_time") / 1000).cast("timestamp"))


def condicion_rango_valido():
    return (
        (F.col("open") > 0) & (F.col("high") > 0) &
        (F.col("low") > 0) & (F.col("close") > 0) &
        (F.col("high") >= F.col("low")) &
        (F.col("high") >= F.col("open")) & (F.col("high") >= F.col("close")) &
        (F.col("low") <= F.col("open")) & (F.col("low") <= F.col("close"))
    )


def t(msg, t0):
    print(f"[TIEMPO] {msg}: {time.time() - t0:.1f} s", flush=True)


def aplicar_reglas_calidad(spark, df, ventana):
    """Devuelve (df_limpio, reporte, huecos_df). Persiste intermedios."""
    t0 = time.time()
    cond = condicion_rango_valido()

    # Unica lectura "suelta" de los CSV (solo scan, sin shuffle)
    total_inicial = df.count()
    t("total_inicial (scan)", t0)

    # Regla 2: duplicados. UN shuffle, resultado persistido.
    df_dedup = df.dropDuplicates(["simbolo", "open_time"]) \
                 .persist(StorageLevel.MEMORY_AND_DISK)

    # Reglas 2+3 en UNA sola agregacion sobre lo persistido
    r = df_dedup.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.when(cond, 1).otherwise(0)).alias("validos"),
    ).first()
    total_dedup = int(r["n"])
    total_validos = int(r["validos"] or 0)
    duplicados_removidos = total_inicial - total_dedup
    rechazados_por_rango = total_dedup - total_validos
    t("dedup + rango (1 agregacion)", t0)

    df_valido = df_dedup.filter(cond)

    # Regla 1: huecos de tiempo
    boundary_huecos = []  # solo modo 'mes'
    if ventana == "simbolo":
        w = Window.partitionBy("simbolo").orderBy("open_time")
        df_gap = df_valido.withColumn(
            "gap_ms", F.col("open_time") - F.lag("open_time").over(w))
    else:
        df_valido = df_valido.withColumn(
            "_per", F.year("open_time_ts") * 100 + F.month("open_time_ts"))
        w = Window.partitionBy("simbolo", "_per").orderBy("open_time")
        df_gap = df_valido.withColumn(
            "gap_ms", F.col("open_time") - F.lag("open_time").over(w))

    df_gap = df_gap.persist(StorageLevel.MEMORY_AND_DISK)

    es_hueco = F.col("gap_ms").isNotNull() & (F.col("gap_ms") != INTERVALO_ESPERADO_MS)
    r2 = df_gap.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.when(es_hueco, 1).otherwise(0)).alias("huecos"),
    ).first()
    total_final = int(r2["n"])
    huecos_internos = int(r2["huecos"] or 0)
    t("ventana lag + conteos (1 agregacion)", t0)

    # df_dedup ya no se necesita: df_gap esta persistido y materializado
    df_dedup.unpersist()

    huecos_df = df_gap.filter(es_hueco) \
                      .select("simbolo", "open_time_ts", "gap_ms")

    if ventana == "mes":
        # Frontera entre meses: tabla diminuta (simbolo x mes)
        filas = df_gap.groupBy("simbolo", "_per").agg(
            F.min("open_time").alias("t_min"),
            F.max("open_time").alias("t_max")
        ).orderBy("simbolo", "_per").collect()
        prev = {}
        for f in filas:
            s = f["simbolo"]
            if s in prev:
                gap = f["t_min"] - prev[s]
                if gap != INTERVALO_ESPERADO_MS:
                    boundary_huecos.append((s, int(f["t_min"]), int(gap)))
            prev[s] = f["t_max"]
        if boundary_huecos:
            bdf = spark.createDataFrame(
                boundary_huecos, "simbolo string, open_time long, gap_ms long") \
                .withColumn("open_time_ts",
                            (F.col("open_time") / 1000).cast("timestamp")) \
                .select("simbolo", "open_time_ts", "gap_ms")
            huecos_df = huecos_df.unionByName(bdf)
        t("frontera entre meses", t0)

    conteo_huecos = huecos_internos + len(boundary_huecos)

    reporte = {
        "total_inicial": total_inicial,
        "duplicados_removidos": duplicados_removidos,
        "rechazados_por_rango": rechazados_por_rango,
        "huecos_de_tiempo_detectados": conteo_huecos,
        "total_final": total_final,
    }

    cols_extra = ["gap_ms"] + (["_per"] if ventana == "mes" else [])
    return df_gap.drop(*cols_extra), reporte, huecos_df, conteo_huecos


def existe_success(spark, ruta):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(ruta.rstrip("/") + "/_SUCCESS")
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(fs.exists(p))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--ventana", choices=["simbolo", "mes"], default="simbolo",
                        help="particion de la ventana lag(): 'simbolo' (identica a "
                             "quality.py) o 'mes' (mas paralelismo)")
    parser.add_argument("--rep-escritura", type=int, default=0,
                        help="si >0, reparticiona por simbolo/anio/mes antes de escribir")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("FTX-SVB-Calidad-Klines-OPT").getOrCreate()
    t_inicio = time.time()
    codigo = 0
    try:
        dfs = []
        for simbolo, fuente in SIMBOLOS.items():
            print(f"Leyendo {simbolo} ...")
            dfs.append(leer_simbolo(spark, args.input, simbolo, fuente))
        df_todos = dfs[0]
        for df_extra in dfs[1:]:
            df_todos = df_todos.unionByName(df_extra)
        df_todos = convertir_tipos(df_todos)

        df_limpio, reporte, huecos, conteo_huecos = aplicar_reglas_calidad(
            spark, df_todos, args.ventana)

        print("\n=== REPORTE DE CALIDAD ===")
        for clave, valor in reporte.items():
            print(f"{clave}: {valor}")

        print("\n=== CONTEO DE FILAS POR SÍMBOLO/AÑO/MES (para el informe) ===")
        df_limpio.groupBy(
            "simbolo",
            F.year("open_time_ts").alias("anio"),
            F.month("open_time_ts").alias("mes")
        ).count().orderBy("simbolo", "anio", "mes").show(100, truncate=False)

        if conteo_huecos > 0:
            print("\n=== HUECOS DE TIEMPO DETECTADOS (no rellenados, ver Fase 3) ===")
            huecos.show(50, truncate=False)

        df_salida = df_limpio.withColumn("anio", F.year("open_time_ts")) \
                             .withColumn("mes", F.month("open_time_ts"))
        if args.rep_escritura > 0:
            df_salida = df_salida.repartition(
                args.rep_escritura, "simbolo", "anio", "mes")
        df_salida.write.mode("overwrite") \
            .partitionBy("simbolo", "anio", "mes") \
            .parquet(args.output)

        if not existe_success(spark, args.output):
            print(f"ERROR: no existe {args.output}/_SUCCESS", file=sys.stderr)
            codigo = 1
        else:
            print(f"\nListo. Datos limpios escritos en: {args.output}")
    except Exception as e:  # noqa: BLE001
        print(f"ERROR en quality_opt: {e!r}", file=sys.stderr)
        codigo = 1
    finally:
        print(f"[TIEMPO] TOTAL quality_opt (ventana={args.ventana}): "
              f"{time.time() - t_inicio:.1f} s")
        spark.stop()
    sys.exit(codigo)


if __name__ == "__main__":
    main()
