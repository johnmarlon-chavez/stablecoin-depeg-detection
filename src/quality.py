"""
Fase 3 — Calidad de datos y limpieza (Spark)

Lee los klines horarios de la zona raw en HDFS, valida contra las reglas
de calidad mínimas definidas en la guía operativa (sección 7.1), y escribe
la versión limpia como Parquet particionado en la zona processed.

Uso:
    spark-submit --deploy-mode client --master yarn \
        src/quality.py \
        --input hdfs:///datalake/raw/binance/klines \
        --output hdfs:///datalake/processed/binance/klines_clean
"""

import argparse

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

INTERVALO_ESPERADO_MS = 60 * 60 * 1000


def leer_simbolo(spark, base_path: str, simbolo: str, fuente_paridad: str):
    rutas = [f"{base_path}/{simbolo}/*.csv"]
    if simbolo in ("BUSDUSDT", "BTCBUSD"):
        rutas.append(f"{base_path}/{simbolo}/diario/*.csv")

    df = spark.read.schema(ESQUEMA_KLINES).csv(rutas)
    df = df.withColumn("simbolo", F.lit(simbolo)) \
           .withColumn("fuente_paridad", F.lit(fuente_paridad))
    return df


def convertir_tipos(df):
    return df \
        .withColumn("open_time_ts", (F.col("open_time") / 1000).cast("timestamp")) \
        .withColumn("close_time_ts", (F.col("close_time") / 1000).cast("timestamp"))


def aplicar_reglas_calidad(df):
    total_inicial = df.count()

    df_sin_duplicados = df.dropDuplicates(["simbolo", "open_time"])
    duplicados_removidos = total_inicial - df_sin_duplicados.count()

    condicion_rango_valido = (
        (F.col("open") > 0) & (F.col("high") > 0) &
        (F.col("low") > 0) & (F.col("close") > 0) &
        (F.col("high") >= F.col("low")) &
        (F.col("high") >= F.col("open")) & (F.col("high") >= F.col("close")) &
        (F.col("low") <= F.col("open")) & (F.col("low") <= F.col("close"))
    )
    df_valido = df_sin_duplicados.filter(condicion_rango_valido)
    rechazados_por_rango = df_sin_duplicados.count() - df_valido.count()

    ventana = Window.partitionBy("simbolo").orderBy("open_time")
    df_con_lag = df_valido.withColumn(
        "open_time_anterior", F.lag("open_time").over(ventana)
    )
    df_con_gap = df_con_lag.withColumn(
        "gap_ms", F.col("open_time") - F.col("open_time_anterior")
    )
    huecos = df_con_gap.filter(
        (F.col("gap_ms").isNotNull()) & (F.col("gap_ms") != INTERVALO_ESPERADO_MS)
    )
    conteo_huecos = huecos.count()

    reporte = {
        "total_inicial": total_inicial,
        "duplicados_removidos": duplicados_removidos,
        "rechazados_por_rango": rechazados_por_rango,
        "huecos_de_tiempo_detectados": conteo_huecos,
        "total_final": df_valido.count(),
    }

    return df_valido.drop("open_time_anterior"), reporte, huecos


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("FTX-SVB-Calidad-Klines") \
        .getOrCreate()

    dfs = []
    for simbolo, fuente in SIMBOLOS.items():
        print(f"Leyendo {simbolo} ...")
        df_simbolo = leer_simbolo(spark, args.input, simbolo, fuente)
        dfs.append(df_simbolo)

    df_todos = dfs[0]
    for df_extra in dfs[1:]:
        df_todos = df_todos.unionByName(df_extra)

    df_todos = convertir_tipos(df_todos)
    df_limpio, reporte, huecos = aplicar_reglas_calidad(df_todos)

    print("\n=== REPORTE DE CALIDAD ===")
    for clave, valor in reporte.items():
        print(f"{clave}: {valor}")

    print("\n=== CONTEO DE FILAS POR SÍMBOLO/AÑO/MES (para el informe) ===")
    df_limpio.groupBy(
        "simbolo",
        F.year("open_time_ts").alias("anio"),
        F.month("open_time_ts").alias("mes")
    ).count().orderBy("simbolo", "anio", "mes").show(100, truncate=False)

    if huecos.count() > 0:
        print("\n=== HUECOS DE TIEMPO DETECTADOS (no rellenados, ver Fase 3) ===")
        huecos.select("simbolo", "open_time_ts", "gap_ms").show(50, truncate=False)

    df_salida = df_limpio.withColumn("anio", F.year("open_time_ts")) \
                          .withColumn("mes", F.month("open_time_ts"))

    df_salida.write.mode("overwrite") \
        .partitionBy("simbolo", "anio", "mes") \
        .parquet(args.output)

    print(f"\nListo. Datos limpios escritos en: {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
