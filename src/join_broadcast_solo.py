"""
Pedido 5 - Join broadcast (version ganadora de Pedido 2), corrida unica
para medir escalabilidad por volumen de datos.
"""

import argparse
import time

from pyspark.sql import SparkSession, functions as F

SIMBOLOS_PRINCIPAL = ["USDCUSDT", "BTCUSDC", "BTCUSDT"]
SIMBOLOS_PROXY = ["BUSDUSDT", "BTCBUSD"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Parquet limpio (salida de quality.py)")
    parser.add_argument("--output", required=True, help="Ruta de salida del join (zona benchmark)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Pedido5-Join-Broadcast").getOrCreate()

    df = spark.read.parquet(args.input)

    df_principal = df.filter(F.col("simbolo").isin(SIMBOLOS_PRINCIPAL))
    df_proxy = df.filter(F.col("simbolo").isin(SIMBOLOS_PROXY)) \
                 .filter(F.col("open_time_ts").isNotNull())

    columnas_proxy_renombradas = [
        F.col(c).alias(f"proxy_{c}") if c != "open_time_ts" else F.col(c)
        for c in df_proxy.columns
    ]
    df_proxy = df_proxy.select(*columnas_proxy_renombradas)

    df_join = df_principal.join(
        F.broadcast(df_proxy),
        on="open_time_ts",
        how="left_outer",
    )

    print("\n=== PLAN FISICO (broadcast) ===")
    df_join.explain()

    inicio = time.perf_counter()
    df_join.write.mode("overwrite").parquet(args.output)
    fin = time.perf_counter()

    total_filas = df_join.count()
    tiempo_total = fin - inicio

    print("\n=== RESULTADO PEDIDO 5 - JOIN BROADCAST ===")
    print(f"Entrada: {args.input}")
    print(f"Salida: {args.output}")
    print(f"Filas procesadas: {total_filas}")
    print(f"Tiempo de escritura (accion forzadora): {tiempo_total:.3f}s")
    print(f"Application ID: {spark.sparkContext.applicationId}")

    spark.stop()


if __name__ == "__main__":
    main()
