#!/usr/bin/env python3
"""
Pedido 9 - Trabajo medido sobre aggTrades (el MISMO codigo en local[*] y en YARN).
Etapas: (1) reglas de calidad  (2) resumen horario por simbolo  (3) union con klines_clean.
Imprime 'ETAPA <nombre> <segundos>'. Sin collect de datos grandes ni toPandas.
Salidas solo en hdfs:///datalake/benchmark/.
"""
import argparse
import time
from pyspark.sql import SparkSession, functions as F

P = "hdfs:///datalake/processed/binance/"


def hora_utc(c):
    return (F.floor(F.col(c) / 3600000) * 3600).cast("timestamp")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)           # .../benchmark/aggtrades_Nx
    ap.add_argument("--klines", default=P + "klines_clean")
    ap.add_argument("--out", required=True)             # .../benchmark/salida_<tag>
    ap.add_argument("--particiones", type=int, default=200)
    a = ap.parse_args()
    assert "/benchmark/" in a.out
    spark = (SparkSession.builder.appName("bench_aggtrades")
             .config("spark.sql.shuffle.partitions", str(a.particiones))
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def etapa(nombre, t0):
        print(f"ETAPA {nombre} {time.time() - t0:.1f}", flush=True)

    df = spark.read.parquet(a.input)

    # 1) calidad: unicidad (shuffle completo), positivos, nulos
    t = time.time()
    dup = (df.groupBy("simbolo", "agg_trade_id").count().filter("count > 1").count())
    mal = df.agg(
        F.count("*").alias("n"),
        F.sum(((F.col("price") <= 0) | F.col("price").isNull()).cast("int")).alias("precio_mal"),
        F.sum(((F.col("quantity") <= 0) | F.col("quantity").isNull()).cast("int")).alias("cant_mal"),
        F.sum(F.col("agg_trade_id").isNull().cast("int")).alias("id_nulo")).collect()[0]
    print(f"QC registros={mal['n']:,} duplicados={dup} precio_mal={mal['precio_mal']} "
          f"cantidad_mal={mal['cant_mal']} id_nulo={mal['id_nulo']}", flush=True)
    etapa("calidad", t)

    # 2) resumen horario por simbolo
    t = time.time()
    h = (df.withColumn("hora", hora_utc("transact_time"))
         .groupBy("simbolo", "hora")
         .agg(F.count("*").alias("n_agg_trades"), F.sum("quantity").alias("volumen_agg"),
              F.sum(F.col("last_trade_id") - F.col("first_trade_id") + 1).alias("n_trades_agg"),
              (F.sum(F.when(F.col("is_buyer_maker"), F.col("quantity")).otherwise(0.0)) /
               F.sum("quantity")).alias("prop_venta_agresiva"),
              F.max(F.abs(F.col("price") - F.lit(1.0))).alias("desv_max_intrahora")))
    h.write.mode("overwrite").parquet(a.out + "/hourly")
    etapa("resumen_horario", t)

    # 3) union con klines_clean (el resumen es pequeno: broadcast de klines)
    t = time.time()
    hh = spark.read.parquet(a.out + "/hourly")
    k = (spark.read.parquet(a.klines)
         .select(F.col("simbolo").alias("k_sim"), hora_utc("open_time").alias("k_hora"),
                 F.col("volume").cast("double").alias("volume_kl")))
    j = hh.join(F.broadcast(k), (hh.simbolo == k.k_sim) & (hh.hora == k.k_hora), "left")
    j.write.mode("overwrite").parquet(a.out + "/join")
    n_j = spark.read.parquet(a.out + "/join").count()
    etapa("union_klines", t)
    print(f"QC filas_union={n_j}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
