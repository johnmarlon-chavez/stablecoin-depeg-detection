#!/usr/bin/env python3
"""
Pedido 9 - Replica aggtrades_clean N veces para la prueba de escalabilidad.
Copias de filas REALES; solo se desplaza agg_trade_id (y first/last_trade_id) en k * 1e12
en la copia k para que la regla de unicidad (simbolo, agg_trade_id) siga valida.
Nunca alimenta el analisis ni el modelo. Salida SOLO en hdfs:///datalake/benchmark/.
Uso: spark-submit src/replicate_aggtrades.py --factor 3 \
        --output hdfs:///datalake/benchmark/aggtrades_3x
"""
import argparse
from functools import reduce
from pyspark.sql import SparkSession, functions as F

P = "hdfs:///datalake/processed/binance/"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=P + "aggtrades_clean")
    ap.add_argument("--output", required=True)
    ap.add_argument("--factor", type=int, required=True)
    a = ap.parse_args()
    assert "/benchmark/" in a.output, "la salida debe ir en /datalake/benchmark/"
    assert a.output != a.input

    spark = (SparkSession.builder.appName(f"replicar_aggtrades_{a.factor}x")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    base = spark.read.parquet(a.input)
    n0 = base.count()
    copias = []
    for k in range(a.factor):
        d = k * 10**12
        copias.append(base.withColumn("agg_trade_id", F.col("agg_trade_id") + F.lit(d))
                          .withColumn("first_trade_id", F.col("first_trade_id") + F.lit(d))
                          .withColumn("last_trade_id", F.col("last_trade_id") + F.lit(d)))
    todo = reduce(lambda x, y: x.unionByName(y), copias)
    todo.write.mode("overwrite").partitionBy("simbolo", "anio", "mes").parquet(a.output)
    n = spark.read.parquet(a.output).count()
    print(f"REPLICA factor={a.factor} filas_base={n0:,} filas_replica={n:,} esperado={n0*a.factor:,} ok={n == n0*a.factor}")
    spark.stop()


if __name__ == "__main__":
    main()
