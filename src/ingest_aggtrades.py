#!/usr/bin/env python3
"""
Pedido 6 - Ingesta y limpieza de aggTrades (zona raw -> processed).
Lee los CSV de hdfs:///datalake/raw/binance/aggtrades/<SIMBOLO>/csv/*.csv con
esquema explicito (sin inferSchema, sin cabecera), aplica reglas de calidad y
escribe los datos limpios en processed/binance/aggtrades_clean (carpeta NUEVA).
No toca klines ni klines_clean. Trabaja en UTC.

Reglas (cada una se cuenta por separado; una fila puede violar varias):
  f_dup    : (simbolo, agg_trade_id) repetido (se conserva la primera)
  f_precio : price > 0 no se cumple
  f_cant   : quantity > 0 no se cumple
  f_dia    : transact_time fuera del dia UTC del archivo
  f_nulo   : alguna de las 8 columnas es nula (fila malformada)
Salida por consola con prefijo QC para poder filtrarla.
"""
import argparse
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StructField,
                               StructType)

ESQUEMA = StructType([
    StructField("agg_trade_id", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("first_trade_id", LongType(), True),
    StructField("last_trade_id", LongType(), True),
    StructField("transact_time", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("is_best_match", BooleanType(), True),
])
COLS = [f.name for f in ESQUEMA.fields]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="hdfs:///datalake/raw/binance/aggtrades")
    ap.add_argument("--out", default="hdfs:///datalake/processed/binance/aggtrades_clean")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("ingest_aggtrades")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = (spark.read.schema(ESQUEMA).option("header", "false")
          .option("mode", "PERMISSIVE").csv(a.raw + "/*/csv/*.csv")
          .withColumn("_f", F.input_file_name())
          .withColumn("simbolo", F.regexp_extract("_f", r"/([A-Z]+)-aggTrades-", 1))
          .withColumn("fecha_archivo",
                      F.to_date(F.regexp_extract("_f", r"aggTrades-(\d{4}-\d{2}-\d{2})\.csv", 1)))
          .drop("_f"))

    ini = F.unix_timestamp(F.col("fecha_archivo").cast("timestamp")) * 1000
    fin = ini + 86400000
    w = Window.partitionBy("simbolo", "agg_trade_id").orderBy("fecha_archivo")

    df = (df.withColumn("_rn", F.row_number().over(w))
          .withColumn("f_dup", (F.col("_rn") > 1) & F.col("agg_trade_id").isNotNull())
          .withColumn("f_nulo", reduce(lambda x, y: x | y, [F.col(c).isNull() for c in COLS]))
          .withColumn("f_precio", ~F.coalesce(F.col("price") > 0, F.lit(False)))
          .withColumn("f_cant", ~F.coalesce(F.col("quantity") > 0, F.lit(False)))
          .withColumn("f_dia", ~F.coalesce((F.col("transact_time") >= ini) &
                                           (F.col("transact_time") < fin), F.lit(False)))
          .withColumn("valida", ~(F.col("f_dup") | F.col("f_nulo") | F.col("f_precio") |
                                  F.col("f_cant") | F.col("f_dia")))
          .drop("_rn"))
    df.persist(StorageLevel.DISK_ONLY)

    # --- unidad de transact_time ---
    print("QC unidad transact_time (digitos -> filas):")
    for r in (df.select(F.length(F.col("transact_time").cast("string")).alias("d"))
              .groupBy("d").count().orderBy("d").collect()):
        print(f"QC   digitos={r['d']} filas={r['count']:,}  "
              f"({'milisegundos' if r['d'] == 13 else 'microsegundos' if r['d'] == 16 else '?'})")

    # --- reglas por archivo ---
    flags = ["f_dup", "f_nulo", "f_precio", "f_cant", "f_dia", "valida"]
    res = (df.groupBy("simbolo", "fecha_archivo")
           .agg(F.count("*").alias("total"),
                *[F.sum(F.col(c).cast("int")).alias(c) for c in flags])
           .orderBy("simbolo", "fecha_archivo").collect())
    print("QC_ARCHIVO simbolo fecha total dup nulo precio cant dia validas")
    tot = dict(total=0, f_dup=0, f_nulo=0, f_precio=0, f_cant=0, f_dia=0, valida=0)
    for r in res:
        print(f"QC_ARCHIVO {r['simbolo']} {r['fecha_archivo']} {r['total']} {r['f_dup']} "
              f"{r['f_nulo']} {r['f_precio']} {r['f_cant']} {r['f_dia']} {r['valida']}")
        for k in tot:
            tot[k] += r[k]
    print(f"QC_TOTAL registros_raw={tot['total']:,} duplicados={tot['f_dup']:,} "
          f"nulos={tot['f_nulo']:,} precio_no_positivo={tot['f_precio']:,} "
          f"cantidad_no_positiva={tot['f_cant']:,} fuera_del_dia={tot['f_dia']:,}")
    print(f"QC_TOTAL registros_validos={tot['valida']:,} "
          f"descartados={tot['total'] - tot['valida']:,}")

    # --- escritura limpia ---
    ts = (F.col("transact_time") / 1000).cast("timestamp")
    limpio = (df.filter("valida").select(*COLS, "simbolo", "fecha_archivo")
              .withColumn("anio", F.year(ts)).withColumn("mes", F.month(ts)))
    (limpio.coalesce(4).write.mode("overwrite")
     .partitionBy("simbolo", "anio", "mes").parquet(a.out))
    n = spark.read.parquet(a.out).count()
    print(f"QC_ESCRITO {a.out} filas={n:,}")
    spark.stop()


if __name__ == "__main__":
    main()
