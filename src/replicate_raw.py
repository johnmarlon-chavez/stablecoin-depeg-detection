"""
Pedido 5 - Replicacion de datos crudos para prueba de escalabilidad (Spark)

Lee los klines crudos de la zona raw en HDFS (solo lectura, no se modifica
la zona raw) y escribe N copias desplazadas en el tiempo bajo la zona de
benchmark, para que quality.py y el join puedan correr sin cambios sobre
un dataset "factor" veces mas grande.

Cada copia desplaza open_time/close_time por un offset fijo (mayor al
rango real de datos de ese simbolo) para que dropDuplicates(["simbolo",
"open_time"]) en quality.py NO elimine las filas replicadas.

Uso:
    spark-submit --deploy-mode client --master yarn \
        src/replicate_raw.py \
        --input hdfs:///datalake/raw/binance/klines \
        --output hdfs:///datalake/benchmark/raw_10x \
        --factor 10
"""

import argparse

from pyspark.sql import SparkSession, functions as F
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

COLUMNAS_SALIDA = [campo.name for campo in ESQUEMA_KLINES.fields]

SIMBOLOS = ["USDCUSDT", "BTCUSDC", "BTCUSDT", "BUSDUSDT", "BTCBUSD"]
SIMBOLOS_CON_DIARIO = {"BUSDUSDT", "BTCBUSD"}

INTERVALO_ESPERADO_MS = 60 * 60 * 1000


def leer_crudo_simbolo(spark, base_path: str, simbolo: str):
    rutas = [f"{base_path}/{simbolo}/*.csv"]
    if simbolo in SIMBOLOS_CON_DIARIO:
        rutas.append(f"{base_path}/{simbolo}/diario/*.csv")
    return spark.read.schema(ESQUEMA_KLINES).csv(rutas)


def replicar_simbolo(df_base, factor: int):
    """Devuelve `factor` copias de df_base con open_time/close_time
    desplazados para no colisionar entre copias ni con el original."""
    stats = df_base.agg(
        F.min("open_time").alias("min_ts"),
        F.max("open_time").alias("max_ts"),
    ).collect()[0]
    rango_ms = (stats["max_ts"] - stats["min_ts"]) + INTERVALO_ESPERADO_MS

    ids = df_base.sparkSession.range(factor).withColumnRenamed("id", "replica_id")
    df_rep = df_base.crossJoin(F.broadcast(ids))
    df_rep = df_rep.withColumn("offset_ms", F.col("replica_id") * F.lit(rango_ms))
    df_rep = df_rep.withColumn("open_time", F.col("open_time") + F.col("offset_ms"))
    df_rep = df_rep.withColumn("close_time", F.col("close_time") + F.col("offset_ms"))
    return df_rep.select(*COLUMNAS_SALIDA)


def escribir_placeholder_vacio(spark, ruta: str):
    """Crea un CSV vacio (0 filas) para satisfacer el glob */diario/*.csv
    que quality.py espera para BUSDUSDT/BTCBUSD, sin duplicar datos.

    Usamos sparkContext.emptyRDD() (RDD vacio del lado JVM) en vez de
    spark.createDataFrame([], schema) con una lista Python: esta ultima
    crea un PythonRDD que necesita lanzar un proceso worker de Python en
    el executor, y ese proceso busca el interprete de la venv de marlon
    (PYSPARK_PYTHON), una ruta que solo existe en el master y no en el
    nodo de Andres. emptyRDD() no necesita ese proceso.
    """
    rdd_vacio = spark.sparkContext.emptyRDD()
    df_vacio = spark.createDataFrame(rdd_vacio, schema=ESQUEMA_KLINES)
    df_vacio.coalesce(1).write.mode("overwrite").option("header", "false").csv(ruta)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Ruta raw original (HDFS), solo lectura")
    parser.add_argument("--output", required=True, help="Ruta destino en zona benchmark (HDFS)")
    parser.add_argument("--factor", required=True, type=int, help="Factor de replicacion (10, 100, ...)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"Pedido5-Replicar-{args.factor}x").getOrCreate()

    total_filas_salida = 0
    for simbolo in SIMBOLOS:
        print(f"Replicando {simbolo} x{args.factor} ...")
        df_crudo = leer_crudo_simbolo(spark, args.input, simbolo)
        df_rep = replicar_simbolo(df_crudo, args.factor)

        ruta_simbolo = f"{args.output}/{simbolo}"
        df_rep.write.mode("overwrite").option("header", "false").csv(ruta_simbolo)

        filas = df_rep.count()
        total_filas_salida += filas
        print(f"  {simbolo}: {filas} filas escritas en {ruta_simbolo}")

        if simbolo in SIMBOLOS_CON_DIARIO:
            escribir_placeholder_vacio(spark, f"{ruta_simbolo}/diario")

    print(f"\nListo. Total de filas replicadas ({args.factor}x): {total_filas_salida}")
    print(f"Datos escritos en: {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
