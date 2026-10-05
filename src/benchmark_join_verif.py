"""
Pedido 2 - Complemento del benchmark de join (recuperacion tras corte por OOM).

  --modo ui     Ejecuta UNA vez la version "despues" (broadcast explicito, con la
                misma configuracion que el benchmark), escribe en una ruta aparte,
                imprime el plan y espera para capturar el Spark UI. Termina al
                crear el archivo /tmp/pedido2_ui_listo.
  --modo verif  Compara las salidas ya escritas (join_tmp_antes y join_tmp_despues):
                total de filas y exceptAll en ambos sentidos.

Solo lee la zona processed; escribe unicamente en hdfs:///datalake/benchmark/.
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

SIMBOLOS_PRINCIPAL = ["USDCUSDT", "BTCUSDC", "BTCUSDT"]
SIMBOLOS_PROXY = ["BUSDUSDT", "BTCBUSD"]
ARCHIVO_LISTO = "/tmp/pedido2_ui_listo"


def preparar(spark, processed):
    df = spark.read.parquet(processed)
    principal = df.filter(df.simbolo.isin(SIMBOLOS_PRINCIPAL))
    proxy = df.filter(df.simbolo.isin(SIMBOLOS_PROXY))
    for c in proxy.columns:
        if c != "open_time_ts":
            proxy = proxy.withColumnRenamed(c, f"proxy_{c}")
    return principal, proxy


def modo_ui(spark, processed, salida):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    principal, proxy = preparar(spark, processed)
    resultado = principal.join(broadcast(proxy), on=["open_time_ts"], how="left")
    print("\n=== PLAN FISICO - DESPUES (broadcast hash join esperado) ===")
    resultado.explain()
    t0 = time.perf_counter()
    resultado.write.mode("overwrite").parquet(salida)
    print(f"\n[ui] escritura unica de la version DESPUES: {time.perf_counter() - t0:.3f}s")
    print(f"[ui] filas escritas: {spark.read.parquet(salida).count()}")
    if os.path.exists(ARCHIVO_LISTO):
        os.remove(ARCHIVO_LISTO)
    print(
        "\n>>> PAUSA: abre el Spark UI (http://master:4040), anota Shuffle Read / "
        "Shuffle Write del job de la escritura (el de 'parquet at ...') y captura "
        "SQL / DataFrame.\n"
        f"(Cuando termines, en OTRA terminal ejecuta: touch {ARCHIVO_LISTO})",
        flush=True,
    )
    while not os.path.exists(ARCHIVO_LISTO):
        time.sleep(2)


def modo_verif(spark, antes, despues):
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    df_a = spark.read.parquet(antes)
    df_d = spark.read.parquet(despues)
    n_a, n_d = df_a.count(), df_d.count()
    print(f"Total de registros (antes):   {n_a}")
    print(f"Total de registros (despues): {n_d}")
    ad = df_a.exceptAll(df_d).count()
    da = df_d.exceptAll(df_a).count()
    print(f"Filas en antes y no en despues: {ad}")
    print(f"Filas en despues y no en antes: {da}")
    if n_a == n_d and ad == 0 and da == 0:
        print("RESULTADO: ambas versiones producen EXACTAMENTE los mismos datos.")
    else:
        print("RESULTADO: *** DIFERENCIAS DETECTADAS - REVISAR ***")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--modo", choices=["ui", "verif"], required=True)
    ap.add_argument("--processed", default="hdfs:///datalake/processed/binance/klines_clean")
    ap.add_argument("--antes", default="hdfs:///datalake/benchmark/join_tmp_antes")
    ap.add_argument("--despues", default="hdfs:///datalake/benchmark/join_tmp_despues")
    ap.add_argument("--salida-ui", default="hdfs:///datalake/benchmark/join_tmp_despues_ui")
    a = ap.parse_args()

    spark = SparkSession.builder.appName(f"Pedido2-Complemento-{a.modo}").getOrCreate()
    try:
        if a.modo == "ui":
            modo_ui(spark, a.processed, a.salida_ui)
        else:
            modo_verif(spark, a.antes, a.despues)
    finally:
        spark.stop()
