#!/usr/bin/env python3
"""
Pedido 6 - Resumen horario de aggTrades, union con klines_clean y validacion cruzada.
Entradas : processed/binance/aggtrades_clean, processed/binance/klines_clean
Salidas  : processed/binance/aggtrades_hourly (Parquet simbolo/anio/mes)
           processed/binance/aggtrades_klines_join (Parquet simbolo/anio/mes)
Todo en UTC. Las diferencias grandes NO se corrigen: se reportan (prefijo QC).
Columnas: n_agg_trades, volumen_agg, n_trades_agg, prop_venta_agresiva, desv_max_intrahora.
Clave de union: (simbolo, hora UTC de apertura de la vela). BUSDUSDT con BUSDUSDT, USDCUSDT con USDCUSDT.
"""
import argparse
import time

from pyspark.sql import SparkSession, functions as F

TOL_VOL = 1e-4   # unidades base de la moneda; el error de coma flotante esta muy por debajo
P = "hdfs:///datalake/processed/binance/"


def hora_utc(col_ms):
    return (F.floor(F.col(col_ms) / 3600000) * 3600).cast("timestamp")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean", default=P + "aggtrades_clean")
    ap.add_argument("--klines", default=P + "klines_clean")
    ap.add_argument("--hourly", default=P + "aggtrades_hourly")
    ap.add_argument("--join", default=P + "aggtrades_klines_join")
    ap.add_argument("--espera", type=int, default=0,
                    help="segundos que mantiene viva la sesion al final (captura de Spark UI)")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("aggtrades_hourly")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---------- resumen horario ----------
    clean = spark.read.parquet(a.clean)
    n_clean = clean.count()
    h = (clean.withColumn("hora", hora_utc("transact_time"))
         .groupBy("simbolo", "hora")
         .agg(F.count("*").alias("n_agg_trades"),
              F.sum("quantity").alias("volumen_agg"),
              F.sum(F.col("last_trade_id") - F.col("first_trade_id") + 1).alias("n_trades_agg"),
              (F.sum(F.when(F.col("is_buyer_maker"), F.col("quantity")).otherwise(0.0)) /
               F.sum("quantity")).alias("prop_venta_agresiva"),
              F.max(F.abs(F.col("price") - F.lit(1.0))).alias("desv_max_intrahora"))
         .withColumn("anio", F.year("hora")).withColumn("mes", F.month("hora")))
    h.write.mode("overwrite").partitionBy("simbolo", "anio", "mes").parquet(a.hourly)
    h2 = spark.read.parquet(a.hourly)          # tabla pequena: de aqui en adelante todo es barato
    ev = (F.when((F.col("anio") == 2022) & (F.col("mes") == 5), "Terra")
          .when((F.col("anio") == 2022) & (F.col("mes") == 11), "FTX")
          .when((F.col("anio") == 2023) & (F.col("mes") == 3), "SVB"))
    h2 = h2.withColumn("evento", ev).cache()
    n_h = h2.count()
    print(f"QC_CLEAN registros_aggtrades_limpios={n_clean:,}")
    print("QC_HORARIO evento simbolo horas_presentes dias horas_esperadas")
    for r in (h2.groupBy("evento", "simbolo")
              .agg(F.count("*").alias("horas"),
                   F.countDistinct(F.to_date("hora")).alias("dias"))
              .orderBy("evento", "simbolo").collect()):
        print(f"QC_HORARIO {r['evento']} {r['simbolo']} {r['horas']} {r['dias']} {r['dias']*24}")
    print(f"QC_HORARIO total_filas_resumen={n_h}")

    # ---------- klines_clean ----------
    k = spark.read.parquet(a.klines)
    print("QC_KLINES columnas:", k.columns)
    falt = {"simbolo", "open_time", "volume", "trades"} - set(k.columns)
    assert not falt, f"faltan columnas en klines_clean: {falt}"
    n_k = k.count()
    ks = (k.select(F.col("simbolo").alias("k_sim"), hora_utc("open_time").alias("k_hora"),
                   F.col("volume").cast("double").alias("volume_kl"),
                   F.col("trades").cast("long").alias("n_trades_kl")))

    # ---------- union (left desde aggTrades) ----------
    j = (h2.join(F.broadcast(ks), (h2.simbolo == ks.k_sim) & (h2.hora == ks.k_hora), "left")
         .withColumn("con_klines", F.col("volume_kl").isNotNull())
         .drop("k_sim", "k_hora"))
    j.write.mode("overwrite").partitionBy("simbolo", "anio", "mes").parquet(a.join)
    j = spark.read.parquet(a.join).cache()
    n_j = j.count()
    m = j.filter("con_klines")
    n_m = m.count()
    print(f"QC_JOIN filas_resumen={n_j} filas_con_match={n_m} horas_aggtrades_sin_match_en_klines={n_j-n_m}")
    for r in j.filter(~F.col("con_klines")).orderBy("simbolo", "hora").collect():
        print(f"QC_SIN_MATCH {r['simbolo']} {r['hora']} n_agg_trades={r['n_agg_trades']}")

    # horas de klines (en los mismos dias) sin aggTrades
    dias = h2.select(F.col("simbolo").alias("d_sim"), F.to_date("hora").alias("d_dia")).distinct()
    kd = ks.withColumn("dia", F.to_date("k_hora")).join(
        dias, (F.col("k_sim") == F.col("d_sim")) & (F.col("dia") == F.col("d_dia")))
    sin_agg = kd.join(h2, (kd.k_sim == h2.simbolo) & (kd.k_hora == h2.hora), "left_anti")
    n_sa = sin_agg.count()
    print(f"QC_KLINES_SIN_AGGTRADES horas_de_klines_en_esos_dias_sin_aggtrades={n_sa}")
    for r in sin_agg.orderBy("k_sim", "k_hora").collect():
        print(f"QC_KLINES_SIN_AGG {r['k_sim']} {r['k_hora']}")

    # ---------- validacion cruzada ----------
    x = (m.withColumn("dif_vol", F.col("volumen_agg") - F.col("volume_kl"))
         .withColumn("dif_trades", F.col("n_trades_agg") - F.col("n_trades_kl"))
         .withColumn("mal_vol", F.abs(F.col("dif_vol")) > TOL_VOL)
         .withColumn("mal_tr", F.col("dif_trades") != 0)).cache()
    g = x.agg(F.max(F.abs("dif_vol")).alias("mx_vol"),
              F.max(F.abs("dif_trades")).alias("mx_tr"),
              F.sum(F.col("mal_vol").cast("int")).alias("n_mal_vol"),
              F.sum(F.col("mal_tr").cast("int")).alias("n_mal_tr"),
              F.sum((F.col("mal_vol") | F.col("mal_tr")).cast("int")).alias("n_mal_alguno")).collect()[0]
    print(f"QC_CRUCE horas_comparadas={n_m} tolerancia_volumen={TOL_VOL}")
    print(f"QC_CRUCE max_abs_dif_volumen={g['mx_vol']} horas_volumen_no_coincide={g['n_mal_vol']}")
    print(f"QC_CRUCE max_abs_dif_trades={g['mx_tr']} horas_trades_no_coincide={g['n_mal_tr']}")
    print(f"QC_CRUCE horas_con_alguna_diferencia={g['n_mal_alguno']}")
    print("QC_CRUCE_EVENTO evento simbolo horas mal_vol mal_trades max_dif_vol max_dif_trades")
    for r in (x.groupBy("evento", "simbolo")
              .agg(F.count("*").alias("h"), F.sum(F.col("mal_vol").cast("int")).alias("mv"),
                   F.sum(F.col("mal_tr").cast("int")).alias("mt"),
                   F.max(F.abs("dif_vol")).alias("xv"), F.max(F.abs("dif_trades")).alias("xt"))
              .orderBy("evento", "simbolo").collect()):
        print(f"QC_CRUCE_EVENTO {r['evento']} {r['simbolo']} {r['h']} {r['mv']} {r['mt']} {r['xv']} {r['xt']}")
    print("QC_TOP simbolo hora volumen_agg volume_kl dif_vol n_trades_agg n_trades_kl dif_trades")
    for r in (x.filter("mal_vol or mal_tr")
              .orderBy(F.abs("dif_trades").desc(), F.abs("dif_vol").desc()).limit(20).collect()):
        print(f"QC_TOP {r['simbolo']} {r['hora']} {r['volumen_agg']:.4f} {r['volume_kl']:.4f} "
              f"{r['dif_vol']:.4f} {r['n_trades_agg']} {r['n_trades_kl']} {r['dif_trades']}")
    print(f"QC_TOTALES aggtrades_limpios={n_clean:,} klines_clean={n_k:,} suma={n_clean+n_k:,}")

    if a.espera:
        print(f"QC_UI sesion viva {a.espera}s en el puerto 4040 para la captura")
        time.sleep(a.espera)
    spark.stop()


if __name__ == "__main__":
    main()
