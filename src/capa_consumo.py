#!/usr/bin/env python3
"""
Pedido 10 - Capa de consumo: modelo en estrella (1 tabla de hechos horaria + 4 dimensiones).

Granularidad de fact_paridad_hora: una fila por hora UTC (misma serie de 17 172 horas del EDA y del modelo).
Entrada : data/features.parquet (serie horaria del modelo)  y  klines_clean (solo para contrastar el volumen).
Salidas : Parquet en <salida>/ (una carpeta por tabla; la tabla de hechos particionada por anio y mes)
          CSV en <csv_dir>/ (cinco archivos) para el tablero.
Reglas  : no se cambian umbrales ni la serie; no se rellenan huecos; spread_btc_usdc_usdt NO entra en la tabla de hechos.
Nota    : features.parquet guarda las marcas de tiempo en nanosegundos; Spark 3.5 las lee como entero largo con
          spark.sql.legacy.parquet.nanosAsLong=true y el script las convierte a timestamp. No requiere pandas.

Uso: spark-submit --master local[*] src/capa_consumo.py \
        --features data/features.parquet \
        --salida hdfs:///datalake/processed/binance/consumo \
        --klines hdfs:///datalake/processed/binance/klines_clean \
        --csv-dir data/consumo
"""
import argparse
import csv
import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Mismas ventanas del notebook 03_eda.ipynb: open_time_ts >= inicio y <= fin (comparacion con medianoche)
EVENTOS = [
    (1, "Terra", "2022-05-07", "2022-05-17", "contagio de mercado"),
    (2, "FTX", "2022-11-05", "2022-11-15", "contagio de mercado"),
    (3, "SVB", "2023-03-08", "2023-03-18", "exposición bancaria"),
]
UMBRAL_ALERTA = 0.0011   # no se cambian (Pedido 10, seccion 5)
UMBRAL_ESTRES = 0.0034
TOL = 1e-9               # los umbrales exactos del notebook 04 difieren de 0.0011 y 0.0034 en ~1e-16
DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def qc(msg):
    print("QC " + msg, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", default="data/features.parquet")
    ap.add_argument("--salida", required=True)
    ap.add_argument("--klines", default="")
    ap.add_argument("--csv-dir", default="data/consumo")
    a = ap.parse_args()
    assert "/consumo" in a.salida, "la salida debe ir en .../consumo"

    spark = (SparkSession.builder.appName("capa_consumo")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---------------- entrada: serie horaria del modelo ----------------
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    ruta = a.features if "://" in a.features else "file://" + os.path.abspath(a.features)
    raw = spark.read.parquet(ruta)
    if str(raw.schema["open_time_ts"].dataType) == "LongType()":          # nanosegundos -> timestamp
        ts = F.timestamp_micros((F.col("open_time_ts") / 1000).cast("long"))
    else:
        ts = F.col("open_time_ts").cast("timestamp")
    # spread_btc_usdc_usdt queda fuera a proposito
    base = raw.select(ts.alias("ts"), "fuente_paridad", "close", F.col("volume"), "desviacion_paridad", "severidad",
                      "volumen_relativo", "volatilidad_ventana")
    for c in ("close", "volume", "desviacion_paridad", "volumen_relativo", "volatilidad_ventana"):
        base = base.withColumn(c, F.when(F.isnan(F.col(c)), None).otherwise(F.col(c).cast("double")))
    n_base = base.count()
    qc(f"ENTRADA filas_serie_horaria={n_base}")

    # ---------------- dimensiones ----------------
    # calendario horario completo, calculado dentro de Spark (sin traer fechas al driver: evita problemas de zona horaria)
    base.createOrReplaceTempView("serie_base")
    cal = spark.sql("SELECT explode(sequence(date_trunc('hour', MIN(ts)), date_trunc('hour', MAX(ts)), "
                    "interval 1 hour)) AS ts FROM serie_base")
    dia_iso = ((F.dayofweek("ts") + 5) % 7)          # 0=lunes ... 6=domingo
    nombres_dia = F.array(*[F.lit(x) for x in DIAS])
    dim_tiempo = (cal.join(base.select("ts").withColumn("tiene_dato", F.lit(1)), "ts", "left")
                  .select(F.date_format("ts", "yyyyMMddHH").cast("long").alias("id_tiempo"),
                          F.to_date("ts").alias("fecha"), F.hour("ts").alias("hora"),
                          (dia_iso + 1).alias("dia_semana"), nombres_dia[dia_iso].alias("nombre_dia"),
                          F.month("ts").alias("mes"), F.year("ts").alias("anio"),
                          F.coalesce(F.col("tiene_dato"), F.lit(0)).alias("tiene_dato"))
                  .orderBy("id_tiempo"))

    dim_fuente = spark.createDataFrame(
        [(1, "USDCUSDT", "sí"), (2, "BUSDUSDT_proxy", "no")], ["id_fuente", "nombre", "es_real"])

    filas_ev = [(0, "fuera de evento", None, None, "no aplica")] + [(i, n, f1, f2, m) for i, n, f1, f2, m in EVENTOS]
    dim_evento = (spark.createDataFrame(filas_ev, ["id_evento", "nombre", "fecha_inicio", "fecha_fin", "mecanismo"])
                  .withColumn("fecha_inicio", F.to_date("fecha_inicio")).withColumn("fecha_fin", F.to_date("fecha_fin")))

    dim_severidad = spark.createDataFrame([
        (1, "normal", 0.0, UMBRAL_ALERTA, "sin acción"),
        (2, "alerta", UMBRAL_ALERTA, UMBRAL_ESTRES, "revisión del área de riesgo"),
        (3, "estrés", UMBRAL_ESTRES, None, "escalar a la jefatura de riesgo")],
        StructType([StructField("id_severidad", IntegerType()), StructField("nivel", StringType()),
                    StructField("umbral_min", DoubleType()), StructField("umbral_max", DoubleType()),
                    StructField("accion_sugerida", StringType())]))

    # ---------------- tabla de hechos ----------------
    evento_expr = F.lit(0)
    for i, _, f1, f2, _ in reversed(EVENTOS):
        evento_expr = F.when((F.col("ts") >= F.to_timestamp(F.lit(f1))) & (F.col("ts") <= F.to_timestamp(F.lit(f2))),
                             F.lit(i)).otherwise(evento_expr)
    sev_expr = (F.when(F.col("severidad") == "normal", 1).when(F.col("severidad") == "alerta", 2)
                .when(F.col("severidad") == "estres", 3))
    fact = (base.select(
        F.date_format("ts", "yyyyMMddHH").cast("long").alias("id_tiempo"),
        F.when(F.col("fuente_paridad") == "USDCUSDT", 1).when(F.col("fuente_paridad") == "BUSDUSDT_proxy", 2).alias("id_fuente"),
        evento_expr.alias("id_evento"), sev_expr.alias("id_severidad"),
        F.col("close").alias("close_paridad"), "desviacion_paridad", F.col("volume").alias("volumen"),
        "volumen_relativo", "volatilidad_ventana",
        (F.col("severidad") != "normal").cast("int").alias("en_alerta_o_estres"),
        (F.col("desviacion_paridad") >= 0.01).cast("int").alias("supera_1pct"),
        F.year("ts").alias("anio"), F.month("ts").alias("mes"))
        .withColumn("id_fuente", F.col("id_fuente").cast("int")).withColumn("id_evento", F.col("id_evento").cast("int"))
        .withColumn("id_severidad", F.col("id_severidad").cast("int")))

    # ---------------- escritura ----------------
    s = a.salida.rstrip("/")
    for nombre, df in [("dim_tiempo", dim_tiempo), ("dim_fuente", dim_fuente), ("dim_evento", dim_evento),
                       ("dim_severidad", dim_severidad)]:
        df.coalesce(1).write.mode("overwrite").parquet(f"{s}/{nombre}")
    fact.write.mode("overwrite").partitionBy("anio", "mes").parquet(f"{s}/fact_paridad_hora")
    qc(f"ESCRITURA tablas escritas en {s}")

    # ---------------- validacion (sobre lo que quedo escrito) ----------------
    f = spark.read.parquet(f"{s}/fact_paridad_hora")
    dt, df_, de, ds = (spark.read.parquet(f"{s}/{n}") for n in ("dim_tiempo", "dim_fuente", "dim_evento", "dim_severidad"))
    n_f = f.count()
    qc(f"HECHOS filas={n_f} esperado={n_base} ok={n_f == n_base == 17172}")
    qc(f"HECHOS horas_duplicadas={n_f - f.select('id_tiempo').distinct().count()}")
    for fk, dim, k in [("id_tiempo", dt, "id_tiempo"), ("id_fuente", df_, "id_fuente"),
                       ("id_evento", de, "id_evento"), ("id_severidad", ds, "id_severidad")]:
        huerf = f.join(dim.select(k), fk, "left_anti").count()
        nul = f.filter(F.col(fk).isNull()).count()
        qc(f"FK {fk} huerfanos={huerf} nulos={nul}")
    qc("DIM filas " + " ".join(f"{n}={t.count()}" for n, t in
                               [("dim_tiempo", dt), ("dim_fuente", df_), ("dim_evento", de), ("dim_severidad", ds)]))
    qc(f"DIM_TIEMPO horas_con_dato={dt.filter('tiene_dato=1').count()} horas_sin_dato(huecos)={dt.filter('tiene_dato=0').count()}")
    qc("COLUMNAS_HECHOS " + ",".join(c for c in f.columns))
    qc(f"SPREAD_EN_HECHOS={'spread_btc_usdc_usdt' in f.columns}")
    qc("CONTEO_POR_SEVERIDAD")
    for r in (f.join(ds, "id_severidad").groupBy("nivel").count().orderBy("nivel").collect()):
        qc(f"  {r['nivel']} {r['count']}")
    qc("CONTEO_POR_FUENTE")
    for r in (f.join(df_, "id_fuente").groupBy("nombre").count().orderBy("nombre").collect()):
        qc(f"  {r['nombre']} {r['count']}")
    qc("CONTEO_POR_EVENTO")
    for r in (f.join(de, "id_evento").groupBy("nombre").count().orderBy("nombre").collect()):
        qc(f"  {r['nombre']} {r['count']}")
    nulos = f.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in f.columns if c not in ("anio", "mes")]).collect()[0]
    qc("NULOS_POR_COLUMNA " + " ".join(f"{k}={v}" for k, v in nulos.asDict().items() if v))
    # coherencia de la severidad guardada con los umbrales (tolerancia 1e-9)
    # alerta: comparacion exacta con 0.0011; estres: el umbral real del notebook 04 es el minimo de los picos de los
    # tres eventos (0.0033999999999999586), que se muestra como 0.0034; por eso se admite una tolerancia de 1e-9 ahi
    inc = f.filter(
        ((F.col("desviacion_paridad") >= UMBRAL_ESTRES - TOL) & (F.col("id_severidad") != 3)) |
        ((F.col("desviacion_paridad") >= UMBRAL_ALERTA) & (F.col("desviacion_paridad") < UMBRAL_ESTRES - TOL) & (F.col("id_severidad") != 2)) |
        ((F.col("desviacion_paridad") < UMBRAL_ALERTA) & (F.col("id_severidad") != 1))).count()
    qc(f"COHERENCIA severidad_inconsistente_con_umbrales={inc}")
    qc(f"BANDERAS en_alerta_o_estres={f.agg(F.sum('en_alerta_o_estres')).collect()[0][0]} supera_1pct={f.agg(F.sum('supera_1pct')).collect()[0][0]}")

    # contraste del volumen con klines_clean (no bloquea si falla)
    if a.klines:
        try:
            k = spark.read.parquet(a.klines)
            k = k.select(F.col("simbolo").alias("k_sim"),
                         (F.floor(F.col("open_time") / 3600000)).cast("long").alias("k_h"),
                         F.col("volume").alias("k_vol"))
            ff = (f.join(df_, "id_fuente").withColumn("h", (F.unix_timestamp(F.to_timestamp(F.col("id_tiempo").cast("string"), "yyyyMMddHH")) / 3600).cast("long"))
                  .withColumn("k_sim", F.when(F.col("nombre") == "USDCUSDT", "USDCUSDT").otherwise("BUSDUSDT")))
            j = ff.join(k, (ff.k_sim == k.k_sim) & (ff.h == k.k_h), "left")
            tot = j.count(); sin = j.filter(F.col("k_vol").isNull()).count()
            dif = j.filter(F.col("k_vol").isNotNull() & (F.abs(F.col("volumen") - F.col("k_vol")) > 1e-6)).count()
            qc(f"KLINES_CLEAN horas_comparadas={tot} sin_fila_en_klines={sin} volumen_distinto={dif}")
        except Exception as e:  # noqa
            qc(f"KLINES_CLEAN no se pudo contrastar: {type(e).__name__}: {str(e)[:150]}")

    # ---------------- consulta de prueba (Spark SQL) ----------------
    for n, t in [("fact_paridad_hora", f), ("dim_tiempo", dt), ("dim_fuente", df_), ("dim_evento", de), ("dim_severidad", ds)]:
        t.createOrReplaceTempView(n)
    q = spark.sql("""
        SELECT e.nombre AS evento, fu.nombre AS fuente,
               COUNT(*) AS horas,
               SUM(CASE WHEN s.nivel = 'alerta' THEN 1 ELSE 0 END) AS horas_alerta,
               SUM(CASE WHEN s.nivel = 'estrés' THEN 1 ELSE 0 END) AS horas_estres,
               ROUND(100.0 * SUM(h.en_alerta_o_estres) / COUNT(*), 1) AS pct_alerta_o_estres,
               SUM(h.supera_1pct) AS horas_sobre_1pct,
               ROUND(MAX(h.desviacion_paridad), 4) AS desviacion_max
        FROM fact_paridad_hora h
        JOIN dim_evento e ON h.id_evento = e.id_evento
        JOIN dim_fuente fu ON h.id_fuente = fu.id_fuente
        JOIN dim_severidad s ON h.id_severidad = s.id_severidad
        GROUP BY e.nombre, fu.nombre ORDER BY e.nombre, fu.nombre""")
    print("\nCONSULTA_DE_PRUEBA: horas en alerta y estrés por evento y fuente")
    q.show(20, truncate=False)

    # ---------------- exportacion CSV (tablas pequeñas, sin pandas) ----------------
    os.makedirs(a.csv_dir, exist_ok=True)

    def a_csv(df, nombre):
        filas = df.collect()
        with open(os.path.join(a.csv_dir, nombre + ".csv"), "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(df.columns)
            for r in filas:
                w.writerow(["" if v is None else v for v in r])
        return len(filas)

    f_out = f.drop("anio", "mes").orderBy("id_tiempo")
    for nombre, t in [("fact_paridad_hora", f_out), ("dim_tiempo", dt.orderBy("id_tiempo")), ("dim_fuente", df_.orderBy("id_fuente")),
                      ("dim_evento", de.orderBy("id_evento")), ("dim_severidad", ds.orderBy("id_severidad"))]:
        qc(f"CSV {nombre}.csv filas={a_csv(t, nombre)}")
    a_csv(q, "consulta_prueba_horas_por_evento_fuente")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
