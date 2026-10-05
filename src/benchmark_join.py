"""
Pedido 2 - Optimizacion de rendimiento (seccion 3.6)
Compara sort-merge join (broadcast desactivado) vs. broadcast join explicito
en la union de los pares principales (USDCUSDT, BTCUSDC, BTCUSDT) con los
sustitutos (BUSDUSDT, BTCBUSD), leyendo de la zona processed.

Uso:
spark-submit --deploy-mode client --master yarn \
    --conf spark.driver.host=100.76.42.126 \
    --conf spark.driver.bindAddress=0.0.0.0 \
    src/benchmark_join.py
"""
import io
import contextlib
import time
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

RUTA_PROCESSED = "hdfs:///datalake/processed/binance/klines_clean"
RUTA_BENCHMARK_ANTES = "hdfs:///datalake/benchmark/join_tmp_antes"
RUTA_BENCHMARK_DESPUES = "hdfs:///datalake/benchmark/join_tmp_despues"
ARCHIVO_EXPLAIN = "benchmark_explain_output.txt"

SIMBOLOS_PRINCIPAL = ["USDCUSDT", "BTCUSDC", "BTCUSDT"]
SIMBOLOS_PROXY = ["BUSDUSDT", "BTCBUSD"]
N_CORRIDAS = 6


def capturar_explain(df, nombre, archivo):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain()
    texto = buf.getvalue()
    print(f"\n=== PLAN FISICO - {nombre} ===")
    print(texto)
    with open(archivo, "a") as f:
        f.write(f"\n=== PLAN FISICO - {nombre} ===\n{texto}\n")


def esperar_senal(ruta_senal, mensaje):
    print(mensaje)
    print(f"(Cuando hayas anotado los valores, en OTRA terminal ejecuta: touch {ruta_senal})")
    if os.path.exists(ruta_senal):
        os.remove(ruta_senal)
    while not os.path.exists(ruta_senal):
        time.sleep(5)
    os.remove(ruta_senal)


def imprimir_config_cluster(spark):
    sc = spark.sparkContext
    print("\n=== CONFIGURACION DEL CLUSTER DURANTE LA PRUEBA ===")
    try:
        jinfos = list(sc._jsc.sc().statusTracker().getExecutorInfos())
        print(f"Numero de ejecutores activos (incluye driver): {len(jinfos)}")
        for e in jinfos:
            print(f"  host: {e.host()} | tasks activas: {e.numRunningTasks()}")
    except Exception as ex:
        print(f"(No se pudo listar ejecutores automaticamente: {ex})")
        print("Nota: revisar manualmente en el Spark UI -> pestana Executors.")
    print(f"spark.executor.memory = {sc.getConf().get('spark.executor.memory', 'default')}")
    print(f"spark.executor.cores  = {sc.getConf().get('spark.executor.cores', 'default')}")
    print(f"spark.executor.instances = {sc.getConf().get('spark.executor.instances', 'default')}")
    print(f"spark.cores.max = {sc.getConf().get('spark.cores.max', 'default')}")
    print(f"spark.default.parallelism (referencia) = {sc.defaultParallelism}")


def medir_corridas(resultado, ruta_salida, etiqueta, n=N_CORRIDAS):
    tiempos = []
    for i in range(n):
        spark.catalog.clearCache()
        t0 = time.perf_counter()
        resultado.write.mode("overwrite").parquet(ruta_salida)
        t1 = time.perf_counter()
        dt = t1 - t0
        tiempos.append(dt)
        print(f"  [{etiqueta}] corrida {i}: {dt:.3f}s")
    validas = tiempos[1:]  # descarta la corrida de calentamiento (indice 0)
    promedio = sum(validas) / len(validas)
    print(f"  [{etiqueta}] corridas validas: {len(validas)} de {n}")
    print(f"  [{etiqueta}] tiempos validos: {[round(t, 3) for t in validas]}")
    print(f"  [{etiqueta}] promedio: {promedio:.3f}s")
    return tiempos, validas, promedio


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Benchmark-Join-Broadcast-Pedido2").getOrCreate()

    # Desactivar difusion automatica y ejecucion adaptativa: sin esto, Spark
    # podria difundir la tabla chica por su cuenta y el "antes" ya seria un
    # broadcast, invalidando la comparacion.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    with open(ARCHIVO_EXPLAIN, "w") as f:
        f.write("Pedido 2 - Salida de .explain() - antes vs. despues\n")

    df = spark.read.parquet(RUTA_PROCESSED)
    principal = df.filter(df.simbolo.isin(SIMBOLOS_PRINCIPAL))
    proxy = df.filter(df.simbolo.isin(SIMBOLOS_PROXY))

    # Ambas tablas vienen del mismo esquema (quality.py), asi que comparten
    # nombres de columna fuera de la llave de union (anio, mes, simbolo, etc.).
    # Se renombra el lado proxy con un prefijo para evitar choque de columnas
    # al escribir el resultado a Parquet. Esto no cambia la estrategia de
    # join que se mide (sigue siendo la misma union logica).
    for c in proxy.columns:
        if c != "open_time_ts":
            proxy = proxy.withColumnRenamed(c, f"proxy_{c}")

    n_principal = principal.count()
    n_proxy = proxy.count()
    print(f"Filas tabla principal (USDCUSDT+BTCUSDC+BTCUSDT): {n_principal}")
    print(f"Filas tabla proxy (BUSDUSDT+BTCBUSD): {n_proxy}")

    imprimir_config_cluster(spark)

    # ===================== VERSION "ANTES": sort-merge join =====================
    resultado_antes = principal.join(proxy, on=["open_time_ts"], how="left")
    capturar_explain(resultado_antes, "ANTES (sort-merge join esperado)", ARCHIVO_EXPLAIN)

    print("\n--- Midiendo version ANTES (sort-merge join) ---")
    tiempos_antes, validas_antes, promedio_antes = medir_corridas(
        resultado_antes, RUTA_BENCHMARK_ANTES, "antes"
    )

    esperar_senal(
        "/tmp/pedido2_continuar_despues",
        "\n>>> PAUSA: abre el Spark UI (YARN -> tracking URL de la app) y anota "
        "Shuffle Read / Shuffle Write de los stages de la etapa 'antes' "
        "(busca los stages de la ultima escritura a join_tmp_antes)."
    )

    # ===================== VERSION "DESPUES": broadcast join explicito =====================
    resultado_despues = principal.join(broadcast(proxy), on=["open_time_ts"], how="left")
    capturar_explain(resultado_despues, "DESPUES (broadcast hash join esperado)", ARCHIVO_EXPLAIN)

    print("\n--- Midiendo version DESPUES (broadcast join) ---")
    tiempos_despues, validas_despues, promedio_despues = medir_corridas(
        resultado_despues, RUTA_BENCHMARK_DESPUES, "despues"
    )

    # ===================== VERIFICACION: MISMO RESULTADO =====================
    df_antes = spark.read.parquet(RUTA_BENCHMARK_ANTES)
    df_despues = spark.read.parquet(RUTA_BENCHMARK_DESPUES)
    count_antes = df_antes.count()
    count_despues = df_despues.count()
    diferencia_ad = df_antes.exceptAll(df_despues).count()
    diferencia_da = df_despues.exceptAll(df_antes).count()

    print("\n=== RESUMEN FINAL - PEDIDO 2 ===")
    print(f"Total de registros procesados en la union (antes):   {count_antes}")
    print(f"Total de registros procesados en la union (despues):  {count_despues}")
    print(f"Tiempos validos ANTES (sort-merge):   {[round(t, 3) for t in validas_antes]}")
    print(f"Promedio ANTES (sort-merge):          {promedio_antes:.3f}s")
    print(f"Tiempos validos DESPUES (broadcast):  {[round(t, 3) for t in validas_despues]}")
    print(f"Promedio DESPUES (broadcast):         {promedio_despues:.3f}s")
    print(f"Mejora (antes/despues):               {promedio_antes / promedio_despues:.2f}x")
    print(f"Filas distintas antes-no-en-despues:  {diferencia_ad}")
    print(f"Filas distintas despues-no-en-antes:  {diferencia_da}")
    if count_antes == count_despues and diferencia_ad == 0 and diferencia_da == 0:
        print("RESULTADO: ambas versiones producen EXACTAMENTE los mismos datos.")
    else:
        print("RESULTADO: *** DIFERENCIAS DETECTADAS - REVISAR ANTES DE REPORTAR ***")

    esperar_senal(
        "/tmp/pedido2_continuar_final",
        "\n>>> PAUSA FINAL: antes de cerrar, revisa de nuevo el Spark UI para "
        "confirmar el Shuffle Read/Write de los stages de 'despues' "
        "(deberia ser 0 o minimo, al no haber Exchange)."
    )

    spark.stop()
    print(f"\nListo. Salida de .explain() guardada en: {ARCHIVO_EXPLAIN}")
