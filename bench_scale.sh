#!/bin/bash
# Pedido 5: pipeline original (quality.py + join con broadcast) en YARN para una escala.
# uso: bench_scale.sh <escala 1|10|100>   (1 = datos raw originales)
# Logs en ~/q_<E>x_r2.log (limpieza) y ~/j_<E>x_r2.log (join).
E=$1
B=hdfs:///datalake/benchmark
# Flags de red comunes: driver.host es la IP de Tailscale del master (el worker se conecta a el por ahi);
# los timeouts altos evitan que la red entre nodos corte tareas largas.
FLAGS=(--conf spark.driver.host=100.76.42.126 --conf spark.driver.bindAddress=0.0.0.0
  --conf "spark.yarn.jars=hdfs:///user/marlon/spark-libs/*.jar"
  --conf spark.task.maxDirectResultSize=104857600
  --conf spark.network.timeout=600s --conf spark.rpc.askTimeout=600s
  --conf spark.rpc.lookupTimeout=600s --conf spark.executor.heartbeatInterval=60s)
RAW=$B/raw_${E}x; [ "$E" = "1" ] && RAW=hdfs:///datalake/raw/binance/klines; CLEAN=$B/clean_${E}x_r2; JOIN=$B/join_${E}x_r2
# Limpia salidas previas (zona benchmark).
hdfs dfs -rm -r -skipTrash $CLEAN $JOIN 2>/dev/null
cd ~/stablecoin-depeg-detection

# Paso 1: limpieza. Si no deja _SUCCESS, el script se detiene.
echo ">>> quality.py ${E}x"
{ time spark-submit --deploy-mode client --master yarn "${FLAGS[@]}" \
  src/quality.py --input "$RAW" --output "$CLEAN"; } 2>&1 | tee ~/q_${E}x_r2.log
hdfs dfs -test -e $CLEAN/_SUCCESS && echo "QUALITY OK" || { echo "QUALITY SIN _SUCCESS (FALLO)"; exit 1; }

# Paso 2: join con broadcast sobre la salida limpia.
echo ">>> join ${E}x"
{ time spark-submit --deploy-mode client --master yarn "${FLAGS[@]}" \
  src/join_broadcast_solo.py --input "$CLEAN" --output "$JOIN"; } 2>&1 | tee ~/j_${E}x_r2.log
hdfs dfs -test -e $JOIN/_SUCCESS && echo "JOIN OK" || echo "JOIN SIN _SUCCESS (FALLO)"

# Resumen: recursos de YARN por aplicacion, tiempos y numero de FetchFailed.
echo "=========== RESUMEN ${E}x ==========="
for f in q j; do
  A=$(grep -o "application_[0-9]*_[0-9]*" ~/${f}_${E}x_r2.log | head -1)
  echo "--- $f : $A"
  yarn application -status $A 2>/dev/null | grep -E "Start-Time|Finish-Time|Final-State|Aggregate Resource"
done
grep -h -E "^real|^user|^sys" ~/q_${E}x_r2.log ~/j_${E}x_r2.log
echo "FetchFailed: $(cat ~/q_${E}x_r2.log ~/j_${E}x_r2.log | grep -c FetchFailed)"
