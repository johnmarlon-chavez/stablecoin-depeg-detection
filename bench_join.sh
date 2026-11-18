#!/bin/bash
# Pedido 5: corre una sola vez el join con broadcast (src/join_broadcast_solo.py) en YARN.
# uso: bench_join.sh <escala> <ruta_limpia_en_hdfs>
# Escribe en hdfs:///datalake/benchmark/joinopt_<E>x y deja el log en ~/jopt_<E>x.log.
E=$1; CLEAN=$2
OUT=hdfs:///datalake/benchmark/joinopt_${E}x
LOG=~/jopt_${E}x.log
cd ~/stablecoin-depeg-detection
# Borra una salida previa de la misma escala (solo dentro de la zona benchmark).
hdfs dfs -rm -r -f -skipTrash $OUT >/dev/null 2>&1
# Marca de inicio para calcular los segundos reales.
S=$(date +%s)
# Flags de red comunes: driver.host es la IP de Tailscale del master (el worker se conecta a el por ahi);
# los timeouts altos evitan que la red entre nodos corte tareas largas.
spark-submit --deploy-mode client --master yarn \
  --conf spark.driver.host=100.76.42.126 --conf spark.driver.bindAddress=0.0.0.0 \
  --conf "spark.yarn.jars=hdfs:///user/marlon/spark-libs/*.jar" \
  --conf spark.network.timeout=600s --conf spark.rpc.askTimeout=600s \
  --conf spark.rpc.lookupTimeout=600s --conf spark.executor.heartbeatInterval=60s \
  src/join_broadcast_solo.py --input $CLEAN --output $OUT 2>&1 | tee $LOG
# Codigo de salida de spark-submit (no el de tee).
RC=${PIPESTATUS[0]}
echo "=== RESUMEN join ${E}x ==="
echo "exit: $RC | segundos reales: $(( $(date +%s) - S ))"
hdfs dfs -test -e $OUT/_SUCCESS && echo "_SUCCESS: SI" || echo "_SUCCESS: NO"
# Datos de la ultima aplicacion de YARN: inicio, fin y recursos agregados (memoria y vcores por segundo).
APP=$(yarn application -list -appStates ALL 2>/dev/null | grep -o 'application_[0-9_]*' | sort | tail -1)
yarn application -status $APP 2>/dev/null | grep -E "Application-Id|Start-Time|Finish-Time|Aggregate Resource"
