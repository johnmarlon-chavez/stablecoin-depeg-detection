#!/bin/bash
# uso: [LIM=segundos] [PART=200] run_escalabilidad_aggtrades.sh <local|cluster> <escala 1|2|3|5> <repeticion 1..3>
# local  : VM maestra, local[*], driver 2g (igual que la prueba anterior)
# cluster: YARN, ajusta EXEC_INST / EXEC_MEM / EXEC_CORES a lo que permita el nodo de Andres
# Entrada : hdfs:///datalake/benchmark/aggtrades_${E}x   Salida: hdfs:///datalake/benchmark/salida_*
M=$1; E=$2; R=${3:-1}; LIM=${LIM:-7200}; PART=${PART:-200}
B=hdfs:///datalake/benchmark
IN=$B/aggtrades_${E}x
OUT=$B/salida_${M}_${E}x_r${R}
TAG=${M}_${E}x_r${R}
RES=~/stablecoin-depeg-detection/benchmarks/resultados/escalabilidad_aggtrades
mkdir -p $RES; cd ~/stablecoin-depeg-detection
COMUN=(--conf spark.network.timeout=600s --conf spark.rpc.askTimeout=600s
       --conf spark.executor.heartbeatInterval=60s)
if [ "$M" = "local" ]; then
  FLAGS=(--master local[*] --driver-memory 2g "${COMUN[@]}")
else
  FLAGS=(--master yarn --deploy-mode client --driver-memory 2g
         --num-executors ${EXEC_INST:-2} --executor-memory ${EXEC_MEM:-4g} --executor-cores ${EXEC_CORES:-2}
         "${COMUN[@]}")
fi
muestrear() { # cada 2 s: RSS del proceso Spark, RAM usada y swap del sistema
  echo "epoch,rss_mb,ram_usada_mb,swap_usado_mb" > $1
  while true; do
    P=$(pgrep -f org.apache.spark.deploy.SparkSubmit | head -1)
    RSS=0; [ -n "$P" ] && RSS=$(ps -o rss= -p $P 2>/dev/null); RSS=${RSS:-0}
    read U S <<< $(LC_ALL=C free -m | awk '/^Mem/{u=$3} /^Swap/{s=$3} END{print u, s}')
    echo "$(date +%s),$((RSS/1024)),$U,$S" >> $1
    sleep 2
  done
}
LOG=$RES/${TAG}.log; CSV=$RES/${TAG}_mem.csv
hdfs dfs -rm -r -f -skipTrash $OUT >/dev/null 2>&1
echo "inicio $(date -Is) | disco maestro: $(df -h / | tail -1)" | tee $RES/${TAG}_resumen.txt
muestrear $CSV & SP_PID=$!
S=$(date +%s)
timeout $LIM spark-submit "${FLAGS[@]}" src/bench_aggtrades.py --input $IN --out $OUT --particiones $PART 2>&1 | tee $LOG
RC=${PIPESTATUS[0]}
kill $SP_PID 2>/dev/null; wait $SP_PID 2>/dev/null
{
  echo "=== RESUMEN $TAG ==="
  echo "exit: $RC (124 = limite LIM=${LIM}s; 137 = matado por falta de memoria) | segundos totales: $(( $(date +%s) - S ))"
  grep -E "^ETAPA|^QC" $LOG
  awk -F, 'NR>1{ if($2>r)r=$2; if($3>u)u=$3; if($4>s)s=$4 } END{print "max RSS Spark (MB): " r " | max RAM usada (MB): " u " | max swap (MB): " s}' $CSV
  echo "OOM en log: $(grep -c OutOfMemoryError $LOG) | heartbeats perdidos: $(grep -c 'no recent heartbeats' $LOG)"
  echo "dmesg (disco/oom): $(dmesg 2>/dev/null | tail -200 | grep -ciE 'i/o error|read-only|out of memory|killed process')"
} | tee -a $RES/${TAG}_resumen.txt
hdfs dfs -rm -r -f -skipTrash $OUT >/dev/null 2>&1
