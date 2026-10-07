#!/bin/bash
# uso: [LIM=segundos] [SP=64] bench_local.sh <orig|opt> <escala 10|100>
# Modo single-node (local[*]) en el master. Salidas solo en hdfs:///datalake/benchmark/
M=$1; E=$2; SP=${SP:-200}; LIM=${LIM:-7200}
B=hdfs:///datalake/benchmark
RAW=$B/raw_${E}x
if [ "$M" = "opt" ]; then CLEAN=$B/cleanopt_${E}x_local; else CLEAN=$B/clean_${E}x_local; fi
JOIN=$B/join_${E}x_local
TAG=local_${M}_${E}x
cd ~/stablecoin-depeg-detection
FLAGS=(--master local[*] --driver-memory 3g
  --conf spark.network.timeout=600s --conf spark.rpc.askTimeout=600s
  --conf spark.rpc.lookupTimeout=600s --conf spark.executor.heartbeatInterval=60s
  --conf spark.sql.shuffle.partitions=$SP)
export TIMEFORMAT='real %3R | user %3U | sys %3S'

muestrear() {
  echo "epoch,rss_mb,ram_usada_mb,swap_usado_mb" > $1
  while true; do
    P=$(pgrep -f org.apache.spark.deploy.SparkSubmit | head -1)
    RSS=0; [ -n "$P" ] && RSS=$(ps -o rss= -p $P 2>/dev/null); RSS=${RSS:-0}
    read U S <<< $(LC_ALL=C free -m | awk '/^Mem/{u=$3} /^Swap/{s=$3} END{print u, s}')
    echo "$(date +%s),$((RSS/1024)),$U,$S" >> $1
    sleep 5
  done
}

paso() { # $1=nombre $2=script $3=salida
  local N=$1 SCR=$2 OUT=$3 LOG=~/${TAG}_$1.log CSV=~/${TAG}_$1_mem.csv EXTRA=""
  [ "$SCR" = "quality_opt.py" ] && EXTRA="--ventana mes --rep-escritura 0"
  hdfs dfs -rm -r -f -skipTrash $OUT >/dev/null 2>&1
  muestrear $CSV & local SP_PID=$!
  local S=$(date +%s)
  { time timeout $LIM spark-submit "${FLAGS[@]}" src/$SCR --input $4 --output $OUT $EXTRA; } 2>&1 | tee $LOG
  local RC=${PIPESTATUS[0]}
  kill $SP_PID 2>/dev/null; wait $SP_PID 2>/dev/null
  local OK=NO; hdfs dfs -test -e $OUT/_SUCCESS && OK=SI
  {
    echo "=== RESUMEN $N ($M ${E}x local[*], driver-memory 3g, SP=$SP) ==="
    echo "exit: $RC (124 = detenido por limite LIM=${LIM}s) | segundos reales: $(( $(date +%s) - S )) | _SUCCESS: $OK"
    grep -E "^real" $LOG
    awk -F, 'NR>1{ if($2>r)r=$2; if($3>u)u=$3; if($4>s)s=$4 } END{print "max RSS JVM (MB): " r " | max RAM usada (MB): " u " | max swap (MB): " s}' $CSV
    echo "HDFS salida: $(hdfs dfs -du -s -h $OUT 2>/dev/null | head -1)"
    echo "FetchFailed/OutOfMemory/heartbeats en log: $(grep -c -E 'OutOfMemoryError' $LOG) OOM | $(grep -c 'no recent heartbeats' $LOG) heartbeat"
  } | tee -a ~/${TAG}_resumen.txt
  [ "$OK" = "SI" ]
}

echo "nproc: $(nproc) | $(LC_ALL=C free -m | awk '/^Mem/{print "RAM total MB: " $2}')" | tee ~/${TAG}_resumen.txt
SCRQ=quality.py; [ "$M" = "opt" ] && SCRQ=quality_opt.py
paso quality $SCRQ $CLEAN $RAW && paso join join_broadcast_solo.py $JOIN $CLEAN
