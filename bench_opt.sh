#!/bin/bash
# uso: [SP=16] bench_opt.sh <escala 1|10|100> <simbolo|mes>
E=$1; V=$2; SP=${SP:-200}; RW=${RW:-0}
B=hdfs:///datalake/benchmark
RAW=$B/raw_${E}x; [ "$E" = "1" ] && RAW=hdfs:///datalake/raw/binance/klines
OUT=$B/cleanopt_${E}x_${V}_sp${SP}_rw${RW}
LOG=~/qopt_${E}x_${V}_sp${SP}_rw${RW}.log
cd ~/stablecoin-depeg-detection
hdfs dfs -rm -r -f -skipTrash $OUT >/dev/null 2>&1
S=$(date +%s)
spark-submit --deploy-mode client --master yarn \
  --conf spark.driver.host=100.76.42.126 --conf spark.driver.bindAddress=0.0.0.0 \
  --conf "spark.yarn.jars=hdfs:///user/marlon/spark-libs/*.jar" \
  --conf spark.network.timeout=600s --conf spark.rpc.askTimeout=600s \
  --conf spark.rpc.lookupTimeout=600s --conf spark.executor.heartbeatInterval=60s \
  --conf spark.sql.shuffle.partitions=$SP \
  src/quality_opt.py --input $RAW --output $OUT --ventana $V --rep-escritura $RW 2>&1 | tee $LOG
RC=${PIPESTATUS[0]}
echo "=== RESUMEN opt ${E}x ventana=${V} SP=${SP} RW=${RW} ==="
echo "exit spark-submit: $RC | segundos reales: $(( $(date +%s) - S ))"
hdfs dfs -test -e $OUT/_SUCCESS && echo "_SUCCESS: SI" || echo "_SUCCESS: NO"
grep -E "TIEMPO|total_|duplicados|rechazados|huecos_de" $LOG
APP=$(yarn application -list -appStates ALL 2>/dev/null | grep -o 'application_[0-9_]*' | sort | tail -1)
yarn application -status $APP 2>/dev/null | grep -E "Application-Id|Start-Time|Finish-Time|Aggregate Resource"
