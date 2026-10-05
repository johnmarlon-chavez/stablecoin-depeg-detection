#!/bin/bash
E=$1; CLEAN=$2
OUT=hdfs:///datalake/benchmark/joinopt_${E}x
LOG=~/jopt_${E}x.log
cd ~/stablecoin-depeg-detection
hdfs dfs -rm -r -f -skipTrash $OUT >/dev/null 2>&1
S=$(date +%s)
spark-submit --deploy-mode client --master yarn \
  --conf spark.driver.host=100.76.42.126 --conf spark.driver.bindAddress=0.0.0.0 \
  --conf "spark.yarn.jars=hdfs:///user/marlon/spark-libs/*.jar" \
  --conf spark.network.timeout=600s --conf spark.rpc.askTimeout=600s \
  --conf spark.rpc.lookupTimeout=600s --conf spark.executor.heartbeatInterval=60s \
  src/join_broadcast_solo.py --input $CLEAN --output $OUT 2>&1 | tee $LOG
RC=${PIPESTATUS[0]}
echo "=== RESUMEN join ${E}x ==="
echo "exit: $RC | segundos reales: $(( $(date +%s) - S ))"
hdfs dfs -test -e $OUT/_SUCCESS && echo "_SUCCESS: SI" || echo "_SUCCESS: NO"
APP=$(yarn application -list -appStates ALL 2>/dev/null | grep -o 'application_[0-9_]*' | sort | tail -1)
yarn application -status $APP 2>/dev/null | grep -E "Application-Id|Start-Time|Finish-Time|Aggregate Resource"
