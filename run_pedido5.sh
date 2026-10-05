#!/bin/bash
set -e

RAW_ORIGINAL="hdfs:///datalake/raw/binance/klines"
BENCH="hdfs:///datalake/benchmark"

SPARK_YARN_NET_FLAGS="--conf spark.driver.host=100.76.42.126 --conf spark.driver.bindAddress=0.0.0.0 --conf spark.yarn.jars=hdfs:///user/marlon/spark-libs/*.jar --conf spark.task.maxDirectResultSize=104857600"

echo "=========================================="
echo " PEDIDO 5 - PRUEBA DE ESCALABILIDAD"
echo " Inicio: $(date)"
echo "=========================================="

for ESCALA in 1 10 100; do
    echo ""
    echo "---------- ESCALA ${ESCALA}x ----------"

    if [ "$ESCALA" -eq 1 ]; then
        RAW_PATH="$RAW_ORIGINAL"
    else
        RAW_PATH="${BENCH}/raw_${ESCALA}x"
        echo ">>> Replicando datos crudos a ${ESCALA}x ..."
        time spark-submit --deploy-mode client --master yarn \
            $SPARK_YARN_NET_FLAGS \
            src/replicate_raw.py \
            --input "$RAW_ORIGINAL" \
            --output "$RAW_PATH" \
            --factor "$ESCALA"
    fi

    CLEAN_PATH="${BENCH}/clean_${ESCALA}x"
    JOIN_PATH="${BENCH}/join_${ESCALA}x"

    echo ">>> quality.py sobre escala ${ESCALA}x ..."
    time spark-submit --deploy-mode client --master yarn \
        $SPARK_YARN_NET_FLAGS \
        src/quality.py \
        --input "$RAW_PATH" \
        --output "$CLEAN_PATH"

    echo ">>> join_broadcast_solo.py sobre escala ${ESCALA}x ..."
    time spark-submit --deploy-mode client --master yarn \
        $SPARK_YARN_NET_FLAGS \
        src/join_broadcast_solo.py \
        --input "$CLEAN_PATH" \
        --output "$JOIN_PATH"

    echo "---------- FIN ESCALA ${ESCALA}x ----------"
    echo "(Anota aqui el Application ID de quality.py y de join_broadcast_solo.py"
    echo " que salieron arriba, y corre: yarn application -status <appId>"
    echo " para memoria/CPU agregados de cada paso)"
done

echo ""
echo "=========================================="
echo " PEDIDO 5 - fin corridas en cluster"
echo " Fin: $(date)"
echo "=========================================="
echo ""
echo "Siguiente paso manual: correr la replica de 100x en modo single-node:"
echo "  spark-submit --deploy-mode client --master local[*] \\"
echo "      src/quality.py --input ${BENCH}/raw_100x --output ${BENCH}/clean_100x_local"
echo "  spark-submit --deploy-mode client --master local[*] \\"
echo "      src/join_broadcast_solo.py --input ${BENCH}/clean_100x_local --output ${BENCH}/join_100x_local"
