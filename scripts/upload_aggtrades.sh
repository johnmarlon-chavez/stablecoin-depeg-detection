#!/usr/bin/env bash
# Sube a la zona raw de HDFS los zips (sin renombrar) y los CSV extraidos de aggTrades.
#   /datalake/raw/binance/aggtrades/<SIMBOLO>/<archivo>.zip
#   /datalake/raw/binance/aggtrades/<SIMBOLO>/csv/<archivo>.csv
# Carpeta NUEVA, hermana de klines; no modifica nada existente.
set -euo pipefail
# LOC: carpeta local de la descarga (primer argumento; por defecto ~/aggtrades_local).
LOC="${1:-$HOME/aggtrades_local}"
RAW=/datalake/raw/binance/aggtrades
# Un directorio por simbolo: se sube el .zip original y el CSV extraido.
for d in "$LOC"/zip/*/; do
  s=$(basename "$d")
  hdfs dfs -mkdir -p "$RAW/$s/csv"
  hdfs dfs -put -f "$d"*.zip "$RAW/$s/"
  hdfs dfs -put -f "$LOC/csv/$s/"*.csv "$RAW/$s/csv/"
done
# Evidencia: listado y tamano de lo que quedo en HDFS.
echo "=== contenido de $RAW ==="
hdfs dfs -ls -R "$RAW"
echo "=== tamano aggtrades (total) ==="
hdfs dfs -du -s -h "$RAW"
