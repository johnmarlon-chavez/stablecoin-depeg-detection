# scripts/ — Descarga y carga de datos

Scripts de la **ingesta** (etapas 1 y 2 del pipeline): bajan los archivos públicos de [data.binance.vision](https://data.binance.vision), verifican su SHA-256 y los suben a la zona raw de HDFS. No usan la API REST de Binance.

| Script | Qué hace | Salida |
|---|---|---|
| `download_klines.py` | Descarga las velas horarias (1h) de `USDCUSDT`, `BTCUSDC` y `BTCUSDT` de 2022-01 a 2023-12, más los proxies `BUSDUSDT` y `BTCBUSD` en el hueco de oct-2022 a mar-2023 (decisión B.1 en [`docs/decisiones_abiertas.md`](../docs/decisiones_abiertas.md)). | `zona_cruda/klines/<SIMBOLO>/*.zip` y `zona_cruda/missing.log` |
| `download_aggtrades.py` | Descarga los aggTrades **diarios** de las tres ventanas de evento (14 archivos). Si el checksum no coincide, reintenta hasta 3 veces. Después extrae los CSV. | `~/aggtrades_local/{zip,csv}/<SIMBOLO>/` y `descarga_resumen.tsv` |
| `upload_aggtrades.sh` | Sube los zips y los CSV de aggTrades a HDFS. | `/datalake/raw/binance/aggtrades/<SIMBOLO>/` |
| `download_aggtrades.sh` | Vacío (marcador; se usa la versión en Python). | — |
| `audit_availability.py` | Vacío (marcador; la auditoría está en `docs/decisiones_abiertas.md`). | — |

## Uso (desde la raíz del repositorio)

```bash
python3 scripts/download_klines.py            # opciones: --solo-proxy, --solo-proxy-svb, --solo-proxy-btc, ...
python3 scripts/download_aggtrades.py         # --out RUTA para cambiar la carpeta local
bash scripts/upload_aggtrades.sh [RUTA_LOCAL]
```

Los klines se suben a HDFS a mano (`hdfs dfs -put`, ver la sección 5.2 del README principal). El notebook `notebooks/01_ingesta.ipynb` puede llamar a estos tres scripts si se pone `EJECUTAR_DESCARGA = True`.

> `descargar_dataset.py`, en la raíz, es la primera versión del descargador. El flujo vigente es el de esta carpeta.
